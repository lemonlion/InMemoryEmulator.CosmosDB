#nullable disable
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="Container"/> for testing. Supports the full
/// breadth of the Cosmos DB SDK surface area: CRUD, SQL queries (40+ built-in functions),
/// LINQ, transactional batches, change feed, patch operations, ETags, TTL, ReadMany,
/// stream APIs, stored procedures, UDFs, state persistence, and more.
/// </summary>
/// <remarks>
/// <para>
/// Create an instance directly for simple unit tests, or use the DI extension methods
/// (<see cref="ServiceCollectionExtensions"/>) for integration tests.
/// For HTTP-level interception with zero production code changes, wrap this container
/// in a <see cref="FakeCosmosHandler"/>.
/// </para>
/// <para>
/// Thread-safe for concurrent reads and writes. All data is stored in
/// <see cref="ConcurrentDictionary{TKey,TValue}"/> collections keyed by (id, partitionKey).
/// </para>
/// </remarks>
internal class InMemoryContainer : Container, IContainerTestSetup
{
	private static readonly JsonSerializerSettings JsonSettings = new()
	{
		TypeNameHandling = TypeNameHandling.None,
		DateParseHandling = DateParseHandling.None,
		ContractResolver = new DefaultContractResolver(),
		Converters = { new StringEnumConverter { AllowIntegerValues = true } }
	};

	private static readonly HashSet<string> AggregateFunctions =
		new(StringComparer.OrdinalIgnoreCase) { "COUNT", "COUNTIF", "SUM", "AVG", "MIN", "MAX" };

	/// <summary>
	/// Recursively checks whether a SqlExpression tree contains any aggregate function call
	/// (COUNT, COUNTIF, SUM, AVG, MIN, MAX). Used to detect aggregates inside object/array literals.
	/// </summary>
	private static bool ContainsAggregateCall(SqlExpression expr) => expr switch
	{
		FunctionCallExpression func => AggregateFunctions.Contains(func.FunctionName),
		ObjectLiteralExpression obj => obj.Properties.Any(p => ContainsAggregateCall(p.Value)),
		ArrayLiteralExpression arr => arr.Elements.Any(ContainsAggregateCall),
		BinaryExpression bin => ContainsAggregateCall(bin.Left) || ContainsAggregateCall(bin.Right),
		UnaryExpression unary => ContainsAggregateCall(unary.Operand),
		TernaryExpression tern => ContainsAggregateCall(tern.Condition) || ContainsAggregateCall(tern.IfTrue) || ContainsAggregateCall(tern.IfFalse),
		_ => false
	};

	private const int RegexCacheMaxSize = 256;
	private static readonly ConcurrentDictionary<(string Pattern, RegexOptions Options), Regex> RegexCache = new();

	private const int MaxDocumentSizeBytes = 2 * 1024 * 1024;
	private const double SyntheticRequestCharge = 1.0;
	private const double EarthRadiusMeters = 6_371_000.0;

	private static long _etagCounter;
	private static int _docRidCounter;

	private readonly ConcurrentDictionary<(string Id, string PartitionKey), string> _items = new();
	private readonly ConcurrentDictionary<(string Id, string PartitionKey), string> _etags = new();
	private readonly ConcurrentDictionary<(string Id, string PartitionKey), DateTimeOffset> _timestamps = new();
	// Ref: Observed behavior on Windows Cosmos DB Emulator — documents return in
	//   insertion order when no ORDER BY is applied. Maintains creation-time ordering
	//   so GetAllItemsForPartition can enumerate deterministically.
	private readonly List<(string Id, string PartitionKey)> _insertionOrder = new();
	private readonly object _insertionOrderLock = new();
	private readonly List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> _changeFeed = new();
	private readonly object _changeFeedLock = new();
	private long _changeFeedLsnCounter;
	private long _sessionSequence;
	private readonly object _uniqueKeyWriteLock = new();
	private readonly ConcurrentDictionary<(string Id, string PartitionKey), SemaphoreSlim> _itemLocks = new();
	private static readonly AsyncLocal<HashSet<(string Id, string PartitionKey)>> BatchWriteTracker = new();
	internal int _throughput = 400;
	internal bool _isDeleted;

	private bool HasUniqueKeys =>
		_containerProperties.UniqueKeyPolicy?.UniqueKeys.Count > 0;
	private ContainerProperties _containerProperties;
	private readonly Scripts _scripts;
	private readonly Dictionary<string, Func<object[], object>> _userDefinedFunctions = new(StringComparer.Ordinal);
	private static readonly Func<object[], object> UdfPlaceholder = _ => null;
	private readonly Dictionary<string, Func<PartitionKey, dynamic[], string>> _storedProcedures = new(StringComparer.Ordinal);
	private readonly Dictionary<string, StoredProcedureProperties> _storedProcedureProperties = new(StringComparer.Ordinal);
	private readonly Dictionary<string, RegisteredTrigger> _triggers = new(StringComparer.Ordinal);
	private readonly Dictionary<string, UserDefinedFunctionProperties> _udfProperties = new(StringComparer.Ordinal);
	private readonly Dictionary<string, TriggerProperties> _triggerProperties = new(StringComparer.Ordinal);
	private volatile (string Name, string FromAlias, SqlExpression Expr)[] _parsedComputedProperties;

	/// <summary>
	/// Optional JavaScript trigger engine. When set, triggers that have a <see cref="TriggerProperties.Body"/>
	/// but no C# handler will be executed via this engine. Set by calling <c>UseJsTriggers()</c>
	/// from the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
	/// </summary>
	public IJsTriggerEngine JsTriggerEngine { get; set; }

	/// <summary>
	/// Optional JavaScript stored procedure engine. When set, stored procedures that have a
	/// <see cref="StoredProcedureProperties.Body"/> but no C# handler will be executed via this engine.
	/// Set by calling <c>UseJsStoredProcedures()</c> from the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
	/// </summary>
	public ISprocEngine SprocEngine { get; set; }

	private sealed record RegisteredTrigger(
		TriggerType TriggerType,
		TriggerOperation TriggerOperation,
		Func<JObject, JObject> PreHandler,
		Action<JObject> PostHandler);

	private sealed class UndefinedValue
	{
		public static readonly UndefinedValue Instance = new();
		public override string ToString() => null;
		private UndefinedValue() { }
	}

	/// <summary>
	/// Type-aware equality comparer for JToken values, matching Cosmos DB semantics.
	/// Different types (number vs string) are never equal, even if their string representations match.
	/// </summary>
	private sealed class JTokenValueComparer : IEqualityComparer<JToken>
	{
		public static readonly JTokenValueComparer Instance = new();

		public bool Equals(JToken x, JToken y)
		{
			if (ReferenceEquals(x, y)) return true;
			if (x is null || y is null) return x is null && y is null;
			return JToken.DeepEquals(x, y);
		}

		public int GetHashCode(JToken obj)
		{
			if (obj is null) return 0;
			return obj.Type switch
			{
				JTokenType.Integer => HashCode.Combine(obj.Type, obj.Value<long>()),
				JTokenType.Float => HashCode.Combine(obj.Type, obj.Value<double>()),
				JTokenType.String => HashCode.Combine(obj.Type, obj.Value<string>()?.GetHashCode() ?? 0),
				JTokenType.Boolean => HashCode.Combine(obj.Type, obj.Value<bool>()),
				JTokenType.Null => HashCode.Combine(obj.Type),
				JTokenType.Object => HashObjectOrderInsensitive((JObject)obj),
				JTokenType.Array => HashArrayElements((JArray)obj),
				_ => HashCode.Combine(obj.Type, obj.ToString().GetHashCode())
			};
		}

		private static int HashObjectOrderInsensitive(JObject obj)
		{
			int hash = 0;
			foreach (var prop in obj.Properties())
			{
				hash ^= HashCode.Combine(prop.Name.GetHashCode(), Instance.GetHashCode(prop.Value));
			}
			return HashCode.Combine(JTokenType.Object, hash);
		}

		private static int HashArrayElements(JArray arr)
		{
			var hash = new HashCode();
			hash.Add(JTokenType.Array);
			foreach (var element in arr)
			{
				hash.Add(Instance.GetHashCode(element));
			}
			return hash.ToHashCode();
		}
	}

	/// <summary>
	/// Compares JSON strings using structural equality (property-order-insensitive).
	/// Used by DISTINCT to deduplicate projected results where objects may have
	/// identical values but different property ordering (e.g. after Patch operations).
	/// </summary>
	private sealed class JsonStructuralStringComparer : IEqualityComparer<string>
	{
		public static readonly JsonStructuralStringComparer Instance = new();

		public bool Equals(string x, string y)
		{
			if (x == y) return true;
			if (x is null || y is null) return false;
			try { return JToken.DeepEquals(JToken.Parse(x), JToken.Parse(y)); }
			catch { return false; }
		}

		public int GetHashCode(string obj)
		{
			if (obj is null) return 0;
			try { return JTokenValueComparer.Instance.GetHashCode(JToken.Parse(obj)); }
			catch { return obj.GetHashCode(); }
		}
	}

	internal Action OnDeleted { get; set; }

	/// <summary>
	/// Creates a new <see cref="InMemoryContainer"/> with a single partition key path.
	/// </summary>
	/// <param name="id">The container identifier. Defaults to <c>"in-memory-container"</c>.</param>
	/// <param name="partitionKeyPath">The JSON path to the partition key field (e.g. <c>/partitionKey</c>). Defaults to <c>/id</c>.</param>
	public InMemoryContainer(string id = "in-memory-container", string partitionKeyPath = "/id")
	{
		Id = id;
		_containerProperties = new ContainerProperties(id, partitionKeyPath);
		PartitionKeyPaths = new[] { partitionKeyPath };
		_scripts = new InMemoryScripts(this);
	}

	/// <summary>
	/// Creates a new <see cref="InMemoryContainer"/> with composite (hierarchical) partition key paths.
	/// </summary>
	/// <param name="id">The container identifier.</param>
	/// <param name="partitionKeyPaths">One or more JSON paths for the composite partition key.</param>
	public InMemoryContainer(string id, IReadOnlyList<string> partitionKeyPaths)
	{
		Id = id;
		PartitionKeyPaths = partitionKeyPaths;
		_containerProperties = new ContainerProperties(id, partitionKeyPaths);
		_scripts = new InMemoryScripts(this);
	}

	/// <summary>
	/// Creates a new <see cref="InMemoryContainer"/> from a <see cref="ContainerProperties"/> instance.
	/// This allows specifying advanced settings such as <see cref="UniqueKeyPolicy"/>.
	/// </summary>
	/// <param name="containerProperties">The container properties to use.</param>
	public InMemoryContainer(ContainerProperties containerProperties)
	{
		ValidateComputedProperties(containerProperties);
		_containerProperties = containerProperties;
		Id = containerProperties.Id;
		DefaultTimeToLive = containerProperties.DefaultTimeToLive;
		var paths = containerProperties.PartitionKeyPaths;
		if (paths is null || paths.Count == 0)
		{
			var singlePath = containerProperties.PartitionKeyPath ?? "/id";
			PartitionKeyPaths = new[] { singlePath };
		}
		else
		{
			PartitionKeyPaths = paths;
		}
		_scripts = new InMemoryScripts(this);
	}

	// ─── Properties ───────────────────────────────────────────────────────────

	/// <summary>The container identifier.</summary>
	public override string Id { get; } = default!;

	/// <summary>Returns a stubbed <see cref="Microsoft.Azure.Cosmos.Database"/> instance.</summary>
	public override Database Database
	{
		get
		{
			if (_cachedDatabase is null)
			{
				var db = Substitute.For<Database>();
				db.Id.Returns(_parentDatabaseId ?? Id);
				_cachedDatabase = db;
			}
			return _cachedDatabase;
		}
	}
	private Database _cachedDatabase;
	private string _parentDatabaseId;

	internal void SetParentDatabase(string databaseId) => _parentDatabaseId = databaseId;

	internal bool ExplicitlyCreated { get; set; } = true;

	/// <summary>Returns a stubbed <see cref="Microsoft.Azure.Cosmos.Conflicts"/> instance.</summary>
	public override Conflicts Conflicts => Substitute.For<Conflicts>();

	/// <summary>The <see cref="Microsoft.Azure.Cosmos.Scripts.Scripts"/> instance for executing stored procedures and UDFs.</summary>
	public override Scripts Scripts => _scripts;

	/// <summary>
	/// Container-level default TTL in seconds. When set, items expire after this duration
	/// unless overridden by a per-item <c>_ttl</c> property. Set to <c>null</c> to disable.
	/// Items are lazily evicted on the next read attempt.
	/// Setting to 0 throws BadRequest as in real Cosmos DB — use -1 for "enabled, no default expiry".
	/// </summary>
	public int? DefaultTimeToLive
	{
		get => _defaultTimeToLive;
		set
		{
			if (value == 0)
				throw InMemoryCosmosException.Create("The value of DefaultTimeToLive must be either null, -1, or a positive integer.",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_defaultTimeToLive = value;
		}
	}
	private int? _defaultTimeToLive;

	/// <inheritdoc />
	public UniqueKeyPolicy UniqueKeyPolicy
	{
		get => _containerProperties.UniqueKeyPolicy;
		set => _containerProperties.UniqueKeyPolicy = value ?? new UniqueKeyPolicy();
	}

	/// <summary>The partition key path(s) for this container.</summary>
	public IReadOnlyList<string> PartitionKeyPaths { get; }

	/// <summary>
	/// Maximum number of entries retained in the change feed log. When a new entry
	/// would exceed this limit, the oldest entries are evicted. Defaults to <c>1000</c>.
	/// Set to <c>0</c> to disable eviction (unbounded growth).
	/// </summary>
	public int MaxChangeFeedSize { get; set; } = 1000;

	/// <summary>
	/// Gets the current session token for this container.
	/// The token advances each time a write operation succeeds.
	/// </summary>
	internal string CurrentSessionToken => $"0:{_sessionSequence}#{_sessionSequence}";

	/// <summary>
	/// Number of feed ranges returned by <see cref="GetFeedRangesAsync"/>. Defaults to 1.
	/// Set to a higher value to simulate multiple physical partitions so that
	/// FeedRange-scoped queries and change feed iterators return subsets of data.
	/// </summary>
	public int FeedRangeCount { get; set; } = 1;

	/// <summary>
	/// When set, the container will automatically save its state to this file path on
	/// <see cref="Dispose"/> and can load state from it via <see cref="LoadPersistedState"/>.
	/// The directory will be created automatically if it does not exist.
	/// <para>
	/// Use with <see cref="LoadPersistedState"/> to preserve container state between test runs.
	/// Set via the <c>StatePersistenceDirectory</c> option on <c>UseInMemoryCosmosDB</c> /
	/// <c>UseInMemoryCosmosContainers</c> for automatic DI integration.
	/// </para>
	/// </summary>
	public string StateFilePath { get; set; }

	// ─── Public helpers for test infrastructure ───────────────────────────────

	/// <summary>
	/// Removes all items, ETags, timestamps, and change feed entries from the container.
	/// </summary>
	public void ClearItems()
	{
		_items.Clear();
		_etags.Clear();
		_timestamps.Clear();
		lock (_insertionOrderLock) { _insertionOrder.Clear(); }
		lock (_changeFeedLock) { _changeFeed.Clear(); }
	}

	/// <summary>Returns the number of non-expired items currently stored in the container.</summary>
	public int ItemCount => DefaultTimeToLive is null
		? _items.Count
		: _items.Keys.Count(k => !IsExpired(k));

	/// <summary>
	/// Saves the current container state to the file specified by <see cref="StateFilePath"/>.
	/// Does nothing if <see cref="StateFilePath"/> is null. Creates the directory if it does not exist.
	/// </summary>
	public void Dispose()
	{
		if (StateFilePath is not null)
		{
			var dir = Path.GetDirectoryName(StateFilePath);
			if (!string.IsNullOrEmpty(dir))
				Directory.CreateDirectory(dir);
			ExportStateToFile(StateFilePath);
		}
	}

	/// <summary>
	/// Loads container state from the file specified by <see cref="StateFilePath"/>.
	/// If the file does not exist, the container starts empty (no-op).
	/// If the file exists, its contents are imported via <see cref="ImportState"/>.
	/// </summary>
	/// <exception cref="InvalidOperationException">Thrown when <see cref="StateFilePath"/> is null.</exception>
	public void LoadPersistedState()
	{
		if (StateFilePath is null)
			throw new InvalidOperationException("StateFilePath must be set before calling LoadPersistedState().");

		if (File.Exists(StateFilePath))
			ImportStateFromFile(StateFilePath);
	}

	// ─── State persistence ────────────────────────────────────────────────────

	/// <summary>
	/// Exports the current container state as a JSON string.
	/// </summary>
	public string ExportState()
	{
		List<(string Id, string PartitionKey)> orderedKeys;
		lock (_insertionOrderLock) { orderedKeys = _insertionOrder.ToList(); }
		var items = orderedKeys
			.Where(key => _items.ContainsKey(key) && !IsExpired(key))
			.Select(key => JsonParseHelpers.ParseJson(_items[key])).ToList();
		var state = new JObject { ["items"] = new JArray(items) };
		return state.ToString(Formatting.Indented);
	}

	/// <summary>
	/// Imports container state from a JSON string, replacing all existing data.
	/// </summary>
	public void ImportState(string json)
	{
		var state = JObject.Parse(json);

		if (state["items"] is not JArray items)
			return; // No "items" key — do nothing (preserve existing data)

		ClearItems();

		foreach (var item in items)
		{
			var itemJson = item.ToString(Formatting.None);
			if (item["id"] is null)
				throw new InvalidOperationException("Each imported item must have an 'id' property.");
			var id = item["id"]!.ToString();
			var jObj = JsonParseHelpers.ParseJson(itemJson);
			var pk = ExtractPartitionKeyValue(null, jObj);
			var key = (id, pk);

			ValidateUniqueKeys(jObj, pk);

			var importEtag = GenerateETag();
			_etags[key] = importEtag;
			_timestamps[key] = DateTimeOffset.UtcNow;
			_items[key] = EnrichWithSystemProperties(itemJson, importEtag, _timestamps[key]);
			lock (_insertionOrderLock) { _insertionOrder.Add(key); }
		}
	}

	/// <summary>
	/// Exports the current container state to a file.
	/// </summary>
	public void ExportStateToFile(string filePath)
	{
		File.WriteAllText(filePath, ExportState());
	}

	/// <summary>
	/// Imports container state from a file, replacing all existing data.
	/// </summary>
	public void ImportStateFromFile(string filePath)
	{
		ImportState(File.ReadAllText(filePath));
	}

	// ─── Point-in-time restore ────────────────────────────────────────────────

	/// <summary>
	/// Restores the container to its state at the specified point in time by replaying
	/// the change feed. All current data is replaced with the reconstructed state.
	/// </summary>
	/// <param name="pointInTime">The timestamp to restore to. Only changes recorded
	/// at or before this time are included.</param>
	public void RestoreToPointInTime(DateTimeOffset pointInTime)
	{
		List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> feedSnapshot;
		lock (_changeFeedLock)
		{
			feedSnapshot = _changeFeed
				.Where(e => e.Timestamp <= pointInTime)
				.ToList();
		}

		// Replay: keep the last entry per (Id, PartitionKey), skip if it was a delete.
		// TTL eviction tombstones (marked with _ttlEviction) are ignored so that PITR
		// can resurrect items whose TTL expired — matching real Cosmos PITR behaviour.
		var lastPerKey = new Dictionary<(string Id, string PartitionKey), (string Json, bool IsDelete)>();
		foreach (var entry in feedSnapshot)
		{
			if (entry.IsDelete && entry.Json.Contains("\"_ttlEviction\":true", StringComparison.Ordinal))
				continue;
			lastPerKey[(entry.Id, entry.PartitionKey)] = (entry.Json, entry.IsDelete);
		}

		_items.Clear();
		_etags.Clear();
		_timestamps.Clear();
		_itemLocks.Clear();
		lock (_insertionOrderLock) { _insertionOrder.Clear(); }

		// Rebuild insertion order from the change feed replay sequence.
		// Track which keys were first created (not deleted) to preserve original insertion order.
		var insertionOrderKeys = new List<(string Id, string PartitionKey)>();
		foreach (var entry in feedSnapshot)
		{
			if (entry.IsDelete && entry.Json.Contains("\"_ttlEviction\":true", StringComparison.Ordinal))
				continue;
			var entryKey = (entry.Id, entry.PartitionKey);
			if (entry.IsDelete)
			{
				insertionOrderKeys.Remove(entryKey);
			}
			else if (!insertionOrderKeys.Contains(entryKey))
			{
				insertionOrderKeys.Add(entryKey);
			}
		}

		foreach (var kvp in lastPerKey)
		{
			if (kvp.Value.IsDelete) continue;

			var key = kvp.Key;
			var etag = $"\"{Guid.NewGuid()}\"";
			_items[key] = EnrichWithSystemProperties(kvp.Value.Json, etag, pointInTime);
			_etags[key] = etag;
			_timestamps[key] = pointInTime;
		}

		lock (_insertionOrderLock)
		{
			foreach (var key in insertionOrderKeys)
			{
				if (_items.ContainsKey(key))
					_insertionOrder.Add(key);
			}
		}
	}

	// ─── IndexingPolicy ───────────────────────────────────────────────────────

	/// <summary>
	/// The indexing policy for this container. Accepted and stored but does not affect
	/// query performance — all queries scan all items regardless of indexing settings.
	/// When set, automatically ensures <c>/_etag/?</c> is present in ExcludedPaths
	/// to match real Cosmos DB behaviour.
	/// </summary>
	public IndexingPolicy IndexingPolicy
	{
		get => _indexingPolicy;
		set
		{
			_indexingPolicy = value;
			EnsureEtagExcludedPath(_indexingPolicy);
		}
	}

	private IndexingPolicy _indexingPolicy = new()
	{
		Automatic = true,
		IndexingMode = IndexingMode.Consistent,
		IncludedPaths = { new IncludedPath { Path = "/*" } },
		ExcludedPaths = { new ExcludedPath { Path = "/\"_etag\"/?" } },
	};

	private static void EnsureEtagExcludedPath(IndexingPolicy policy)
	{
		const string etagPath = "/\"_etag\"/?";
		if (!policy.ExcludedPaths.Any(p => p.Path == etagPath))
			policy.ExcludedPaths.Add(new ExcludedPath { Path = etagPath });
	}

	/// <summary>
	/// Registers a user-defined function that can be called in SQL queries as <c>udf.name(args)</c>.
	/// </summary>
	public void RegisterUdf(string name, Func<object[], object> implementation)
	{
		ArgumentNullException.ThrowIfNull(name);
		_userDefinedFunctions["UDF." + name.TrimStart('.')] = implementation;
	}

	/// <summary>
	/// Removes a previously registered user-defined function.
	/// </summary>
	public void DeregisterUdf(string name)
	{
		ArgumentNullException.ThrowIfNull(name);
		_userDefinedFunctions.Remove("UDF." + name.TrimStart('.'));
	}

	/// <summary>
	/// Registers a stored procedure handler that is invoked when <c>ExecuteStoredProcedureAsync</c> is called.
	/// </summary>
	public void RegisterStoredProcedure(string sprocId, Func<PartitionKey, dynamic[], string> handler)
	{
		ArgumentNullException.ThrowIfNull(sprocId);
		ArgumentNullException.ThrowIfNull(handler);
		_storedProcedures[sprocId] = handler;
	}

	/// <summary>
	/// Removes a previously registered stored procedure handler.
	/// </summary>
	public void DeregisterStoredProcedure(string sprocId)
	{
		ArgumentNullException.ThrowIfNull(sprocId);
		_storedProcedures.Remove(sprocId);
	}

	/// <summary>
	/// Registers a pre-trigger handler invoked when <c>ItemRequestOptions.PreTriggers</c> includes this trigger's ID.
	/// The handler receives the document as a <see cref="JObject"/> and must return the (possibly modified) document.
	/// </summary>
	public void RegisterTrigger(string triggerId, TriggerType triggerType, TriggerOperation triggerOperation,
		Func<JObject, JObject> preHandler)
	{
		ArgumentNullException.ThrowIfNull(triggerId);
		_triggers[triggerId] = new RegisteredTrigger(triggerType, triggerOperation, preHandler, null);
	}

	/// <summary>
	/// Registers a post-trigger handler invoked when <c>ItemRequestOptions.PostTriggers</c> includes this trigger's ID.
	/// The handler receives the committed document as a <see cref="JObject"/>.
	/// If the handler throws, the write is rolled back (matching real Cosmos DB transactional semantics).
	/// </summary>
	public void RegisterTrigger(string triggerId, TriggerType triggerType, TriggerOperation triggerOperation,
		Action<JObject> postHandler)
	{
		ArgumentNullException.ThrowIfNull(triggerId);
		_triggers[triggerId] = new RegisteredTrigger(triggerType, triggerOperation, null, postHandler);
	}

	/// <summary>
	/// Removes a previously registered trigger handler.
	/// </summary>
	public void DeregisterTrigger(string triggerId)
	{
		ArgumentNullException.ThrowIfNull(triggerId);
		_triggers.Remove(triggerId);
	}

	// ─── JS body registrations (IContainerTestSetup) ──────────────────────────

	private const string JsTriggersNotInstalled =
		"JavaScript stored procedure execution requires the CosmosDB.InMemoryEmulator.JsTriggers package. " +
		"Install it and call container.UseJsTriggers(), or use the C# delegate overload instead.";

	private const string JsUdfNotInstalled =
		"JavaScript UDF execution requires the CosmosDB.InMemoryEmulator.JsTriggers package. " +
		"Install it and call container.UseJsTriggers(), or use the C# delegate overload instead.";

	private const string JsTriggerNotInstalled =
		"JavaScript trigger execution requires the CosmosDB.InMemoryEmulator.JsTriggers package. " +
		"Install it and call container.UseJsTriggers(), or use the C# delegate overload instead.";

	void IContainerTestSetup.RegisterStoredProcedure(string id, string jsBody)
	{
		if (SprocEngine is null)
			throw new NotImplementedException(JsTriggersNotInstalled);

		RegisterStoredProcedure(id, (pk, args) =>
		{
			var result = SprocEngine.Execute(jsBody, pk, args, new PartitionScopedCollectionContext(this, pk));
			return result ?? "null";
		});

		_storedProcedureProperties[id] = new StoredProcedureProperties { Id = id, Body = jsBody };
	}

	void IContainerTestSetup.RegisterUdf(string name, string jsBody)
	{
		if (JsTriggerEngine is not IJsUdfEngine)
			throw new NotImplementedException(JsUdfNotInstalled);

		// Register a placeholder — actual JS execution happens at query time via the UDF properties
		_userDefinedFunctions["UDF." + name.TrimStart('.')] = UdfPlaceholder;
		_udfProperties[name] = new UserDefinedFunctionProperties { Id = name, Body = jsBody };
	}

	void IContainerTestSetup.RegisterTrigger(string id, TriggerType type, TriggerOperation operation,
		string jsBody)
	{
		if (JsTriggerEngine is null)
			throw new NotImplementedException(JsTriggerNotInstalled);

		_triggerProperties[id] = new TriggerProperties
		{
			Id = id,
			TriggerType = type,
			TriggerOperation = operation,
			Body = jsBody
		};

		if (type == TriggerType.Pre)
		{
			_triggers[id] = new RegisteredTrigger(type, operation,
				doc => JsTriggerEngine.ExecutePreTrigger(jsBody, doc), null);
		}
		else
		{
			_triggers[id] = new RegisteredTrigger(type, operation, null,
				doc => JsTriggerEngine.ExecutePostTrigger(jsBody, doc));
		}
	}

	/// <summary>
	/// Returns a checkpoint value representing the current position in the change feed.
	/// Pass this to <see cref="GetChangeFeedIterator{T}(long)"/> to read changes since this point.
	/// </summary>
	public long GetChangeFeedCheckpoint()
	{
		lock (_changeFeedLock)
		{
			return _changeFeed.Count;
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Item CRUD — Typed
	// ═══════════════════════════════════════════════════════════════════════════

	public override async Task<ItemResponse<T>> CreateItemAsync<T>(
		T item, PartitionKey? partitionKey = null,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var json = JsonConvert.SerializeObject(item, JsonSettings);
		ValidateDocumentSize(json);
		var jObj = JsonParseHelpers.ParseJson(json);

		jObj = ExecutePreTriggers(requestOptions, jObj, "Create");
		json = jObj.ToString(Newtonsoft.Json.Formatting.None);
		ValidateDocumentSize(json);

		var itemId = jObj["id"]?.ToString() ?? throw new InvalidOperationException("Item must have an 'id' property.");

		if (itemId.Length == 0)
		{
			throw InMemoryCosmosException.Create("The 'id' property cannot be an empty string.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		ValidatePartitionKeyConsistency(partitionKey, jObj);
		ValidatePerItemTtl(jObj);
		var pk = ExtractPartitionKeyValue(partitionKey, jObj);
		var key = ItemKey(itemId, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			EvictIfExpired(key);

			if (HasUniqueKeys)
			{
				lock (_uniqueKeyWriteLock)
				{
					ValidateUniqueKeys(jObj, pk);
					if (!_items.TryAdd(key, json))
					{
						throw InMemoryCosmosException.Create($"Entity with the specified id already exists in the system. id = {itemId}",
							HttpStatusCode.Conflict, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
					}
				}
			}
			else
			{
				if (!_items.TryAdd(key, json))
				{
					throw InMemoryCosmosException.Create($"Entity with the specified id already exists in the system. id = {itemId}",
						HttpStatusCode.Conflict, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}
			}

			TrackBatchWrite(key);
			lock (_insertionOrderLock) { _insertionOrder.Add(key); }
			var etag = GenerateETag();
			_etags[key] = etag;
			_timestamps[key] = DateTimeOffset.UtcNow;
			_items[key] = EnrichWithSystemProperties(json, etag, _timestamps[key]);

			try
			{
				var committedDoc = JsonParseHelpers.ParseJson(_items[key]);
				var responseBodyOverride = ExecutePostTriggers(requestOptions, committedDoc, "Create");
				var postTriggerJson = committedDoc.ToString(Newtonsoft.Json.Formatting.None);
				if (postTriggerJson != _items[key])
				{
					ValidateDocumentSize(postTriggerJson);
					_items[key] = postTriggerJson;
				}

				RecordChangeFeed(itemId, pk, _items[key]);

				var suppressContent = requestOptions?.EnableContentResponseOnWrite == false;
				if (responseBodyOverride is not null && !suppressContent)
				{
					return CreateItemResponse(
						responseBodyOverride.ToObject<T>(JsonSerializer.Create(JsonSettings)),
						HttpStatusCode.Created, etag, suppressContent);
				}

				return CreateItemResponse(
					JsonConvert.DeserializeObject<T>(_items[key], JsonSettings),
					HttpStatusCode.Created, etag, suppressContent);
			}
			catch
			{
				_items.TryRemove(key, out _);
				_etags.TryRemove(key, out _);
				_timestamps.TryRemove(key, out _);
				lock (_insertionOrderLock) { _insertionOrder.Remove(key); }
				throw;
			}
		}
		finally
		{
			itemLock.Release();
		}
	}

	public override Task<ItemResponse<T>> ReadItemAsync<T>(
		string id, PartitionKey partitionKey,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var pk = PartitionKeyToString(partitionKey);
		var key = ItemKey(id, pk);

		if (!_items.TryGetValue(key, out var json) || IsExpired(key))
		{
			EvictIfExpired(key);
			// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
			//   404 Not found — "The operation is attempting to act on a resource that no longer exists."
			throw InMemoryCosmosException.Create($"Resource Not Found. Entity with the specified id does not exist in the system. id = {id}",
				HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		CheckIfNoneMatch(requestOptions, key);
		var etag = _etags.GetValueOrDefault(key);
		var result = JsonConvert.DeserializeObject<T>(json, JsonSettings);
		return Task.FromResult(CreateItemResponse(result, HttpStatusCode.OK, etag));
	}

	public override async Task<ItemResponse<T>> UpsertItemAsync<T>(
		T item, PartitionKey? partitionKey = null,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var json = JsonConvert.SerializeObject(item, JsonSettings);
		ValidateDocumentSize(json);
		var jObj = JsonParseHelpers.ParseJson(json);

		jObj = ExecutePreTriggers(requestOptions, jObj, "Upsert");
		json = jObj.ToString(Newtonsoft.Json.Formatting.None);
		ValidateDocumentSize(json);

		var itemId = jObj["id"]?.ToString() ?? throw new InvalidOperationException("Item must have an 'id' property.");
		ValidatePartitionKeyConsistency(partitionKey, jObj);
		ValidatePerItemTtl(jObj);
		var pk = ExtractPartitionKeyValue(partitionKey, jObj);
		var key = ItemKey(itemId, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			EvictIfExpired(key);

			// Note: If-Match is documented as "applicable only on PUT and DELETE" in the REST API.
			// Upsert is POST-based, so If-Match is not evaluated for the insert path.
			// If the item exists, CheckIfMatch will evaluate it on the update path.
			CheckIfMatch(requestOptions, key);
			bool existed;
			string previousJson;
			string previousEtag;
			DateTimeOffset? previousTimestamp;
			string etag;
			if (HasUniqueKeys)
			{
				lock (_uniqueKeyWriteLock)
				{
					ValidateUniqueKeys(jObj, pk, excludeItemId: itemId);
					existed = _items.ContainsKey(key);
					previousJson = existed ? _items[key] : null;
					previousEtag = existed ? _etags.GetValueOrDefault(key) : null;
					previousTimestamp = existed ? _timestamps.GetValueOrDefault(key) : default(DateTimeOffset?);
					etag = GenerateETag();
					_etags[key] = etag;
					_timestamps[key] = DateTimeOffset.UtcNow;
					_items[key] = EnrichWithSystemProperties(json, etag, _timestamps[key]);
				}
			}
			else
			{
				existed = _items.ContainsKey(key);
				previousJson = existed ? _items[key] : null;
				previousEtag = existed ? _etags.GetValueOrDefault(key) : null;
				previousTimestamp = existed ? _timestamps.GetValueOrDefault(key) : default(DateTimeOffset?);
				etag = GenerateETag();
				_etags[key] = etag;
				_timestamps[key] = DateTimeOffset.UtcNow;
				_items[key] = EnrichWithSystemProperties(json, etag, _timestamps[key]);
			}

			TrackBatchWrite(key);
			if (!existed) { lock (_insertionOrderLock) { _insertionOrder.Add(key); } }

			try
			{
				var committedDoc = JsonParseHelpers.ParseJson(_items[key]);
				ExecutePostTriggers(requestOptions, committedDoc, "Upsert");
				var postTriggerJson = committedDoc.ToString(Newtonsoft.Json.Formatting.None);
				if (postTriggerJson != _items[key])
				{
					ValidateDocumentSize(postTriggerJson);
					_items[key] = postTriggerJson;
				}
				RecordChangeFeed(itemId, pk, _items[key]);
			}
			catch
			{
				if (existed && previousJson is not null)
				{
					_items[key] = previousJson;
					_etags[key] = previousEtag!;
					if (previousTimestamp.HasValue) _timestamps[key] = previousTimestamp.Value;
				}
				else
				{
					_items.TryRemove(key, out _);
					_etags.TryRemove(key, out _);
					_timestamps.TryRemove(key, out _);
					lock (_insertionOrderLock) { _insertionOrder.Remove(key); }
				}
				throw;
			}

			var suppressContent = requestOptions?.EnableContentResponseOnWrite == false;
			return CreateItemResponse(
				JsonConvert.DeserializeObject<T>(_items[key], JsonSettings),
				existed ? HttpStatusCode.OK : HttpStatusCode.Created, etag, suppressContent);
		}
		finally
		{
			itemLock.Release();
		}
	}

	public override async Task<ItemResponse<T>> ReplaceItemAsync<T>(
		T item, string id, PartitionKey? partitionKey = null,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var json = JsonConvert.SerializeObject(item, JsonSettings);
		ValidateDocumentSize(json);
		var jObj = JsonParseHelpers.ParseJson(json);

		// Validate body id matches parameter id.
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
		//   "The Container.ReplaceItemAsync<> method requires the provided string for the id
		//    parameter to match the unique identifier of the item parameter."
		var bodyId = jObj["id"]?.ToString();
		if (bodyId is not null && bodyId != id)
		{
			throw InMemoryCosmosException.Create(
				"The 'id' property in the body does not match the 'id' parameter.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		ValidatePartitionKeyConsistency(partitionKey, jObj);

		ValidatePerItemTtl(jObj);

		var pk = ExtractPartitionKeyValue(partitionKey, jObj);
		var key = ItemKey(id, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			if (!_items.ContainsKey(key) || IsExpired(key))
			{
				EvictIfExpired(key);
				// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
				//   404 Not found — "The operation is attempting to act on a resource that no longer exists."
				throw InMemoryCosmosException.Create($"Resource Not Found. Entity with the specified id does not exist in the system. id = {id}",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			}

			CheckIfMatch(requestOptions, key);

			// Pre-triggers run after ETag check (matching real Cosmos behavior)
			jObj = ExecutePreTriggers(requestOptions, jObj, "Replace");
			json = jObj.ToString(Newtonsoft.Json.Formatting.None);
			ValidateDocumentSize(json);
			string previousJson;
			string previousEtag;
			DateTimeOffset previousTimestamp;
			string etag;
			if (HasUniqueKeys)
			{
				lock (_uniqueKeyWriteLock)
				{
					ValidateUniqueKeys(jObj, pk, excludeItemId: id);
					previousJson = _items[key];
					previousEtag = _etags.GetValueOrDefault(key);
					previousTimestamp = _timestamps.GetValueOrDefault(key);
					etag = GenerateETag();
					_etags[key] = etag;
					_timestamps[key] = DateTimeOffset.UtcNow;
					_items[key] = EnrichWithSystemProperties(json, etag, _timestamps[key]);
				}
			}
			else
			{
				previousJson = _items[key];
				previousEtag = _etags.GetValueOrDefault(key);
				previousTimestamp = _timestamps.GetValueOrDefault(key);
				etag = GenerateETag();
				_etags[key] = etag;
				_timestamps[key] = DateTimeOffset.UtcNow;
				_items[key] = EnrichWithSystemProperties(json, etag, _timestamps[key]);
			}

			TrackBatchWrite(key);
			try
			{
				var committedDoc = JsonParseHelpers.ParseJson(_items[key]);
				ExecutePostTriggers(requestOptions, committedDoc, "Replace");
				var postTriggerJson = committedDoc.ToString(Newtonsoft.Json.Formatting.None);
				if (postTriggerJson != _items[key])
				{
					ValidateDocumentSize(postTriggerJson);
					_items[key] = postTriggerJson;
				}
				RecordChangeFeed(id, pk, _items[key]);
			}
			catch
			{
				_items[key] = previousJson;
				_etags[key] = previousEtag!;
				_timestamps[key] = previousTimestamp;
				throw;
			}

			var suppressContent = requestOptions?.EnableContentResponseOnWrite == false;
			return CreateItemResponse(JsonConvert.DeserializeObject<T>(_items[key], JsonSettings), HttpStatusCode.OK, etag, suppressContent);
		}
		finally
		{
			itemLock.Release();
		}
	}

	public override async Task<ItemResponse<T>> DeleteItemAsync<T>(
		string id, PartitionKey partitionKey,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var pk = PartitionKeyToString(partitionKey);
		var key = ItemKey(id, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			if (!_items.TryGetValue(key, out var existingJson) || IsExpired(key))
			{
				EvictIfExpired(key);
				// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
				//   404 Not found — "The operation is attempting to act on a resource that no longer exists."
				throw InMemoryCosmosException.Create($"Resource Not Found. Entity with the specified id does not exist in the system. id = {id}",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			}

			CheckIfMatch(requestOptions, key);

			ExecutePreTriggers(requestOptions, JsonParseHelpers.ParseJson(existingJson), "Delete");

			var previousEtag = _etags.TryGetValue(key, out var e) ? e : null;
			var previousTimestamp = _timestamps.TryGetValue(key, out var ts) ? ts : (DateTimeOffset?)null;

			_items.TryRemove(key, out _);
			_etags.TryRemove(key, out _);
			_timestamps.TryRemove(key, out _);
			lock (_insertionOrderLock) { _insertionOrder.Remove(key); }

			TrackBatchWrite(key);
			try
			{
				ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(existingJson), "Delete");
				RecordDeleteTombstone(id, pk, partitionKey);
			}
			catch
			{
				_items[key] = existingJson;
				if (previousEtag is not null) _etags[key] = previousEtag;
				if (previousTimestamp.HasValue) _timestamps[key] = previousTimestamp.Value;
				lock (_insertionOrderLock) { _insertionOrder.Add(key); }
				throw;
			}

			return CreateItemResponse(default(T), HttpStatusCode.NoContent);
		}
		finally
		{
			itemLock.Release();
		}
	}

	public override async Task<ItemResponse<T>> PatchItemAsync<T>(
		string id, PartitionKey partitionKey,
		IReadOnlyList<PatchOperation> patchOperations,
		PatchItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();

		if (patchOperations is null || patchOperations.Count == 0)
		{
			throw InMemoryCosmosException.Create("Patch request has no operations.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		if (patchOperations.Count > 10)
		{
			throw InMemoryCosmosException.Create("Patch request has too many operations.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		ValidatePatchPaths(patchOperations);

		var pk = PartitionKeyToString(partitionKey);
		var key = ItemKey(id, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			return PatchItemCore<T>(id, pk, key, patchOperations, requestOptions);
		}
		finally
		{
			itemLock.Release();
		}
	}

	private ItemResponse<T> PatchItemCore<T>(
		string id, string pk, (string Id, string PartitionKey) key,
		IReadOnlyList<PatchOperation> patchOperations,
		PatchItemRequestOptions requestOptions)
	{
		if (!_items.TryGetValue(key, out var existingJson) || IsExpired(key))
		{
			EvictIfExpired(key);
			// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
			//   404 Not found — "The operation is attempting to act on a resource that no longer exists."
			throw InMemoryCosmosException.Create($"Resource Not Found. Entity with the specified id does not exist in the system. id = {id}",
				HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		CheckIfMatch(requestOptions, key);
		CheckIfNoneMatchForWrite(requestOptions, key);

		var jObj = JsonParseHelpers.ParseJson(existingJson);

		if (requestOptions?.FilterPredicate is not null)
		{
			var predicateSql = $"SELECT * {requestOptions.FilterPredicate}";
			var predicateParsed = CosmosSqlParser.Parse(predicateSql);
			if (predicateParsed.Where is not null)
			{
				var matches = EvaluateWhereExpression(predicateParsed.Where, jObj, predicateParsed.FromAlias,
					new Dictionary<string, object>(), null, treatUndefinedAsNull: true);
				if (!matches)
				{
					throw InMemoryCosmosException.Create("Precondition Failed",
						HttpStatusCode.PreconditionFailed, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}
			}
		}

		jObj = ExecutePreTriggers(requestOptions, jObj, "Replace");

		ApplyPatchOperations(jObj, patchOperations);
		ValidatePerItemTtl(jObj);
		var updatedJson = jObj.ToString(Formatting.None);
		ValidateDocumentSize(updatedJson);
		string etag;
		if (HasUniqueKeys)
		{
			lock (_uniqueKeyWriteLock)
			{
				ValidateUniqueKeys(jObj, pk, excludeItemId: id);
				etag = GenerateETag();
				_etags[key] = etag;
				_timestamps[key] = DateTimeOffset.UtcNow;
				var enriched = EnrichWithSystemProperties(updatedJson, etag, _timestamps[key]);
				_items[key] = enriched;
			}
		}
		else
		{
			etag = GenerateETag();
			_etags[key] = etag;
			_timestamps[key] = DateTimeOffset.UtcNow;
			var enriched = EnrichWithSystemProperties(updatedJson, etag, _timestamps[key]);
			_items[key] = enriched;
		}
		var enrichedJson = _items[key];

		TrackBatchWrite(key);
		ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(enrichedJson), "Replace");
		RecordChangeFeed(id, pk, _items[key]);

		var suppressContent = requestOptions?.EnableContentResponseOnWrite == false;
		var result = JsonConvert.DeserializeObject<T>(enrichedJson, JsonSettings);
		return CreateItemResponse(result, HttpStatusCode.OK, etag, suppressContent);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Item CRUD — Stream
	// ═══════════════════════════════════════════════════════════════════════════

	public override async Task<ResponseMessage> CreateItemStreamAsync(
		Stream streamPayload, PartitionKey partitionKey,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var json = ReadStream(streamPayload);
		var sizeError = ValidateDocumentSizeStream(json);
		if (sizeError is not null) return sizeError;
		JObject jObj;
		try { jObj = JsonParseHelpers.ParseJson(json); }
		catch (Newtonsoft.Json.JsonReaderException)
		{ return CreateResponseMessage(HttpStatusCode.BadRequest); }

		jObj = ExecutePreTriggers(requestOptions, jObj, "Create");
		json = jObj.ToString(Newtonsoft.Json.Formatting.None);
		sizeError = ValidateDocumentSizeStream(json);
		if (sizeError is not null) return sizeError;
		var ttlError = ValidatePerItemTtlStream(jObj);
		if (ttlError is not null) return ttlError;

		var itemId = jObj["id"]?.ToString() ?? Guid.NewGuid().ToString();

		if (itemId.Length == 0)
			return CreateResponseMessage(HttpStatusCode.BadRequest);

		var pkMismatch = ValidatePartitionKeyConsistencyStream(partitionKey, jObj);
		if (pkMismatch is not null) return pkMismatch;

		var pk = ExtractPartitionKeyValue(partitionKey, jObj);
		var key = ItemKey(itemId, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			EvictIfExpired(key);

			if (HasUniqueKeys)
			{
				lock (_uniqueKeyWriteLock)
				{
					if (!ValidateUniqueKeysStream(jObj, pk))
						return CreateResponseMessage(HttpStatusCode.Conflict);

					if (!_items.TryAdd(key, json))
						return CreateResponseMessage(HttpStatusCode.Conflict);
				}
			}
			else
			{
				if (!_items.TryAdd(key, json))
					return CreateResponseMessage(HttpStatusCode.Conflict);
			}

			TrackBatchWrite(key);
			lock (_insertionOrderLock) { _insertionOrder.Add(key); }
			var etag = GenerateETag();
			_etags[key] = etag;
			_timestamps[key] = DateTimeOffset.UtcNow;
			var enrichedJson = EnrichWithSystemProperties(json, etag, _timestamps[key]);
			_items[key] = enrichedJson;

			try
			{
				ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(enrichedJson), "Create");
				RecordChangeFeed(itemId, pk, _items[key]);
			}
			catch
			{
				_items.TryRemove(key, out _);
				_etags.TryRemove(key, out _);
				_timestamps.TryRemove(key, out _);
				lock (_insertionOrderLock) { _insertionOrder.Remove(key); }
				throw;
			}

			return CreateResponseMessage(HttpStatusCode.Created,
				requestOptions?.EnableContentResponseOnWrite == false ? null : enrichedJson, etag);
		}
		finally
		{
			itemLock.Release();
		}
	}

	public override Task<ResponseMessage> ReadItemStreamAsync(
		string id, PartitionKey partitionKey,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var pk = PartitionKeyToString(partitionKey);
		var key = ItemKey(id, pk);
		if (!_items.TryGetValue(key, out var json) || IsExpired(key))
		{
			EvictIfExpired(key);
			return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotFound));
		}
		var etag = _etags.GetValueOrDefault(key);
		if (!CheckIfNoneMatchStream(requestOptions, key))
		{
			return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotModified, etag: etag));
		}
		return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, json, etag));
	}

	public override async Task<ResponseMessage> UpsertItemStreamAsync(
		Stream streamPayload, PartitionKey partitionKey,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var json = ReadStream(streamPayload);
		var sizeError = ValidateDocumentSizeStream(json);
		if (sizeError is not null) return sizeError;
		JObject jObj;
		try { jObj = JsonParseHelpers.ParseJson(json); }
		catch (Newtonsoft.Json.JsonReaderException)
		{ return CreateResponseMessage(HttpStatusCode.BadRequest); }

		jObj = ExecutePreTriggers(requestOptions, jObj, "Upsert");
		json = jObj.ToString(Newtonsoft.Json.Formatting.None);
		sizeError = ValidateDocumentSizeStream(json);
		if (sizeError is not null) return sizeError;
		var ttlError = ValidatePerItemTtlStream(jObj);
		if (ttlError is not null) return ttlError;

		var itemId = jObj["id"]?.ToString();
		if (itemId is null)
			return CreateResponseMessage(HttpStatusCode.BadRequest);
		var pkMismatch = ValidatePartitionKeyConsistencyStream(partitionKey, jObj);
		if (pkMismatch is not null) return pkMismatch;
		var pk = ExtractPartitionKeyValue(partitionKey, jObj);
		var key = ItemKey(itemId, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			EvictIfExpired(key);

			// Note: If-Match is documented as "applicable only on PUT and DELETE" in the REST API.
			// Upsert is POST-based, so If-Match is not evaluated for the insert path.
			// If the item exists, CheckIfMatchStream will evaluate it on the update path.
			if (!CheckIfMatchStream(requestOptions, key))
			{
				return CreateResponseMessage(HttpStatusCode.PreconditionFailed);
			}
			bool existed;
			string etag;
			string enrichedJson;
			string previousJson = null;
			string previousEtag = null;
			DateTimeOffset? previousTimestamp = null;
			if (HasUniqueKeys)
			{
				lock (_uniqueKeyWriteLock)
				{
					if (!ValidateUniqueKeysStream(jObj, pk, excludeItemId: itemId))
						return CreateResponseMessage(HttpStatusCode.Conflict);
					existed = _items.TryGetValue(key, out previousJson);
					if (existed) { previousEtag = _etags.GetValueOrDefault(key); previousTimestamp = _timestamps.GetValueOrDefault(key); }
					etag = GenerateETag();
					_etags[key] = etag;
					_timestamps[key] = DateTimeOffset.UtcNow;
					enrichedJson = EnrichWithSystemProperties(json, etag, _timestamps[key]);
					_items[key] = enrichedJson;
				}
			}
			else
			{
				if (!ValidateUniqueKeysStream(jObj, pk, excludeItemId: itemId))
					return CreateResponseMessage(HttpStatusCode.Conflict);
				existed = _items.TryGetValue(key, out previousJson);
				if (existed) { previousEtag = _etags.GetValueOrDefault(key); previousTimestamp = _timestamps.GetValueOrDefault(key); }
				etag = GenerateETag();
				_etags[key] = etag;
				_timestamps[key] = DateTimeOffset.UtcNow;
				enrichedJson = EnrichWithSystemProperties(json, etag, _timestamps[key]);
				_items[key] = enrichedJson;
			}

			TrackBatchWrite(key);
			if (!existed) { lock (_insertionOrderLock) { _insertionOrder.Add(key); } }
			try
			{
				ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(enrichedJson), "Upsert");
				RecordChangeFeed(itemId, pk, _items[key]);
			}
			catch
			{
				if (existed && previousJson is not null)
				{
					_items[key] = previousJson;
					if (previousEtag is not null) _etags[key] = previousEtag;
					if (previousTimestamp.HasValue) _timestamps[key] = previousTimestamp.Value;
				}
				else
				{
					_items.TryRemove(key, out _);
					_etags.TryRemove(key, out _);
					_timestamps.TryRemove(key, out _);
					lock (_insertionOrderLock) { _insertionOrder.Remove(key); }
				}
				throw;
			}

			return CreateResponseMessage(existed ? HttpStatusCode.OK : HttpStatusCode.Created,
				requestOptions?.EnableContentResponseOnWrite == false ? null : enrichedJson, etag);
		}
		finally
		{
			itemLock.Release();
		}
	}

	public override async Task<ResponseMessage> ReplaceItemStreamAsync(
		Stream streamPayload, string id, PartitionKey partitionKey,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var json = ReadStream(streamPayload);
		var sizeError = ValidateDocumentSizeStream(json);
		if (sizeError is not null) return sizeError;
		var pk = PartitionKeyToString(partitionKey);
		var key = ItemKey(id, pk);

		JObject jObj;
		try { jObj = JsonParseHelpers.ParseJson(json); }
		catch (Newtonsoft.Json.JsonReaderException)
		{ return CreateResponseMessage(HttpStatusCode.BadRequest); }

		var pkMismatch = ValidatePartitionKeyConsistencyStream(partitionKey, jObj);
		if (pkMismatch is not null) return pkMismatch;

		// Validate body id matches parameter id.
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
		//   "The Container.ReplaceItemAsync<> method requires the provided string for the id
		//    parameter to match the unique identifier of the item parameter."
		var bodyId = jObj["id"]?.ToString();
		if (bodyId is not null && bodyId != id)
		{
			return CreateResponseMessage(HttpStatusCode.BadRequest);
		}

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			if (!_items.TryGetValue(key, out var previousJson) || IsExpired(key))
			{
				EvictIfExpired(key);
				return CreateResponseMessage(HttpStatusCode.NotFound);
			}

			if (!CheckIfMatchStream(requestOptions, key))
			{
				return CreateResponseMessage(HttpStatusCode.PreconditionFailed);
			}

			var previousEtag = _etags.GetValueOrDefault(key);
			var previousTimestamp = _timestamps.GetValueOrDefault(key);

			jObj = ExecutePreTriggers(requestOptions, jObj, "Replace");
			json = jObj.ToString(Newtonsoft.Json.Formatting.None);
			sizeError = ValidateDocumentSizeStream(json);
			if (sizeError is not null) return sizeError;
			var ttlError = ValidatePerItemTtlStream(jObj);
			if (ttlError is not null) return ttlError;

			string etag;
			string enrichedJson;
			if (HasUniqueKeys)
			{
				lock (_uniqueKeyWriteLock)
				{
					if (!ValidateUniqueKeysStream(jObj, pk, excludeItemId: id))
						return CreateResponseMessage(HttpStatusCode.Conflict);
					etag = GenerateETag();
					_etags[key] = etag;
					_timestamps[key] = DateTimeOffset.UtcNow;
					enrichedJson = EnrichWithSystemProperties(json, etag, _timestamps[key]);
					_items[key] = enrichedJson;
				}
			}
			else
			{
				if (!ValidateUniqueKeysStream(jObj, pk, excludeItemId: id))
					return CreateResponseMessage(HttpStatusCode.Conflict);
				etag = GenerateETag();
				_etags[key] = etag;
				_timestamps[key] = DateTimeOffset.UtcNow;
				enrichedJson = EnrichWithSystemProperties(json, etag, _timestamps[key]);
				_items[key] = enrichedJson;
			}

			TrackBatchWrite(key);
			try
			{
				ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(enrichedJson), "Replace");
				RecordChangeFeed(id, pk, _items[key]);
			}
			catch
			{
				_items[key] = previousJson;
				if (previousEtag is not null) _etags[key] = previousEtag;
				_timestamps[key] = previousTimestamp;
				throw;
			}

			return CreateResponseMessage(HttpStatusCode.OK,
				requestOptions?.EnableContentResponseOnWrite == false ? null : enrichedJson, etag);
		}
		finally
		{
			itemLock.Release();
		}
	}

	public override async Task<ResponseMessage> DeleteItemStreamAsync(
		string id, PartitionKey partitionKey,
		ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		var pk = PartitionKeyToString(partitionKey);
		var key = ItemKey(id, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			if (!_items.TryGetValue(key, out var existingJson) || IsExpired(key))
			{
				EvictIfExpired(key);
				return CreateResponseMessage(HttpStatusCode.NotFound);
			}

			if (!CheckIfMatchStream(requestOptions, key))
			{
				return CreateResponseMessage(HttpStatusCode.PreconditionFailed);
			}

			ExecutePreTriggers(requestOptions, JsonParseHelpers.ParseJson(existingJson), "Delete");

			var previousEtag = _etags.TryGetValue(key, out var e) ? e : null;
			var previousTimestamp = _timestamps.TryGetValue(key, out var ts) ? ts : (DateTimeOffset?)null;

			_items.TryRemove(key, out _);
			_etags.TryRemove(key, out _);
			_timestamps.TryRemove(key, out _);
			lock (_insertionOrderLock) { _insertionOrder.Remove(key); }

			TrackBatchWrite(key);
			try
			{
				ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(existingJson), "Delete");
				RecordDeleteTombstone(id, pk, partitionKey);
			}
			catch
			{
				_items[key] = existingJson;
				if (previousEtag is not null) _etags[key] = previousEtag;
				if (previousTimestamp.HasValue) _timestamps[key] = previousTimestamp.Value;
				lock (_insertionOrderLock) { _insertionOrder.Add(key); }
				throw;
			}

			return CreateResponseMessage(HttpStatusCode.NoContent);
		}
		finally
		{
			itemLock.Release();
		}
	}

	public override async Task<ResponseMessage> PatchItemStreamAsync(
		string id, PartitionKey partitionKey,
		IReadOnlyList<PatchOperation> patchOperations,
		PatchItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();

		if (patchOperations is null || patchOperations.Count == 0)
		{
			return CreateResponseMessage(HttpStatusCode.BadRequest);
		}

		if (patchOperations.Count > 10)
		{
			return CreateResponseMessage(HttpStatusCode.BadRequest);
		}

		try
		{
			ValidatePatchPaths(patchOperations);
		}
		catch (CosmosException)
		{
			return CreateResponseMessage(HttpStatusCode.BadRequest);
		}

		var pk = PartitionKeyToString(partitionKey);
		var key = ItemKey(id, pk);

		var itemLock = _itemLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
		await itemLock.WaitAsync(cancellationToken);
		try
		{
			return PatchItemStreamCore(id, pk, key, patchOperations, requestOptions);
		}
		finally
		{
			itemLock.Release();
		}
	}

	private ResponseMessage PatchItemStreamCore(
		string id, string pk, (string Id, string PartitionKey) key,
		IReadOnlyList<PatchOperation> patchOperations,
		PatchItemRequestOptions requestOptions)
	{
		if (!_items.TryGetValue(key, out var existingJson) || IsExpired(key))
		{
			EvictIfExpired(key);
			return CreateResponseMessage(HttpStatusCode.NotFound);
		}

		if (!CheckIfMatchStream(requestOptions, key))
		{
			return CreateResponseMessage(HttpStatusCode.PreconditionFailed);
		}

		var jObj = JsonParseHelpers.ParseJson(existingJson);

		if (requestOptions?.FilterPredicate is not null)
		{
			var predicateSql = $"SELECT * {requestOptions.FilterPredicate}";
			var predicateParsed = CosmosSqlParser.Parse(predicateSql);
			if (predicateParsed.Where is not null)
			{
				var matches = EvaluateWhereExpression(predicateParsed.Where, jObj, predicateParsed.FromAlias,
					new Dictionary<string, object>(), null, treatUndefinedAsNull: true);
				if (!matches)
				{
					return CreateResponseMessage(HttpStatusCode.PreconditionFailed);
				}
			}
		}

		jObj = ExecutePreTriggers(requestOptions, jObj, "Replace");

		ApplyPatchOperations(jObj, patchOperations);
		var updatedJson = jObj.ToString(Formatting.None);
		var sizeError = ValidateDocumentSizeStream(updatedJson);
		if (sizeError is not null) return sizeError;
		var ttlError = ValidatePerItemTtlStream(jObj);
		if (ttlError is not null) return ttlError;

		var previousEtag = _etags.GetValueOrDefault(key);
		var previousTimestamp = _timestamps.GetValueOrDefault(key);

		string etag;
		if (HasUniqueKeys)
		{
			lock (_uniqueKeyWriteLock)
			{
				if (!ValidateUniqueKeysStream(jObj, pk, excludeItemId: id))
					return CreateResponseMessage(HttpStatusCode.Conflict);
				etag = GenerateETag();
				_etags[key] = etag;
				_timestamps[key] = DateTimeOffset.UtcNow;
				var enriched = EnrichWithSystemProperties(updatedJson, etag, _timestamps[key]);
				_items[key] = enriched;
			}
		}
		else
		{
			etag = GenerateETag();
			_etags[key] = etag;
			_timestamps[key] = DateTimeOffset.UtcNow;
			var enriched = EnrichWithSystemProperties(updatedJson, etag, _timestamps[key]);
			_items[key] = enriched;
		}
		var enrichedJson = _items[key];

		TrackBatchWrite(key);
		try
		{
			ExecutePostTriggers(requestOptions, JsonParseHelpers.ParseJson(enrichedJson), "Replace");
			RecordChangeFeed(id, pk, _items[key]);
		}
		catch
		{
			_items[key] = existingJson;
			if (previousEtag is not null) _etags[key] = previousEtag;
			_timestamps[key] = previousTimestamp;
			throw;
		}

		return CreateResponseMessage(HttpStatusCode.OK,
			requestOptions?.EnableContentResponseOnWrite == false ? null : enrichedJson, etag);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  ReadMany
	// ═══════════════════════════════════════════════════════════════════════════

	public override Task<FeedResponse<T>> ReadManyItemsAsync<T>(
		IReadOnlyList<(string id, PartitionKey partitionKey)> items,
		ReadManyRequestOptions readManyRequestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		ArgumentNullException.ThrowIfNull(items);
		var results = new List<T>();
		var etagParts = new List<string>();
		foreach (var (itemId, pk) in items)
		{
			var pkStr = PartitionKeyToString(pk);
			var key = ItemKey(itemId, pkStr);
			if (_items.TryGetValue(key, out var json) && !IsExpired(key))
			{
				var jObj = JsonParseHelpers.ParseJson(json);
				var itemEtag = jObj["_etag"]?.ToString();
				if (itemEtag != null) etagParts.Add(itemEtag);
				var deserialized = JsonConvert.DeserializeObject<T>(json, JsonSettings);
				if (deserialized is not null)
				{
					results.Add(deserialized);
				}
			}
		}
		var compositeEtag = etagParts.Count > 0 ? $"\"{string.Join(",", etagParts)}\"" : null;
		if (readManyRequestOptions?.IfNoneMatchEtag != null && compositeEtag == readManyRequestOptions.IfNoneMatchEtag)
		{
			return Task.FromResult<FeedResponse<T>>(new InMemoryFeedResponse<T>(
				Array.Empty<T>(), HttpStatusCode.NotModified, compositeEtag));
		}
		return Task.FromResult<FeedResponse<T>>(new InMemoryFeedResponse<T>(results, etag: compositeEtag));
	}

	public override Task<ResponseMessage> ReadManyItemsStreamAsync(
		IReadOnlyList<(string id, PartitionKey partitionKey)> items,
		ReadManyRequestOptions readManyRequestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		ArgumentNullException.ThrowIfNull(items);
		var results = new JArray();
		var etagParts = new List<string>();
		foreach (var (itemId, pk) in items)
		{
			var pkStr = PartitionKeyToString(pk);
			var key = ItemKey(itemId, pkStr);
			if (_items.TryGetValue(key, out var json) && !IsExpired(key))
			{
				var jObj = JsonParseHelpers.ParseJson(json);
				var itemEtag = jObj["_etag"]?.ToString();
				if (itemEtag != null) etagParts.Add(itemEtag);
				results.Add(jObj);
			}
		}
		var compositeEtag = etagParts.Count > 0 ? $"\"{string.Join(",", etagParts)}\"" : null;
		if (readManyRequestOptions?.IfNoneMatchEtag != null && compositeEtag == readManyRequestOptions.IfNoneMatchEtag)
		{
			return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotModified, etag: compositeEtag));
		}
		var envelope = new JObject { ["_rid"] = "", ["Documents"] = results, ["_count"] = results.Count };
		return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, envelope.ToString(Formatting.None), etag: compositeEtag));
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Query — Typed FeedIterator
	// ═══════════════════════════════════════════════════════════════════════════

	private static List<string> ExecuteQuerySafe(Func<List<string>> queryFunc)
	{
		try
		{
			return queryFunc();
		}
		catch (CosmosException) { throw; }
		catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException or FormatException)
		{
			throw InMemoryCosmosException.Create(
				ex.Message, HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
		}
	}

	public override FeedIterator<T> GetItemQueryIterator<T>(
		QueryDefinition queryDefinition, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		ValidateMaxItemCount(requestOptions);
		var parameters = ExtractQueryParameters(queryDefinition);
		var filtered = ExecuteQuerySafe(() => FilterItemsByQuery(queryDefinition.QueryText, parameters, requestOptions));
		var items = filtered.Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings)).ToList();
		var initialOffset = ParseContinuationToken(continuationToken);
		return new InMemoryFeedIterator<T>(items, requestOptions?.MaxItemCount, initialOffset)
		{
			PopulateIndexMetrics = requestOptions?.PopulateIndexMetrics ?? false,
			GuaranteeFirstPage = true
		};
	}

	public override FeedIterator<T> GetItemQueryIterator<T>(
		string queryText = null, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		ValidateMaxItemCount(requestOptions);
		List<T> items;
		if (string.IsNullOrEmpty(queryText))
		{
			items = GetAllItemsForPartition(requestOptions)
				.Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings)).ToList();
		}
		else
		{
			var filtered = ExecuteQuerySafe(() => FilterItemsByQuery(queryText, new Dictionary<string, object>(), requestOptions));
			items = filtered.Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings)).ToList();
		}
		var initialOffset = ParseContinuationToken(continuationToken);
		return new InMemoryFeedIterator<T>(items, requestOptions?.MaxItemCount, initialOffset)
		{
			PopulateIndexMetrics = requestOptions?.PopulateIndexMetrics ?? false,
			GuaranteeFirstPage = true
		};
	}

	public override FeedIterator<T> GetItemQueryIterator<T>(
		FeedRange feedRange, QueryDefinition queryDefinition, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		ValidateMaxItemCount(requestOptions);
		var parameters = ExtractQueryParameters(queryDefinition);
		var preFiltered = FilterByFeedRange(GetAllItemsForPartition(requestOptions).ToList(), feedRange);
		var filtered = ExecuteQuerySafe(() => FilterItemsByQuery(queryDefinition.QueryText, parameters, requestOptions, preFiltered));
		var items = filtered.Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings)).ToList();
		var initialOffset = ParseContinuationToken(continuationToken);
		return new InMemoryFeedIterator<T>(items, requestOptions?.MaxItemCount, initialOffset)
		{
			PopulateIndexMetrics = requestOptions?.PopulateIndexMetrics ?? false,
			GuaranteeFirstPage = true
		};
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Query — Stream FeedIterator
	// ═══════════════════════════════════════════════════════════════════════════

	public override FeedIterator GetItemQueryStreamIterator(
		QueryDefinition queryDefinition, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		var parameters = ExtractQueryParameters(queryDefinition);
		var filtered = ExecuteQuerySafe(() => FilterItemsByQuery(queryDefinition.QueryText, parameters, requestOptions));
		return CreateStreamFeedIterator(filtered, ParseContinuationToken(continuationToken), requestOptions?.MaxItemCount);
	}

	public override FeedIterator GetItemQueryStreamIterator(
		string queryText = null, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		var offset = ParseContinuationToken(continuationToken);
		if (string.IsNullOrEmpty(queryText))
		{
			return CreateStreamFeedIterator(GetAllItemsForPartition(requestOptions).ToList(), offset, requestOptions?.MaxItemCount);
		}

		return CreateStreamFeedIterator(ExecuteQuerySafe(() => FilterItemsByQuery(queryText, new Dictionary<string, object>(), requestOptions)), offset, requestOptions?.MaxItemCount);
	}

	public override FeedIterator GetItemQueryStreamIterator(
		FeedRange feedRange, QueryDefinition queryDefinition, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		var parameters = ExtractQueryParameters(queryDefinition);
		var preFiltered = FilterByFeedRange(GetAllItemsForPartition(requestOptions).ToList(), feedRange);
		var filtered = ExecuteQuerySafe(() => FilterItemsByQuery(queryDefinition.QueryText, parameters, requestOptions, preFiltered));
		return CreateStreamFeedIterator(filtered, ParseContinuationToken(continuationToken), requestOptions?.MaxItemCount);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  LINQ
	// ═══════════════════════════════════════════════════════════════════════════

	public override IOrderedQueryable<T> GetItemLinqQueryable<T>(
		bool allowSynchronousQueryExecution = false, string continuationToken = null,
		QueryRequestOptions requestOptions = null, CosmosLinqSerializerOptions linqSerializerOptions = null)
	{
		InMemoryFeedIteratorSetup.LastMaxItemCount = requestOptions?.MaxItemCount;
		return GetAllItemsForPartition(requestOptions)
			.Select(json => JsonConvert.DeserializeObject<T>(json, JsonSettings))
			.AsQueryable()
			.OrderBy(item => 0);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  TransactionalBatch
	// ═══════════════════════════════════════════════════════════════════════════

	public override TransactionalBatch CreateTransactionalBatch(PartitionKey partitionKey)
		=> new InMemoryTransactionalBatch(this, partitionKey);

	// ═══════════════════════════════════════════════════════════════════════════
	//  Change Feed — Iterators
	// ═══════════════════════════════════════════════════════════════════════════

	public override FeedIterator<T> GetChangeFeedIterator<T>(
		ChangeFeedStartFrom changeFeedStartFrom, ChangeFeedMode changeFeedMode,
		ChangeFeedRequestOptions changeFeedRequestOptions = null)
	{
		var pageSize = changeFeedRequestOptions?.PageSizeHint;
		var typeName = changeFeedStartFrom.GetType().Name;
		var feedRange = ExtractFeedRangeFromStartFrom(changeFeedStartFrom);

		// "Now" and "Time" start types use lazy evaluation so items added
		// after iterator creation are included when ReadNextAsync is called.
		if (typeName.Contains("Now", StringComparison.OrdinalIgnoreCase) ||
			typeName.Contains("Time", StringComparison.OrdinalIgnoreCase))
		{
			// Capture the checkpoint (current feed length) at creation time for "Now",
			// or extract the timestamp for "Time" filtering.
			DateTimeOffset? capturedTimestamp = null;
			if (typeName.Contains("Now", StringComparison.OrdinalIgnoreCase))
			{
				lock (_changeFeedLock)
				{
					capturedTimestamp = _changeFeed.Count > 0
						? _changeFeed[^1].Timestamp
						: DateTimeOffset.UtcNow;
				}
			}
			else
			{
				var startTime = ExtractStartTime(changeFeedStartFrom);
				if (startTime.HasValue)
					capturedTimestamp = new DateTimeOffset(startTime.Value, TimeSpan.Zero);
			}

			var isNowStart = typeName.Contains("Now", StringComparison.OrdinalIgnoreCase);
			var ts = capturedTimestamp;
			var capturedRange = feedRange;
			return new InMemoryFeedIterator<T>(() =>
			{
				lock (_changeFeedLock)
				{
					var entries = ts.HasValue
						? _changeFeed.Where(entry => isNowStart
							? entry.Timestamp > ts.Value
							: entry.Timestamp >= ts.Value).ToList()
						: _changeFeed.ToList();

					entries = FilterChangeFeedEntriesByFeedRange(entries, capturedRange);

					if (changeFeedMode == ChangeFeedMode.Incremental)
					{
						entries = entries
							.GroupBy(entry => (entry.Id, entry.PartitionKey))
							.Select(group => group.Last())
							.Where(entry => !entry.IsDelete)
							.ToList();
					}

					return entries
						.Select(entry => JsonConvert.DeserializeObject<T>(entry.Json, JsonSettings))
						.ToList();
				}
			}, pageSize);
		}

		// "Beginning" and other start types — eager evaluation is fine
		List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> eagerEntries;
		lock (_changeFeedLock)
		{
			eagerEntries = _changeFeed.ToList();
		}

		eagerEntries = FilterChangeFeedEntriesByFeedRange(eagerEntries, feedRange);

		if (changeFeedMode == ChangeFeedMode.Incremental)
		{
			eagerEntries = eagerEntries
				.GroupBy(entry => (entry.Id, entry.PartitionKey))
				.Select(group => group.Last())
				.Where(entry => !entry.IsDelete)
				.ToList();
		}

		var items = eagerEntries
			.Select(entry => JsonConvert.DeserializeObject<T>(entry.Json, JsonSettings))
			.ToList();
		return new InMemoryFeedIterator<T>(items, pageSize);
	}

	/// <summary>
	/// Returns a change feed iterator that reads changes from the given checkpoint position.
	/// Obtain a checkpoint via <see cref="GetChangeFeedCheckpoint"/>.
	/// </summary>
	public FeedIterator<T> GetChangeFeedIterator<T>(long checkpoint)
	{
		List<T> items;
		lock (_changeFeedLock)
		{
			items = _changeFeed
				.Skip((int)checkpoint)
				.Select(entry => JsonConvert.DeserializeObject<T>(entry.Json, JsonSettings))
				.ToList();
		}
		return new InMemoryFeedIterator<T>(items);
	}

	public override FeedIterator GetChangeFeedStreamIterator(
		ChangeFeedStartFrom changeFeedStartFrom, ChangeFeedMode changeFeedMode,
		ChangeFeedRequestOptions changeFeedRequestOptions = null)
	{
		var feedRange = ExtractFeedRangeFromStartFrom(changeFeedStartFrom);
		var pageSizeHint = changeFeedRequestOptions?.PageSizeHint;
		var creationTime = DateTimeOffset.UtcNow;
		return CreateStreamFeedIteratorFromFactory(() =>
		{
			List<string> items;
			lock (_changeFeedLock)
			{
				var entries = FilterChangeFeedByStartFrom(changeFeedStartFrom, creationTime);
				entries = FilterChangeFeedEntriesByFeedRange(entries, feedRange);

				if (changeFeedMode == ChangeFeedMode.Incremental)
				{
					entries = entries
						.GroupBy(entry => (entry.Id, entry.PartitionKey))
						.Select(group => group.Last())
						.Where(entry => !entry.IsDelete)
						.ToList();
				}

				items = entries.Select(entry => entry.Json).ToList();
			}
			return items;
		}, 0, pageSizeHint);
	}

	private List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> FilterChangeFeedByStartFrom(
		ChangeFeedStartFrom startFrom, DateTimeOffset? creationTime = null)
	{
		var typeName = startFrom.GetType().Name;

		if (typeName.Contains("Now", StringComparison.OrdinalIgnoreCase))
		{
			var now = creationTime ?? DateTimeOffset.UtcNow;
			return _changeFeed.Where(entry => entry.Timestamp > now).ToList();
		}

		if (typeName.Contains("Time", StringComparison.OrdinalIgnoreCase))
		{
			var startTime = ExtractStartTime(startFrom);
			if (startTime.HasValue)
			{
				var dto = new DateTimeOffset(startTime.Value, TimeSpan.Zero);
				return _changeFeed.Where(entry => entry.Timestamp >= dto).ToList();
			}
		}

		return _changeFeed.ToList();
	}

	private static DateTime? ExtractStartTime(ChangeFeedStartFrom startFrom)
	{
		foreach (var prop in startFrom.GetType().GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
		{
			if (prop.PropertyType == typeof(DateTime) && prop.GetValue(startFrom) is DateTime dt)
			{
				return dt;
			}
		}

		foreach (var field in startFrom.GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Instance))
		{
			if (field.FieldType == typeof(DateTime) && field.GetValue(startFrom) is DateTime dt)
			{
				return dt;
			}
		}

		return null;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  FeedRange Filtering Helpers
	// ═══════════════════════════════════════════════════════════════════════════

	private static FeedRange ExtractFeedRangeFromStartFrom(ChangeFeedStartFrom startFrom)
	{
		// ChangeFeedStartFrom subtypes (Beginning, Now, Time, ContinuationAndFeedRange)
		// store the FeedRange in an internal property. Use reflection to extract it.
		foreach (var prop in startFrom.GetType().GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
		{
			if (typeof(FeedRange).IsAssignableFrom(prop.PropertyType) && prop.GetValue(startFrom) is FeedRange fr)
				return fr;
		}

		foreach (var field in startFrom.GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Instance))
		{
			if (typeof(FeedRange).IsAssignableFrom(field.FieldType) && field.GetValue(startFrom) is FeedRange fr)
				return fr;
		}

		return null;
	}

	private List<string> FilterByFeedRange(List<string> items, FeedRange feedRange)
	{
		var pkValue = TryExtractPartitionKeyFromFeedRange(feedRange);
		if (pkValue != null)
		{
			return items.Where(json =>
			{
				var itemPk = ExtractPartitionKeyValueFromJson(json);
				return string.Equals(itemPk, pkValue, StringComparison.Ordinal);
			}).ToList();
		}

		var (min, max) = ParseFeedRangeBoundaries(feedRange);
		if (min == null) return items;

		return items.Where(json =>
		{
			var itemPk = ExtractPartitionKeyValueFromJson(json);
			var hash = PartitionKeyHash.MurmurHash3(itemPk);
			return IsHashInRange(hash, min.Value, max.Value);
		}).ToList();
	}

	private List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)>
		FilterChangeFeedEntriesByFeedRange(
			List<(DateTimeOffset Timestamp, string Id, string PartitionKey, string Json, bool IsDelete)> entries,
			FeedRange feedRange)
	{
		var pkValue = TryExtractPartitionKeyFromFeedRange(feedRange);
		if (pkValue != null)
		{
			return entries.Where(entry =>
				string.Equals(entry.PartitionKey ?? "", pkValue, StringComparison.Ordinal)).ToList();
		}

		var (min, max) = ParseFeedRangeBoundaries(feedRange);
		if (min == null) return entries;

		return entries.Where(entry =>
		{
			var hash = PartitionKeyHash.MurmurHash3(entry.PartitionKey ?? "");
			return IsHashInRange(hash, min.Value, max.Value);
		}).ToList();
	}

	private static (uint? Min, uint? Max) ParseFeedRangeBoundaries(FeedRange feedRange)
	{
		if (feedRange == null) return (null, null);

		try
		{
			var json = feedRange.ToJsonString();
			var obj = JObject.Parse(json);
			var rangeObj = obj["Range"];
			if (rangeObj == null) return (null, null);

			var minStr = rangeObj["min"]?.ToString() ?? "";
			var maxStr = rangeObj["max"]?.ToString() ?? "";

			var minVal = string.IsNullOrEmpty(minStr) ? 0u : Convert.ToUInt32(minStr, 16);
			var maxVal = string.Equals(maxStr, "FF", StringComparison.OrdinalIgnoreCase)
				? uint.MaxValue
				: Convert.ToUInt32(maxStr, 16);

			return (minVal, maxVal);
		}
		catch
		{
			return (null, null);
		}
	}

	private static string TryExtractPartitionKeyFromFeedRange(FeedRange feedRange)
	{
		if (feedRange == null) return null;
		try
		{
			var json = feedRange.ToJsonString();
			var obj = JObject.Parse(json);
			var pkToken = obj["PK"];
			if (pkToken == null) return null;

			// PK value is a string containing a JSON array, e.g. "[\"pk-5\"]" or "[42]"
			var pkStr = pkToken.ToString();
			var pkArray = JArray.Parse(pkStr);
			if (pkArray.Count == 0) return null;
			// For multi-component (hierarchical) partition keys, join all parts with "|"
			// to match the format used by ExtractPartitionKeyValueCore.
			if (pkArray.Count == 1)
				return JTokenToTypedKey(pkArray[0]) ?? "";
			var parts = pkArray.Select(t => JTokenToTypedKey(t) ?? "").ToList();
			return string.Join("|", parts);
		}
		catch
		{
			return null;
		}
	}

	private static bool IsHashInRange(uint hash, uint min, uint max)
	{
		return hash >= min && (max == uint.MaxValue || hash < max);
	}

	private string ExtractPartitionKeyValueFromJson(string json)
	{
		JToken token;
		try { token = JsonConvert.DeserializeObject<JToken>(json, JsonSettings); }
		catch { return ""; }

		if (token is not JObject jObj) return "";

		if (PartitionKeyPaths is { Count: > 0 })
		{
			var parts = PartitionKeyPaths.Select(path =>
			{
				var t = jObj.SelectToken(path.TrimStart('/'));
				return t is not null ? JTokenToTypedKey(t) : null;
			}).ToList();
			if (parts.Count == 1) return parts[0] ?? "";
			return string.Join("|", parts.Select(p => p ?? ""));
		}

		return "";
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Change Feed — Processor Builders
	// ═══════════════════════════════════════════════════════════════════════════

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder<T>(
		string processorName, Container.ChangesHandler<T> onChangesDelegate)
		=> ChangeFeedProcessorBuilderFactory.Create(processorName,
			new InMemoryChangeFeedProcessor<T>(this, onChangesDelegate));

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder<T>(
		string processorName, Container.ChangeFeedHandler<T> onChangesDelegate)
		=> ChangeFeedProcessorBuilderFactory.Create(processorName,
			new InMemoryChangeFeedProcessor<T>(this, onChangesDelegate));

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder(
		string processorName, Container.ChangeFeedStreamHandler onChangesDelegate)
		=> ChangeFeedProcessorBuilderFactory.Create(processorName,
			new InMemoryChangeFeedStreamProcessor(this, onChangesDelegate));

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilderWithManualCheckpoint<T>(
		string processorName, Container.ChangeFeedHandlerWithManualCheckpoint<T> onChangesDelegate)
		=> ChangeFeedProcessorBuilderFactory.Create(processorName,
			new InMemoryManualCheckpointChangeFeedProcessor<T>(this, onChangesDelegate));

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilderWithManualCheckpoint(
		string processorName, Container.ChangeFeedStreamHandlerWithManualCheckpoint onChangesDelegate)
		=> ChangeFeedProcessorBuilderFactory.Create(processorName,
			new InMemoryManualCheckpointStreamChangeFeedProcessor(this, onChangesDelegate));

	public override ChangeFeedEstimator GetChangeFeedEstimator(
		string processorName, Container leaseContainer)
		=> Substitute.For<ChangeFeedEstimator>();

	public override ChangeFeedProcessorBuilder GetChangeFeedEstimatorBuilder(
		string processorName, Container.ChangesEstimationHandler estimationDelegate,
		TimeSpan? estimationPeriod = null)
		=> ChangeFeedProcessorBuilderFactory.Create(processorName, new NoOpChangeFeedProcessor());

	// ═══════════════════════════════════════════════════════════════════════════
	//  Feed Ranges
	// ═══════════════════════════════════════════════════════════════════════════

	public override Task<IReadOnlyList<FeedRange>> GetFeedRangesAsync(CancellationToken cancellationToken = default)
	{
		var count = Math.Max(1, FeedRangeCount);
		var ranges = new List<FeedRange>(count);
		var step = 0x1_0000_0000L / count;

		for (var i = 0; i < count; i++)
		{
			var min = PartitionKeyHash.RangeBoundaryToHex(i * step);
			var max = (i == count - 1) ? "FF" : PartitionKeyHash.RangeBoundaryToHex((i + 1) * step);
			ranges.Add(FeedRange.FromJsonString($"{{\"Range\":{{\"min\":\"{min}\",\"max\":\"{max}\"}}}}"));
		}

		return Task.FromResult<IReadOnlyList<FeedRange>>(ranges);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Container management
	// ═══════════════════════════════════════════════════════════════════════════

	public override Task<ContainerResponse> ReadContainerAsync(
		ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		if (_isDeleted || (!ExplicitlyCreated && _items.IsEmpty))
		{
			throw InMemoryCosmosException.Create($"Container '{Id}' not found.", HttpStatusCode.NotFound, 1003, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}
		_containerProperties.IndexingPolicy = IndexingPolicy;
		_containerProperties.DefaultTimeToLive = DefaultTimeToLive;
		var r = Substitute.For<ContainerResponse>();
		r.StatusCode.Returns(HttpStatusCode.OK);
		r.Resource.Returns(_containerProperties);
		r.Container.Returns(this);
		return Task.FromResult(r);
	}

	public override Task<ResponseMessage> ReadContainerStreamAsync(
		ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		if (_isDeleted || (!ExplicitlyCreated && _items.IsEmpty))
		{
			return Task.FromResult(CreateResponseMessage(HttpStatusCode.NotFound));
		}
		_containerProperties.IndexingPolicy = IndexingPolicy;
		_containerProperties.DefaultTimeToLive = DefaultTimeToLive;
		return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, JsonConvert.SerializeObject(_containerProperties, JsonSettings)));
	}

	public override Task<ContainerResponse> ReplaceContainerAsync(
		ContainerProperties containerProperties, ContainerRequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		if (containerProperties.Id != Id)
			throw InMemoryCosmosException.Create($"Container id '{containerProperties.Id}' does not match the existing container id '{Id}'.", HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		ValidateContainerReplace(containerProperties);
		ValidateComputedProperties(containerProperties);
		// Preserve UniqueKeyPolicy from the existing properties if the replacement doesn't include it
		if (_containerProperties.UniqueKeyPolicy?.UniqueKeys?.Count > 0 &&
			(containerProperties.UniqueKeyPolicy is null || containerProperties.UniqueKeyPolicy.UniqueKeys.Count == 0))
		{
			containerProperties.UniqueKeyPolicy = _containerProperties.UniqueKeyPolicy;
		}
		_containerProperties = containerProperties;
		_parsedComputedProperties = null;
		DefaultTimeToLive = containerProperties.DefaultTimeToLive;
		if (containerProperties.IndexingPolicy is not null)
			IndexingPolicy = containerProperties.IndexingPolicy;
		var r = Substitute.For<ContainerResponse>();
		r.StatusCode.Returns(HttpStatusCode.OK);
		r.Resource.Returns(containerProperties);
		r.Container.Returns(this);
		return Task.FromResult(r);
	}

	public override Task<ResponseMessage> ReplaceContainerStreamAsync(
		ContainerProperties containerProperties, ContainerRequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		if (containerProperties.Id != Id)
			return Task.FromResult(CreateResponseMessage(HttpStatusCode.BadRequest));
		try
		{
			ValidateContainerReplace(containerProperties);
			ValidateComputedProperties(containerProperties);
		}
		catch (CosmosException)
		{
			return Task.FromResult(CreateResponseMessage(HttpStatusCode.BadRequest));
		}
		// Preserve UniqueKeyPolicy from the existing properties if the replacement doesn't include it
		if (_containerProperties.UniqueKeyPolicy?.UniqueKeys?.Count > 0 &&
			(containerProperties.UniqueKeyPolicy is null || containerProperties.UniqueKeyPolicy.UniqueKeys.Count == 0))
		{
			containerProperties.UniqueKeyPolicy = _containerProperties.UniqueKeyPolicy;
		}
		_containerProperties = containerProperties;
		_parsedComputedProperties = null;
		DefaultTimeToLive = containerProperties.DefaultTimeToLive;
		if (containerProperties.IndexingPolicy is not null)
			IndexingPolicy = containerProperties.IndexingPolicy;
		return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK, JsonConvert.SerializeObject(containerProperties, JsonSettings)));
	}

	public override Task<ContainerResponse> DeleteContainerAsync(
		ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		_isDeleted = true;
		_items.Clear();
		_etags.Clear();
		_timestamps.Clear();
		lock (_insertionOrderLock) { _insertionOrder.Clear(); }
		lock (_changeFeedLock) { _changeFeed.Clear(); }
		_storedProcedures.Clear();
		_userDefinedFunctions.Clear();
		_udfProperties.Clear();
		_triggers.Clear();
		_storedProcedureProperties.Clear();
		_triggerProperties.Clear();
		OnDeleted?.Invoke();
		var r = Substitute.For<ContainerResponse>();
		r.StatusCode.Returns(HttpStatusCode.NoContent);
		r.Container.Returns(this);
		return Task.FromResult(r);
	}

	public override Task<ResponseMessage> DeleteContainerStreamAsync(
		ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		_isDeleted = true;
		_items.Clear();
		_etags.Clear();
		_timestamps.Clear();
		lock (_insertionOrderLock) { _insertionOrder.Clear(); }
		lock (_changeFeedLock) { _changeFeed.Clear(); }
		_storedProcedures.Clear();
		_userDefinedFunctions.Clear();
		_udfProperties.Clear();
		_triggers.Clear();
		_storedProcedureProperties.Clear();
		_triggerProperties.Clear();
		OnDeleted?.Invoke();
		return Task.FromResult(CreateResponseMessage(HttpStatusCode.NoContent));
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Throughput
	// ═══════════════════════════════════════════════════════════════════════════

	public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = default)
		=> Task.FromResult<int?>(_throughput);

	public override Task<ThroughputResponse> ReadThroughputAsync(
		RequestOptions requestOptions, CancellationToken cancellationToken = default)
	{
		var r = Substitute.For<ThroughputResponse>();
		r.StatusCode.Returns(HttpStatusCode.OK);
		r.Resource.Returns(ThroughputProperties.CreateManualThroughput(_throughput));
		return Task.FromResult(r);
	}

	public override Task<ThroughputResponse> ReplaceThroughputAsync(
		int throughput, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		_throughput = throughput;
		var r = Substitute.For<ThroughputResponse>();
		r.StatusCode.Returns(HttpStatusCode.OK);
		r.Resource.Returns(ThroughputProperties.CreateManualThroughput(throughput));
		return Task.FromResult(r);
	}

	public override Task<ThroughputResponse> ReplaceThroughputAsync(
		ThroughputProperties throughputProperties, RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		if (throughputProperties.Throughput.HasValue)
			_throughput = throughputProperties.Throughput.Value;
		var r = Substitute.For<ThroughputResponse>();
		r.StatusCode.Returns(HttpStatusCode.OK);
		r.Resource.Returns(throughputProperties);
		return Task.FromResult(r);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Delete all items by partition key
	// ═══════════════════════════════════════════════════════════════════════════

	public override Task<ResponseMessage> DeleteAllItemsByPartitionKeyStreamAsync(
		PartitionKey partitionKey, RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		var pk = PartitionKeyToString(partitionKey);
		foreach (var key in _items.Keys.Where(k => k.PartitionKey == pk).ToList())
		{
			_items.TryRemove(key, out _);
			_etags.TryRemove(key, out _);
			_timestamps.TryRemove(key, out _);
			lock (_insertionOrderLock) { _insertionOrder.Remove(key); }
			RecordDeleteTombstone(key.Id, pk, partitionKey);
		}
		return Task.FromResult(CreateResponseMessage(HttpStatusCode.OK));
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Private helpers — Key management & ETag
	// ═══════════════════════════════════════════════════════════════════════════

	private static (string Id, string PartitionKey) ItemKey(string id, string partitionKey) => (id, partitionKey);

	private string ExtractPartitionKeyValue(PartitionKey? partitionKey, JObject jObj)
	{
		var pk = ExtractPartitionKeyValueCore(partitionKey, jObj);
		if (pk is not null && System.Text.Encoding.UTF8.GetByteCount(pk) > 2048)
			throw InMemoryCosmosException.Create("Partition key value exceeds the maximum allowed size of 2KB.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		return pk;
	}

	private string ExtractPartitionKeyValueCore(PartitionKey? partitionKey, JObject jObj)
	{
		if (partitionKey.HasValue)
		{
			if (partitionKey.Value == PartitionKey.Null)
			{
				return null;
			}

			if (partitionKey.Value != PartitionKey.None)
			{
				return PartitionKeyToString(partitionKey.Value);
			}
		}

		if (PartitionKeyPaths is { Count: > 0 })
		{
			if (PartitionKeyPaths.Count > 1)
			{
				// Composite key — preserve null positions as empty strings for consistency
				// with PartitionKeyToString which also uses empty string for null components
				var parts = PartitionKeyPaths.Select(path =>
				{
					var t = jObj.SelectToken(path.TrimStart('/'));
					return t is not null ? JTokenToTypedKey(t) : string.Empty;
				}).ToList();
				return string.Join("|", parts.Select(p => p ?? string.Empty));
			}

			// Single path — check if the field exists
			var token = jObj.SelectToken(PartitionKeyPaths[0].TrimStart('/'));
			if (token is not null)
			{
				// Field exists — if value is null, return null (don't fall back to id)
				return token.Type == JTokenType.Null ? null : JTokenToTypedKey(token);
			}
		}

		return null;
	}

	internal static string JTokenToTypedKey(JToken token)
	{
		return token.Type switch
		{
			JTokenType.String => "S:" + token.Value<string>(),
			JTokenType.Integer or JTokenType.Float => "N:" + token.Value<double>().ToString("R", System.Globalization.CultureInfo.InvariantCulture),
			JTokenType.Boolean => "B:" + (token.Value<bool>() ? "true" : "false"),
			JTokenType.Null => null,
			_ => token.ToString()
		};
	}

	/// <summary>
	/// Converts a type-prefixed internal key back to a JToken for document fields.
	/// </summary>
	private static JToken TypedKeyToJToken(string typedKey)
	{
		if (typedKey is null) return JValue.CreateNull();
		if (typedKey.Length >= 2 && typedKey[1] == ':')
		{
			var value = typedKey.Substring(2);
			return typedKey[0] switch
			{
				'S' => new JValue(value),
				'N' => new JValue(double.Parse(value, System.Globalization.CultureInfo.InvariantCulture)),
				'B' => new JValue(value == "true"),
				_ => new JValue(typedKey)
			};
		}
		return new JValue(typedKey);
	}

	/// <summary>
	/// Converts a type-prefixed internal key back to a typed PartitionKey.
	/// </summary>
	private static PartitionKey TypedKeyToPartitionKey(string typedKey)
	{
		if (typedKey is null) return PartitionKey.Null;
		if (typedKey.Length >= 2 && typedKey[1] == ':')
		{
			var value = typedKey.Substring(2);
			return typedKey[0] switch
			{
				'S' => new PartitionKey(value),
				'N' => new PartitionKey(double.Parse(value, System.Globalization.CultureInfo.InvariantCulture)),
				'B' => new PartitionKey(bool.Parse(value)),
				_ => new PartitionKey(typedKey)
			};
		}
		return new PartitionKey(typedKey);
	}

	private static string PartitionKeyToString(PartitionKey partitionKey)
	{
		if (partitionKey == PartitionKey.None || partitionKey == PartitionKey.Null)
		{
			return null;
		}

		var raw = partitionKey.ToString();
		if (raw.StartsWith("["))
		{
			try
			{
				var arr = JArray.Parse(raw);
				if (arr.Count == 1)
				{
					return JTokenToTypedKey(arr[0]);
				}

				return string.Join("|", arr.Select(JTokenToTypedKey));
			}
			catch { /* fall through */ }
		}
		return raw;
	}

	private static string GenerateETag() => $"\"{Interlocked.Increment(ref _etagCounter):x16}\"";

	private void ValidatePartitionKeyConsistency(PartitionKey? explicitKey, JObject jObj)
	{
		if (!explicitKey.HasValue || explicitKey.Value == PartitionKey.None || explicitKey.Value == PartitionKey.Null)
			return;

		// Only validate single-path partition keys (composite PKs have complex semantics)
		if (PartitionKeyPaths is not { Count: 1 })
			return;

		var pkPath = PartitionKeyPaths[0].TrimStart('/');
		var bodyToken = jObj.SelectToken(pkPath);
		if (bodyToken is null)
			return; // Field not present in body — nothing to validate

		var explicitPk = PartitionKeyToString(explicitKey.Value);
		var bodyPk = JTokenToTypedKey(bodyToken);

		if (bodyPk != explicitPk)
		{
			throw InMemoryCosmosException.Create(
				"PartitionKey extracted from document doesn't match the one specified in the header. " +
				"Learn more: https://aka.ms/CosmosDB/sql/errors/wrong-pk-value",
				HttpStatusCode.BadRequest, 1001, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}
	}

	/// <summary>
	/// Stream-API variant of <see cref="ValidatePartitionKeyConsistency"/> that returns
	/// a <see cref="ResponseMessage"/> instead of throwing, matching the stream API convention.
	/// </summary>
	private ResponseMessage ValidatePartitionKeyConsistencyStream(PartitionKey? explicitKey, JObject jObj)
	{
		if (!explicitKey.HasValue || explicitKey.Value == PartitionKey.None || explicitKey.Value == PartitionKey.Null)
			return null;

		if (PartitionKeyPaths is not { Count: 1 })
			return null;

		var pkPath = PartitionKeyPaths[0].TrimStart('/');
		var bodyToken = jObj.SelectToken(pkPath);
		if (bodyToken is null)
			return null;

		var explicitPk = PartitionKeyToString(explicitKey.Value);
		var bodyPk = JTokenToTypedKey(bodyToken);

		if (bodyPk != explicitPk)
		{
			return CreateResponseMessage(HttpStatusCode.BadRequest, subStatusCode: 1001);
		}

		return null;
	}

	private void ValidatePatchPaths(IReadOnlyList<PatchOperation> operations)
	{
		foreach (var op in operations)
		{
			var path = op.Path;
			if (string.Equals(path, "/id", StringComparison.OrdinalIgnoreCase))
			{
				throw InMemoryCosmosException.Create(
					"Cannot patch the 'id' field.",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			}

			if (PartitionKeyPaths is { Count: > 0 })
			{
				foreach (var pkPath in PartitionKeyPaths)
				{
					if (string.Equals(path, pkPath, StringComparison.OrdinalIgnoreCase))
					{
						throw InMemoryCosmosException.Create(
							"Cannot patch the partition key field.",
							HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
					}
				}
			}

			var cps = _containerProperties?.ComputedProperties;
			if (cps is { Count: > 0 })
			{
				foreach (var cp in cps)
				{
					if (string.Equals(path, "/" + cp.Name, StringComparison.OrdinalIgnoreCase))
					{
						throw InMemoryCosmosException.Create(
							"Cannot patch a computed property path.",
							HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
					}
				}
			}
		}
	}

	private void ValidateContainerReplace(ContainerProperties newProperties)
	{
		// Real Cosmos DB rejects partition key path changes
		var existingPkPath = _containerProperties.PartitionKeyPath;
		var newPkPath = newProperties.PartitionKeyPath;
		if (!string.IsNullOrEmpty(newPkPath) && !string.IsNullOrEmpty(existingPkPath) && newPkPath != existingPkPath)
		{
			throw InMemoryCosmosException.Create(
				"Partition key paths for a container cannot be changed.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}
	}

	private void ValidatePerItemTtl(JObject jObj)
	{
		var ttlToken = jObj["ttl"] ?? jObj["_ttl"];
		if (ttlToken is not null && int.TryParse(ttlToken.ToString(), out var ttlValue) && ttlValue == 0)
		{
			throw InMemoryCosmosException.Create(
				"The value of ttl must be either -1 or a positive integer.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}
	}

	private ResponseMessage ValidatePerItemTtlStream(JObject jObj)
	{
		var ttlToken = jObj["ttl"] ?? jObj["_ttl"];
		if (ttlToken is not null && int.TryParse(ttlToken.ToString(), out var ttlValue) && ttlValue == 0)
		{
			return CreateResponseMessage(HttpStatusCode.BadRequest);
		}
		return null;
	}

	private static void ValidateMaxItemCount(QueryRequestOptions requestOptions)
	{
		if (requestOptions?.MaxItemCount == 0)
		{
			throw InMemoryCosmosException.Create(
				"MaxItemCount must be a positive value or -1.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
		}
	}

	private static readonly HashSet<string> ReservedComputedPropertyNames = new(StringComparer.OrdinalIgnoreCase)
	{
		"id", "_rid", "_ts", "_etag", "_self", "_attachments"
	};

	private static readonly string[] ProhibitedCpClauses =
		{ " WHERE ", " ORDER BY ", " GROUP BY ", " TOP ", " DISTINCT ", " OFFSET ", " LIMIT ", " JOIN " };

	private static void ValidateComputedProperties(ContainerProperties properties)
	{
		var cps = properties.ComputedProperties;
		if (cps is null or { Count: 0 }) return;

		if (cps.Count > 20)
		{
			throw InMemoryCosmosException.Create(
				"A container can have at most 20 computed properties.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
		}

		// Two-pass: collect all names first for cross-CP reference check (order-independent)
		var allNames = new HashSet<string>(StringComparer.Ordinal);
		foreach (var cp in cps)
		{
			if (!allNames.Add(cp.Name))
			{
				throw InMemoryCosmosException.Create(
					$"Duplicate computed property name '{cp.Name}'.",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
			}
		}

		foreach (var cp in cps)
		{
			if (ReservedComputedPropertyNames.Contains(cp.Name))
			{
				throw InMemoryCosmosException.Create(
					$"Computed property name '{cp.Name}' is a reserved system property name.",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
			}

			if (string.IsNullOrWhiteSpace(cp.Query))
			{
				throw InMemoryCosmosException.Create(
					$"Computed property '{cp.Name}' must have a non-empty query.",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
			}

			// Normalize whitespace for robust checking
			var normalized = System.Text.RegularExpressions.Regex.Replace(cp.Query.Trim(), @"\s+", " ");
			if (!normalized.StartsWith("SELECT VALUE", StringComparison.OrdinalIgnoreCase))
			{
				throw InMemoryCosmosException.Create(
					"Computed property query must use 'SELECT VALUE' syntax.",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
			}

			// Check entire query for prohibited clauses
			foreach (var clause in ProhibitedCpClauses)
			{
				if (normalized.IndexOf(clause, StringComparison.OrdinalIgnoreCase) >= 0)
				{
					throw InMemoryCosmosException.Create(
						$"Computed property query cannot contain '{clause.Trim()}' clause.",
						HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
				}
			}

			// Check for self-referencing CP
			var selfPattern = @"\." + System.Text.RegularExpressions.Regex.Escape(cp.Name) + @"(?![a-zA-Z0-9_])";
			if (System.Text.RegularExpressions.Regex.IsMatch(normalized, selfPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
			{
				throw InMemoryCosmosException.Create(
					$"Computed property '{cp.Name}' cannot reference itself.",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
			}

			// Check for cross-CP references (all other CP names, order-independent)
			foreach (var otherName in allNames)
			{
				if (otherName.Equals(cp.Name, StringComparison.OrdinalIgnoreCase)) continue;
				var crossPattern = @"\." + System.Text.RegularExpressions.Regex.Escape(otherName) + @"(?![a-zA-Z0-9_])";
				if (System.Text.RegularExpressions.Regex.IsMatch(normalized, crossPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase))
				{
					throw InMemoryCosmosException.Create(
						$"Computed property '{cp.Name}' cannot reference another computed property '{otherName}'.",
						HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
				}
			}
		}
	}

	private string EnrichWithSystemProperties(string json, string etag, DateTimeOffset timestamp)
	{
		var jObj = JsonParseHelpers.ParseJson(json);
		var containerHash = PartitionKeyHash.MurmurHash3(Id);
		var docId = (uint)Interlocked.Increment(ref _docRidCounter);
		var ridBytes = new byte[8];
		Buffer.BlockCopy(BitConverter.GetBytes(containerHash), 0, ridBytes, 0, 4);
		Buffer.BlockCopy(BitConverter.GetBytes(docId), 0, ridBytes, 4, 4);
		jObj["_rid"] = Convert.ToBase64String(ridBytes);
		jObj["_self"] = $"dbs/db/colls/col/docs/{jObj["id"]}";
		jObj["_etag"] = etag;
		jObj["_ts"] = timestamp.ToUnixTimeSeconds();
		jObj["_attachments"] = "attachments/";
		var enriched = jObj.ToString(Formatting.None);
		ValidateDocumentSize(enriched);
		return enriched;
	}

	private static int ParseContinuationToken(string continuationToken)
	{
		if (continuationToken is null)
			return 0;

		if (int.TryParse(continuationToken, out var offset))
			return offset;

		throw InMemoryCosmosException.Create(
			$"Invalid continuation token '{continuationToken}'.",
			HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), 0);
	}

	private void RecordChangeFeed(string id, string partitionKey, string json, bool isDelete = false)
	{
		lock (_changeFeedLock)
		{
			_sessionSequence++;
			var lsn = _changeFeedLsnCounter++;
			var jsonWithLsn = json.Insert(1, $"\"_lsn\":{lsn},");
			_changeFeed.Add((DateTimeOffset.UtcNow, id, partitionKey, jsonWithLsn, isDelete));

			if (MaxChangeFeedSize > 0 && _changeFeed.Count > MaxChangeFeedSize)
			{
				var excess = _changeFeed.Count - MaxChangeFeedSize;
				_changeFeed.RemoveRange(0, excess);
			}
		}
	}

	private void RecordDeleteTombstone(string id, string pk, PartitionKey partitionKey = default, bool isTtlEviction = false)
	{
		var tombstone = new JObject { ["id"] = id, ["_deleted"] = true, ["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds() };
		if (isTtlEviction) tombstone["_ttlEviction"] = true;

		if (PartitionKeyPaths.Count > 1 && partitionKey != default && partitionKey != PartitionKey.None && partitionKey != PartitionKey.Null)
		{
			// For composite keys, parse from the PartitionKey object directly to avoid
			// pipe-delimiter issues when a PK value itself contains '|'.
			var raw = partitionKey.ToString();
			try
			{
				var arr = JArray.Parse(raw);
				for (var i = 0; i < PartitionKeyPaths.Count && i < arr.Count; i++)
				{
					tombstone[PartitionKeyPaths[i].TrimStart('/')] = arr[i].Type == JTokenType.String
						? arr[i].Value<string>()
						: arr[i].ToString();
				}
			}
			catch
			{
				// Fallback to split-based approach
				var pkValues = (pk ?? string.Empty).Split('|');
				for (var i = 0; i < PartitionKeyPaths.Count; i++)
				{
					tombstone[PartitionKeyPaths[i].TrimStart('/')] = i < pkValues.Length ? TypedKeyToJToken(pkValues[i]) : null;
				}
			}
		}
		else if (pk is not null)
		{
			if (PartitionKeyPaths.Count == 1)
			{
				// Single PK path — convert the typed key back to a JToken value
				tombstone[PartitionKeyPaths[0].TrimStart('/')] = TypedKeyToJToken(pk);
			}
			else
			{
				var pkValues = pk.Split('|');
				for (var i = 0; i < PartitionKeyPaths.Count; i++)
				{
					tombstone[PartitionKeyPaths[i].TrimStart('/')] = i < pkValues.Length ? TypedKeyToJToken(pkValues[i]) : null;
				}
			}
		}

		RecordChangeFeed(id, pk, tombstone.ToString(Newtonsoft.Json.Formatting.None), isDelete: true);
	}

	private static TriggerOperation OperationNameToTriggerOp(string operationName) => operationName switch
	{
		"Create" => TriggerOperation.Create,
		"Replace" => TriggerOperation.Replace,
		"Upsert" => TriggerOperation.Upsert,
		"Delete" => TriggerOperation.Delete,
		_ => TriggerOperation.All
	};

	private PartitionScopedCollectionContext CreateCollectionContextFromDoc(JObject jObj)
	{
		var pkValue = ExtractPartitionKeyValueCore(null, jObj);
		var pk = TypedKeyToPartitionKey(pkValue);
		return new PartitionScopedCollectionContext(this, pk);
	}

	private static bool TriggerOperationMatches(TriggerOperation registered, TriggerOperation current)
		=> registered == TriggerOperation.All || registered == current;

	private JObject ExecutePreTriggers(ItemRequestOptions requestOptions, JObject jObj, string operationName)
	{
		var triggerNames = requestOptions?.PreTriggers;
		if (triggerNames is null) return jObj;

		var currentOp = OperationNameToTriggerOp(operationName);

		foreach (var name in triggerNames)
		{
			// Priority 1: C# handler
			if (_triggers.TryGetValue(name, out var trigger))
			{
				if (trigger.TriggerType != TriggerType.Pre || trigger.PreHandler is null) continue;
				if (!TriggerOperationMatches(trigger.TriggerOperation, currentOp)) continue;

				try
				{
					jObj = trigger.PreHandler(jObj);
				}
				catch (CosmosException) { throw; }
				catch (Exception ex)
				{
					throw InMemoryCosmosException.Create(
						$"Pre-trigger '{name}' failed: {ex.Message}",
						HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}
				break; // Real Cosmos only fires the first matching trigger
			}

			// Priority 2: JS body via JsTriggerEngine
			if (_triggerProperties.TryGetValue(name, out var props) && props.Body is not null)
			{
				if (props.TriggerType != TriggerType.Pre) continue;
				if (!TriggerOperationMatches(props.TriggerOperation, currentOp)) continue;

				if (JsTriggerEngine is null)
				{
					throw InMemoryCosmosException.Create(
						$"Trigger '{name}' has a JavaScript body but no JS trigger engine is configured. " +
						"Install the CosmosDB.InMemoryEmulator.JsTriggers package and call container.UseJsTriggers().",
						HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}

				var originalId = jObj["id"]?.ToString();
				var pkPath = PartitionKeyPaths[0].TrimStart('/');
				var originalPk = jObj.SelectToken(pkPath)?.ToString();

				jObj = JsTriggerEngine.ExecutePreTrigger(props.Body, jObj,
					CreateCollectionContextFromDoc(jObj));

				// Validate the trigger did not change id or partition key
				var newId = jObj["id"]?.ToString();
				var newPk = jObj.SelectToken(pkPath)?.ToString();
				if (!string.Equals(originalId, newId, StringComparison.Ordinal))
				{
					throw InMemoryCosmosException.Create(
						$"Pre-trigger '{name}' is not allowed to modify the document id.",
						HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}
				if (!string.Equals(originalPk, newPk, StringComparison.Ordinal))
				{
					throw InMemoryCosmosException.Create(
						$"Pre-trigger '{name}' is not allowed to modify the partition key.",
						HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}

				break; // Real Cosmos only fires the first matching trigger
			}

			throw InMemoryCosmosException.Create(
				$"Trigger '{name}' is not registered. Register it via RegisterTrigger() or CreateTriggerAsync() before referencing it in PreTriggers.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		return jObj;
	}

	private JObject ExecutePostTriggers(ItemRequestOptions requestOptions, JObject committedDoc, string operationName)
	{
		var triggerNames = requestOptions?.PostTriggers;
		if (triggerNames is null) return null;

		JObject responseBodyOverride = null;
		var currentOp = OperationNameToTriggerOp(operationName);

		foreach (var name in triggerNames)
		{
			// Priority 1: C# handler
			if (_triggers.TryGetValue(name, out var trigger))
			{
				if (trigger.TriggerType != TriggerType.Post || trigger.PostHandler is null) continue;
				if (!TriggerOperationMatches(trigger.TriggerOperation, currentOp)) continue;

				try
				{
					trigger.PostHandler(committedDoc);
				}
				catch (CosmosException)
				{
					throw;
				}
				catch (Exception ex)
				{
					throw InMemoryCosmosException.Create(
						$"Post-trigger '{name}' failed: {ex.Message}",
						HttpStatusCode.InternalServerError, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}
				break; // Real Cosmos only fires the first matching trigger
			}

			// Priority 2: JS body via JsTriggerEngine
			if (_triggerProperties.TryGetValue(name, out var props) && props.Body is not null)
			{
				if (props.TriggerType != TriggerType.Post) continue;
				if (!TriggerOperationMatches(props.TriggerOperation, currentOp)) continue;

				if (JsTriggerEngine is null)
				{
					throw InMemoryCosmosException.Create(
						$"Trigger '{name}' has a JavaScript body but no JS trigger engine is configured. " +
						"Install the CosmosDB.InMemoryEmulator.JsTriggers package and call container.UseJsTriggers().",
						HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}

				try
				{
					var setBodyResult = JsTriggerEngine.ExecutePostTrigger(props.Body, committedDoc,
						CreateCollectionContextFromDoc(committedDoc));
					if (setBodyResult is not null)
					{
						responseBodyOverride = setBodyResult;
					}
				}
				catch (CosmosException)
				{
					throw;
				}
				catch (Exception ex)
				{
					throw InMemoryCosmosException.Create(
						$"Post-trigger '{name}' failed: {ex.Message}",
						HttpStatusCode.InternalServerError, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}
				break; // Real Cosmos only fires the first matching trigger
			}

			throw InMemoryCosmosException.Create(
				$"Trigger '{name}' is not registered. Register it via RegisterTrigger() or CreateTriggerAsync() before referencing it in PostTriggers.",
				HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		return responseBodyOverride;
	}

	private void CheckIfMatch(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
	{
		if (requestOptions?.IfMatchEtag is null)
		{
			return;
		}

		if (requestOptions.IfMatchEtag == "*")
		{
			return;
		}

		if (!_etags.TryGetValue(key, out var currentEtag))
		{
			return;
		}

		if (requestOptions.IfMatchEtag != currentEtag)
		{
			throw InMemoryCosmosException.Create("Precondition Failed", HttpStatusCode.PreconditionFailed, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}
	}

	private void CheckIfNoneMatch(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
	{
		if (requestOptions?.IfNoneMatchEtag is null)
		{
			return;
		}

		if (requestOptions.IfNoneMatchEtag == "*" && _etags.ContainsKey(key))
		{
			throw InMemoryCosmosException.Create("Not Modified", HttpStatusCode.NotModified, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		if (_etags.TryGetValue(key, out var currentEtag) && requestOptions.IfNoneMatchEtag == currentEtag)
		{
			throw InMemoryCosmosException.Create("Not Modified", HttpStatusCode.NotModified, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}
	}

	/// <summary>
	/// IfNoneMatch check for write operations (Patch). Write operations return 412 PreconditionFailed
	/// instead of 304 NotModified when the ETag matches.
	/// </summary>
	private void CheckIfNoneMatchForWrite(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
	{
		if (requestOptions?.IfNoneMatchEtag is null)
		{
			return;
		}

		if (requestOptions.IfNoneMatchEtag == "*" && _etags.ContainsKey(key))
		{
			throw InMemoryCosmosException.Create("Precondition Failed", HttpStatusCode.PreconditionFailed, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}

		if (_etags.TryGetValue(key, out var currentEtag) && requestOptions.IfNoneMatchEtag == currentEtag)
		{
			throw InMemoryCosmosException.Create("Precondition Failed", HttpStatusCode.PreconditionFailed, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}
	}

	private bool CheckIfMatchStream(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
	{
		if (requestOptions?.IfMatchEtag is null)
		{
			return true;
		}

		if (requestOptions.IfMatchEtag == "*")
		{
			return true;
		}

		if (_etags.TryGetValue(key, out var currentEtag) && requestOptions.IfMatchEtag != currentEtag)
		{
			return false;
		}

		return true;
	}

	private bool CheckIfNoneMatchStream(ItemRequestOptions requestOptions, (string Id, string PartitionKey) key)
	{
		if (requestOptions?.IfNoneMatchEtag is null)
		{
			return true;
		}

		if (requestOptions.IfNoneMatchEtag == "*" && _etags.ContainsKey(key))
		{
			return false;
		}

		if (_etags.TryGetValue(key, out var currentEtag) && requestOptions.IfNoneMatchEtag == currentEtag)
		{
			return false;
		}

		return true;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Private helpers — Serialization & Response factories
	// ═══════════════════════════════════════════════════════════════════════════

	private static string ReadStream(Stream stream)
	{
		using var reader = new StreamReader(stream, Encoding.UTF8, true, 1024, true);
		return reader.ReadToEnd();
	}

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	private static readonly CosmosDiagnostics FakeDiagnostics = new InMemoryDiagnostics();

	private sealed class InMemoryDiagnostics : CosmosDiagnostics
	{
		public override TimeSpan GetClientElapsedTime() => TimeSpan.Zero;
		public override IReadOnlyList<(string regionName, Uri uri)> GetContactedRegions() => Array.Empty<(string, Uri)>();
		public override string ToString() => "{}";
	}

	private ItemResponse<T> CreateItemResponse<T>(T item, HttpStatusCode statusCode, string etag = null, bool suppressContent = false)
	{
		var activityId = Guid.NewGuid().ToString();
		var headers = new Headers
		{
			["x-ms-session-token"] = CurrentSessionToken,
			["x-ms-activity-id"] = activityId,
			["x-ms-request-charge"] = SyntheticRequestCharge.ToString(CultureInfo.InvariantCulture)
		};
		return new InMemoryItemResponse<T>(
			statusCode, headers, suppressContent ? default : item, FakeDiagnostics,
			SyntheticRequestCharge, activityId, etag);
	}

	private sealed class InMemoryItemResponse<T> : ItemResponse<T>
	{
		private readonly HttpStatusCode _statusCode;
		private readonly Headers _headers;
		private readonly T _resource;
		private readonly CosmosDiagnostics _diagnostics;
		private readonly double _requestCharge;
		private readonly string _activityId;
		private readonly string _etag;

		public InMemoryItemResponse(
			HttpStatusCode statusCode, Headers headers, T resource, CosmosDiagnostics diagnostics,
			double requestCharge, string activityId, string etag)
		{
			_statusCode = statusCode;
			_headers = headers;
			_resource = resource;
			_diagnostics = diagnostics;
			_requestCharge = requestCharge;
			_activityId = activityId;
			_etag = etag;
		}

		public override HttpStatusCode StatusCode => _statusCode;
		public override Headers Headers => _headers;
		public override T Resource => _resource;
		public override CosmosDiagnostics Diagnostics => _diagnostics;
		public override double RequestCharge => _requestCharge;
		public override string ActivityId => _activityId;
		public override string ETag => _etag;
	}

	private ResponseMessage CreateResponseMessage(HttpStatusCode statusCode, string json = null, string etag = null, int subStatusCode = 0)
	{
		// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
		var errorMessage = statusCode switch
		{
			HttpStatusCode.NotFound => "Resource Not Found. Entity with the specified id does not exist in the system.",
			_ when (int)statusCode >= 400 => $"Response status code does not indicate success: {statusCode} ({(int)statusCode})",
			_ => null
		};

		// For error responses with no explicit body, synthesize a JSON error body matching the
		// real Cosmos DB REST API format. The SDK reads this body to populate CosmosException.ResponseBody.
		// Ref: Observed Cosmos DB REST API error body: {"code":"NotFound","message":"Resource Not Found. ..."}
		if (json is null && errorMessage is not null)
		{
			json = $"{{\"code\":\"{statusCode}\",\"message\":\"{errorMessage}\"}}";
		}

		var msg = new ResponseMessage(statusCode, requestMessage: null, errorMessage: errorMessage)
		{
			Content = json is not null ? ToStream(json) : null
		};
		msg.Headers["x-ms-activity-id"] = Guid.NewGuid().ToString();
		msg.Headers["x-ms-request-charge"] = SyntheticRequestCharge.ToString(CultureInfo.InvariantCulture);
		msg.Headers["x-ms-session-token"] = CurrentSessionToken;
		if (subStatusCode != 0)
		{
			msg.Headers["x-ms-substatus"] = subStatusCode.ToString(CultureInfo.InvariantCulture);
		}
		if (etag is not null)
		{
			msg.Headers["ETag"] = etag;
		}

		return msg;
	}

	private static void ValidateDocumentSize(string json)
	{
		var byteCount = Encoding.UTF8.GetByteCount(json);
		if (byteCount > MaxDocumentSizeBytes)
		{
			throw InMemoryCosmosException.Create(
				$"Request size is too large. Max allowed size in bytes: {MaxDocumentSizeBytes}. Found: {byteCount}.",
				HttpStatusCode.RequestEntityTooLarge, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
		}
	}

	private ResponseMessage ValidateDocumentSizeStream(string json)
	{
		var byteCount = Encoding.UTF8.GetByteCount(json);
		if (byteCount > MaxDocumentSizeBytes)
		{
			return CreateResponseMessage(HttpStatusCode.RequestEntityTooLarge);
		}
		return null;
	}

	private void ValidateUniqueKeys(JObject jObj, string partitionKey, string excludeItemId = null)
	{
		var policy = _containerProperties.UniqueKeyPolicy;
		if (policy == null || policy.UniqueKeys.Count == 0) return;

		foreach (var uniqueKey in policy.UniqueKeys)
		{
			// Cosmos DB unique key paths use /parent/child format.
			// JObject.SelectToken uses dot notation (parent.child).
			var newValues = uniqueKey.Paths.Select(p => jObj.SelectToken(p.TrimStart('/').Replace('/', '.'))?.ToString()).ToList();

			foreach (var (existingKey, existingJson) in _items)
			{
				if (existingKey.PartitionKey != partitionKey) continue;
				if (excludeItemId != null && existingKey.Id == excludeItemId) continue;

				var existingObj = JsonParseHelpers.ParseJson(existingJson);
				var existingValues = uniqueKey.Paths.Select(p => existingObj.SelectToken(p.TrimStart('/').Replace('/', '.'))?.ToString()).ToList();

				if (newValues.SequenceEqual(existingValues))
				{
					throw InMemoryCosmosException.Create(
						"Unique index constraint violation.",
						HttpStatusCode.Conflict, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				}
			}
		}
	}

	private bool ValidateUniqueKeysStream(JObject jObj, string partitionKey, string excludeItemId = null)
	{
		try
		{
			ValidateUniqueKeys(jObj, partitionKey, excludeItemId);
			return true;
		}
		catch (CosmosException)
		{
			return false;
		}
	}

	private bool IsExpired((string Id, string PartitionKey) key)
	{
		// TTL feature is completely disabled when DefaultTimeToLive is null.
		// Per-item _ttl is ignored in this case (matches real Cosmos DB behaviour).
		if (DefaultTimeToLive is null)
		{
			return false;
		}

		if (!_timestamps.TryGetValue(key, out var ts))
		{
			return false;
		}

		var elapsed = (DateTimeOffset.UtcNow - ts).TotalSeconds;

		if (_items.TryGetValue(key, out var json))
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			var itemTtl = jObj["ttl"] ?? jObj["_ttl"];
			if (itemTtl is not null && int.TryParse(itemTtl.ToString(), out var perItemTtl))
			{
				// ttl = -1 means "never expire" even if container has a default TTL
				if (perItemTtl == -1) return false;
				return elapsed >= perItemTtl;
			}
		}

		// DefaultTimeToLive = -1 means TTL is ON but items without per-item ttl don't expire
		if (DefaultTimeToLive.Value <= 0)
		{
			return false;
		}

		return elapsed >= DefaultTimeToLive.Value;
	}

	private void EvictIfExpired((string Id, string PartitionKey) key)
	{
		if (!IsExpired(key))
		{
			return;
		}

		_items.TryRemove(key, out _);
		_etags.TryRemove(key, out _);
		_timestamps.TryRemove(key, out _);
		lock (_insertionOrderLock) { _insertionOrder.Remove(key); }

		// Record a delete tombstone in the change feed so consumers see TTL evictions
		RecordDeleteTombstone(key.Id, key.PartitionKey, isTtlEviction: true);
	}

	internal Dictionary<(string Id, string PartitionKey), string> SnapshotItems()
		=> new(_items);

	internal Dictionary<(string Id, string PartitionKey), string> SnapshotEtags()
		=> new(_etags);

	internal Dictionary<(string Id, string PartitionKey), DateTimeOffset> SnapshotTimestamps()
		=> new(_timestamps);

	internal int GetChangeFeedCount()
	{
		lock (_changeFeedLock)
		{
			return _changeFeed.Count;
		}
	}

	internal void BeginBatchTracking()
	{
		BatchWriteTracker.Value = new HashSet<(string Id, string PartitionKey)>();
	}

	internal HashSet<(string Id, string PartitionKey)> EndBatchTracking()
	{
		var keys = BatchWriteTracker.Value ?? new HashSet<(string Id, string PartitionKey)>();
		BatchWriteTracker.Value = null;
		return keys;
	}

	private void TrackBatchWrite((string Id, string PartitionKey) key)
	{
		BatchWriteTracker.Value?.Add(key);
	}

	internal void RestoreSnapshot(
		Dictionary<(string Id, string PartitionKey), string> itemsSnapshot,
		Dictionary<(string Id, string PartitionKey), string> etagsSnapshot,
		Dictionary<(string Id, string PartitionKey), DateTimeOffset> timestampsSnapshot,
		int changeFeedCount,
		IReadOnlySet<(string Id, string PartitionKey)> touchedKeys)
	{
		foreach (var key in touchedKeys)
		{
			if (itemsSnapshot.TryGetValue(key, out var snapshotItem))
			{
				_items[key] = snapshotItem;
				_etags[key] = etagsSnapshot[key];
				_timestamps[key] = timestampsSnapshot[key];
			}
			else
			{
				_items.TryRemove(key, out _);
				_etags.TryRemove(key, out _);
				_timestamps.TryRemove(key, out _);
				lock (_insertionOrderLock) { _insertionOrder.Remove(key); }
			}
		}

		// Restore insertion order for keys that were newly added during the batch
		// but didn't exist in the snapshot — they need to be removed from insertion order.
		// Keys that existed in the snapshot should already be in insertion order.
		lock (_insertionOrderLock)
		{
			foreach (var key in touchedKeys)
			{
				if (!itemsSnapshot.ContainsKey(key))
				{
					// Already removed above
				}
				else if (!_insertionOrder.Contains(key))
				{
					// Item existed in snapshot but is missing from insertion order
					// (shouldn't normally happen, but be safe)
					_insertionOrder.Add(key);
				}
			}
		}

		lock (_changeFeedLock)
		{
			while (_changeFeed.Count > changeFeedCount)
			{
				_changeFeed.RemoveAt(_changeFeed.Count - 1);
			}
		}
	}

	private sealed class InMemoryScripts : Scripts
	{
		private readonly InMemoryContainer _c;

		public InMemoryScripts(InMemoryContainer container) => _c = container;

		// ── Stored Procedure CRUD (typed) ─────────────────────────────────

		public override Task<StoredProcedureResponse> CreateStoredProcedureAsync(
			StoredProcedureProperties storedProcedureProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (_c._storedProcedureProperties.ContainsKey(storedProcedureProperties.Id))
				throw InMemoryCosmosException.Create($"StoredProcedure '{storedProcedureProperties.Id}' already exists.",
					HttpStatusCode.Conflict, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._storedProcedureProperties[storedProcedureProperties.Id] = storedProcedureProperties;
			EnrichStoredProcedureSystemProperties(storedProcedureProperties);
			var r = Substitute.For<StoredProcedureResponse>();
			r.StatusCode.Returns(HttpStatusCode.Created);
			r.Resource.Returns(storedProcedureProperties);
			return Task.FromResult(r);
		}

		public override Task<StoredProcedureResponse> ReadStoredProcedureAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._storedProcedureProperties.TryGetValue(id, out var props))
				throw InMemoryCosmosException.Create($"StoredProcedure '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			EnrichStoredProcedureSystemProperties(props);
			var r = Substitute.For<StoredProcedureResponse>();
			r.StatusCode.Returns(HttpStatusCode.OK);
			r.Resource.Returns(props);
			return Task.FromResult(r);
		}

		public override Task<StoredProcedureResponse> ReplaceStoredProcedureAsync(
			StoredProcedureProperties storedProcedureProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (!_c._storedProcedureProperties.ContainsKey(storedProcedureProperties.Id))
				throw InMemoryCosmosException.Create($"StoredProcedure '{storedProcedureProperties.Id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._storedProcedureProperties[storedProcedureProperties.Id] = storedProcedureProperties;
			EnrichStoredProcedureSystemProperties(storedProcedureProperties);
			var r = Substitute.For<StoredProcedureResponse>();
			r.StatusCode.Returns(HttpStatusCode.OK);
			r.Resource.Returns(storedProcedureProperties);
			return Task.FromResult(r);
		}

		public override Task<StoredProcedureResponse> DeleteStoredProcedureAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._storedProcedureProperties.Remove(id))
				throw InMemoryCosmosException.Create($"StoredProcedure '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._storedProcedures.Remove(id);
			var r = Substitute.For<StoredProcedureResponse>();
			r.StatusCode.Returns(HttpStatusCode.NoContent);
			return Task.FromResult(r);
		}

		// ── Stored Procedure CRUD (stream) ────────────────────────────────

		public override Task<ResponseMessage> CreateStoredProcedureStreamAsync(
			StoredProcedureProperties storedProcedureProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (_c._storedProcedureProperties.ContainsKey(storedProcedureProperties.Id))
				throw InMemoryCosmosException.Create($"StoredProcedure '{storedProcedureProperties.Id}' already exists.",
					HttpStatusCode.Conflict, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._storedProcedureProperties[storedProcedureProperties.Id] = storedProcedureProperties;
			EnrichStoredProcedureSystemProperties(storedProcedureProperties);
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.Created,
				JsonConvert.SerializeObject(storedProcedureProperties)));
		}

		public override Task<ResponseMessage> ReadStoredProcedureStreamAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._storedProcedureProperties.TryGetValue(id, out var props))
				throw InMemoryCosmosException.Create($"StoredProcedure '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			EnrichStoredProcedureSystemProperties(props);
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.OK,
				JsonConvert.SerializeObject(props)));
		}

		public override Task<ResponseMessage> ReplaceStoredProcedureStreamAsync(
			StoredProcedureProperties storedProcedureProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (!_c._storedProcedureProperties.ContainsKey(storedProcedureProperties.Id))
				throw InMemoryCosmosException.Create($"StoredProcedure '{storedProcedureProperties.Id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._storedProcedureProperties[storedProcedureProperties.Id] = storedProcedureProperties;
			EnrichStoredProcedureSystemProperties(storedProcedureProperties);
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.OK,
				JsonConvert.SerializeObject(storedProcedureProperties)));
		}

		public override Task<ResponseMessage> DeleteStoredProcedureStreamAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._storedProcedureProperties.Remove(id))
				throw InMemoryCosmosException.Create($"StoredProcedure '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._storedProcedures.Remove(id);
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.NoContent));
		}

		// ── Stored Procedure Execute ──────────────────────────────────────

		public override Task<StoredProcedureExecuteResponse<TOutput>> ExecuteStoredProcedureAsync<TOutput>(
			string storedProcedureId, PartitionKey partitionKey, dynamic[] parameters,
			StoredProcedureRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._storedProcedures.ContainsKey(storedProcedureId) && !_c._storedProcedureProperties.ContainsKey(storedProcedureId))
				throw InMemoryCosmosException.Create($"StoredProcedure '{storedProcedureId}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);

			string handlerResult = null;
			var hasHandler = false;
			if (_c._storedProcedures.TryGetValue(storedProcedureId, out var handler))
			{
				handlerResult = ExecuteHandler(storedProcedureId, () => handler(partitionKey, parameters));
				hasHandler = true;
			}
			else if (_c.SprocEngine is not null
				&& _c._storedProcedureProperties.TryGetValue(storedProcedureId, out var sprocProps)
				&& sprocProps.Body is not null)
			{
				handlerResult = ExecuteJsEngine(storedProcedureId, sprocProps.Body, partitionKey, parameters);
				hasHandler = true;
			}

			var r = Substitute.For<StoredProcedureExecuteResponse<TOutput>>();
			r.StatusCode.Returns(HttpStatusCode.OK);
			r.RequestCharge.Returns(SyntheticRequestCharge);
			if (hasHandler)
			{
				if (handlerResult is not null)
				{
					TOutput deserialized = typeof(TOutput) == typeof(string)
						? (TOutput)(object)handlerResult
						: JsonConvert.DeserializeObject<TOutput>(handlerResult);
					r.Resource.Returns(deserialized);
				}
				else
				{
					r.Resource.Returns(default(TOutput));
				}
			}

			PopulateScriptLogHeaders(r);
			return Task.FromResult(r);
		}

		public override Task<ResponseMessage> ExecuteStoredProcedureStreamAsync(
			string storedProcedureId, PartitionKey partitionKey, dynamic[] parameters,
			StoredProcedureRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._storedProcedures.ContainsKey(storedProcedureId) && !_c._storedProcedureProperties.ContainsKey(storedProcedureId))
				throw InMemoryCosmosException.Create($"StoredProcedure '{storedProcedureId}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);

			string handlerResult = null;
			if (_c._storedProcedures.TryGetValue(storedProcedureId, out var handler))
			{
				handlerResult = ExecuteHandler(storedProcedureId, () => handler(partitionKey, parameters));
			}
			else if (_c.SprocEngine is not null
				&& _c._storedProcedureProperties.TryGetValue(storedProcedureId, out var sprocProps)
				&& sprocProps.Body is not null)
			{
				handlerResult = ExecuteJsEngine(storedProcedureId, sprocProps.Body, partitionKey, parameters);
			}

			var msg = _c.CreateResponseMessage(HttpStatusCode.OK, handlerResult);
			if (_c.SprocEngine?.CapturedLogs is { Count: > 0 } logs)
				msg.Headers["x-ms-documentdb-script-log-results"] = Uri.EscapeDataString(string.Join("\n", logs));
			return Task.FromResult(msg);
		}

		public override Task<ResponseMessage> ExecuteStoredProcedureStreamAsync(
			string storedProcedureId, Stream streamPayload, PartitionKey partitionKey,
			StoredProcedureRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			using var reader = new StreamReader(streamPayload);
			var json = reader.ReadToEnd();
			var parameters = JsonConvert.DeserializeObject<dynamic[]>(json) ?? Array.Empty<dynamic>();
			return ExecuteStoredProcedureStreamAsync(storedProcedureId, partitionKey, parameters, requestOptions, cancellationToken);
		}

		// ── Trigger CRUD (typed) ──────────────────────────────────────────

		public override Task<TriggerResponse> CreateTriggerAsync(
			TriggerProperties triggerProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (_c._triggerProperties.ContainsKey(triggerProperties.Id))
				throw InMemoryCosmosException.Create($"Trigger '{triggerProperties.Id}' already exists.",
					HttpStatusCode.Conflict, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._triggerProperties[triggerProperties.Id] = triggerProperties;
			var r = Substitute.For<TriggerResponse>();
			r.StatusCode.Returns(HttpStatusCode.Created);
			r.Resource.Returns(triggerProperties);
			return Task.FromResult(r);
		}

		public override Task<TriggerResponse> ReadTriggerAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._triggerProperties.TryGetValue(id, out var props))
				throw InMemoryCosmosException.Create($"Trigger '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			var r = Substitute.For<TriggerResponse>();
			r.StatusCode.Returns(HttpStatusCode.OK);
			r.Resource.Returns(props);
			return Task.FromResult(r);
		}

		public override Task<TriggerResponse> ReplaceTriggerAsync(
			TriggerProperties triggerProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (!_c._triggerProperties.ContainsKey(triggerProperties.Id))
				throw InMemoryCosmosException.Create($"Trigger '{triggerProperties.Id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._triggerProperties[triggerProperties.Id] = triggerProperties;
			var r = Substitute.For<TriggerResponse>();
			r.StatusCode.Returns(HttpStatusCode.OK);
			r.Resource.Returns(triggerProperties);
			return Task.FromResult(r);
		}

		public override Task<TriggerResponse> DeleteTriggerAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._triggerProperties.Remove(id))
				throw InMemoryCosmosException.Create($"Trigger '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._triggers.Remove(id);
			var r = Substitute.For<TriggerResponse>();
			r.StatusCode.Returns(HttpStatusCode.NoContent);
			return Task.FromResult(r);
		}

		// ── Trigger CRUD (stream) ─────────────────────────────────────────

		public override Task<ResponseMessage> CreateTriggerStreamAsync(
			TriggerProperties triggerProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (_c._triggerProperties.ContainsKey(triggerProperties.Id))
				return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.Conflict));
			_c._triggerProperties[triggerProperties.Id] = triggerProperties;
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.Created,
				JsonConvert.SerializeObject(triggerProperties)));
		}

		public override Task<ResponseMessage> ReadTriggerStreamAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._triggerProperties.TryGetValue(id, out var props))
				return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.NotFound));
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.OK,
				JsonConvert.SerializeObject(props)));
		}

		public override Task<ResponseMessage> ReplaceTriggerStreamAsync(
			TriggerProperties triggerProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (!_c._triggerProperties.ContainsKey(triggerProperties.Id))
				return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.NotFound));
			_c._triggerProperties[triggerProperties.Id] = triggerProperties;
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.OK,
				JsonConvert.SerializeObject(triggerProperties)));
		}

		public override Task<ResponseMessage> DeleteTriggerStreamAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._triggerProperties.Remove(id))
				return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.NotFound));
			_c._triggers.Remove(id);
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.NoContent));
		}

		// ── UDF CRUD (typed) ──────────────────────────────────────────────

		public override Task<UserDefinedFunctionResponse> CreateUserDefinedFunctionAsync(
			UserDefinedFunctionProperties userDefinedFunctionProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (_c._udfProperties.ContainsKey(userDefinedFunctionProperties.Id))
				throw InMemoryCosmosException.Create($"UDF '{userDefinedFunctionProperties.Id}' already exists.",
					HttpStatusCode.Conflict, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._udfProperties[userDefinedFunctionProperties.Id] = userDefinedFunctionProperties;
			if (!_c._userDefinedFunctions.ContainsKey("UDF." + userDefinedFunctionProperties.Id))
				_c._userDefinedFunctions["UDF." + userDefinedFunctionProperties.Id] = UdfPlaceholder;
			var r = Substitute.For<UserDefinedFunctionResponse>();
			r.StatusCode.Returns(HttpStatusCode.Created);
			r.Resource.Returns(userDefinedFunctionProperties);
			return Task.FromResult(r);
		}

		public override Task<UserDefinedFunctionResponse> ReadUserDefinedFunctionAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._udfProperties.TryGetValue(id, out var props))
				throw InMemoryCosmosException.Create($"UDF '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			var r = Substitute.For<UserDefinedFunctionResponse>();
			r.StatusCode.Returns(HttpStatusCode.OK);
			r.Resource.Returns(props);
			return Task.FromResult(r);
		}

		public override Task<UserDefinedFunctionResponse> ReplaceUserDefinedFunctionAsync(
			UserDefinedFunctionProperties userDefinedFunctionProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (!_c._udfProperties.ContainsKey(userDefinedFunctionProperties.Id))
				throw InMemoryCosmosException.Create($"UDF '{userDefinedFunctionProperties.Id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._udfProperties[userDefinedFunctionProperties.Id] = userDefinedFunctionProperties;
			var r = Substitute.For<UserDefinedFunctionResponse>();
			r.StatusCode.Returns(HttpStatusCode.OK);
			r.Resource.Returns(userDefinedFunctionProperties);
			return Task.FromResult(r);
		}

		public override Task<UserDefinedFunctionResponse> DeleteUserDefinedFunctionAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._udfProperties.Remove(id))
				throw InMemoryCosmosException.Create($"UDF '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._userDefinedFunctions.Remove("UDF." + id);
			var r = Substitute.For<UserDefinedFunctionResponse>();
			r.StatusCode.Returns(HttpStatusCode.NoContent);
			return Task.FromResult(r);
		}

		// ── UDF CRUD (stream) ─────────────────────────────────────────────

		public override Task<ResponseMessage> CreateUserDefinedFunctionStreamAsync(
			UserDefinedFunctionProperties userDefinedFunctionProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (_c._udfProperties.ContainsKey(userDefinedFunctionProperties.Id))
				throw InMemoryCosmosException.Create($"UDF '{userDefinedFunctionProperties.Id}' already exists.",
					HttpStatusCode.Conflict, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._udfProperties[userDefinedFunctionProperties.Id] = userDefinedFunctionProperties;
			if (!_c._userDefinedFunctions.ContainsKey("UDF." + userDefinedFunctionProperties.Id))
				_c._userDefinedFunctions["UDF." + userDefinedFunctionProperties.Id] = UdfPlaceholder;
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.Created,
				JsonConvert.SerializeObject(userDefinedFunctionProperties)));
		}

		public override Task<ResponseMessage> ReadUserDefinedFunctionStreamAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._udfProperties.TryGetValue(id, out var props))
				throw InMemoryCosmosException.Create($"UDF '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.OK,
				JsonConvert.SerializeObject(props)));
		}

		public override Task<ResponseMessage> ReplaceUserDefinedFunctionStreamAsync(
			UserDefinedFunctionProperties userDefinedFunctionProperties, RequestOptions requestOptions = null,
			CancellationToken cancellationToken = default)
		{
			if (!_c._udfProperties.ContainsKey(userDefinedFunctionProperties.Id))
				throw InMemoryCosmosException.Create($"UDF '{userDefinedFunctionProperties.Id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._udfProperties[userDefinedFunctionProperties.Id] = userDefinedFunctionProperties;
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.OK,
				JsonConvert.SerializeObject(userDefinedFunctionProperties)));
		}

		public override Task<ResponseMessage> DeleteUserDefinedFunctionStreamAsync(
			string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
		{
			if (!_c._udfProperties.Remove(id))
				throw InMemoryCosmosException.Create($"UDF '{id}' not found.",
					HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			_c._userDefinedFunctions.Remove("UDF." + id);
			return Task.FromResult(_c.CreateResponseMessage(HttpStatusCode.NoContent));
		}

		// ── Query iterators (typed) ───────────────────────────────────────

		public override FeedIterator<T> GetStoredProcedureQueryIterator<T>(
			string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryFeedIterator<T>(() => FilterById(_c._storedProcedureProperties, queryText).Cast<T>().ToList());

		public override FeedIterator<T> GetStoredProcedureQueryIterator<T>(
			QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryFeedIterator<T>(() => FilterById(_c._storedProcedureProperties, queryDefinition).Cast<T>().ToList());

		public override FeedIterator<T> GetTriggerQueryIterator<T>(
			string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryFeedIterator<T>(() => FilterById(_c._triggerProperties, queryText).Cast<T>().ToList());

		public override FeedIterator<T> GetTriggerQueryIterator<T>(
			QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryFeedIterator<T>(() => FilterById(_c._triggerProperties, queryDefinition).Cast<T>().ToList());

		public override FeedIterator<T> GetUserDefinedFunctionQueryIterator<T>(
			string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryFeedIterator<T>(() => FilterById(_c._udfProperties, queryText).Cast<T>().ToList());

		public override FeedIterator<T> GetUserDefinedFunctionQueryIterator<T>(
			QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryFeedIterator<T>(() => FilterById(_c._udfProperties, queryDefinition).Cast<T>().ToList());

		// ── Query iterators (stream) ──────────────────────────────────────

		public override FeedIterator GetStoredProcedureQueryStreamIterator(
			string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryStreamFeedIterator(
				() => FilterById(_c._storedProcedureProperties, queryText).Cast<object>().ToList(), "StoredProcedures", () => _c.CurrentSessionToken);

		public override FeedIterator GetStoredProcedureQueryStreamIterator(
			QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryStreamFeedIterator(
				() => FilterById(_c._storedProcedureProperties, queryDefinition).Cast<object>().ToList(), "StoredProcedures", () => _c.CurrentSessionToken);

		public override FeedIterator GetTriggerQueryStreamIterator(
			string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryStreamFeedIterator(
				() => FilterById(_c._triggerProperties, queryText).Cast<object>().ToList(), "Triggers", () => _c.CurrentSessionToken);

		public override FeedIterator GetTriggerQueryStreamIterator(
			QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryStreamFeedIterator(
				() => FilterById(_c._triggerProperties, queryDefinition).Cast<object>().ToList(), "Triggers", () => _c.CurrentSessionToken);

		public override FeedIterator GetUserDefinedFunctionQueryStreamIterator(
			string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryStreamFeedIterator(
				() => FilterById(_c._udfProperties, queryText).Cast<object>().ToList(), "UserDefinedFunctions", () => _c.CurrentSessionToken);

		public override FeedIterator GetUserDefinedFunctionQueryStreamIterator(
			QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
			=> new InMemoryStreamFeedIterator(
				() => FilterById(_c._udfProperties, queryDefinition).Cast<object>().ToList(), "UserDefinedFunctions", () => _c.CurrentSessionToken);

		// ── Private helpers ───────────────────────────────────────────────

		private static IEnumerable<TValue> FilterById<TValue>(
			Dictionary<string, TValue> dict, string queryText)
		{
			var id = ExtractIdFromQueryText(queryText);
			return id != null && dict.TryGetValue(id, out var match)
				? new[] { match }
				: dict.Values;
		}

		private static IEnumerable<TValue> FilterById<TValue>(
			Dictionary<string, TValue> dict, QueryDefinition queryDefinition)
		{
			if (queryDefinition == null) return dict.Values;
			var id = ExtractIdFromQueryText(queryDefinition.QueryText);
			if (id != null && id.StartsWith("@"))
			{
				var parameters = ExtractQueryParameters(queryDefinition);
				if (parameters.TryGetValue(id, out var paramValue))
					id = paramValue?.ToString();
				else
					return dict.Values;
			}
			return id != null && dict.TryGetValue(id, out var match)
				? new[] { match }
				: dict.Values;
		}

		private static readonly System.Text.RegularExpressions.Regex IdFilterRegex =
			new(@"WHERE\s+c\.id\s*=\s*(?:'([^']+)'|""([^""]+)""|(@\w+))",
				System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled);

		private static string ExtractIdFromQueryText(string queryText)
		{
			if (string.IsNullOrWhiteSpace(queryText)) return null;
			var m = IdFilterRegex.Match(queryText);
			if (!m.Success) return null;
			return m.Groups[1].Success ? m.Groups[1].Value
				 : m.Groups[2].Success ? m.Groups[2].Value
				 : m.Groups[3].Success ? m.Groups[3].Value
				 : null;
		}

		private string ExecuteHandler(string sprocId, Func<string> invoker)
		{
			string result;
			try
			{
				var task = Task.Run(invoker);
				if (!task.Wait(TimeSpan.FromSeconds(10)))
					throw InMemoryCosmosException.Create(
						$"Stored procedure '{sprocId}' exceeded the 10-second execution timeout.",
						HttpStatusCode.RequestTimeout, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
				result = task.Result;
			}
			catch (CosmosException) { throw; }
			catch (AggregateException ae) when (ae.InnerException is CosmosException) { throw ae.InnerException; }
			catch (AggregateException ae)
			{
				var inner = ae.InnerException ?? ae;
				throw InMemoryCosmosException.Create($"Stored procedure '{sprocId}' failed: {inner.Message}",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			}
			catch (Exception ex)
			{
				throw InMemoryCosmosException.Create($"Stored procedure '{sprocId}' failed: {ex.Message}",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			}

			const int MaxResponseSize = 2 * 1024 * 1024;
			if (result != null && Encoding.UTF8.GetByteCount(result) > MaxResponseSize)
				throw InMemoryCosmosException.Create($"Stored procedure '{sprocId}' response exceeds the 2MB size limit.",
					(HttpStatusCode)413, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			return result;
		}

		private string ExecuteJsEngine(string sprocId, string jsBody, PartitionKey pk, dynamic[] args)
		{
			try
			{
				var context = new PartitionScopedCollectionContext(_c, pk);
				return _c.SprocEngine.Execute(jsBody, pk, args, context);
			}
			catch (CosmosException) { throw; }
			catch (Exception ex)
			{
				throw InMemoryCosmosException.Create($"Stored procedure '{sprocId}' failed: {ex.Message}",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			}
		}

		private void PopulateScriptLogHeaders<TOutput>(StoredProcedureExecuteResponse<TOutput> r)
		{
			if (_c.SprocEngine?.CapturedLogs is { Count: > 0 } logs)
			{
				var logString = Uri.EscapeDataString(string.Join("\n", logs));
				var headers = new Headers
				{
					["x-ms-request-charge"] = SyntheticRequestCharge.ToString(CultureInfo.InvariantCulture),
					["x-ms-documentdb-script-log-results"] = logString
				};
				r.Headers.Returns(headers);
				r.ScriptLog.Returns(Uri.UnescapeDataString(logString));
			}
		}
	}

	private static void EnrichStoredProcedureSystemProperties(StoredProcedureProperties props)
	{
		// ETag and SelfLink are read-only on StoredProcedureProperties,
		// so we use Newtonsoft.Json to populate the internal backing fields.
		var json = JsonConvert.SerializeObject(props);
		var jObj = JObject.Parse(json);
		if (jObj["_etag"] == null || string.IsNullOrEmpty(jObj["_etag"]?.ToString()))
			jObj["_etag"] = $"\"{Guid.NewGuid()}\"";
		if (jObj["_self"] == null || string.IsNullOrEmpty(jObj["_self"]?.ToString()))
			jObj["_self"] = $"dbs/db/colls/col/sprocs/{props.Id}";
		if (jObj["_ts"] == null || jObj["_ts"]!.Type == JTokenType.Null)
			jObj["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
		if (jObj["_rid"] == null || string.IsNullOrEmpty(jObj["_rid"]?.ToString()))
			jObj["_rid"] = Convert.ToBase64String(Guid.NewGuid().ToByteArray()).TrimEnd('=');
		JsonConvert.PopulateObject(jObj.ToString(), props);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Private helpers — Query execution pipeline
	// ═══════════════════════════════════════════════════════════════════════════

	// Ref: Observed behavior on the Windows Cosmos DB Emulator (priority 6):
	//   Documents returned by queries without ORDER BY are in insertion order.
	private IEnumerable<string> GetAllItemsForPartition(QueryRequestOptions requestOptions)
	{
		List<(string Id, string PartitionKey)> orderedKeys;
		lock (_insertionOrderLock)
		{
			orderedKeys = _insertionOrder.ToList();
		}

		if (requestOptions?.PartitionKey is not null
			&& requestOptions.PartitionKey != PartitionKey.None)
		{
			var pk = PartitionKeyToString(requestOptions.PartitionKey.Value);

			// Hierarchical PK prefix match: when the query PK has fewer components
			// than the container's partition key paths, treat it as a prefix filter.
			if (PartitionKeyPaths is { Count: > 1 })
			{
				var queryComponents = CountPartitionKeyComponents(requestOptions.PartitionKey.Value);
				if (queryComponents > 0 && queryComponents < PartitionKeyPaths.Count)
				{
					var prefix = pk + "|";
					return orderedKeys
						.Where(key => (key.PartitionKey?.StartsWith(prefix, StringComparison.Ordinal) ?? false) && !IsExpired(key) && _items.ContainsKey(key))
						.Select(key => _items[key]);
				}
			}

			return orderedKeys
				.Where(key => key.PartitionKey == pk && !IsExpired(key) && _items.ContainsKey(key))
				.Select(key => _items[key]);
		}
		return orderedKeys
			.Where(key => !IsExpired(key) && _items.ContainsKey(key))
			.Select(key => _items[key]);
	}

	private static int CountPartitionKeyComponents(PartitionKey partitionKey)
	{
		try
		{
			var raw = partitionKey.ToString();
			if (raw.StartsWith("["))
			{
				var arr = JArray.Parse(raw);
				return arr.Count;
			}
			return 1;
		}
		catch { return 1; }
	}

	private const string UdfRegistryKey = "__udf_registry__";
	private const string UdfPropertiesKey = "__udf_properties__";
	private const string UdfEngineKey = "__udf_engine__";

	private (string Name, string FromAlias, SqlExpression Expr)[] GetParsedComputedProperties()
	{
		var cached = _parsedComputedProperties;
		if (cached is not null) return cached;

		var cps = _containerProperties.ComputedProperties;
		if (cps is null or { Count: 0 })
		{
			_parsedComputedProperties = Array.Empty<(string, string, SqlExpression)>();
			return _parsedComputedProperties;
		}

		var result = new (string Name, string FromAlias, SqlExpression Expr)[cps.Count];
		for (var i = 0; i < cps.Count; i++)
		{
			var parsed = CosmosSqlParser.Parse(cps[i].Query);
			if (parsed.IsValueSelect && parsed.SelectFields.Length == 1 && parsed.SelectFields[0].SqlExpr is not null)
			{
				result[i] = (cps[i].Name, parsed.FromAlias, parsed.SelectFields[0].SqlExpr);
			}
			else
			{
				// Fallback: try to evaluate via the expression string
				result[i] = (cps[i].Name, parsed.FromAlias, null);
			}
		}

		_parsedComputedProperties = result;
		return result;
	}

	private List<string> AugmentWithComputedProperties(
		IEnumerable<string> items, IDictionary<string, object> parameters)
	{
		var cps = GetParsedComputedProperties();
		if (cps.Length == 0) return items.ToList();

		return items.Select(json =>
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			foreach (var (name, fromAlias, expr) in cps)
			{
				if (expr is null) continue;
				var value = EvaluateSqlExpression(expr, jObj, fromAlias, parameters);
				if (value is UndefinedValue)
					continue;
				jObj[name] = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
			}
			return jObj.ToString(Formatting.None);
		}).ToList();
	}

	private static List<string> StripComputedProperties(
		IEnumerable<string> items, (string Name, string FromAlias, SqlExpression Expr)[] cps)
	{
		if (cps.Length == 0) return items.ToList();
		var names = new HashSet<string>(cps.Select(cp => cp.Name));
		return items.Select(json =>
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			foreach (var name in names)
				jObj.Remove(name);
			return jObj.ToString(Formatting.None);
		}).ToList();
	}

	private List<string> FilterItemsByQuery(
		string queryText, IDictionary<string, object> parameters, QueryRequestOptions requestOptions,
		IEnumerable<string> preFilteredItems = null)
	{
		// Detect ORDER BY RANK RRF(...) early — the parser may silently drop it on some runtimes
		if (System.Text.RegularExpressions.Regex.IsMatch(
				queryText, @"\bORDER\s+BY\s+RANK\s+RRF\s*\(", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
		{
			throw new NotSupportedException(
				"ORDER BY RANK RRF() is not implemented in the in-memory emulator.");
		}

		if (_userDefinedFunctions.Count > 0)
		{
			parameters[UdfRegistryKey] = _userDefinedFunctions;
		}

		if (_udfProperties.Count > 0)
			parameters[UdfPropertiesKey] = _udfProperties;
		if (JsTriggerEngine is IJsUdfEngine udfEngine)
			parameters[UdfEngineKey] = udfEngine;

		// Snapshot static datetime values so they remain constant for the entire query
		var now = DateTime.UtcNow;
		parameters["__staticDateTime"] = now.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
		parameters["__staticTicks"] = now.Ticks;
		parameters["__staticTimestamp"] = new DateTimeOffset(now).ToUnixTimeMilliseconds();

		var parsed = CosmosSqlParser.Parse(queryText);

		IEnumerable<string> items = preFilteredItems ?? GetAllItemsForPartition(requestOptions);

		// Computed properties — augment items with virtual properties before any filtering/projection
		var computedProps = GetParsedComputedProperties();
		if (computedProps.Length > 0)
		{
			items = AugmentWithComputedProperties(items, parameters);
		}

		// FROM alias IN c.field — top-level array iteration (must come before JOINs/WHERE)
		if (parsed.FromSource is not null)
		{
			items = ExpandFromSource(items, parsed);
		}

		// JOIN expansion (supports multiple JOINs) — must come before WHERE
		if (parsed.Joins is { Length: > 0 })
		{
			items = ExpandAllJoins(items, parsed);
		}
		else if (parsed.Join is not null)
		{
			items = ExpandJoinedItems(items, parsed);
		}

		// WHERE
		if (parsed.Where is not null)
		{
			items = items.Where(json =>
			{
				var jObj = JsonParseHelpers.ParseJson(json);
				return EvaluateWhereExpression(parsed.Where, jObj, parsed.FromAlias, parameters, parsed.Join);
			});
		}

		// GROUP BY / HAVING — also handles SELECT projection for grouped results
		var groupByApplied = false;
		if (parsed.GroupByFields is { Length: > 0 })
		{
			items = ApplyGroupBy(items, parsed, parameters);
			groupByApplied = true;
		}

		// ORDER BY
		if (parsed.OrderByFields is { Length: > 0 })
		{
			var effectiveOrderBy = parsed.OrderByFields;
			// After GROUP BY, aggregate ORDER BY expressions (e.g. ORDER BY COUNT(1))
			// must resolve to the alias of the matching SELECT field because the items
			// are already projected group results (e.g. {"cat":"A","cnt":2}).
			// EvaluateSqlExpression returns pass-through values for aggregates (COUNT→1),
			// which makes sorting non-deterministic when groups share the same count.
			if (groupByApplied)
			{
				effectiveOrderBy = ResolveOrderByAliasesAfterGroupBy(parsed.OrderByFields, parsed.SelectFields);
			}
			items = ApplyOrderByFields(items, effectiveOrderBy, parsed.FromAlias, parameters);
		}
		else if (parsed.OrderBy is not null)
		{
			items = ApplyOrderBy(items, parsed.OrderBy, parsed.FromAlias);
		}
		else if (parsed.RankExpression is not null)
		{
			items = ApplyOrderByRank(items, parsed.RankExpression, parsed.FromAlias, parameters);
		}

		// TOP — applied before projection only when DISTINCT is not active.
		// When DISTINCT is active, TOP is deferred until after deduplication.
		if (parsed.TopCount.HasValue && !parsed.IsDistinct)
		{
			items = items.Take(parsed.TopCount.Value);
		}

		// OFFSET / LIMIT — when DISTINCT is active, defer until after dedup
		if (!parsed.IsDistinct && parsed.Offset.HasValue)
		{
			items = items.Skip(parsed.Offset.Value);
		}

		if (!parsed.IsDistinct && parsed.Limit.HasValue)
		{
			items = items.Take(parsed.Limit.Value);
		}

		// SELECT projection (skip if GROUP BY already projected)
		if (!groupByApplied && !parsed.IsSelectAll && parsed.SelectFields.Length > 0)
		{
			items = ProjectFields(items, parsed, parameters);
		}

		// SELECT * must not include computed properties (they are virtual)
		if (parsed.IsSelectAll && computedProps.Length > 0)
		{
			items = StripComputedProperties(items, computedProps);
		}

		// DISTINCT — applied after projection so dedup works on projected shapes
		if (parsed.IsDistinct)
		{
			items = items.Distinct(JsonStructuralStringComparer.Instance);
		}

		// OFFSET / LIMIT — deferred application after DISTINCT
		if (parsed.IsDistinct && parsed.Offset.HasValue)
		{
			items = items.Skip(parsed.Offset.Value);
		}

		if (parsed.IsDistinct && parsed.Limit.HasValue)
		{
			items = items.Take(parsed.Limit.Value);
		}

		// TOP — deferred application after DISTINCT
		if (parsed.TopCount.HasValue && parsed.IsDistinct)
		{
			items = items.Take(parsed.TopCount.Value);
		}

		// VALUE SELECT — unwrap scalar values from projected JObjects
		if (parsed.IsValueSelect && parsed.SelectFields.Length == 1)
		{
			items = UnwrapValueSelect(items);
		}

		return items as List<string> ?? items.ToList();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Query helpers — ORDER BY
	// ═══════════════════════════════════════════════════════════════════════════

	private static List<string> ApplyOrderBy(IEnumerable<string> items, OrderByClause orderBy, string fromAlias)
		=> ApplyOrderByFields(items, new[] { new OrderByField(orderBy.Field, orderBy.Ascending) }, fromAlias);

	private static List<string> ApplyOrderByRank(
		IEnumerable<string> items, SqlExpression rankExpr, string fromAlias, IDictionary<string, object> parameters)
	{
		// ORDER BY RANK sorts by the evaluated expression descending (highest score first).
		return items
			.OrderByDescending(json =>
			{
				var jObj = JsonParseHelpers.ParseJson(json);
				var score = EvaluateSqlExpression(rankExpr, jObj, fromAlias, parameters);
				return score switch
				{
					double d => d,
					long l => (double)l,
					int i => (double)i,
					_ => 0.0
				};
			})
			.ToList();
	}

	// Sentinel for undefined (missing field) — distinct from null
	private static readonly object UndefinedSortSentinel = new();

	private static List<string> ApplyOrderByFields(
		IEnumerable<string> items, OrderByField[] orderByFields, string fromAlias,
		IDictionary<string, object> parameters = null)
	{
		if (orderByFields.Length == 0)
		{
			return items.ToList();
		}

		IOrderedEnumerable<string> ordered = null;
		foreach (var field in orderByFields)
		{
			object KeySelector(string json)
			{
				var jObj = JsonParseHelpers.ParseJson(json);

				if (field.Expression is not null)
				{
					var value = EvaluateSqlExpression(field.Expression, jObj, fromAlias,
						parameters ?? new Dictionary<string, object>());
					return value switch
					{
						double d => (object)d,
						long l => (object)(double)l,
						int i => (object)(double)i,
						decimal dec => (object)(double)dec,
						bool b => (object)b,
						string s => (object)s,
						UndefinedValue => UndefinedSortSentinel,
						null => null,
						JToken jt => jt,
						_ => (object)value.ToString()
					};
				}

				var fieldPath = field.Field;
				if (fieldPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
				{
					fieldPath = fieldPath[(fromAlias.Length + 1)..];
				}

				var token = jObj.SelectToken(fieldPath);
				if (token is null)
				{
					return UndefinedSortSentinel; // field missing — undefined
				}

				return token.Type switch
				{
					JTokenType.Null => null, // field present but null
					JTokenType.Boolean => (object)token.Value<bool>(),
					JTokenType.Integer => (object)token.Value<long>(),
					JTokenType.Float => (object)token.Value<double>(),
					JTokenType.String => (object)token.Value<string>(),
					_ => (object)token // Return JArray/JObject for element-by-element comparison
				};
			}

			var asc = field.Ascending;
			var comparer = Comparer<object>.Create((l, r) =>
			{
				var result = CompareValues(l, r);
				return asc ? result : -result;
			});
			ordered = ordered == null ? items.OrderBy(KeySelector, comparer) : ordered.ThenBy(KeySelector, comparer);
		}
		return ordered?.ToList() ?? items.ToList();
	}

	/// <summary>
	/// After GROUP BY projection, ORDER BY aggregate expressions (e.g. COUNT(1), SUM(c.val))
	/// must be resolved to their SELECT alias so sorting works on the projected field value
	/// instead of the aggregate passthrough (which would be 1 for COUNT, etc.).
	/// </summary>
	private static OrderByField[] ResolveOrderByAliasesAfterGroupBy(
		OrderByField[] orderByFields, SelectField[] selectFields)
	{
		var resolved = new OrderByField[orderByFields.Length];
		for (var i = 0; i < orderByFields.Length; i++)
		{
			var obf = orderByFields[i];
			if (obf.Expression is FunctionCallExpression func
				&& AggregateFunctions.Contains(func.FunctionName))
			{
				// Convert both to text form using ExprToString for consistent comparison
				var orderByText = CosmosSqlParser.ExprToString(obf.Expression);
				foreach (var sf in selectFields)
				{
					if (sf.Alias is not null && sf.Expression.Trim().Equals(
							orderByText, StringComparison.OrdinalIgnoreCase))
					{
						// Replace with a simple field lookup on the alias
						resolved[i] = new OrderByField(sf.Alias, obf.Ascending);
						break;
					}
				}

				resolved[i] ??= obf; // no matching alias found — keep original
			}
			else
			{
				resolved[i] = obf;
			}
		}

		return resolved;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Query helpers — GROUP BY / HAVING
	// ═══════════════════════════════════════════════════════════════════════════

	private List<string> ApplyGroupBy(IEnumerable<string> items, CosmosSqlQuery parsed, IDictionary<string, object> parameters)
	{
		var fromAlias = parsed.FromAlias;

		var groups = items.GroupBy(json =>
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			var keyParts = new List<string>();
			var groupByExprs = parsed.GroupByExpressions;
			for (var i = 0; i < parsed.GroupByFields.Length; i++)
			{
				// If the GROUP BY field is a function expression (e.g., LOWER(c.name)),
				// evaluate it with the expression tree rather than treating it as a path.
				if (groupByExprs != null && i < groupByExprs.Length
					&& groupByExprs[i] is not IdentifierExpression
					&& groupByExprs[i] is not null)
				{
					var val = EvaluateSqlExpression(groupByExprs[i], jObj, fromAlias,
						parameters ?? new Dictionary<string, object>());
					keyParts.Add(val is UndefinedValue ? "\x00undefined" : val?.ToString() ?? "null");
				}
				else
				{
					var path = StripAliasPrefix(parsed.GroupByFields[i], fromAlias);
					var token = jObj.SelectToken(path);
					// Distinguish null (JTokenType.Null) from undefined (missing property)
					if (token is null)
						keyParts.Add("\x00undefined");
					else if (token.Type == JTokenType.Null)
						keyParts.Add("\x00null");
					else
						keyParts.Add(token.ToString());
				}
			}
			return string.Join("\x1F", keyParts.Select(k => k.Replace("\x1F", "\x1F\x1F")));
		});

		var hasAggregate = parsed.SelectFields.Any(f =>
		{
			var expr = f.Expression.TrimStart();
			if (AggregateFunctions.Any(fn => expr.StartsWith(fn + "(", StringComparison.OrdinalIgnoreCase)))
				return true;
			// Also detect aggregates nested inside object/array literals (e.g. SELECT VALUE {"cnt": COUNT(1)})
			if (f.SqlExpr is not null)
				return ContainsAggregateCall(f.SqlExpr);
			return false;
		});

		if (!hasAggregate)
		{
			return groups.Select(g =>
			{
				var jObj = JsonParseHelpers.ParseJson(g.First());
				var projected = new JObject();
				foreach (var field in parsed.SelectFields)
				{
					var outputName = field.Alias ?? field.Expression.Split('.').Last();

					// If the select field has a non-trivial expression (function call, etc.),
					// evaluate it rather than treating it as a property path.
					if (field.SqlExpr is not null and not IdentifierExpression)
					{
						var val = EvaluateSqlExpression(field.SqlExpr, jObj, fromAlias,
							parameters ?? new Dictionary<string, object>());
						projected[outputName] = val is not null ? JToken.FromObject(val) : JValue.CreateNull();
					}
					else
					{
						var path = StripAliasPrefix(field.Expression, fromAlias);
						projected[outputName] = jObj.SelectToken(path)?.DeepClone();
					}
				}
				return projected.ToString(Formatting.None);
			}).ToList();
		}

		var result = new List<string>();
		foreach (var group in groups)
		{
			var groupItems = group.ToList();
			var resultObj = new JObject();

			foreach (var field in parsed.SelectFields)
			{
				var expr = field.Expression.TrimStart();
				string funcName = null;
				string innerArg = null;

				foreach (var fn in AggregateFunctions)
				{
					if (expr.StartsWith(fn + "(", StringComparison.OrdinalIgnoreCase))
					{
						funcName = fn;
						var open = expr.IndexOf('(');
						var close = expr.LastIndexOf(')');
						if (open >= 0 && close > open)
						{
							innerArg = expr.Substring(open + 1, close - open - 1).Trim();
						}

						break;
					}
				}

				var outputName = field.Alias ?? field.Expression;

				if (funcName == "COUNT")
				{
					if (innerArg is "1" or "*" or null)
					{
						resultObj[outputName] = groupItems.Count;
					}
					else
					{
						var countPath = innerArg;
						if (countPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
							countPath = countPath[(fromAlias.Length + 1)..];
						resultObj[outputName] = groupItems.Count(json =>
						{
							var jObj = JsonParseHelpers.ParseJson(json);
							return jObj.SelectToken(countPath) is JToken t && t.Type != JTokenType.Null;
						});
					}
				}
				// Ref: https://github.com/Azure/azure-cosmos-dotnet-v3/pull/4738
				//   COUNTIF(<bool_expr>) counts items where the expression evaluates to true.
				else if (funcName == "COUNTIF" && field.SqlExpr is FunctionCallExpression countIfFunc)
				{
					resultObj[outputName] = EvaluateCountIf(countIfFunc, groupItems, fromAlias, parameters);
				}
				else if (funcName is "SUM" or "AVG" && innerArg != null)
				{
					var values = ExtractNumericValues(groupItems, innerArg, fromAlias, parameters);
					if (funcName == "SUM")
					{
						if (values.Count > 0)
							resultObj[outputName] = NormalizeNumericResult(values.Sum());
						// else: omit field entirely (undefined) — matches Cosmos DB
					}
					else // AVG
					{
						if (values.Count > 0)
							resultObj[outputName] = NormalizeNumericResult(values.Average());
					}
				}
				else if (funcName is "MIN" or "MAX" && innerArg != null)
				{
					var tokens = ExtractTokenValues(groupItems, innerArg, fromAlias, parameters);
					var minMaxResult = AggregateMinMax(tokens, funcName == "MIN");
					if (minMaxResult is not UndefinedValue)
						resultObj[outputName] = JToken.FromObject(minMaxResult);
				}
				else
				{
					var jObj = JsonParseHelpers.ParseJson(groupItems[0]);

					// If the select field has a non-trivial expression (function call, etc.),
					// evaluate it rather than treating it as a property path.
					if (field.SqlExpr is not null and not IdentifierExpression && funcName is null)
					{
						var fieldOutputName = field.Alias ?? field.Expression;
						// Use aggregate-aware evaluation if the expression contains nested aggregates
						// (e.g. SELECT VALUE {"cnt": COUNT(1), "total": SUM(c.val)})
						var val = ContainsAggregateCall(field.SqlExpr)
							? EvaluateGroupByProjectionExpression(field.SqlExpr, groupItems, jObj, fromAlias, parameters)
							: EvaluateSqlExpression(field.SqlExpr, jObj, fromAlias,
								parameters ?? new Dictionary<string, object>());
						resultObj[fieldOutputName] = val is not null ? JToken.FromObject(val) : JValue.CreateNull();
					}
					else
					{
						var path = StripAliasPrefix(field.Expression, fromAlias);
						var fieldOutputName = field.Alias ?? path.Split('.').Last();
						resultObj[fieldOutputName] = jObj.SelectToken(path)?.DeepClone();
					}
				}
			}

			if (parsed.Having is not null)
			{
				if (!EvaluateHavingCondition(parsed.Having, groupItems, resultObj, fromAlias, parameters))
				{
					continue;
				}
			}

			result.Add(resultObj.ToString(Formatting.None));
		}
		return result;
	}

	private static bool IsComplexExpression(string arg)
		=> arg.Contains('(') || arg.Contains('*') || arg.Contains('+') ||
		   arg.Contains('/') || arg.Contains('%') ||
		   (arg.Contains('-') && !arg.StartsWith("-") && arg.IndexOf('-') != arg.IndexOf(".") + 1);

	/// <summary>
	/// Counts items for which <paramref name="innerExpr"/> evaluates to a defined,
	/// non-null value. Matches Cosmos DB <c>COUNT(expr)</c> semantics: rows producing
	/// <c>undefined</c> or <c>null</c> are excluded.
	/// </summary>
	private static int CountDefinedResults(
		SqlExpression innerExpr, List<string> items, string fromAlias, IDictionary<string, object> parameters)
	{
		var count = 0;
		foreach (var json in items)
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			var result = EvaluateSqlExpression(innerExpr, jObj, fromAlias,
				parameters ?? new Dictionary<string, object>());
			if (result is null or UndefinedValue)
				continue;
			if (result is JToken jt && jt.Type == JTokenType.Null)
				continue;
			count++;
		}
		return count;
	}

	private static List<double> ExtractNumericValues(IEnumerable<string> items, string innerArg, string fromAlias, IDictionary<string, object> parameters = null)
	{
		var values = new List<double>();

		// Handle numeric literals (e.g., SUM(1), AVG(2.5))
		if (double.TryParse(innerArg, NumberStyles.Any, CultureInfo.InvariantCulture, out var literalValue))
		{
			values.AddRange(Enumerable.Repeat(literalValue, items.Count()));
			return values;
		}

		var isExpr = IsComplexExpression(innerArg);
		SqlExpression parsedInnerExpr = null;
		if (isExpr)
		{
			CosmosSqlParser.TryParse($"SELECT VALUE {innerArg} FROM {fromAlias}", out var innerParsed);
			if (innerParsed?.SelectFields.Length > 0)
				parsedInnerExpr = innerParsed.SelectFields[0].SqlExpr;
		}

		foreach (var json in items)
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			double? val = null;

			if (parsedInnerExpr is not null)
			{
				var result = EvaluateSqlExpression(parsedInnerExpr, jObj, fromAlias,
					parameters ?? new Dictionary<string, object>());
				if (result is double d)
					val = d;
				else if (result is long l)
					val = l;
				else if (result != null && double.TryParse(result.ToString(), NumberStyles.Any,
					CultureInfo.InvariantCulture, out var parsed2))
					val = parsed2;
			}
			else
			{
				var path = StripAliasPrefix(innerArg, fromAlias);
				var token = jObj.SelectToken(path);
				if (token != null && double.TryParse(token.ToString(), NumberStyles.Any,
						CultureInfo.InvariantCulture, out var parsed3))
					val = parsed3;
			}

			if (val.HasValue)
				values.Add(val.Value);
		}
		return values;
	}

	private static List<JToken> ExtractTokenValues(IEnumerable<string> items, string innerArg, string fromAlias, IDictionary<string, object> parameters = null)
	{
		var tokens = new List<JToken>();

		// Handle numeric literals (e.g., MIN(1), MAX(2.5))
		if (double.TryParse(innerArg, NumberStyles.Any, CultureInfo.InvariantCulture, out var literalValue))
		{
			var token = new JValue(literalValue);
			tokens.AddRange(Enumerable.Repeat((JToken)token, items.Count()));
			return tokens;
		}

		var isExpr = IsComplexExpression(innerArg);
		SqlExpression parsedInnerExpr = null;
		if (isExpr)
		{
			CosmosSqlParser.TryParse($"SELECT VALUE {innerArg} FROM {fromAlias}", out var innerParsed);
			if (innerParsed?.SelectFields.Length > 0)
				parsedInnerExpr = innerParsed.SelectFields[0].SqlExpr;
		}

		foreach (var json in items)
		{
			var jObj = JsonParseHelpers.ParseJson(json);

			if (parsedInnerExpr is not null)
			{
				var result = EvaluateSqlExpression(parsedInnerExpr, jObj, fromAlias,
					parameters ?? new Dictionary<string, object>());
				if (result is not null and not UndefinedValue)
					tokens.Add(JToken.FromObject(result));
			}
			else
			{
				var path = StripAliasPrefix(innerArg, fromAlias);
				var token = jObj.SelectToken(path);
				if (token != null && token.Type != JTokenType.Null)
					tokens.Add(token);
			}
		}
		return tokens;
	}

	/// <summary>
	/// Computes MIN or MAX across a list of JTokens, respecting Cosmos DB type ordering:
	/// boolean &lt; number &lt; string. Booleans compare as false &lt; true.
	/// Returns <see cref="UndefinedValue.Instance"/> when <paramref name="tokens"/> is empty.
	/// </summary>
	private static object AggregateMinMax(List<JToken> tokens, bool isMin)
	{
		if (tokens.Count == 0)
			return UndefinedValue.Instance;

		// Cosmos DB MIN/MAX type rank: boolean(0) < number(1) < string(2)
		// MIN returns the value with the lowest rank; within a rank, the smallest value.
		// MAX returns the value with the highest rank; within a rank, the largest value.
		static int MinMaxTypeRank(JToken t) => t.Type switch
		{
			JTokenType.Boolean => 0,
			JTokenType.Integer or JTokenType.Float => 1,
			JTokenType.String => 2,
			_ => 3 // other types ignored below
		};

		var eligible = tokens.Where(t => MinMaxTypeRank(t) <= 2).ToList();
		if (eligible.Count == 0)
			return UndefinedValue.Instance;

		JToken best = eligible[0];
		foreach (var t in eligible.Skip(1))
		{
			var rankBest = MinMaxTypeRank(best);
			var rankT = MinMaxTypeRank(t);
			bool tIsBetter;
			if (rankBest != rankT)
			{
				tIsBetter = isMin ? rankT < rankBest : rankT > rankBest;
			}
			else
			{
				tIsBetter = rankBest switch
				{
					0 => isMin
						? !t.Value<bool>() && best.Value<bool>()   // false < true
						: t.Value<bool>() && !best.Value<bool>(),
					1 => isMin
						? t.Value<double>() < best.Value<double>()
						: t.Value<double>() > best.Value<double>(),
					2 => isMin
						? string.Compare(t.Value<string>(), best.Value<string>(), StringComparison.Ordinal) < 0
						: string.Compare(t.Value<string>(), best.Value<string>(), StringComparison.Ordinal) > 0,
					_ => false
				};
			}
			if (tIsBetter) best = t;
		}

		return best.Type switch
		{
			JTokenType.Boolean => best.Value<bool>(),
			JTokenType.Integer => best.Value<long>(),
			JTokenType.Float => best.Value<double>(),
			JTokenType.String => best.Value<string>(),
			_ => UndefinedValue.Instance
		};
	}

	// Ref: JavaScript: JSON.stringify(750.0) === "750"
	//   Real Cosmos DB uses a JavaScript engine which normalises whole-number aggregate results
	//   to integers. Storing the result as JValue(double) causes platform-dependent serialisation
	//   ("750.0" on Linux, "750" on Windows). We explicitly emit a long-backed JValue for whole
	//   numbers to guarantee consistent cross-platform output.
	private static JToken NormalizeNumericResult(double value)
	{
		if (!double.IsNaN(value) && !double.IsInfinity(value) && value == Math.Truncate(value))
			return new JValue((long)value);
		return new JValue(value);
	}

	private static bool EvaluateHavingCondition(
		WhereExpression having, List<string> groupItems, JObject resultObj,
		string fromAlias, IDictionary<string, object> parameters)
	{
		if (having is SqlExpressionCondition sec)
		{
			return IsTruthy(EvaluateHavingSqlExpression(sec.Expression, groupItems, resultObj, fromAlias, parameters));
		}

		return EvaluateWhereExpression(having, resultObj, fromAlias, parameters, null);
	}

	/// <summary>
	/// Strips the FROM alias prefix from a property path, handling both
	/// dot notation (c.name) and bracket notation (c["name"]) from the LINQ provider.
	/// </summary>
	private static string StripAliasPrefix(string path, string fromAlias)
	{
		if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
			return path[(fromAlias.Length + 1)..];
		if (path.StartsWith(fromAlias + "[", StringComparison.OrdinalIgnoreCase))
		{
			var bracketPart = path[fromAlias.Length..]; // ["name"] or ['name']
			if (bracketPart.StartsWith("[\"") && bracketPart.EndsWith("\"]"))
				return bracketPart[2..^2]; // Extract property name from ["prop"]
			if (bracketPart.StartsWith("['") && bracketPart.EndsWith("']"))
				return bracketPart[2..^2]; // Extract property name from ['prop']
			return bracketPart;
		}
		return path;
	}

	private static object EvaluateHavingSqlExpression(
		SqlExpression expr, List<string> groupItems, JObject resultObj,
		string fromAlias, IDictionary<string, object> parameters)
	{
		return expr switch
		{
			FunctionCallExpression func when AggregateFunctions.Contains(func.FunctionName) =>
				EvaluateHavingAggregate(func, groupItems, fromAlias, parameters),
			BinaryExpression bin => EvaluateHavingBinaryExpression(bin, groupItems, resultObj, fromAlias, parameters),
			LiteralExpression lit => lit.Value,
			UndefinedLiteralExpression => UndefinedValue.Instance,
			IdentifierExpression ident => ResolveValue(ident.Name, resultObj, fromAlias, parameters),
			_ => EvaluateSqlExpression(expr, resultObj, fromAlias, parameters)
		};
	}

	private static object EvaluateHavingBinaryExpression(
		BinaryExpression bin, List<string> groupItems, JObject resultObj,
		string fromAlias, IDictionary<string, object> parameters)
	{
		var left = EvaluateHavingSqlExpression(bin.Left, groupItems, resultObj, fromAlias, parameters);
		var right = EvaluateHavingSqlExpression(bin.Right, groupItems, resultObj, fromAlias, parameters);
		return bin.Operator switch
		{
			BinaryOp.GreaterThan => CompareValues(left, right) > 0,
			BinaryOp.GreaterThanOrEqual => CompareValues(left, right) >= 0,
			BinaryOp.LessThan => CompareValues(left, right) < 0,
			BinaryOp.LessThanOrEqual => CompareValues(left, right) <= 0,
			BinaryOp.Equal => (object)ValuesEqual(left, right),
			BinaryOp.NotEqual => !ValuesEqual(left, right),
			BinaryOp.And => IsTruthy(left) && IsTruthy(right),
			BinaryOp.Or => IsTruthy(left) || IsTruthy(right),
			BinaryOp.Add => ArithmeticOp(left, right, (a, b) => a + b),
			BinaryOp.Subtract => ArithmeticOp(left, right, (a, b) => a - b),
			BinaryOp.Multiply => ArithmeticOp(left, right, (a, b) => a * b),
			BinaryOp.Divide => ArithmeticOp(left, right, (a, b) => b != 0 ? a / b : double.NaN),
			BinaryOp.Modulo => ArithmeticOp(left, right, (a, b) => b != 0 ? a % b : double.NaN),
			_ => null
		};
	}

	/// <summary>
	/// Evaluates COUNTIF(<bool_expr>) — counts items where the boolean expression evaluates to true.
	/// Ref: https://github.com/Azure/azure-cosmos-dotnet-v3/pull/4738
	///   "Adds an aggregate operator for CountIf" — undocumented server-side aggregate.
	/// </summary>
	private static int EvaluateCountIf(
		FunctionCallExpression func, List<string> items, string fromAlias,
		IDictionary<string, object> parameters)
	{
		if (func.Arguments.Length < 1) return 0;
		var boolExpr = func.Arguments[0];
		return items.Count(json =>
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			var val = EvaluateSqlExpression(boolExpr, jObj, fromAlias,
				parameters ?? new Dictionary<string, object>());
			return IsTruthy(val);
		});
	}

	private static object EvaluateHavingAggregate(
		FunctionCallExpression func, List<string> groupItems, string fromAlias,
		IDictionary<string, object> parameters = null)
	{
		switch (func.FunctionName)
		{
			case "COUNT":
				{
					// COUNT(1) / COUNT(*) — count all items in group
					// COUNT(c.field) — count only items where field is defined
					if (func.Arguments.Length < 1)
						return (double)groupItems.Count;
					var countArg = func.Arguments[0] is IdentifierExpression countIdent ? countIdent.Name : "1";
					if (countArg is "1" or "*")
						return (double)groupItems.Count;
					var countPath = countArg;
					if (countPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
						countPath = countPath[(fromAlias.Length + 1)..];
					return (double)groupItems.Count(json =>
					{
						var jObj = JsonParseHelpers.ParseJson(json);
						return jObj.SelectToken(countPath) is JToken t && t.Type != JTokenType.Null;
					});
				}
			// Ref: https://github.com/Azure/azure-cosmos-dotnet-v3/pull/4738
			//   COUNTIF(<bool_expr>) counts items where the expression evaluates to true.
			case "COUNTIF":
				return (double)EvaluateCountIf(func, groupItems, fromAlias, parameters);
			case "SUM":
			case "AVG":
			case "MIN":
			case "MAX":
				{
					if (func.Arguments.Length < 1)
					{
						return 0.0;
					}

					var innerArg = func.Arguments[0] is IdentifierExpression ident ? ident.Name : "1";
					var values = ExtractNumericValues(groupItems, innerArg, fromAlias, parameters);
					if (func.FunctionName is "MIN" or "MAX")
					{
						var tokens = ExtractTokenValues(groupItems, innerArg, fromAlias, parameters);
						return AggregateMinMax(tokens, func.FunctionName == "MIN");
					}
					return func.FunctionName switch
					{
						"SUM" => values.Count > 0 ? (object)NormalizeNumericResult(values.Sum()) : UndefinedValue.Instance,
						"AVG" => values.Count > 0 ? (object)NormalizeNumericResult(values.Average()) : UndefinedValue.Instance,
						_ => 0.0
					};
				}
			default: return null;
		}
	}

	/// <summary>
	/// Evaluates a SELECT expression within GROUP BY context, resolving aggregate calls
	/// (COUNT, COUNTIF, SUM, AVG, MIN, MAX) against the group items rather than treating them as passthroughs.
	/// Used for expressions like: SELECT VALUE {"cnt": COUNT(1), "total": SUM(c.val)}
	/// </summary>
	private static object EvaluateGroupByProjectionExpression(
		SqlExpression expr, List<string> groupItems, JObject sampleItem,
		string fromAlias, IDictionary<string, object> parameters)
	{
		return expr switch
		{
			FunctionCallExpression func when AggregateFunctions.Contains(func.FunctionName)
				=> EvaluateHavingAggregate(func, groupItems, fromAlias, parameters),
			ObjectLiteralExpression obj => EvaluateGroupByObjectLiteral(obj, groupItems, sampleItem, fromAlias, parameters),
			ArrayLiteralExpression arr => arr.Elements
				.Select(e => EvaluateGroupByProjectionExpression(e, groupItems, sampleItem, fromAlias, parameters))
				.Where(v => v is not UndefinedValue)
				.Select(v => v is not null ? JToken.FromObject(v) : JValue.CreateNull())
				.ToArray(),
			_ => EvaluateSqlExpression(expr, sampleItem, fromAlias,
				parameters ?? new Dictionary<string, object>())
		};
	}

	private static JObject EvaluateGroupByObjectLiteral(
		ObjectLiteralExpression obj, List<string> groupItems, JObject sampleItem,
		string fromAlias, IDictionary<string, object> parameters)
	{
		var result = new JObject();
		foreach (var prop in obj.Properties)
		{
			var val = EvaluateGroupByProjectionExpression(prop.Value, groupItems, sampleItem, fromAlias, parameters);
			if (val is not UndefinedValue)
				result[prop.Key] = val is not null ? JToken.FromObject(val) : JValue.CreateNull();
		}
		return result;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Query helpers — JOIN expansion
	// ═══════════════════════════════════════════════════════════════════════════

	private static List<string> ExpandJoinedItems(IEnumerable<string> items, CosmosSqlQuery parsed)
	{
		if (parsed.Join is null)
		{
			return items.ToList();
		}

		var expanded = new List<string>();
		foreach (var json in items)
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			var arrayToken = jObj.SelectToken(parsed.Join.ArrayField);
			if (arrayToken is JArray jArray)
			{
				foreach (var element in jArray)
				{
					var combined = new JObject(jObj.Properties())
					{
						[parsed.Join.Alias] = element
					};
					expanded.Add(combined.ToString(Formatting.None));
				}
			}
		}
		return expanded;
	}

	private static List<string> ExpandAllJoins(IEnumerable<string> items, CosmosSqlQuery parsed)
	{
		var current = items;
		foreach (var join in parsed.Joins)
		{
			var expanded = new List<string>();
			foreach (var json in current)
			{
				var jObj = JsonParseHelpers.ParseJson(json);
				// Resolve the array from the correct source alias (e.g. "g.tags" for JOIN t IN g.tags)
				var sourcePath = join.SourceAlias == parsed.FromAlias
					? join.ArrayField
					: $"{join.SourceAlias}.{join.ArrayField}";
				var arrayToken = jObj.SelectToken(sourcePath);
				if (arrayToken is JArray jArray)
				{
					foreach (var element in jArray)
					{
						var combined = new JObject(jObj.Properties())
						{
							[join.Alias] = element
						};
						expanded.Add(combined.ToString(Formatting.None));
					}
				}
			}
			current = expanded;
		}
		return current as List<string> ?? current.ToList();
	}

	/// <summary>
	/// Expands top-level <c>FROM alias IN c.field</c> — each array element becomes a result row.
	/// </summary>
	private static List<string> ExpandFromSource(IEnumerable<string> items, CosmosSqlQuery parsed)
	{
		var sourcePath = parsed.FromSource;
		// The FromSource is the full dotted path (e.g. "c.tags"). We need to resolve it
		// relative to each document, but the outer alias isn't available here (the FROM clause
		// redefines the alias). Use a simple heuristic: strip the first segment if it looks
		// like an alias (contains a dot).
		var dotIndex = sourcePath.IndexOf('.');
		var arrayPath = dotIndex >= 0 ? sourcePath[(dotIndex + 1)..] : sourcePath;

		var expanded = new List<string>();
		foreach (var json in items)
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			var arrayToken = jObj.SelectToken(arrayPath);
			if (arrayToken is not JArray jArray)
				continue;

			foreach (var element in jArray)
			{
				// The alias is the range variable (e.g. "item" in FROM item IN c.items).
				// - For object elements: spread properties at root so "item.price" → strip alias → "price" resolves.
				// - Also keep the element under the alias for "SELECT item" / "SELECT VALUE item".
				var combined = element is JObject elementObj
					? new JObject(elementObj.Properties()) { [parsed.FromAlias] = element.DeepClone() }
					: new JObject { [parsed.FromAlias] = element };
				expanded.Add(combined.ToString(Formatting.None));
			}
		}
		return expanded;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Query helpers — SELECT projection
	// ═══════════════════════════════════════════════════════════════════════════

	private static List<string> ProjectFields(IEnumerable<string> items, CosmosSqlQuery parsed, IDictionary<string, object> parameters)
	{
		var hasAggregate = parsed.SelectFields.Any(f =>
		{
			var expr = f.Expression.TrimStart();
			if (AggregateFunctions.Any(fn => expr.StartsWith(fn + "(", StringComparison.OrdinalIgnoreCase)))
				return true;
			// Also detect aggregates nested inside object/array literals (e.g. SELECT VALUE {"cnt": COUNT(1)})
			if (f.SqlExpr is not null)
				return ContainsAggregateCall(f.SqlExpr);
			return false;
		});

		if (hasAggregate)
		{
			return ProjectAggregateFields(items, parsed, parameters);
		}

		var projected = new List<string>();
		foreach (var json in items)
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			var resultObj = new JObject();
			foreach (var field in parsed.SelectFields)
			{
				// Handle SELECT * combined with other fields (e.g. SELECT *, expr AS alias)
				if (field.Expression == "*")
				{
					foreach (var prop in jObj.Properties())
						resultObj[prop.Name] = prop.Value.DeepClone();
					continue;
				}

				var outputName = field.Alias ?? field.Expression.Split('.').Last();

				if (field.SqlExpr is not null and not IdentifierExpression)
				{
					var value = EvaluateSqlExpression(field.SqlExpr, jObj, parsed.FromAlias, parameters);
					if (value is not UndefinedValue)
						resultObj[outputName] = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
				}
				else
				{
					var path = field.Expression;

					// When the expression is exactly the FROM alias (e.g., SELECT VALUE root),
					// return the entire document rather than looking for a property named "root".
					// Exception: for FROM alias IN c.field, the alias is a property on the expanded
					// JObject — use the alias value (the array element) instead of the whole doc.
					if (string.Equals(path, parsed.FromAlias, StringComparison.OrdinalIgnoreCase))
					{
						var aliasToken = jObj[parsed.FromAlias];
						if (parsed.FromSource is not null && aliasToken is not null)
						{
							resultObj[outputName] = aliasToken.DeepClone();
						}
						else
						{
							resultObj[outputName] = jObj.DeepClone();
						}
						continue;
					}

					if (path.StartsWith(parsed.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
					{
						path = path[(parsed.FromAlias.Length + 1)..];
					}

					var token = jObj.SelectToken(path);
					outputName = field.Alias ?? path.Split('.').Last();
					if (token is not null)
						resultObj[outputName] = token.DeepClone();
				}
			}
			projected.Add(resultObj.ToString(Formatting.None));
		}
		return projected;
	}

	private static List<string> UnwrapValueSelect(IEnumerable<string> items)
	{
		var unwrapped = new List<string>();
		foreach (var json in items)
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			if (!jObj.HasValues)
			{
				// Empty object means the projected value was undefined — skip (omit from results)
				continue;
			}

			var first = jObj.Properties().FirstOrDefault();
			if (first?.Value is not null)
			{
				var val = first.Value;
				if (val.Type == JTokenType.String)
				{
					unwrapped.Add(JsonConvert.SerializeObject(val.Value<string>()));
				}
				else
				{
					unwrapped.Add(val.ToString(Formatting.None));
				}
			}
			else
			{
				unwrapped.Add("null");
			}
		}
		return unwrapped;
	}

	private static List<string> ProjectAggregateFields(IEnumerable<string> itemsEnumerable, CosmosSqlQuery parsed, IDictionary<string, object> parameters = null)
	{
		// Aggregates need multiple passes (Count, Sum, indexing) — materialize once
		var items = itemsEnumerable as List<string> ?? itemsEnumerable.ToList();
		var resultObj = new JObject();
		foreach (var field in parsed.SelectFields)
		{
			var expr = field.Expression.TrimStart();
			var outputName = field.Alias ?? field.Expression;

			// Prefer SqlExpr-based evaluation when the expression contains aggregates
			// but is NOT a direct aggregate call (e.g., ternary/binary wrapping an aggregate).
			// This prevents string-based matching from incorrectly extracting a partial aggregate.
			if (field.SqlExpr is not null && ContainsAggregateCall(field.SqlExpr)
				&& field.SqlExpr is not FunctionCallExpression directAgg)
			{
				var val = EvaluateAggregateExpression(field.SqlExpr, items, parsed.FromAlias, parameters ?? new Dictionary<string, object>());
				if (val is not null and not UndefinedValue)
					resultObj[outputName] = val is JToken jt ? jt.DeepClone() : JToken.FromObject(val);
				continue;
			}

			string funcName = null;
			string innerArg = null;

			foreach (var fn in AggregateFunctions)
			{
				if (expr.StartsWith(fn + "(", StringComparison.OrdinalIgnoreCase))
				{
					funcName = fn.ToUpperInvariant();
					var open = expr.IndexOf('(');
					var close = expr.LastIndexOf(')');
					if (open >= 0 && close > open)
					{
						innerArg = expr.Substring(open + 1, close - open - 1).Trim();
					}

					break;
				}
			}

			if (funcName == "COUNT")
			{
				if (innerArg is "1" or "*" or null)
				{
					resultObj[outputName] = items.Count;
				}
				else if (field.SqlExpr is FunctionCallExpression countFunc && countFunc.Arguments.Length > 0)
				{
					resultObj[outputName] = CountDefinedResults(
						countFunc.Arguments[0], items, parsed.FromAlias,
						parameters ?? new Dictionary<string, object>());
				}
				else
				{
					var countPath = innerArg;
					if (countPath.StartsWith(parsed.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
						countPath = countPath[(parsed.FromAlias.Length + 1)..];
					resultObj[outputName] = items.Count(json =>
					{
						var jObj = JsonParseHelpers.ParseJson(json);
						return jObj.SelectToken(countPath) is JToken t && t.Type != JTokenType.Null;
					});
				}
			}
			// Ref: https://github.com/Azure/azure-cosmos-dotnet-v3/pull/4738
			//   COUNTIF(<bool_expr>) counts items where the expression evaluates to true.
			else if (funcName == "COUNTIF" && field.SqlExpr is FunctionCallExpression countIfFunc)
			{
				resultObj[outputName] = EvaluateCountIf(countIfFunc, items, parsed.FromAlias, parameters);
			}
			else if (funcName is "SUM" or "AVG" && innerArg != null)
			{
				var values = ExtractNumericValues(items, innerArg, parsed.FromAlias, parameters);
				if (funcName == "SUM")
				{
					if (values.Count > 0)
						resultObj[outputName] = NormalizeNumericResult(values.Sum());
					// else: omit field entirely (undefined) — matches Cosmos DB
				}
				else // AVG
				{
					if (values.Count > 0)
						resultObj[outputName] = NormalizeNumericResult(values.Average());
					// else: omit field entirely (undefined)
				}
			}
			else if (funcName is "MIN" or "MAX" && innerArg != null)
			{
				var tokens = ExtractTokenValues(items, innerArg, parsed.FromAlias, parameters);
				var minMaxResult = AggregateMinMax(tokens, funcName == "MIN");
				if (minMaxResult is not UndefinedValue)
					resultObj[outputName] = JToken.FromObject(minMaxResult);
			}
			else if (field.SqlExpr is not null && ContainsAggregateCall(field.SqlExpr))
			{
				// Handle aggregates inside object/array literals (e.g. SELECT VALUE {"cnt": COUNT(1), "total": SUM(c.val)})
				var val = EvaluateAggregateExpression(field.SqlExpr, items, parsed.FromAlias, parameters ?? new Dictionary<string, object>());
				if (val is not null and not UndefinedValue)
					resultObj[outputName] = val is JToken jt ? jt.DeepClone() : JToken.FromObject(val);
			}
			// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/select
			//   "The SELECT clause supports arbitrary expressions including literal values."
			// Handle non-aggregate, non-trivial expressions (literals, function calls, etc.)
			// by evaluating the SqlExpr directly instead of treating as a property path.
			else if (field.SqlExpr is not null and not IdentifierExpression)
			{
				var jObj = items.Count > 0 ? JsonParseHelpers.ParseJson(items[0]) : new JObject();
				var val = EvaluateSqlExpression(field.SqlExpr, jObj, parsed.FromAlias,
					parameters ?? new Dictionary<string, object>());
				if (val is not null and not UndefinedValue)
					resultObj[outputName] = val is JToken jt ? jt.DeepClone() : JToken.FromObject(val);
				else if (val is null)
					resultObj[outputName] = JValue.CreateNull();
			}
			else
			{
				var path = field.Expression;
				if (path.StartsWith(parsed.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
				{
					path = path[(parsed.FromAlias.Length + 1)..];
				}

				if (items.Count > 0)
				{
					var jObj = JsonParseHelpers.ParseJson(items[0]);
					resultObj[outputName] = jObj.SelectToken(path)?.DeepClone();
				}
			}
		}
		return new List<string> { resultObj.ToString(Formatting.None) };
	}

	/// <summary>
	/// Evaluates a SqlExpression that may contain aggregate function calls,
	/// resolving them globally across all items. Used for expressions like
	/// SELECT VALUE {"cnt": COUNT(1), "total": SUM(c.value)} FROM c.
	/// </summary>
	private static object EvaluateAggregateExpression(
		SqlExpression expr, IEnumerable<string> itemsEnumerable, string fromAlias, IDictionary<string, object> parameters)
	{
		// Aggregates need multiple passes — materialize once
		var items = itemsEnumerable as List<string> ?? itemsEnumerable.ToList();
		switch (expr)
		{
			case FunctionCallExpression func when AggregateFunctions.Contains(func.FunctionName):
				{
					var innerArg = func.Arguments.Length > 0 ? CosmosSqlParser.ExprToString(func.Arguments[0]) : "1";
					return func.FunctionName.ToUpperInvariant() switch
					{
						"COUNT" when innerArg is "1" or "*" => (object)items.Count,
						"COUNT" => (object)CountDefinedResults(func.Arguments[0], items, fromAlias, parameters),
						// Ref: https://github.com/Azure/azure-cosmos-dotnet-v3/pull/4738
						//   COUNTIF(<bool_expr>) counts items where the expression evaluates to true.
						"COUNTIF" => (object)EvaluateCountIf(func, items, fromAlias, parameters),
						"SUM" => ExtractNumericValues(items, innerArg, fromAlias, parameters) is var sv && sv.Count > 0 ? (object)NormalizeNumericResult(sv.Sum()) : UndefinedValue.Instance,
						"AVG" => ExtractNumericValues(items, innerArg, fromAlias, parameters) is var av && av.Count > 0 ? (object)NormalizeNumericResult(av.Average()) : UndefinedValue.Instance,
						"MIN" => AggregateMinMax(ExtractTokenValues(items, innerArg, fromAlias, parameters), true),
						"MAX" => AggregateMinMax(ExtractTokenValues(items, innerArg, fromAlias, parameters), false),
						_ => null
					};
				}
			case ObjectLiteralExpression obj:
				{
					var result = new JObject();
					foreach (var prop in obj.Properties)
					{
						var val = EvaluateAggregateExpression(prop.Value, items, fromAlias, parameters);
						if (val is not null and not UndefinedValue)
							result[prop.Key] = val is JToken jt ? jt.DeepClone() : JToken.FromObject(val);
					}
					return result;
				}
			case ArrayLiteralExpression arr:
				{
					var result = new JArray();
					foreach (var element in arr.Elements)
					{
						var val = EvaluateAggregateExpression(element, items, fromAlias, parameters);
						result.Add(val is JToken jt ? jt.DeepClone() : val is not null and not UndefinedValue ? JToken.FromObject(val) : JValue.CreateNull());
					}
					return result;
				}
			case BinaryExpression bin:
				{
					var left = EvaluateAggregateExpression(bin.Left, items, fromAlias, parameters);
					var right = EvaluateAggregateExpression(bin.Right, items, fromAlias, parameters);
					return bin.Operator switch
					{
						BinaryOp.GreaterThan => (object)(CompareValues(left, right) > 0),
						BinaryOp.GreaterThanOrEqual => CompareValues(left, right) >= 0,
						BinaryOp.LessThan => CompareValues(left, right) < 0,
						BinaryOp.LessThanOrEqual => CompareValues(left, right) <= 0,
						BinaryOp.Equal => ValuesEqual(left, right),
						BinaryOp.NotEqual => !ValuesEqual(left, right),
						BinaryOp.And => IsTruthy(left) && IsTruthy(right),
						BinaryOp.Or => IsTruthy(left) || IsTruthy(right),
						BinaryOp.Add => ArithmeticOp(left, right, (a, b) => a + b),
						BinaryOp.Subtract => ArithmeticOp(left, right, (a, b) => a - b),
						BinaryOp.Multiply => ArithmeticOp(left, right, (a, b) => a * b),
						BinaryOp.Divide => ArithmeticOp(left, right, (a, b) => b != 0 ? a / b : double.NaN),
						BinaryOp.Modulo => ArithmeticOp(left, right, (a, b) => b != 0 ? a % b : double.NaN),
						_ => null
					};
				}
			case TernaryExpression tern:
				{
					var condition = EvaluateAggregateExpression(tern.Condition, items, fromAlias, parameters);
					return condition is bool b && b
						? EvaluateAggregateExpression(tern.IfTrue, items, fromAlias, parameters)
						: EvaluateAggregateExpression(tern.IfFalse, items, fromAlias, parameters);
				}
			default:
				// Non-aggregate expression — evaluate against first item (for non-aggregate fields in mixed projection)
				if (items.Count > 0)
				{
					var jObj = JsonParseHelpers.ParseJson(items[0]);
					return EvaluateSqlExpression(expr, jObj, fromAlias, parameters);
				}
				return null;
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Query helpers — Parameter extraction
	// ═══════════════════════════════════════════════════════════════════════════

	private static Dictionary<string, object> ExtractQueryParameters(QueryDefinition queryDefinition)
	{
		var parameters = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
		try
		{
			foreach (var (Name, Value) in queryDefinition.GetQueryParameters())
			{
				parameters[Name] = Value;
			}

			if (parameters.Count > 0)
			{
				return parameters;
			}
		}
		catch { /* Fall through to reflection */ }

		try
		{
			var internalField = typeof(QueryDefinition)
				.GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
				.FirstOrDefault(f => f.Name.Contains("parameter", StringComparison.OrdinalIgnoreCase));

			if (internalField?.GetValue(queryDefinition) is System.Collections.IEnumerable enumerable)
			{
				foreach (var item in enumerable)
				{
					var itemType = item.GetType();
					var nameProp = itemType.GetProperty("Name") ?? itemType.GetProperty("Item1");
					var valueProp = itemType.GetProperty("Value") ?? itemType.GetProperty("Item2");
					if (nameProp is not null && valueProp is not null)
					{
						var name = nameProp.GetValue(item)?.ToString();
						if (name is not null)
						{
							parameters[name] = valueProp.GetValue(item);
						}
					}
					else
					{
						var nameField = itemType.GetField("Name") ?? itemType.GetField("Item1");
						var valueField = itemType.GetField("Value") ?? itemType.GetField("Item2");
						if (nameField is not null && valueField is not null)
						{
							var name = nameField.GetValue(item)?.ToString();
							if (name is not null)
							{
								parameters[name] = valueField.GetValue(item);
							}
						}
					}
				}
			}
		}
		catch { /* Parameters cannot be extracted */ }

		return parameters;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  WHERE expression evaluation
	// ═══════════════════════════════════════════════════════════════════════════

	private static bool EvaluateWhereExpression(
		WhereExpression expression, JObject item, string fromAlias,
		IDictionary<string, object> parameters, JoinClause join, bool treatUndefinedAsNull = false)
	{
		return expression switch
		{
			ComparisonCondition c => EvaluateComparison(c, item, fromAlias, parameters, treatUndefinedAsNull),
			AndCondition a => EvaluateWhereExpression(a.Left, item, fromAlias, parameters, join, treatUndefinedAsNull)
							  && EvaluateWhereExpression(a.Right, item, fromAlias, parameters, join, treatUndefinedAsNull),
			OrCondition o => EvaluateWhereExpression(o.Left, item, fromAlias, parameters, join, treatUndefinedAsNull)
							 || EvaluateWhereExpression(o.Right, item, fromAlias, parameters, join, treatUndefinedAsNull),
			NotCondition n => !EvaluateWhereExpressionIncludesUndefined(n.Inner, item, fromAlias, parameters, join, treatUndefinedAsNull)
							  && !EvaluateWhereExpression(n.Inner, item, fromAlias, parameters, join, treatUndefinedAsNull),
			FunctionCondition f => EvaluateFunction(f, item, fromAlias, parameters),
			ExistsCondition e => EvaluateExists(e, item, fromAlias, parameters, join),
			SqlExpressionCondition s => IsTruthy(EvaluateSqlExpression(s.Expression, item, fromAlias, parameters)),
			_ => true
		};
	}

	private static bool EvaluateComparison(
		ComparisonCondition comparison, JObject item, string fromAlias, IDictionary<string, object> parameters,
		bool treatUndefinedAsNull = false)
	{
		var leftValue = ResolveValue(comparison.Left, item, fromAlias, parameters);
		var rightValue = ResolveValue(comparison.Right, item, fromAlias, parameters);

		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update#filter-predicate
		//   In FilterPredicate context, missing properties are treated as null.
		if (treatUndefinedAsNull)
		{
			if (leftValue is UndefinedValue) leftValue = null;
			if (rightValue is UndefinedValue) rightValue = null;
		}

		if (leftValue is UndefinedValue || rightValue is UndefinedValue)
			return false;
		return comparison.Operator switch
		{
			ComparisonOp.Equal => ValuesEqual(leftValue, rightValue),
			ComparisonOp.NotEqual => !ValuesEqual(leftValue, rightValue),
			ComparisonOp.LessThan => CompareValues(leftValue, rightValue) < 0,
			ComparisonOp.GreaterThan => CompareValues(leftValue, rightValue) > 0,
			ComparisonOp.LessThanOrEqual => CompareValues(leftValue, rightValue) <= 0,
			ComparisonOp.GreaterThanOrEqual => CompareValues(leftValue, rightValue) >= 0,
			ComparisonOp.Like => IsTruthy(EvaluateLike(leftValue, rightValue)),
			_ => false
		};
	}

	/// <summary>
	/// Returns true if the inner expression involves undefined operands (three-value logic).
	/// Used by NOT to propagate undefined — NOT undefined = undefined (excluded).
	/// </summary>
	private static bool EvaluateWhereExpressionIncludesUndefined(
		WhereExpression expression, JObject item, string fromAlias,
		IDictionary<string, object> parameters, JoinClause join, bool treatUndefinedAsNull = false)
	{
		return expression switch
		{
			ComparisonCondition c => ComparisonIncludesUndefined(c, item, fromAlias, parameters, treatUndefinedAsNull),
			AndCondition a =>
				EvaluateWhereExpressionIncludesUndefined(a.Left, item, fromAlias, parameters, join, treatUndefinedAsNull) ||
				EvaluateWhereExpressionIncludesUndefined(a.Right, item, fromAlias, parameters, join, treatUndefinedAsNull),
			OrCondition o =>
				EvaluateWhereExpressionIncludesUndefined(o.Left, item, fromAlias, parameters, join, treatUndefinedAsNull) ||
				EvaluateWhereExpressionIncludesUndefined(o.Right, item, fromAlias, parameters, join, treatUndefinedAsNull),
			NotCondition n =>
				EvaluateWhereExpressionIncludesUndefined(n.Inner, item, fromAlias, parameters, join, treatUndefinedAsNull),
			SqlExpressionCondition s =>
				EvaluateSqlExpression(s.Expression, item, fromAlias, parameters) is UndefinedValue,
			_ => false,
		};
	}

	/// <summary>
	/// Checks if a comparison involves undefined semantics. For most operators, only
	/// UndefinedValue counts. For LIKE, null also produces undefined (three-value logic).
	/// When treatUndefinedAsNull is true, undefined is treated as null (FilterPredicate context).
	/// </summary>
	private static bool ComparisonIncludesUndefined(
		ComparisonCondition c, JObject item, string fromAlias, IDictionary<string, object> parameters,
		bool treatUndefinedAsNull = false)
	{
		var left = ResolveValue(c.Left, item, fromAlias, parameters);
		var right = ResolveValue(c.Right, item, fromAlias, parameters);

		// In FilterPredicate context, undefined is treated as null — not "undefined"
		if (treatUndefinedAsNull)
		{
			if (left is UndefinedValue) left = null;
			if (right is UndefinedValue) right = null;
		}

		if (left is UndefinedValue || right is UndefinedValue)
			return true;
		// LIKE with null operand(s) produces undefined per three-value logic
		if (c.Operator == ComparisonOp.Like && (left is null || right is null))
			return true;
		return false;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Value resolution & comparison
	// ═══════════════════════════════════════════════════════════════════════════

	private static object ResolveValue(
		string expression, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var trimmed = expression.Trim();
		if (trimmed.StartsWith("@"))
		{
			return parameters.TryGetValue(trimmed, out var v) ? v : null;
		}

		if (trimmed.StartsWith("'") && trimmed.EndsWith("'"))
		{
			return trimmed[1..^1];
		}

		if (string.Equals(trimmed, "true", StringComparison.OrdinalIgnoreCase))
		{
			return true;
		}

		if (string.Equals(trimmed, "false", StringComparison.OrdinalIgnoreCase))
		{
			return false;
		}

		if (string.Equals(trimmed, "null", StringComparison.OrdinalIgnoreCase))
		{
			return null;
		}

		if (double.TryParse(trimmed, NumberStyles.Any, CultureInfo.InvariantCulture, out var num))
		{
			return num;
		}

		var jsonPath = trimmed;
		if (jsonPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
		{
			jsonPath = jsonPath[(fromAlias.Length + 1)..];
		}
		else if (jsonPath.StartsWith(fromAlias + "[", StringComparison.OrdinalIgnoreCase))
		{
			jsonPath = jsonPath[fromAlias.Length..]; // Keep brackets — SelectToken handles ['prop']
		}

		var token = item.SelectToken(jsonPath);
		if (token == null)
		{
			// Handle string pseudo-properties: SDK LINQ translates d.Name.Length
			// to root["name"]["Length"] which SelectToken can't resolve on a string value.
			if (TryResolveStringPseudoProperty(jsonPath, item, out var pseudoResult))
			{
				return pseudoResult;
			}

			return UndefinedValue.Instance;
		}

		return token.Type switch
		{
			JTokenType.String => token.Value<string>(),
			JTokenType.Integer => token.Value<long>(),
			JTokenType.Float => token.Value<double>(),
			JTokenType.Boolean => token.Value<bool>(),
			JTokenType.Null => null,
			JTokenType.Undefined => UndefinedValue.Instance,
			JTokenType.Array => (object)token,
			JTokenType.Object => (object)token,
			_ => token.ToString()
		};
	}

	/// <summary>
	/// Handles string/array pseudo-properties that the SDK LINQ provider generates.
	/// For example, <c>d.Name.Length</c> is translated to <c>root["name"]["Length"]</c>
	/// which SelectToken cannot resolve on a string JValue. This method detects the
	/// pattern and returns the string length (or array count) instead.
	/// </summary>
	private static bool TryResolveStringPseudoProperty(string jsonPath, JObject item, out object result)
	{
		result = null;

		string parentPath = null;
		if (jsonPath.EndsWith("['Length']", StringComparison.OrdinalIgnoreCase))
			parentPath = jsonPath[..^"['Length']".Length];
		else if (jsonPath.EndsWith("[\"Length\"]", StringComparison.OrdinalIgnoreCase))
			parentPath = jsonPath[..^"[\"Length\"]".Length];
		else if (jsonPath.EndsWith(".Length", StringComparison.OrdinalIgnoreCase))
			parentPath = jsonPath[..^".Length".Length];

		if (parentPath is null || parentPath.Length == 0) return false;

		var parentToken = item.SelectToken(parentPath);
		if (parentToken is JValue { Type: JTokenType.String } sv)
		{
			result = (long)sv.Value<string>()!.Length;
			return true;
		}

		if (parentToken is JArray arr)
		{
			result = (long)arr.Count;
			return true;
		}

		return false;
	}

	private static bool ValuesEqual(object left, object right)
	{
		if (left is UndefinedValue || right is UndefinedValue)
		{
			return false;
		}

		if (left is null && right is null)
		{
			return true;
		}

		if (left is null || right is null)
		{
			return false;
		}

		if ((left is double or long) && (right is double or long))
		{
			if (double.TryParse(left.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var l) &&
				double.TryParse(right.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var r))
			{
				return l == r;
			}
		}

		// Normalize non-primitive / non-JToken values (e.g. anonymous objects from parameters) to JToken
		var leftJ = left is JToken ? left : NormalizeToJTokenIfComplex(left);
		var rightJ = right is JToken ? right : NormalizeToJTokenIfComplex(right);

		// Cosmos DB uses strict type comparison: different type ranks are never equal
		// (except numeric types which are handled above)
		if (GetTypeRank(leftJ) != GetTypeRank(rightJ))
		{
			return false;
		}

		// Deep-compare JTokens
		if (leftJ is JToken jtLeft && rightJ is JToken jtRight)
		{
			return JToken.DeepEquals(jtLeft, jtRight);
		}

		return string.Equals(leftJ.ToString(), rightJ.ToString(), StringComparison.Ordinal);
	}

	private static object NormalizeToJTokenIfComplex(object value)
	{
		if (value is null or string or bool or int or long or double or float or decimal or UndefinedValue)
			return value;
		try { return JToken.FromObject(value); }
		catch { return value; }
	}

	/// <summary>
	/// Returns the Cosmos DB type rank for ordering:
	/// undefined(0) &lt; null(1) &lt; bool(2) &lt; number(3) &lt; string(4) &lt; array(5) &lt; object(6).
	/// </summary>
	private static int GetTypeRank(object value)
	{
		if (ReferenceEquals(value, UndefinedSortSentinel)) return 0;
		if (value is UndefinedValue) return 0;
		if (value is null) return 1;
		if (value is bool) return 2;
		if (value is int or long or double or float or decimal) return 3;
		if (value is string) return 4;
		if (value is JToken jt)
		{
			return jt.Type switch
			{
				JTokenType.Undefined => 0,
				JTokenType.Null => 1,
				JTokenType.Boolean => 2,
				JTokenType.Integer or JTokenType.Float => 3,
				JTokenType.String => 4,
				JTokenType.Array => 5,
				JTokenType.Object => 6,
				_ => 7
			};
		}
		return 7;
	}

	private static int CompareValues(object left, object right)
	{
		var leftRank = GetTypeRank(left);
		var rightRank = GetTypeRank(right);
		if (leftRank != rightRank) return leftRank.CompareTo(rightRank);

		// Both undefined or both null
		if (leftRank <= 1) return 0;

		if (left is bool lb && right is bool rb)
			return lb.CompareTo(rb); // false < true

		if (double.TryParse(left?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var l) &&
			double.TryParse(right?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var r))
		{
			return l.CompareTo(r);
		}

		// Element-by-element array comparison (Cosmos DB behavior)
		if (leftRank == 5)
		{
			var leftArr = left is JArray la ? la : (left is JToken lt ? new JArray(lt) : null);
			var rightArr = right is JArray ra ? ra : (right is JToken rt ? new JArray(rt) : null);
			if (leftArr is not null && rightArr is not null)
			{
				for (var i = 0; i < Math.Min(leftArr.Count, rightArr.Count); i++)
				{
					var cmp = CompareValues(
						JTokenToObject(leftArr[i]),
						JTokenToObject(rightArr[i]));
					if (cmp != 0) return cmp;
				}
				return leftArr.Count.CompareTo(rightArr.Count);
			}
		}

		// Property-by-property object comparison (sorted by property name)
		if (leftRank == 6)
		{
			var leftObj = left as JObject ?? (left is JToken lk ? lk as JObject : null);
			var rightObj = right as JObject ?? (right is JToken rk ? rk as JObject : null);
			if (leftObj is not null && rightObj is not null)
			{
				var leftProps = leftObj.Properties().OrderBy(p => p.Name, StringComparer.Ordinal).ToList();
				var rightProps = rightObj.Properties().OrderBy(p => p.Name, StringComparer.Ordinal).ToList();
				for (var i = 0; i < Math.Min(leftProps.Count, rightProps.Count); i++)
				{
					var nameCmp = string.Compare(leftProps[i].Name, rightProps[i].Name, StringComparison.Ordinal);
					if (nameCmp != 0) return nameCmp;
					var valCmp = CompareValues(
						JTokenToObject(leftProps[i].Value),
						JTokenToObject(rightProps[i].Value));
					if (valCmp != 0) return valCmp;
				}
				return leftProps.Count.CompareTo(rightProps.Count);
			}
		}

		return string.Compare(left?.ToString(), right?.ToString(), StringComparison.Ordinal);
	}

	private static object JTokenToObject(JToken token)
	{
		return token.Type switch
		{
			JTokenType.Integer => (object)token.Value<long>(),
			JTokenType.Float => token.Value<double>(),
			JTokenType.Boolean => token.Value<bool>(),
			JTokenType.String => token.Value<string>(),
			JTokenType.Null => null,
			JTokenType.Undefined => UndefinedValue.Instance,
			_ => token // Return JArray/JObject as-is for recursive comparison
		};
	}

	private static object EvaluateLike(object left, object right, string escapeChar = null)
	{
		if (left is UndefinedValue || right is UndefinedValue)
		{
			return UndefinedValue.Instance;
		}

		if (left is null || right is null)
		{
			return UndefinedValue.Instance;
		}

		// Real Cosmos DB: LIKE only operates on strings. Non-string left operand returns undefined.
		if (left is not string)
		{
			return UndefinedValue.Instance;
		}

		var patternStr = right.ToString();
		if (escapeChar is { Length: > 0 })
		{
			var esc = escapeChar[0];
			var sb = new System.Text.StringBuilder();
			for (var i = 0; i < patternStr.Length; i++)
			{
				if (patternStr[i] == esc && i + 1 < patternStr.Length)
				{
					sb.Append(Regex.Escape(patternStr[i + 1].ToString()));
					i++;
				}
				else if (patternStr[i] == '%')
					sb.Append(".*");
				else if (patternStr[i] == '_')
					sb.Append('.');
				else
					sb.Append(Regex.Escape(patternStr[i].ToString()));
			}
			var pattern = $"^{sb}$";
			return GetOrCreateRegex(pattern, RegexOptions.Singleline).IsMatch(left.ToString());
		}

		var simplePattern = ConvertLikeToRegex(patternStr);
		return GetOrCreateRegex(simplePattern, RegexOptions.Singleline).IsMatch(left.ToString());
	}

	private static string ConvertLikeToRegex(string pattern)
	{
		var sb = new System.Text.StringBuilder("^");
		foreach (var ch in pattern)
		{
			if (ch == '%') sb.Append(".*");
			else if (ch == '_') sb.Append('.');
			else sb.Append(Regex.Escape(ch.ToString()));
		}
		sb.Append('$');
		return sb.ToString();
	}

	private static Regex GetOrCreateRegex(string pattern, RegexOptions options)
	{
		var key = (pattern, options);
		if (RegexCache.TryGetValue(key, out var cached))
		{
			return cached;
		}

		var regex = new Regex(pattern, options | RegexOptions.Compiled);
		if (RegexCache.Count < RegexCacheMaxSize)
		{
			RegexCache.TryAdd(key, regex);
		}

		return regex;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Legacy FunctionCondition evaluation (backward compat)
	// ═══════════════════════════════════════════════════════════════════════════

	private static bool EvaluateFunction(
		FunctionCondition func, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		switch (func.FunctionName)
		{
			case "STARTSWITH":
				{
					if (func.Arguments.Length < 2)
					{
						return false;
					}

					var fieldValue = ResolveValue(func.Arguments[0], item, fromAlias, parameters)?.ToString();
					var prefix = ResolveValue(func.Arguments[1], item, fromAlias, parameters)?.ToString();
					if (fieldValue is null || prefix is null)
					{
						return false;
					}

					var ignoreCase = func.Arguments.Length >= 3 &&
						string.Equals(ResolveValue(func.Arguments[2], item, fromAlias, parameters)?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
					return fieldValue.StartsWith(prefix, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
				}
			case "ENDSWITH":
				{
					if (func.Arguments.Length < 2)
					{
						return false;
					}

					var fieldValue = ResolveValue(func.Arguments[0], item, fromAlias, parameters)?.ToString();
					var suffix = ResolveValue(func.Arguments[1], item, fromAlias, parameters)?.ToString();
					if (fieldValue is null || suffix is null)
					{
						return false;
					}

					var ignoreCase = func.Arguments.Length >= 3 &&
						string.Equals(ResolveValue(func.Arguments[2], item, fromAlias, parameters)?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
					return fieldValue.EndsWith(suffix, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
				}
			case "CONTAINS":
				{
					if (func.Arguments.Length < 2)
					{
						return false;
					}

					var fieldValue = ResolveValue(func.Arguments[0], item, fromAlias, parameters)?.ToString();
					var search = ResolveValue(func.Arguments[1], item, fromAlias, parameters)?.ToString();
					if (fieldValue is null || search is null)
					{
						return false;
					}

					var ignoreCase = func.Arguments.Length >= 3 &&
						string.Equals(ResolveValue(func.Arguments[2], item, fromAlias, parameters)?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
					return fieldValue.Contains(search, ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
				}
			case "ARRAY_CONTAINS":
				{
					if (func.Arguments.Length < 2)
					{
						return false;
					}

					JArray jArray;
					var firstArg = func.Arguments[0].Trim();
					if (firstArg.StartsWith("@") && parameters.TryGetValue(firstArg, out var paramVal))
					{
						// First argument is a parameter (e.g. ARRAY_CONTAINS(@names, c.name))
						jArray = paramVal switch
						{
							JArray ja => ja,
							System.Collections.IEnumerable enumerable when paramVal is not string =>
								new JArray(enumerable.Cast<object>().Select(JToken.FromObject)),
							_ => null
						};
					}
					else
					{
						var arrayPath = firstArg;
						if (arrayPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
						{
							arrayPath = arrayPath[(fromAlias.Length + 1)..];
						}
						jArray = item.SelectToken(arrayPath) as JArray;
					}

					if (jArray is null)
					{
						return false;
					}

					var searchValue = ResolveValue(func.Arguments[1], item, fromAlias, parameters);
					if (searchValue is null)
					{
						return jArray.Any(t => t.Type == JTokenType.Null);
					}

					var searchStr = searchValue.ToString();
					var partial = func.Arguments.Length >= 3 &&
						string.Equals(func.Arguments[2].Trim(), "true", StringComparison.OrdinalIgnoreCase);
					return ArrayContainsMatch(jArray, searchStr, partial);
				}
			case "IS_DEFINED":
				{
					if (func.Arguments.Length < 1)
					{
						return false;
					}

					var path = func.Arguments[0].Trim();

					// Resolve parameterized values (e.g. IS_DEFINED(@param))
					// A resolved parameter is always "defined" (even if null).
					if (path.StartsWith("@"))
						return parameters.ContainsKey(path);

					if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
					{
						path = path[(fromAlias.Length + 1)..];
					}

					var token = item.SelectToken(path);
					return token is not null && token.Type != JTokenType.Undefined;
				}
			case "IS_NULL":
				{
					if (func.Arguments.Length < 1)
					{
						return false;
					}

					var path = func.Arguments[0].Trim();

					// Resolve parameterized values (e.g. IS_NULL(@param))
					if (path.StartsWith("@"))
					{
						if (parameters.TryGetValue(path, out var paramVal))
							return paramVal is null || (paramVal is JToken jt && jt.Type == JTokenType.Null);
						return true; // Unresolved parameter treated as null
					}

					if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
					{
						path = path[(fromAlias.Length + 1)..];
					}

					var token = item.SelectToken(path);
					return token is not null && token.Type is JTokenType.Null;
				}
			default:
				return true;
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  EXISTS subquery evaluation
	// ═══════════════════════════════════════════════════════════════════════════

	private static bool EvaluateExists(
		ExistsCondition exists, JObject item, string fromAlias,
		IDictionary<string, object> parameters, JoinClause join)
	{
		try
		{
			var raw = exists.RawSubquery.Trim();
			var queryToParse = raw.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
				? raw
				: "SELECT * FROM " + raw;

			CosmosSqlQuery subquery;
			try
			{
				subquery = CosmosSqlParser.Parse(queryToParse);
			}
			catch (NotSupportedException)
			{
				// Malformed SQL — throw 400 like real Cosmos DB
				throw InMemoryCosmosException.Create(
					$"Syntax error in EXISTS subquery: {raw}",
					System.Net.HttpStatusCode.BadRequest, 0, string.Empty, 0);
			}

			// Handle FROM alias IN source.path (array iteration in EXISTS subquery)
			if (subquery.FromSource is not null)
			{
				var sourcePath = subquery.FromSource;
				// Strip outer alias prefix (e.g. "c.tags" → "tags" when fromAlias is "c")
				if (sourcePath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
				{
					sourcePath = sourcePath[(fromAlias.Length + 1)..];
				}

				var arrayToken = item.SelectToken(sourcePath);
				if (arrayToken is not JArray jArray || jArray.Count == 0)
				{
					return false;
				}

				if (subquery.Where is not null)
				{
					var iterAlias = subquery.FromAlias;
					foreach (var element in jArray)
					{
						// Create a combined object with the array element available under its alias
						var combined = new JObject(item.Properties());
						combined[iterAlias] = element is JObject elementObj
							? elementObj.DeepClone()
							: new JValue(element.ToObject<object>());
						if (EvaluateWhereExpression(subquery.Where, combined, iterAlias, parameters, null))
						{
							return true;
						}
					}
					return false;
				}
				return true;
			}

			if (subquery.Join is not null)
			{
				var arrayPath = subquery.Join.ArrayField;
				var sourceAlias = subquery.Join.SourceAlias;
				if (sourceAlias.Equals(fromAlias, StringComparison.OrdinalIgnoreCase))
				{
					var arrayToken = item.SelectToken(arrayPath);
					if (arrayToken is not JArray jArray || jArray.Count == 0)
					{
						return false;
					}

					if (subquery.Where is not null)
					{
						foreach (var element in jArray)
						{
							if (element is JObject elementObj)
							{
								var combined = new JObject(item.Properties())
								{
									[subquery.Join.Alias] = elementObj
								};
								if (EvaluateWhereExpression(subquery.Where, combined, subquery.FromAlias, parameters, subquery.Join))
								{
									return true;
								}
							}
						}
						return false;
					}
					return true;
				}
			}
			if (subquery.Where is not null)
			{
				return EvaluateWhereExpression(subquery.Where, item, fromAlias, parameters, join);
			}

			return true;
		}
		catch (CosmosException)
		{
			throw; // Re-throw parse errors (400 Bad Request)
		}
		catch
		{
			return false; // Runtime evaluation errors — EXISTS evaluates to false
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  SqlExpression tree evaluation
	// ═══════════════════════════════════════════════════════════════════════════

	private static object EvaluateSqlExpression(
		SqlExpression expr, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		return expr switch
		{
			LiteralExpression lit => lit.Value,
			UndefinedLiteralExpression => UndefinedValue.Instance,
			IdentifierExpression ident => ResolveValue(ident.Name, item, fromAlias, parameters),
			ParameterExpression param => parameters.TryGetValue(param.Name, out var v) ? UnwrapJToken(v) : null,
			FunctionCallExpression func => EvaluateSqlFunction(func, item, fromAlias, parameters),
			BinaryExpression bin => EvaluateBinaryExpression(bin, item, fromAlias, parameters),
			UnaryExpression unary => EvaluateUnaryExpression(unary, item, fromAlias, parameters),
			BetweenExpression between => EvalBetween(between, item, fromAlias, parameters),
			InExpression inExpr => EvalIn(inExpr, item, fromAlias, parameters),
			LikeExpression like => EvaluateLike(
				EvaluateSqlExpression(like.Value, item, fromAlias, parameters),
				EvaluateSqlExpression(like.Pattern, item, fromAlias, parameters),
				like.EscapeChar),
			ExistsExpression exists => EvaluateExists(new ExistsCondition(exists.RawSubquery), item, fromAlias, parameters, null),
			CoalesceExpression coal => EvalCoalesce(coal, item, fromAlias, parameters),
			TernaryExpression tern => EvaluateSqlExpression(tern.Condition, item, fromAlias, parameters) is bool tb && tb
				? EvaluateSqlExpression(tern.IfTrue, item, fromAlias, parameters)
				: EvaluateSqlExpression(tern.IfFalse, item, fromAlias, parameters),
			ObjectLiteralExpression obj => EvaluateObjectLiteral(obj, item, fromAlias, parameters),
			ArrayLiteralExpression arr => EvaluateArrayLiteral(arr, item, fromAlias, parameters),
			PropertyAccessExpression prop => ResolveValue(
				$"{CosmosSqlParser.ExprToString(prop.Object)}.{prop.Property}", item, fromAlias, parameters),
			IndexAccessExpression idx => ResolveValue(
				$"{CosmosSqlParser.ExprToString(idx.Object)}[{CosmosSqlParser.ExprToString(idx.Index)}]", item, fromAlias, parameters),
			SubqueryExpression sub => EvaluateSubquery(sub.Subquery, item, fromAlias, parameters),
			_ => null
		};
	}

	private static object UnwrapJToken(object value)
	{
		if (value is JValue jv)
		{
			return jv.Type switch
			{
				JTokenType.String => jv.Value<string>(),
				JTokenType.Integer => jv.Value<long>(),
				JTokenType.Float => jv.Value<double>(),
				JTokenType.Boolean => jv.Value<bool>(),
				JTokenType.Null => null,
				_ => value
			};
		}
		return value;
	}

	private static object EvalBetween(BetweenExpression b, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var value = EvaluateSqlExpression(b.Value, item, fromAlias, parameters);
		var low = EvaluateSqlExpression(b.Low, item, fromAlias, parameters);
		var high = EvaluateSqlExpression(b.High, item, fromAlias, parameters);
		if (value is UndefinedValue || low is UndefinedValue || high is UndefinedValue)
			return UndefinedValue.Instance;
		return CompareValues(value, low) >= 0 && CompareValues(value, high) <= 0;
	}

	private static object EvalCoalesce(CoalesceExpression coal, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var left = EvaluateSqlExpression(coal.Left, item, fromAlias, parameters);
		return left is not null and not UndefinedValue ? left : EvaluateSqlExpression(coal.Right, item, fromAlias, parameters);
	}

	private static object EvalIn(InExpression inExpr, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var value = EvaluateSqlExpression(inExpr.Value, item, fromAlias, parameters);
		if (value is UndefinedValue)
			return UndefinedValue.Instance;
		return inExpr.List.Any(li => ValuesEqual(value, EvaluateSqlExpression(li, item, fromAlias, parameters)));
	}

	private static object EvaluateBinaryExpression(
		BinaryExpression bin, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var left = EvaluateSqlExpression(bin.Left, item, fromAlias, parameters);
		var right = EvaluateSqlExpression(bin.Right, item, fromAlias, parameters);

		// Three-value logic: comparisons with undefined operand(s) produce undefined
		if (bin.Operator is BinaryOp.Equal or BinaryOp.NotEqual or BinaryOp.LessThan or BinaryOp.GreaterThan
			or BinaryOp.LessThanOrEqual or BinaryOp.GreaterThanOrEqual or BinaryOp.Like)
		{
			if (left is UndefinedValue || right is UndefinedValue)
				return UndefinedValue.Instance;
		}

		return bin.Operator switch
		{
			BinaryOp.Equal => (object)ValuesEqual(left, right),
			BinaryOp.NotEqual => !ValuesEqual(left, right),
			BinaryOp.LessThan => CompareValues(left, right) < 0,
			BinaryOp.GreaterThan => CompareValues(left, right) > 0,
			BinaryOp.LessThanOrEqual => CompareValues(left, right) <= 0,
			BinaryOp.GreaterThanOrEqual => CompareValues(left, right) >= 0,
			// Three-value AND: false AND undefined = false; true AND undefined = undefined
			BinaryOp.And => left is UndefinedValue
				? (IsTruthy(right) ? UndefinedValue.Instance : (object)false)
				: right is UndefinedValue
					? (IsTruthy(left) ? UndefinedValue.Instance : (object)false)
					: IsTruthy(left) && IsTruthy(right),
			// Three-value OR: true OR undefined = true; false OR undefined = undefined
			BinaryOp.Or => left is UndefinedValue
				? (IsTruthy(right) ? (object)true : UndefinedValue.Instance)
				: right is UndefinedValue
					? (IsTruthy(left) ? (object)true : UndefinedValue.Instance)
					: IsTruthy(left) || IsTruthy(right),
			BinaryOp.Like => EvaluateLike(left, right),
			BinaryOp.Add => ArithmeticOp(left, right, (a, b) => a + b),
			BinaryOp.Subtract => ArithmeticOp(left, right, (a, b) => a - b),
			BinaryOp.Multiply => ArithmeticOp(left, right, (a, b) => a * b),
			BinaryOp.Divide => ArithmeticOp(left, right, (a, b) => b != 0 ? a / b : double.NaN),
			BinaryOp.Modulo => ArithmeticOp(left, right, (a, b) => b != 0 ? a % b : double.NaN),
			BinaryOp.StringConcat => left is null or UndefinedValue || right is null or UndefinedValue ? UndefinedValue.Instance : left.ToString() + right.ToString(),
			BinaryOp.BitwiseAnd => BitwiseOp(left, right, (a, b) => a & b),
			BinaryOp.BitwiseOr => BitwiseOp(left, right, (a, b) => a | b),
			BinaryOp.BitwiseXor => BitwiseOp(left, right, (a, b) => a ^ b),
			_ => null
		};
	}

	private static object EvaluateUnaryExpression(
		UnaryExpression unary, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var operand = EvaluateSqlExpression(unary.Operand, item, fromAlias, parameters);
		return unary.Operator switch
		{
			UnaryOp.Not => operand is UndefinedValue or null ? UndefinedValue.Instance : (object)!IsTruthy(operand),
			UnaryOp.Negate => operand is double d ? (object)(-d) : operand is long l ? (object)(-l) : UndefinedValue.Instance,
			UnaryOp.BitwiseNot => operand is long lng ? (object)(~lng) : UndefinedValue.Instance,
			_ => UndefinedValue.Instance
		};
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  SQL function evaluation
	// ═══════════════════════════════════════════════════════════════════════════

	private static object EvaluateSqlFunction(
		FunctionCallExpression func, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		// ARRAY(subquery) — evaluate subquery and collect results into a JArray
		if (string.Equals(func.FunctionName, "ARRAY", StringComparison.OrdinalIgnoreCase) &&
			func.Arguments.Length == 1 && func.Arguments[0] is SubqueryExpression subExpr)
		{
			return EvaluateArraySubquery(subExpr.Subquery, item, fromAlias, parameters);
		}

		var args = func.Arguments.Select(a => EvaluateSqlExpression(a, item, fromAlias, parameters)).ToArray();

		// Undefined propagation: most scalar functions return undefined when any
		// argument is undefined (missing property). Type-checking functions (IS_*),
		// emulator-specific functions that handle missing values with special
		// semantics, and a few other functions are excluded.
		if (args.Any(a => a is UndefinedValue))
		{
			var name = func.FunctionName.ToUpperInvariant();
			if (!name.StartsWith("IS_") && name is not "COALESCE" and not "IIF"
				and not "ARRAY_CONTAINS" and not "ARRAY_CONTAINS_ANY" and not "ARRAY_CONTAINS_ALL"
				and not "DOCUMENTID" and not "VECTORDISTANCE"
				and not "FULLTEXTSCORE" and not "FULLTEXTCONTAINS" and not "FULLTEXTCONTAINSALL" and not "FULLTEXTCONTAINSANY"
				and not "INDEX_OF" and not "TYPE")
			{
				return UndefinedValue.Instance;
			}
		}

		switch (func.FunctionName)
		{
			// ── Type checking ──
			case "IS_DEFINED":
				{
					if (func.Arguments.Length < 1)
					{
						return false;
					}

					if (func.Arguments[0] is IdentifierExpression ident)
					{
						var path = ident.Name;
						if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
						{
							path = path[(fromAlias.Length + 1)..];
						}

						return item.SelectToken(path) != null;
					}
					// A parameter is always "defined" if it exists in the dictionary
					// (even when its value is null).
					if (func.Arguments[0] is ParameterExpression param)
						return parameters.ContainsKey(param.Name);
					return args[0] != null;
				}
			case "IS_NULL": return args.Length > 0 && args[0] is null;
			case "IS_ARRAY":
				return args.Length > 0 && (ResolveTokenType(func.Arguments, item, fromAlias) is JArray || args[0] is JArray);
			case "IS_BOOL": return args.Length > 0 && args[0] is bool;
			case "IS_NUMBER": return args.Length > 0 && args[0] is long or double or int or float or decimal;
			case "IS_STRING": return args.Length > 0 && args[0] is string;
			case "IS_OBJECT":
				return args.Length > 0 && (ResolveTokenType(func.Arguments, item, fromAlias) is JObject || args[0] is JObject);
			case "IS_PRIMITIVE":
				{
					if (args.Length == 0)
					{
						return false;
					}

					if (args[0] is null)
					{
						return true;
					}

					var tokenForPrimitive = ResolveTokenType(func.Arguments, item, fromAlias);
					if (tokenForPrimitive is JArray or JObject)
					{
						return false;
					}

					return args[0] is string or bool or long or double;
				}
			case "IS_FINITE_NUMBER":
				{
					if (args.Length == 0)
					{
						return false;
					}

					return args[0] switch
					{
						long => true,
						int => true,
						double d => !double.IsInfinity(d) && !double.IsNaN(d),
						float f => !float.IsInfinity(f) && !float.IsNaN(f),
						decimal => true,
						_ => false,
					};
				}
			case "IS_INTEGER":
				{
					if (args.Length == 0)
					{
						return false;
					}

					return args[0] is long or int;
				}
			case "IS_NAN":
				{
					if (args.Length == 0)
					{
						return false;
					}

					return args[0] is double d && double.IsNaN(d);
				}
			case "TYPE":
				{
					if (args.Length == 0)
					{
						return UndefinedValue.Instance;
					}

					if (args[0] is UndefinedValue)
					{
						return UndefinedValue.Instance;
					}

					var tokenForType = ResolveTokenType(func.Arguments, item, fromAlias);
					if (tokenForType is JArray) return "array";
					if (tokenForType is JObject) return "object";

					return args[0] switch
					{
						null => "null",
						bool => "boolean",
						long or int or double or float or decimal => "number",
						string => "string",
						_ => "undefined"
					};
				}

			// ── String functions ──
			case "STARTSWITH":
				{
					if (args.Length < 2)
					{
						return false;
					}

					if (args[0] is not string s || args[1] is UndefinedValue) return UndefinedValue.Instance;
					if (args[1] is not string p) return UndefinedValue.Instance;

					var ic = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
					return s.StartsWith(p, ic ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
				}
			case "ENDSWITH":
				{
					if (args.Length < 2)
					{
						return false;
					}

					if (args[0] is not string s || args[1] is UndefinedValue) return UndefinedValue.Instance;
					if (args[1] is not string p) return UndefinedValue.Instance;

					var ic = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
					return s.EndsWith(p, ic ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
				}
			case "CONTAINS":
				{
					if (args.Length < 2)
					{
						return false;
					}

					if (args[0] is not string s || args[1] is UndefinedValue) return UndefinedValue.Instance;
					if (args[1] is not string p) return UndefinedValue.Instance;

					var ic = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
					return s.Contains(p, ic ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
				}
			case "CONCAT":
				{
					// Cosmos DB: if any arg is null, undefined, or not a string, CONCAT returns undefined
					if (args.Any(a => a is null or UndefinedValue)) return UndefinedValue.Instance;
					if (args.Any(a => a is not string)) return UndefinedValue.Instance;
					return string.Concat(args.Cast<string>());
				}
			case "LENGTH":
				{
					if (args.Length == 0) return null;
					if (args[0] is null or UndefinedValue) return UndefinedValue.Instance;
					if (args[0] is not string s) return UndefinedValue.Instance;
					return (object)(long)s.Length;
				}
			case "LOWER": return args.Length > 0 ? (args[0] is null or UndefinedValue ? UndefinedValue.Instance : args[0] is string sl ? (object)sl.ToLowerInvariant() : UndefinedValue.Instance) : null;
			case "UPPER": return args.Length > 0 ? (args[0] is null or UndefinedValue ? UndefinedValue.Instance : args[0] is string su ? (object)su.ToUpperInvariant() : UndefinedValue.Instance) : null;
			case "TRIM": return args.Length > 0 ? (args[0] is null or UndefinedValue ? UndefinedValue.Instance : args[0] is string st ? (object)st.Trim() : UndefinedValue.Instance) : null;
			case "LTRIM": return args.Length > 0 ? (args[0] is null or UndefinedValue ? UndefinedValue.Instance : args[0] is string slt ? (object)slt.TrimStart() : UndefinedValue.Instance) : null;
			case "RTRIM": return args.Length > 0 ? (args[0] is null or UndefinedValue ? UndefinedValue.Instance : args[0] is string srt ? (object)srt.TrimEnd() : UndefinedValue.Instance) : null;
			case "REVERSE": return args.Length > 0 ? (args[0] is null or UndefinedValue ? UndefinedValue.Instance : args[0] is string rs ? (object)new string(rs.Reverse().ToArray()) : UndefinedValue.Instance) : null;
			case "LEFT":
				{
					if (args.Length < 2)
					{
						return null;
					}

					if (args[0] is not string s) return UndefinedValue.Instance;
					var c = ToLong(args[1]);
					if (!c.HasValue) return UndefinedValue.Instance;
					if (c.Value < 0) return UndefinedValue.Instance;
					return s[..(int)Math.Min(c.Value, s.Length)];
				}
			case "RIGHT":
				{
					if (args.Length < 2)
					{
						return null;
					}

					if (args[0] is not string s) return UndefinedValue.Instance;
					var c = ToLong(args[1]);
					if (!c.HasValue) return UndefinedValue.Instance;
					if (c.Value < 0) return UndefinedValue.Instance;
					return s[Math.Max(0, s.Length - (int)c.Value)..];
				}
			case "SUBSTRING":
				{
					if (args.Length < 3)
					{
						return UndefinedValue.Instance;
					}

					if (args[0] is UndefinedValue || args[1] is UndefinedValue || args[2] is UndefinedValue)
					{
						return UndefinedValue.Instance;
					}

					if (args[0] is not string s) return UndefinedValue.Instance;
					var start = ToLong(args[1]); var len = ToLong(args[2]);
					if (!start.HasValue || !len.HasValue)
					{
						return UndefinedValue.Instance;
					}

					var si = (int)Math.Max(0, Math.Min(start.Value, s.Length));
					var li = (int)Math.Max(0, Math.Min(len.Value, s.Length - si));
					return s.Substring(si, li);
				}
			case "REPLACE":
				{
					if (args.Length < 3)
					{
						return null;
					}

					var s = args[0] is string ss ? ss : null;
					var find = args[1] is string fs ? fs : null;
					var rep = args[2] is string rps ? rps : null;
					if (s is null || find is null) return UndefinedValue.Instance;
					if (args[2] is not (null or UndefinedValue or string)) return UndefinedValue.Instance;
					if (find.Length == 0) return s;
					return s.Replace(find, rep ?? "");
				}
			case "INDEX_OF":
				{
					if (args.Length < 2)
					{
						return UndefinedValue.Instance;
					}

					if (args[0] is not string s || args[1] is not string sub)
					{
						return UndefinedValue.Instance;
					}

					if (args.Length >= 3 && double.TryParse(args[2]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var startPos))
					{
						var sp = (int)startPos;
						if (sp < 0 || sp > s.Length) return UndefinedValue.Instance;
						return (object)(long)s.IndexOf(sub, sp, StringComparison.Ordinal);
					}
					return (object)(long)s.IndexOf(sub, StringComparison.Ordinal);
				}
			case "REGEXMATCH":
				{
					if (args.Length < 2)
					{
						return false;
					}

					if (args[0] is not string input || args[1] is not string pattern)
					{
						return UndefinedValue.Instance;
					}

					var options = RegexOptions.None;
					if (args.Length >= 3)
					{
						var modifiers = args[2]?.ToString() ?? "";
						foreach (var ch in modifiers)
						{
							options |= ch switch
							{
								'i' => RegexOptions.IgnoreCase,
								'm' => RegexOptions.Multiline,
								's' => RegexOptions.Singleline,
								'x' => RegexOptions.IgnorePatternWhitespace,
								_ => RegexOptions.None,
							};
						}
					}
					try
					{
						return GetOrCreateRegex(pattern, options).IsMatch(input);
					}
					catch (ArgumentException)
					{
						return UndefinedValue.Instance;
					}
				}
			case "REPLICATE":
				{
					if (args.Length < 2)
					{
						return null;
					}

					if (args[0] is not string s)
					{
						return UndefinedValue.Instance;
					}

					var count = ToLong(args[1]);
					if (!count.HasValue || count.Value < 0 || count.Value > 10000)
					{
						return UndefinedValue.Instance;
					}

					return count.Value == 0 ? "" : string.Concat(Enumerable.Repeat(s, (int)count.Value));
				}
			case "STRING_EQUALS":
			case "STRINGEQUALS":
				{
					if (args.Length < 2)
					{
						return UndefinedValue.Instance;
					}

					if (args[0] is not string s1)
					{
						return UndefinedValue.Instance;
					}

					if (args[1] is not string s2)
					{
						return UndefinedValue.Instance;
					}

					var ic = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
					return string.Equals(s1, s2, ic ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
				}
			case "STRINGTOARRAY":
				{
					if (args.Length == 0)
					{
						return null;
					}

					if (args[0] is null or UndefinedValue) return UndefinedValue.Instance;
					var s = args[0].ToString();
					if (s is null)
					{
						return UndefinedValue.Instance;
					}

					try
					{
						var token = JToken.Parse(s);
						return token is JArray ? token : UndefinedValue.Instance;
					}
					catch
					{
						return UndefinedValue.Instance;
					}
				}
			case "STRINGTOBOOLEAN":
				{
					if (args.Length == 0)
					{
						return null;
					}

					if (args[0] is null or UndefinedValue) return UndefinedValue.Instance;
					var s = args[0].ToString()?.Trim();
					return s switch
					{
						"true" => true,
						"false" => (object)false,
						_ => UndefinedValue.Instance,
					};
				}
			case "STRINGTONULL":
				{
					if (args.Length == 0)
					{
						return null;
					}

					if (args[0] is null or UndefinedValue) return UndefinedValue.Instance;
					return args[0].ToString()?.Trim() == "null" ? null : UndefinedValue.Instance;
				}
			case "STRINGTONUMBER":
				{
					if (args.Length == 0)
					{
						return null;
					}

					if (args[0] is null or UndefinedValue) return UndefinedValue.Instance;
					var s = args[0].ToString()?.Trim();
					if (s is null)
					{
						return UndefinedValue.Instance;
					}

					if (long.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var longVal))
					{
						return longVal;
					}

					if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var doubleVal))
					{
						if (double.IsNaN(doubleVal) || double.IsInfinity(doubleVal))
							return UndefinedValue.Instance;
						return doubleVal;
					}

					return UndefinedValue.Instance;
				}
			case "STRINGTOOBJECT":
				{
					if (args.Length == 0)
					{
						return null;
					}

					if (args[0] is null or UndefinedValue) return UndefinedValue.Instance;
					var s = args[0].ToString();
					if (s is null)
					{
						return UndefinedValue.Instance;
					}

					try
					{
						var token = JToken.Parse(s);
						return token is JObject ? token : UndefinedValue.Instance;
					}
					catch
					{
						return UndefinedValue.Instance;
					}
				}
			case "TOSTRING" or "ToString":
				if (args.Length == 0) return null;
				if (args[0] is null or UndefinedValue) return UndefinedValue.Instance;
				if (args[0] is bool boolVal) return boolVal ? "true" : "false";
				if (args[0] is string or long or int or double or float or decimal) return args[0].ToString();
				if (args[0] is JArray or JObject) return ((JToken)args[0]).ToString(Newtonsoft.Json.Formatting.None);
				if (args[0] is JValue jv && jv.Type == JTokenType.Null) return UndefinedValue.Instance;
				return args[0].ToString();
			case "TONUMBER" or "ToNumber":
				{
					if (args.Length == 0)
					{
						return null;
					}

					if (args[0] is null)
					{
						return UndefinedValue.Instance;
					}

					if (args[0] is long or double)
					{
						return args[0];
					}

					if (double.TryParse(args[0].ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var n))
					{
						return n;
					}

					return UndefinedValue.Instance;
				}
			case "TOBOOLEAN" or "ToBoolean":
				{
					if (args.Length == 0)
					{
						return null;
					}

					if (args[0] is bool)
					{
						return args[0];
					}

					if (args[0] != null && bool.TryParse(args[0].ToString(), out var b))
					{
						return b;
					}

					return UndefinedValue.Instance;
				}

			// ── Array functions ──
			case "ARRAY_CONTAINS":
				{
					if (func.Arguments.Length < 2)
					{
						return false;
					}

					JArray jArray = null;
					if (func.Arguments[0] is IdentifierExpression ident)
					{
						var arrayPath = ident.Name;
						if (arrayPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
						{
							arrayPath = arrayPath[(fromAlias.Length + 1)..];
						}

						var arrayToken = item.SelectToken(arrayPath);
						jArray = arrayToken as JArray;
					}
					else if (args[0] is JArray evalArr)
					{
						jArray = evalArr;
					}

					if (jArray is null)
					{
						return false;
					}

					{
						var partial = args.Length >= 3 && string.Equals(args[2]?.ToString(), "true", StringComparison.OrdinalIgnoreCase);
						var searchValue = args[1];
						if (searchValue is JObject searchObj)
						{
							return ArrayContainsMatchJObject(jArray, searchObj, partial);
						}

						if (searchValue is null)
						{
							return jArray.Any(t => t.Type == JTokenType.Null);
						}

						var searchStr = searchValue.ToString();
						return ArrayContainsMatch(jArray, searchStr, partial);
					}
				}
			case "ARRAY_LENGTH":
				{
					if (func.Arguments.Length < 1)
					{
						return null;
					}

					if (func.Arguments[0] is IdentifierExpression ident)
					{
						var path = ident.Name;
						if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
						{
							path = path[(fromAlias.Length + 1)..];
						}

						var token = item.SelectToken(path);
						return token is JArray arr ? (object)(long)arr.Count : UndefinedValue.Instance;
					}

					// Support nested function calls like ARRAY_LENGTH(SetIntersect(...))
					var evaluated = EvaluateSqlExpression(func.Arguments[0], item, fromAlias, parameters);
					return evaluated is JArray evalArr ? (object)(long)evalArr.Count : UndefinedValue.Instance;
				}
			case "ARRAY_SLICE":
				{
					if (func.Arguments.Length < 2)
					{
						return null;
					}

					if (func.Arguments[0] is IdentifierExpression ident)
					{
						var path = ident.Name;
						if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
						{
							path = path[(fromAlias.Length + 1)..];
						}

						var token = item.SelectToken(path);
						if (token is not JArray arr)
						{
							return null;
						}

						var start = (int)(ToLong(args[1]) ?? 0);
						if (start < 0)
						{
							start = Math.Max(0, arr.Count + start);
						}

						var length = args.Length >= 3 ? (int)(ToLong(args[2]) ?? arr.Count) : arr.Count;
						return new JArray(arr.Skip(start).Take(length));
					}

					// Support literal arrays and nested expressions like ARRAY_SLICE([1,2,3], 0, 2)
					var sliceEval = EvaluateSqlExpression(func.Arguments[0], item, fromAlias, parameters);
					if (sliceEval is JArray evalArr2)
					{
						var start2 = (int)(ToLong(args[1]) ?? 0);
						if (start2 < 0) start2 = Math.Max(0, evalArr2.Count + start2);
						var length2 = args.Length >= 3 ? (int)(ToLong(args[2]) ?? evalArr2.Count) : evalArr2.Count;
						return new JArray(evalArr2.Skip(start2).Take(length2));
					}
					return null;
				}
			case "ARRAY_CONCAT":
				{
					var result = new JArray();
					foreach (var argExpr in func.Arguments)
					{
						JArray ja = null;
						if (argExpr is IdentifierExpression ident)
						{
							var path = ident.Name;
							if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
							{
								path = path[(fromAlias.Length + 1)..];
							}

							ja = item.SelectToken(path) as JArray;
						}
						else
						{
							var evaluated = EvaluateSqlExpression(argExpr, item, fromAlias, parameters);
							ja = evaluated as JArray;
						}

						if (ja is null)
						{
							return UndefinedValue.Instance;
						}

						foreach (var el in ja)
						{
							result.Add(el.DeepClone());
						}
					}
					return result;
				}

			// ── Math functions ──
			case "ABS": return args.Length > 0 ? MathOp(args[0], Math.Abs) : null;
			case "CEILING": return args.Length > 0 ? MathOp(args[0], Math.Ceiling) : null;
			case "FLOOR": return args.Length > 0 ? MathOp(args[0], Math.Floor) : null;
			case "ROUND":
				if (args.Length >= 2 && double.TryParse(args[0]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var roundVal)
					&& double.TryParse(args[1]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var roundPrec))
				{
					var prec = (int)roundPrec;
					if (prec < 0)
					{
						var factor = Math.Pow(10, -prec);
						return Math.Round(roundVal / factor, MidpointRounding.AwayFromZero) * factor;
					}
					return Math.Round(roundVal, prec, MidpointRounding.AwayFromZero);
				}
				return args.Length > 0 ? MathOp(args[0], v => Math.Round(v, MidpointRounding.AwayFromZero)) : null;
			case "SQRT": return args.Length > 0 ? MathOp(args[0], Math.Sqrt) : null;
			case "SQUARE": return args.Length > 0 ? MathOp(args[0], v => v * v) : null;
			case "POWER": return args.Length >= 2 ? ArithmeticOp(args[0], args[1], Math.Pow) : null;
			case "EXP": return args.Length > 0 ? MathOp(args[0], Math.Exp) : null;
			case "LOG":
				if (args.Length >= 2 && double.TryParse(args[0]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var logVal)
					&& double.TryParse(args[1]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var logBase))
				{
					var logResult = Math.Log(logVal, logBase);
					if (double.IsNaN(logResult) || double.IsInfinity(logResult))
						return UndefinedValue.Instance;
					return logResult;
				}
				return args.Length > 0 ? MathOp(args[0], Math.Log) : null;
			case "LOG10": return args.Length > 0 ? MathOp(args[0], Math.Log10) : null;
			case "SIGN": return args.Length > 0 ? MathOp(args[0], v => Math.Sign(v)) : null;
			case "TRUNC": return args.Length > 0 ? MathOp(args[0], Math.Truncate) : null;
			case "PI": return Math.PI;
			case "SIN": return args.Length > 0 ? MathOp(args[0], Math.Sin) : null;
			case "COS": return args.Length > 0 ? MathOp(args[0], Math.Cos) : null;
			case "TAN": return args.Length > 0 ? MathOp(args[0], Math.Tan) : null;
			case "COT": return args.Length > 0 ? MathOp(args[0], v => 1.0 / Math.Tan(v)) : null;
			case "ASIN": return args.Length > 0 ? MathOp(args[0], Math.Asin) : null;
			case "ACOS": return args.Length > 0 ? MathOp(args[0], Math.Acos) : null;
			case "ATAN": return args.Length > 0 ? MathOp(args[0], Math.Atan) : null;
			case "ATN2": return args.Length >= 2 ? ArithmeticOp(args[0], args[1], Math.Atan2) : null;
			case "DEGREES": return args.Length > 0 ? MathOp(args[0], v => v * (180.0 / Math.PI)) : null;
			case "RADIANS": return args.Length > 0 ? MathOp(args[0], v => v * (Math.PI / 180.0)) : null;
			case "RAND": return Random.Shared.NextDouble();

			// ── Integer math functions ──
			case "NUMBERBIN":
				{
					if (args.Length < 2)
					{
						return null;
					}

					if (double.TryParse(args[0]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var val) &&
						double.TryParse(args[1]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var binSize) &&
						binSize > 0)
					{
						return Math.Floor(val / binSize) * binSize;
					}

					return UndefinedValue.Instance;
				}
			case "INTADD": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a + b) : null;
			case "INTSUB": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a - b) : null;
			case "INTMUL": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a * b) : null;
			case "INTDIV":
				{
					if (args.Length < 2)
					{
						return UndefinedValue.Instance;
					}

					var dividend = ToLong(args[0]);
					var divisor = ToLong(args[1]);
					if (!dividend.HasValue || !divisor.HasValue || divisor.Value == 0)
					{
						return UndefinedValue.Instance;
					}

					return dividend.Value / divisor.Value;
				}
			case "INTMOD":
				{
					if (args.Length < 2)
					{
						return UndefinedValue.Instance;
					}

					var dividend = ToLong(args[0]);
					var divisor = ToLong(args[1]);
					if (!dividend.HasValue || !divisor.HasValue || divisor.Value == 0)
					{
						return UndefinedValue.Instance;
					}

					return dividend.Value % divisor.Value;
				}
			case "INTBITAND": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a & b) : null;
			case "INTBITOR": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a | b) : null;
			case "INTBITXOR": return args.Length >= 2 ? BitwiseOp(args[0], args[1], (a, b) => a ^ b) : null;
			case "INTBITNOT":
				{
					if (args.Length == 0)
					{
						return null;
					}

					var value = ToLong(args[0]);
					return value.HasValue ? ~value.Value : null;
				}
			case "INTBITLEFTSHIFT": return args.Length >= 2 ? BitwiseShiftOp(args[0], args[1], isLeft: true) : null;
			case "INTBITRIGHTSHIFT": return args.Length >= 2 ? BitwiseShiftOp(args[0], args[1], isLeft: false) : null;

			// ── Aggregates (passthrough for non-GROUP-BY contexts) ──
			// When there is no GROUP BY, each document is emitted individually and the
			// SDK accumulates partial results client-side (cross-partition fan-out model).
			// COUNT always emits 1 per document (each document counts as one row).
			// SUM/MIN/MAX emit the field value so the SDK can aggregate across partitions.
			// AVG is handled by the SDK via sum+count, so SUM and COUNT passthroughs suffice.
			case "COUNT":
				return 1L;
			case "SUM":
			case "AVG":
			case "MIN":
			case "MAX":
				return args.Length > 0 ? args[0] : null;

			// ── Conditional functions ──
			case "IIF":
				{
					if (args.Length < 3)
					{
						return null;
					}

					// Real Cosmos DB IIF only treats boolean true as truthy.
					// Non-boolean values (numbers, strings, arrays, objects) always yield the false branch.
					return args[0] is bool b && b ? args[1] : args[2];
				}

			// ── Extended array functions ──
			case "ARRAY_CONTAINS_ANY":
				{
					if (func.Arguments.Length < 2)
					{
						return false;
					}

					var sourceArray = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
					if (sourceArray is null || sourceArray.Count == 0)
					{
						return false;
					}

					// Support both array form: ARRAY_CONTAINS_ANY(c.tags, ['a','b'])
					// and variadic form: ARRAY_CONTAINS_ANY(c.tags, 'a', 'b')
					var searchArray = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
					if (searchArray is null && func.Arguments.Length >= 2)
					{
						searchArray = new JArray();
						for (int i = 1; i < func.Arguments.Length; i++)
						{
							var val = EvaluateSqlExpression(func.Arguments[i], item, fromAlias, parameters);
							if (val is UndefinedValue) continue;
							searchArray.Add(val is JToken jt ? jt : (val is null ? JValue.CreateNull() : JToken.FromObject(val)));
						}
					}

					if (searchArray is null || searchArray.Count == 0)
					{
						return false;
					}

					var sourceValues = new HashSet<JToken>(sourceArray, JTokenValueComparer.Instance);
					return searchArray.Any(t => sourceValues.Contains(t));
				}
			case "ARRAY_CONTAINS_ALL":
				{
					if (func.Arguments.Length < 2)
					{
						return false;
					}

					var sourceArray = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
					if (sourceArray is null)
					{
						return false;
					}

					// Support both array form: ARRAY_CONTAINS_ALL(c.tags, ['a','b'])
					// and variadic form: ARRAY_CONTAINS_ALL(c.tags, 'a', 'b')
					var searchArray = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
					if (searchArray is null && func.Arguments.Length >= 2)
					{
						searchArray = new JArray();
						for (int i = 1; i < func.Arguments.Length; i++)
						{
							var val = EvaluateSqlExpression(func.Arguments[i], item, fromAlias, parameters);
							if (val is UndefinedValue) continue;
							searchArray.Add(val is JToken jt ? jt : (val is null ? JValue.CreateNull() : JToken.FromObject(val)));
						}
					}

					if (searchArray is null || searchArray.Count == 0)
					{
						return true;
					}

					var sourceValues = new HashSet<JToken>(sourceArray, JTokenValueComparer.Instance);
					return searchArray.All(t => sourceValues.Contains(t));
				}
			case "SETINTERSECT":
				{
					if (func.Arguments.Length < 2)
					{
						return new JArray();
					}

					var arr1 = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
					var arr2 = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
					if (arr1 is null || arr2 is null)
					{
						return UndefinedValue.Instance;
					}

					var set2 = new HashSet<JToken>(arr2, JTokenValueComparer.Instance);
					var result = new JArray();
					var seen = new HashSet<JToken>(JTokenValueComparer.Instance);
					foreach (var element in arr1)
					{
						if (set2.Contains(element) && seen.Add(element))
						{
							result.Add(element.DeepClone());
						}
					}
					return result;
				}
			case "SETUNION":
				{
					if (func.Arguments.Length < 2)
					{
						return new JArray();
					}

					var arr1 = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
					var arr2 = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
					if (arr1 is null || arr2 is null)
					{
						return UndefinedValue.Instance;
					}
					var result = new JArray();
					var seen = new HashSet<JToken>(JTokenValueComparer.Instance);
					foreach (var element in arr1)
					{
						if (seen.Add(element))
						{
							result.Add(element.DeepClone());
						}
					}
					foreach (var element in arr2)
					{
						if (seen.Add(element))
						{
							result.Add(element.DeepClone());
						}
					}
					return result;
				}
			case "SETDIFFERENCE":
				{
					if (func.Arguments.Length < 2)
					{
						return new JArray();
					}

					var arr1 = ResolveJArray(func.Arguments[0], item, fromAlias, parameters);
					var arr2 = ResolveJArray(func.Arguments[1], item, fromAlias, parameters);
					if (arr1 is null || arr2 is null)
					{
						return UndefinedValue.Instance;
					}

					var exclude = new HashSet<JToken>(arr2, JTokenValueComparer.Instance);
					var result = new JArray();
					var seen = new HashSet<JToken>(JTokenValueComparer.Instance);
					foreach (var element in arr1)
					{
						if (!exclude.Contains(element) && seen.Add(element))
						{
							result.Add(element.DeepClone());
						}
					}
					return result;
				}

			// ── Array/Object utility functions ──
			case "CHOOSE":
				{
					if (args.Length < 2) return UndefinedValue.Instance;
					var idx = ToLong(args[0]);
					if (!idx.HasValue || idx.Value < 1 || idx.Value >= args.Length) return UndefinedValue.Instance;
					return args[(int)idx.Value];
				}
			case "OBJECTTOARRAY":
				{
					if (args.Length < 1) return null;
					JObject obj;
					if (args[0] is JObject jo) obj = jo;
					else if (args[0] is string s) { try { obj = JObject.Parse(s); } catch { return UndefinedValue.Instance; } }
					else return UndefinedValue.Instance;
					var result = new JArray();
					foreach (var prop in obj.Properties())
					{
						result.Add(new JObject { ["k"] = prop.Name, ["v"] = prop.Value.DeepClone() });
					}
					return result;
				}
			case "ARRAYTOOBJECT":
				{
					if (args.Length < 1) return UndefinedValue.Instance;
					JArray arr;
					if (args[0] is JArray ja) arr = ja;
					else if (args[0] is string s) { try { arr = JArray.Parse(s); } catch { return UndefinedValue.Instance; } }
					else return UndefinedValue.Instance;
					var result = new JObject();
					foreach (var element in arr)
					{
						if (element is JObject kvObj && kvObj["k"] != null && kvObj["v"] != null)
						{
							result[kvObj["k"]!.Value<string>()] = kvObj["v"]!.DeepClone();
						}
						else
						{
							return UndefinedValue.Instance;
						}
					}
					return result;
				}

			// ── String utility functions ──
			case "STRINGJOIN":
				{
					if (args.Length < 2) return null;
					var separator = args[1]?.ToString();
					if (separator is null) return null;
					JArray joinArr;
					if (args[0] is JArray ja) joinArr = ja;
					else if (args[0] is string s) { try { joinArr = JArray.Parse(s); } catch { return null; } }
					else return null;
					return string.Join(separator, joinArr.Select(t => t.Value<string>()));
				}
			case "STRINGSPLIT":
				{
					if (args.Length < 2) return null;
					var input = args[0]?.ToString();
					var delimiter = args[1]?.ToString();
					if (input is null || delimiter is null) return UndefinedValue.Instance;
					if (delimiter.Length == 0) return UndefinedValue.Instance;
					var parts = input.Split(delimiter);
					return new JArray(parts.Select(p => (JToken)p));
				}

			// ── Item functions ──
			case "DOCUMENTID":
				{
					// DOCUMENTID returns the _rid system property of the current document.
					// The emulator synthesises from id since it doesn't generate _rid.
					return item["_rid"]?.Value<string>() ?? item["id"]?.Value<string>();
				}

			// ── Full-text search functions (approximate) ──
			// These use case-insensitive substring matching instead of real NLP tokenization.
			case "FULLTEXTCONTAINS":
				{
					if (args.Length < 2) return false;
					var text = args[0]?.ToString();
					var term = args[1]?.ToString();
					if (text is null || term is null) return false;
					return text.Contains(term, StringComparison.OrdinalIgnoreCase);
				}
			case "FULLTEXTCONTAINSALL":
				{
					if (args.Length < 2) return false;
					var text = args[0]?.ToString();
					if (text is null) return false;
					for (var i = 1; i < args.Length; i++)
					{
						var term = args[i]?.ToString();
						if (term is null || !text.Contains(term, StringComparison.OrdinalIgnoreCase))
							return false;
					}
					return true;
				}
			case "FULLTEXTCONTAINSANY":
				{
					if (args.Length < 2) return false;
					var text = args[0]?.ToString();
					if (text is null) return false;
					for (var i = 1; i < args.Length; i++)
					{
						var term = args[i]?.ToString();
						if (term is not null && text.Contains(term, StringComparison.OrdinalIgnoreCase))
							return true;
					}
					return false;
				}
			case "FULLTEXTSCORE":
				{
					// Naive term-frequency scoring: count occurrences of each search term
					// in the field text. Returns the total count as a double.
					// Real Cosmos DB uses BM25 with IDF and length normalization.
					if (args.Length < 2) return 0.0;
					var text = args[0]?.ToString();
					if (text is null) return 0.0;

					var score = 0.0;
					// arg[1] may be a JArray (from [...] literal) or individual terms
					if (args[1] is JArray searchTerms)
					{
						foreach (var termToken in searchTerms)
						{
							var term = termToken.Value<string>();
							if (term is not null)
								score += CountOccurrences(text, term);
						}
					}
					else
					{
						for (var i = 1; i < args.Length; i++)
						{
							var term = args[i]?.ToString();
							if (term is not null)
								score += CountOccurrences(text, term);
						}
					}
					return score;
				}

			// ── Spatial functions ──
			case "ST_DISTANCE":
				return args.Length >= 2 ? StDistance(args[0], args[1]) : null;
			case "ST_WITHIN":
				return args.Length >= 2 ? (object)StWithin(args[0], args[1]) : null;
			case "ST_INTERSECTS":
				return args.Length >= 2 ? (object)StIntersects(args[0], args[1]) : null;
			case "ST_ISVALID":
				return args.Length >= 1 ? (object)StIsValid(args[0]) : false;
			case "ST_ISVALIDDETAILED":
				return args.Length >= 1 ? StIsValidDetailed(args[0]) : null;
			case "ST_AREA":
				return args.Length >= 1 ? StArea(args[0]) : null;

			// ── Vector functions ──
			case "VECTORDISTANCE":
				return args.Length >= 2 ? VectorDistanceFunc(args) : null;

			// ── Date/time functions ──
			case "GETCURRENTDATETIME":
			case "GETCURRENTDATETIMESTATIC": return parameters.TryGetValue("__staticDateTime", out var sdt) ? sdt : DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
			case "GETCURRENTTIMESTAMP":
			case "GETCURRENTTIMESTAMPSTATIC": return parameters.TryGetValue("__staticTimestamp", out var sts) ? sts : (object)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
			case "GETCURRENTTICKS":
			case "GETCURRENTTICKSSTATIC": return parameters.TryGetValue("__staticTicks", out var stk) ? stk : (object)DateTime.UtcNow.Ticks;
			case "DATETIMEADD":
				{
					if (args.Length < 3)
					{
						return UndefinedValue.Instance;
					}

					var part = args[0]?.ToString()?.ToLowerInvariant();
					var number = ToLong(args[1]);
					var dateTime = args[2] is not null and not UndefinedValue ? args[2].ToString() : null;
					if (part is null || !number.HasValue || dateTime is null)
					{
						return UndefinedValue.Instance;
					}

					if (!DateTime.TryParse(dateTime, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt))
					{
						return UndefinedValue.Instance;
					}

					var n = (int)number.Value;
					try
					{
						DateTime? result = part switch
						{
							"year" or "yyyy" or "yy" => dt.AddYears(n),
							"month" or "mm" or "m" => dt.AddMonths(n),
							"day" or "dd" or "d" => dt.AddDays(n),
							"hour" or "hh" => dt.AddHours(n),
							"minute" or "mi" or "n" => dt.AddMinutes(n),
							"second" or "ss" or "s" => dt.AddSeconds(n),
							"millisecond" or "ms" => dt.AddMilliseconds(n),
							"microsecond" or "mcs" => dt.AddTicks(n * 10L),
							"nanosecond" or "ns" => dt.AddTicks(n / 100L),
							_ => null,
						};
						if (result is null) return UndefinedValue.Instance;
						return result.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
					}
					catch (ArgumentOutOfRangeException)
					{
						return UndefinedValue.Instance;
					}
				}
			case "DATETIMEPART":
				{
					if (args.Length < 2)
					{
						return UndefinedValue.Instance;
					}

					var part = args[0]?.ToString()?.ToLowerInvariant();
					var dateTime = args[1] is not null and not UndefinedValue ? args[1].ToString() : null;
					if (part is null || dateTime is null)
					{
						return UndefinedValue.Instance;
					}

					if (!DateTime.TryParse(dateTime, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt))
					{
						return UndefinedValue.Instance;
					}

					return part switch
					{
						"year" or "yyyy" or "yy" => (object)(long)dt.Year,
						"month" or "mm" or "m" => (long)dt.Month,
						"day" or "dd" or "d" => (long)dt.Day,
						"hour" or "hh" => (long)dt.Hour,
						"minute" or "mi" or "n" => (long)dt.Minute,
						"second" or "ss" or "s" => (long)dt.Second,
						"millisecond" or "ms" => (long)dt.Millisecond,
						"microsecond" or "mcs" => (long)(dt.Ticks % TimeSpan.TicksPerSecond / 10),
						"nanosecond" or "ns" => (long)(dt.Ticks % TimeSpan.TicksPerSecond * 100),
						"weekday" or "dw" or "w" => (long)(dt.DayOfWeek + 1), // Sunday=1..Saturday=7
						_ => null,
					};
				}
			case "DATETIMEDIFF":
				{
					if (args.Length < 3) return UndefinedValue.Instance;
					var part = args[0]?.ToString()?.ToLowerInvariant();
					var startStr = args[1]?.ToString();
					var endStr = args[2]?.ToString();
					if (part is null || startStr is null || endStr is null) return UndefinedValue.Instance;
					if (!DateTime.TryParse(startStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dtStart)) return UndefinedValue.Instance;
					if (!DateTime.TryParse(endStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dtEnd)) return UndefinedValue.Instance;

					return part switch
					{
						"year" or "yyyy" or "yy" => (object)(long)(dtEnd.Year - dtStart.Year),
						"month" or "mm" or "m" => (long)((dtEnd.Year - dtStart.Year) * 12 + dtEnd.Month - dtStart.Month),
						"day" or "dd" or "d" => (long)(dtEnd.Date - dtStart.Date).TotalDays,
						"hour" or "hh" => (long)(FloorToUnit(dtEnd, TimeSpan.TicksPerHour) - FloorToUnit(dtStart, TimeSpan.TicksPerHour)),
						"minute" or "mi" or "n" => (long)(FloorToUnit(dtEnd, TimeSpan.TicksPerMinute) - FloorToUnit(dtStart, TimeSpan.TicksPerMinute)),
						"second" or "ss" or "s" => (long)(FloorToUnit(dtEnd, TimeSpan.TicksPerSecond) - FloorToUnit(dtStart, TimeSpan.TicksPerSecond)),
						"millisecond" or "ms" => (long)(FloorToUnit(dtEnd, TimeSpan.TicksPerMillisecond) - FloorToUnit(dtStart, TimeSpan.TicksPerMillisecond)),
						"microsecond" or "mcs" => (long)((dtEnd - dtStart).Ticks / 10),
						"nanosecond" or "ns" => (long)((dtEnd - dtStart).Ticks * 100),
						_ => UndefinedValue.Instance,
					};
				}
			case "DATETIMEFROMPARTS":
				{
					if (args.Length < 3) return UndefinedValue.Instance;
					var y = ToLong(args[0]);
					var mo = ToLong(args[1]);
					var d = ToLong(args[2]);
					var h = args.Length > 3 ? ToLong(args[3]) : 0;
					var mi = args.Length > 4 ? ToLong(args[4]) : 0;
					var s = args.Length > 5 ? ToLong(args[5]) : 0;
					var fraction = args.Length > 6 ? ToLong(args[6]) : 0;
					if (!y.HasValue || !mo.HasValue || !d.HasValue || !h.HasValue || !mi.HasValue || !s.HasValue || !fraction.HasValue) return UndefinedValue.Instance;
					if (fraction.Value < 0 || fraction.Value > 9999999) return UndefinedValue.Instance;
					try
					{
						var dt = new DateTime((int)y.Value, (int)mo.Value, (int)d.Value,
							(int)h.Value, (int)mi.Value, (int)s.Value, DateTimeKind.Utc).AddTicks(fraction.Value);
						return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
					}
					catch (ArgumentOutOfRangeException)
					{
						return UndefinedValue.Instance;
					}
				}
			case "DATETIMEBIN":
				{
					if (args.Length < 2) return UndefinedValue.Instance;
					var dtStr = args[0] is not null and not UndefinedValue ? args[0].ToString() : null;
					var part = args[1]?.ToString()?.ToLowerInvariant();
					var binSize = args.Length >= 3 ? ToLong(args[2]) : 1;
					if (dtStr is null || part is null || !binSize.HasValue) return UndefinedValue.Instance;
					if (!DateTime.TryParse(dtStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt)) return UndefinedValue.Instance;

					var bs = (int)binSize.Value;
					if (bs <= 0) return UndefinedValue.Instance;

					DateTime origin;
					if (args.Length >= 4)
					{
						if (args[3] is not string originStr || !DateTime.TryParse(originStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out origin))
							return UndefinedValue.Instance;
					}
					else
					{
						origin = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
					}

					if (origin.Year < 1601) return UndefinedValue.Instance;

					if (part is "year" or "yyyy" or "yy")
					{
						var yearBin = (int)(Math.Floor((double)(dt.Year - origin.Year) / bs) * bs);
						dt = new DateTime(origin.Year + yearBin, 1, 1, 0, 0, 0, DateTimeKind.Utc);
					}
					else if (part is "month" or "mm" or "m")
					{
						var totalMonths = (dt.Year - origin.Year) * 12 + (dt.Month - origin.Month);
						var binned = (int)(Math.Floor((double)totalMonths / bs) * bs);
						dt = new DateTime(origin.Year, origin.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(binned);
					}
					else
					{
						var ticksPerUnit = part switch
						{
							"day" or "dd" or "d" => TimeSpan.TicksPerDay,
							"hour" or "hh" => TimeSpan.TicksPerHour,
							"minute" or "mi" or "n" => TimeSpan.TicksPerMinute,
							"second" or "ss" or "s" => TimeSpan.TicksPerSecond,
							"millisecond" or "ms" => TimeSpan.TicksPerMillisecond,
							"microsecond" or "mcs" => 10L,
							"nanosecond" or "ns" => 1L,
							_ => -1L,
						};
						if (ticksPerUnit < 0) return UndefinedValue.Instance;
						var tickSpan = (dt - origin).Ticks;
						var binTicks = ticksPerUnit * bs;
						var binnedTicks = (long)Math.Floor((double)tickSpan / binTicks) * binTicks;
						dt = origin.AddTicks(binnedTicks);
					}
					return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
				}
			case "DATETIMETOTICKS":
				{
					if (args.Length < 1) return null;
					var dtStr = args[0]?.ToString();
					if (dtStr is null) return UndefinedValue.Instance;
					if (!DateTime.TryParse(dtStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt)) return UndefinedValue.Instance;
					if (dt.Kind == DateTimeKind.Unspecified) dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
					return (object)dt.ToUniversalTime().Ticks;
				}
			case "TICKSTODATETIME":
				{
					if (args.Length < 1) return UndefinedValue.Instance;
					var ticks = ToLong(args[0]);
					if (!ticks.HasValue) return UndefinedValue.Instance;
					try
					{
						var dt = new DateTime(ticks.Value, DateTimeKind.Utc);
						return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
					}
					catch (ArgumentOutOfRangeException)
					{
						return UndefinedValue.Instance;
					}
				}
			case "DATETIMETOTIMESTAMP":
				{
					if (args.Length < 1) return UndefinedValue.Instance;
					var dtStr = args[0]?.ToString();
					if (dtStr is null) return UndefinedValue.Instance;
					if (!DateTime.TryParse(dtStr, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt)) return UndefinedValue.Instance;
					if (dt.Kind == DateTimeKind.Unspecified) dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
					return (object)new DateTimeOffset(dt.ToUniversalTime()).ToUnixTimeMilliseconds();
				}
			case "TIMESTAMPTODATETIME":
				{
					if (args.Length < 1) return UndefinedValue.Instance;
					var ms = ToLong(args[0]);
					if (!ms.HasValue) return UndefinedValue.Instance;
					var dt = DateTimeOffset.FromUnixTimeMilliseconds(ms.Value).UtcDateTime;
					return dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ");
				}
			// ── COALESCE function ──
			// COALESCE skips null and undefined, returning the first concrete value.
			// When all args are exhausted: returns null if any arg was null,
			// otherwise returns undefined (omitted from results).
			case "COALESCE":
				{
					// COALESCE returns the first expression that is NOT undefined.
					// null is a defined value and should be returned (unlike ?? which skips both null and undefined).
					foreach (var arg in args)
					{
						if (arg is UndefinedValue)
							continue;
						return arg; // returns first non-undefined value (including null)
					}

					return UndefinedValue.Instance;
				}

			default:
				if (func.FunctionName.StartsWith("UDF.", StringComparison.OrdinalIgnoreCase))
				{
					var udfName = func.FunctionName.Substring(4); // strip "UDF." prefix

					// JavaScript has no int/float distinction — all numbers are IEEE 754 doubles.
					// Normalise any long values to double so C# UDF handlers can safely cast args to double.
					var jsArgs = args.Select(a => a is long l ? (object)(double)l : a).ToArray();

					// Priority 1: C# handler (skip placeholder delegates)
					if (parameters.TryGetValue(UdfRegistryKey, out var registry) &&
						registry is Dictionary<string, Func<object[], object>> udfs &&
						udfs.TryGetValue(func.FunctionName, out var udfImpl) &&
						!ReferenceEquals(udfImpl, UdfPlaceholder))
					{
						return udfImpl(jsArgs);
					}

					// Priority 2: JS body via engine
					if (parameters.TryGetValue(UdfPropertiesKey, out var propsObj) &&
						propsObj is Dictionary<string, UserDefinedFunctionProperties> udfProps &&
						udfProps.TryGetValue(udfName, out var udfProp) &&
						udfProp.Body is not null &&
						parameters.TryGetValue(UdfEngineKey, out var engineObj) &&
						engineObj is IJsUdfEngine jsUdfEngine)
					{
						return jsUdfEngine.ExecuteUdf(udfProp.Body, jsArgs);
					}

					throw new NotSupportedException(
						$"Unregistered user-defined function: {func.FunctionName}. " +
						"Call RegisterUdf() on the InMemoryContainer to register it before querying.");
				}

				throw new NotSupportedException($"Unsupported Cosmos SQL function: {func.FunctionName}");
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Utility helpers
	// ═══════════════════════════════════════════════════════════════════════════

	private static bool IsTruthy(object value) => value switch
	{
		null => false,
		UndefinedValue => false,
		bool b => b,
		// Cosmos DB: WHERE requires strict boolean — non-boolean values evaluate to false
		_ => false
	};

	private static int CountOccurrences(string text, string term)
	{
		if (term.Length == 0) return 0;
		var count = 0;
		var idx = 0;
		while ((idx = text.IndexOf(term, idx, StringComparison.OrdinalIgnoreCase)) >= 0)
		{
			count++;
			idx += term.Length;
		}
		return count;
	}

	private static JObject EvaluateObjectLiteral(
		ObjectLiteralExpression obj, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var result = new JObject();
		foreach (var prop in obj.Properties)
		{
			var value = EvaluateSqlExpression(prop.Value, item, fromAlias, parameters);
			if (value is not UndefinedValue)
				result[prop.Key] = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
		}
		return result;
	}

	private static JArray EvaluateArrayLiteral(
		ArrayLiteralExpression arr, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var result = new JArray();
		foreach (var element in arr.Elements)
		{
			var value = EvaluateSqlExpression(element, item, fromAlias, parameters);
			result.Add(value is not null and not UndefinedValue ? JToken.FromObject(value) : JValue.CreateNull());
		}
		return result;
	}

	private static object EvaluateSubquery(
		CosmosSqlQuery subquery, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var results = ExecuteSubqueryAgainstItem(subquery, item, fromAlias, parameters);
		if (subquery.IsValueSelect && results.Count > 0)
		{
			return JsonParseHelpers.ParseJsonToken(results[0]).ToObject<object>();
		}

		return results.Count > 0 ? JsonParseHelpers.ParseJsonToken(results[0]).ToObject<object>() : null;
	}

	private static object EvaluateSubqueryAggregate(
		FunctionCallExpression func, List<JObject> sourceItems, string fromAlias, IDictionary<string, object> parameters)
	{
		var name = func.FunctionName.ToUpperInvariant();
		if (name is "COUNT")
		{
			// COUNT(1) / COUNT(*) → count all items; COUNT(c.field) → count defined only
			if (func.Arguments.Length == 1 && func.Arguments[0] is IdentifierExpression ident)
			{
				var fieldPath = ident.Name;
				if (fieldPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
					fieldPath = fieldPath[(fromAlias.Length + 1)..];
				return (double)sourceItems.Count(si => si.SelectToken(fieldPath) is not null);
			}
			return (double)sourceItems.Count;
		}

		// Ref: https://github.com/Azure/azure-cosmos-dotnet-v3/pull/4738
		//   COUNTIF(<bool_expr>) counts items where the expression evaluates to true.
		if (name is "COUNTIF")
		{
			if (func.Arguments.Length < 1) return 0.0;
			var boolExpr = func.Arguments[0];
			return (double)sourceItems.Count(si =>
			{
				var val = EvaluateSqlExpression(boolExpr, si, fromAlias, parameters);
				return IsTruthy(val);
			});
		}

		// For SUM/AVG/MIN/MAX, extract numeric values from the first argument
		var argExpr = func.Arguments.Length > 0 ? func.Arguments[0] : null;
		if (argExpr is null) return null;

		var values = new List<JToken>();
		foreach (var si in sourceItems)
		{
			var val = EvaluateSqlExpression(argExpr, si, fromAlias, parameters);
			if (val is not null and not UndefinedValue)
				values.Add(JToken.FromObject(val));
		}

		return name switch
		{
			"SUM" => values.Count > 0 ? values.Sum(v => v.Value<double>()) : (object)UndefinedValue.Instance,
			"AVG" => values.Count > 0 ? values.Average(v => v.Value<double>()) : (object)UndefinedValue.Instance,
			"MIN" => AggregateMinMax(values, true),
			"MAX" => AggregateMinMax(values, false),
			_ => null
		};
	}

	private static object EvaluateArraySubquery(
		CosmosSqlQuery subquery, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		var results = ExecuteSubqueryAgainstItem(subquery, item, fromAlias, parameters);
		var jArray = new JArray();
		foreach (var resultJson in results)
		{
			jArray.Add(JsonParseHelpers.ParseJsonToken(resultJson));
		}
		return jArray;
	}

	private static List<string> ExecuteSubqueryAgainstItem(
		CosmosSqlQuery subquery, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		// Determine the data to iterate over
		List<JObject> sourceItems;

		if (subquery.FromSource is not null)
		{
			// FROM alias IN path — expand the array at path, aliasing each element
			var sourcePath = subquery.FromSource;
			if (sourcePath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
			{
				sourcePath = sourcePath[(fromAlias.Length + 1)..];
			}

			var arrayToken = item.SelectToken(sourcePath);
			if (arrayToken is not JArray sourceArray)
			{
				return [];
			}

			sourceItems = [];
			foreach (var element in sourceArray)
			{
				var combined = new JObject(item.Properties())
				{
					[subquery.FromAlias] = element
				};
				sourceItems.Add(combined);
			}
		}
		else if (subquery.Join is not null)
		{
			// Correlated subquery with JOIN — expand the join array from the parent item
			var arrayPath = subquery.Join.ArrayField;
			var sourceAlias = subquery.Join.SourceAlias;
			JToken arrayToken;

			if (sourceAlias.Equals(fromAlias, StringComparison.OrdinalIgnoreCase) ||
				sourceAlias.Equals(subquery.FromAlias, StringComparison.OrdinalIgnoreCase))
			{
				arrayToken = item.SelectToken(arrayPath);
			}
			else
			{
				arrayToken = item.SelectToken($"{sourceAlias}.{arrayPath}");
			}

			if (arrayToken is not JArray jArray)
			{
				return [];
			}

			sourceItems = [];
			foreach (var element in jArray)
			{
				var combined = new JObject(item.Properties())
				{
					[subquery.Join.Alias] = element
				};
				sourceItems.Add(combined);
			}
		}
		else
		{
			// Non-correlated — treat the current item as the sole source
			sourceItems = [item];
		}

		// Apply WHERE filter
		if (subquery.Where is not null)
		{
			sourceItems = sourceItems.Where(sourceItem =>
				EvaluateWhereExpression(subquery.Where, sourceItem, subquery.FromAlias, parameters, subquery.Join)
			).ToList();
		}

		// Apply SELECT projection
		// Check if any SELECT field contains an aggregate function — if so, collapse into a single row
		var hasAggregateSelect = !subquery.IsSelectAll &&
			subquery.SelectFields.Any(f => f.SqlExpr is not null && ContainsAggregateCall(f.SqlExpr));

		var results = new List<string>();
		if (hasAggregateSelect)
		{
			// Aggregate subquery: compute aggregates over all sourceItems and return one row
			var projected = new JObject();
			foreach (var field in subquery.SelectFields)
			{
				var outputName = field.Alias ?? field.Expression.Split('.').Last();
				if (field.SqlExpr is FunctionCallExpression func && AggregateFunctions.Contains(func.FunctionName))
				{
					var aggValue = EvaluateSubqueryAggregate(func, sourceItems, subquery.FromAlias, parameters);
					if (aggValue is not null and not UndefinedValue)
						projected[outputName] = JToken.FromObject(aggValue);
				}
				else if (field.SqlExpr is not null)
				{
					// Non-aggregate expression in an aggregate query — evaluate against first item
					if (sourceItems.Count > 0)
					{
						var value = EvaluateSqlExpression(field.SqlExpr, sourceItems[0], subquery.FromAlias, parameters);
						if (value is not null and not UndefinedValue)
							projected[outputName] = JToken.FromObject(value);
					}
				}
			}

			if (subquery.IsValueSelect)
			{
				var first = projected.Properties().FirstOrDefault();
				if (first?.Value is not null)
				{
					results.Add(first.Value.Type == JTokenType.String
						? JsonConvert.SerializeObject(first.Value.Value<string>())
						: first.Value.ToString(Formatting.None));
				}
			}
			else
			{
				results.Add(projected.ToString(Formatting.None));
			}
		}
		else
		{
			foreach (var sourceItem in sourceItems)
			{
				if (subquery.IsSelectAll)
				{
					results.Add(sourceItem.ToString(Formatting.None));
				}
				else
				{
					var projected = new JObject();
					foreach (var field in subquery.SelectFields)
					{
						var outputName = field.Alias ?? field.Expression.Split('.').Last();
						if (field.SqlExpr is not null and not IdentifierExpression)
						{
							var value = EvaluateSqlExpression(field.SqlExpr, sourceItem, subquery.FromAlias, parameters);
							if (value is not UndefinedValue)
								projected[outputName] = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
						}
						else
						{
							var path = field.Expression;
							if (string.Equals(path, subquery.FromAlias, StringComparison.OrdinalIgnoreCase))
							{
								// For range variables (FROM t IN c.tags), use the aliased element
								var aliasToken = sourceItem[subquery.FromAlias];
								projected[outputName] = aliasToken is not null
									? aliasToken.DeepClone()
									: sourceItem.DeepClone();
								continue;
							}
							if (path.StartsWith(subquery.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
							{
								path = path[(subquery.FromAlias.Length + 1)..];
							}
							var token = sourceItem.SelectToken(path);
							outputName = field.Alias ?? path.Split('.').Last();
							projected[outputName] = token?.DeepClone();
						}
					}

					if (subquery.IsValueSelect)
					{
						var first = projected.Properties().FirstOrDefault();
						if (first?.Value is not null)
						{
							results.Add(first.Value.Type == JTokenType.String
								? JsonConvert.SerializeObject(first.Value.Value<string>())
								: first.Value.ToString(Formatting.None));
						}
					}
					else
					{
						results.Add(projected.ToString(Formatting.None));
					}
				}
			}
		} // end else (non-aggregate)

		// Apply DISTINCT
		if (subquery.IsDistinct)
		{
			results = results.Distinct(JsonStructuralStringComparer.Instance).ToList();
		}

		// Apply ORDER BY (must happen before TOP so TOP takes from sorted results)
		if (subquery.OrderByFields is { Length: > 0 })
		{
			IOrderedEnumerable<string> ordered = null;
			foreach (var field in subquery.OrderByFields)
			{
				var fieldPath = field.Field;
				if (fieldPath.StartsWith(subquery.FromAlias + ".", StringComparison.OrdinalIgnoreCase))
					fieldPath = fieldPath[(subquery.FromAlias.Length + 1)..];

				object KeySelector(string json)
				{
					var token = JsonParseHelpers.ParseJsonToken(json);
					// For SELECT VALUE results, the token is the scalar value itself
					if (token is not JObject obj)
					{
						// If the ORDER BY field matches the FROM alias, use the raw value
						return token.Type switch
						{
							JTokenType.Integer => token.Value<long>(),
							JTokenType.Float => token.Value<double>(),
							_ => (object)token.ToString()
						};
					}
					var selected = obj.SelectToken(fieldPath);
					if (selected is null) return null;
					return selected.Type switch
					{
						JTokenType.Integer => selected.Value<long>(),
						JTokenType.Float => selected.Value<double>(),
						_ => (object)selected.ToString()
					};
				}

				var asc = field.Ascending;
				var comparer = Comparer<object>.Create((l, r) =>
				{
					var result = CompareValues(l, r);
					return asc ? result : -result;
				});
				ordered = ordered == null ? results.OrderBy(KeySelector, comparer) : ordered.ThenBy(KeySelector, comparer);
			}
			results = ordered?.ToList() ?? results;
		}

		// Apply TOP (after ORDER BY so we take from sorted results)
		if (subquery.TopCount.HasValue)
		{
			results = results.Take(subquery.TopCount.Value).ToList();
		}

		// Apply OFFSET
		if (subquery.Offset.HasValue)
		{
			results = results.Skip(subquery.Offset.Value).ToList();
		}

		// Apply LIMIT
		if (subquery.Limit.HasValue)
		{
			results = results.Take(subquery.Limit.Value).ToList();
		}

		return results;
	}

	private static object ArithmeticOp(object left, object right, Func<double, double, double> op)
	{
		if (left is null or UndefinedValue || right is null or UndefinedValue)
		{
			return UndefinedValue.Instance;
		}

		if (double.TryParse(left.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var l) &&
			double.TryParse(right.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var r))
		{
			var result = op(l, r);
			// Cosmos DB treats NaN and Infinity as undefined
			if (double.IsNaN(result) || double.IsInfinity(result))
				return UndefinedValue.Instance;
			// Only convert back to long when both operands were integers and the result
			// fits within long range. We use strict < for MaxValue because (double)long.MaxValue
			// rounds up beyond the actual max, causing (long) cast to overflow.
			if (left is long or int && right is long or int && result == Math.Floor(result)
				&& result >= long.MinValue && result < 9.2233720368547758E+18)
			{
				return (long)result;
			}

			return result;
		}

		return null;
	}

	private static object BitwiseOp(object left, object right, Func<long, long, long> op)
	{
		if (left is null or UndefinedValue || right is null or UndefinedValue)
		{
			return UndefinedValue.Instance;
		}

		if (long.TryParse(left.ToString(), out var l) && long.TryParse(right.ToString(), out var r))
		{
			return op(l, r);
		}

		return UndefinedValue.Instance;
	}

	private static object BitwiseShiftOp(object left, object right, bool isLeft)
	{
		if (left is null or UndefinedValue || right is null or UndefinedValue)
		{
			return UndefinedValue.Instance;
		}

		if (long.TryParse(left.ToString(), out var l) && long.TryParse(right.ToString(), out var r))
		{
			if (r < 0 || r >= 64) return UndefinedValue.Instance;
			return isLeft ? l << (int)r : l >> (int)r;
		}

		return UndefinedValue.Instance;
	}

	private static object MathOp(object value, Func<double, double> op)
	{
		if (value is null or UndefinedValue)
		{
			return UndefinedValue.Instance;
		}

		var isIntegerInput = value is long or int;

		if (double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var n))
		{
			var result = op(n);
			// Cosmos DB treats NaN and Infinity as undefined
			if (double.IsNaN(result) || double.IsInfinity(result))
				return UndefinedValue.Instance;
			// Preserve integer type when input was integer and result is a whole number
			if (isIntegerInput && result == Math.Floor(result) && result is >= long.MinValue and <= long.MaxValue)
				return (long)result;
			return result;
		}

		return UndefinedValue.Instance;
	}

	private static long? ToLong(object value) => value switch
	{
		long l => l,
		double d => (long)d,
		int i => i,
		_ when value != null && long.TryParse(value.ToString(), out var p) => p,
		_ => null
	};

	private static long FloorToUnit(DateTime dt, long ticksPerUnit)
		=> dt.Ticks / ticksPerUnit;

	private static JToken ResolveTokenType(SqlExpression[] arguments, JObject item, string fromAlias)
	{
		if (arguments.Length < 1)
		{
			return null;
		}

		if (arguments[0] is IdentifierExpression ident)
		{
			var path = ident.Name;
			if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
			{
				path = path[(fromAlias.Length + 1)..];
			}

			return item.SelectToken(path);
		}
		return null;
	}

	private static JArray ResolveJArray(
		SqlExpression argument, JObject item, string fromAlias, IDictionary<string, object> parameters)
	{
		if (argument is IdentifierExpression ident)
		{
			var path = ident.Name;
			if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
			{
				path = path[(fromAlias.Length + 1)..];
			}

			return item.SelectToken(path) as JArray;
		}

		var evaluated = EvaluateSqlExpression(argument, item, fromAlias, parameters);
		return evaluated as JArray;
	}

	private static bool ArrayContainsMatch(JArray jArray, string searchStr, bool partial)
	{
		foreach (var element in jArray)
		{
			if (element.Type == JTokenType.Object && searchStr.StartsWith("{"))
			{
				var searchObj = JsonParseHelpers.ParseJson(searchStr);
				if (partial)
				{
					var allMatch = searchObj.Properties().All(prop =>
					{
						var elementValue = element[prop.Name];
						return elementValue is not null && JToken.DeepEquals(elementValue, prop.Value);
					});
					if (allMatch)
					{
						return true;
					}
				}
				else if (JToken.DeepEquals(element, searchObj))
				{
					return true;
				}
			}
			else if (string.Equals(element.ToString(), searchStr, StringComparison.Ordinal))
			{
				return true;
			}
		}
		return false;
	}

	private static bool ArrayContainsMatchJObject(JArray jArray, JObject searchObj, bool partial)
	{
		foreach (var element in jArray)
		{
			if (element.Type != JTokenType.Object)
			{
				continue;
			}

			if (partial)
			{
				var allMatch = searchObj.Properties().All(prop =>
				{
					var elementValue = element[prop.Name];
					return elementValue is not null && JToken.DeepEquals(elementValue, prop.Value);
				});
				if (allMatch)
				{
					return true;
				}
			}
			else if (JToken.DeepEquals(element, searchObj))
			{
				return true;
			}
		}
		return false;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Patch operations
	// ═══════════════════════════════════════════════════════════════════════════

	private static readonly HashSet<string> SystemProperties = new(StringComparer.OrdinalIgnoreCase)
		{ "/_ts", "/_etag", "/_rid", "/_self", "/_attachments" };

	private static void ApplyPatchOperations(JObject jObj, IReadOnlyList<PatchOperation> patchOperations)
	{
		foreach (var operation in patchOperations)
		{
			var path = GetPatchPath(operation);

			// Reject patches to system-generated properties
			if (SystemProperties.Contains(path))
			{
				throw InMemoryCosmosException.Create(
					$"Cannot patch system property '{path}'.",
					HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
			}

			var segments = path.TrimStart('/').Split('/');
			var propertyName = segments.Last();
			var parentPath = BuildSelectTokenPath(segments.Take(segments.Length - 1));
			var rawParent = segments.Length > 1 ? jObj.SelectToken(parentPath) : (JToken)jObj;

			switch (operation.OperationType)
			{
				case PatchOperationType.Add:
					{
						var value = GetPatchValue(operation);
						var newToken = value is not null ? JToken.FromObject(value) : JValue.CreateNull();

						if (propertyName == "-" && rawParent is JArray appendArray)
						{
							appendArray.Add(newToken);
						}
						else if (int.TryParse(propertyName, out var insertIdx) && rawParent is JArray insertArray)
						{
							if (insertIdx < 0 || insertIdx > insertArray.Count)
							{
								throw InMemoryCosmosException.Create("Array index out of bounds.",
									HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
							}
							insertArray.Insert(insertIdx, newToken);
						}
						else
						{
							var parent = rawParent as JObject ?? jObj;
							parent[propertyName] = newToken;
						}

						break;
					}
				case PatchOperationType.Set:
					{
						var value = GetPatchValue(operation);
						var newToken = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
						if (int.TryParse(propertyName, out var idx) && rawParent is JArray arr)
						{
							if (idx < 0 || idx >= arr.Count)
							{
								throw InMemoryCosmosException.Create("Array index out of bounds.",
									HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
							}
							arr[idx] = newToken;
						}
						else
						{
							var parent = rawParent as JObject ?? jObj;
							parent[propertyName] = newToken;
						}
						break;
					}
				case PatchOperationType.Replace:
					{
						var value = GetPatchValue(operation);
						var newToken = value is not null ? JToken.FromObject(value) : JValue.CreateNull();
						if (int.TryParse(propertyName, out var idx) && rawParent is JArray arr)
						{
							if (idx < 0 || idx >= arr.Count)
							{
								throw InMemoryCosmosException.Create("Array index out of bounds.",
									HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
							}
							arr[idx] = newToken;
						}
						else
						{
							var parent = rawParent as JObject ?? jObj;
							if (parent[propertyName] is null)
							{
								throw InMemoryCosmosException.Create("Replace target does not exist.",
									HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
							}
							parent[propertyName] = newToken;
						}
						break;
					}
				case PatchOperationType.Remove:
					{
						if (int.TryParse(propertyName, out var idx) && rawParent is JArray arr)
						{
							if (idx < 0 || idx >= arr.Count)
							{
								throw InMemoryCosmosException.Create("Array index out of bounds.",
									HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
							}
							arr.RemoveAt(idx);
						}
						else
						{
							var parent = rawParent as JObject ?? jObj;
							if (parent[propertyName] is null)
							{
								throw InMemoryCosmosException.Create("Remove target does not exist.",
									HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
							}
							parent.Remove(propertyName);
						}
						break;
					}
				case PatchOperationType.Move:
					{
						var sourcePath = GetPatchSourcePath(operation);
						if (sourcePath is not null)
						{
							// Reject when destination is a child of source (e.g. move /nested → /nested/child)
							if (path.StartsWith(sourcePath + "/", StringComparison.Ordinal))
							{
								throw InMemoryCosmosException.Create(
									"The 'path' attribute can't be a JSON child of the 'from' JSON location.",
									HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
							}

							var sourceSegments = sourcePath.TrimStart('/').Split('/');
							var sourcePropertyName = sourceSegments.Last();
							var sourceParentPath = BuildSelectTokenPath(sourceSegments.Take(sourceSegments.Length - 1));
							var sourceParent = sourceSegments.Length > 1
								? jObj.SelectToken(sourceParentPath) as JObject ?? jObj
								: jObj;
							var sourceValue = sourceParent[sourcePropertyName];
							if (sourceValue is null)
							{
								throw InMemoryCosmosException.Create("Move source does not exist.",
									HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
							}
							sourceParent.Remove(sourcePropertyName);
							var parent = rawParent as JObject ?? jObj;
							parent[propertyName] = sourceValue;
						}

						break;
					}
				case PatchOperationType.Increment:
					{
						var incrementValue = GetPatchValue(operation);
						if (incrementValue is not null)
						{
							var parent = rawParent as JObject ?? jObj;
							var existingToken = parent[propertyName];
							if (existingToken is not null)
							{
								if (existingToken.Type is not (JTokenType.Integer or JTokenType.Float))
								{
									throw InMemoryCosmosException.Create(
										$"Cannot increment non-numeric field '{path}'. Field type is {existingToken.Type}.",
										HttpStatusCode.BadRequest, 0, Guid.NewGuid().ToString(), SyntheticRequestCharge);
								}
								var existingDouble = existingToken.Value<double>();
								var incrementDouble = Convert.ToDouble(incrementValue);
								var result = existingDouble + incrementDouble;
								if (existingToken.Type == JTokenType.Integer && result == Math.Floor(result))
								{
									parent[propertyName] = (long)result;
								}
								else
								{
									parent[propertyName] = result;
								}
							}
							else
							{
								var incrementDouble = Convert.ToDouble(incrementValue);
								if (incrementDouble == Math.Floor(incrementDouble))
								{
									parent[propertyName] = (long)incrementDouble;
								}
								else
								{
									parent[propertyName] = incrementDouble;
								}
							}
						}
						break;
					}
			}
		}
	}

	private static string GetPatchPath(PatchOperation operation) => operation.Path;

	/// <summary>
	/// Converts path segments to a Newtonsoft.Json SelectToken-compatible path.
	/// Numeric segments become array indexers (e.g., ["runs","0"] → "runs[0]").
	/// </summary>
	internal static string BuildSelectTokenPath(IEnumerable<string> segments)
	{
		var sb = new System.Text.StringBuilder();
		foreach (var segment in segments)
		{
			if (int.TryParse(segment, out _))
			{
				sb.Append('[').Append(segment).Append(']');
			}
			else
			{
				if (sb.Length > 0) sb.Append('.');
				sb.Append(segment);
			}
		}
		return sb.ToString();
	}

	private static string GetPatchSourcePath(PatchOperation operation)
	{
		var fromProp = operation.GetType()
			.GetProperty("From", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
		return fromProp?.GetValue(operation)?.ToString();
	}

	private static object GetPatchValue(PatchOperation operation)
	{
		var valueProp = operation.GetType()
			.GetProperty("Value", BindingFlags.Public | BindingFlags.Instance);
		return valueProp?.GetValue(operation);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Stream FeedIterator factory
	// ═══════════════════════════════════════════════════════════════════════════

	private FeedIterator CreateStreamFeedIterator(List<string> items, int initialOffset = 0, int? maxItemCount = null)
	{
		return CreateStreamFeedIteratorFromFactory(() => items, initialOffset, maxItemCount ?? items.Count);
	}

	private FeedIterator CreateStreamFeedIterator(Func<IReadOnlyList<object>> itemsFactory)
	{
		return CreateStreamFeedIteratorFromFactory(() => itemsFactory().Select(o => o?.ToString() ?? "").ToList(), 0, null);
	}

	private FeedIterator CreateStreamFeedIteratorFromFactory(Func<List<string>> itemsFactory, int initialOffset, int? maxItemCount)
	{
		var offset = initialOffset;
		var done = false;

		var feedIterator = Substitute.For<FeedIterator>();
		feedIterator.HasMoreResults.Returns(_ => !done);
		feedIterator.ReadNextAsync(Arg.Any<CancellationToken>()).Returns(callInfo =>
		{
			var ct = callInfo.Arg<CancellationToken>();
			ct.ThrowIfCancellationRequested();
			var items = itemsFactory();
			var pageSize = maxItemCount ?? items.Count;
			if (pageSize <= 0) pageSize = items.Count;
			var page = items.Skip(offset).Take(pageSize).ToList();
			offset += page.Count;
			if (offset >= items.Count)
				done = true;
			var documentsArray = new JArray(page.Select(JsonParseHelpers.ParseJson));
			var envelope = new JObject
			{
				["Documents"] = documentsArray,
				["_count"] = documentsArray.Count,
				["_rid"] = string.Empty
			};
			var stream = ToStream(envelope.ToString(Formatting.None));
			var response = new ResponseMessage(HttpStatusCode.OK) { Content = stream };
			response.Headers["x-ms-activity-id"] = Guid.NewGuid().ToString();
			response.Headers["x-ms-request-charge"] = "1";
			response.Headers["x-ms-session-token"] = CurrentSessionToken;
			response.Headers["x-ms-item-count"] = documentsArray.Count.ToString();
			if (!done)
				response.Headers.Add("x-ms-continuation", offset.ToString());
			return Task.FromResult(response);
		});
		return feedIterator;
	}



	// ═══════════════════════════════════════════════════════════════════════════
	//  InMemoryFeedResponse (for ReadManyItemsAsync)
	// ═══════════════════════════════════════════════════════════════════════════

	private sealed class InMemoryFeedResponse<T> : FeedResponse<T>
	{
		private readonly IReadOnlyList<T> _items;
		private readonly HttpStatusCode _statusCode;
		private readonly string _etag;

		public InMemoryFeedResponse(IReadOnlyList<T> items, HttpStatusCode statusCode = HttpStatusCode.OK, string etag = null)
		{
			_items = items;
			_statusCode = statusCode;
			_etag = etag;
			Headers["x-ms-request-charge"] = SyntheticRequestCharge.ToString();
			Headers["x-ms-item-count"] = items.Count.ToString();
		}
		public override Headers Headers { get; } = new();
		public override IEnumerable<T> Resource => _items;
		public override HttpStatusCode StatusCode => _statusCode;
		public override CosmosDiagnostics Diagnostics => FakeDiagnostics;
		public override int Count => _items.Count;
		public override string IndexMetrics => null;
		public override string ContinuationToken => null;
		public override double RequestCharge => SyntheticRequestCharge;
		public override string ActivityId { get; } = Guid.NewGuid().ToString();
		public override string ETag => _etag;
		public override IEnumerator<T> GetEnumerator() => _items.GetEnumerator();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Vector distance helpers
	// ═══════════════════════════════════════════════════════════════════════════

	private static object VectorDistanceFunc(object[] args)
	{
		// VECTORDISTANCE(vector1, vector2 [, bool_bruteForce] [, {distanceFunction:'cosine'|'dotproduct'|'euclidean'})
		if (args.Length > 4)
			throw InMemoryCosmosException.Create("VECTORDISTANCE accepts at most 4 arguments.", HttpStatusCode.BadRequest, 0, string.Empty, 0);

		var vec1 = ToDoubleArray(args[0]);
		var vec2 = ToDoubleArray(args[1]);
		if (vec1 is null || vec2 is null || vec1.Length != vec2.Length || vec1.Length == 0)
			return null;

		// 3rd arg (bool bruteForce) is accepted but ignored in the emulator
		// 4th arg (object options) may contain distanceFunction override
		var distanceFunction = "cosine";
		if (args.Length > 3)
		{
			var options = args[3] switch
			{
				JObject jo => jo,
				string s when s.TrimStart().StartsWith("{") => JObject.Parse(s),
				string s => new JObject { ["distanceFunction"] = s },
				_ => null,
			};
			var df = options?["distanceFunction"]?.ToString();
			if (df is not null) distanceFunction = df;
		}

		var result = distanceFunction.ToLowerInvariant() switch
		{
			"cosine" => CosineSimilarity(vec1, vec2),
			"dotproduct" => (object)DotProduct(vec1, vec2),
			"euclidean" => (object)EuclideanDistance(vec1, vec2),
			_ => throw InMemoryCosmosException.Create($"Unknown distanceFunction '{distanceFunction}'. Supported values: 'cosine', 'dotproduct', 'euclidean'.", HttpStatusCode.BadRequest, 0, string.Empty, 0),
		};

		// Guard against Infinity/NaN which are not valid JSON numbers
		if (result is double d && (double.IsInfinity(d) || double.IsNaN(d)))
			return null;

		return result;
	}

	private static object CosineSimilarity(double[] a, double[] b)
	{
		double dot = 0, magA = 0, magB = 0;
		for (var i = 0; i < a.Length; i++)
		{
			dot += a[i] * b[i];
			magA += a[i] * a[i];
			magB += b[i] * b[i];
		}
		var denominator = Math.Sqrt(magA) * Math.Sqrt(magB);
		return denominator == 0 ? null : (object)(dot / denominator);
	}

	private static double DotProduct(double[] a, double[] b)
	{
		double sum = 0;
		for (var i = 0; i < a.Length; i++)
			sum += a[i] * b[i];
		return sum;
	}

	private static double EuclideanDistance(double[] a, double[] b)
	{
		double sum = 0;
		for (var i = 0; i < a.Length; i++)
		{
			var diff = a[i] - b[i];
			sum += diff * diff;
		}
		return Math.Sqrt(sum);
	}

	private static double[] ToDoubleArray(object value)
	{
		if (value is double[] dArr) return dArr;
		if (value is float[] fArr) return Array.ConvertAll(fArr, f => (double)f);
		if (value is IEnumerable<double> dEnum) return dEnum.ToArray();

		JArray ja = value switch
		{
			JArray arr => arr,
			string s when s.TrimStart().StartsWith("[") => JArray.Parse(s),
			_ => null,
		};
		if (ja is null) return null;

		var result = new double[ja.Count];
		for (var i = 0; i < ja.Count; i++)
		{
			if (ja[i].Type is not (JTokenType.Float or JTokenType.Integer))
				return null;
			result[i] = ja[i].Value<double>();
		}
		return result;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Spatial function helpers
	// ═══════════════════════════════════════════════════════════════════════════

	private static object StDistance(object left, object right)
	{
		var p1 = ExtractPoint(left);
		var p2 = ExtractPoint(right);
		if (p1 is null || p2 is null)
		{
			return null;
		}

		return HaversineDistanceMeters(p1.Value.Lat, p1.Value.Lon, p2.Value.Lat, p2.Value.Lon);
	}

	private static bool StWithin(object point, object region)
	{
		var p = ExtractPoint(point);
		if (p is null)
		{
			return false;
		}

		var polygon = ExtractPolygonRings(region);
		if (polygon is not null)
		{
			return PointInPolygon(p.Value, polygon);
		}

		var circle = ExtractCircle(region);
		if (circle is not null)
		{
			var dist = HaversineDistanceMeters(p.Value.Lat, p.Value.Lon, circle.Value.Center.Lat, circle.Value.Center.Lon);
			return dist <= circle.Value.RadiusMeters;
		}

		return false;
	}

	private static bool StIntersects(object geo1, object geo2)
	{
		var p1 = ExtractPoint(geo1);
		var p2 = ExtractPoint(geo2);

		if (p1 is not null && p2 is not null)
		{
			return Math.Abs(p1.Value.Lat - p2.Value.Lat) < 1e-10 &&
				   Math.Abs(p1.Value.Lon - p2.Value.Lon) < 1e-10;
		}

		if (p1 is not null)
		{
			return StWithin(geo1, geo2);
		}

		if (p2 is not null)
		{
			return StWithin(geo2, geo1);
		}

		var poly1 = ExtractPolygonRings(geo1);
		var poly2 = ExtractPolygonRings(geo2);
		if (poly1 is not null && poly2 is not null)
		{
			return PolygonsShareAnyPoint(poly1, poly2);
		}

		return false;
	}

	private static bool StIsValid(object geo)
	{
		var obj = AsJObject(geo);
		if (obj is null)
		{
			return false;
		}

		var type = obj["type"]?.ToString();
		return type switch
		{
			"Point" => ExtractPoint(geo) is not null,
			"Polygon" => ExtractPolygonRings(geo) is not null && ValidatePolygonRings(ExtractPolygonRings(geo)),
			"LineString" => ExtractCoordinateArray(obj["coordinates"]) is { Count: >= 2 },
			"MultiPoint" => ExtractCoordinateArray(obj["coordinates"]) is { Count: >= 1 },
			_ => false
		};
	}

	private static JObject StIsValidDetailed(object geo)
	{
		var obj = AsJObject(geo);
		if (obj is null)
		{
			return JObject.FromObject(new { valid = false, reason = "Not a valid GeoJSON object." });
		}

		var type = obj["type"]?.ToString();
		if (string.IsNullOrEmpty(type))
		{
			return JObject.FromObject(new { valid = false, reason = "GeoJSON object missing 'type' property." });
		}

		switch (type)
		{
			case "Point":
				if (ExtractPoint(geo) is null)
				{
					return JObject.FromObject(new { valid = false, reason = "Point must have coordinates [longitude, latitude]." });
				}

				return JObject.FromObject(new { valid = true, reason = "" });

			case "Polygon":
				var rings = ExtractPolygonRings(geo);
				if (rings is null)
				{
					return JObject.FromObject(new { valid = false, reason = "Polygon coordinates must be an array of linear rings." });
				}

				if (!ValidatePolygonRings(rings))
				{
					return JObject.FromObject(new { valid = false, reason = "Polygon rings must be closed (first and last position must be identical) and have at least 4 positions." });
				}

				return JObject.FromObject(new { valid = true, reason = "" });

			case "LineString":
				if (ExtractCoordinateArray(obj["coordinates"]) is not { Count: >= 2 })
				{
					return JObject.FromObject(new { valid = false, reason = "LineString must have at least 2 positions." });
				}

				return JObject.FromObject(new { valid = true, reason = "" });

			default:
				return JObject.FromObject(new { valid = false, reason = $"Unsupported GeoJSON type: {type}." });
		}
	}

	private readonly record struct GeoPoint(double Lat, double Lon);

	private readonly record struct GeoCircle(GeoPoint Center, double RadiusMeters);

	private static object StArea(object geo)
	{
		var rings = ExtractPolygonRings(geo);
		if (rings is null || rings.Count == 0) return null;
		// Approximate area using spherical excess formula for the outer ring, minus holes
		var area = SphericalPolygonArea(rings[0]);
		for (var i = 1; i < rings.Count; i++)
			area -= SphericalPolygonArea(rings[i]);
		return Math.Abs(area);
	}

	private static double SphericalPolygonArea(List<GeoPoint> ring)
	{
		// Spherical excess method (Girard's theorem) for area on a sphere
		double sum = 0;
		for (var i = 0; i < ring.Count - 1; i++)
		{
			var lon1 = ring[i].Lon * Math.PI / 180;
			var lat1 = ring[i].Lat * Math.PI / 180;
			var lon2 = ring[(i + 1) % (ring.Count - 1)].Lon * Math.PI / 180;
			var lat2 = ring[(i + 1) % (ring.Count - 1)].Lat * Math.PI / 180;
			sum += (lon2 - lon1) * (2 + Math.Sin(lat1) + Math.Sin(lat2));
		}
		return Math.Abs(sum * EarthRadiusMeters * EarthRadiusMeters / 2.0);
	}

	private static JObject AsJObject(object value)
	{
		return value switch
		{
			JObject jo => jo,
			string s when s.TrimStart().StartsWith("{") => JsonParseHelpers.ParseJson(s),
			_ => null
		};
	}

	private static GeoPoint? ExtractPoint(object value)
	{
		var obj = AsJObject(value);
		if (obj is null)
		{
			return null;
		}

		if (!string.Equals(obj["type"]?.ToString(), "Point", StringComparison.OrdinalIgnoreCase))
		{
			return null;
		}

		if (obj["coordinates"] is not JArray coords || coords.Count < 2)
		{
			return null;
		}

		var lon = coords[0].Value<double>();
		var lat = coords[1].Value<double>();
		if (lat < -90 || lat > 90 || lon < -180 || lon > 180)
		{
			return null;
		}

		return new GeoPoint(lat, lon);
	}

	private static List<List<GeoPoint>> ExtractPolygonRings(object value)
	{
		var obj = AsJObject(value);
		if (obj is null)
		{
			return null;
		}

		if (!string.Equals(obj["type"]?.ToString(), "Polygon", StringComparison.OrdinalIgnoreCase))
		{
			return null;
		}

		if (obj["coordinates"] is not JArray coords || coords.Count == 0)
		{
			return null;
		}

		var rings = new List<List<GeoPoint>>();
		foreach (var ring in coords)
		{
			var points = ExtractCoordinateArray(ring);
			if (points is null || points.Count < 4)
			{
				return null;
			}

			rings.Add(points);
		}

		return rings;
	}

	private static GeoCircle? ExtractCircle(object value)
	{
		var obj = AsJObject(value);
		if (obj is null)
		{
			return null;
		}

		var center = obj["center"];
		var radius = obj["radius"];
		if (center is null || radius is null)
		{
			return null;
		}

		var centerPoint = ExtractPoint(center);
		if (centerPoint is null)
		{
			return null;
		}

		if (!double.TryParse(radius.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var radiusMeters))
		{
			return null;
		}

		return new GeoCircle(centerPoint.Value, radiusMeters);
	}

	private static List<GeoPoint> ExtractCoordinateArray(JToken token)
	{
		if (token is not JArray arr)
		{
			return null;
		}

		var points = new List<GeoPoint>();
		foreach (var item in arr)
		{
			if (item is JArray coord && coord.Count >= 2)
			{
				var lon = coord[0].Value<double>();
				var lat = coord[1].Value<double>();
				points.Add(new GeoPoint(lat, lon));
			}
			else
			{
				return null;
			}
		}

		return points;
	}

	private static bool ValidatePolygonRings(List<List<GeoPoint>> rings)
	{
		foreach (var ring in rings)
		{
			if (ring.Count < 4)
			{
				return false;
			}

			var first = ring[0];
			var last = ring[^1];
			if (Math.Abs(first.Lat - last.Lat) > 1e-10 || Math.Abs(first.Lon - last.Lon) > 1e-10)
			{
				return false;
			}
		}

		return true;
	}

	private static double HaversineDistanceMeters(double lat1, double lon1, double lat2, double lon2)
	{
		var dLat = DegreesToRadians(lat2 - lat1);
		var dLon = DegreesToRadians(lon2 - lon1);
		var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
				Math.Cos(DegreesToRadians(lat1)) * Math.Cos(DegreesToRadians(lat2)) *
				Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
		var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
		return EarthRadiusMeters * c;
	}

	private static double DegreesToRadians(double degrees) => degrees * (Math.PI / 180.0);

	private static bool PointInPolygon(GeoPoint point, List<List<GeoPoint>> rings)
	{
		if (rings.Count == 0)
		{
			return false;
		}

		var inOuter = PointInRing(point, rings[0]);
		if (!inOuter)
		{
			return false;
		}

		for (var i = 1; i < rings.Count; i++)
		{
			if (PointInRing(point, rings[i]))
			{
				return false;
			}
		}

		return true;
	}

	private static bool PointInRing(GeoPoint point, List<GeoPoint> ring)
	{
		var inside = false;
		for (int i = 0, j = ring.Count - 1; i < ring.Count; j = i++)
		{
			if ((ring[i].Lat > point.Lat) != (ring[j].Lat > point.Lat) &&
				point.Lon < (ring[j].Lon - ring[i].Lon) * (point.Lat - ring[i].Lat) / (ring[j].Lat - ring[i].Lat) + ring[i].Lon)
			{
				inside = !inside;
			}
		}

		return inside;
	}

	private static bool PolygonsShareAnyPoint(List<List<GeoPoint>> poly1, List<List<GeoPoint>> poly2)
	{
		foreach (var point in poly1[0])
		{
			if (PointInPolygon(point, poly2))
			{
				return true;
			}
		}

		foreach (var point in poly2[0])
		{
			if (PointInPolygon(point, poly1))
			{
				return true;
			}
		}

		return false;
	}
}
