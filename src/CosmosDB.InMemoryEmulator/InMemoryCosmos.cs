using System.Collections;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Entry point for creating in-memory Cosmos DB test infrastructure.
/// Use <see cref="Create(string, string, Func{HttpMessageHandler, HttpMessageHandler}?, Action{CosmosClientOptions}?, Action{InMemoryContainer}?)"/>
/// for single-container setups (the 80% case) or <see cref="Builder"/> for multi-container/advanced scenarios.
/// </summary>
public static class InMemoryCosmos
{
	/// <summary>
	/// The default database name used by <see cref="Create"/> and <see cref="InMemoryCosmosBuilder.AddContainer"/>.
	/// Value is <c>"default"</c>.
	/// </summary>
	public const string DefaultDatabaseName = "default";

	/// <summary>
	/// Creates a single-container in-memory Cosmos setup with a single partition key path.
	/// </summary>
	public static InMemoryCosmosResult Create(
		string containerName,
		string partitionKeyPath = "/id",
		Func<HttpMessageHandler, HttpMessageHandler>? wrapHandler = null,
		Action<CosmosClientOptions>? configureOptions = null,
		Action<IContainerTestSetup>? configureContainer = null)
	{
		ValidateContainerName(containerName);
		ValidatePartitionKeyPath(partitionKeyPath);

		return Builder()
			.AddContainer(containerName, partitionKeyPath, configureContainer)
			.ApplyWrapHandler(wrapHandler)
			.ApplyConfigureOptions(configureOptions)
			.Build(singleContainerName: containerName);
	}

	/// <summary>
	/// Creates a single-container in-memory Cosmos setup with hierarchical (composite) partition key paths.
	/// </summary>
	public static InMemoryCosmosResult Create(
		string containerName,
		string[] partitionKeyPaths,
		Func<HttpMessageHandler, HttpMessageHandler>? wrapHandler = null,
		Action<CosmosClientOptions>? configureOptions = null,
		Action<IContainerTestSetup>? configureContainer = null)
	{
		ValidateContainerName(containerName);
		if (partitionKeyPaths is null || partitionKeyPaths.Length == 0)
			throw new ArgumentException("At least one partition key path must be provided.", nameof(partitionKeyPaths));
		foreach (var path in partitionKeyPaths)
			ValidatePartitionKeyPath(path);

		return Builder()
			.AddContainer(containerName, partitionKeyPaths, configureContainer)
			.ApplyWrapHandler(wrapHandler)
			.ApplyConfigureOptions(configureOptions)
			.Build(singleContainerName: containerName);
	}

	/// <summary>
	/// Returns a builder for multi-container or advanced configuration.
	/// </summary>
	public static InMemoryCosmosBuilder Builder() => new();

	internal static void ValidateContainerName(string containerName)
	{
		if (string.IsNullOrWhiteSpace(containerName))
			throw new ArgumentException("Container name must not be null, empty, or whitespace.", nameof(containerName));
	}

	internal static void ValidatePartitionKeyPath(string path)
	{
		if (string.IsNullOrWhiteSpace(path))
			throw new ArgumentException("Partition key path must not be null or empty.", nameof(path));
		if (!path.StartsWith('/'))
			throw new ArgumentException($"Partition key path must start with '/'. Got: '{path}'.", nameof(path));
	}
}

/// <summary>
/// Fluent builder for creating multi-container in-memory Cosmos setups.
/// </summary>
public sealed class InMemoryCosmosBuilder
{
	private readonly List<ContainerSpec> _containers = new();
	private readonly List<DatabaseSpec> _databases = new();
	private readonly List<Func<HttpMessageHandler, HttpMessageHandler>> _wrapHandlers = new();
	private readonly List<Action<CosmosClientOptions>> _configureOptions = new();

	internal InMemoryCosmosBuilder() { }

	/// <summary>
	/// Adds a container with a single partition key path.
	/// </summary>
	public InMemoryCosmosBuilder AddContainer(string name, string partitionKeyPath,
		Action<IContainerTestSetup>? configure = null)
	{
		InMemoryCosmos.ValidateContainerName(name);
		InMemoryCosmos.ValidatePartitionKeyPath(partitionKeyPath);
		ValidateNoDuplicate(name);
		_containers.Add(new ContainerSpec(name, new[] { partitionKeyPath }, configure));
		return this;
	}

	/// <summary>
	/// Adds a container with hierarchical (composite) partition key paths.
	/// </summary>
	public InMemoryCosmosBuilder AddContainer(string name, string[] partitionKeyPaths,
		Action<IContainerTestSetup>? configure = null)
	{
		InMemoryCosmos.ValidateContainerName(name);
		if (partitionKeyPaths is null || partitionKeyPaths.Length == 0)
			throw new ArgumentException("At least one partition key path must be provided.", nameof(partitionKeyPaths));
		foreach (var path in partitionKeyPaths)
			InMemoryCosmos.ValidatePartitionKeyPath(path);
		ValidateNoDuplicate(name);
		_containers.Add(new ContainerSpec(name, partitionKeyPaths, configure));
		return this;
	}

	/// <summary>
	/// Adds a named database with its own containers. Use this when a single
	/// <see cref="CosmosClient"/> talks to multiple databases that may have
	/// overlapping container names (e.g. users-db/events and orders-db/events).
	/// </summary>
	public InMemoryCosmosBuilder AddDatabase(string databaseName,
		Action<InMemoryDatabaseBuilder> configure)
	{
		ArgumentNullException.ThrowIfNull(configure);
		if (string.IsNullOrWhiteSpace(databaseName))
			throw new ArgumentException("Database name must not be null, empty, or whitespace.", nameof(databaseName));
		if (_databases.Any(d => d.Name == databaseName))
			throw new InvalidOperationException($"Database '{databaseName}' has already been added.");

		var builder = new InMemoryDatabaseBuilder();
		configure(builder);

		if (builder.Containers.Count == 0)
			throw new InvalidOperationException($"Database '{databaseName}' must have at least one container.");

		_databases.Add(new DatabaseSpec(databaseName, builder.Containers.ToList()));
		return this;
	}

	/// <summary>
	/// Adds an HTTP handler wrapper. Multiple calls compose: <c>WrapHandler(a).WrapHandler(b)</c>
	/// produces <c>b(a(handler))</c>.
	/// </summary>
	public InMemoryCosmosBuilder WrapHandler(Func<HttpMessageHandler, HttpMessageHandler> wrapper)
	{
		ArgumentNullException.ThrowIfNull(wrapper);
		_wrapHandlers.Add(wrapper);
		return this;
	}

	/// <summary>
	/// Configures <see cref="CosmosClientOptions"/>. Multiple calls run sequentially.
	/// Do not set <see cref="CosmosClientOptions.HttpClientFactory"/> — use
	/// <see cref="WrapHandler"/> instead.
	/// </summary>
	public InMemoryCosmosBuilder ConfigureOptions(Action<CosmosClientOptions> configure)
	{
		ArgumentNullException.ThrowIfNull(configure);
		_configureOptions.Add(configure);
		return this;
	}

	internal InMemoryCosmosBuilder ApplyWrapHandler(Func<HttpMessageHandler, HttpMessageHandler>? wrapper)
	{
		if (wrapper is not null)
			_wrapHandlers.Add(wrapper);
		return this;
	}

	internal InMemoryCosmosBuilder ApplyConfigureOptions(Action<CosmosClientOptions>? configure)
	{
		if (configure is not null)
			_configureOptions.Add(configure);
		return this;
	}

	/// <summary>
	/// Builds the in-memory Cosmos setup. Can be called multiple times — each call
	/// produces an independent result with fresh containers and handlers.
	/// </summary>
	public InMemoryCosmosResult Build() => Build(singleContainerName: null);

	internal InMemoryCosmosResult Build(string? singleContainerName)
	{
		var totalContainers = _containers.Count + _databases.Sum(d => d.Containers.Count);
		if (totalContainers == 0)
			throw new InvalidOperationException("At least one container must be added before calling Build().");

		var registry = new FakeCosmosHandler.DynamicContainerRegistry
		{
			DatabaseName = InMemoryCosmos.DefaultDatabaseName
		};

		var handlers = new Dictionary<string, FakeCosmosHandler>(StringComparer.Ordinal);
		var configureCallbacks = new List<(string RegistryKey, Action<InMemoryContainer>? Configure)>();

		// Process flat containers → default database, plain keys
		if (_containers.Count > 0)
		{
			var defaultDbMap = registry.DatabaseContainerKeys.GetOrAdd(
				InMemoryCosmos.DefaultDatabaseName, _ => new(StringComparer.Ordinal));

			foreach (var spec in _containers)
			{
				var container = CreateContainer(spec);
				var handler = new FakeCosmosHandler(container);
				handlers[spec.Name] = handler;
				registry.BackingContainers[spec.Name] = container;
				defaultDbMap[spec.Name] = spec.Name;
				configureCallbacks.Add((spec.Name, spec.Configure));
			}
		}

		// Process explicit databases → compound keys
		foreach (var dbSpec in _databases)
		{
			var dbMap = registry.DatabaseContainerKeys.GetOrAdd(
				dbSpec.Name, _ => new(StringComparer.Ordinal));
			registry.CompoundKeyDatabases.Add(dbSpec.Name);

			foreach (var spec in dbSpec.Containers)
			{
				var registryKey = $"{dbSpec.Name}/{spec.Name}";
				var container = CreateContainer(spec);
				var handler = new FakeCosmosHandler(container);
				handlers[registryKey] = handler;
				registry.BackingContainers[registryKey] = container;
				dbMap[spec.Name] = registryKey;
				configureCallbacks.Add((registryKey, spec.Configure));
			}
		}

		// Always use a RoutingHandler (even for single container) to enable dynamic container management
		HttpMessageHandler httpHandler = new FakeCosmosHandler.RoutingHandler(handlers, registry);

		// Apply handler wrappers in order
		foreach (var wrapper in _wrapHandlers)
			httpHandler = wrapper(httpHandler);

		// Build CosmosClientOptions
		var options = new CosmosClientOptions();
		foreach (var configure in _configureOptions)
			configure(options);

		// Validate user didn't set HttpClientFactory
		if (options.HttpClientFactory is not null)
			throw new InvalidOperationException(
				"Do not set HttpClientFactory directly — use the wrapHandler parameter to customize " +
				"the HTTP pipeline. HttpClientFactory is managed internally by InMemoryCosmos.");

		// Force required settings
		options.ConnectionMode = ConnectionMode.Gateway;
		options.LimitToEndpoint = true;
		var capturedHandler = httpHandler;
		options.HttpClientFactory = () => new HttpClient(capturedHandler);

		var innerClient = new CosmosClient(
			FakeCosmosHandler.FakeConnectionString,
			options);

		var client = FakeCosmosHandler.WrapClient(innerClient, handlers);

		// Set client on registry so dynamic container creation can register SDK containers
		registry.Client = client;

		// Run configure callbacks after wiring is complete
		foreach (var (registryKey, configure) in configureCallbacks)
		{
			configure?.Invoke(registry.BackingContainers[registryKey]);
		}

		// Build SDK Container references
		// Flat containers → default database
		foreach (var spec in _containers)
			registry.SdkContainers[spec.Name] = client.GetContainer(InMemoryCosmos.DefaultDatabaseName, spec.Name);

		// Explicit databases → use database name
		foreach (var dbSpec in _databases)
		{
			foreach (var spec in dbSpec.Containers)
			{
				var registryKey = $"{dbSpec.Name}/{spec.Name}";
				registry.SdkContainers[registryKey] = client.GetContainer(dbSpec.Name, spec.Name);
			}
		}

		// Determine single-container name
		var isSingle = singleContainerName is not null || totalContainers == 1;
		var singleName = singleContainerName ?? (isSingle ? GetSingleContainerRegistryKey() : null);

		return new InMemoryCosmosResult(client, registry, singleName);

		string? GetSingleContainerRegistryKey()
		{
			if (_containers.Count == 1 && _databases.Count == 0)
				return _containers[0].Name;
			if (_containers.Count == 0 && _databases.Count == 1 && _databases[0].Containers.Count == 1)
				return $"{_databases[0].Name}/{_databases[0].Containers[0].Name}";
			return null;
		}
	}

	private static InMemoryContainer CreateContainer(ContainerSpec spec)
	{
		return spec.PartitionKeyPaths.Length == 1
			? new InMemoryContainer(spec.Name, spec.PartitionKeyPaths[0])
			: new InMemoryContainer(spec.Name, spec.PartitionKeyPaths);
	}

	private void ValidateNoDuplicate(string name)
	{
		if (_containers.Any(c => c.Name == name))
			throw new InvalidOperationException($"Container '{name}' has already been added.");
	}

	internal sealed record ContainerSpec(
		string Name,
		string[] PartitionKeyPaths,
		Action<IContainerTestSetup>? Configure);

	private sealed record DatabaseSpec(
		string Name,
		List<ContainerSpec> Containers);
}

/// <summary>
/// The result of creating an in-memory Cosmos setup. Provides access to the
/// <see cref="CosmosClient"/>, SDK <see cref="Container"/> references,
/// test setup operations, and fault injection handlers.
/// </summary>
public sealed class InMemoryCosmosResult : IDisposable, IAsyncDisposable
{
	private readonly FakeCosmosHandler.DynamicContainerRegistry _registry;
	private readonly string? _singleContainerName;

	internal InMemoryCosmosResult(
		CosmosClient client,
		FakeCosmosHandler.DynamicContainerRegistry registry,
		string? singleContainerName)
	{
		Client = client;
		_registry = registry;
		_singleContainerName = singleContainerName;
	}

	// ─── Tier 1: Production-like ──────────────────────────────────────────────

	/// <summary>The <see cref="CosmosClient"/> backed by in-memory handlers.</summary>
	public CosmosClient Client { get; }

	/// <summary>
	/// The SDK <see cref="Container"/> for single-container setups.
	/// Throws <see cref="InvalidOperationException"/> if multiple containers are registered.
	/// </summary>
	public Container Container
	{
		get
		{
			if (_singleContainerName is null)
				throw new InvalidOperationException(
					"Container property requires a single-container setup. " +
					$"Use Containers[\"name\"] instead. Available containers: {AvailableNames}.");
			return _registry.SdkContainers[_singleContainerName];
		}
	}

	/// <summary>
	/// All SDK <see cref="Container"/> references keyed by container name.
	/// Case-sensitive (matching real Cosmos DB). Contents may change when containers
	/// are dynamically created or deleted via the SDK.
	/// When container names are unique across databases, flat access works.
	/// When names collide, throws <see cref="KeyNotFoundException"/> with guidance.
	/// </summary>
	public IReadOnlyDictionary<string, Container> Containers =>
		BuildFlatDictionary(
			(dbName, containerName, registryKey) =>
				_registry.SdkContainers.TryGetValue(registryKey, out var c) ? c : null!);

	// ─── Tier 2: Test setup ───────────────────────────────────────────────────

	/// <summary>
	/// Returns an <see cref="IContainerTestSetup"/> for the specified container.
	/// For single-container setups, <paramref name="containerName"/> can be omitted.
	/// </summary>
	public IContainerTestSetup SetupContainer(string? containerName = null)
	{
		if (containerName is null)
		{
			if (_singleContainerName is null)
				throw new InvalidOperationException(
					"SetupContainer() with no name requires a single-container setup. " +
					$"Use SetupContainer(\"name\") instead. Available containers: {AvailableNames}.");
			return _registry.BackingContainers[_singleContainerName];
		}

		var registryKey = ResolveContainerRegistryKey(containerName, "SetupContainer");
		return _registry.BackingContainers[registryKey];
	}

	// ─── State management convenience methods ─────────────────────────────────

	/// <summary>Exports the current container state as a JSON string.</summary>
	public string ExportState(string? containerName = null) =>
		SetupContainer(containerName).ExportState();

	/// <summary>Exports the current container state to a file.</summary>
	public void ExportStateToFile(string path, string? containerName = null) =>
		SetupContainer(containerName).ExportStateToFile(path);

	/// <summary>Imports container state from a JSON string, replacing all existing data.</summary>
	public void ImportState(string json, string? containerName = null) =>
		SetupContainer(containerName).ImportState(json);

	/// <summary>Imports container state from a file, replacing all existing data.</summary>
	public void ImportStateFromFile(string filePath, string? containerName = null) =>
		SetupContainer(containerName).ImportStateFromFile(filePath);

	/// <summary>
	/// Restores the container to its state at the specified point in time
	/// by replaying the change feed.
	/// </summary>
	public void RestoreToPointInTime(DateTimeOffset timestamp, string? containerName = null) =>
		SetupContainer(containerName).RestoreToPointInTime(timestamp);

	/// <summary>Removes all items, ETags, timestamps, and change feed entries.</summary>
	public void ClearItems(string? containerName = null) =>
		SetupContainer(containerName).ClearItems();

	// ─── Tier 3: Fault injection & diagnostics ────────────────────────────────

	/// <summary>
	/// The <see cref="FakeCosmosHandler"/> for single-container setups.
	/// Throws <see cref="InvalidOperationException"/> if multiple containers are registered.
	/// </summary>
	public FakeCosmosHandler Handler
	{
		get
		{
			if (_singleContainerName is null)
				throw new InvalidOperationException(
					"Handler property requires a single-container setup. " +
					$"Use Handlers[\"name\"] or GetHandler(\"name\") instead. Available containers: {AvailableNames}.");
			return _registry.Handlers[_singleContainerName];
		}
	}

	/// <summary>
	/// All <see cref="FakeCosmosHandler"/> instances keyed by container name.
	/// Contents may change when containers are dynamically created or deleted.
	/// When container names collide across databases, throws with guidance.
	/// </summary>
	public IReadOnlyDictionary<string, FakeCosmosHandler> Handlers =>
		BuildFlatDictionary(
			(dbName, containerName, registryKey) =>
				_registry.Handlers.TryGetValue(registryKey, out var h) ? h : null!);

	/// <summary>
	/// Returns the <see cref="FakeCosmosHandler"/> for the specified container.
	/// </summary>
	public FakeCosmosHandler GetHandler(string name)
	{
		var registryKey = ResolveContainerRegistryKey(name, "GetHandler");
		return _registry.Handlers[registryKey];
	}

	/// <summary>
	/// Sets or clears the fault injector on all handlers.
	/// Pass <c>null</c> to clear.
	/// </summary>
	public void SetFaultInjector(Func<HttpRequestMessage, HttpResponseMessage?>? injector)
	{
		foreach (var handler in _registry.Handlers.Values)
			handler.FaultInjector = injector;
	}

	// ─── Tier 4: Multi-database ───────────────────────────────────────────────

	/// <summary>
	/// Returns an <see cref="InMemoryDatabaseResult"/> for the specified database.
	/// </summary>
	public InMemoryDatabaseResult Database(string databaseName)
	{
		if (!_registry.DatabaseContainerKeys.TryGetValue(databaseName, out var containerKeys))
			throw new InvalidOperationException(
				$"Database '{databaseName}' not found. Available databases: " +
				string.Join(", ", _registry.DatabaseContainerKeys.Keys.OrderBy(k => k)) + ".");
		return new InMemoryDatabaseResult(databaseName, _registry, containerKeys);
	}

	/// <summary>
	/// All databases keyed by name.
	/// </summary>
	public IReadOnlyDictionary<string, InMemoryDatabaseResult> Databases
	{
		get
		{
			var dict = new Dictionary<string, InMemoryDatabaseResult>(StringComparer.Ordinal);
			foreach (var (dbName, containerKeys) in _registry.DatabaseContainerKeys)
				dict[dbName] = new InMemoryDatabaseResult(dbName, _registry, containerKeys);
			return new ReadOnlyDictionary<string, InMemoryDatabaseResult>(dict);
		}
	}

	// ─── Dispose ──────────────────────────────────────────────────────────────

	/// <inheritdoc />
	public void Dispose()
	{
		Client.Dispose();
	}

	/// <inheritdoc />
	public ValueTask DisposeAsync()
	{
		Dispose();
		return ValueTask.CompletedTask;
	}

	private string AvailableNames => string.Join(", ",
		_registry.DatabaseContainerKeys.Values
			.SelectMany(m => m.Keys)
			.Distinct(StringComparer.Ordinal)
			.OrderBy(k => k));

	/// <summary>
	/// Resolves a plain container name to its registry key, throwing for ambiguous
	/// or missing containers.
	/// </summary>
	private string ResolveContainerRegistryKey(string containerName, string methodName)
	{
		var matches = new List<(string DatabaseName, string RegistryKey)>();
		foreach (var (dbName, containers) in _registry.DatabaseContainerKeys)
		{
			if (containers.TryGetValue(containerName, out var registryKey))
				matches.Add((dbName, registryKey));
		}

		return matches.Count switch
		{
			0 => throw new InvalidOperationException(
				$"Container '{containerName}' not found. Available containers: {AvailableNames}."),
			1 => matches[0].RegistryKey,
			_ => throw new InvalidOperationException(
				$"Container '{containerName}' exists in multiple databases: " +
				string.Join(", ", matches.Select(m => m.DatabaseName).OrderBy(n => n)) + ". " +
				$"Use cosmos.Database(\"...\").{methodName}(\"{containerName}\") instead.")
		};
	}

	/// <summary>
	/// Builds a flat dictionary from container names to values, with ambiguity detection.
	/// </summary>
	private IReadOnlyDictionary<string, T> BuildFlatDictionary<T>(
		Func<string, string, string, T?> valueSelector) where T : class
	{
		var unambiguous = new Dictionary<string, T>(StringComparer.Ordinal);
		var ambiguous = new Dictionary<string, List<string>>(StringComparer.Ordinal);

		foreach (var (dbName, containers) in _registry.DatabaseContainerKeys)
		{
			foreach (var (containerName, registryKey) in containers)
			{
				var value = valueSelector(dbName, containerName, registryKey);
				if (value is null) continue;

				if (ambiguous.ContainsKey(containerName))
				{
					ambiguous[containerName].Add(dbName);
				}
				else if (unambiguous.ContainsKey(containerName))
				{
					// Move from unambiguous to ambiguous
					var existingDb = _registry.DatabaseContainerKeys
						.Where(d => d.Value.ContainsKey(containerName))
						.Select(d => d.Key)
						.First(d => d != dbName);
					ambiguous[containerName] = new List<string> { existingDb, dbName };
					unambiguous.Remove(containerName);
				}
				else
				{
					unambiguous[containerName] = value;
				}
			}
		}

		if (ambiguous.Count == 0)
			return new ReadOnlyDictionary<string, T>(unambiguous);

		return new AmbiguityDetectingDictionary<T>(unambiguous, ambiguous);
	}
}

/// <summary>
/// Fluent builder for configuring containers within a named database.
/// </summary>
public sealed class InMemoryDatabaseBuilder
{
	internal List<InMemoryCosmosBuilder.ContainerSpec> Containers { get; } = new();

	/// <summary>
	/// Adds a container with a single partition key path.
	/// </summary>
	public InMemoryDatabaseBuilder AddContainer(string name, string partitionKeyPath,
		Action<IContainerTestSetup>? configure = null)
	{
		InMemoryCosmos.ValidateContainerName(name);
		InMemoryCosmos.ValidatePartitionKeyPath(partitionKeyPath);
		ValidateNoDuplicate(name);
		Containers.Add(new InMemoryCosmosBuilder.ContainerSpec(name, new[] { partitionKeyPath }, configure));
		return this;
	}

	/// <summary>
	/// Adds a container with hierarchical (composite) partition key paths.
	/// </summary>
	public InMemoryDatabaseBuilder AddContainer(string name, string[] partitionKeyPaths,
		Action<IContainerTestSetup>? configure = null)
	{
		InMemoryCosmos.ValidateContainerName(name);
		if (partitionKeyPaths is null || partitionKeyPaths.Length == 0)
			throw new ArgumentException("At least one partition key path must be provided.", nameof(partitionKeyPaths));
		foreach (var path in partitionKeyPaths)
			InMemoryCosmos.ValidatePartitionKeyPath(path);
		ValidateNoDuplicate(name);
		Containers.Add(new InMemoryCosmosBuilder.ContainerSpec(name, partitionKeyPaths, configure));
		return this;
	}

	private void ValidateNoDuplicate(string name)
	{
		if (Containers.Any(c => c.Name == name))
			throw new InvalidOperationException($"Container '{name}' has already been added.");
	}
}

/// <summary>
/// Provides database-scoped access to containers, test setup, and fault injection.
/// </summary>
public sealed class InMemoryDatabaseResult
{
	private readonly FakeCosmosHandler.DynamicContainerRegistry _registry;
	private readonly ConcurrentDictionary<string, string> _containerKeys;

	internal InMemoryDatabaseResult(
		string databaseName,
		FakeCosmosHandler.DynamicContainerRegistry registry,
		ConcurrentDictionary<string, string> containerKeys)
	{
		DatabaseName = databaseName;
		_registry = registry;
		_containerKeys = containerKeys;
	}

	/// <summary>The database name.</summary>
	public string DatabaseName { get; }

	/// <summary>
	/// All SDK <see cref="Container"/> references in this database, keyed by container name.
	/// </summary>
	public IReadOnlyDictionary<string, Container> Containers
	{
		get
		{
			var dict = new Dictionary<string, Container>(StringComparer.Ordinal);
			foreach (var (name, registryKey) in _containerKeys)
			{
				if (_registry.SdkContainers.TryGetValue(registryKey, out var container))
					dict[name] = container;
			}
			return new ReadOnlyDictionary<string, Container>(dict);
		}
	}

	/// <summary>
	/// Returns an <see cref="IContainerTestSetup"/> for the specified container in this database.
	/// </summary>
	public IContainerTestSetup SetupContainer(string containerName)
	{
		if (!_containerKeys.TryGetValue(containerName, out var registryKey))
			throw new InvalidOperationException(
				$"Container '{containerName}' not found in database '{DatabaseName}'.");
		return _registry.BackingContainers[registryKey];
	}

	// ─── State management convenience methods ─────────────────────────────────

	/// <summary>Exports the current container state as a JSON string.</summary>
	public string ExportState(string containerName) =>
		SetupContainer(containerName).ExportState();

	/// <summary>Exports the current container state to a file.</summary>
	public void ExportStateToFile(string containerName, string path) =>
		SetupContainer(containerName).ExportStateToFile(path);

	/// <summary>Imports container state from a JSON string, replacing all existing data.</summary>
	public void ImportState(string containerName, string json) =>
		SetupContainer(containerName).ImportState(json);

	/// <summary>Imports container state from a file, replacing all existing data.</summary>
	public void ImportStateFromFile(string containerName, string filePath) =>
		SetupContainer(containerName).ImportStateFromFile(filePath);

	/// <summary>
	/// Restores the container to its state at the specified point in time
	/// by replaying the change feed.
	/// </summary>
	public void RestoreToPointInTime(string containerName, DateTimeOffset timestamp) =>
		SetupContainer(containerName).RestoreToPointInTime(timestamp);

	/// <summary>Removes all items, ETags, timestamps, and change feed entries.</summary>
	public void ClearItems(string containerName) =>
		SetupContainer(containerName).ClearItems();

	/// <summary>
	/// Returns the <see cref="FakeCosmosHandler"/> for the specified container in this database.
	/// </summary>
	public FakeCosmosHandler GetHandler(string containerName)
	{
		if (!_containerKeys.TryGetValue(containerName, out var registryKey))
			throw new InvalidOperationException(
				$"Container '{containerName}' not found in database '{DatabaseName}'.");
		return _registry.Handlers[registryKey];
	}

	/// <summary>
	/// All <see cref="FakeCosmosHandler"/> instances in this database, keyed by container name.
	/// </summary>
	public IReadOnlyDictionary<string, FakeCosmosHandler> Handlers
	{
		get
		{
			var dict = new Dictionary<string, FakeCosmosHandler>(StringComparer.Ordinal);
			foreach (var (name, registryKey) in _containerKeys)
			{
				if (_registry.Handlers.TryGetValue(registryKey, out var handler))
					dict[name] = handler;
			}
			return new ReadOnlyDictionary<string, FakeCosmosHandler>(dict);
		}
	}

	/// <summary>
	/// Sets or clears the fault injector on all handlers in this database.
	/// </summary>
	public void SetFaultInjector(Func<HttpRequestMessage, HttpResponseMessage?>? injector)
	{
		foreach (var (_, registryKey) in _containerKeys)
		{
			if (_registry.Handlers.TryGetValue(registryKey, out var handler))
				handler.FaultInjector = injector;
		}
	}
}

/// <summary>
/// A read-only dictionary that detects ambiguous container names across databases
/// and throws descriptive errors.
/// </summary>
internal sealed class AmbiguityDetectingDictionary<TValue> : IReadOnlyDictionary<string, TValue>
{
	private readonly Dictionary<string, TValue> _unambiguous;
	private readonly Dictionary<string, List<string>> _ambiguous;

	internal AmbiguityDetectingDictionary(
		Dictionary<string, TValue> unambiguous,
		Dictionary<string, List<string>> ambiguous)
	{
		_unambiguous = unambiguous;
		_ambiguous = ambiguous;
	}

	public TValue this[string key]
	{
		get
		{
			if (_ambiguous.TryGetValue(key, out var databases))
				throw new KeyNotFoundException(
					$"Container '{key}' exists in multiple databases: " +
					string.Join(", ", databases.OrderBy(d => d)) + ". " +
					$"Use cosmos.Database(\"...\").Containers[\"{key}\"] instead.");
			if (!_unambiguous.TryGetValue(key, out var value))
				throw new KeyNotFoundException($"Container '{key}' not found.");
			return value;
		}
	}

	public bool ContainsKey(string key) => _unambiguous.ContainsKey(key);

	public bool TryGetValue(string key, out TValue value) => _unambiguous.TryGetValue(key, out value!);

	public int Count => _unambiguous.Count;

	public IEnumerable<string> Keys => _unambiguous.Keys;

	public IEnumerable<TValue> Values => _unambiguous.Values;

	public IEnumerator<KeyValuePair<string, TValue>> GetEnumerator() => _unambiguous.GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
