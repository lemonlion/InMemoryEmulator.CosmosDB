#nullable disable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="CosmosClient"/> for testing.
/// Manages a collection of <see cref="InMemoryDatabase"/> instances, each containing
/// <see cref="InMemoryContainer"/> instances. Databases and containers are created
/// lazily on first access via <see cref="GetDatabase"/> or <see cref="GetContainer"/>.
/// </summary>
/// <remarks>
/// <para>
/// This class is intentionally <b>not sealed</b> to support Pattern 2 (typed CosmosClient subclasses).
/// In test projects, you can create one-liner subclasses that shadow production typed clients:
/// <code>
/// // Production code (unchanged):
/// public class EmployeeCosmosClient : CosmosClient { ... }
///
/// // Test project shadow type:
/// public class EmployeeCosmosClient : InMemoryCosmosClient { }
/// </code>
/// </para>
/// <para>
/// The <see cref="Endpoint"/> returns <c>https://localhost:8081/</c> and throughput/RU values
/// are synthetic. No network calls are made.
/// </para>
/// </remarks>
[Obsolete("Use UseInMemoryCosmosDB() or UseInMemoryCosmosContainers() instead. " +
		  "InMemoryCosmosClient bypasses the SDK HTTP pipeline. " +
		  "FakeCosmosHandler-based methods provide higher fidelity and native .ToFeedIterator() support.")]
internal class InMemoryCosmosClient : CosmosClient
{
	private readonly ConcurrentDictionary<string, InMemoryDatabase> _databases = new();
	private readonly ConcurrentDictionary<string, bool> _explicitlyCreatedDatabases = new();
	private bool _disposed;

	private static readonly Uri EmulatorEndpoint = new("https://localhost:8081/");
	private readonly CosmosClientOptions _clientOptions = new();
	private readonly CosmosResponseFactory _responseFactory = Substitute.For<CosmosResponseFactory>();

	/// <summary>Returns <c>https://localhost:8081/</c>.</summary>
	public override Uri Endpoint => EmulatorEndpoint;

	/// <summary>Returns a default <see cref="CosmosClientOptions"/> instance.</summary>
	public override CosmosClientOptions ClientOptions => _clientOptions;

	/// <summary>Returns a stubbed <see cref="CosmosResponseFactory"/>.</summary>
	public override CosmosResponseFactory ResponseFactory => _responseFactory;

	/// <summary>
	/// Creates a database if it does not already exist. Returns <see cref="HttpStatusCode.Created"/>
	/// for new databases or <see cref="HttpStatusCode.OK"/> for existing ones.
	/// </summary>
	/// <param name="id">The database identifier.</param>
	/// <param name="throughput">Ignored. Throughput is not enforced in-memory.</param>
	/// <param name="requestOptions">Ignored.</param>
	/// <param name="cancellationToken">Ignored.</param>
	public override Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(
		string id, int? throughput = null, RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		ThrowIfDisposed();
		cancellationToken.ThrowIfCancellationRequested();
		ArgumentNullException.ThrowIfNull(id);
		ArgumentException.ThrowIfNullOrEmpty(id);
		ValidateResourceName(id, "Database");
		var created = false;
		var database = _databases.GetOrAdd(id, name => { created = true; return new InMemoryDatabase(name, this); });
		_explicitlyCreatedDatabases.TryAdd(id, true);
		var response = BuildDatabaseResponse(database, created ? HttpStatusCode.Created : HttpStatusCode.OK);
		return Task.FromResult(response);
	}

	public override Task<DatabaseResponse> CreateDatabaseIfNotExistsAsync(
		string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		return CreateDatabaseIfNotExistsAsync(id, (int?)null, requestOptions, cancellationToken);
	}

	/// <summary>
	/// Creates a new database. Throws <see cref="CosmosException"/> with
	/// <see cref="HttpStatusCode.Conflict"/> if a database with the same <paramref name="id"/> already exists.
	/// </summary>
	/// <param name="id">The database identifier.</param>
	/// <param name="throughput">Ignored. Throughput is not enforced in-memory.</param>
	/// <param name="requestOptions">Ignored.</param>
	/// <param name="cancellationToken">Ignored.</param>
	public override Task<DatabaseResponse> CreateDatabaseAsync(
		string id, int? throughput = null, RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		ThrowIfDisposed();
		cancellationToken.ThrowIfCancellationRequested();
		ArgumentNullException.ThrowIfNull(id);
		ArgumentException.ThrowIfNullOrEmpty(id);
		ValidateResourceName(id, "Database");
		var database = new InMemoryDatabase(id, this);
		if (!_databases.TryAdd(id, database))
		{
			throw InMemoryCosmosException.Create("Database already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);
		}
		_explicitlyCreatedDatabases.TryAdd(id, true);
		var response = BuildDatabaseResponse(database, HttpStatusCode.Created);
		return Task.FromResult(response);
	}

	public override Task<DatabaseResponse> CreateDatabaseAsync(
		string id, ThroughputProperties throughputProperties, RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		return CreateDatabaseAsync(id, (int?)null, requestOptions, cancellationToken);
	}

	/// <summary>
	/// Creates a new database using stream semantics. Returns <see cref="HttpStatusCode.Created"/>
	/// on success or <see cref="HttpStatusCode.Conflict"/> if the database already exists.
	/// </summary>
	public override Task<ResponseMessage> CreateDatabaseStreamAsync(
		DatabaseProperties databaseProperties, int? throughput = null,
		RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		ThrowIfDisposed();
		cancellationToken.ThrowIfCancellationRequested();
		ArgumentNullException.ThrowIfNull(databaseProperties);
		var id = databaseProperties.Id;
		ArgumentNullException.ThrowIfNull(id);
		ArgumentException.ThrowIfNullOrEmpty(id);
		ValidateResourceName(id, "Database");
		var database = new InMemoryDatabase(id, this);
		if (!_databases.TryAdd(id, database))
		{
			return Task.FromResult(CreateStreamResponse(HttpStatusCode.Conflict));
		}
		_explicitlyCreatedDatabases.TryAdd(id, true);
		return Task.FromResult(CreateStreamResponse(HttpStatusCode.Created, databaseProperties));
	}

	private static ResponseMessage CreateStreamResponse(HttpStatusCode statusCode, DatabaseProperties databaseProperties = null)
	{
		var msg = new ResponseMessage(statusCode);
		msg.Headers["x-ms-activity-id"] = Guid.NewGuid().ToString();
		msg.Headers["x-ms-request-charge"] = "1";
		msg.Headers["x-ms-session-token"] = "0:0#0";
		if (databaseProperties != null)
		{
			var json = Newtonsoft.Json.JsonConvert.SerializeObject(databaseProperties);
			msg.Content = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
		}
		return msg;
	}

	public Task<ResponseMessage> CreateDatabaseStreamAsync(
		DatabaseProperties databaseProperties, ThroughputProperties throughputProperties,
		RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
	{
		return CreateDatabaseStreamAsync(databaseProperties, (int?)null, requestOptions, cancellationToken);
	}

	/// <summary>
	/// Gets or creates an <see cref="InMemoryDatabase"/> with the given <paramref name="id"/>.
	/// The database is created lazily on first access.
	/// </summary>
	/// <param name="id">The database identifier.</param>
	public override Database GetDatabase(string id)
	{
		ThrowIfDisposed();
		ArgumentNullException.ThrowIfNull(id);
		ArgumentException.ThrowIfNullOrEmpty(id);
		return _databases.GetOrAdd(id, name => new InMemoryDatabase(name, this));
	}

	/// <summary>
	/// Gets or creates a container within the specified database. Both the database and
	/// container are created lazily on first access.
	/// </summary>
	/// <param name="databaseId">The database identifier.</param>
	/// <param name="containerId">The container identifier.</param>
	public override Container GetContainer(string databaseId, string containerId)
	{
		ThrowIfDisposed();
		ArgumentNullException.ThrowIfNull(databaseId);
		ArgumentException.ThrowIfNullOrEmpty(databaseId);
		ArgumentNullException.ThrowIfNull(containerId);
		ArgumentException.ThrowIfNullOrEmpty(containerId);
		var database = _databases.GetOrAdd(databaseId, name => new InMemoryDatabase(name, this));
		return database.GetOrCreateContainer(containerId);
	}

	/// <summary>
	/// Returns a feed iterator over all databases. The query text is ignored;
	/// all databases are returned as <see cref="DatabaseProperties"/>.
	/// </summary>
	public override FeedIterator<T> GetDatabaseQueryIterator<T>(
		string queryText = null, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		var offset = int.TryParse(continuationToken, out var o) ? o : 0;
		IEnumerable<DatabaseProperties> items = _databases.Values
			.Select(db => new DatabaseProperties(db.Id));
		var idFilter = ExtractIdFilter(queryText);
		if (idFilter is not null)
			items = items.Where(d => string.Equals(d.Id, idFilter, StringComparison.Ordinal));
		return new InMemoryFeedIterator<T>(items.Cast<T>().ToList(), requestOptions?.MaxItemCount, offset);
	}

	public override FeedIterator<T> GetDatabaseQueryIterator<T>(
		QueryDefinition queryDefinition, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		return GetDatabaseQueryIterator<T>(queryDefinition?.QueryText, continuationToken, requestOptions);
	}

	public override FeedIterator GetDatabaseQueryStreamIterator(
		string queryText = null, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		return new InMemoryStreamFeedIterator(
			() => _databases.Values
				.Select(db => new { id = db.Id })
				.ToList(),
			"Databases");
	}

	public override FeedIterator GetDatabaseQueryStreamIterator(
		QueryDefinition queryDefinition, string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		return GetDatabaseQueryStreamIterator((string)null, continuationToken, requestOptions);
	}

	/// <summary>
	/// Returns synthetic <see cref="AccountProperties"/> with Id "in-memory-emulator".
	/// Falls back to an NSubstitute stub if reflection fails.
	/// </summary>
	public override Task<AccountProperties> ReadAccountAsync()
	{
		try
		{
			var account = (AccountProperties)Activator.CreateInstance(
				typeof(AccountProperties),
				nonPublic: true);
			var idProp = typeof(AccountProperties).GetProperty(nameof(AccountProperties.Id));
			idProp?.SetValue(account, "in-memory-emulator");
			return Task.FromResult(account);
		}
		catch (Exception ex) when (ex is MissingMethodException or MissingMemberException or System.Reflection.TargetInvocationException)
		{
			Trace.TraceWarning(
				"InMemoryCosmosClient: AccountProperties reflection failed " +
				$"({ex.GetType().Name}: {ex.Message}). " +
				"Falling back to NSubstitute stub.");
			var stub = Substitute.For<AccountProperties>();
			return Task.FromResult(stub);
		}
	}

	internal bool RemoveDatabase(string id)
	{
		_explicitlyCreatedDatabases.TryRemove(id, out _);
		return _databases.TryRemove(id, out _);
	}

	internal bool IsDatabaseExplicitlyCreated(string id)
	{
		return _explicitlyCreatedDatabases.ContainsKey(id);
	}

	/// <summary>
	/// Disposes the client and cascades disposal to all containers, triggering
	/// state persistence for any container with <see cref="InMemoryContainer.StateFilePath"/> set.
	/// </summary>
	protected override void Dispose(bool disposing)
	{
		if (disposing && !_disposed)
		{
			foreach (var db in _databases.Values)
				foreach (var container in db.GetAllContainers())
					container.Dispose();
		}
		_disposed = true;
	}

	private void ThrowIfDisposed()
	{
		ObjectDisposedException.ThrowIf(_disposed, this);
	}

	internal static void ValidateResourceName(string name, string resourceType)
	{
		if (name.Length > 255)
		{
			throw InMemoryCosmosException.Create(
				$"{resourceType} name must not exceed 255 characters.",
				HttpStatusCode.BadRequest, 0, string.Empty, 0);
		}
		if (name.IndexOfAny(['/', '\\', '#', '?']) >= 0)
		{
			throw InMemoryCosmosException.Create(
				$"{resourceType} name must not contain '/', '\\', '#', or '?' characters.",
				HttpStatusCode.BadRequest, 0, string.Empty, 0);
		}
	}

	private static DatabaseResponse BuildDatabaseResponse(Database database, HttpStatusCode statusCode)
	{
		var response = Substitute.For<DatabaseResponse>();
		response.Database.Returns(database);
		response.StatusCode.Returns(statusCode);
		response.Resource.Returns(new DatabaseProperties(database.Id));
		return response;
	}

	/// <summary>
	/// Extracts a simple <c>WHERE c.id = 'value'</c> filter from SQL text.
	/// Returns the extracted id value, or null if no simple id filter is found.
	/// </summary>
	internal static string ExtractIdFilter(string queryText)
	{
		if (string.IsNullOrWhiteSpace(queryText)) return null;
		var match = System.Text.RegularExpressions.Regex.Match(queryText,
			@"WHERE\s+\w+\.id\s*=\s*'([^']*)'", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
		return match.Success ? match.Groups[1].Value : null;
	}
}
