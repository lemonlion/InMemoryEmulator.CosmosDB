#nullable disable
#pragma warning disable CS0618 // InMemoryCosmosClient is obsolete but InMemoryDatabase still depends on it
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="Database"/> for testing.
/// Manages a collection of <see cref="InMemoryContainer"/> instances.
/// Containers are created lazily via <see cref="GetContainer"/> or explicitly
/// via <see cref="CreateContainerAsync"/> / <see cref="CreateContainerIfNotExistsAsync"/>.
/// </summary>
/// <remarks>
/// Throughput operations return synthetic values (400 RU/s by default).
/// User operations return stub responses with synthetic metadata.
/// Client encryption key operations throw <see cref="System.NotImplementedException"/>.
/// </remarks>
internal class InMemoryDatabase : Database
{
    private readonly ConcurrentDictionary<string, InMemoryContainer> _containers = new();
    private readonly ConcurrentDictionary<string, bool> _explicitlyCreatedContainers = new();
    private readonly ConcurrentDictionary<string, InMemoryUser> _users = new();
    private readonly InMemoryCosmosClient _client;
    private int _throughput = 400;

    /// <summary>
    /// Creates a new <see cref="InMemoryDatabase"/> with no parent client.
    /// </summary>
    /// <param name="id">The database identifier.</param>
    public InMemoryDatabase(string id) : this(id, null) { }

    /// <summary>
    /// Creates a new <see cref="InMemoryDatabase"/> owned by the given <paramref name="client"/>.
    /// </summary>
    /// <param name="id">The database identifier.</param>
    /// <param name="client">The owning <see cref="InMemoryCosmosClient"/>, or null.</param>
    public InMemoryDatabase(string id, InMemoryCosmosClient client)
    {
        Id = id;
        _client = client;
    }

    /// <summary>The database identifier.</summary>
    public override string Id { get; }

    /// <summary>The owning <see cref="InMemoryCosmosClient"/>, or null.</summary>
    public override CosmosClient Client => _client;

    /// <summary>
    /// Gets or creates an <see cref="InMemoryContainer"/> with the given identifier and partition key path.
    /// Used internally by DI extensions and <see cref="GetContainer"/>.
    /// </summary>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="partitionKeyPath">The JSON path to the partition key field (e.g. <c>/partitionKey</c>).</param>
    internal InMemoryContainer GetOrCreateContainer(string containerId, string partitionKeyPath = "/id")
    {
        var isNew = false;
        var container = _containers.GetOrAdd(containerId, name => { isNew = true; return new InMemoryContainer(name, partitionKeyPath); });
        if (isNew)
            container.ExplicitlyCreated = false;
        container.OnDeleted ??= () => _containers.TryRemove(containerId, out _);
        container.SetParentDatabase(Id);
        return container;
    }

    internal InMemoryContainer GetOrCreateContainer(ContainerProperties containerProperties)
    {
        var isNew = false;
        var container = _containers.GetOrAdd(containerProperties.Id, _ => { isNew = true; return new InMemoryContainer(containerProperties); });
        if (isNew)
            container.ExplicitlyCreated = false;
        container.OnDeleted ??= () => _containers.TryRemove(containerProperties.Id, out _);
        container.SetParentDatabase(Id);
        return container;
    }

    // ── CreateContainerIfNotExistsAsync ─────────────────────────────────────

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var created = false;
        var container = _containers.GetOrAdd(id, name => { created = true; return new InMemoryContainer(name, partitionKeyPath); });
        // If the container was lazily created by GetContainer() with a default PK path,
        // replace it with one that has the correct partition key path.
        if (!created && !container.ExplicitlyCreated)
        {
            var replacement = new InMemoryContainer(id, partitionKeyPath);
            replacement.OnDeleted = () => _containers.TryRemove(id, out _);
            replacement.SetParentDatabase(Id);
            replacement.ExplicitlyCreated = true;
            _containers[id] = replacement;
            container = replacement;
            created = true;
        }
        container.OnDeleted ??= () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        if (throughput.HasValue)
            container._throughput = throughput.Value;
        _explicitlyCreatedContainers.TryAdd(id, true);
        var response = BuildContainerResponse(container, partitionKeyPath, created ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var created = false;
        var container = _containers.GetOrAdd(id, _ => { created = true; return new InMemoryContainer(containerProperties); });
        // If the container was lazily created by GetContainer() with a default PK path,
        // replace it with one that has the correct properties.
        if (!created && !container.ExplicitlyCreated)
        {
            var replacement = new InMemoryContainer(containerProperties);
            replacement.OnDeleted = () => _containers.TryRemove(id, out _);
            replacement.SetParentDatabase(Id);
            replacement.ExplicitlyCreated = true;
            replacement.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
            if (containerProperties.IndexingPolicy is not null)
                replacement.IndexingPolicy = containerProperties.IndexingPolicy;
            _containers[id] = replacement;
            container = replacement;
            created = true;
        }
        container.OnDeleted ??= () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        if (throughput.HasValue)
            container._throughput = throughput.Value;
        _explicitlyCreatedContainers.TryAdd(id, true);
        if (created)
        {
            container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
            if (containerProperties.IndexingPolicy is not null)
                container.IndexingPolicy = containerProperties.IndexingPolicy;
        }
        var response = BuildContainerResponse(container, containerProperties, created ? HttpStatusCode.Created : HttpStatusCode.OK);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerIfNotExistsAsync(
        ContainerProperties containerProperties, ThroughputProperties throughputProperties,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return CreateContainerIfNotExistsAsync(containerProperties, throughputProperties?.Throughput, requestOptions, cancellationToken);
    }

    // ── CreateContainerAsync ────────────────────────────────────────────────

    public override Task<ContainerResponse> CreateContainerAsync(
        string id, string partitionKeyPath, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentException.ThrowIfNullOrEmpty(id);
        InMemoryCosmosClient.ValidateResourceName(id, "Container");
        ArgumentNullException.ThrowIfNull(partitionKeyPath);
        var container = new InMemoryContainer(id, partitionKeyPath);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        if (throughput.HasValue)
            container._throughput = throughput.Value;
        if (!_containers.TryAdd(id, container))
        {
            // If the existing container was lazily created by GetContainer(), replace it.
            // Real Cosmos DB: GetContainer() returns a lightweight proxy that doesn't create anything.
            if (_containers.TryGetValue(id, out var existing) && !existing.ExplicitlyCreated)
            {
                _containers[id] = container;
            }
            else
            {
                throw InMemoryCosmosException.Create("Container already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);
            }
        }
        _explicitlyCreatedContainers.TryAdd(id, true);
        var response = BuildContainerResponse(container, partitionKeyPath, HttpStatusCode.Created);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        InMemoryCosmosClient.ValidateResourceName(id, "Container");
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var container = new InMemoryContainer(containerProperties);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
        if (throughput.HasValue)
            container._throughput = throughput.Value;
        if (containerProperties.IndexingPolicy is not null)
            container.IndexingPolicy = containerProperties.IndexingPolicy;
        if (!_containers.TryAdd(id, container))
        {
            // If the existing container was lazily created by GetContainer(), replace it.
            if (_containers.TryGetValue(id, out var existing) && !existing.ExplicitlyCreated)
            {
                _containers[id] = container;
            }
            else
            {
                throw InMemoryCosmosException.Create("Container already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);
            }
        }
        _explicitlyCreatedContainers.TryAdd(id, true);
        var response = BuildContainerResponse(container, containerProperties, HttpStatusCode.Created);
        return Task.FromResult(response);
    }

    public override Task<ContainerResponse> CreateContainerAsync(
        ContainerProperties containerProperties, ThroughputProperties throughputProperties,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection
        //   Throughput can be specified via ThroughputProperties; extract the value.
        return CreateContainerAsync(containerProperties, throughputProperties?.Throughput, requestOptions, cancellationToken);
    }

    // ── CreateContainerStreamAsync ──────────────────────────────────────────

    public override Task<ResponseMessage> CreateContainerStreamAsync(
        ContainerProperties containerProperties, int? throughput = null,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var id = containerProperties.Id;
        if (string.IsNullOrEmpty(containerProperties.PartitionKeyPath) && containerProperties.PartitionKeyPaths is null)
            containerProperties.PartitionKeyPath = "/id";
        var container = new InMemoryContainer(containerProperties);
        container.OnDeleted = () => _containers.TryRemove(id, out _);
        container.SetParentDatabase(Id);
        container.ExplicitlyCreated = true;
        container.DefaultTimeToLive = containerProperties.DefaultTimeToLive;
        if (throughput.HasValue)
            container._throughput = throughput.Value;
        if (containerProperties.IndexingPolicy is not null)
            container.IndexingPolicy = containerProperties.IndexingPolicy;
        if (!_containers.TryAdd(id, container))
        {
            if (_containers.TryGetValue(id, out var existing) && !existing.ExplicitlyCreated)
            {
                _containers[id] = container;
            }
            else
            {
                return Task.FromResult(CreateStreamResponse(HttpStatusCode.Conflict));
            }
        }
        _explicitlyCreatedContainers.TryAdd(id, true);
        return Task.FromResult(CreateStreamResponse(HttpStatusCode.Created));
    }

    public override Task<ResponseMessage> CreateContainerStreamAsync(
        ContainerProperties containerProperties, ThroughputProperties throughputProperties,
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return CreateContainerStreamAsync(containerProperties, throughputProperties?.Throughput, requestOptions, cancellationToken);
    }

    // ── GetContainer ────────────────────────────────────────────────────────

    public override Container GetContainer(string id)
    {
        return GetOrCreateContainer(id);
    }

    internal bool IsContainerExplicitlyCreated(string id)
    {
        return _explicitlyCreatedContainers.ContainsKey(id);
    }

    internal IEnumerable<InMemoryContainer> GetAllContainers() => _containers.Values;

    // ── GetContainerQueryIterator ───────────────────────────────────────────

    public override FeedIterator<T> GetContainerQueryIterator<T>(
        string queryText = null, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        var offset = int.TryParse(continuationToken, out var o) ? o : 0;
        IEnumerable<ContainerProperties> items = _containers.Values
            .Select(c => new ContainerProperties(c.Id, c.PartitionKeyPaths));
        var idFilter = InMemoryCosmosClient.ExtractIdFilter(queryText);
        if (idFilter is not null)
            items = items.Where(cp => string.Equals(cp.Id, idFilter, StringComparison.Ordinal));

        // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-collections
        //   SELECT VALUE queries project scalar values (e.g. container IDs as strings).
        if (typeof(T) != typeof(ContainerProperties) && IsSelectValueIdQuery(queryText))
        {
            var ids = items.Select(cp => (T)(object)cp.Id).ToList();
            return new InMemoryFeedIterator<T>(ids, requestOptions?.MaxItemCount, offset);
        }

        return new InMemoryFeedIterator<T>(items.Select(cp => (T)(object)cp).ToList(), requestOptions?.MaxItemCount, offset);
    }

    private static bool IsSelectValueIdQuery(string queryText)
    {
        if (string.IsNullOrWhiteSpace(queryText))
            return false;
        // Matches patterns like: SELECT VALUE c.id, SELECT VALUE(c.id), SELECT VALUE c["id"]
        return Regex.IsMatch(queryText, @"SELECT\s+VALUE\s*\(?.*\.id\)?", RegexOptions.IgnoreCase);
    }

    public override FeedIterator<T> GetContainerQueryIterator<T>(
        QueryDefinition queryDefinition, string continuationToken = null,
        QueryRequestOptions requestOptions = null)
    {
        return GetContainerQueryIterator<T>(queryDefinition?.QueryText, continuationToken, requestOptions);
    }

    // ── Read / Delete ───────────────────────────────────────────────────────

    public override Task<DatabaseResponse> ReadAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        if (_client != null && !_client.IsDatabaseExplicitlyCreated(Id))
        {
            throw InMemoryCosmosException.Create($"Database '{Id}' not found.", HttpStatusCode.NotFound, 1003, Guid.NewGuid().ToString(), 0);
        }
        var response = Substitute.For<DatabaseResponse>();
        response.Database.Returns(this);
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(new DatabaseProperties(Id));
        return Task.FromResult(response);
    }

    public override Task<ResponseMessage> ReadStreamAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(CreateStreamResponse(HttpStatusCode.OK));
    }

    public override Task<DatabaseResponse> DeleteAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        foreach (var container in _containers.Values)
            container.DeleteContainerAsync().GetAwaiter().GetResult();
        _containers.Clear();
        _explicitlyCreatedContainers.Clear();
        _users.Clear();
        _client?.RemoveDatabase(Id);
        var response = Substitute.For<DatabaseResponse>();
        response.StatusCode.Returns(HttpStatusCode.NoContent);
        return Task.FromResult(response);
    }

    public override Task<ResponseMessage> DeleteStreamAsync(
        RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        foreach (var container in _containers.Values)
            container.DeleteContainerAsync().GetAwaiter().GetResult();
        _containers.Clear();
        _explicitlyCreatedContainers.Clear();
        _users.Clear();
        _client?.RemoveDatabase(Id);
        return Task.FromResult(CreateStreamResponse(HttpStatusCode.NoContent));
    }

    private static ResponseMessage CreateStreamResponse(HttpStatusCode statusCode)
    {
        var msg = new ResponseMessage(statusCode);
        msg.Headers["x-ms-activity-id"] = Guid.NewGuid().ToString();
        msg.Headers["x-ms-request-charge"] = "1";
        msg.Headers["x-ms-session-token"] = "0:0#0";
        return msg;
    }

    // ── Response builder (reuses NSubstitute pattern from BuildDatabaseResponse) ─

    private static ContainerResponse BuildContainerResponse(Container container, string partitionKeyPath, HttpStatusCode statusCode)
    {
        var response = Substitute.For<ContainerResponse>();
        response.Container.Returns(container);
        response.StatusCode.Returns(statusCode);
        response.Resource.Returns(new ContainerProperties(container.Id, partitionKeyPath ?? "/id"));
        return response;
    }

    private static ContainerResponse BuildContainerResponse(Container container, ContainerProperties properties, HttpStatusCode statusCode)
    {
        var response = Substitute.For<ContainerResponse>();
        response.Container.Returns(container);
        response.StatusCode.Returns(statusCode);
        response.Resource.Returns(properties);
        return response;
    }

    // ── Throughput (not meaningful for in-memory, but returns sensible defaults) ─

    public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<int?>(_throughput);

    public override Task<ThroughputResponse> ReadThroughputAsync(RequestOptions requestOptions, CancellationToken cancellationToken = default)
    {
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(ThroughputProperties.CreateManualThroughput(_throughput));
        return Task.FromResult(response);
    }

    public override Task<ThroughputResponse> ReplaceThroughputAsync(int throughput, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        _throughput = throughput;
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(ThroughputProperties.CreateManualThroughput(throughput));
        return Task.FromResult(response);
    }

    public override Task<ThroughputResponse> ReplaceThroughputAsync(ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        if (throughputProperties?.Throughput.HasValue == true)
            _throughput = throughputProperties.Throughput.Value;
        var response = Substitute.For<ThroughputResponse>();
        response.StatusCode.Returns(HttpStatusCode.OK);
        response.Resource.Returns(throughputProperties);
        return Task.FromResult(response);
    }

    // ── Stream query iterators ──────────────────────────────────────────────

    public override FeedIterator GetContainerQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => GetContainerQueryStreamIterator((string)null, continuationToken, requestOptions);

    public override FeedIterator GetContainerQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
    {
        return new InMemoryStreamFeedIterator(
            () => _containers.Values
                .Select(c => (object)new { id = c.Id })
                .ToList(),
            "DocumentCollections");
    }

    // ── DefineContainer (fluent builder) ────────────────────────────────────

    public override ContainerBuilder DefineContainer(string name, string partitionKeyPath)
        => new ContainerBuilder(this, name, partitionKeyPath);

    // ── User management (stub store — no authorization enforced) ───────────

    public override User GetUser(string id)
    {
        if (_users.TryGetValue(id, out var existing))
            return existing;

        // Return a proxy that is NOT registered in _users.
        // ReadAsync will check _users and throw 404 if not explicitly created.
        return new InMemoryUser(id, () => _users.TryRemove(id, out _), _users);
    }

    public override Task<UserResponse> CreateUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var user = new InMemoryUser(id, () => _users.TryRemove(id, out _));
        if (!_users.TryAdd(id, user))
            throw InMemoryCosmosException.Create($"User '{id}' already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);

        var response = Substitute.For<UserResponse>();
        response.StatusCode.Returns(HttpStatusCode.Created);
        response.Resource.Returns(new UserProperties(id));
        response.User.Returns(user);
        return Task.FromResult(response);
    }

    public override Task<UserResponse> UpsertUserAsync(string id, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
    {
        var created = false;
        var user = _users.GetOrAdd(id, uid => { created = true; return new InMemoryUser(uid, () => _users.TryRemove(uid, out _)); });

        var response = Substitute.For<UserResponse>();
        response.StatusCode.Returns(created ? HttpStatusCode.Created : HttpStatusCode.OK);
        response.Resource.Returns(new UserProperties(id));
        response.User.Returns(user);
        return Task.FromResult(response);
    }

    public override FeedIterator<T> GetUserQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
    {
        return new InMemoryFeedIterator<T>(
            () => _users.Values
                .Select(u => (T)(object)new UserProperties(u.Id))
                .ToList());
    }

    public override FeedIterator<T> GetUserQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
    {
        return GetUserQueryIterator<T>((string)null, continuationToken, requestOptions);
    }

    // ── Not implemented overrides (encryption) ──────────────────────────────
    public override ClientEncryptionKey GetClientEncryptionKey(string id) => throw new System.NotImplementedException();
    public override FeedIterator<ClientEncryptionKeyProperties> GetClientEncryptionKeyQueryIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
        => throw new System.NotImplementedException();
    public override Task<ClientEncryptionKeyResponse> CreateClientEncryptionKeyAsync(ClientEncryptionKeyProperties clientEncryptionKeyProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        => throw new System.NotImplementedException();
}
