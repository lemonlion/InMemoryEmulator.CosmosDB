using System.Net;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Per-test-class fixture that creates real containers on the emulator shared
/// via <see cref="EmulatorSession"/>. Containers are cached by name in the
/// session for reuse across all tests in a class (and across classes that
/// share the same container name). This avoids per-test container create/delete
/// churn that exhausts the emulator's finite partition pool (PARTITION_COUNT).
/// Containers are deleted once at the end of the test run in
/// <see cref="EmulatorSession.DisposeAsync"/>.
/// </summary>
public sealed class EmulatorTestFixture : ITestContainerFixture
{
    private readonly EmulatorSession _session;

    public TestTarget Target => _session.Target;
    public bool IsEmulator => true;

    public EmulatorTestFixture(EmulatorSession session)
    {
        _session = session;
        if (!session.IsEmulator || session.EmulatorClient is null || session.EmulatorDatabase is null)
            throw new InvalidOperationException(
                $"EmulatorTestFixture requires an initialised emulator session. Target={session.Target}");
    }

    public Task<Container> CreateContainerAsync(
        string containerName,
        string partitionKeyPath,
        Action<ContainerProperties>? configure = null)
        => CreateContainerCoreAsync(containerName, partitionKeyPath, name => new ContainerProperties(name, partitionKeyPath), configure);

    public Task<Container> CreateContainerAsync(
        string containerName,
        IReadOnlyList<string> partitionKeyPaths,
        Action<ContainerProperties>? configure = null)
        => CreateContainerCoreAsync(containerName, partitionKeyPaths[0], name => new ContainerProperties(name, partitionKeyPaths), configure);

    private async Task<Container> CreateContainerCoreAsync(
        string containerName, string partitionKeyPath,
        Func<string, ContainerProperties> propsFactory, Action<ContainerProperties>? configure)
    {
        // Reuse cached container if it already exists for this name.
        // Clean any leftover documents from previous tests to maintain isolation.
        if (_session.ContainerCache.TryGetValue(containerName, out var existing))
        {
            await CleanContainerAsync(existing, partitionKeyPath);
            return existing;
        }

        var props = propsFactory(containerName);
        configure?.Invoke(props);

        // Partition services can return 503 when the emulator is still starting
        // up a new container, and a freshly-created container's first read can
        // return 404 / 1013 ("Collection is not yet available for read") until
        // partition routing settles. Both are tagged transient in EmulatorRetry,
        // so we probe both planes inside one retry — only return a container
        // that is genuinely usable for tests.
        var container = await EmulatorRetry.RunAsync(
            async () =>
            {
                var resp = await _session.EmulatorDatabase!.CreateContainerIfNotExistsAsync(props);
                var probeId = $"__warmup__{Guid.NewGuid():N}";
                await resp.Container.UpsertItemAsync(
                    new { id = probeId, __pk = probeId }, new PartitionKey(probeId));
                try { await resp.Container.DeleteItemAsync<object>(probeId, new PartitionKey(probeId)); }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound) { }
                return resp.Container;
            },
            $"CreateContainer({props.Id})", maxRetries: 30, maxBackoffSeconds: 15);

        _session.ContainerCache.TryAdd(containerName, container);
        return container;
    }

    /// <summary>
    /// Removes all documents from a cached container to maintain per-test isolation.
    /// Faster than dropping and recreating the container (which exhausts the partition pool).
    /// </summary>
    private static async Task CleanContainerAsync(Container container, string partitionKeyPath)
    {
        var pkProp = partitionKeyPath.TrimStart('/');
        // Use a small page size to avoid oversized responses
        var query = container.GetItemQueryIterator<JObject>(
            $"SELECT c.id, c[\"{pkProp}\"] AS __pk FROM c",
            requestOptions: new QueryRequestOptions { MaxItemCount = 100 });

        while (query.HasMoreResults)
        {
            var page = await query.ReadNextAsync();
            foreach (var item in page)
            {
                var id = item["id"]?.ToString();
                var pkValue = item["__pk"];
                if (id is null) continue;

                var pk = pkValue?.Type switch
                {
                    JTokenType.String => new PartitionKey(pkValue.ToString()),
                    JTokenType.Integer => new PartitionKey(pkValue.Value<long>()),
                    JTokenType.Float => new PartitionKey(pkValue.Value<double>()),
                    JTokenType.Boolean => new PartitionKey(pkValue.Value<bool>()),
                    JTokenType.Null or null => PartitionKey.None,
                    _ => new PartitionKey(pkValue.ToString())
                };

                try
                {
                    await container.DeleteItemAsync<object>(id, pk);
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // Item may have been deleted by TTL or another concurrent operation
                }
            }
        }
    }

    // No per-test container deletion needed: containers are cached and deleted
    // at the end of the test run by EmulatorSession.
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
