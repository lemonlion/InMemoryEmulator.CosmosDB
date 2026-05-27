using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for container management, stored procedures, UDFs, triggers, and
/// computed properties through FakeCosmosHandler.
/// Features are configured via backing container, then exercised through the SDK pipeline.
/// All tests in this class use InMemoryContainer-specific APIs (BackingContainer,
/// RegisterUdf, RegisterStoredProcedure, ClearItems, CreateRouter) and cannot run
/// against the real emulator.
/// </summary>
public class FakeCosmosHandlerAdvancedFeatureTests : IDisposable
{
    private readonly InMemoryContainer _inMemoryContainer;
    private readonly FakeCosmosHandler _handler;
    private readonly CosmosClient _client;
    private readonly Container _container;

    public FakeCosmosHandlerAdvancedFeatureTests()
    {
        _inMemoryContainer = new InMemoryContainer("test-advanced", "/partitionKey");
        _handler = new FakeCosmosHandler(_inMemoryContainer);
        _client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        _container = _client.GetContainer("db", "test-advanced");
    }

    public void Dispose()
    {
        _client.Dispose();
        _handler.Dispose();
    }

    private async Task<List<T>> DrainQuery<T>(string sql)
    {
        var iterator = _container.GetItemQueryIterator<T>(sql);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    private async Task<List<T>> DrainQuery<T>(QueryDefinition queryDef)
    {
        var iterator = _container.GetItemQueryIterator<T>(queryDef);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  UDF through handler — register on backing container, query via SDK
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task UDF_RegisteredOnBackingContainer_UsableInQueryThroughHandler()
    {
        _inMemoryContainer.RegisterUdf("doubleIt", args => Convert.ToDouble(args[0]) * 2);

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 5,
                Nested = new NestedObject { Score = 7.0 } },
            new PartitionKey("pk1"));

        var results = await DrainQuery<JObject>(
            "SELECT c.name, udf.doubleIt(c.nested.score) AS doubled FROM c");

        results.Should().HaveCount(1);
        results[0]["doubled"]!.Value<double>().Should().Be(14);
    }

    [Fact]
    public async Task UDF_InWhereClause_FiltersCorrectly()
    {
        _inMemoryContainer.RegisterUdf("isLongName", args => args[0]?.ToString()?.Length > 3);

        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bo" },
            new PartitionKey("pk1"));

        var results = await DrainQuery<TestDocument>(
            "SELECT * FROM c WHERE udf.isLongName(c.name)");

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Stored Procedures — register on backing container, execute via Scripts
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StoredProc_RegisteredOnBackingContainer_ExecutableThroughHandler()
    {
        _inMemoryContainer.RegisterStoredProcedure("echo",
            (pk, args) => JsonConvert.SerializeObject(new { echo = args[0]?.ToString() }));

        var response = await _inMemoryContainer.Scripts.ExecuteStoredProcedureAsync<string>(
            "echo", new PartitionKey("pk1"), new dynamic[] { "hello" });

        var parsed = JObject.Parse(response.Resource);
        parsed["echo"]!.Value<string>().Should().Be("hello");
    }

    [Fact]
    public async Task StoredProc_CanAccessContainerData()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
            new PartitionKey("pk1"));

        _inMemoryContainer.RegisterStoredProcedure("countItems",
            (pk, args) =>
            {
                // Use query to count items in the container
                var iterator = _inMemoryContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
                var items = new List<TestDocument>();
                while (iterator.HasMoreResults)
                {
                    var page = iterator.ReadNextAsync().GetAwaiter().GetResult();
                    items.AddRange(page);
                }
                return items.Count.ToString();
            });

        var response = await _inMemoryContainer.Scripts.ExecuteStoredProcedureAsync<string>(
            "countItems", new PartitionKey("pk1"), new dynamic[] { });

        response.Resource.Should().Be("2");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Computed Properties through handler queries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ComputedProperty_AppearsInQueryResults()
    {
        var props = new ContainerProperties("test-computed", "/partitionKey");
        props.ComputedProperties.Add(new ComputedProperty
            { Name = "cp_upper", Query = "SELECT VALUE UPPER(c.name) FROM c" });
        var container = new InMemoryContainer(props);
        using var handler = new FakeCosmosHandler(container);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(handler)
            });
        var sdkContainer = client.GetContainer("db", "test-computed");

        await sdkContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "alice" },
            new PartitionKey("pk1"));

        var iterator = sdkContainer.GetItemQueryIterator<JObject>(
            "SELECT c.name, c.cp_upper FROM c");
        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0]["cp_upper"]!.Value<string>().Should().Be("ALICE");
    }

    [Fact]
    public async Task ComputedProperty_NotVisibleInPointRead()
    {
        var props = new ContainerProperties("test-computed2", "/partitionKey");
        props.ComputedProperties.Add(new ComputedProperty
            { Name = "cp_lower", Query = "SELECT VALUE LOWER(c.name) FROM c" });
        var container = new InMemoryContainer(props);
        using var handler = new FakeCosmosHandler(container);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(handler)
            });
        var sdkContainer = client.GetContainer("db", "test-computed2");

        await sdkContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        var response = await sdkContainer.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
        response.Resource.ContainsKey("cp_lower").Should().BeFalse();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Container management via backing container
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Container_ClearItems_ThenQueryThroughHandler_ReturnsEmpty()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        _inMemoryContainer.ClearItems();

        var results = await DrainQuery<TestDocument>("SELECT * FROM c");
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Container_MultiContainerRouter_IndependentData()
    {
        var container1 = new InMemoryContainer("container1", "/partitionKey");
        var container2 = new InMemoryContainer("container2", "/partitionKey");
        var handler1 = new FakeCosmosHandler(container1);
        var handler2 = new FakeCosmosHandler(container2);

        using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
        {
            ["container1"] = handler1,
            ["container2"] = handler2,
        });

        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(router)
            });

        var c1 = client.GetContainer("db", "container1");
        var c2 = client.GetContainer("db", "container2");

        await c1.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "InC1" },
            new PartitionKey("pk1"));
        await c2.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "InC2" },
            new PartitionKey("pk1"));

        var read1 = await c1.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read1.Resource.Name.Should().Be("InC1");

        var read2 = await c2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        read2.Resource.Name.Should().Be("InC2");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Unique Key Policy
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task UniqueKeyPolicy_EnforcedThroughHandler()
    {
        var props = new ContainerProperties("test-unique", "/partitionKey")
        {
            UniqueKeyPolicy = new UniqueKeyPolicy
            {
                UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
            }
        };
        var container = new InMemoryContainer(props);
        using var handler = new FakeCosmosHandler(container);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                HttpClientFactory = () => new HttpClient(handler)
            });
        var sdkContainer = client.GetContainer("db", "test-unique");

        await sdkContainer.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        // Duplicate name in same partition should fail
        var act = () => sdkContainer.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice" },
            new PartitionKey("pk1"));

        await act.Should().ThrowAsync<CosmosException>();
    }
}
