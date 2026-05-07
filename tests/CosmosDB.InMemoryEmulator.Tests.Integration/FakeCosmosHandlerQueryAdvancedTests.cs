using System.Reflection;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for advanced SQL query features through FakeCosmosHandler.
/// Covers subqueries, built-in functions, vector/FTS/geospatial queries,
/// and parameterized queries that go through the full SDK → HTTP → handler pipeline.
/// Parity-validated: string/math/array/type/aggregate/subquery/parameterized tests run against both backends.
/// Vector, FTS, and geospatial tests create inline containers and are tagged InMemoryOnly.
/// NullCoalesce and GroupByHaving tests are tagged InMemoryOnly — Windows emulator (v2.14.0) does not support ?? operator or HAVING clause (see #53).
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerQueryAdvancedTests(EmulatorSession session) : IAsyncLifetime
{
    private class QueryDoc
    {
        [JsonProperty("id")] public string Id { get; set; } = default!;
        [JsonProperty("partitionKey")] public string PartitionKey { get; set; } = default!;
        [JsonProperty("name")] public string Name { get; set; } = default!;
        [JsonProperty("score")] public int Score { get; set; }
        [JsonProperty("isActive")] public bool IsActive { get; set; } = true;
        [JsonProperty("tags")] public string[] Tags { get; set; } = [];
        [JsonProperty("nested")] public NestedObject? Nested { get; set; }
    }

    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("test-query-adv", "/partitionKey");
        await SeedData();
    }

    private async Task SeedData()
    {
        var docs = new[]
        {
            new QueryDoc { Id = "1", PartitionKey = "pk1", Name = "Alice", Score = 30, Tags = ["admin", "user"] },
            new QueryDoc { Id = "2", PartitionKey = "pk1", Name = "Bob", Score = 20, Tags = ["user"] },
            new QueryDoc { Id = "3", PartitionKey = "pk2", Name = "Charlie", Score = 50, Tags = ["admin"] },
            new QueryDoc { Id = "4", PartitionKey = "pk2", Name = "Diana", Score = 10, Tags = ["user", "moderator"] },
            new QueryDoc { Id = "5", PartitionKey = "pk1", Name = "Eve", Score = 40, Tags = [] },
        };
        foreach (var doc in docs)
            await _container.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
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
    //  String Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_StringUpper_ReturnsUppercased()
    {
        var results = await DrainQuery<JObject>(
            "SELECT UPPER(c.name) AS upper_name FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["upper_name"]!.Value<string>().Should().Be("ALICE");
    }

    [Fact]
    public async Task Query_StringLower_ReturnsLowercased()
    {
        var results = await DrainQuery<JObject>(
            "SELECT LOWER(c.name) AS lower_name FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["lower_name"]!.Value<string>().Should().Be("alice");
    }

    [Fact]
    public async Task Query_StringConcat_Works()
    {
        var results = await DrainQuery<JObject>(
            "SELECT CONCAT(c.name, '-', c.partitionKey) AS full FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["full"]!.Value<string>().Should().Be("Alice-pk1");
    }

    [Fact]
    public async Task Query_StringContains_Filters()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE CONTAINS(c.name, 'li')");

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
    }

    [Fact]
    public async Task Query_Length_ReturnsCorrectLength()
    {
        var results = await DrainQuery<JObject>(
            "SELECT LENGTH(c.name) AS len FROM c WHERE c.id = '3'");

        results.Should().HaveCount(1);
        results[0]["len"]!.Value<int>().Should().Be(7); // "Charlie"
    }

    [Fact]
    public async Task Query_Substring_ExtractsCorrectly()
    {
        var results = await DrainQuery<JObject>(
            "SELECT SUBSTRING(c.name, 0, 3) AS sub FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["sub"]!.Value<string>().Should().Be("Ali");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Math Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_MathAbs_ReturnsAbsoluteValue()
    {
        var results = await DrainQuery<JObject>(
            "SELECT ABS(c.score - 35) AS diff FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["diff"]!.Value<double>().Should().Be(5);
    }

    [Fact]
    public async Task Query_MathFloor_Works()
    {
        var results = await DrainQuery<JObject>(
            "SELECT FLOOR(c.score / 3.0) AS floored FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["floored"]!.Value<double>().Should().Be(10);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Array Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ArrayContains_Filters()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'admin')");

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
    }

    [Fact]
    public async Task Query_ArrayLength_ReturnsCount()
    {
        var results = await DrainQuery<JObject>(
            "SELECT ARRAY_LENGTH(c.tags) AS tagCount FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["tagCount"]!.Value<int>().Should().Be(2);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Type-Checking Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_IsDefined_ChecksPropertyExistence()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE IS_DEFINED(c.nonExistentProperty)");

        // No docs have this property
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Query_IsString_ChecksType()
    {
        var results = await DrainQuery<JObject>(
            "SELECT IS_STRING(c.name) AS isStr FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["isStr"]!.Value<bool>().Should().BeTrue();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Subqueries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ExistsSubquery_FiltersCorrectly()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE EXISTS (SELECT VALUE t FROM t IN c.tags WHERE t = 'admin')");

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
    }

    [Fact]
    public async Task Query_ArraySubquery_ReturnsArray()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.name, ARRAY(SELECT VALUE UPPER(t) FROM t IN c.tags) AS upperTags FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        var tags = results[0]["upperTags"]!.ToObject<string[]>();
        tags.Should().BeEquivalentTo("ADMIN", "USER");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Parameterized Queries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_Parameterized_WithStringParam()
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @name")
            .WithParameter("@name", "Charlie");

        var results = await DrainQuery<QueryDoc>(query);
        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Charlie");
    }

    [Fact]
    public async Task Query_Parameterized_WithNumericParam()
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.score > @minVal")
            .WithParameter("@minVal", 25);

        var results = await DrainQuery<QueryDoc>(query);
        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task Query_Parameterized_WithMultipleParams()
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @name AND c.partitionKey = @pk")
            .WithParameter("@name", "Alice")
            .WithParameter("@pk", "pk1");

        var results = await DrainQuery<QueryDoc>(query);
        results.Should().HaveCount(1);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Conditional Expressions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_TernaryExpression_ReturnsCorrectValue()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.name, (c.score >= 30 ? 'high' : 'low') AS level FROM c");

        results.Should().HaveCount(5);
        results.First(r => r["name"]!.Value<string>() == "Alice")["level"]!.Value<string>().Should().Be("high");
        results.First(r => r["name"]!.Value<string>() == "Bob")["level"]!.Value<string>().Should().Be("low");
    }

    // Windows emulator (v2.14.0) returns null instead of coalesced value for ?? operator.
    // Tracked: https://github.com/lemonlion/CosmosDB.InMemoryEmulator/issues/53
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task Query_NullCoalesce_Works()
    {
        await _container.CreateItemAsync(
            new QueryDoc { Id = "null1", PartitionKey = "pk1", Name = null!, Score = 0 },
            new PartitionKey("pk1"));

        var results = await DrainQuery<JObject>(
            "SELECT (c.name ?? 'Unknown') AS displayName FROM c WHERE c.id = 'null1'");

        results.Should().HaveCount(1);
        results[0]["displayName"]!.Value<string>().Should().Be("Unknown");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  IN / BETWEEN operators
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_InOperator_FiltersCorrectly()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE c.name IN ('Alice', 'Charlie', 'Eve')");

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie", "Eve");
    }

    [Fact]
    public async Task Query_BetweenOperator_FiltersRange()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE c.score BETWEEN 20 AND 40");

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Bob", "Eve");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LIKE operator
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_LikeOperator_Wildcard()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c WHERE c.name LIKE 'A%'");

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  JOIN
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_Join_FlattensTags()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.name, t AS tag FROM c JOIN t IN c.tags WHERE c.id = '1'");

        results.Should().HaveCount(2);
        results.Select(r => r["tag"]!.Value<string>()).Should().BeEquivalentTo("admin", "user");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Conversion Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ToString_ConvertsNumber()
    {
        var results = await DrainQuery<JObject>(
            "SELECT ToString(c.score) AS strVal FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        results[0]["strVal"]!.Value<string>().Should().Be("30");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Aggregate with GROUP BY
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_GroupBy_WithCount_ThroughHandler()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey");

        results.Should().HaveCount(2);
        var pk1 = results.First(r => r["partitionKey"]!.Value<string>() == "pk1");
        pk1["cnt"]!.Value<int>().Should().Be(3);
        var pk2 = results.First(r => r["partitionKey"]!.Value<string>() == "pk2");
        pk2["cnt"]!.Value<int>().Should().Be(2);
    }

    [Fact]
    public async Task Query_GroupBy_WithSum_ThroughHandler()
    {
        var results = await DrainQuery<JObject>(
            "SELECT c.partitionKey, SUM(c.score) AS total FROM c GROUP BY c.partitionKey");

        results.Should().HaveCount(2);
        var pk1 = results.First(r => r["partitionKey"]!.Value<string>() == "pk1");
        pk1["total"]!.Value<int>().Should().Be(90); // 30+20+40
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Multiple aggregates
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_MultipleAggregates_ThroughHandler()
    {
        var results = await DrainQuery<JObject>(
            "SELECT COUNT(1) AS cnt, SUM(c.score) AS total, AVG(c.score) AS average FROM c");

        results.Should().HaveCount(1);
        results[0]["cnt"]!.Value<int>().Should().Be(5);
        results[0]["total"]!.Value<int>().Should().Be(150);
        results[0]["average"]!.Value<double>().Should().Be(30);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Continuation token pagination through queries
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ContinuationToken_PaginatesCorrectly()
    {
        var results = new List<QueryDoc>();
        var iterator = _container.GetItemQueryIterator<QueryDoc>(
            "SELECT * FROM c ORDER BY c.name",
            requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

        int pageCount = 0;
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
            pageCount++;
        }

        results.Should().HaveCount(5);
        pageCount.Should().BeGreaterThan(1);
        results.Select(r => r.Name).Should().ContainInOrder("Alice", "Bob", "Charlie", "Diana", "Eve");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Date/Time Functions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_GetCurrentTimestamp_ReturnsNumber()
    {
        var results = await DrainQuery<JObject>(
            "SELECT GetCurrentTimestamp() AS ts FROM c WHERE c.id = '1'");

        results.Should().HaveCount(1);
        var ts = results[0]["ts"]!.Value<long>();
        ts.Should().BeGreaterThan(0);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  DISTINCT
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_Distinct_ReturnsUniqueValues()
    {
        var results = await DrainQuery<JObject>(
            "SELECT DISTINCT c.partitionKey FROM c");

        results.Should().HaveCount(2);
        results.Select(r => r["partitionKey"]!.Value<string>()).Should().BeEquivalentTo("pk1", "pk2");
    }

    [Fact]
    public async Task Query_DistinctValue_ReturnsScalars()
    {
        var results = await DrainQuery<JValue>(
            "SELECT DISTINCT VALUE c.partitionKey FROM c");

        results.Should().HaveCount(2);
        results.Select(r => r.Value<string>()).Should().BeEquivalentTo("pk1", "pk2");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  TOP
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_Top_LimitsResults()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT TOP 3 * FROM c ORDER BY c.name");

        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().ContainInOrder("Alice", "Bob", "Charlie");
    }

    [Fact]
    public async Task Query_Top1_ReturnsSingleResult()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT TOP 1 * FROM c ORDER BY c.score DESC");

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Charlie"); // Score = 50
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  OFFSET / LIMIT
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_OffsetLimit_PaginatesCorrectly()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c ORDER BY c.name OFFSET 1 LIMIT 2");

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().ContainInOrder("Bob", "Charlie");
    }

    [Fact]
    public async Task Query_OffsetLimit_LastPage()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c ORDER BY c.name OFFSET 4 LIMIT 10");

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Eve");
    }

    [Fact]
    public async Task Query_OffsetLimit_BeyondResults_ReturnsEmpty()
    {
        var results = await DrainQuery<QueryDoc>(
            "SELECT * FROM c ORDER BY c.name OFFSET 100 LIMIT 10");

        results.Should().BeEmpty();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  GROUP BY with HAVING
    // ═══════════════════════════════════════════════════════════════════════════

    // Windows emulator (v2.14.0) returns 400 "Syntax error near 'HAVING'" for GROUP BY HAVING queries.
    // Tracked: https://github.com/lemonlion/CosmosDB.InMemoryEmulator/issues/53
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task Query_GroupByHaving_FiltersGroups()
    {
        // HAVING is rejected by SDK ServiceInterop (SC1001), so enable distributed query gateway mode
        // to bypass ServiceInterop and send raw SQL directly through the handler pipeline
        var results = await DrainDistributedQuery<JObject>(
            "SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey HAVING COUNT(1) >= 3");

        results.Should().HaveCount(1);
        results[0]["partitionKey"]!.Value<string>().Should().Be("pk1"); // pk1 has 3 items
        results[0]["cnt"]!.Value<int>().Should().Be(3);
    }

    private async Task<List<T>> DrainDistributedQuery<T>(string sql)
    {
        return await DrainDistributedQuery<T>(_container, sql);
    }

    private static async Task<List<T>> DrainDistributedQuery<T>(Container container, string sql)
    {
        var options = new QueryRequestOptions();
        typeof(QueryRequestOptions)
            .GetProperty("EnableDistributedQueryGatewayMode", BindingFlags.Instance | BindingFlags.NonPublic)
            ?.SetValue(options, true);
        var iterator = container.GetItemQueryIterator<T>(sql, requestOptions: options);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Vector Search (VECTORDISTANCE) — InMemoryOnly (uses inline containers)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task Query_VectorDistance_Cosine_OrdersByDistance()
    {
        // VectorDistance ORDER BY goes through the full SDK → HTTP → handler pipeline
        using var cosmos = InMemoryCosmos.Create("test-vector", "/pk");
        var container = cosmos.Container;

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "v1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "v2", pk = "a", embedding = new[] { 0.0, 1.0, 0.0 } }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "v3", pk = "a", embedding = new[] { 0.9, 0.1, 0.0 } }),
            new PartitionKey("a"));

        // Query through SDK pipeline — VectorDistance in ORDER BY is now handled by FakeCosmosHandler
        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0])");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(3);
        // Cosine similarity ORDER BY ascending: v2 (orthogonal, ~0) is first, v1 (exact, 1.0) is last
        var lastResult = results[2];
        lastResult["id"]!.Value<string>().Should().Be("v1");
        lastResult["score"]!.Value<double>().Should().BeApproximately(1.0, 0.001);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task Query_VectorDistance_Euclidean_Works()
    {
        // VectorDistance without ORDER BY goes through the full SDK pipeline
        using var cosmos = InMemoryCosmos.Create("test-vec-euc", "/pk");
        var container = cosmos.Container;

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "e1", pk = "a", emb = new[] { 0.0, 0.0 } }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "e2", pk = "a", emb = new[] { 3.0, 4.0 } }),
            new PartitionKey("a"));

        // Query through SDK pipeline
        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT c.id, VectorDistance(c.emb, [0.0, 0.0], false, {'distanceFunction': 'euclidean'}) AS dist FROM c");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        var e1 = results.First(r => r["id"]!.Value<string>() == "e1");
        e1["dist"]!.Value<double>().Should().BeApproximately(0.0, 0.001);
        var e2 = results.First(r => r["id"]!.Value<string>() == "e2");
        e2["dist"]!.Value<double>().Should().BeApproximately(5.0, 0.001);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Full-Text Search — InMemoryOnly (uses inline containers)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task Query_FullTextContains_Filters()
    {
        // FullTextContains is rejected by SDK ServiceInterop (SC2005),
        // so enable distributed query gateway mode to bypass ServiceInterop
        using var cosmos = InMemoryCosmos.Create("test-fts", "/pk");
        var container = cosmos.Container;

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "f1", pk = "a", text = "Azure Cosmos DB is a NoSQL database" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "f2", pk = "a", text = "SQL Server is a relational database" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "f3", pk = "a", text = "Redis is an in-memory cache" }),
            new PartitionKey("a"));

        // Query through SDK pipeline with distributed query gateway mode
        var results = await DrainDistributedQuery<JObject>(container,
            "SELECT c.id FROM c WHERE FullTextContains(c.text, 'database')");

        results.Should().HaveCount(2);
        results.Select(r => r["id"]!.Value<string>()).Should().BeEquivalentTo("f1", "f2");
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task Query_FullTextContainsAll_RequiresAllTerms()
    {
        using var cosmos = InMemoryCosmos.Create("test-fts-all", "/pk");
        var container = cosmos.Container;

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "f1", pk = "a", text = "Azure Cosmos DB is a NoSQL database" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "f2", pk = "a", text = "SQL Server is a relational database" }),
            new PartitionKey("a"));

        // Query through SDK pipeline with distributed query gateway mode
        var results = await DrainDistributedQuery<JObject>(container,
            "SELECT c.id FROM c WHERE FullTextContainsAll(c.text, 'database', 'NoSQL')");

        results.Should().HaveCount(1);
        results[0]["id"]!.Value<string>().Should().Be("f1");
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task Query_FullTextContainsAny_MatchesAnyTerm()
    {
        using var cosmos = InMemoryCosmos.Create("test-fts-any", "/pk");
        var container = cosmos.Container;

        await container.CreateItemAsync(
            JObject.FromObject(new { id = "f1", pk = "a", text = "Azure Cosmos DB" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "f2", pk = "a", text = "Redis cache" }),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "f3", pk = "a", text = "PostgreSQL database" }),
            new PartitionKey("a"));

        // Query through SDK pipeline with distributed query gateway mode
        var results = await DrainDistributedQuery<JObject>(container,
            "SELECT c.id FROM c WHERE FullTextContainsAny(c.text, 'Cosmos', 'Redis')");

        results.Should().HaveCount(2);
        results.Select(r => r["id"]!.Value<string>()).Should().BeEquivalentTo("f1", "f2");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Geospatial (ST_* functions) — InMemoryOnly (uses inline containers)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task Query_StDistance_CalculatesDistanceBetweenPoints()
    {
        using var cosmos = InMemoryCosmos.Create("test-geo", "/pk");
        var container = cosmos.Container;

        await container.CreateItemAsync(
            JObject.Parse("""{"id":"g1","pk":"a","location":{"type":"Point","coordinates":[-122.12,47.67]}}"""),
            new PartitionKey("a"));
        await container.CreateItemAsync(
            JObject.Parse("""{"id":"g2","pk":"a","location":{"type":"Point","coordinates":[-73.97,40.77]}}"""),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            """SELECT c.id, ST_DISTANCE(c.location, {"type":"Point","coordinates":[-122.12,47.67]}) AS dist FROM c""");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(2);
        var g1 = results.First(r => r["id"]!.Value<string>() == "g1");
        g1["dist"]!.Value<double>().Should().BeLessThan(1); // Same point, ~0m
        var g2 = results.First(r => r["id"]!.Value<string>() == "g2");
        g2["dist"]!.Value<double>().Should().BeGreaterThan(3_000_000); // ~3900km
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task Query_StWithin_ChecksPointInPolygon()
    {
        using var cosmos = InMemoryCosmos.Create("test-geo-within", "/pk");
        var container = cosmos.Container;

        // Point inside polygon (Seattle area)
        await container.CreateItemAsync(
            JObject.Parse("""{"id":"in","pk":"a","loc":{"type":"Point","coordinates":[-122.3,47.6]}}"""),
            new PartitionKey("a"));
        // Point outside polygon (New York)
        await container.CreateItemAsync(
            JObject.Parse("""{"id":"out","pk":"a","loc":{"type":"Point","coordinates":[-73.97,40.77]}}"""),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            """SELECT c.id FROM c WHERE ST_WITHIN(c.loc, {"type":"Polygon","coordinates":[[[-123,47],[-123,48],[-121,48],[-121,47],[-123,47]]]})""");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0]["id"]!.Value<string>().Should().Be("in");
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task Query_StIsValid_ValidatesGeoJson()
    {
        using var cosmos = InMemoryCosmos.Create("test-geo-valid", "/pk");
        var container = cosmos.Container;

        await container.CreateItemAsync(
            JObject.Parse("""{"id":"valid","pk":"a","loc":{"type":"Point","coordinates":[-122.12,47.67]}}"""),
            new PartitionKey("a"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT c.id, ST_ISVALID(c.loc) AS isValid FROM c");

        var results = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0]["isValid"]!.Value<bool>().Should().BeTrue();
    }

    [Fact]
    public async Task Query_DollarPrefixedProperty_Works()
    {
        // Issue #35: EF Core Cosmos generates queries using c.$type as a discriminator column.
        // The parser must accept $-prefixed property names.
        var doc = JObject.FromObject(new
        {
            id = "dollar-1",
            partitionKey = "pk1",
            // $type is the discriminator column used by EF Core Cosmos
        });
        doc["$type"] = "IdentityUser";
        await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE c FROM c WHERE c[\"$type\"] = @type")
            .WithParameter("@type", "IdentityUser");

        var results = new List<JObject>();
        using var iterator = _container.GetItemQueryIterator<JObject>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0]["$type"]!.Value<string>().Should().Be("IdentityUser");
    }

    [Fact]
    public async Task Query_DollarPrefixedProperty_DotNotation_Works()
    {
        // Issue #35: Verify c.$type dot notation works (not just bracket notation)
        var doc = JObject.FromObject(new
        {
            id = "dollar-2",
            partitionKey = "pk1",
        });
        doc["$type"] = "IdentityRole";
        await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT c.id, c[\"$type\"] AS dtype FROM c WHERE c[\"$type\"] = 'IdentityRole'");

        var results = new List<JObject>();
        using var iterator = _container.GetItemQueryIterator<JObject>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0]["dtype"]!.Value<string>().Should().Be("IdentityRole");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Nested Function Calls (Issue #11)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Query_ContainsLower_NestedFunctionCall_Works()
    {
        // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/contains
        // Nested function calls like CONTAINS(LOWER(c.field), @value) should be evaluated correctly.
        var doc = new TestDocument { Id = "nf1", PartitionKey = "pk1", Name = "T1000" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(LOWER(c.name), @val)")
            .WithParameter("@val", "t1000");
        var results = new List<TestDocument>();
        using var iterator = _container.GetItemQueryIterator<TestDocument>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1, "CONTAINS(LOWER(c.name), 't1000') should match document with Name='T1000'");
        results[0].Name.Should().Be("T1000");
    }

    [Fact]
    public async Task Query_StartsWithUpper_NestedFunctionCall_Works()
    {
        var doc = new TestDocument { Id = "nf2", PartitionKey = "pk1", Name = "hello-world" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(UPPER(c.name), @val)")
            .WithParameter("@val", "HELLO");
        var results = new List<TestDocument>();
        using var iterator = _container.GetItemQueryIterator<TestDocument>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("hello-world");
    }

    // Issue #16: SELECT VALUE projecting scalar strings should be deserialized correctly
    [Fact]
    public async Task Query_SelectValueScalarString_Works()
    {
        var doc = new TestDocument { Id = "sv1", PartitionKey = "pk1", Name = "Widget" };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE c.name FROM c WHERE c.id = 'sv1'");
        var results = new List<string>();
        using var iterator = _container.GetItemQueryIterator<string>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0].Should().Be("Widget");
    }

    // Issue #16: SELECT VALUE projecting scalar int should be deserialized correctly
    [Fact]
    public async Task Query_SelectValueScalarInt_Works()
    {
        // Use a JObject to have a custom integer property (avoiding 'value' which is a Cosmos SQL reserved word)
        var doc = JObject.FromObject(new { id = "sv2", partitionKey = "pk1", amount = 42 });
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var query = new QueryDefinition("SELECT VALUE c.amount FROM c WHERE c.id = 'sv2'");
        var results = new List<int>();
        using var iterator = _container.GetItemQueryIterator<int>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }

        results.Should().HaveCount(1);
        results[0].Should().Be(42);
    }
}
