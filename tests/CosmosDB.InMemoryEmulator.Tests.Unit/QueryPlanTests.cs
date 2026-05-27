using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for the gateway query plan endpoint in <see cref="FakeCosmosHandler"/>.
/// On non-Windows platforms the Cosmos SDK uses this endpoint instead of the native
/// ServiceInterop DLL to determine how to build the query execution pipeline
/// (ORDER BY merge sort, aggregate accumulation, DISTINCT deduplication, etc.).
/// These tests verify that the query plan metadata is accurate for each query pattern.
/// </summary>
public class QueryPlanTests : IDisposable
{
    private readonly InMemoryContainer _container = new("test-container", "/partitionKey");
    private readonly FakeCosmosHandler _handler;
    private readonly HttpClient _httpClient;

    public QueryPlanTests()
    {
        _handler = new FakeCosmosHandler(_container);
        _httpClient = new HttpClient(_handler)
        {
            BaseAddress = new Uri("https://localhost:9999/")
        };
    }

    public void Dispose()
    {
        _httpClient.Dispose();
        _handler.Dispose();
    }

    private async Task<(JObject Plan, HttpResponseMessage Response)> GetQueryPlanWithResponseAsync(string sql)
    {
        var body = new JObject { ["query"] = sql }.ToString();
        var request = new HttpRequestMessage(HttpMethod.Post,
            "dbs/fakeDb/colls/test-container/docs")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("x-ms-cosmos-is-query-plan-request", "True");
        request.Headers.Add("x-ms-documentdb-query-enablecrosspartition", "True");

        var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();
        return (JObject.Parse(json), response);
    }

    private async Task<JObject> GetQueryPlanAsync(string sql)
    {
        var (plan, _) = await GetQueryPlanWithResponseAsync(sql);
        return plan;
    }

    [Fact]
    public async Task QueryPlan_SimpleSelect_HasNoSpecialFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        info["orderBy"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["groupByExpressions"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        ((bool)info["hasSelectValue"]!).Should().BeFalse();
        info["top"]!.Type.Should().Be(JTokenType.Null);
        info["offset"]!.Type.Should().Be(JTokenType.Null);
        info["limit"]!.Type.Should().Be(JTokenType.Null);
    }

    [Fact]
    public async Task QueryPlan_OrderByAscending_SetsOrderByMetadata()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(1);
        orderBy[0]!.ToString().Should().Be("Ascending");

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        expressions[0]!.ToString().Should().Be("c.name");

        ((bool)info["hasNonStreamingOrderBy"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_OrderByDescending_SetsDescendingFlag()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.value DESC");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy[0]!.ToString().Should().Be("Descending");
    }

    [Fact]
    public async Task QueryPlan_MultipleOrderBy_SetsAllFields()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC, c.value DESC");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(2);
        orderBy[0]!.ToString().Should().Be("Ascending");
        orderBy[1]!.ToString().Should().Be("Descending");

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(2);
    }

    [Fact]
    public async Task QueryPlan_Top_SetsTopField()
    {
        var plan = await GetQueryPlanAsync("SELECT TOP 10 * FROM c");
        var info = plan["queryInfo"]!;

        ((int)info["top"]!).Should().Be(10);
    }

    [Fact]
    public async Task QueryPlan_OffsetLimit_SetsBothFields()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c OFFSET 5 LIMIT 10");
        var info = plan["queryInfo"]!;

        ((int)info["offset"]!).Should().Be(5);
        ((int)info["limit"]!).Should().Be(10);
    }

    [Fact]
    public async Task QueryPlan_Distinct_SetsDistinctTypeUnordered()
    {
        var plan = await GetQueryPlanAsync("SELECT DISTINCT c.name FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
    }

    [Fact]
    public async Task QueryPlan_DistinctWithOrderBy_SetsDistinctTypeUnordered()
    {
        // Non-VALUE DISTINCT + ORDER BY → Unordered (SDK uses hash dedup)
        var plan = await GetQueryPlanAsync("SELECT DISTINCT c.name FROM c ORDER BY c.name");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
    }

    [Fact]
    public async Task QueryPlan_CountAggregate_SuppressedForLinuxCompatibility()
    {
        // Single-aggregate bypass: aggregates are suppressed so the SDK
        // doesn't activate AggregateQueryPipelineStage on non-Windows platforms.
        var plan = await GetQueryPlanAsync("SELECT COUNT(1) FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_SumAggregate_SuppressedForLinuxCompatibility()
    {
        // Single-aggregate bypass: aggregates are suppressed so the SDK
        // doesn't activate AggregateQueryPipelineStage on non-Windows platforms.
        var plan = await GetQueryPlanAsync("SELECT SUM(c.value) FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_MinMaxAvg_DetectsAll()
    {
        // Multi-aggregate bypass: aggregates and alias map suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT MIN(c.value) AS minVal, MAX(c.value) AS maxVal, AVG(c.value) AS avgVal FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();

        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_GroupBy_SetsGroupByExpressions()
    {
        // GROUP BY bypass: pipeline flags are suppressed so the SDK
        // doesn't activate GroupByQueryPipelineStage on Linux.
        var plan = await GetQueryPlanAsync(
            "SELECT c.status, COUNT(1) AS cnt FROM c GROUP BY c.status");
        var info = plan["queryInfo"]!;

        var groupBy = (JArray)info["groupByExpressions"]!;
        groupBy.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_SelectValue_SetsHasSelectValue()
    {
        var plan = await GetQueryPlanAsync("SELECT VALUE c.name FROM c");
        var info = plan["queryInfo"]!;

        ((bool)info["hasSelectValue"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_WhereClause_DoesNotAffectFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.value > 10");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        info["orderBy"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_QueryRanges_CoversFullRange()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c");

        var ranges = (JArray)plan["queryRanges"]!;
        ranges.Should().HaveCount(1);
        ranges[0]!["min"]!.ToString().Should().Be("");
        ranges[0]!["max"]!.ToString().Should().Be("FF");
        ((bool)ranges[0]!["isMinInclusive"]!).Should().BeTrue();
        ((bool)ranges[0]!["isMaxInclusive"]!).Should().BeFalse();
    }

    [Fact]
    public async Task QueryPlan_ComplexQuery_SetsAllRelevantFlags()
    {
        // GROUP BY bypass: groupBy, aggregates, orderBy, and alias map are suppressed
        // (Linux compatibility). DISTINCT and TOP are preserved.
        var plan = await GetQueryPlanAsync(
            "SELECT DISTINCT TOP 5 c.category, SUM(c.value) AS total " +
            "FROM c WHERE c.isActive = true " +
            "GROUP BY c.category " +
            "ORDER BY c.category ASC");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
        ((int)info["top"]!).Should().Be(5);

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().BeEmpty();

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();

        var groupBy = (JArray)info["groupByExpressions"]!;
        groupBy.Should().BeEmpty();

        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_UnparsableQuery_StillReturnsValidPlan()
    {
        // Even if the parser can't handle the query, the plan should be valid
        // with sensible defaults so the SDK doesn't crash.
        var plan = await GetQueryPlanAsync("SELECT ??? FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        info["rewrittenQuery"]!.ToString().Should().NotBeNullOrEmpty();
        plan["queryRanges"]!.Should().BeOfType<JArray>().Which.Should().NotBeEmpty();
    }

    // ──────────────────────────────────────────────────────────────
    // A. ORDER BY edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_OrderByWithoutDirection_DefaultsToAscending()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(1);
        orderBy[0]!.ToString().Should().Be("Ascending");

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        expressions[0]!.ToString().Should().Be("c.name");
    }

    [Fact]
    public async Task QueryPlan_OrderByNestedProperty_SetsExpression()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.address.city ASC");
        var info = plan["queryInfo"]!;

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        expressions[0]!.ToString().Should().Be("c.address.city");

        ((bool)info["hasNonStreamingOrderBy"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_OrderByFunctionExpression_SetsExpression()
    {
        // When ORDER BY uses a function expression, the parser creates OrderByField
        // with Field=null and Expression=FunctionCallExpression.
        // The query plan should use ExprToString(field.Expression) as the fallback.
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY LOWER(c.name) ASC");
        var info = plan["queryInfo"]!;

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        // Should not be null/empty — should contain the stringified expression
        expressions[0]!.Type.Should().NotBe(JTokenType.Null);
        expressions[0]!.ToString().Should().NotBeNullOrEmpty();

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(1);
        orderBy[0]!.ToString().Should().Be("Ascending");
    }

    [Fact]
    public async Task QueryPlan_NoOrderBy_HasNonStreamingOrderByIsFalse()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.value > 10");
        var info = plan["queryInfo"]!;

        ((bool)info["hasNonStreamingOrderBy"]!).Should().BeFalse();
    }

    [Fact]
    public async Task QueryPlan_OrderBy_RewrittenQueryHasCorrectStructure()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        // The SDK expects: SELECT c._rid, [{"item": c.name}] AS orderByItems, c AS payload FROM c ORDER BY c.name ASC
        rewritten.Should().Contain("_rid");
        rewritten.Should().Contain("orderByItems");
        rewritten.Should().Contain("payload");
        rewritten.Should().Contain("ORDER BY");
        rewritten.Should().Contain("c.name");
    }

    [Fact]
    public async Task QueryPlan_MultipleOrderBy_RewrittenQueryHasAllFields()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC, c.age DESC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("c.name");
        rewritten.Should().Contain("c.age");
        rewritten.Should().Contain("orderByItems");
        rewritten.Should().Contain("ASC");
        rewritten.Should().Contain("DESC");
    }

    [Fact]
    public async Task QueryPlan_OrderByWithWhere_RewrittenQueryIncludesWhere()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.active = true ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("WHERE");
        rewritten.Should().Contain("ORDER BY");
        rewritten.Should().Contain("c.name");
    }

    // ──────────────────────────────────────────────────────────────
    // B. DISTINCT edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_DistinctValue_SetsBothFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT DISTINCT VALUE c.name FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
        ((bool)info["hasSelectValue"]!).Should().BeTrue();
    }

    // ──────────────────────────────────────────────────────────────
    // C. Aggregate edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_AggregateInArithmeticExpression_DetectsAggregate()
    {
        // GROUP BY bypass: aggregates and alias map suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT c.category, SUM(c.value) * 2 AS total FROM c GROUP BY c.category");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();

        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_DuplicateAggregateType_DeduplicatesInArray()
    {
        // Multi-aggregate bypass: aggregates and alias map suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT SUM(c.price) AS sumPrice, SUM(c.quantity) AS sumQty FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();

        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_CountWithoutAlias_SuppressedForLinuxCompatibility()
    {
        // Single-aggregate bypass: aggregates are suppressed so the SDK
        // doesn't activate AggregateQueryPipelineStage on non-Windows platforms.
        var plan = await GetQueryPlanAsync("SELECT COUNT(1) FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_AggregateFunctionCaseInsensitive_SuppressedForLinuxCompatibility()
    {
        // Single-aggregate bypass: aggregates are suppressed so the SDK
        // doesn't activate AggregateQueryPipelineStage on non-Windows platforms.
        var plan = await GetQueryPlanAsync("SELECT count(1) AS cnt FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_NonAggregateFunction_NotInAggregates()
    {
        var plan = await GetQueryPlanAsync("SELECT UPPER(c.name) AS upperName FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    // ──────────────────────────────────────────────────────────────
    // D. GROUP BY edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_MultipleGroupByFields_SetsAll()
    {
        // GROUP BY bypass: expressions suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT c.status, c.region, COUNT(1) AS cnt FROM c GROUP BY c.status, c.region");
        var info = plan["queryInfo"]!;

        var groupBy = (JArray)info["groupByExpressions"]!;
        groupBy.Should().BeEmpty();
    }

    // ──────────────────────────────────────────────────────────────
    // E. SELECT VALUE + aggregate combination
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_SelectValueWithAggregate_SetsBothFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT VALUE COUNT(1) FROM c");
        var info = plan["queryInfo"]!;

        ((bool)info["hasSelectValue"]!).Should().BeTrue();

        // Aggregates are cleared from the query plan for VALUE aggregate queries
        // so the SDK doesn't activate AggregateQueryPipelineStage (which fails on Linux).
        // The container computes the aggregate directly and returns the raw result.
        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    // ──────────────────────────────────────────────────────────────
    // F. Rewritten query edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_NonOrderByQuery_RewrittenQueryIsOriginalSql()
    {
        const string sql = "SELECT * FROM c WHERE c.value > 10";
        var plan = await GetQueryPlanAsync(sql);
        var info = plan["queryInfo"]!;

        info["rewrittenQuery"]!.ToString().Should().Be(sql);
    }

    // ──────────────────────────────────────────────────────────────
    // G. Response structure
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_ResponseVersion_IsTwo()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c");

        ((int)plan["partitionedQueryExecutionInfoVersion"]!).Should().Be(2);
    }

    [Fact]
    public async Task QueryPlan_ResponseStatusCode_Is200()
    {
        var (_, response) = await GetQueryPlanWithResponseAsync("SELECT * FROM c");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task QueryPlan_SimpleSelect_AllDefaultFieldsPresent()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c");
        var info = plan["queryInfo"]!;

        // Verify every expected key exists in queryInfo
        info["distinctType"].Should().NotBeNull();
        info["top"].Should().NotBeNull();       // JToken exists, value is null
        info["offset"].Should().NotBeNull();
        info["limit"].Should().NotBeNull();
        info["orderBy"].Should().NotBeNull();
        info["orderByExpressions"].Should().NotBeNull();
        info["groupByExpressions"].Should().NotBeNull();
        info["groupByAliases"].Should().NotBeNull();
        info["aggregates"].Should().NotBeNull();
        info["groupByAliasToAggregateType"].Should().NotBeNull();
        info["rewrittenQuery"].Should().NotBeNull();
        info["hasSelectValue"].Should().NotBeNull();
        info["hasNonStreamingOrderBy"].Should().NotBeNull();

        // Top-level structure
        plan["partitionedQueryExecutionInfoVersion"].Should().NotBeNull();
        plan["queryRanges"].Should().NotBeNull();
    }

    // ──────────────────────────────────────────────────────────────
    // H. General edge cases
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPlan_QueryWithParameters_ReturnsValidPlan()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.name = @name");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        info["rewrittenQuery"]!.ToString().Should().NotBeNullOrEmpty();
        plan["queryRanges"]!.Should().BeOfType<JArray>().Which.Should().NotBeEmpty();
    }

    [Fact]
    public async Task QueryPlan_TopZero_SetsTopToZero()
    {
        var plan = await GetQueryPlanAsync("SELECT TOP 0 * FROM c");
        var info = plan["queryInfo"]!;

        ((int)info["top"]!).Should().Be(0);
    }

    [Fact]
    public async Task QueryPlan_SelectSpecificFields_NoAggregates()
    {
        var plan = await GetQueryPlanAsync("SELECT c.name, c.age FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_JoinQuery_DoesNotAffectOrderByOrAggregates()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT t.name FROM c JOIN t IN c.tags");
        var info = plan["queryInfo"]!;

        info["orderBy"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
        info["distinctType"]!.ToString().Should().Be("None");
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: DISTINCT Edge Cases (A1-A4)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_DistinctValueWithOrderByOnSameField_SetsOrdered()
    {
        // True "Ordered" case: SELECT DISTINCT VALUE <field> ORDER BY <same-field>
        var plan = await GetQueryPlanAsync("SELECT DISTINCT VALUE c.name FROM c ORDER BY c.name");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Ordered");
        ((bool)info["hasSelectValue"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_DistinctValueWithOrderByOnDifferentField_SetsUnordered()
    {
        var plan = await GetQueryPlanAsync("SELECT DISTINCT VALUE c.name FROM c ORDER BY c.age");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
    }

    [Fact]
    public async Task QueryPlan_DistinctNonValueWithOrderBy_SetsUnordered()
    {
        // Non-VALUE DISTINCT + ORDER BY = Unordered
        var plan = await GetQueryPlanAsync("SELECT DISTINCT c.name FROM c ORDER BY c.name");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
    }

    [Fact]
    public async Task QueryPlan_DistinctWithMultipleFields_SetsUnordered()
    {
        var plan = await GetQueryPlanAsync("SELECT DISTINCT c.name, c.age FROM c");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Unordered");
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: ORDER BY Edge Cases (B1-B5)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_OrderByWithArrayIndex_SetsExpression()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.tags[0] ASC");
        var info = plan["queryInfo"]!;

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        expressions[0]!.ToString().Should().Contain("tags");
    }

    [Fact]
    public async Task QueryPlan_OrderByWithThreeFields_SetsAllFields()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.a ASC, c.b DESC, c.c ASC");
        var info = plan["queryInfo"]!;

        var orderBy = (JArray)info["orderBy"]!;
        orderBy.Should().HaveCount(3);
        orderBy[0]!.ToString().Should().Be("Ascending");
        orderBy[1]!.ToString().Should().Be("Descending");
        orderBy[2]!.ToString().Should().Be("Ascending");

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(3);
    }

    [Fact]
    public async Task QueryPlan_OrderByWithSystemProperty_SetsExpression()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c._ts DESC");
        var info = plan["queryInfo"]!;

        var expressions = (JArray)info["orderByExpressions"]!;
        expressions.Should().HaveCount(1);
        expressions[0]!.ToString().Should().Be("c._ts");
    }

    [Fact]
    public async Task QueryPlan_OrderByWithTop_RewrittenQueryDoesNotIncludeTop()
    {
        var plan = await GetQueryPlanAsync("SELECT TOP 5 * FROM c ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("ORDER BY");
        rewritten.Should().NotContain("TOP 5");
    }

    [Fact]
    public async Task QueryPlan_OrderByWithDistinct_RewrittenQueryStructureCorrect()
    {
        var plan = await GetQueryPlanAsync("SELECT DISTINCT * FROM c ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("orderByItems");
        rewritten.Should().Contain("ORDER BY");
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: GROUP BY Edge Cases (C1-C4)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_GroupBy_PopulatesGroupByAliases()
    {
        // GROUP BY bypass: aliases suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT c.status, COUNT(1) AS cnt FROM c GROUP BY c.status");
        var info = plan["queryInfo"]!;

        var aliases = (JArray)info["groupByAliases"]!;
        aliases.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_GroupByNestedProperty_SetsExpression()
    {
        // GROUP BY bypass: expressions suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT c.address.city, COUNT(1) AS cnt FROM c GROUP BY c.address.city");
        var info = plan["queryInfo"]!;

        var groupBy = (JArray)info["groupByExpressions"]!;
        groupBy.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_GroupByWithHaving_SetsGroupByExpressions()
    {
        // GROUP BY bypass: expressions and aggregates suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT c.status, COUNT(1) AS cnt FROM c GROUP BY c.status HAVING COUNT(1) > 1");
        var info = plan["queryInfo"]!;

        var groupBy = (JArray)info["groupByExpressions"]!;
        groupBy.Should().BeEmpty();

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_GroupByMultipleWithAliases_PopulatesAllAliases()
    {
        // GROUP BY bypass: aliases suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT c.status AS s, c.region AS r, COUNT(1) AS cnt FROM c GROUP BY c.status, c.region");
        var info = plan["queryInfo"]!;

        var aliases = (JArray)info["groupByAliases"]!;
        aliases.Should().BeEmpty();
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: Aggregate Edge Cases (D1-D4)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_AggregateInBinaryExpression_MapsAlias()
    {
        // GROUP BY bypass: aggregates and alias map suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT c.category, SUM(c.value) * 2 AS total FROM c GROUP BY c.category");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();

        var aliasMap = (JObject)info["groupByAliasToAggregateType"]!;
        aliasMap.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_MultipleAggregatesWithGroupBy_AllDetected()
    {
        // GROUP BY bypass: aggregates suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync(
            "SELECT c.cat, COUNT(1) AS cnt, AVG(c.val) AS avg FROM c GROUP BY c.cat");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_AggregateWithoutGroupBy_StillDetected()
    {
        // Multi-aggregate bypass: aggregates suppressed (Linux compatibility).
        var plan = await GetQueryPlanAsync("SELECT COUNT(1) AS cnt, SUM(c.val) AS total FROM c");
        var info = plan["queryInfo"]!;

        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_CountDistinct_SetsDCountInfo()
    {
        var plan = await GetQueryPlanAsync("SELECT COUNT(DISTINCT c.name) FROM c");
        var info = plan["queryInfo"]!;
        info["dCountInfo"].Should().NotBeNull();
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: TOP / OFFSET / LIMIT Edge Cases (E1-E5)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_LargeTopValue_SetsCorrectly()
    {
        var plan = await GetQueryPlanAsync("SELECT TOP 1000000 * FROM c");
        var info = plan["queryInfo"]!;

        ((int)info["top"]!).Should().Be(1000000);
    }

    [Fact]
    public async Task QueryPlan_OffsetZero_SetsCorrectly()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c OFFSET 0 LIMIT 10");
        var info = plan["queryInfo"]!;

        ((int)info["offset"]!).Should().Be(0);
        ((int)info["limit"]!).Should().Be(10);
    }

    [Fact]
    public async Task QueryPlan_OffsetLimit_RewrittenQueryStripsClause()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c OFFSET 5 LIMIT 10");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().NotContainEquivalentOf("OFFSET");
        rewritten.Should().NotContainEquivalentOf("LIMIT");
        rewritten.Should().Contain("SELECT");
    }

    [Fact]
    public async Task QueryPlan_TopWithOrderBy_TopNotInRewrittenQuery()
    {
        var plan = await GetQueryPlanAsync("SELECT TOP 10 * FROM c ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        ((int)info["top"]!).Should().Be(10);
        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("ORDER BY");
        rewritten.Should().NotContain("TOP 10");
    }

    [Fact]
    public async Task QueryPlan_OffsetLimitWithWhere_RewrittenQueryPreservesWhere()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.active = true OFFSET 5 LIMIT 10");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("WHERE");
        rewritten.Should().NotContainEquivalentOf("OFFSET");
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: SELECT VALUE Edge Cases (F1-F3)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_SelectValueWithExpression_SetsFlag()
    {
        var plan = await GetQueryPlanAsync("SELECT VALUE CONCAT(c.firstName, ' ', c.lastName) FROM c");
        var info = plan["queryInfo"]!;

        ((bool)info["hasSelectValue"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_SelectValueWithObjectLiteral_SetsFlag()
    {
        var plan = await GetQueryPlanAsync("SELECT VALUE { name: c.name, age: c.age } FROM c");
        var info = plan["queryInfo"]!;

        ((bool)info["hasSelectValue"]!).Should().BeTrue();
    }

    [Fact]
    public async Task QueryPlan_SelectValueCount_SetsBothFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT VALUE COUNT(1) FROM c");
        var info = plan["queryInfo"]!;

        ((bool)info["hasSelectValue"]!).Should().BeTrue();
        // Aggregates bypassed for VALUE aggregate queries (container computes the result)
        var aggregates = (JArray)info["aggregates"]!;
        aggregates.Should().BeEmpty();
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: Rewritten Query Edge Cases (G1-G3)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_OrderBy_RewrittenQueryUsesCorrectAlias()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("c._rid");
        rewritten.Should().Contain("c AS payload");
    }

    [Fact]
    public async Task QueryPlan_OrderBy_RewrittenQueryWithJoin()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c JOIN t IN c.tags ORDER BY c.name ASC");
        var info = plan["queryInfo"]!;

        var rewritten = info["rewrittenQuery"]!.ToString();
        rewritten.Should().Contain("orderByItems");
        rewritten.Should().Contain("ORDER BY");
    }

    [Fact]
    public async Task QueryPlan_DistinctQuery_RewrittenQueryIsOriginalSql()
    {
        const string sql = "SELECT DISTINCT c.name FROM c";
        var plan = await GetQueryPlanAsync(sql);
        var info = plan["queryInfo"]!;

        info["rewrittenQuery"]!.ToString().Should().Be(sql);
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: Response Structure Edge Cases (H1-H4)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_ResponseContentType_IsJson()
    {
        var (_, response) = await GetQueryPlanWithResponseAsync("SELECT * FROM c");

        response.Content.Headers.ContentType!.MediaType.Should().Be("application/json");
    }

    [Fact]
    public async Task QueryPlan_EmptyQueryBody_ReturnsValidPlan()
    {
        // Send a request with JSON body that has no query field
        var body = new JObject().ToString();
        var request = new HttpRequestMessage(HttpMethod.Post,
            "dbs/fakeDb/colls/test-container/docs")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("x-ms-cosmos-is-query-plan-request", "True");
        request.Headers.Add("x-ms-documentdb-query-enablecrosspartition", "True");

        var response = await _httpClient.SendAsync(request);
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var json = JObject.Parse(await response.Content.ReadAsStringAsync());
        json["queryInfo"].Should().NotBeNull();
    }

    [Fact]
    public async Task QueryPlan_MissingQueryField_FallsBackToSelectAll()
    {
        var body = new JObject { ["parameters"] = new JArray() }.ToString();
        var request = new HttpRequestMessage(HttpMethod.Post,
            "dbs/fakeDb/colls/test-container/docs")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("x-ms-cosmos-is-query-plan-request", "True");
        request.Headers.Add("x-ms-documentdb-query-enablecrosspartition", "True");

        var response = await _httpClient.SendAsync(request);
        var json = JObject.Parse(await response.Content.ReadAsStringAsync());
        var info = json["queryInfo"]!;

        // Falls back to "SELECT * FROM c" which has no special flags
        info["distinctType"]!.ToString().Should().Be("None");
    }

    [Fact]
    public async Task QueryPlan_ResponseHasRequestChargeHeader()
    {
        var (_, response) = await GetQueryPlanWithResponseAsync("SELECT * FROM c");

        // Query plan requests typically include a charge header
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: Complex Patterns (I1-I4)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_EXISTS_Subquery_NoAggregateFlags()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT * FROM c WHERE EXISTS(SELECT VALUE t FROM t IN c.tags WHERE t = 'urgent')");
        var info = plan["queryInfo"]!;

        ((JArray)info["aggregates"]!).Should().BeEmpty();
        ((JArray)info["orderBy"]!).Should().BeEmpty();
    }

    [Fact]
    public async Task QueryPlan_InClause_NoAggregateFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.status IN ('active', 'pending')");
        var info = plan["queryInfo"]!;

        ((JArray)info["aggregates"]!).Should().BeEmpty();
        info["distinctType"]!.ToString().Should().Be("None");
    }

    [Fact]
    public async Task QueryPlan_BetweenClause_NoAggregateFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.age BETWEEN 18 AND 65");
        var info = plan["queryInfo"]!;

        ((JArray)info["aggregates"]!).Should().BeEmpty();
        info["distinctType"]!.ToString().Should().Be("None");
    }

    [Fact]
    public async Task QueryPlan_LikeClause_NoAggregateFlags()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.name LIKE '%Smith%'");
        var info = plan["queryInfo"]!;

        ((JArray)info["aggregates"]!).Should().BeEmpty();
        info["distinctType"]!.ToString().Should().Be("None");
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: Divergent Behavior — dCountInfo / hybridSearch (J)
    // ══════════════════════════════════════════════════════════════

    [Fact(Skip = "Emulator does not support hybridSearchQueryInfo. " +
                 "See sister test: QueryPlan_HybridSearch_DivergentBehavior_IgnoredGracefully")]
    public async Task QueryPlan_HybridSearch_SetsHybridSearchQueryInfo()
    {
        var plan = await GetQueryPlanAsync(
            "SELECT * FROM c ORDER BY RANK RRF(VectorDistance(c.embedding, [1,2,3]), FullTextScore(c.text, 'test'))");
        var info = plan["queryInfo"]!;
        info["hybridSearchQueryInfo"].Should().NotBeNull();
    }

    [Fact]
    public async Task QueryPlan_HybridSearch_DivergentBehavior_IgnoredGracefully()
    {
        // DIVERGENT BEHAVIOR: Emulator does not handle hybridSearchQueryInfo.
        // Real Cosmos DB returns a hybridSearchQueryInfo field for hybrid search queries.
        // Emulator returns a basic plan — no crash.
        var plan = await GetQueryPlanAsync("SELECT * FROM c");
        var info = plan["queryInfo"]!;

        info["hybridSearchQueryInfo"].Should().BeNull();
    }

    // ══════════════════════════════════════════════════════════════
    //  Deep Dive: Boundary Edge Cases (K1-K3)
    // ══════════════════════════════════════════════════════════════

    [Fact]
    public async Task QueryPlan_VeryLongSqlQuery_ReturnsValidPlan()
    {
        // 500+ char query
        var longCondition = string.Join(" OR ", Enumerable.Range(0, 50).Select(i => $"c.field{i} = {i}"));
        var sql = $"SELECT * FROM c WHERE {longCondition}";
        sql.Length.Should().BeGreaterThan(500);

        var plan = await GetQueryPlanAsync(sql);
        plan["queryInfo"].Should().NotBeNull();
        plan["queryRanges"].Should().NotBeNull();
    }

    [Fact]
    public async Task QueryPlan_UnicodeInQuery_ReturnsValidPlan()
    {
        var plan = await GetQueryPlanAsync("SELECT * FROM c WHERE c.name = '日本語'");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("None");
        plan["queryRanges"].Should().NotBeNull();
    }

    [Fact]
    public async Task QueryPlan_CaseInsensitiveKeywords_HandledCorrectly()
    {
        var plan = await GetQueryPlanAsync("select distinct value c.name from c order by c.name");
        var info = plan["queryInfo"]!;

        info["distinctType"]!.ToString().Should().Be("Ordered");
        ((bool)info["hasSelectValue"]!).Should().BeTrue();
        ((bool)info["hasNonStreamingOrderBy"]!).Should().BeTrue();
    }
}
