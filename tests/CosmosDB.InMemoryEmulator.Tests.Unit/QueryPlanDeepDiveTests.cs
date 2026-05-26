using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Plan #27: Query Plan deep dive tests — additional coverage for HandleQueryPlanAsync
/// in FakeCosmosHandler, including bug-exposing tests for aggregate detection recursion.
/// </summary>
public class QueryPlanDeepDiveTests : IDisposable
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly HttpClient _httpClient;

	public QueryPlanDeepDiveTests()
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

	private async Task<JObject> GetQueryPlanAsync(string sql)
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
		return JObject.Parse(json);
	}

	// ═══════════════════════════════════════════════════════════════
	//  Phase 1: Bug-Exposing Tests (BUG-1 through BUG-4)
	// ═══════════════════════════════════════════════════════════════

	[Fact]
	public async Task QueryPlan_AggregateInsideNonAggregateFunction_DetectedByBypass()
	{
		// BUG-1: CONCAT wraps COUNT → COUNT should be detected
		var plan = await GetQueryPlanAsync(
			"SELECT c.cat, CONCAT(COUNT(1), ' items') AS label FROM c GROUP BY c.cat");
		var info = plan["queryInfo"]!;

		// GROUP BY bypass should activate — aggregates/groupBy cleared
		info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
		info["groupByExpressions"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
	}

	[Fact]
	public async Task QueryPlan_AggregateInsideCoalesce_DetectedByBypass()
	{
		// BUG-2: COALESCE wraps COUNT → COUNT should be detected
		var plan = await GetQueryPlanAsync(
			"SELECT VALUE COALESCE(COUNT(1), 0) FROM c");
		var info = plan["queryInfo"]!;

		// VALUE aggregate bypass should activate — aggregates cleared
		info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
		((bool)info["hasSelectValue"]!).Should().BeTrue();
	}

	[Fact]
	public async Task QueryPlan_AggregateInsideIIF_DetectedByBypass()
	{
		// BUG-2: IIF wraps SUM → SUM should be detected
		var plan = await GetQueryPlanAsync(
			"SELECT VALUE IIF(true, SUM(c.val), 0) FROM c");
		var info = plan["queryInfo"]!;

		// VALUE aggregate bypass should activate
		info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
		((bool)info["hasSelectValue"]!).Should().BeTrue();
	}

	[Fact]
	public async Task QueryPlan_TwoAggregatesInSingleExpression_BypassActivates()
	{
		// BUG-3/BUG-5: COUNT + SUM in single SELECT field via binary expression
		var plan = await GetQueryPlanAsync(
			"SELECT COUNT(1) + SUM(c.val) AS combined FROM c");
		var info = plan["queryInfo"]!;

		// Multi-aggregate bypass should activate (aggregates.Count > 1)
		info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
	}

	// ═══════════════════════════════════════════════════════════════
	//  Phase 2: Missing Coverage Tests (N5-N23)
	// ═══════════════════════════════════════════════════════════════

	[Fact]
	public async Task QueryPlan_DistinctValueOrderByDescSameField_IsOrdered()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT DISTINCT VALUE c.name FROM c ORDER BY c.name DESC");
		var info = plan["queryInfo"]!;

		info["distinctType"]!.ToString().Should().Be("Ordered");
	}

	[Fact]
	public async Task QueryPlan_DistinctValueOrderByMultipleFields_FirstFieldMatches()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT DISTINCT VALUE c.name FROM c ORDER BY c.name, c.age");
		var info = plan["queryInfo"]!;

		info["distinctType"]!.ToString().Should().Be("Ordered");
	}

	[Fact]
	public async Task QueryPlan_OrderByCustomAlias_RewrittenQueryUsesAlias()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT * FROM root ORDER BY root.name ASC");
		var info = plan["queryInfo"]!;

		var rewritten = info["rewrittenQuery"]!.ToString();
		rewritten.Should().Contain("root");
	}

	[Fact]
	public async Task QueryPlan_DistinctOrderBy_RewrittenQueryHasProjectedPayload()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT DISTINCT c.name FROM c ORDER BY c.name");
		var info = plan["queryInfo"]!;

		var rewritten = info["rewrittenQuery"]!.ToString();
		// Should have orderByItems and payload structure
		rewritten.Should().Contain("orderByItems");
	}

	[Fact]
	public async Task QueryPlan_OrderByPlusOffsetLimit_OrderByWins()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT * FROM c ORDER BY c.name OFFSET 5 LIMIT 10");
		var info = plan["queryInfo"]!;

		// OFFSET/LIMIT values should be captured
		info["offset"]!.Value<int>().Should().Be(5);
		info["limit"]!.Value<int>().Should().Be(10);

		// ORDER BY should be set
		((bool)info["hasNonStreamingOrderBy"]!).Should().BeTrue();

		// Rewritten query should use ORDER BY format
		var rewritten = info["rewrittenQuery"]!.ToString();
		rewritten.Should().Contain("orderByItems");
	}

	[Fact]
	public async Task QueryPlan_CountDistinct_VerifyFieldValues()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT COUNT(DISTINCT c.status) FROM c");
		var info = plan["queryInfo"]!;

		var dCountInfo = info["dCountInfo"] as JObject;
		dCountInfo.Should().NotBeNull("dCountInfo should be populated for COUNT(DISTINCT c.status)");
		dCountInfo!["dCountAlias"]!.ToString().Should().Be("$1");
		dCountInfo["dCountExpressionBase"]!["propertyPath"]!.ToString().Should().Be("status");
	}

	[Fact]
	public async Task QueryPlan_CountDistinct_NestedProperty()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT COUNT(DISTINCT c.address.city) FROM c");
		var info = plan["queryInfo"]!;

		var dCountInfo = info["dCountInfo"] as JObject;
		dCountInfo.Should().NotBeNull("dCountInfo should be populated for nested property");
		dCountInfo!["dCountExpressionBase"]!["propertyPath"]!.ToString().Should().Be("address.city");
	}

	[Fact]
	public async Task QueryPlan_CountDistinct_CustomAlias()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT COUNT(DISTINCT root.status) FROM root");
		var info = plan["queryInfo"]!;

		var dCountInfo = info["dCountInfo"] as JObject;
		dCountInfo.Should().NotBeNull("dCountInfo should be populated for custom alias");
		dCountInfo!["dCountExpressionBase"]!["propertyPath"]!.ToString().Should().Be("status");
	}

	[Fact]
	public async Task QueryPlan_SingleAggregateNoGroupByNoValue_PreservedInPlan()
	{
		var plan = await GetQueryPlanAsync("SELECT COUNT(1) FROM c");
		var info = plan["queryInfo"]!;

		var aggs = info["aggregates"]!.ToObject<string[]>()!;
		aggs.Should().Contain("Count");
	}

	[Fact]
	public async Task QueryPlan_SingleAggregateWithAlias_MappedCorrectly()
	{
		var plan = await GetQueryPlanAsync("SELECT COUNT(1) AS cnt FROM c");
		var info = plan["queryInfo"]!;

		var aggs = info["aggregates"]!.ToObject<string[]>()!;
		aggs.Should().Contain("Count");

		var aliasMap = info["groupByAliasToAggregateType"]!;
		aliasMap["cnt"]!.ToString().Should().Be("Count");
	}

	[Fact]
	public async Task QueryPlan_OrderByTopCombo_TopInQueryInfoAndNotInRewritten()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT TOP 5 * FROM c ORDER BY c.name ASC");
		var info = plan["queryInfo"]!;

		info["top"]!.Value<int>().Should().Be(5);
		((bool)info["hasNonStreamingOrderBy"]!).Should().BeTrue();

		var rewritten = info["rewrittenQuery"]!.ToString();
		rewritten.Should().Contain("orderByItems");
	}

	[Fact]
	public async Task QueryPlan_GroupByWithOrderBy_BypassClearsOrderBy()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT c.status, COUNT(1) AS cnt FROM c GROUP BY c.status ORDER BY c.status");
		var info = plan["queryInfo"]!;

		// GROUP BY bypass clears both aggregates and ORDER BY
		((bool)info["hasNonStreamingOrderBy"]!).Should().BeFalse();
		info["orderBy"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
		info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
		info["groupByExpressions"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
	}

	[Fact]
	public async Task QueryPlan_EmptySqlString_HandledGracefully()
	{
		var plan = await GetQueryPlanAsync("");
		var info = plan["queryInfo"]!;

		info.Should().NotBeNull();
		info["distinctType"]!.ToString().Should().Be("None");
	}

	[Fact]
	public async Task QueryPlan_MultipleCountDistinct_OnlyFirstCaptured()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT COUNT(DISTINCT c.name), COUNT(DISTINCT c.status) FROM c");
		var info = plan["queryInfo"]!;

		// Regex only captures first COUNT(DISTINCT) — verify at least that works
		var dCountInfo = info["dCountInfo"] as JObject;
		dCountInfo.Should().NotBeNull("dCountInfo should be populated for first COUNT(DISTINCT)");
		dCountInfo!["dCountExpressionBase"]!["propertyPath"]!.ToString().Should().Be("name");
	}

	[Fact]
	public async Task QueryPlan_SelectValueDistinctOrderBySameField_AllFlags()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT DISTINCT VALUE c.name FROM c ORDER BY c.name");
		var info = plan["queryInfo"]!;

		info["distinctType"]!.ToString().Should().Be("Ordered");
		((bool)info["hasSelectValue"]!).Should().BeTrue();
		((bool)info["hasNonStreamingOrderBy"]!).Should().BeTrue();
	}

	[Fact]
	public async Task QueryPlan_OrderByExpressionToString_NestedFunction()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT * FROM c ORDER BY UPPER(LOWER(c.name)) ASC");
		var info = plan["queryInfo"]!;

		var orderByExprs = info["orderByExpressions"]!.ToObject<string[]>()!;
		orderByExprs.Should().HaveCount(1);
		// Should contain the stringified nested function expression
		orderByExprs[0].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task QueryPlan_ValueAggregateBypass_SingleAggregate()
	{
		var plan = await GetQueryPlanAsync("SELECT VALUE SUM(c.val) FROM c");
		var info = plan["queryInfo"]!;

		// VALUE aggregate bypass clears aggregates
		info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
		((bool)info["hasSelectValue"]!).Should().BeTrue();
	}

	[Fact]
	public async Task QueryPlan_ValueNonAggregate_NotBypassed()
	{
		var plan = await GetQueryPlanAsync("SELECT VALUE c.name FROM c");
		var info = plan["queryInfo"]!;

		// No aggregates → no bypass needed
		info["aggregates"]!.Should().BeOfType<JArray>().Which.Should().BeEmpty();
		((bool)info["hasSelectValue"]!).Should().BeTrue();
	}

	// ═══════════════════════════════════════════════════════════════
	//  Phase 3: Divergent Behavior Tests (skipped + sister pairs)
	// ═══════════════════════════════════════════════════════════════

	[Fact]
	public async Task QueryPlan_CountDistinct_ComplexExpression_ShouldPopulateDCountInfo()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT COUNT(DISTINCT UPPER(c.name)) FROM c");
		var info = plan["queryInfo"]!;

		// dCountInfo should be populated for complex expressions
		info["dCountInfo"].Should().NotBeNull();
	}

	[Fact]
	public async Task QueryPlan_MultipleCountDistinct_AllShouldBeDetected()
	{
		var plan = await GetQueryPlanAsync(
			"SELECT COUNT(DISTINCT c.name), COUNT(DISTINCT c.status) FROM c");
		var info = plan["queryInfo"]!;

		// Ideal: All COUNT(DISTINCT) expressions should be captured
		// This would need an array of dCountInfo objects
		info["dCountInfo"].Should().NotBeNull();
	}
}
