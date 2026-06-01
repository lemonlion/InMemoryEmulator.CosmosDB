using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Regression tests for GitHub Issue #64 — COUNT(expr > 0 ? 1 : undefined)
/// incorrectly counts documents where the expression evaluates to undefined.
///
/// Root cause: <c>ExprToString</c> in <c>CosmosSqlParser</c> did not parenthesise
/// ternary/coalesce sub-expressions within higher-precedence binary operators, so the
/// round-tripped SQL was re-parsed into a different AST.
///
/// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/ternary-coalesce-operators
///   Ternary (?) and Coalesce (??) have the lowest operator precedence in Cosmos DB SQL.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Issue64CountTernaryUndefinedTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("issue64", "/partitionKey");
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

	// ═══════════════════════════════════════════════════════════════════════════
	//  SELECT VALUE with ternary returning undefined
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SelectValue_SimpleTernary_WhenConditionFalse_ReturnsEmpty()
	{
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", amount = 0 }, new PartitionKey("pk1"));

		// amount = 0, so (0 > 0) is false → ternary returns undefined → excluded from results
		var results = await DrainQuery<JToken>(
			"SELECT VALUE c.amount > 0 ? 1 : undefined FROM c");

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task SelectValue_SimpleTernary_WhenConditionTrue_ReturnsValue()
	{
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", amount = 5 }, new PartitionKey("pk1"));

		// amount = 5, so (5 > 0) is true → ternary returns 1
		var results = await DrainQuery<long>(
			"SELECT VALUE c.amount > 0 ? 1 : undefined FROM c");

		results.Should().ContainSingle().Which.Should().Be(1);
	}

	[Fact]
	public async Task SelectValue_NestedTernary_WhenInnerResolvesAndComparisonFails_ReturnsEmpty()
	{
		// creditValue.amount = 0, so inner ternary resolves to 0; 0 > 0 is false → undefined
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", creditValue = new { amount = 0 }, grossValue = new { amount = 100 } },
			new PartitionKey("pk1"));

		var results = await DrainQuery<JToken>(
			"SELECT VALUE (IS_DEFINED(c.creditValue) ? c.creditValue.amount : c.grossValue.amount) > 0 ? 1 : undefined FROM c");

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task SelectValue_NestedTernary_WhenInnerResolvesAndComparisonSucceeds_ReturnsValue()
	{
		// creditValue.amount = 50, so inner ternary resolves to 50; 50 > 0 is true → 1
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", creditValue = new { amount = 50 }, grossValue = new { amount = 100 } },
			new PartitionKey("pk1"));

		var results = await DrainQuery<long>(
			"SELECT VALUE (IS_DEFINED(c.creditValue) ? c.creditValue.amount : c.grossValue.amount) > 0 ? 1 : undefined FROM c");

		results.Should().ContainSingle().Which.Should().Be(1);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  COUNT with ternary returning undefined — the core bug scenario
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Count_SimpleTernary_WhenConditionFalse_ShouldNotCount()
	{
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", amount = 0 }, new PartitionKey("pk1"));

		// amount = 0, so (0 > 0) is false → undefined → COUNT should skip
		var results = await DrainQuery<int>(
			"SELECT VALUE COUNT(c.amount > 0 ? 1 : undefined) FROM c");

		results.Should().ContainSingle().Which.Should().Be(0);
	}

	[Fact]
	public async Task Count_SimpleTernary_MixedDocuments_CountsOnlyMatching()
	{
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", amount = 0 }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new { id = "2", partitionKey = "pk1", amount = 5 }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new { id = "3", partitionKey = "pk1", amount = -1 }, new PartitionKey("pk1"));

		// Only id=2 (amount=5) satisfies amount > 0
		var results = await DrainQuery<int>(
			"SELECT VALUE COUNT(c.amount > 0 ? 1 : undefined) FROM c");

		results.Should().ContainSingle().Which.Should().Be(1);
	}

	[Fact]
	public async Task Count_NestedTernary_WhenInnerResolvesToZero_ShouldNotCount()
	{
		// This is the exact bug scenario from the issue:
		// creditValue.amount = 0, IS_DEFINED(c.creditValue) is true, so inner ternary → 0
		// 0 > 0 is false → outer ternary returns undefined → COUNT should NOT count this doc
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", creditValue = new { amount = 0 }, grossValue = new { amount = 100 } },
			new PartitionKey("pk1"));

		var results = await DrainQuery<int>(
			"SELECT VALUE COUNT((IS_DEFINED(c.creditValue) ? c.creditValue.amount : c.grossValue.amount) > 0 ? 1 : undefined) FROM c");

		results.Should().ContainSingle().Which.Should().Be(0);
	}

	[Fact]
	public async Task Count_NestedTernary_WhenInnerResolvesToPositive_ShouldCount()
	{
		// creditValue.amount = 50, IS_DEFINED(c.creditValue) is true, so inner ternary → 50
		// 50 > 0 is true → outer ternary returns 1 → COUNT SHOULD count this doc
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", creditValue = new { amount = 50 }, grossValue = new { amount = 100 } },
			new PartitionKey("pk1"));

		var results = await DrainQuery<int>(
			"SELECT VALUE COUNT((IS_DEFINED(c.creditValue) ? c.creditValue.amount : c.grossValue.amount) > 0 ? 1 : undefined) FROM c");

		results.Should().ContainSingle().Which.Should().Be(1);
	}

	[Fact]
	public async Task Count_NestedTernary_FallsBackToGrossValue_WhenCreditUndefined()
	{
		// creditValue is NOT defined, so IS_DEFINED is false → inner ternary → grossValue.amount = 200
		// 200 > 0 is true → outer ternary returns 1 → COUNT SHOULD count
		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", grossValue = new { amount = 200 } },
			new PartitionKey("pk1"));

		var results = await DrainQuery<int>(
			"SELECT VALUE COUNT((IS_DEFINED(c.creditValue) ? c.creditValue.amount : c.grossValue.amount) > 0 ? 1 : undefined) FROM c");

		results.Should().ContainSingle().Which.Should().Be(1);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Multi-aggregate query (mirrors the original issue's query pattern)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Count_MultipleAggregatesWithTernary_IssueScenario()
	{
		// Mirrors the exact pattern from the bug report
		await _container.CreateItemAsync(new
		{
			id = "doc1",
			partitionKey = "pk1",
			merchantId = "m1",
			transactionType = "Settlement",
			settlementDate = "2023-09-07",
			creditValue = new { amount = 0 },
			grossSettlementValue = new { amount = 1000.25 },
			upfrontPaymentValue = new { amount = 1000.25 }
		}, new PartitionKey("pk1"));

		var query =
			"SELECT " +
			"COUNT((IS_DEFINED(c.creditValue) ? c.creditValue.amount : c.grossSettlementValue.amount) > 0 ? 1 : undefined) AS NumberTransactions, " +
			"COUNT(c.upfrontPaymentValue.amount > 0 ? 1 : undefined) AS UpfrontPaymentCount " +
			"FROM c WHERE c.transactionType = 'Settlement' AND c.settlementDate = '2023-09-07' AND c.merchantId = 'm1'";

		var results = await DrainQuery<JObject>(query);

		var row = results.Should().ContainSingle().Which;
		// creditValue.amount = 0 → inner ternary resolves to 0 → 0 > 0 is false → undefined → NOT counted
		row["NumberTransactions"]!.Value<int>().Should().Be(0);
		// upfrontPaymentValue.amount = 1000.25 → 1000.25 > 0 is true → 1 → counted
		row["UpfrontPaymentCount"]!.Value<int>().Should().Be(1);
	}
}
