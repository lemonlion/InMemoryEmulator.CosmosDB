using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.Tests.Infrastructure;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Regression tests for GitHub Issue #67 — String literal aliases in aggregate queries
/// return null on Linux (where ServiceInterop is unavailable).
///
/// Root cause: <c>ProjectAggregateFields</c> did not evaluate non-aggregate literal
/// expressions (like <c>'Settlement' AS Label</c>). Instead it fell through to a
/// path-lookup branch that called <c>SelectToken("'Settlement'")</c> → null.
///
/// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/select
///   "The SELECT clause supports arbitrary expressions including literal values."
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Issue67StringLiteralAliasTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("issue67", "/partitionKey");

		await _container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", type = "Settlement", amount = 100.0m },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new { id = "2", partitionKey = "pk1", type = "Settlement", amount = 200.0m },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new { id = "3", partitionKey = "pk1", type = "Refund", amount = 50.0m },
			new PartitionKey("pk1"));
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	private async Task<List<T>> DrainQuery<T>(string sql, PartitionKey? pk = null)
	{
		var opts = pk is not null ? new QueryRequestOptions { PartitionKey = pk } : null;
		var iterator = _container.GetItemQueryIterator<T>(sql, requestOptions: opts);
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  String literal in aggregate SELECT projection
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task StringLiteralAlias_InAggregateQuery_ShouldDeserializeCorrectly()
	{
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/select
		//   "The SELECT clause supports arbitrary expressions including literal values."
		var results = await DrainQuery<JObject>(
			"SELECT 'Settlement' AS Label, COUNT(1) AS ItemCount, SUM(c.amount) AS Total FROM c WHERE c.type = 'Settlement'",
			new PartitionKey("pk1"));

		results.Should().ContainSingle();
		var result = results[0];
		result["Label"]!.Value<string>().Should().Be("Settlement");
		result["ItemCount"]!.Value<int>().Should().Be(2);
		result["Total"]!.Value<decimal>().Should().Be(300.0m);
	}

	[Fact]
	public async Task StringLiteralAlias_WithMultipleQueries_ShouldReturnDistinctLiterals()
	{
		var settlements = await DrainQuery<JObject>(
			"SELECT 'Settlement' AS Label, COUNT(1) AS ItemCount, SUM(c.amount) AS Total FROM c WHERE c.type = 'Settlement'",
			new PartitionKey("pk1"));

		var refunds = await DrainQuery<JObject>(
			"SELECT 'Refund' AS Label, COUNT(1) AS ItemCount, SUM(c.amount) AS Total FROM c WHERE c.type = 'Refund'",
			new PartitionKey("pk1"));

		settlements.Should().ContainSingle();
		settlements[0]["Label"]!.Value<string>().Should().Be("Settlement");
		settlements[0]["ItemCount"]!.Value<int>().Should().Be(2);
		settlements[0]["Total"]!.Value<decimal>().Should().Be(300.0m);

		refunds.Should().ContainSingle();
		refunds[0]["Label"]!.Value<string>().Should().Be("Refund");
		refunds[0]["ItemCount"]!.Value<int>().Should().Be(1);
		refunds[0]["Total"]!.Value<decimal>().Should().Be(50.0m);
	}

	[Fact]
	public async Task NumericLiteralAlias_InAggregateQuery_ShouldDeserializeCorrectly()
	{
		// Also test numeric and boolean literal aliases for completeness
		var results = await DrainQuery<JObject>(
			"SELECT 42 AS MagicNumber, COUNT(1) AS ItemCount FROM c WHERE c.type = 'Settlement'",
			new PartitionKey("pk1"));

		results.Should().ContainSingle();
		results[0]["MagicNumber"]!.Value<int>().Should().Be(42);
		results[0]["ItemCount"]!.Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task BooleanLiteralAlias_InAggregateQuery_ShouldDeserializeCorrectly()
	{
		var results = await DrainQuery<JObject>(
			"SELECT true AS IsActive, COUNT(1) AS ItemCount FROM c WHERE c.type = 'Settlement'",
			new PartitionKey("pk1"));

		results.Should().ContainSingle();
		results[0]["IsActive"]!.Value<bool>().Should().BeTrue();
		results[0]["ItemCount"]!.Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task NullLiteralAlias_InAggregateQuery_ShouldDeserializeCorrectly()
	{
		var results = await DrainQuery<JObject>(
			"SELECT null AS Nothing, COUNT(1) AS ItemCount FROM c WHERE c.type = 'Settlement'",
			new PartitionKey("pk1"));

		results.Should().ContainSingle();
		results[0]["ItemCount"]!.Value<int>().Should().Be(2);

		// On Linux (no ServiceInterop), our ProjectAggregateFields correctly returns null.
		// On Windows, ServiceInterop's pipeline may omit null literal fields from aggregates.
		var nothingToken = results[0]["Nothing"];
		if (nothingToken is not null)
		{
			nothingToken.Type.Should().Be(JTokenType.Null);
		}
	}
}
