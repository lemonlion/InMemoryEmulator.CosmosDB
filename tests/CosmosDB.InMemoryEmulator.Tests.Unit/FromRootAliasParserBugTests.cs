using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Reproduces a bug where the SQL parser sets the alias to "root" instead of "c"
/// when using <c>FROM root c</c> syntax. EF Core Cosmos provider generates all
/// queries in this form: <c>SELECT VALUE {...} FROM root c WHERE c.xxx = @p</c>
/// </summary>
public class FromRootAliasParserBugTests : IDisposable
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly HttpClient _httpClient;

	public FromRootAliasParserBugTests()
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

	[Fact]
	public async Task SelectFieldWithFromRootAlias_ReturnsCorrectValue()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT c.name FROM root c WHERE c.id = '1'");

		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(1);
		results[0]["name"]!.Value<string>().Should().Be("Alice");
	}

	[Fact]
	public async Task SelectValueWithFromRootAlias_ReturnsProjectedObject()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bob", Value = 42 },
			new PartitionKey("pk1"));

		// EF Core-style query: SELECT VALUE { "Name": c.name } FROM root c
		var query = new QueryDefinition(
			"SELECT VALUE { \"Name\": c.name, \"Val\": c.value } FROM root c WHERE c.id = '1'");

		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(1);
		results[0]["Name"]!.Value<string>().Should().Be("Bob");
		results[0]["Val"]!.Value<int>().Should().Be(42);
	}

	[Fact]
	public async Task SelectStarWithFromRootAlias_ReturnsAllDocuments()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 99 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Diana", Value = 50 },
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT * FROM root c WHERE c.partitionKey = 'pk1'");

		var iterator = _container.GetItemQueryIterator<TestDocument>(query);
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(2);
		results.Select(d => d.Name).Should().BeEquivalentTo(["Charlie", "Diana"]);
	}

	[Fact]
	public async Task WhereWithFromRootAlias_FiltersCorrectly()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Eve", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Frank", Value = 20 },
			new PartitionKey("pk1"));

		var query = new QueryDefinition(
			"SELECT c.name FROM root c WHERE c.partitionKey = 'pk1' AND c.value > 15");

		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(1);
		results[0]["name"]!.Value<string>().Should().Be("Frank");
	}

	[Fact]
	public async Task FromRootWithAsAlias_AlsoWorks()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Grace", Value = 5 },
			new PartitionKey("pk1"));

		// FROM root AS c — explicit AS keyword
		var query = new QueryDefinition("SELECT c.name FROM root AS c WHERE c.id = '1'");

		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(1);
		results[0]["name"]!.Value<string>().Should().Be("Grace");
	}

	[Fact]
	public async Task QueryPlan_CountDistinct_FromRootAlias_ExtractsCorrectPropertyPath()
	{
		// The regex in HandleQueryPlanAsync captures the first word after FROM,
		// which is "root" for "FROM root c" — but the alias is "c".
		// The distinctExpr is "c.status", so stripping the alias should yield "status".
		var sql = "SELECT COUNT(DISTINCT c.status) FROM root c";
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
		var plan = JObject.Parse(json);

		var dCountInfo = plan["queryInfo"]!["dCountInfo"] as JObject;
		dCountInfo.Should().NotBeNull();
		// With the bug, fromAlias is "root", so "c.status" doesn't start with "root."
		// and the propertyPath would be "c.status" instead of "status"
		dCountInfo!["dCountExpressionBase"]!["propertyPath"]!.ToString().Should().Be("status");
	}
}
