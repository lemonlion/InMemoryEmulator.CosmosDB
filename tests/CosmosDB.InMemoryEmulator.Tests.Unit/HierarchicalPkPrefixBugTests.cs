using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class HierarchicalPkPrefixBugTests(ITestOutputHelper output)
{
	public class HierarchicalDoc
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("level1")] public string Level1 { get; set; } = "";
		[JsonProperty("level2")] public string Level2 { get; set; } = "";
		[JsonProperty("level3")] public string Level3 { get; set; } = "";
		[JsonProperty("data")] public string Data { get; set; } = "";
	}

	private class LoggingHandler(HttpMessageHandler inner, ITestOutputHelper output) : DelegatingHandler(inner)
	{
		protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
		{
			var line = $"{request.Method} {request.RequestUri?.PathAndQuery}";
			foreach (var h in request.Headers)
			{
				if (h.Key.StartsWith("x-ms-documentdb-partition"))
					line += $"  [{h.Key}={string.Join(",", h.Value)}]";
			}
			if (request.Content is not null)
			{
				var body = await request.Content.ReadAsStringAsync(cancellationToken);
				if (body.Contains("query"))
					line += $"  BODY={body}";
			}
			output.WriteLine(line);
			return await base.SendAsync(request, cancellationToken);
		}
	}

	/// <summary>
	/// Diagnostic: see which range IDs the SDK targets for prefix PK queries
	/// when the handler exposes multiple partition key ranges.
	/// </summary>
	[Theory]
	[InlineData(4)]
	[InlineData(8)]
	[InlineData(16)]
	public async Task Diagnostic_MultiRange_PrefixPK_Routing(int rangeCount)
	{
		var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
		var options = new FakeCosmosHandlerOptions { PartitionKeyRangeCount = rangeCount };
		var fakeHandler = new FakeCosmosHandler(inMemoryContainer, options);
		using var client = fakeHandler.CreateClient();

		var container = client.GetContainer("test-db", "items");

		// Seed documents with different tenants
		await container.CreateItemAsync(
			new { id = "id-1", tenantId = "tenant-A", category = "CatA", region = "eu" },
			new PartitionKeyBuilder().Add("tenant-A").Add("CatA").Add("eu").Build());
		await container.CreateItemAsync(
			new { id = "id-2", tenantId = "tenant-A", category = "CatB", region = "us" },
			new PartitionKeyBuilder().Add("tenant-A").Add("CatB").Add("us").Build());
		await container.CreateItemAsync(
			new { id = "id-3", tenantId = "tenant-B", category = "CatA", region = "eu" },
			new PartitionKeyBuilder().Add("tenant-B").Add("CatA").Add("eu").Build());

		// Check which range tenant-A and tenant-B hash to
		var hashA = PartitionKeyHash.MurmurHash3("tenant-A");
		var hashB = PartitionKeyHash.MurmurHash3("tenant-B");
		var rangeA = PartitionKeyHash.GetRangeIndex("tenant-A", rangeCount);
		var rangeB = PartitionKeyHash.GetRangeIndex("tenant-B", rangeCount);
		output.WriteLine($"\n=== Range count: {rangeCount} ===");
		output.WriteLine($"tenant-A hash=0x{hashA:X8} range={rangeA}");
		output.WriteLine($"tenant-B hash=0x{hashB:X8} range={rangeB}");

		output.WriteLine("\n=== Query with 1-component prefix PK (tenant-A) ===");
		var prefixPk = new PartitionKeyBuilder().Add("tenant-A").Build();
		var results = new List<JObject>();
		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		output.WriteLine($"Results: {results.Count} items: {string.Join(", ", results.Select(r => r["id"]))}");

		output.WriteLine("\n=== Query with 2-component prefix PK (tenant-A, CatA) ===");
		prefixPk = new PartitionKeyBuilder().Add("tenant-A").Add("CatA").Build();
		results.Clear();
		iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		output.WriteLine($"Results: {results.Count} items: {string.Join(", ", results.Select(r => r["id"]))}");

		output.WriteLine("\n=== Query with full 3-component PK (tenant-A, CatA, eu) ===");
		var fullPk = new PartitionKeyBuilder().Add("tenant-A").Add("CatA").Add("eu").Build();
		results.Clear();
		iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = fullPk });
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		output.WriteLine($"Results: {results.Count} items: {string.Join(", ", results.Select(r => r["id"]))}");
	}

	[Fact]
	public async Task PrefixQuery_SingleComponent_ReturnsMatchingItems_Direct()
	{
		var container = new InMemoryContainer("test", new[] { "/level1", "/level2", "/level3" });

		var fullPk1 = new PartitionKeyBuilder().Add("a").Add("b").Add("c").Build();
		var fullPk2 = new PartitionKeyBuilder().Add("a").Add("x").Add("y").Build();
		var fullPk3 = new PartitionKeyBuilder().Add("z").Add("b").Add("c").Build();

		await container.CreateItemAsync(new HierarchicalDoc { Id = "1", Level1 = "a", Level2 = "b", Level3 = "c", Data = "match1" }, fullPk1);
		await container.CreateItemAsync(new HierarchicalDoc { Id = "2", Level1 = "a", Level2 = "x", Level3 = "y", Data = "match2" }, fullPk2);
		await container.CreateItemAsync(new HierarchicalDoc { Id = "3", Level1 = "z", Level2 = "b", Level3 = "c", Data = "nomatch" }, fullPk3);

		// Query with 1-component prefix PK
		var prefixPk = new PartitionKeyBuilder().Add("a").Build();
		var iterator = container.GetItemQueryIterator<HierarchicalDoc>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

		var results = new List<HierarchicalDoc>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(["1", "2"]);
	}

	[Fact]
	public async Task PrefixQuery_TwoComponents_ReturnsMatchingItems_Direct()
	{
		var container = new InMemoryContainer("test", new[] { "/level1", "/level2", "/level3" });

		var fullPk1 = new PartitionKeyBuilder().Add("a").Add("b").Add("c").Build();
		var fullPk2 = new PartitionKeyBuilder().Add("a").Add("b").Add("z").Build();
		var fullPk3 = new PartitionKeyBuilder().Add("a").Add("x").Add("y").Build();

		await container.CreateItemAsync(new HierarchicalDoc { Id = "1", Level1 = "a", Level2 = "b", Level3 = "c", Data = "match1" }, fullPk1);
		await container.CreateItemAsync(new HierarchicalDoc { Id = "2", Level1 = "a", Level2 = "b", Level3 = "z", Data = "match2" }, fullPk2);
		await container.CreateItemAsync(new HierarchicalDoc { Id = "3", Level1 = "a", Level2 = "x", Level3 = "y", Data = "nomatch" }, fullPk3);

		// Query with 2-component prefix PK
		var prefixPk = new PartitionKeyBuilder().Add("a").Add("b").Build();
		var iterator = container.GetItemQueryIterator<HierarchicalDoc>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

		var results = new List<HierarchicalDoc>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(["1", "2"]);
	}

	[Fact]
	public async Task PrefixQuery_ViaFakeCosmosHandler_Works()
	{
		var inMemoryContainer = new InMemoryContainer("test", new[] { "/level1", "/level2", "/level3" });
		var handler = new FakeCosmosHandler(inMemoryContainer);
		using var client = handler.CreateClient();

		var container = client.GetContainer("db", "test");

		var fullPk1 = new PartitionKeyBuilder().Add("a").Add("b").Add("c").Build();
		var fullPk2 = new PartitionKeyBuilder().Add("a").Add("x").Add("y").Build();
		var fullPk3 = new PartitionKeyBuilder().Add("z").Add("b").Add("c").Build();

		await container.CreateItemAsync(new HierarchicalDoc { Id = "1", Level1 = "a", Level2 = "b", Level3 = "c", Data = "match1" }, fullPk1);
		await container.CreateItemAsync(new HierarchicalDoc { Id = "2", Level1 = "a", Level2 = "x", Level3 = "y", Data = "match2" }, fullPk2);
		await container.CreateItemAsync(new HierarchicalDoc { Id = "3", Level1 = "z", Level2 = "b", Level3 = "c", Data = "nomatch" }, fullPk3);

		// Query with prefix PK through FakeCosmosHandler — no WHERE clause needed,
		// the prefix PK scoping should filter by partition key alone
		var prefixPk = new PartitionKeyBuilder().Add("a").Build();
		var iterator = container.GetItemQueryIterator<HierarchicalDoc>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

		var results = new List<HierarchicalDoc>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(["1", "2"]);
	}

	/// <summary>
	/// GitHub issue #6: prefix PK scoping broken via FakeCosmosHandler + CosmosClient path.
	/// Direct InMemoryContainer works, but the SDK path returns all documents.
	/// </summary>
	[Fact]
	public async Task Issue6_TwoLevelPrefix_ViaFakeCosmosHandler_ScopesCorrectly()
	{
		var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
		var handler = new FakeCosmosHandler(inMemoryContainer);
		using var client = handler.CreateClient();

		var container = client.GetContainer("test-db", "items");

		var tenantId = "tenant-001";
		var region = "eu-west";

		await container.CreateItemAsync(
			new { id = "id-1", tenantId, category = "CategoryA", region, data = "data A" },
			new PartitionKeyBuilder().Add(tenantId).Add("CategoryA").Add(region).Build());

		await container.CreateItemAsync(
			new { id = "id-2", tenantId, category = "CategoryB", region, data = "data B" },
			new PartitionKeyBuilder().Add(tenantId).Add("CategoryB").Add(region).Build());

		// Query scoped to 2-level prefix (tenantId + "CategoryA")
		var prefixPk = new PartitionKeyBuilder().Add(tenantId).Add("CategoryA").Build();
		var requestOptions = new QueryRequestOptions { PartitionKey = prefixPk };

		var results = new List<JObject>();
		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions);
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
		results[0]["category"]!.Value<string>().Should().Be("CategoryA");
	}

	[Fact]
	public async Task Issue6_OneLevelPrefix_ViaFakeCosmosHandler_ScopesCorrectly()
	{
		var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
		var handler = new FakeCosmosHandler(inMemoryContainer);
		using var client = handler.CreateClient();

		var container = client.GetContainer("test-db", "items");

		var region = "eu-west";

		await container.CreateItemAsync(
			new { id = "id-a", tenantId = "tenant-A", category = "CategoryA", region, data = "data A" },
			new PartitionKeyBuilder().Add("tenant-A").Add("CategoryA").Add(region).Build());

		await container.CreateItemAsync(
			new { id = "id-b", tenantId = "tenant-B", category = "CategoryA", region, data = "data B" },
			new PartitionKeyBuilder().Add("tenant-B").Add("CategoryA").Add(region).Build());

		// Query scoped to 1-level prefix (tenant-A only)
		var prefixPk = new PartitionKeyBuilder().Add("tenant-A").Build();
		var requestOptions = new QueryRequestOptions { PartitionKey = prefixPk };

		var results = new List<JObject>();
		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"), requestOptions: requestOptions);
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
		results[0]["tenantId"]!.Value<string>().Should().Be("tenant-A");
	}

	[Fact]
	public async Task LinqPrefixPK_ViaFakeCosmosHandler_ScopesCorrectly()
	{
		var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
		var handler = new FakeCosmosHandler(inMemoryContainer);
		using var client = handler.CreateClient();

		var container = client.GetContainer("test-db", "items");

		await container.CreateItemAsync(
			new { id = "id-1", tenantId = "tenant-A", category = "CatA", region = "eu", data = "A1" },
			new PartitionKeyBuilder().Add("tenant-A").Add("CatA").Add("eu").Build());

		await container.CreateItemAsync(
			new { id = "id-2", tenantId = "tenant-A", category = "CatB", region = "us", data = "A2" },
			new PartitionKeyBuilder().Add("tenant-A").Add("CatB").Add("us").Build());

		await container.CreateItemAsync(
			new { id = "id-3", tenantId = "tenant-B", category = "CatA", region = "eu", data = "B1" },
			new PartitionKeyBuilder().Add("tenant-B").Add("CatA").Add("eu").Build());

		// LINQ query with 1-component prefix PK — no WithPartitionKey wrapper needed
		var prefixPk = new PartitionKeyBuilder().Add("tenant-A").Build();
		var iterator = container.GetItemLinqQueryable<JObject>(
				requestOptions: new QueryRequestOptions { PartitionKey = prefixPk })
			.ToFeedIterator();

		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
		results.Select(r => r["tenantId"]!.Value<string>()).Should().AllBe("tenant-A");
	}

	[Fact]
	public async Task WrapClient_WithCustomPipeline_PrefixPKStillWorks()
	{
		var inMemoryContainer = new InMemoryContainer("items", new[] { "/tenantId", "/category", "/region" });
		var handler = new FakeCosmosHandler(inMemoryContainer);

		// Simulate a custom HTTP pipeline (e.g. tracking handler) by wrapping the handler
		var delegatingHandler = new PassThroughHandler(handler);

		var innerClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				HttpClientFactory = () => new HttpClient(delegatingHandler)
			});

		// WrapClient adds prefix PK support on top of the user's custom pipeline
		using var client = handler.WrapClient(innerClient);

		var container = client.GetContainer("test-db", "items");

		await container.CreateItemAsync(
			new { id = "id-1", tenantId = "tenant-A", category = "CatA", region = "eu" },
			new PartitionKeyBuilder().Add("tenant-A").Add("CatA").Add("eu").Build());

		await container.CreateItemAsync(
			new { id = "id-2", tenantId = "tenant-B", category = "CatA", region = "eu" },
			new PartitionKeyBuilder().Add("tenant-B").Add("CatA").Add("eu").Build());

		var prefixPk = new PartitionKeyBuilder().Add("tenant-A").Build();
		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
		results[0]["tenantId"]!.Value<string>().Should().Be("tenant-A");
	}

	private class PassThroughHandler(HttpMessageHandler inner) : DelegatingHandler(inner)
	{
		protected override Task<HttpResponseMessage> SendAsync(
			HttpRequestMessage request, CancellationToken cancellationToken)
			=> base.SendAsync(request, cancellationToken);
	}
}
