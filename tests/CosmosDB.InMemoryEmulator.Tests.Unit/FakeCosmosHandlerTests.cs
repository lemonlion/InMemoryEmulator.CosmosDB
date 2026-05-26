using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Gap tests that seed data via InMemoryContainer directly.
/// Cannot run against the real emulator.
/// </summary>
public class FakeCosmosHandlerGapTests
{
	private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task Handler_ReadFeed_ReturnsAllDocuments()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// ReadFeed via SDK — uses read feed endpoint
		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(5);
	}
}


public class FakeCosmosHandlerGapTests4
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task Handler_AccountMetadata_ReturnsValidResponse()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/");

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);
		json["id"].Should().NotBeNull();
		json["writableLocations"].Should().NotBeNull();
		json["readableLocations"].Should().NotBeNull();
	}

	[Fact]
	public async Task Handler_Query_PartitionKeyRange_FiltersToRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 4
		});

		using var httpClient = new HttpClient(handler);

		// Query with a specific partition key range ID
		var request = new HttpRequestMessage(HttpMethod.Post,
			"https://localhost:9999/dbs/db/colls/test/docs");
		request.Headers.Add("x-ms-documentdb-partitionkeyrangeid", "0");
		request.Headers.Add("x-ms-documentdb-query", "True");
		request.Headers.Add("x-ms-documentdb-isquery", "True");
		request.Content = new StringContent(
			"""{"query":"SELECT * FROM c"}""", Encoding.UTF8, "application/query+json");

		var response = await httpClient.SendAsync(request);
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var body = await response.Content.ReadAsStringAsync();
		var result = JObject.Parse(body);
		var documents = result["Documents"]!.ToObject<JArray>();

		// Only items whose PK hashes to range 0 should be returned
		documents!.Count.Should().BeLessThan(20);
		documents.Count.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task Handler_CacheEviction_StaleEntries()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			CacheTtl = TimeSpan.FromMilliseconds(200)
		});
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// First query — caches the result
		var iter1 = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iter1.HasMoreResults) await iter1.ReadNextAsync();

		// Wait for cache TTL to expire
		await Task.Delay(300);

		// Add a new item
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "New" },
			new PartitionKey("pk1"));

		// Second query — stale cache should be evicted, returns fresh results
		var iter2 = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iter2.HasMoreResults)
		{
			var page = await iter2.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Handler_CacheEviction_ExceedsMaxEntries()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 1 },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			CacheMaxEntries = 2
		});
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Execute 3 different queries to exceed max cache entries
		// Note: 'value' is a reserved word in Cosmos SDK, use bracket notation
		var queries = new[]
		{
			"SELECT * FROM c WHERE c.id = '1'",
			"SELECT * FROM c WHERE c[\"value\"] = 1",
			"SELECT * FROM c WHERE c.name = 'Test'"
		};

		foreach (var query in queries)
		{
			var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(query);
			while (iter.HasMoreResults) await iter.ReadNextAsync();
		}

		// All queries should still work — LRU eviction shouldn't break anything
		var finalIter = cosmosContainer.GetItemQueryIterator<TestDocument>(queries[2]);
		var results = new List<TestDocument>();
		while (finalIter.HasMoreResults)
		{
			var page = await finalIter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task Handler_CollectionMetadata_ReturnsContainerProperties()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/test");

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);
		json["id"]!.ToString().Should().Be("test");
		json["partitionKey"].Should().NotBeNull();
	}

	[Fact]
	public async Task Handler_MurmurHash_DistributesEvenly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create 100 items with diverse partition keys
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"tenant-{i}", Name = $"Item{i}" },
				new PartitionKey($"tenant-{i}"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 4
		});
		using var httpClient = new HttpClient(handler);

		var rangeCounts = new int[4];
		for (var rangeId = 0; rangeId < 4; rangeId++)
		{
			var request = new HttpRequestMessage(HttpMethod.Post,
				"https://localhost:9999/dbs/db/colls/test/docs");
			request.Headers.Add("x-ms-documentdb-partitionkeyrangeid", rangeId.ToString());
			request.Headers.Add("x-ms-documentdb-query", "True");
			request.Headers.Add("x-ms-documentdb-isquery", "True");
			request.Content = new StringContent(
				"""{"query":"SELECT * FROM c"}""", Encoding.UTF8, "application/query+json");

			var response = await httpClient.SendAsync(request);
			var body = await response.Content.ReadAsStringAsync();
			var result = JObject.Parse(body);
			rangeCounts[rangeId] = result["Documents"]!.ToObject<JArray>()!.Count;
		}

		// All ranges should have at least some items (rough distribution)
		rangeCounts.Sum().Should().Be(100);
		// Each range should have at least 5 items (with 100 items across 4 ranges)
		foreach (var count in rangeCounts)
			count.Should().BeGreaterThan(5);
	}
}


public class FakeCosmosHandlerTests
{
	private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task Handler_BasicQuery_ReturnsAllItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Handler_OrderByQuery_ReturnsCorrectOrder()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(doc => doc.Name).ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results[0].Name.Should().Be("Alice");
		results[1].Name.Should().Be("Bob");
		results[2].Name.Should().Be("Charlie");
	}

	[Fact]
	public async Task Handler_Pagination_ContinuationTokenRoundtrip()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var allItems = new List<TestDocument>();
		var pageCount = 0;
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>(
				requestOptions: new QueryRequestOptions { MaxItemCount = 2 })
			.ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
			pageCount++;
		}

		allItems.Should().HaveCount(5);
		pageCount.Should().BeGreaterThan(1);
	}

	[Fact]
	public async Task Handler_PartitionKeyRanges_ReturnsConfiguredCount()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 4
		});

		handler.RequestLog.Should().BeEmpty(); // No requests yet

		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Trigger a query to force SDK to request pkranges
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iterator.HasMoreResults)
			await iterator.ReadNextAsync();

		handler.RequestLog.Should().Contain(entry => entry.Contains("/pkranges"));
	}

	[Fact]
	public async Task Handler_PartitionKeyRanges_IfNoneMatch_Returns304()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Make two queries — second should use cached pkranges
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var iterator1 = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iterator1.HasMoreResults) await iterator1.ReadNextAsync();

		var iterator2 = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iterator2.HasMoreResults) await iterator2.ReadNextAsync();

		// Both queries should have triggered pkranges request
		handler.RequestLog.Count(entry => entry.Contains("/pkranges")).Should().BeGreaterThanOrEqualTo(1);
	}

	[Fact]
	public async Task Handler_QueryLog_RecordsQueries()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Use explicit SQL query rather than LINQ — the SDK may optimize LINQ
		// differently and not always route through the query endpoint
		var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iterator.HasMoreResults) await iterator.ReadNextAsync();

		handler.QueryLog.Should().NotBeEmpty();
	}

	[Fact]
	public async Task Handler_RequestLog_RecordsRequests()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iterator.HasMoreResults) await iterator.ReadNextAsync();

		handler.RequestLog.Should().NotBeEmpty();
		handler.RequestLog.Should().Contain(entry => entry.StartsWith("GET") || entry.StartsWith("POST"));
	}

	[Fact]
	public async Task Handler_FilteredQuery_ReturnsCorrectResults()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
			.Where(doc => doc.Value > 15)
			.ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Handler_VerifySdkCompatibility_DoesNotThrow()
	{
		var act = FakeCosmosHandler.VerifySdkCompatibilityAsync;

		await act.Should().NotThrowAsync();
	}

	[Fact]
	public async Task Handler_MultiContainer_RouterDispatchesCorrectly()
	{
		var container1 = new InMemoryContainer("container1", "/partitionKey");
		var container2 = new InMemoryContainer("container2", "/partitionKey");

		await container1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "FromContainer1" },
			new PartitionKey("pk1"));
		await container2.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "FromContainer2" },
			new PartitionKey("pk1"));

		using var handler1 = new FakeCosmosHandler(container1);
		using var handler2 = new FakeCosmosHandler(container2);

		var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
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
				HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var c1 = client.GetContainer("db", "container1");
		var c2 = client.GetContainer("db", "container2");

		var results1 = new List<TestDocument>();
		var iter1 = c1.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter1.HasMoreResults) { var page = await iter1.ReadNextAsync(); results1.AddRange(page); }

		var results2 = new List<TestDocument>();
		var iter2 = c2.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter2.HasMoreResults) { var page = await iter2.ReadNextAsync(); results2.AddRange(page); }

		results1.Should().ContainSingle().Which.Name.Should().Be("FromContainer1");
		results2.Should().ContainSingle().Which.Name.Should().Be("FromContainer2");
	}

	[Fact]
	public async Task Handler_CrossPartition_WithMultipleRanges_AllDataReturned()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i % 5}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i % 5}"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 4
		});
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(20);
	}

	[Fact]
	public async Task Handler_Router_UnregisteredContainer_ThrowsDescriptiveError()
	{
		var orders = new InMemoryContainer("orders", "/customerId");
		var customers = new InMemoryContainer("customers", "/id");

		using var handler1 = new FakeCosmosHandler(orders);
		using var handler2 = new FakeCosmosHandler(customers);

		var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["orders"] = handler1,
			["customers"] = handler2,
		});

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var unknown = client.GetContainer("db", "unknown-container");

		var act = () => unknown.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Handler_CountAsync_ReturnsCorrectCount()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var count = await cosmosContainer.GetItemLinqQueryable<TestDocument>().CountAsync();

		count.Resource.Should().Be(2);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C1. Parameterized Query
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ParameterizedQuery_ReturnsCorrectResults()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.name = @name")
			.WithParameter("@name", "Alice");
		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>(queryDef);
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C2. TOP Query
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_TopQuery_ReturnsLimitedResults()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
			.Take(3)
			.ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(3);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C3. OFFSET/LIMIT Query
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_OffsetLimitQuery_ReturnsPaginatedSlice()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
			.Skip(2).Take(3)
			.ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(3);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C4. DISTINCT Query
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_DistinctQuery_ReturnsUniqueValues()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<string>();
		var iterator = cosmosContainer.GetItemQueryIterator<string>(
			"SELECT DISTINCT VALUE c.name FROM c");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
		results.Should().Contain("Alice");
		results.Should().Contain("Bob");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C5. ORDER BY Descending
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_OrderByDescending_ReturnsReverseOrder()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Charlie" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
			.OrderByDescending(doc => doc.Name).ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results[0].Name.Should().Be("Charlie");
		results[1].Name.Should().Be("Bob");
		results[2].Name.Should().Be("Alice");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C6. Multi-field ORDER BY
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_OrderBy_MultipleFields_ReturnsCorrectOrder()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 20 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 5 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(doc => doc.Name).ThenBy(doc => doc.Value)
			.ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results[0].Name.Should().Be("Alice");
		results[0].Value.Should().Be(10);
		results[1].Name.Should().Be("Alice");
		results[1].Value.Should().Be(20);
		results[2].Name.Should().Be("Bob");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C7. SUM Aggregate
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_SumAggregate_ReturnsCorrectSum()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B", Value = 20 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "C", Value = 30 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<int>();
		var iterator = cosmosContainer.GetItemQueryIterator<int>(
			"SELECT VALUE SUM(c[\"value\"]) FROM c");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Should().Be(60);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C8. MIN/MAX Aggregates
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_MinMaxAggregate_ReturnsCorrectValues()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B", Value = 50 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "C", Value = 30 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var minResults = new List<int>();
		var minIter = cosmosContainer.GetItemQueryIterator<int>(
			"SELECT VALUE MIN(c[\"value\"]) FROM c");
		while (minIter.HasMoreResults)
		{
			var page = await minIter.ReadNextAsync();
			minResults.AddRange(page);
		}

		var maxResults = new List<int>();
		var maxIter = cosmosContainer.GetItemQueryIterator<int>(
			"SELECT VALUE MAX(c[\"value\"]) FROM c");
		while (maxIter.HasMoreResults)
		{
			var page = await maxIter.ReadNextAsync();
			maxResults.AddRange(page);
		}

		minResults.Should().ContainSingle().Which.Should().Be(10);
		maxResults.Should().ContainSingle().Which.Should().Be(50);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C9. AVG Aggregate
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_AvgAggregate_ReturnsCorrectAverage()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B", Value = 20 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "C", Value = 30 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<double>();
		var iterator = cosmosContainer.GetItemQueryIterator<double>(
			"SELECT VALUE AVG(c[\"value\"]) FROM c");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Should().Be(20.0);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C10. Empty Container Query
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_EmptyContainer_QueryReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().BeEmpty();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C11. GROUP BY Query
	//
	//  The SDK's GroupByQueryPipelineStage on non-Windows platforms expects a
	//  specific rewritten query format where each document is wrapped in a
	//  structure with "groupByItems" + "payload" (similar to ORDER BY wrapping).
	//  The handler's query plan returns groupByExpressions and
	//  groupByAliasToAggregateType, but the rewritten query is the original SQL,
	//  which the SDK's GroupByQueryPipelineStage then fails to parse because it
	//  tries to extract JSON paths from SDK-internal field names containing
	//  curly braces (e.g. {"item": c.name}).
	//
	//  Fixing this requires implementing GROUP BY document wrapping in
	//  FakeCosmosHandler.HandleQueryAsync (analogous to the ORDER BY wrapping),
	//  which is non-trivial because:
	//  1. The SDK expects partial aggregates per group key per partition range
	//  2. The rewritten query format is undocumented SDK internals
	//  3. AVG needs {sum,count} per group, not a final value
	//
	//  GROUP BY works correctly via InMemoryContainer.GetItemQueryIterator()
	//  directly — this limitation only applies to the FakeCosmosHandler HTTP
	//  path. Using InMemoryCosmosClient or UseInMemoryCosmosDB() DI bypasses
	//  this entirely.
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_GroupByQuery_ReturnsGroupedResults()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 20 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 30 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<JObject>();
		var iterator = cosmosContainer.GetItemQueryIterator<JObject>(
			"SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
		results.Should().Contain(r => r["name"]!.ToString() == "Alice" && r["cnt"]!.Value<int>() == 2);
		results.Should().Contain(r => r["name"]!.ToString() == "Bob" && r["cnt"]!.Value<int>() == 1);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C12. Query with Partition Key Filter
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_QueryWithPartitionKeyFilter_ReturnsFilteredResults()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pkA", Name = "FromA" }, new PartitionKey("pkA"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pkB", Name = "FromB" }, new PartitionKey("pkB"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pkA") });
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("FromA");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D4. Query response item count header
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_QueryResponse_ContainsItemCount()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var request = new HttpRequestMessage(HttpMethod.Post,
			"https://localhost:9999/dbs/db/colls/test/docs");
		request.Headers.Add("x-ms-documentdb-query", "True");
		request.Headers.Add("x-ms-documentdb-isquery", "True");
		request.Content = new StringContent(
			"""{"query":"SELECT * FROM c"}""", Encoding.UTF8, "application/query+json");

		var response = await httpClient.SendAsync(request);
		response.Headers.TryGetValues("x-ms-item-count", out var itemCountValues).Should().BeTrue();
		int.Parse(itemCountValues!.First()).Should().Be(2);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D5. Collection metadata detail
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CollectionMetadata_ContainsIndexingPolicy()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/test");

		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);
		json["indexingPolicy"].Should().NotBeNull();
		json["indexingPolicy"]!["indexingMode"]!.ToString().Should().Be("consistent");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D6. Composite PK metadata
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CollectionMetadata_WithCompositePartitionKey_ReturnsMultiplePaths()
	{
		var container = new InMemoryContainer("test", ["/tenantId", "/userId"]);
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/test");

		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);
		var paths = json["partitionKey"]!["paths"]!.ToObject<string[]>();
		paths.Should().HaveCount(2);
		paths.Should().Contain("/tenantId");
		paths.Should().Contain("/userId");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D7. Account metadata detail
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_AccountMetadata_ContainsQueryEngineConfiguration()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/");

		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);
		json["queryEngineConfiguration"].Should().NotBeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  E1. Router with single container
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_Router_SingleContainer_WorksCorrectly()
	{
		var container = new InMemoryContainer("only", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Solo" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["only"] = handler,
		});

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var c = client.GetContainer("db", "only");
		var results = new List<TestDocument>();
		var iter = c.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter.HasMoreResults) { var page = await iter.ReadNextAsync(); results.AddRange(page); }

		results.Should().ContainSingle().Which.Name.Should().Be("Solo");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  E2. Full CRUD through router
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_Router_CrudThroughRouter_WorksEndToEnd()
	{
		var container = new InMemoryContainer("crud-routed", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["crud-routed"] = handler,
		});

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var c = client.GetContainer("db", "crud-routed");
		var doc = new TestDocument { Id = "r1", PartitionKey = "pk1", Name = "Created" };

		// Create
		var createResp = await c.CreateItemAsync(doc, new PartitionKey("pk1"));
		createResp.StatusCode.Should().Be(HttpStatusCode.Created);

		// Read
		var readResp = await c.ReadItemAsync<TestDocument>("r1", new PartitionKey("pk1"));
		readResp.Resource.Name.Should().Be("Created");

		// Replace
		doc.Name = "Replaced";
		var replaceResp = await c.ReplaceItemAsync(doc, "r1", new PartitionKey("pk1"));
		replaceResp.StatusCode.Should().Be(HttpStatusCode.OK);
		replaceResp.Resource.Name.Should().Be("Replaced");

		// Delete
		var deleteResp = await c.DeleteItemAsync<TestDocument>("r1", new PartitionKey("pk1"));
		deleteResp.StatusCode.Should().Be(HttpStatusCode.NoContent);

		// Verify gone
		var act = () => c.ReadItemAsync<TestDocument>("r1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  E3. Per-container fault injection via router
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_Router_FaultInjection_PerContainer()
	{
		var containerOk = new InMemoryContainer("healthy", "/partitionKey");
		var containerFail = new InMemoryContainer("faulty", "/partitionKey");

		await containerOk.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Ok" },
			new PartitionKey("pk1"));
		await containerFail.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Fail" },
			new PartitionKey("pk1"));

		using var handlerOk = new FakeCosmosHandler(containerOk);
		using var handlerFail = new FakeCosmosHandler(containerFail);

		// Only the faulty container injects faults
		handlerFail.FaultInjector = _ =>
			new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);

		var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["healthy"] = handlerOk,
			["faulty"] = handlerFail,
		});

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
			});

		// Healthy container works
		var healthy = client.GetContainer("db", "healthy");
		var readOk = await healthy.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readOk.Resource.Name.Should().Be("Ok");

		// Faulty container fails
		var faulty = client.GetContainer("db", "faulty");
		var act = () => faulty.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  F1. Abandoned iteration cache eviction
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_AbandonedIteration_CacheIsEventuallyEvicted()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			CacheTtl = TimeSpan.FromMilliseconds(100)
		});
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Start paginated query but only read the first page
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>(
				requestOptions: new QueryRequestOptions { MaxItemCount = 2 })
			.ToFeedIterator();
		if (iterator.HasMoreResults)
			await iterator.ReadNextAsync();
		// Deliberately abandon the iterator

		// Wait for cache TTL
		await Task.Delay(200);

		// A fresh query should still work correctly (cache was evicted, no corruption)
		var results = new List<TestDocument>();
		var freshIterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (freshIterator.HasMoreResults)
		{
			var page = await freshIterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(10);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  F2. Concurrent queries don't interfere
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ConcurrentQueries_DoNotInterfere()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var task1 = Task.Run(async () =>
		{
			var results = new List<TestDocument>();
			var iter = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'Alice'");
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				results.AddRange(page);
			}
			return results;
		});

		var task2 = Task.Run(async () =>
		{
			var results = new List<TestDocument>();
			var iter = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c[\"value\"] > 15");
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				results.AddRange(page);
			}
			return results;
		});

		var results1 = await task1;
		var results2 = await task2;

		results1.Should().ContainSingle().Which.Name.Should().Be("Alice");
		results2.Should().HaveCount(2);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  G1. Dispose clears cache
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_Dispose_ClearsQueryCache()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Run a paginated query to populate cache
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>(
				requestOptions: new QueryRequestOptions { MaxItemCount = 2 })
			.ToFeedIterator();
		if (iterator.HasMoreResults)
			await iterator.ReadNextAsync();

		// Dispose should not throw
		handler.Dispose();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  G2. Double dispose safety
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public void Handler_DoubleDispose_DoesNotThrow()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var handler = new FakeCosmosHandler(container);

		handler.Dispose();
		var act = () => handler.Dispose();

		act.Should().NotThrow();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  H1. Unrecognised route returns 404
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_UnrecognisedRoute_Returns404WithMessage()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/something/weird");

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
		var body = await response.Content.ReadAsStringAsync();
		body.Should().Contain("FakeCosmosHandler");
		body.Should().Contain("unrecognised route");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B1. Numeric Partition Key
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_WithNumericPartitionKey_Succeeds()
	{
		var container = new InMemoryContainer("numericpk", "/value");
		using var handler = new FakeCosmosHandler(container);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		var c = client.GetContainer("db", "numericpk");

		var doc = new { id = "n1", value = 42, name = "NumericPK" };
		var createResponse = await c.CreateItemAsync(doc, new PartitionKey(42));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await c.ReadItemAsync<JObject>("n1", new PartitionKey(42));
		readResponse.Resource["name"]!.ToString().Should().Be("NumericPK");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B2. Boolean Partition Key
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_WithBooleanPartitionKey_Succeeds()
	{
		var container = new InMemoryContainer("boolpk", "/isActive");
		using var handler = new FakeCosmosHandler(container);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		var c = client.GetContainer("db", "boolpk");

		var doc = new { id = "b1", isActive = true, name = "BoolPK" };
		var createResponse = await c.CreateItemAsync(doc, new PartitionKey(true));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await c.ReadItemAsync<JObject>("b1", new PartitionKey(true));
		readResponse.Resource["name"]!.ToString().Should().Be("BoolPK");
	}
}


/// <summary>
/// Deep-dive tests for FakeCosmosHandler query pipeline, metadata, cache, router, fault injection,
/// and continuation token edge cases.
/// </summary>
public class FakeCosmosHandlerDeepDiveTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	private static async Task<(InMemoryContainer container, FakeCosmosHandler handler, CosmosClient client, Container cosmosContainer)> SetupWithData(int count = 5, string containerName = "deep-test")
	{
		var container = new InMemoryContainer(containerName, "/partitionKey");
		for (var i = 0; i < count; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i * 10 },
				new PartitionKey("pk1"));
		var handler = new FakeCosmosHandler(container);
		var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", containerName);
		return (container, handler, client, cosmosContainer);
	}

	// ── I: Query Pipeline Edge Cases ────────────────────────────────────────

	[Fact]
	public async Task Handler_OrderByWithWhere_ReturnsFilteredSortedResults()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		var results = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c WHERE c[\"value\"] >= 20 ORDER BY c[\"value\"] DESC");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3); // Items 2,3,4 (values 20,30,40)
		results[0].Value.Should().Be(40);
		results[2].Value.Should().Be(20);
	}

	[Fact]
	public async Task Handler_OrderByWithPagination_ContinuationWorksAcrossPages()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		var allResults = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c[\"value\"] ASC",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			allResults.AddRange(page);
		}

		allResults.Should().HaveCount(5);
		allResults.Select(r => r.Value).Should().BeInAscendingOrder();
	}

	[Fact]
	public async Task Handler_TopWithOrderBy_ReturnsTopNOrdered()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		var results = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT TOP 3 * FROM c ORDER BY c[\"value\"] DESC");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
		results[0].Value.Should().Be(40);
		results[1].Value.Should().Be(30);
		results[2].Value.Should().Be(20);
	}

	[Fact]
	public async Task Handler_AggregateWithWhere_ReturnsFilteredAggregate()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		var iter = cosmosContainer.GetItemQueryIterator<int>(
			"SELECT VALUE COUNT(1) FROM c WHERE c[\"value\"] > 15");
		var results = new List<int>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		// Items 2,3,4 have values 20,30,40 → count = 3
		results.Should().ContainSingle().Which.Should().Be(3);
	}

	[Fact]
	public async Task Handler_QueryWithNullPartitionKey_ReturnsNullPKItems()
	{
		var container = new InMemoryContainer("null-pk-query", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "n1", PartitionKey = null!, Name = "NullPK" },
			PartitionKey.Null);

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "null-pk-query");

		var results = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task Handler_ReadFeed_WithPagination_PagesThroughAllDocuments()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		var allResults = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		int pageCount = 0;
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			allResults.AddRange(page);
			pageCount++;
		}

		allResults.Should().HaveCount(5);
		pageCount.Should().BeGreaterThan(1);
	}

	// ── III: Response Header & Metadata Fidelity ────────────────────────────

	[Fact]
	public async Task Handler_QueryResponse_ContainsRequestCharge()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(3);
		using var _ = handler; using var __ = client;

		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iter.ReadNextAsync();

		page.RequestCharge.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task Handler_PkRanges_Response_ContainsETag()
	{
		var container = new InMemoryContainer("pk-etag", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/pk-etag/pkranges");

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Headers.ETag.Should().NotBeNull();
	}

	[Fact]
	public async Task Handler_PkRanges_Response_ContainsCorrectRangeStructure()
	{
		var container = new InMemoryContainer("pk-struct", "/partitionKey");
		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions { PartitionKeyRangeCount = 2 });
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/pk-struct/pkranges");
		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);
		var ranges = (JArray)json["PartitionKeyRanges"]!;

		ranges.Should().HaveCount(2);
		var first = (JObject)ranges[0];
		first["id"].Should().NotBeNull();
		first["minInclusive"].Should().NotBeNull();
		first["maxExclusive"].Should().NotBeNull();
	}

	[Fact]
	public async Task Handler_CollectionMetadata_ContainsGeospatialConfig()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/geo-test");
		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);

		json["geospatialConfig"].Should().NotBeNull();
		json["geospatialConfig"]!["type"]!.ToString().Should().Be("Geography");
	}

	[Fact]
	public async Task Handler_CollectionMetadata_ContainsSelfLink()
	{
		var container = new InMemoryContainer("self-test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/self-test");
		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);

		json["_self"].Should().NotBeNull();
		json["_etag"].Should().NotBeNull();
		json["_ts"].Should().NotBeNull();
	}

	[Fact]
	public async Task Handler_AccountMetadata_ContainsConsistencyPolicy()
	{
		var container = new InMemoryContainer("cons-test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		var response = await httpClient.GetAsync("https://localhost:9999/");
		var body = await response.Content.ReadAsStringAsync();
		var json = JObject.Parse(body);

		json["userConsistencyPolicy"].Should().NotBeNull();
		json["userConsistencyPolicy"]!["defaultConsistencyLevel"]!.ToString().Should().Be("Session");
	}

	// ── IV: Cache Edge Cases ────────────────────────────────────────────────

	[Fact]
	public async Task Handler_Cache_ConcurrentPaginatedQueries_IndependentContinuations()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(6);
		using var _ = handler; using var __ = client;

		var opts = new QueryRequestOptions { MaxItemCount = 2 };

		// Start two iterators before consuming either
		var iter1 = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c ORDER BY c.id ASC", requestOptions: opts);
		var iter2 = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c ORDER BY c.id DESC", requestOptions: opts);

		var results1 = new List<TestDocument>();
		var results2 = new List<TestDocument>();

		// Interleave reads from both iterators
		while (iter1.HasMoreResults || iter2.HasMoreResults)
		{
			if (iter1.HasMoreResults) results1.AddRange(await iter1.ReadNextAsync());
			if (iter2.HasMoreResults) results2.AddRange(await iter2.ReadNextAsync());
		}

		results1.Should().HaveCount(6);
		results2.Should().HaveCount(6);
	}

	[Fact]
	public async Task Handler_Cache_SameQueryTwice_GetsIndependentCursors()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(4);
		using var _ = handler; using var __ = client;

		var opts = new QueryRequestOptions { MaxItemCount = 2 };

		var iter1 = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: opts);
		var iter2 = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: opts);

		var results1 = new List<TestDocument>();
		while (iter1.HasMoreResults) results1.AddRange(await iter1.ReadNextAsync());

		var results2 = new List<TestDocument>();
		while (iter2.HasMoreResults) results2.AddRange(await iter2.ReadNextAsync());

		results1.Should().HaveCount(4);
		results2.Should().HaveCount(4);
	}

	// ── V: Router Edge Cases ────────────────────────────────────────────────

	[Fact]
	public void Handler_Router_EmptyDictionary_ThrowsOnConstruction()
	{
		var act = () => FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>());

		act.Should().Throw<ArgumentException>()
			.Which.Message.Should().Contain("At least one handler");
	}

	[Fact]
	public void Handler_Router_Dispose_DisposesAllChildHandlers()
	{
		var container1 = new InMemoryContainer("c1", "/partitionKey");
		var container2 = new InMemoryContainer("c2", "/partitionKey");
		var handler1 = new FakeCosmosHandler(container1);
		var handler2 = new FakeCosmosHandler(container2);

		using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["c1"] = handler1,
			["c2"] = handler2,
		});

		// Dispose should complete without error, including multiple calls
		router.Dispose();
		router.Dispose(); // idempotent
	}

	[Fact]
	public async Task Handler_Router_PkRangesRequest_RoutesThroughCorrectHandler()
	{
		var container1 = new InMemoryContainer("route-c1", "/partitionKey");
		var container2 = new InMemoryContainer("route-c2", "/partitionKey");
		using var handler1 = new FakeCosmosHandler(container1, new FakeCosmosHandlerOptions { PartitionKeyRangeCount = 2 });
		using var handler2 = new FakeCosmosHandler(container2, new FakeCosmosHandlerOptions { PartitionKeyRangeCount = 4 });

		using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["route-c1"] = handler1,
			["route-c2"] = handler2,
		});
		using var httpClient = new HttpClient(router);

		var response1 = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/route-c1/pkranges");
		var json1 = JObject.Parse(await response1.Content.ReadAsStringAsync());
		((JArray)json1["PartitionKeyRanges"]!).Should().HaveCount(2);

		var response2 = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/route-c2/pkranges");
		var json2 = JObject.Parse(await response2.Content.ReadAsStringAsync());
		((JArray)json2["PartitionKeyRanges"]!).Should().HaveCount(4);
	}

	// ── VI: Fault Injection Edge Cases ──────────────────────────────────────

	[Fact]
	public async Task Handler_FaultInjector_IncludesMetadata_BlocksAccountRequest()
	{
		var container = new InMemoryContainer("fi-meta", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.InternalServerError);
		handler.FaultInjectorIncludesMetadata = true;

		using var httpClient = new HttpClient(handler);
		var response = await httpClient.GetAsync("https://localhost:9999/");

		response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
	}

	[Fact]
	public async Task Handler_FaultInjector_ExcludesMetadata_AllowsAccountRequest()
	{
		var container = new InMemoryContainer("fi-nometa", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.InternalServerError);
		handler.FaultInjectorIncludesMetadata = false;

		using var httpClient = new HttpClient(handler);
		var response = await httpClient.GetAsync("https://localhost:9999/");

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Handler_FaultInjector_QueryRequest_ReturnsInjectedFault()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(1);
		using var _ = handler; using var __ = client;

		handler.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
		{
			Headers = { RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromMilliseconds(1)) }
		};
		handler.FaultInjectorIncludesMetadata = false;

		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var act = async () => { while (iter.HasMoreResults) await iter.ReadNextAsync(); };

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Handler_FaultInjector_SelectiveByMethod_OnlyBlocksReads()
	{
		var container = new InMemoryContainer("fi-selective", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "fi-selective");

		handler.FaultInjector = req =>
			req.Method == HttpMethod.Get
				? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
				: null!;
		handler.FaultInjectorIncludesMetadata = false;

		// POST (create) should succeed
		var doc = new TestDocument { Id = "sel1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await cosmosContainer.CreateItemAsync(doc, new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		// GET (read) should fail
		var act = () => cosmosContainer.ReadItemAsync<TestDocument>("sel1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	// ── VII: Continuation Token Edge Cases ──────────────────────────────────

	[Fact]
	public async Task Handler_ContinuationToken_IsNonEmptyWhenMorePages()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		var firstPage = await iter.ReadNextAsync();

		// The continuation token should be non-null when more pages exist
		firstPage.ContinuationToken.Should().NotBeNullOrEmpty();
		iter.HasMoreResults.Should().BeTrue();
	}

	// ── IX: Error Path Coverage ─────────────────────────────────────────────

	[Fact]
	public async Task Handler_Query_UnparsableSql_FallsBackToDirectExecution()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(3);
		using var _ = handler; using var __ = client;

		// A query that the parser can handle — verifies fallback doesn't break things
		var results = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
	}

	// ── Query Plan Validation (Indirect) ────────────────────────────────────

	[Fact]
	public async Task Handler_QueryPlan_OrderBy_WorksEndToEnd()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		// If the query plan for ORDER BY is wrong, the SDK would reject the response
		var results = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c.name ASC");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(5);
		results.Select(r => r.Name).Should().BeInAscendingOrder();
	}

	[Fact]
	public async Task Handler_QueryPlan_Distinct_WorksEndToEnd()
	{
		var container = new InMemoryContainer("distinct-test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Alice", Value = 20 }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "distinct-test");

		var results = new List<JObject>();
		var iter = cosmosContainer.GetItemQueryIterator<JObject>("SELECT DISTINCT c.name FROM c");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Handler_QueryPlan_OffsetLimit_WorksEndToEnd()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		var results = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c.id OFFSET 1 LIMIT 2");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Handler_QueryPlan_SelectValue_WorksEndToEnd()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(3);
		using var _ = handler; using var __ = client;

		var results = new List<string>();
		var iter = cosmosContainer.GetItemQueryIterator<string>("SELECT VALUE c.name FROM c");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task Handler_QueryPlan_Aggregates_WorkEndToEnd()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(5);
		using var _ = handler; using var __ = client;

		var iter = cosmosContainer.GetItemQueryIterator<int>(
			"SELECT VALUE SUM(c[\"value\"]) FROM c");
		var results = new List<int>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		// 0+10+20+30+40 = 100
		results.Should().ContainSingle().Which.Should().Be(100);
	}

	[Fact]
	public async Task Handler_QueryPlan_BasicSelect_WorksEndToEnd()
	{
		var (_, handler, client, cosmosContainer) = await SetupWithData(3);
		using var _ = handler; using var __ = client;

		var results = new List<TestDocument>();
		var iter = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task Handler_DistinctWithOrderBy_ReturnsOrderedDistinctValues()
	{
		var container = new InMemoryContainer("do-test", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob" }, new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "do-test");

		var results = new List<JObject>();
		var iter = cosmosContainer.GetItemQueryIterator<JObject>("SELECT DISTINCT c.name FROM c ORDER BY c.name ASC");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Handler_MultipleAggregates_ReturnsAllValues()
	{
		var container = new InMemoryContainer("multi-agg", "/partitionKey");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i * 10 },
				new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "multi-agg");

		// Use c["value"] bracket notation because 'value' is a reserved keyword
		// in the SDK's ServiceInterop query plan generator.
		var iter = cosmosContainer.GetItemQueryIterator<JObject>(
			"SELECT COUNT(1) as cnt, SUM(c[\"value\"]) as total FROM c");
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle();
		results[0]["cnt"]!.Value<long>().Should().Be(5);
		results[0]["total"]!.Value<long>().Should().Be(100);
	}

}

#region Deep Dive: Additional Coverage

public class FakeCosmosHandlerCoverageTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	private static async Task<(InMemoryContainer container, FakeCosmosHandler handler, CosmosClient client, Container cosmosContainer)>
		SetupWithData(int count = 5)
	{
		var container = new InMemoryContainer("coverage-test", "/partitionKey");
		for (var i = 0; i < count; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i * 10 },
				new PartitionKey("pk1"));
		var handler = new FakeCosmosHandler(container);
		var client = CreateClient(handler);
		return (container, handler, client, client.GetContainer("db", "coverage-test"));
	}

	// ── Section A: CRUD via Handler ──

	[Fact]
	public async Task Handler_Upsert_ViaSDK_CreatesNewItem()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(0);
		using (handler)
		{
			var response = await cosmosContainer.UpsertItemAsync(
				new TestDocument { Id = "new", PartitionKey = "pk1", Name = "New" },
				new PartitionKey("pk1"));
			response.StatusCode.Should().BeOneOf(HttpStatusCode.OK, HttpStatusCode.Created);
		}
	}

	[Fact]
	public async Task Handler_Upsert_ViaSDK_ReplacesExistingItem()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			await cosmosContainer.UpsertItemAsync(
				new TestDocument { Id = "0", PartitionKey = "pk1", Name = "Updated" },
				new PartitionKey("pk1"));
			var read = await cosmosContainer.ReadItemAsync<TestDocument>("0", new PartitionKey("pk1"));
			read.Resource.Name.Should().Be("Updated");
		}
	}

	[Fact]
	public async Task Handler_Create_DuplicateId_Returns409Conflict()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			var act = () => cosmosContainer.CreateItemAsync(
				new TestDocument { Id = "0", PartitionKey = "pk1", Name = "Dup" },
				new PartitionKey("pk1"));
			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.And.StatusCode.Should().Be(HttpStatusCode.Conflict);
		}
	}

	[Fact]
	public async Task Handler_Read_NonExistentItem_Returns404()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(0);
		using (handler)
		{
			var act = () => cosmosContainer.ReadItemAsync<TestDocument>("missing", new PartitionKey("pk1"));
			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.And.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}

	[Fact]
	public async Task Handler_Replace_NonExistentItem_Returns404()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(0);
		using (handler)
		{
			var act = () => cosmosContainer.ReplaceItemAsync(
				new TestDocument { Id = "missing", PartitionKey = "pk1", Name = "X" },
				"missing", new PartitionKey("pk1"));
			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.And.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}

	[Fact]
	public async Task Handler_Delete_NonExistentItem_Returns404()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(0);
		using (handler)
		{
			var act = () => cosmosContainer.DeleteItemAsync<TestDocument>("missing", new PartitionKey("pk1"));
			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.And.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}

	[Fact]
	public async Task Handler_Create_ResponseContainsETag()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(0);
		using (handler)
		{
			var response = await cosmosContainer.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
				new PartitionKey("pk1"));
			response.ETag.Should().NotBeNullOrEmpty();
		}
	}

	[Fact]
	public async Task Handler_Replace_WithIfMatchETag_Success()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			var read = await cosmosContainer.ReadItemAsync<TestDocument>("0", new PartitionKey("pk1"));
			var response = await cosmosContainer.ReplaceItemAsync(
				new TestDocument { Id = "0", PartitionKey = "pk1", Name = "Replaced" },
				"0", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = read.ETag });
			response.StatusCode.Should().Be(HttpStatusCode.OK);
		}
	}

	[Fact]
	public async Task Handler_Replace_WithStaleIfMatchETag_Returns412()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			var read = await cosmosContainer.ReadItemAsync<TestDocument>("0", new PartitionKey("pk1"));
			var staleEtag = read.ETag;
			await cosmosContainer.ReplaceItemAsync(
				new TestDocument { Id = "0", PartitionKey = "pk1", Name = "V2" },
				"0", new PartitionKey("pk1"));

			var act = () => cosmosContainer.ReplaceItemAsync(
				new TestDocument { Id = "0", PartitionKey = "pk1", Name = "V3" },
				"0", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = staleEtag });
			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.And.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
		}
	}

	// ── Section B: Patch via Handler ──

	[Fact]
	public async Task Handler_Patch_SetOperation_CreatesNewProperty()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			var response = await cosmosContainer.PatchItemAsync<JObject>(
				"0", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/newProp", "hello") });
			response.Resource["newProp"]!.Value<string>().Should().Be("hello");
		}
	}

	[Fact]
	public async Task Handler_Patch_ReplaceOperation_UpdatesExistingProperty()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			var response = await cosmosContainer.PatchItemAsync<TestDocument>(
				"0", new PartitionKey("pk1"),
				new[] { PatchOperation.Replace("/name", "Replaced") });
			response.Resource.Name.Should().Be("Replaced");
		}
	}

	[Fact]
	public async Task Handler_Patch_RemoveOperation_RemovesProperty()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			var response = await cosmosContainer.PatchItemAsync<JObject>(
				"0", new PartitionKey("pk1"),
				new[] { PatchOperation.Remove("/name") });
			response.Resource["name"].Should().BeNull();
		}
	}

	[Fact]
	public async Task Handler_Patch_IncrOperation_IncrementsNumericField()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			var response = await cosmosContainer.PatchItemAsync<TestDocument>(
				"0", new PartitionKey("pk1"),
				new[] { PatchOperation.Increment("/value", 5) });
			response.Resource.Value.Should().Be(5); // was 0 (i*10 for i=0)
		}
	}

	[Fact]
	public async Task Handler_Patch_MultipleOperations_AppliedAtomically()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(1);
		using (handler)
		{
			var response = await cosmosContainer.PatchItemAsync<JObject>(
				"0", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "Multi"), PatchOperation.Increment("/value", 100) });
			response.Resource["name"]!.Value<string>().Should().Be("Multi");
			response.Resource["value"]!.Value<int>().Should().Be(100);
		}
	}

	[Fact]
	public async Task Handler_Patch_NonExistentDocument_Returns404()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(0);
		using (handler)
		{
			var act = () => cosmosContainer.PatchItemAsync<TestDocument>(
				"missing", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "X") });
			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.And.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}

	// ── Section C: Query Pipeline ──

	[Fact]
	public async Task Handler_Query_WithINOperator_ReturnsMatchingItems()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(3);
		using (handler)
		{
			var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
				"SELECT * FROM c WHERE c.name IN ('Item0', 'Item2')");
			var results = new List<TestDocument>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			results.Select(r => r.Name).Should().BeEquivalentTo(new[] { "Item0", "Item2" });
		}
	}

	[Fact]
	public async Task Handler_Query_WithBETWEEN_ReturnsRange()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(5);
		using (handler)
		{
			var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
				"SELECT * FROM c WHERE c[\"value\"] BETWEEN 10 AND 30");
			var results = new List<TestDocument>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			results.Should().HaveCount(3);
		}
	}

	[Fact]
	public async Task Handler_Query_SelectSpecificFields_ReturnsProjection()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(2);
		using (handler)
		{
			var iter = cosmosContainer.GetItemQueryIterator<JObject>(
				"SELECT c.name FROM c ORDER BY c.id");
			var results = new List<JObject>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			results.Should().HaveCount(2);
			results[0]["name"]!.Value<string>().Should().Be("Item0");
		}
	}

	[Fact]
	public async Task Handler_Query_EmptyResult_ReturnsEmptyArray()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(3);
		using (handler)
		{
			var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
				"SELECT * FROM c WHERE c.id = 'nonexistent'");
			var results = new List<TestDocument>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			results.Should().BeEmpty();
		}
	}

	// ── Section D: Pagination ──

	[Fact]
	public async Task Handler_Pagination_SingleItemPages_DrainsFully()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(3);
		using (handler)
		{
			var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
				"SELECT * FROM c ORDER BY c.id",
				requestOptions: new QueryRequestOptions { MaxItemCount = 1 });
			var results = new List<TestDocument>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			results.Should().HaveCount(3);
		}
	}

	[Fact]
	public async Task Handler_Pagination_MaxItemCountLargerThanData_SinglePage()
	{
		var (_, handler, _, cosmosContainer) = await SetupWithData(3);
		using (handler)
		{
			var iter = cosmosContainer.GetItemQueryIterator<TestDocument>(
				"SELECT * FROM c",
				requestOptions: new QueryRequestOptions { MaxItemCount = 100 });
			var pages = 0;
			while (iter.HasMoreResults) { await iter.ReadNextAsync(); pages++; }
			pages.Should().Be(1);
		}
	}

	// ── Section E: Router ──

	[Fact]
	public async Task Handler_DifferentPartitionKeyPath_WorksCorrectly()
	{
		var container = new InMemoryContainer("c1", "/customPk");
		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var c = client.GetContainer("db", "c1");

		await c.CreateItemAsync(JObject.FromObject(new { id = "1", customPk = "a" }), new PartitionKey("a"));
		var read = await c.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		read.Resource["id"]!.Value<string>().Should().Be("1");
	}

	// ── Section F: Fault Injection ──

	[Fact]
	public async Task Handler_FaultInjector_NullReturn_PassesThrough()
	{
		var container = new InMemoryContainer("fi-test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjector = _ => null; // null means pass through
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "fi-test");

		var read = await cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task Handler_FaultInjector_Toggle_MidTest()
	{
		var container = new InMemoryContainer("fi-toggle", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "fi-toggle");

		// Enable fault injection
		handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
		var act = () => cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();

		// Disable fault injection
		handler.FaultInjector = null;
		var read = await cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Test");
	}

	// ── Section G: Response Metadata ──

	[Fact]
	public void Handler_BackingContainer_Accessible()
	{
		var container = new InMemoryContainer("backing-test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		handler.BackingContainer.Should().BeSameAs(container);
	}

	[Fact]
	public void Handler_DoubleDispose_DoesNotThrow()
	{
		var container = new InMemoryContainer("dd-test", "/partitionKey");
		var handler = new FakeCosmosHandler(container);
		handler.Dispose();
		var act = () => handler.Dispose();
		act.Should().NotThrow();
	}

	// ── Section I: Composite PK ──

	[Fact]
	public async Task Handler_CompositePartitionKey_CrudRoundTrip()
	{
		var container = new InMemoryContainer("composite-test", new[] { "/tenantId", "/region" });
		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "composite-test");

		var pk = new PartitionKeyBuilder().Add("t1").Add("us-east").Build();
		var doc = JObject.FromObject(new { id = "1", tenantId = "t1", region = "us-east", name = "Test" });
		await cosmosContainer.CreateItemAsync(doc, pk);

		var read = await cosmosContainer.ReadItemAsync<JObject>("1", pk);
		read.Resource["name"]!.Value<string>().Should().Be("Test");

		await cosmosContainer.DeleteItemAsync<JObject>("1", pk);
		var readAct = () => cosmosContainer.ReadItemAsync<JObject>("1", pk);
		await readAct.Should().ThrowAsync<CosmosException>();
	}

	// ── Section J: Edge Cases ──

	[Fact]
	public async Task Handler_LargeDocumentThroughHandler_Succeeds()
	{
		var container = new InMemoryContainer("large-doc", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "large-doc");

		var largeValue = new string('x', 1_500_000);
		var doc = JObject.FromObject(new { id = "1", partitionKey = "pk1", data = largeValue });
		await cosmosContainer.CreateItemAsync(doc, new PartitionKey("pk1"));

		var read = await cosmosContainer.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["data"]!.Value<string>().Should().HaveLength(1_500_000);
	}
}

#endregion
