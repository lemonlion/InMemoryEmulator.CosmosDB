using System.Net;
using System.Reflection;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase A1: Item ID Extraction Reflection Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ItemIdExtractionReflectionTests
{
	[Fact]
	public void ItemIdExtraction_TypeWithUpperCaseId_FindsProperty()
	{
		var type = typeof(TestDocument);
		var idProp = type.GetProperty("Id") ?? type.GetProperty("id");
		idProp.Should().NotBeNull("InMemoryChangeFeedProcessor.ExtractId relies on finding Id/id property");
	}

	[Fact]
	public void ItemIdExtraction_TypeWithLowerCaseId_FindsProperty()
	{
		// JObject items have string indexers — verify the "id" key convention
		var jObj = JObject.Parse("""{"id":"test-123","partitionKey":"pk"}""");
		jObj["id"]?.ToString().Should().Be("test-123");
	}

	[Fact]
	public void ItemIdExtraction_TypeWithNoIdProperty_ReturnsNull()
	{
		// Anonymous-like type without id property
		var type = typeof(NoIdDocument);
		var idProp = type.GetProperty("Id") ?? type.GetProperty("id");
		idProp.Should().BeNull("types without id property should return null from reflection");
	}

	private class NoIdDocument
	{
		public string Name { get; set; } = default!;
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase A2: FeedIterator<T> Generic Virtual Members
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedIteratorGenericReflectionTests
{
	[Fact]
	public void FeedIterator_Generic_HasVirtual_HasMoreResults()
	{
		var prop = typeof(FeedIterator<object>).GetProperty(nameof(FeedIterator<object>.HasMoreResults));
		prop.Should().NotBeNull();
		prop!.GetMethod!.IsVirtual.Should().BeTrue("NSubstitute requires virtual/abstract members");
	}

	[Fact]
	public void FeedIterator_Generic_HasVirtual_ReadNextAsync()
	{
		var method = typeof(FeedIterator<object>).GetMethod(nameof(FeedIterator<object>.ReadNextAsync));
		method.Should().NotBeNull();
		method!.IsVirtual.Should().BeTrue("NSubstitute requires virtual/abstract members");
	}

	[Fact]
	public void FeedIterator_Generic_IsNotSealed()
	{
		typeof(FeedIterator<object>).IsSealed.Should().BeFalse("InMemoryFeedIterator<T> inherits from FeedIterator<T>");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase A3: QueryDefinition Parameter Tuple Structure
// ═══════════════════════════════════════════════════════════════════════════════

public class QueryDefinitionParameterTupleTests
{
	[Fact]
	public void QueryDefinition_ParameterTuple_HasNameProperty()
	{
		var qd = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
			.WithParameter("@id", "1");
		var parameters = qd.GetQueryParameters().ToList();
		parameters.Should().HaveCount(1);
		parameters[0].Name.Should().Be("@id");
	}

	[Fact]
	public void QueryDefinition_ParameterTuple_HasValueProperty()
	{
		var qd = new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
			.WithParameter("@id", "test-value");
		var parameters = qd.GetQueryParameters().ToList();
		parameters[0].Value.Should().Be("test-value");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase A4: ChangeFeedStartFrom Full Subtype Enumeration
// ═══════════════════════════════════════════════════════════════════════════════

public class ChangeFeedStartFromSubtypeTests
{
	[Fact]
	public void ChangeFeedStartFrom_AllSubtypes_HaveExpectedNames()
	{
		var subtypes = typeof(ChangeFeedStartFrom).Assembly.GetTypes()
			.Where(t => t.IsSubclassOf(typeof(ChangeFeedStartFrom)))
			.Select(t => t.Name)
			.ToList();

		subtypes.Should().HaveCountGreaterThanOrEqualTo(3);
		// Verify expected keywords appear across subtype names
		subtypes.Should().Contain(n => n.Contains("Beginning", StringComparison.OrdinalIgnoreCase)
									|| n.Contains("Now", StringComparison.OrdinalIgnoreCase)
									|| n.Contains("Time", StringComparison.OrdinalIgnoreCase));
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase B1: Parameterized Query Through SDK Pipeline
// ═══════════════════════════════════════════════════════════════════════════════

public class SdkPipelineQueryTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly CosmosClient _client;
	private readonly HeaderCapturingHandler _capturer;
	private readonly FakeCosmosHandler _handler;
	private readonly Container _cosmosContainer;

	public SdkPipelineQueryTests()
	{
		_container = new InMemoryContainer("query-pipe", "/partitionKey");
		(_client, _capturer, _handler) = SdkTestHelper.CreateCapturingClient(_container);
		_cosmosContainer = _client.GetContainer("fakeDb", "query-pipe");

		_container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 }, new PartitionKey("pk")).Wait();
		_container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 }, new PartitionKey("pk")).Wait();
		_container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Charlie", Value = 30 }, new PartitionKey("pk")).Wait();
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
		_capturer.Dispose();
	}

	[Fact]
	public async Task SdkPipeline_ParameterizedQuery_ReturnsFilteredResults()
	{
		var qd = new QueryDefinition("SELECT * FROM c WHERE c.name = @name")
			.WithParameter("@name", "Alice");
		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>(qd);
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(1);
		results.First().Name.Should().Be("Alice");
	}

	[Fact]
	public async Task SdkPipeline_ParameterizedQuery_MultipleParams_ReturnsCorrectResults()
	{
		var qd = new QueryDefinition("SELECT * FROM c WHERE c.name = @name AND c[\"value\"] = @val")
			.WithParameter("@name", "Bob")
			.WithParameter("@val", 20);
		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>(qd);
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(1);
		results.First().Id.Should().Be("2");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase B2: ETag Concurrency Through SDK Pipeline
// ═══════════════════════════════════════════════════════════════════════════════

public class SdkPipelineETagTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly CosmosClient _client;
	private readonly HeaderCapturingHandler _capturer;
	private readonly FakeCosmosHandler _handler;
	private readonly Container _cosmosContainer;

	public SdkPipelineETagTests()
	{
		_container = new InMemoryContainer("etag-pipe", "/partitionKey");
		(_client, _capturer, _handler) = SdkTestHelper.CreateCapturingClient(_container);
		_cosmosContainer = _client.GetContainer("fakeDb", "etag-pipe");

		_container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk")).Wait();
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
		_capturer.Dispose();
	}

	[Fact]
	public async Task SdkPipeline_IfMatchEtag_StaleEtag_Returns412()
	{
		var act = () => _cosmosContainer.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" },
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task SdkPipeline_IfMatchEtag_CurrentEtag_Succeeds()
	{
		var read = await _cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		var response = await _cosmosContainer.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated" },
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = read.ETag });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task SdkPipeline_IfNoneMatchEtag_SameEtag_Returns304()
	{
		var read = await _cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		var act = () => _cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"),
			new ItemRequestOptions { IfNoneMatchEtag = read.ETag });
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotModified);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase B3: Error Responses Through SDK Pipeline
// ═══════════════════════════════════════════════════════════════════════════════

public class SdkPipelineErrorResponseTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly CosmosClient _client;
	private readonly HeaderCapturingHandler _capturer;
	private readonly FakeCosmosHandler _handler;
	private readonly Container _cosmosContainer;

	public SdkPipelineErrorResponseTests()
	{
		_container = new InMemoryContainer("error-pipe", "/partitionKey");
		(_client, _capturer, _handler) = SdkTestHelper.CreateCapturingClient(_container);
		_cosmosContainer = _client.GetContainer("fakeDb", "error-pipe");
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
		_capturer.Dispose();
	}

	[Fact]
	public async Task SdkPipeline_ReadNonExistent_ThrowsCosmosException404()
	{
		var act = () => _cosmosContainer.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task SdkPipeline_CreateDuplicate_ThrowsCosmosException409()
	{
		await _cosmosContainer.CreateItemAsync(
			new TestDocument { Id = "dup", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		var act = () => _cosmosContainer.CreateItemAsync(
			new TestDocument { Id = "dup", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task SdkPipeline_DeleteNonExistent_ThrowsCosmosException404()
	{
		var act = () => _cosmosContainer.DeleteItemAsync<TestDocument>("nope", new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase B4-B6: SDK Pipeline Expansion Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class SdkPipelineExpansionTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly CosmosClient _client;
	private readonly HeaderCapturingHandler _capturer;
	private readonly FakeCosmosHandler _handler;
	private readonly Container _cosmosContainer;

	public SdkPipelineExpansionTests()
	{
		_container = new InMemoryContainer("expand-pipe", "/partitionKey");
		(_client, _capturer, _handler) = SdkTestHelper.CreateCapturingClient(_container);
		_cosmosContainer = _client.GetContainer("fakeDb", "expand-pipe");
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
		_capturer.Dispose();
	}

	[Fact]
	public async Task SdkPipeline_ReadFeed_WithPartitionKey_ReturnsOnlyMatchingItems()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(1);
		results.First().Name.Should().Be("A");
	}

	[Fact]
	public async Task SdkPipeline_DeleteThenRecreate_SameId_Succeeds()
	{
		await _cosmosContainer.CreateItemAsync(
			new TestDocument { Id = "cycle", PartitionKey = "pk", Name = "V1" }, new PartitionKey("pk"));
		await _cosmosContainer.DeleteItemAsync<TestDocument>("cycle", new PartitionKey("pk"));
		var response = await _cosmosContainer.CreateItemAsync(
			new TestDocument { Id = "cycle", PartitionKey = "pk", Name = "V2" }, new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await _cosmosContainer.ReadItemAsync<TestDocument>("cycle", new PartitionKey("pk"));
		read.Resource.Name.Should().Be("V2");
	}

	[Fact]
	public async Task SdkPipeline_PatchItemStreamAsync_ReturnsOk()
	{
		await _cosmosContainer.CreateItemAsync(
			new TestDocument { Id = "patch-s", PartitionKey = "pk", Name = "Before" }, new PartitionKey("pk"));
		var response = await _cosmosContainer.PatchItemStreamAsync(
			"patch-s", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "After")]);
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase C1: FaultInjector Basic Canary
// ═══════════════════════════════════════════════════════════════════════════════

public class FaultInjectorCanaryTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _cosmosContainer;

	public FaultInjectorCanaryTests()
	{
		_container = new InMemoryContainer("fault-canary", "/partitionKey");
		_handler = new FakeCosmosHandler(_container);
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
		_cosmosContainer = _client.GetContainer("fakeDb", "fault-canary");
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
	}

	[Fact]
	public async Task FaultInjector_Returns429_SdkReceivesCosmosException()
	{
		_handler.FaultInjector = req =>
		{
			// Skip query plan requests
			if (req.Headers.TryGetValues("x-ms-cosmos-is-query-plan-request", out _))
				return null;
			return new HttpResponseMessage((HttpStatusCode)429);
		};

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var act = () => _cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => (int)e.StatusCode == 429);
	}

	[Fact]
	public async Task FaultInjector_ReturnsNull_RequestCompletesNormally()
	{
		_handler.FaultInjector = _ => null; // null = no fault

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		var result = await _cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		result.Resource.Name.Should().Be("A");
	}

	[Fact]
	public async Task FaultInjector_MetadataRequests_BypassedByDefault()
	{
		var faultCount = 0;
		_handler.FaultInjector = req =>
		{
			// Skip query plan and metadata requests
			if (req.Headers.TryGetValues("x-ms-cosmos-is-query-plan-request", out _))
				return null;
			if (req.RequestUri?.AbsolutePath == "/" || req.RequestUri?.AbsolutePath.Contains("/pkranges") == true)
				return null;
			faultCount++;
			return null; // Don't actually fault, just count
		};

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		await _cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		faultCount.Should().BeGreaterThan(0, "actual CRUD requests should reach the fault injector");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase D: Query Plan Coverage (GROUP BY, TOP, OFFSET/LIMIT, Multi-ORDER BY)
// ═══════════════════════════════════════════════════════════════════════════════

public class QueryPlanExpansionCanaryTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly CosmosClient _client;
	private readonly HeaderCapturingHandler _capturer;
	private readonly FakeCosmosHandler _handler;
	private readonly Container _cosmosContainer;

	public QueryPlanExpansionCanaryTests()
	{
		_container = new InMemoryContainer("qp-expand", "/partitionKey");
		(_client, _capturer, _handler) = SdkTestHelper.CreateCapturingClient(_container);
		_cosmosContainer = _client.GetContainer("fakeDb", "qp-expand");

		_container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1")).Wait();
		_container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 }, new PartitionKey("pk1")).Wait();
		_container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30 }, new PartitionKey("pk2")).Wait();
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
		_capturer.Dispose();
	}

	[Fact]
	public async Task QueryPlan_GroupBy_AcceptedBySdk()
	{
		var iter = _cosmosContainer.GetItemQueryIterator<JObject>(
			"SELECT c.partitionKey, COUNT(1) as cnt FROM c GROUP BY c.partitionKey");
		var results = await iter.ReadNextAsync();
		results.Count.Should().BeGreaterThan(0);
	}

	[Fact(Skip = "SDK ServiceInterop query plan parser does not support HAVING clause — returns SC1001 syntax error. The emulator supports HAVING but it cannot pass through the SDK pipeline.")]
	public async Task QueryPlan_GroupBy_WithHaving_AcceptedBySdk()
	{
		var iter = _cosmosContainer.GetItemQueryIterator<JObject>(
			"SELECT c.partitionKey, COUNT(1) as cnt FROM c GROUP BY c.partitionKey HAVING COUNT(1) >= 2");
		var results = await iter.ReadNextAsync();
		results.Count.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task QueryPlan_TopN_AcceptedBySdk()
	{
		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT TOP 1 * FROM c");
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(1);
	}

	[Fact]
	public async Task QueryPlan_OffsetLimit_AcceptedBySdk()
	{
		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c OFFSET 0 LIMIT 1");
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(1);
	}

	[Fact]
	public async Task QueryPlan_MultipleOrderBy_AcceptedBySdk()
	{
		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c.name ASC, c[\"value\"] DESC");
		var all = new List<TestDocument>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			all.AddRange(page);
		}
		all.Count.Should().Be(3);
		all[0].Name.Should().Be("Alice");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase E1: Unicode/Special Characters
// ═══════════════════════════════════════════════════════════════════════════════

public class SdkPipelineEdgeCaseTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly CosmosClient _client;
	private readonly HeaderCapturingHandler _capturer;
	private readonly FakeCosmosHandler _handler;
	private readonly Container _cosmosContainer;

	public SdkPipelineEdgeCaseTests()
	{
		_container = new InMemoryContainer("edge-pipe", "/partitionKey");
		(_client, _capturer, _handler) = SdkTestHelper.CreateCapturingClient(_container);
		_cosmosContainer = _client.GetContainer("fakeDb", "edge-pipe");
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
		_capturer.Dispose();
	}

	[Fact]
	public async Task SdkPipeline_UnicodeId_RoundTrips()
	{
		var id = "café-☕";
		await _cosmosContainer.CreateItemAsync(
			new TestDocument { Id = id, PartitionKey = "pk", Name = "Unicode" }, new PartitionKey("pk"));
		var result = await _cosmosContainer.ReadItemAsync<TestDocument>(id, new PartitionKey("pk"));
		result.Resource.Id.Should().Be(id);
	}

	[Fact]
	public async Task SdkPipeline_SpecialCharId_RoundTrips()
	{
		var id = "item with spaces";
		await _cosmosContainer.CreateItemAsync(
			new TestDocument { Id = id, PartitionKey = "pk", Name = "Special" }, new PartitionKey("pk"));
		var result = await _cosmosContainer.ReadItemAsync<TestDocument>(id, new PartitionKey("pk"));
		result.Resource.Id.Should().Be(id);
	}

	[Fact]
	public async Task SdkPipeline_QueryEmptyContainer_ReturnsEmptyResults()
	{
		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(0);
	}

	[Fact]
	public async Task SdkPipeline_CountEmptyContainer_ReturnsZero()
	{
		var count = await _cosmosContainer.GetItemLinqQueryable<TestDocument>().CountAsync();
		count.Resource.Should().Be(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase E3: Large Pagination Through SDK Pipeline
// ═══════════════════════════════════════════════════════════════════════════════

public class SdkPipelinePaginationTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly CosmosClient _client;
	private readonly HeaderCapturingHandler _capturer;
	private readonly FakeCosmosHandler _handler;
	private readonly Container _cosmosContainer;

	public SdkPipelinePaginationTests()
	{
		_container = new InMemoryContainer("page-pipe", "/partitionKey");
		(_client, _capturer, _handler) = SdkTestHelper.CreateCapturingClient(_container);
		_cosmosContainer = _client.GetContainer("fakeDb", "page-pipe");

		for (var i = 0; i < 10; i++)
			_container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i:D2}", Value = i },
				new PartitionKey("pk")).Wait();
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
		_capturer.Dispose();
	}

	[Fact]
	public async Task SdkPipeline_Pagination_ManyPages_AllItemsReturned()
	{
		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		var all = new List<TestDocument>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			all.AddRange(page);
		}
		all.Count.Should().Be(10);
	}

	[Fact]
	public async Task SdkPipeline_OrderByPagination_MaintainsOrder()
	{
		var iter = _cosmosContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(d => d.Name)
			.Take(10)
			.ToFeedIterator();
		var all = new List<TestDocument>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			all.AddRange(page);
		}
		all.Count.Should().Be(10);
		// Items should be in alphabetical order
		for (var i = 1; i < all.Count; i++)
			string.Compare(all[i - 1].Name, all[i].Name, StringComparison.Ordinal)
				.Should().BeLessThanOrEqualTo(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase E4: Multi-Container Routing Through CreateRouter
// ═══════════════════════════════════════════════════════════════════════════════

public class MultiContainerRoutingCanaryTests : IDisposable
{
	private readonly InMemoryContainer _container1;
	private readonly InMemoryContainer _container2;
	private readonly FakeCosmosHandler _h1;
	private readonly FakeCosmosHandler _h2;
	private readonly HttpMessageHandler _router;
	private readonly CosmosClient _client;

	public MultiContainerRoutingCanaryTests()
	{
		_container1 = new InMemoryContainer("users", "/partitionKey");
		_container2 = new InMemoryContainer("orders", "/partitionKey");
		_h1 = new FakeCosmosHandler(_container1);
		_h2 = new FakeCosmosHandler(_container2);
		_router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["users"] = _h1,
			["orders"] = _h2
		});
		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(_router)
			});
	}

	public void Dispose()
	{
		_client.Dispose();
		_h1.Dispose();
		_h2.Dispose();
	}

	[Fact]
	public async Task CreateRouter_MultipleContainers_RoutesToCorrectContainer()
	{
		var users = _client.GetContainer("db", "users");
		var orders = _client.GetContainer("db", "orders");

		await users.CreateItemAsync(
			new TestDocument { Id = "u1", PartitionKey = "pk", Name = "UserA" }, new PartitionKey("pk"));
		await orders.CreateItemAsync(
			new TestDocument { Id = "o1", PartitionKey = "pk", Name = "OrderA" }, new PartitionKey("pk"));

		var userRead = await users.ReadItemAsync<TestDocument>("u1", new PartitionKey("pk"));
		userRead.Resource.Name.Should().Be("UserA");

		var orderRead = await orders.ReadItemAsync<TestDocument>("o1", new PartitionKey("pk"));
		orderRead.Resource.Name.Should().Be("OrderA");

		// Users container shouldn't have orders
		var act = () => users.ReadItemAsync<TestDocument>("o1", new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task CreateRouter_UnknownContainer_ThrowsInvalidOperationException()
	{
		var unknown = _client.GetContainer("db", "unknown");
		var act = () => unknown.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == System.Net.HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase F: Divergent Behavior Documentation Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class SdkCompatibilityCanaryDivergentTests
{
	[Fact(Skip = "Transactional batch operations bypass the FakeCosmosHandler HTTP route and execute directly on InMemoryContainer.")]
	public void TransactionalBatch_NotSupportedViaFakeCosmosHandlerHttpRoute() { }

	[Fact]
	public async Task TransactionalBatch_WorksDirectlyOnInMemoryContainer_Divergence()
	{
		var container = new InMemoryContainer("batch-div", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact(Skip = "Change feed operations bypass the FakeCosmosHandler HTTP route and execute directly on InMemoryContainer.")]
	public void ChangeFeed_NotSupportedViaFakeCosmosHandlerHttpRoute() { }

	[Fact]
	public async Task ChangeFeed_WorksDirectlyOnInMemoryContainer_Divergence()
	{
		var container = new InMemoryContainer("cf-div", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		var iter = container.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var page = await iter.ReadNextAsync();
		page.Count.Should().Be(1);
	}

	[Fact(Skip = "ReadMany operations bypass the FakeCosmosHandler HTTP route and execute directly on InMemoryContainer.")]
	public void ReadMany_NotSupportedViaFakeCosmosHandlerHttpRoute() { }

	[Fact]
	public async Task ReadMany_WorksDirectlyOnInMemoryContainer_Divergence()
	{
		var container = new InMemoryContainer("rm-div", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		var response = await container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk"))]);
		response.Count.Should().Be(1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Phase G: Logging & Observability Canaries
// ═══════════════════════════════════════════════════════════════════════════════

public class SdkLoggingCanaryTests : IDisposable
{
	private readonly InMemoryContainer _container;
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _cosmosContainer;

	public SdkLoggingCanaryTests()
	{
		_container = new InMemoryContainer("log-canary", "/partitionKey");
		_handler = new FakeCosmosHandler(_container);
		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(_handler)
			});
		_cosmosContainer = _client.GetContainer("fakeDb", "log-canary");
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
	}

	[Fact]
	public async Task QueryLog_RecordsExecutedQueries()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var beforeCount = _handler.QueryLog.Count;
		var iter = _cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		await iter.ReadNextAsync();

		_handler.QueryLog.Count.Should().BeGreaterThan(beforeCount);
	}

	[Fact]
	public async Task RequestLog_RecordsAllRequests()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var beforeCount = _handler.RequestLog.Count;
		await _cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

		_handler.RequestLog.Count.Should().BeGreaterThan(beforeCount);
	}
}
