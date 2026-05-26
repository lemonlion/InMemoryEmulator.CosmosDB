using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Section A: FakeCosmosHandler Pagination + Q: ReadFeed
// ══════════════════════════════════════════════════════════════════════════════

public class RealToFeedIteratorPaginationDeepDiveTests : IAsyncLifetime
{
	private readonly InMemoryContainer _backing = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _container;

	public RealToFeedIteratorPaginationDeepDiveTests()
	{
		_handler = new FakeCosmosHandler(_backing);
		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_container = _client.GetContainer("fakeDb", "fakeContainer");
	}

	public async ValueTask InitializeAsync()
	{
		for (int i = 1; i <= 5; i++)
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i * 10 },
				new PartitionKey("pk1"));
	}

	public ValueTask DisposeAsync()
	{
		_client.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task Pagination_WithMaxItemCount_ReturnsMultiplePages()
	{
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		var allItems = new List<TestDocument>();
		int pages = 0;
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
			pages++;
		}
		allItems.Should().HaveCount(5);
		pages.Should().BeGreaterThan(1);
	}

	[Fact]
	public async Task Pagination_WithMaxItemCount1_ReturnsOneItemPerPage()
	{
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 1 });
		var pagesSizes = new List<int>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			pagesSizes.Add(page.Count);
		}
		pagesSizes.Should().AllSatisfy(s => s.Should().BeLessThanOrEqualTo(1));
		pagesSizes.Sum().Should().Be(5);
	}

	[Fact]
	public async Task Pagination_ContinuationToken_AllowsDrainToComplete()
	{
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		var allItems = new List<TestDocument>();
		string? lastToken = null;
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
			lastToken = page.ContinuationToken;
		}
		allItems.Should().HaveCount(5);
		lastToken.Should().BeNull();
	}

	[Fact]
	public async Task Pagination_WithOrderBy_MaintainsOrderAcrossPages()
	{
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c[\"value\"] ASC",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		var allValues = new List<int>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allValues.AddRange(page.Select(d => d.Value));
		}
		allValues.Should().BeInAscendingOrder();
	}

	[Fact]
	public async Task Pagination_WithWhereFilter_PaginatesFilteredResults()
	{
		// Add more items so we have enough to filter
		await _container.CreateItemAsync(
			new TestDocument { Id = "6", PartitionKey = "pk1", Name = "Item6", Value = 60 },
			new PartitionKey("pk1"));
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c WHERE c[\"value\"] > 20",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		var allItems = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
		}
		allItems.Should().HaveCount(4); // 30, 40, 50, 60
	}

	[Fact]
	public async Task ReadFeed_ViaGetItemQueryIterator_WithoutSql_ReturnsAll()
	{
		var iterator = _container.GetItemQueryIterator<TestDocument>();
		var allItems = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
		}
		allItems.Should().HaveCount(5);
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Section B: SQL Query via GetItemQueryIterator
// ══════════════════════════════════════════════════════════════════════════════

public class RealToFeedIteratorSqlQueryDeepDiveTests : IAsyncLifetime
{
	private readonly InMemoryContainer _backing = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _container;

	public RealToFeedIteratorSqlQueryDeepDiveTests()
	{
		_handler = new FakeCosmosHandler(_backing);
		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_container = _client.GetContainer("fakeDb", "fakeContainer");
	}

	public async ValueTask InitializeAsync()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30 },
			new PartitionKey("pk2"));
	}

	public ValueTask DisposeAsync()
	{
		_client.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task SqlQuery_WithParameterizedQuery_ReturnsCorrectResults()
	{
		var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @name")
			.WithParameter("@name", "Alice");
		var iterator = _container.GetItemQueryIterator<TestDocument>(query);
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Alice");
	}

	[Fact]
	public async Task SqlQuery_WithOrderBy_ReturnsOrderedResults()
	{
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c[\"value\"] ASC");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Select(r => r.Value).Should().BeInAscendingOrder();
	}

	[Fact]
	public async Task SqlQuery_WithTopN_LimitsResults()
	{
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT TOP 2 * FROM c ORDER BY c[\"value\"] ASC");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task SqlQuery_WithGroupByCount_ReturnsGroupedResults()
	{
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey");
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task SqlQuery_EmptyResult_ReturnsEmptyIterator()
	{
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c WHERE c.name = 'Nobody'");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().BeEmpty();
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Section C: Stream API
// ══════════════════════════════════════════════════════════════════════════════

public class RealToFeedIteratorStreamDeepDiveTests : IAsyncLifetime
{
	private readonly InMemoryContainer _backing = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _container;

	public RealToFeedIteratorStreamDeepDiveTests()
	{
		_handler = new FakeCosmosHandler(_backing);
		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_container = _client.GetContainer("fakeDb", "fakeContainer");
	}

	public async ValueTask InitializeAsync()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));
	}

	public ValueTask DisposeAsync()
	{
		_client.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task StreamQuery_WithWhereFilter_ReturnsFilteredJson()
	{
		var iterator = _container.GetItemQueryStreamIterator(
			"SELECT * FROM c WHERE c.name = 'Alice'");
		var response = await iterator.ReadNextAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(1);
	}

	[Fact]
	public async Task StreamQuery_WithOrderBy_ReturnsOrderedJson()
	{
		var iterator = _container.GetItemQueryStreamIterator(
			"SELECT * FROM c ORDER BY c[\"value\"] DESC");
		var response = await iterator.ReadNextAsync();
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		var docs = (JArray)body["Documents"]!;
		docs[0]!["value"]!.Value<int>().Should().BeGreaterThan(docs[1]!["value"]!.Value<int>());
	}

	[Fact]
	public async Task StreamQuery_EmptyContainer_ReturnsEmptyDocuments()
	{
		var emptyBacking = new InMemoryContainer("empty", "/partitionKey");
		using var handler = new FakeCosmosHandler(emptyBacking);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		var container = client.GetContainer("db", "c");
		var iterator = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await iterator.ReadNextAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().BeEmpty();
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Section D-F, O-P: LINQ Deep Dive Additions
// ══════════════════════════════════════════════════════════════════════════════

public class RealToFeedIteratorLinqDeepDiveV2Tests : IAsyncLifetime
{
	private readonly InMemoryContainer _backing = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _container;

	public RealToFeedIteratorLinqDeepDiveV2Tests()
	{
		_handler = new FakeCosmosHandler(_backing);
		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_container = _client.GetContainer("fakeDb", "fakeContainer");
	}

	public async ValueTask InitializeAsync()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Alice", Value = 30, IsActive = true },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "4", PartitionKey = "pk2", Name = "Charlie", Value = 40, IsActive = false },
			new PartitionKey("pk2"));
	}

	public ValueTask DisposeAsync()
	{
		_client.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	// -- Section D: DISTINCT --

	[Fact]
	public async Task Distinct_OnStringField_ReturnsUniqueStrings()
	{
		var queryable = _container.GetItemLinqQueryable<TestDocument>()
			.Select(d => d.Name).Distinct();
		var iterator = queryable.ToFeedIterator();
		var results = new List<string>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().HaveCount(3); // Alice, Bob, Charlie
	}

	[Fact]
	public async Task Distinct_OnBoolField_ReturnsTrueAndFalse()
	{
		var queryable = _container.GetItemLinqQueryable<TestDocument>()
			.Select(d => d.IsActive).Distinct();
		var iterator = queryable.ToFeedIterator();
		var results = new List<bool>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().HaveCount(2);
		results.Should().Contain(true);
		results.Should().Contain(false);
	}

	// -- Section E: ORDER BY --

	[Fact]
	public async Task OrderByDescending_ThenByAscending_MixedDirections()
	{
		var queryable = _container.GetItemLinqQueryable<TestDocument>()
			.OrderByDescending(d => d.Name).ThenBy(d => d.Value);
		var iterator = queryable.ToFeedIterator();
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().HaveCount(4);
		// Charlie(40), Bob(20), Alice(10), Alice(30) — desc name, asc value
		results[0].Name.Should().Be("Charlie");
	}

	[Fact]
	public async Task OrderBy_EmptyContainer_ReturnsEmpty()
	{
		var emptyBacking = new InMemoryContainer("empty", "/partitionKey");
		using var handler = new FakeCosmosHandler(emptyBacking);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		var container = client.GetContainer("db", "c");
		var queryable = container.GetItemLinqQueryable<TestDocument>().OrderBy(d => d.Value);
		var iterator = queryable.ToFeedIterator();
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task OrderBy_SingleItem_ReturnsThatItem()
	{
		var singleBacking = new InMemoryContainer("single", "/partitionKey");
		using var handler = new FakeCosmosHandler(singleBacking);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		var container = client.GetContainer("db", "c");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Only", Value = 42 },
			new PartitionKey("pk1"));
		var queryable = container.GetItemLinqQueryable<TestDocument>().OrderBy(d => d.Value);
		var iterator = queryable.ToFeedIterator();
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Only");
	}

	// -- Section O: Offset/Skip, Aggregates --

	[Fact]
	public async Task OffsetWithLimit_ViaLinq_SkipsAndTakes()
	{
		var queryable = _container.GetItemLinqQueryable<TestDocument>()
			.OrderBy(d => d.Name).Skip(1).Take(2);
		var iterator = queryable.ToFeedIterator();
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task CountAsync_ReturnsCorrectValue()
	{
		var count = await _container.GetItemLinqQueryable<TestDocument>().CountAsync();
		count.Resource.Should().Be(4);
	}

	[Fact]
	public async Task SumAsync_WithFilter_ReturnsSumOfFilteredItems()
	{
		var queryable = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.IsActive);
		var sum = await queryable.Select(d => d.Value).SumAsync();
		sum.Resource.Should().Be(40); // 10 + 30
	}

	// -- Section P: CancellationToken --

	[Fact]
	public async Task ReadNextAsync_WithCancelledToken_ThrowsOrCompletes()
	{
		var cts = new CancellationTokenSource();
		cts.Cancel();
		var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var act = () => iterator.ReadNextAsync(cts.Token);
		// May throw TaskCanceledException or OperationCanceledException
		await act.Should().ThrowAsync<OperationCanceledException>();
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Section G-K, N: Handler Route Deep Dive Additions
// ══════════════════════════════════════════════════════════════════════════════

public class RealToFeedIteratorHandlerDeepDiveV2Tests : IAsyncLifetime
{
	private readonly InMemoryContainer _backing = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _container;

	public RealToFeedIteratorHandlerDeepDiveV2Tests()
	{
		_handler = new FakeCosmosHandler(_backing);
		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_container = _client.GetContainer("fakeDb", "fakeContainer");
	}

	public async ValueTask InitializeAsync()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));
	}

	public ValueTask DisposeAsync()
	{
		_client.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	// -- Section G: RequestLog & QueryLog --

	[Fact]
	public async Task QueryLog_AfterLinqQuery_ContainsGeneratedSql()
	{
		var queryable = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "Alice");
		var iterator = queryable.ToFeedIterator();
		while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		_handler.QueryLog.Should().NotBeEmpty();
		_handler.QueryLog.Should().Contain(q => q.Contains("Alice"));
	}

	[Fact]
	public async Task RequestLog_AfterCrud_ContainsHttpMethods()
	{
		// Create already happened in InitializeAsync (POST)
		// Read
		await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		// Delete
		await _container.DeleteItemAsync<TestDocument>("2", new PartitionKey("pk1"));

		_handler.RequestLog.Should().Contain(e => e.Contains("POST"));
		_handler.RequestLog.Should().Contain(e => e.Contains("GET"));
		_handler.RequestLog.Should().Contain(e => e.Contains("DELETE"));
	}

	[Fact]
	public async Task QueryLog_AfterMultipleQueries_RecordsAll()
	{
		var initialCount = _handler.QueryLog.Count;
		var q1 = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'Alice'");
		while (q1.HasMoreResults) await q1.ReadNextAsync();
		var q2 = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'Bob'");
		while (q2.HasMoreResults) await q2.ReadNextAsync();
		_handler.QueryLog.Count.Should().BeGreaterThanOrEqualTo(initialCount + 2);
	}

	// -- Section H: Fault Injection --

	[Fact]
	public async Task FaultInjector_ConditionalByRequest_SelectivelyFaults()
	{
		int callCount = 0;
		_handler.FaultInjector = req =>
		{
			// Let query plan and metadata through
			if (req.Headers.TryGetValues("x-ms-cosmos-is-query-plan-request", out _))
				return null;
			callCount++;
			if (callCount <= 1) return null; // let first data call through
			return new HttpResponseMessage((HttpStatusCode)503) { Content = new StringContent("{}") };
		};
		// First query succeeds
		var q1 = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (q1.HasMoreResults) await q1.ReadNextAsync();

		// Second query gets faulted
		var q2 = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var act = async () => { while (q2.HasMoreResults) await q2.ReadNextAsync(); };
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjector_OnCrudNotQuery_FaultsCreate()
	{
		_handler.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)503)
		{ Content = new StringContent("{}") };
		var act = () => _container.CreateItemAsync(
			new TestDocument { Id = "99", PartitionKey = "pk1", Name = "Fail" },
			new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
		_handler.FaultInjector = null;
		// After removing fault, create succeeds
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "99", PartitionKey = "pk1", Name = "Success" },
			new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	// -- Section I: Patch --

	[Fact]
	public async Task Patch_MultipleOps_ViaRealSdk_AllApplied()
	{
		var response = await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			[
				PatchOperation.Set("/name", "Patched"),
				PatchOperation.Increment("/value", 5),
				PatchOperation.Add("/tags/0", "new-tag")
			]);
		response.Resource.Name.Should().Be("Patched");
		response.Resource.Value.Should().Be(15);
		response.Resource.Tags.Should().Contain("new-tag");
	}

	[Fact]
	public async Task Patch_WithConditionalFilter_Matching_Succeeds()
	{
		var response = await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Filtered")],
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.value = 10" });
		response.Resource.Name.Should().Be("Filtered");
	}

	[Fact]
	public async Task Patch_WithConditionalFilter_NonMatching_Fails()
	{
		var act = () => _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "NoMatch")],
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.value = 999" });
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	// -- Section J: ETag Concurrency --

	[Fact]
	public async Task Upsert_WithIfMatchEtag_Succeeds()
	{
		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var etag = read.ETag;
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Upsert_WithStaleEtag_Fails()
	{
		var act = () => _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Stale" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Delete_WithIfMatchEtag_Succeeds()
	{
		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var etag = read.ETag;
		var response = await _container.DeleteItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task Delete_WithStaleEtag_Fails()
	{
		var act = () => _container.DeleteItemAsync<TestDocument>(
			"2", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	// -- Section K: BackingContainer --

	[Fact]
	public void BackingContainer_IsSameAsConstructorContainer()
	{
		_handler.BackingContainer.Should().BeSameAs(_backing);
	}

	[Fact]
	public async Task Upsert_ViaHandler_ReadViaBackingContainer()
	{
		await _container.UpsertItemAsync(
			new TestDocument { Id = "99", PartitionKey = "pk1", Name = "ViaHandler" },
			new PartitionKey("pk1"));
		var direct = await _backing.ReadItemAsync<TestDocument>("99", new PartitionKey("pk1"));
		direct.Resource.Name.Should().Be("ViaHandler");
	}

	[Fact]
	public async Task Patch_ViaHandler_ReadViaBackingContainer()
	{
		await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "PatchedViaHandler")]);
		var direct = await _backing.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		direct.Resource.Name.Should().Be("PatchedViaHandler");
	}

	// -- Section N: Concurrent Operations --

	[Fact]
	public async Task ParallelIterators_IndependentQueries_BothComplete()
	{
		var t1 = Task.Run(async () =>
		{
			var q = _container.GetItemQueryIterator<TestDocument>(
				"SELECT * FROM c WHERE c.name = 'Alice'");
			var r = new List<TestDocument>();
			while (q.HasMoreResults) r.AddRange(await q.ReadNextAsync());
			return r;
		});
		var t2 = Task.Run(async () =>
		{
			var q = _container.GetItemQueryIterator<TestDocument>(
				"SELECT * FROM c WHERE c.name = 'Bob'");
			var r = new List<TestDocument>();
			while (q.HasMoreResults) r.AddRange(await q.ReadNextAsync());
			return r;
		});
		var results = await Task.WhenAll(t1, t2);
		results[0].Should().HaveCount(1);
		results[1].Should().HaveCount(1);
	}

	[Fact]
	public async Task ConcurrentSeedAndQuery_DoesNotThrow()
	{
		var seedTask = Task.Run(async () =>
		{
			for (int i = 100; i < 110; i++)
			{
				await _container.UpsertItemAsync(
					new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Concurrent{i}" },
					new PartitionKey("pk1"));
			}
		});
		var queryTask = Task.Run(async () =>
		{
			for (int j = 0; j < 5; j++)
			{
				var q = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
				while (q.HasMoreResults) await q.ReadNextAsync();
				await Task.Delay(10);
			}
		});
		await Task.WhenAll(seedTask, queryTask);
		// No exception thrown = success
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Section L: InMemoryStreamFeedIterator Direct (internal, via InMemoryContainer)
// ══════════════════════════════════════════════════════════════════════════════

public class InMemoryStreamFeedIteratorDirectDeepDiveTests
{
	[Fact]
	public async Task StreamFeedIterator_HasMoreResults_FalseAfterRead()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"));
		var iterator = container.GetItemQueryStreamIterator("SELECT * FROM c");
		iterator.HasMoreResults.Should().BeTrue();
		await iterator.ReadNextAsync();
		iterator.HasMoreResults.Should().BeFalse();
	}

	[Fact]
	public async Task StreamFeedIterator_EmptyItems_ReturnsValidJson()
	{
		var container = new InMemoryContainer("test", "/pk");
		var iterator = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await iterator.ReadNextAsync();
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		body["Documents"].Should().NotBeNull();
		((JArray)body["Documents"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task StreamFeedIterator_ResponseHeaders_ContainMetadata()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"));
		var iterator = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await iterator.ReadNextAsync();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Section M: InMemoryFeedIterator Additional Edge Cases
// ══════════════════════════════════════════════════════════════════════════════

public class InMemoryFeedIteratorEdgeCaseDeepDiveTests
{
	[Fact]
	public async Task FeedIterator_MaxItemCount_ExactMultiple_NoExtraPage()
	{
		var items = Enumerable.Range(1, 6).ToList();
		var iterator = new InMemoryFeedIterator<int>(items, maxItemCount: 3);
		int pages = 0;
		var all = new List<int>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			all.AddRange(page);
			pages++;
		}
		all.Should().HaveCount(6);
		pages.Should().Be(2);
	}

	[Fact]
	public async Task FeedIterator_SingleItem_OnePage()
	{
		var iterator = new InMemoryFeedIterator<int>([42]);
		var page = await iterator.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First().Should().Be(42);
		iterator.HasMoreResults.Should().BeFalse();
	}

	[Fact]
	public async Task FeedIterator_FeedResponse_StatusCode_IsOk()
	{
		var iterator = new InMemoryFeedIterator<int>([1, 2, 3]);
		var page = await iterator.ReadNextAsync();
		page.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task FeedIterator_FeedResponse_RequestCharge_IsOne()
	{
		var iterator = new InMemoryFeedIterator<int>([1, 2, 3]);
		var page = await iterator.ReadNextAsync();
		page.RequestCharge.Should().Be(1.0);
	}

	[Fact]
	public async Task FeedIterator_FeedResponse_ActivityId_IsValidGuid()
	{
		var iterator = new InMemoryFeedIterator<int>([1]);
		var page = await iterator.ReadNextAsync();
		Guid.TryParse(page.ActivityId, out _).Should().BeTrue();
	}

	[Fact]
	public async Task FeedIterator_FeedResponse_Diagnostics_NotNull()
	{
		var iterator = new InMemoryFeedIterator<int>([1]);
		var page = await iterator.ReadNextAsync();
		page.Diagnostics.Should().NotBeNull();
	}
}
