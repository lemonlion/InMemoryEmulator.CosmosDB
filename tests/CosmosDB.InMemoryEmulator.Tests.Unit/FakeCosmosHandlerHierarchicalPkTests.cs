using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Hierarchical partition key tests using per-method inline container setups.
/// Change feed tests use InMemoryContainer-specific APIs (BackingContainer).
/// Tagged InMemoryOnly because all tests use inline FakeCosmosHandler stacks.
/// </summary>
public class FakeCosmosHandlerHierarchicalPkTests
{
	public class HierDoc
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("region")] public string Region { get; set; } = "";
		[JsonProperty("tenant")] public string Tenant { get; set; } = "";
		[JsonProperty("category")] public string Category { get; set; } = "";
		[JsonProperty("data")] public string Data { get; set; } = "";
		[JsonProperty("count")] public int Count { get; set; }
	}

	private static (CosmosClient client, Container container, InMemoryContainer backing) CreateTwoPathSetup()
	{
		var backing = new InMemoryContainer("hier2", new[] { "/region", "/tenant" });
		var handler = new FakeCosmosHandler(backing);
		var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});
		return (client, client.GetContainer("db", "hier2"), backing);
	}

	private static (CosmosClient client, Container container, InMemoryContainer backing) CreateThreePathSetup()
	{
		var backing = new InMemoryContainer("hier3", new[] { "/region", "/tenant", "/category" });
		var handler = new FakeCosmosHandler(backing);
		var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});
		return (client, client.GetContainer("db", "hier3"), backing);
	}

	private static PartitionKey PK2(string a, string b) =>
		new PartitionKeyBuilder().Add(a).Add(b).Build();

	private static PartitionKey PK3(string a, string b, string c) =>
		new PartitionKeyBuilder().Add(a).Add(b).Add(c).Build();

	private static PartitionKey Prefix1(string a) =>
		new PartitionKeyBuilder().Add(a).Build();

	private static PartitionKey Prefix2(string a, string b) =>
		new PartitionKeyBuilder().Add(a).Add(b).Build();

	private static async Task<List<T>> DrainIterator<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}

	#region 3.1 Collection Metadata Correctness

	[Fact]
	public async Task CollectionMetadata_SinglePathPK_ReturnsHash()
	{
		var backing = new InMemoryContainer("single", "/partitionKey");
		var handler = new FakeCosmosHandler(backing);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});
		var container = client.GetContainer("db", "single");

		// Verify single-path PK works (Hash kind) — basic CRUD proves metadata is correct
		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us" }, new PartitionKey("us"));
		var read = await container.ReadItemAsync<HierDoc>("1", new PartitionKey("us"));
		read.Resource.Id.Should().Be("1");
	}

	[Fact]
	public async Task CollectionMetadata_MultiPathPK_ReturnsMultiHash()
	{
		var (client, container, _) = CreateTwoPathSetup();

		// If metadata returns "Hash" instead of "MultiHash", this prefix query would throw ArgumentException
		await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1" }, PK2("us", "t1"));

		var prefixPk = Prefix1("us");
		var iterator = container.GetItemQueryIterator<HierDoc>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task CollectionMetadata_ThreePathPK_ReturnsMultiHash()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "eu", Tenant = "t1", Category = "cat1" }, PK3("eu", "t1", "cat1"));

		var prefixPk = Prefix1("eu");
		var iterator = container.GetItemQueryIterator<HierDoc>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(1);
	}

	#endregion

	#region 3.2 Prefix Partition Key Queries via FakeCosmosHandler

	[Fact]
	public async Task FakeHandler_TwoPathPK_PrefixQuery_OneComponent_ReturnsMatches()
	{
		var (client, container, _) = CreateTwoPathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1" }, PK2("us", "t1"));
		await container.CreateItemAsync(new HierDoc { Id = "2", Region = "us", Tenant = "t2" }, PK2("us", "t2"));
		await container.CreateItemAsync(new HierDoc { Id = "3", Region = "eu", Tenant = "t1" }, PK2("eu", "t1"));

		// Prefix PK queries via FakeCosmosHandler require a WHERE clause because
		// the SDK routes prefix queries using partition key ranges, not the PK header.
		var query = new QueryDefinition("SELECT * FROM c WHERE c.region = @r")
			.WithParameter("@r", "us");
		var iterator = container.GetItemQueryIterator<HierDoc>(
			query,
			requestOptions: new QueryRequestOptions { PartitionKey = Prefix1("us") });

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(new[] { "1", "2" });
	}

	[Fact]
	public async Task FakeHandler_TwoPathPK_FullQuery_ReturnsExactMatch()
	{
		var (client, container, _) = CreateTwoPathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1" }, PK2("us", "t1"));
		await container.CreateItemAsync(new HierDoc { Id = "2", Region = "us", Tenant = "t2" }, PK2("us", "t2"));

		var iterator = container.GetItemQueryIterator<HierDoc>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = PK2("us", "t1") });

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(1);
		results[0].Id.Should().Be("1");
	}

	[Fact]
	public async Task FakeHandler_ThreePathPK_PrefixQuery_OneComponent_ReturnsMatches()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1" }, PK3("us", "t1", "c1"));
		await container.CreateItemAsync(new HierDoc { Id = "2", Region = "us", Tenant = "t2", Category = "c2" }, PK3("us", "t2", "c2"));
		await container.CreateItemAsync(new HierDoc { Id = "3", Region = "eu", Tenant = "t1", Category = "c1" }, PK3("eu", "t1", "c1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.region = @r")
			.WithParameter("@r", "us");
		var iterator = container.GetItemQueryIterator<HierDoc>(
			query,
			requestOptions: new QueryRequestOptions { PartitionKey = Prefix1("us") });

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(new[] { "1", "2" });
	}

	[Fact]
	public async Task FakeHandler_ThreePathPK_PrefixQuery_TwoComponents_ReturnsMatches()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1" }, PK3("us", "t1", "c1"));
		await container.CreateItemAsync(new HierDoc { Id = "2", Region = "us", Tenant = "t1", Category = "c2" }, PK3("us", "t1", "c2"));
		await container.CreateItemAsync(new HierDoc { Id = "3", Region = "us", Tenant = "t2", Category = "c1" }, PK3("us", "t2", "c1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.region = @r AND c.tenant = @t")
			.WithParameter("@r", "us")
			.WithParameter("@t", "t1");
		var iterator = container.GetItemQueryIterator<HierDoc>(
			query,
			requestOptions: new QueryRequestOptions { PartitionKey = Prefix2("us", "t1") });

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(new[] { "1", "2" });
	}

	[Fact]
	public async Task FakeHandler_ThreePathPK_FullQuery_ReturnsExactMatch()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1" }, PK3("us", "t1", "c1"));
		await container.CreateItemAsync(new HierDoc { Id = "2", Region = "us", Tenant = "t1", Category = "c2" }, PK3("us", "t1", "c2"));

		var iterator = container.GetItemQueryIterator<HierDoc>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = PK3("us", "t1", "c1") });

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(1);
		results[0].Id.Should().Be("1");
	}

	[Fact]
	public async Task FakeHandler_PrefixQuery_NoMatches_ReturnsEmpty()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1" }, PK3("us", "t1", "c1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.region = @r")
			.WithParameter("@r", "nonexistent");
		var iterator = container.GetItemQueryIterator<HierDoc>(
			query,
			requestOptions: new QueryRequestOptions { PartitionKey = Prefix1("nonexistent") });

		var results = await DrainIterator(iterator);
		results.Should().BeEmpty();
	}

	#endregion

	#region 3.3 CRUD Operations via FakeCosmosHandler with Hierarchical PKs

	[Fact]
	public async Task FakeHandler_HierarchicalPK_Create_Works()
	{
		var (client, container, _) = CreateThreePathSetup();

		var response = await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1", Data = "hello" },
			PK3("us", "t1", "c1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Resource.Data.Should().Be("hello");
	}

	[Fact]
	public async Task FakeHandler_HierarchicalPK_Read_Works()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1", Data = "test" },
			PK3("us", "t1", "c1"));

		var response = await container.ReadItemAsync<HierDoc>("1", PK3("us", "t1", "c1"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Data.Should().Be("test");
	}

	[Fact]
	public async Task FakeHandler_HierarchicalPK_Upsert_Works()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1", Data = "v1" },
			PK3("us", "t1", "c1"));

		var response = await container.UpsertItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1", Data = "v2" },
			PK3("us", "t1", "c1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Data.Should().Be("v2");
	}

	[Fact]
	public async Task FakeHandler_HierarchicalPK_Delete_Works()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1" },
			PK3("us", "t1", "c1"));

		var response = await container.DeleteItemAsync<HierDoc>("1", PK3("us", "t1", "c1"));
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);

		var ex = await Assert.ThrowsAsync<CosmosException>(
			() => container.ReadItemAsync<HierDoc>("1", PK3("us", "t1", "c1")));
		ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task FakeHandler_HierarchicalPK_Patch_Works()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1", Data = "old" },
			PK3("us", "t1", "c1"));

		var response = await container.PatchItemAsync<HierDoc>("1", PK3("us", "t1", "c1"),
			new[] { PatchOperation.Set("/data", "patched") });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Data.Should().Be("patched");
	}

	[Fact]
	public async Task FakeHandler_HierarchicalPK_Replace_Works()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1", Data = "old" },
			PK3("us", "t1", "c1"));

		var response = await container.ReplaceItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1", Data = "replaced" },
			"1", PK3("us", "t1", "c1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Data.Should().Be("replaced");
	}

	#endregion

	#region 3.4 LINQ Queries via FakeCosmosHandler with Hierarchical PKs

	[Fact]
	public async Task FakeHandler_HierarchicalPK_LinqQuery_WithPartitionKey_Works()
	{
		var (client, container, _) = CreateThreePathSetup();

		await container.CreateItemAsync(
			new HierDoc { Id = "1", Region = "us", Tenant = "t1", Category = "c1", Data = "match" },
			PK3("us", "t1", "c1"));
		await container.CreateItemAsync(
			new HierDoc { Id = "2", Region = "eu", Tenant = "t1", Category = "c1", Data = "nomatch" },
			PK3("eu", "t1", "c1"));

		var queryable = container.GetItemLinqQueryable<HierDoc>(
			requestOptions: new QueryRequestOptions { PartitionKey = PK3("us", "t1", "c1") });
		var iterator = queryable.Where(d => d.Data == "match").ToFeedIterator();

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(1);
		results[0].Id.Should().Be("1");
	}

	[Fact]
	public async Task FakeHandler_HierarchicalPK_LinqQuery_CrossPartition_Works()
	{
		var (client, container, _) = CreateTwoPathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1", Data = "a" }, PK2("us", "t1"));
		await container.CreateItemAsync(new HierDoc { Id = "2", Region = "eu", Tenant = "t2", Data = "b" }, PK2("eu", "t2"));

		var queryable = container.GetItemLinqQueryable<HierDoc>();
		var iterator = queryable.OrderBy(d => d.Id).ToFeedIterator();

		var results = await DrainIterator(iterator);
		results.Should().HaveCount(2);
	}

	#endregion

	#region 3.5 Change Feed with Hierarchical PKs via FakeCosmosHandler

	[Fact]
	public async Task FakeHandler_HierarchicalPK_ChangeFeed_ByPartitionKey_Works()
	{
		var (_, container, backing) = CreateTwoPathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1" }, PK2("us", "t1"));
		await container.CreateItemAsync(new HierDoc { Id = "2", Region = "us", Tenant = "t2" }, PK2("us", "t2"));
		await container.CreateItemAsync(new HierDoc { Id = "3", Region = "eu", Tenant = "t1" }, PK2("eu", "t1"));

		// Read change feed for specific partition key via backing container
		// (change feed with hierarchical PKs through FakeCosmosHandler uses range-based routing)
		var changeFeedIterator = backing.GetChangeFeedIterator<HierDoc>(
			ChangeFeedStartFrom.Beginning(FeedRange.FromPartitionKey(PK2("us", "t1"))),
			ChangeFeedMode.Incremental);

		var changes = new List<HierDoc>();
		while (changeFeedIterator.HasMoreResults)
		{
			var response = await changeFeedIterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(response);
		}

		changes.Should().HaveCount(1);
		changes[0].Id.Should().Be("1");
	}

	[Fact]
	public async Task FakeHandler_HierarchicalPK_ChangeFeed_AllPartitions_Works()
	{
		var (_, container, backing) = CreateTwoPathSetup();

		await container.CreateItemAsync(new HierDoc { Id = "1", Region = "us", Tenant = "t1" }, PK2("us", "t1"));
		await container.CreateItemAsync(new HierDoc { Id = "2", Region = "eu", Tenant = "t2" }, PK2("eu", "t2"));

		// Read change feed via backing container
		var changeFeedIterator = backing.GetChangeFeedIterator<HierDoc>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);

		var allChanges = new List<HierDoc>();
		while (changeFeedIterator.HasMoreResults)
		{
			var response = await changeFeedIterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			allChanges.AddRange(response);
		}

		allChanges.Should().HaveCount(2);
		allChanges.Select(c => c.Id).Should().BeEquivalentTo(new[] { "1", "2" });
	}

	#endregion
}
