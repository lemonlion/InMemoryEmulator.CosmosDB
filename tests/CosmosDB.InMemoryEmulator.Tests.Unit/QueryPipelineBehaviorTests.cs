using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Query Pipeline Behavior — regression guard for deferred-enumeration refactor
//  These tests verify exact behavior of the query pipeline so that internal
//  changes (List<string> → IEnumerable<string>, removing intermediate .ToList())
//  do not alter observable results.
// ═══════════════════════════════════════════════════════════════════════════

public class QueryPipelineBehaviorTests
{
	private static async Task<InMemoryContainer> CreateSeededContainer(int count = 10)
	{
		var container = new InMemoryContainer("qpb-test", "/partitionKey");
		for (var i = 1; i <= count; i++)
		{
			await container.CreateItemAsync(
				new TestDocument
				{
					Id = i.ToString(),
					PartitionKey = $"pk-{i % 3}", // 3 partitions: pk-0, pk-1, pk-2
					Name = $"Item{i}",
					Value = i * 10,
					IsActive = i % 2 == 0, // even items are active
					Tags = new[] { $"tag-{i % 4}", $"tag-{i % 5}" }
				},
				new PartitionKey($"pk-{i % 3}"));
		}
		return container;
	}

	// ── Basic WHERE ──────────────────────────────────────────────────────

	[Fact]
	public async Task WhereFilter_ReturnsOnlyMatchingItems()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.isActive = true"));

		var results = await DrainIterator(iterator);

		results.Should().OnlyContain(d => d.IsActive);
		results.Should().HaveCount(5); // items 2,4,6,8,10
	}

	// ── WHERE + TOP (no ORDER BY) — key lazy optimization path ──────

	[Fact]
	public async Task WhereWithTop_ReturnsCorrectCount()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT TOP 3 * FROM c WHERE c.value > 30"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(3);
		results.Should().OnlyContain(d => d.Value > 30);
	}

	// ── WHERE + ORDER BY ────────────────────────────────────────────────

	[Fact]
	public async Task WhereWithOrderBy_ReturnsSortedFilteredResults()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.value >= 50 ORDER BY c.value DESC"));

		var results = await DrainIterator(iterator);

		results.Should().OnlyContain(d => d.Value >= 50);
		results.Should().BeInDescendingOrder(d => d.Value);
	}

	// ── WHERE + TOP + ORDER BY ──────────────────────────────────────────

	[Fact]
	public async Task WhereTopOrderBy_ReturnsCorrectSortedSubset()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT TOP 2 * FROM c WHERE c.isActive = true ORDER BY c.value ASC"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(2);
		results.Should().OnlyContain(d => d.IsActive);
		results.Should().BeInAscendingOrder(d => d.Value);
		results[0].Value.Should().Be(20); // item 2
		results[1].Value.Should().Be(40); // item 4
	}

	// ── OFFSET / LIMIT ──────────────────────────────────────────────────

	[Fact]
	public async Task OffsetLimit_ReturnsCorrectPage()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c ORDER BY c.value ASC OFFSET 3 LIMIT 4"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(4);
		results[0].Value.Should().Be(40); // 4th item (0-indexed: skip 10,20,30)
		results[3].Value.Should().Be(70); // 7th item
	}

	// ── DISTINCT ────────────────────────────────────────────────────────

	[Fact]
	public async Task Distinct_ReturnsUniqueValues()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT DISTINCT VALUE c.partitionKey FROM c"));

		var results = await DrainIterator(iterator);

		// Should be exactly 3 distinct partition keys: pk-0, pk-1, pk-2
		results.Should().HaveCount(3);
		results.Should().BeEquivalentTo("pk-0", "pk-1", "pk-2");
	}

	// ── DISTINCT + TOP ──────────────────────────────────────────────────

	[Fact]
	public async Task DistinctTop_ReturnsLimitedUniqueValues()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT DISTINCT TOP 2 VALUE c.partitionKey FROM c"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(2);
	}

	// ── VALUE SELECT with WHERE ─────────────────────────────────────────

	[Fact]
	public async Task ValueSelect_WithWhere_ReturnsFilteredScalarValues()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.name FROM c WHERE c.value <= 30 ORDER BY c.value ASC"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(3);
		results[0].Should().Be("Item1");
		results[1].Should().Be("Item2");
		results[2].Should().Be("Item3");
	}

	// ── Cross-partition query ───────────────────────────────────────────

	[Fact]
	public async Task CrossPartitionQuery_ReturnsAllPartitions()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(10);
		results.Select(d => d.PartitionKey).Distinct().Should().HaveCount(3);
	}

	// ── Partition-scoped query ──────────────────────────────────────────

	[Fact]
	public async Task PartitionScopedQuery_ReturnsOnlyMatchingPartition()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.value > 0"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk-0") });

		var results = await DrainIterator(iterator);

		results.Should().OnlyContain(d => d.PartitionKey == "pk-0");
		// Items with pk-0: 3,6,9 → values 30,60,90
		results.Should().HaveCount(3);
	}

	// ── GetItemQueryIterator with null queryText ────────────────────────

	[Fact]
	public async Task NullQueryText_ReturnsAllItems()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			queryText: null);

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(10);
	}

	// ── GetItemQueryIterator with null queryText + partition key ────────

	[Fact]
	public async Task NullQueryText_WithPartitionKey_ReturnsPartitionItems()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			queryText: null,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk-1") });

		var results = await DrainIterator(iterator);

		results.Should().OnlyContain(d => d.PartitionKey == "pk-1");
	}

	// ── LINQ queryable ──────────────────────────────────────────────────

	[Fact]
	public async Task LinqQueryable_ReturnsCorrectResults()
	{
		var container = await CreateSeededContainer();

		var queryable = container.GetItemLinqQueryable<TestDocument>();

		var results = queryable.Where(d => d.Value > 50).ToList();

		results.Should().HaveCount(5); // items 6,7,8,9,10
		results.Should().OnlyContain(d => d.Value > 50);
	}

	// ── LINQ queryable with partition key ────────────────────────────────

	[Fact]
	public async Task LinqQueryable_WithPartitionKey_ReturnsOnlyMatchingPartition()
	{
		var container = await CreateSeededContainer();

		var queryable = container.GetItemLinqQueryable<TestDocument>(
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk-2") });

		var results = queryable.ToList();

		results.Should().OnlyContain(d => d.PartitionKey == "pk-2");
	}

	// ── Stream iterator with null queryText ─────────────────────────────

	[Fact]
	public async Task StreamIterator_NullQueryText_ReturnsAllItems()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryStreamIterator(queryText: null);

		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			var body = await new StreamReader(response.Content).ReadToEndAsync();
			var arr = JObject.Parse(body)["Documents"] as JArray;
			if (arr != null)
				foreach (var item in arr)
					results.Add((JObject)item);
		}

		results.Should().HaveCount(10);
	}

	// ── GROUP BY ────────────────────────────────────────────────────────

	[Fact]
	public async Task GroupBy_ReturnsCorrectGroupCounts()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(3);

		var pk0 = results.First(r => r["partitionKey"]?.ToString() == "pk-0");
		pk0["cnt"]!.Value<int>().Should().Be(3); // items 3,6,9

		var pk1 = results.First(r => r["partitionKey"]?.ToString() == "pk-1");
		pk1["cnt"]!.Value<int>().Should().Be(4); // items 1,4,7,10

		var pk2 = results.First(r => r["partitionKey"]?.ToString() == "pk-2");
		pk2["cnt"]!.Value<int>().Should().Be(3); // items 2,5,8
	}

	// ── TTL expired items filtered from queries ─────────────────────────

	[Fact]
	public async Task ExpiredItems_FilteredFromQueries()
	{
		var container = new InMemoryContainer("qpb-ttl", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Live", Value = 10 },
			new PartitionKey("pk"));

		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "WillExpire", Value = 20 },
			new PartitionKey("pk"));

		// Wait for items to expire
		await Task.Delay(1500);

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c"));

		var results = await DrainIterator(iterator);

		results.Should().BeEmpty();
	}

	// ── TTL expired items filtered from LINQ ────────────────────────────

	[Fact]
	public async Task ExpiredItems_FilteredFromLinq()
	{
		var container = new InMemoryContainer("qpb-ttl-linq", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Live", Value = 10 },
			new PartitionKey("pk"));

		await Task.Delay(1500);

		var results = container.GetItemLinqQueryable<TestDocument>().ToList();

		results.Should().BeEmpty();
	}

	// ── TTL expired items filtered from null-queryText iterator ──────────

	[Fact]
	public async Task ExpiredItems_FilteredFromNullQueryTextIterator()
	{
		var container = new InMemoryContainer("qpb-ttl-null", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Live", Value = 10 },
			new PartitionKey("pk"));

		await Task.Delay(1500);

		var iterator = container.GetItemQueryIterator<TestDocument>(queryText: null);
		var results = await DrainIterator(iterator);

		results.Should().BeEmpty();
	}

	// ── Hierarchical partition key prefix query ─────────────────────────

	[Fact]
	public async Task HierarchicalPartitionKey_PrefixQuery_ReturnsMatchingItems()
	{
		var container = new InMemoryContainer("qpb-hpk", new[] { "/region", "/tenant" });

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", region = "US", tenant = "A", data = "one" }),
			new PartitionKeyBuilder().Add("US").Add("A").Build());
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", region = "US", tenant = "B", data = "two" }),
			new PartitionKeyBuilder().Add("US").Add("B").Build());
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", region = "EU", tenant = "A", data = "three" }),
			new PartitionKeyBuilder().Add("EU").Add("A").Build());

		// Query with prefix PK = "US" should return items 1 and 2
		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions
			{
				PartitionKey = new PartitionKeyBuilder().Add("US").Build()
			});

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(2);
		results.Select(r => r["id"]!.ToString()).Should().BeEquivalentTo("1", "2");
	}

	// ── JOIN query ──────────────────────────────────────────────────────

	[Fact]
	public async Task JoinQuery_ExpandsArrayCorrectly()
	{
		var container = await CreateSeededContainer(3);

		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, t AS tag FROM c JOIN t IN c.tags"));

		var results = await DrainIterator(iterator);

		// Each of 3 items has 2 tags → 6 expanded rows
		results.Should().HaveCount(6);
	}

	// ── Aggregate query ────────────────────────────────────────────────

	[Fact]
	public async Task AggregateQuery_ReturnsCorrectResults()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT COUNT(1) AS total, SUM(c.value) AS valueSum FROM c"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(1);
		results[0]["total"]!.Value<int>().Should().Be(10);
		results[0]["valueSum"]!.Value<int>().Should().Be(550); // sum of 10+20+...+100
	}

	// ── Stream iterator with query ─────────────────────────────────────

	[Fact]
	public async Task StreamIterator_WithQuery_ReturnsFilteredResults()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryStreamIterator(
			new QueryDefinition("SELECT * FROM c WHERE c.value > 80"));

		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			var body = await new StreamReader(response.Content).ReadToEndAsync();
			var arr = JObject.Parse(body)["Documents"] as JArray;
			if (arr != null)
				foreach (var item in arr)
					results.Add((JObject)item);
		}

		results.Should().HaveCount(2); // items with value 90, 100
	}

	// ── Mixed: WHERE + OFFSET + LIMIT ──────────────────────────────────

	[Fact]
	public async Task WhereWithOffsetLimit_ReturnsCorrectPage()
	{
		var container = await CreateSeededContainer();

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.isActive = true ORDER BY c.value ASC OFFSET 1 LIMIT 2"));

		var results = await DrainIterator(iterator);

		results.Should().HaveCount(2);
		results[0].Value.Should().Be(40); // skip item 2 (value=20), get items 4,6
		results[1].Value.Should().Be(60);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Helper
	// ═══════════════════════════════════════════════════════════════════════════

	private static async Task<List<T>> DrainIterator<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}
		return results;
	}
}
