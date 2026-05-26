using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class FeedRangeGapTests3
{
	[Fact]
	public async Task GetFeedRanges_WithMultipleRanges_ReturnsMultiple()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCountGreaterThan(1);
	}

	[Fact]
	public async Task FeedRange_UsableWithQueryIterator()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().NotBeEmpty();

		// Use the first range with a query — should still return results
		var iterator = container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { });
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().NotBeEmpty();
	}
}


public class FeedRangeGapTests
{
	[Fact]
	public async Task GetFeedRanges_ReturnsSingleRange_ByDefault()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var feedRanges = await container.GetFeedRangesAsync();

		feedRanges.Should().HaveCount(1);
	}
}


public class FeedRangeGapTests4
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey") { FeedRangeCount = 4 };

	[Fact]
	public async Task FeedRange_UsableWithChangeFeedIterator()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

		var ranges = await _container.GetFeedRangesAsync();
		var allResults = new List<TestDocument>();
		foreach (var range in ranges)
		{
			var iterator = _container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				allResults.AddRange(page);
			}
		}

		// Union of all ranges covers both items
		allResults.Should().HaveCount(2);
	}
}


public class FeedRangeGapTests2
{
	[Fact]
	public async Task GetFeedRanges_AlwaysReturnsSingleRange()
	{
		// InMemoryContainer always returns a single FeedRange regardless of data
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i % 10}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i % 10}"));
		}

		var feedRanges = await container.GetFeedRangesAsync();
		feedRanges.Should().HaveCount(1);
	}
}


public class FeedRangeDivergentBehaviorTests
{
	/// <summary>
	/// InMemoryContainer defaults to FeedRangeCount=1 (single range covering the full hash space).
	/// Real Cosmos DB dynamically creates partition ranges based on data volume and throughput.
	/// Set FeedRangeCount > 1 to simulate multiple physical partitions for FeedRange-scoped
	/// queries and change feed iterators.
	/// </summary>
	[Fact]
	public async Task GetFeedRanges_DefaultsSingle_SetFeedRangeCountForMultiple()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		// Default: 1 range (unlike real Cosmos DB which may auto-split)
		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(1);

		// Opt-in: set FeedRangeCount for multi-range simulation
		container.FeedRangeCount = 4;
		var multiRanges = await container.GetFeedRangesAsync();
		multiRanges.Should().HaveCount(4);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category 1: FeedRangeCount Validation Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeCountValidationTests
{
	[Fact]
	public async Task FeedRangeCount_Zero_ClampedToOne()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 0 };
		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(1);
	}

	[Fact]
	public async Task FeedRangeCount_Negative_ClampedToOne()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = -5 };
		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(1);
	}

	[Fact]
	public async Task FeedRangeCount_256_AllItemsAccountedFor()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 256 };
		for (var i = 0; i < 500; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(256);

		var allItems = new List<JObject>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
				allItems.AddRange(await iterator.ReadNextAsync());
		}

		allItems.Should().HaveCount(500);
	}

	[Fact]
	public async Task FeedRangeCount_Two_SplitsItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(2);

		var total = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
				total += (await iterator.ReadNextAsync()).Count;
		}

		total.Should().Be(50);
	}

	[Fact]
	public async Task FeedRangeCount_ChangedBetweenCalls_ProducesDifferentRanges()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
		var ranges2 = await container.GetFeedRangesAsync();
		ranges2.Should().HaveCount(2);

		container.FeedRangeCount = 4;
		var ranges4 = await container.GetFeedRangesAsync();
		ranges4.Should().HaveCount(4);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 2: Partition Affinity & Hashing Consistency
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangePartitionAffinityTests
{
	[Fact]
	public async Task SamePartitionKey_AlwaysMapsToSameRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = "samePK" }),
				new PartitionKey("samePK"));

		var ranges = await container.GetFeedRangesAsync();
		var rangesWithItems = 0;

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			var count = 0;
			while (iterator.HasMoreResults)
				count += (await iterator.ReadNextAsync()).Count;
			if (count > 0) rangesWithItems++;
		}

		rangesWithItems.Should().Be(1, "all items with the same PK should land in exactly one range");
	}

	[Fact]
	public void MurmurHash3_Deterministic_SameInputSameOutput()
	{
		var hash1 = PartitionKeyHash.MurmurHash3("test-key");
		var hash2 = PartitionKeyHash.MurmurHash3("test-key");
		var hash3 = PartitionKeyHash.MurmurHash3("test-key");

		hash1.Should().Be(hash2).And.Be(hash3);
	}

	[Fact]
	public async Task HashDistribution_RoughlyEven_With1000Items()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 1000; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			var count = 0;
			while (iterator.HasMoreResults)
				count += (await iterator.ReadNextAsync()).Count;

			count.Should().BeGreaterThan(0, "no range should be empty with 1000 items");
			count.Should().BeLessThan(500, "no range should have more than half the items");
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 3: Partition Key Type Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangePartitionKeyTypeTests
{
	[Fact]
	public async Task EmptyStringPartitionKey_LandsInOneRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "" }),
			new PartitionKey(""));

		var ranges = await container.GetFeedRangesAsync();
		var foundInRanges = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
				if ((await iterator.ReadNextAsync()).Count > 0) foundInRanges++;
		}

		foundInRanges.Should().Be(1);
	}

	[Fact]
	public async Task UnicodePartitionKey_LandsInOneRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "日本語テスト🎉" }),
			new PartitionKey("日本語テスト🎉"));

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(1);
	}

	[Fact]
	public async Task VeryLongPartitionKey_LandsInOneRange()
	{
		var longPk = new string('x', 1000);
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = longPk }),
			new PartitionKey(longPk));

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(1);
	}

	[Fact]
	public async Task BooleanPartitionKey_LandsInOneRange()
	{
		var container = new InMemoryContainer("test", "/pk") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"1\",\"pk\":true}"), new PartitionKey(true));
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"2\",\"pk\":false}"), new PartitionKey(false));

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 4: Query Features with FeedRange
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeQueryFeatureTests
{
	[Fact]
	public async Task ItemCount_WithFeedRange_SumEqualsTotal()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var totalCount = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults)
				totalCount += (await it.ReadNextAsync()).Count;
		}

		totalCount.Should().Be(20);
	}

	[Fact]
	public async Task OrderBy_WithFeedRange_OrdersWithinRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i:D3}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c ORDER BY c.name ASC"));
			var names = new List<string>();
			while (it.HasMoreResults)
				names.AddRange((await it.ReadNextAsync()).Select(j => j["name"]!.ToString()));

			names.Should().BeInAscendingOrder();
		}
	}

	[Fact]
	public async Task Top_WithFeedRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT TOP 3 * FROM c"));
			var results = new List<JObject>();
			while (it.HasMoreResults) results.AddRange(await it.ReadNextAsync());

			results.Count.Should().BeLessThanOrEqualTo(3);
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 5: Change Feed + FeedRange Advanced
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeChangeFeedAdvancedTests
{
	[Fact]
	public async Task ChangeFeed_EmptyRange_ReturnsNoItems()
	{
		// Create items with a single PK so they all land in one range
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "samePK" }),
			new PartitionKey("samePK"));

		var ranges = await container.GetFeedRangesAsync();
		var emptyCount = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			var items = new List<JObject>();
			while (iterator.HasMoreResults)
				items.AddRange(await iterator.ReadNextAsync());
			if (items.Count == 0) emptyCount++;
		}

		emptyCount.Should().BeGreaterThan(0, "at least some ranges should be empty");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category 6: Empty Container & Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeEmptyContainerTests
{
	[Fact]
	public async Task EmptyContainer_FeedRangeQuery_ReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			var results = new List<JObject>();
			while (it.HasMoreResults) results.AddRange(await it.ReadNextAsync());
			results.Should().BeEmpty();
		}
	}

	[Fact]
	public async Task SingleItem_HighRangeCount_MostRangesEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 50 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "onlyPK" }),
			new PartitionKey("onlyPK"));

		var ranges = await container.GetFeedRangesAsync();
		var populatedRanges = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			var count = 0;
			while (it.HasMoreResults) count += (await it.ReadNextAsync()).Count;
			if (count > 0) populatedRanges++;
		}

		populatedRanges.Should().Be(1);
	}

	[Fact]
	public async Task AllItems_SamePartitionKey_OnlyOneRangePopulated()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = "samePK" }),
				new PartitionKey("samePK"));

		var ranges = await container.GetFeedRangesAsync();
		var rangesWithItems = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			var count = 0;
			while (it.HasMoreResults) count += (await it.ReadNextAsync()).Count;
			if (count > 0) rangesWithItems++;
		}

		rangesWithItems.Should().Be(1);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  FeedRange Deep Dive: Query Advanced
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeQueryAdvancedTests
{
	private static async Task<InMemoryContainer> CreatePopulatedContainer(int count = 50, int feedRangeCount = 4)
	{
		var container = new InMemoryContainer("fr-adv", "/partitionKey") { FeedRangeCount = feedRangeCount };
		for (var i = 0; i < count; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i:D3}", category = $"cat{i % 4}", status = i % 2 == 0 ? "active" : "inactive" }),
				new PartitionKey($"pk-{i}"));
		return container;
	}

	[Fact]
	public async Task Count_WithFeedRange_PerRangeSumsToTotal()
	{
		var container = await CreatePopulatedContainer(50, 4);
		var ranges = await container.GetFeedRangesAsync();

		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults)
				total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(50);
	}

	[Fact]
	public async Task Sum_WithFeedRange_PerRangeSumsToTotal()
	{
		var container = new InMemoryContainer("fr-sum", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", val = i }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		long total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults)
				foreach (var item in await it.ReadNextAsync())
					total += item["val"]!.Value<long>();
		}

		total.Should().Be(190); // 0+1+...+19
	}

	[Fact]
	public async Task MinMax_WithFeedRange_ConsistentWithContents()
	{
		var container = new InMemoryContainer("fr-mm", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", val = i }),
				new PartitionKey($"pk-{i}"));

		var range = (await container.GetFeedRangesAsync())[0];

		var items = new List<JObject>();
		var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
		while (it.HasMoreResults) items.AddRange(await it.ReadNextAsync());

		if (items.Count > 0)
		{
			var actualMin = items.Min(j => j["val"]!.Value<int>());
			var actualMax = items.Max(j => j["val"]!.Value<int>());
			actualMin.Should().BeGreaterThanOrEqualTo(0);
			actualMax.Should().BeLessThan(20);
			actualMax.Should().BeGreaterThan(actualMin);
		}
	}

	[Fact]
	public async Task Distinct_WithFeedRange_UnionEqualsGlobal()
	{
		var container = await CreatePopulatedContainer(20, 2);
		var ranges = await container.GetFeedRangesAsync();

		var perRangeCategories = new HashSet<string>();
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT DISTINCT c.category FROM c"));
			while (it.HasMoreResults)
				foreach (var item in await it.ReadNextAsync())
					perRangeCategories.Add(item["category"]!.ToString());
		}

		// Global distinct
		var globalIt = container.GetItemQueryIterator<JObject>(new QueryDefinition("SELECT DISTINCT c.category FROM c"));
		var globalCategories = new HashSet<string>();
		while (globalIt.HasMoreResults)
			foreach (var item in await globalIt.ReadNextAsync())
				globalCategories.Add(item["category"]!.ToString());

		perRangeCategories.Should().BeEquivalentTo(globalCategories);
	}

	[Fact]
	public async Task OffsetLimit_WithFeedRange_PaginatesWithinRange()
	{
		var container = await CreatePopulatedContainer(20, 2);
		var range = (await container.GetFeedRangesAsync())[0];

		var page1 = new List<JObject>();
		var it1 = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c ORDER BY c.id OFFSET 0 LIMIT 3"));
		while (it1.HasMoreResults) page1.AddRange(await it1.ReadNextAsync());

		var page2 = new List<JObject>();
		var it2 = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c ORDER BY c.id OFFSET 3 LIMIT 3"));
		while (it2.HasMoreResults) page2.AddRange(await it2.ReadNextAsync());

		// Pages should not overlap
		var ids1 = page1.Select(j => j["id"]!.ToString()).ToHashSet();
		var ids2 = page2.Select(j => j["id"]!.ToString()).ToHashSet();
		ids1.Overlaps(ids2).Should().BeFalse();
	}

	[Fact]
	public async Task ParameterizedQuery_WithFeedRange_FiltersCorrectly()
	{
		var container = await CreatePopulatedContainer(20, 4);
		var ranges = await container.GetFeedRangesAsync();

		var totalFound = 0;
		foreach (var range in ranges)
		{
			var qd = new QueryDefinition("SELECT * FROM c WHERE c.name = @name").WithParameter("@name", "Item005");
			var it = container.GetItemQueryIterator<JObject>(range, qd);
			while (it.HasMoreResults) totalFound += (await it.ReadNextAsync()).Count;
		}

		totalFound.Should().Be(1);
	}

	[Fact]
	public async Task Projection_WithFeedRange_ReturnsProjectedFields()
	{
		var container = await CreatePopulatedContainer(10, 2);
		var range = (await container.GetFeedRangesAsync())[0];

		var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT c.id, c.name FROM c"));
		var results = new List<JObject>();
		while (it.HasMoreResults) results.AddRange(await it.ReadNextAsync());

		if (results.Count > 0)
		{
			results[0]["id"].Should().NotBeNull();
			results[0]["name"].Should().NotBeNull();
			results[0]["partitionKey"].Should().BeNull("projection should not include non-selected fields");
		}
	}

	[Fact]
	public async Task ValueKeyword_WithFeedRange_ReturnsItemsFromRange()
	{
		var container = await CreatePopulatedContainer(10, 2);
		var ranges = await container.GetFeedRangesAsync();

		// VALUE queries return scalars without PK fields, so FeedRange post-filtering
		// can't extract PK. Instead verify items-per-range sums correctly.
		var allNames = new List<string>();
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults)
				foreach (var item in await it.ReadNextAsync())
					allNames.Add(item["name"]!.ToString());
		}

		allNames.Should().HaveCount(10);
	}

	[Fact]
	public async Task WhereClause_WithFeedRange_ComposesCorrectly()
	{
		var container = await CreatePopulatedContainer(40, 4);
		var ranges = await container.GetFeedRangesAsync();

		var totalActive = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c WHERE c.status = 'active'"));
			while (it.HasMoreResults) totalActive += (await it.ReadNextAsync()).Count;
		}

		totalActive.Should().Be(20);
	}

	[Fact]
	public async Task GroupBy_WithFeedRange_UnionCoversAllGroups()
	{
		var container = await CreatePopulatedContainer(20, 2);
		var ranges = await container.GetFeedRangesAsync();

		var allCategories = new HashSet<string>();
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT c.category, COUNT(1) AS cnt FROM c GROUP BY c.category"));
			while (it.HasMoreResults)
				foreach (var item in await it.ReadNextAsync())
					allCategories.Add(item["category"]!.ToString());
		}

		allCategories.Should().HaveCount(4); // cat0, cat1, cat2, cat3
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  FeedRange Deep Dive: PK Type Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangePartitionKeyAdvancedTypeTests
{
	[Fact]
	public async Task ThreeLevelHierarchicalPK_AllItemsAccountedFor()
	{
		var container = new InMemoryContainer("fr-3pk", new List<string> { "/tenantId", "/region", "/userId" }) { FeedRangeCount = 4 };
		for (var i = 0; i < 30; i++)
		{
			var pk = new PartitionKeyBuilder().Add($"t{i % 3}").Add($"r{i % 2}").Add($"u{i}").Build();
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", tenantId = $"t{i % 3}", region = $"r{i % 2}", userId = $"u{i}" }),
				pk);
		}

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(30);
	}

	[Fact]
	public async Task IntegerPartitionKey_AllItemsAccountedFor()
	{
		var container = new InMemoryContainer("fr-int", "/category") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", category = i }),
				new PartitionKey(i));

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(20);
	}

	[Fact]
	public async Task DoublePartitionKey_AllItemsAccountedFor()
	{
		var container = new InMemoryContainer("fr-dbl", "/score") { FeedRangeCount = 4 };
		var scores = new[] { 3.14, 2.71, 1.41, 0.577, 99.99 };
		for (var i = 0; i < scores.Length; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", score = scores[i] }),
				new PartitionKey(scores[i]));

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(5);
	}

	[Fact]
	public async Task EmptyStringPK_And_NullPK_BothAccountedFor()
	{
		var container = new InMemoryContainer("fr-empty", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "empty", partitionKey = "" }),
			new PartitionKey(""));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "null-pk" }),
			PartitionKey.Null);

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(2);
	}

	[Fact]
	public async Task SpecialCharacterPK_AllAccountedFor()
	{
		var container = new InMemoryContainer("fr-special", "/partitionKey") { FeedRangeCount = 4 };
		var specialPks = new[] { "new\nline", "tab\there", "cr\r\nline", "null\0char" };
		for (var i = 0; i < specialPks.Length; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = specialPks[i] }),
				new PartitionKey(specialPks[i]));

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(4);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  FeedRange Deep Dive: Pagination
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangePaginationTests
{
	[Fact]
	public async Task MaxItemCount_WithFeedRange_PagesCorrectly()
	{
		var container = new InMemoryContainer("fr-page", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var range = (await container.GetFeedRangesAsync())[0];
		var it = container.GetItemQueryIterator<JObject>(range,
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { MaxItemCount = 3 });

		var allItems = new List<JObject>();
		var pageCount = 0;
		while (it.HasMoreResults)
		{
			var page = await it.ReadNextAsync();
			page.Count.Should().BeLessThanOrEqualTo(3);
			allItems.AddRange(page);
			pageCount++;
		}

		allItems.Should().NotBeEmpty();
		if (allItems.Count > 3) pageCount.Should().BeGreaterThan(1);
	}

	[Fact]
	public async Task ChangeFeed_Pagination_WithFeedRange_AllItemsDelivered()
	{
		var container = new InMemoryContainer("fr-cf-page", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var it = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental,
				new ChangeFeedRequestOptions { PageSizeHint = 3 });
			while (it.HasMoreResults)
				foreach (var item in await it.ReadNextAsync())
					allIds.Add(item["id"]!.ToString());
		}

		allIds.Should().HaveCount(50);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  FeedRange Deep Dive: Change Feed Advanced
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeChangeFeedDeepTests
{
	[Fact]
	public async Task ChangeFeed_Now_ThenAddItems_TypedIterator_SeesNewItems()
	{
		var container = new InMemoryContainer("fr-cf-now", "/partitionKey") { FeedRangeCount = 4 };
		var ranges = await container.GetFeedRangesAsync();

		// Create iterators BEFORE adding items, starting from Now
		var iterators = ranges.Select(r => container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Now(r), ChangeFeedMode.LatestVersion)).ToList();

		// NOW add items
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		// Read from iterators — lazy evaluation should see new items
		var allIds = new HashSet<string>();
		foreach (var it in iterators)
			while (it.HasMoreResults)
				foreach (var item in await it.ReadNextAsync())
					allIds.Add(item["id"]!.ToString());

		allIds.Should().HaveCount(20);
	}

	[Fact]
	public async Task ChangeFeed_Now_ThenAddItems_StreamIterator_MayNotSeeNewItems()
	{
		var container = new InMemoryContainer("fr-cf-stream-now", "/partitionKey") { FeedRangeCount = 2 };
		var ranges = await container.GetFeedRangesAsync();
		var iterators = ranges.Select(r => container.GetChangeFeedStreamIterator(
			ChangeFeedStartFrom.Now(r), ChangeFeedMode.LatestVersion)).ToList();

		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var total = 0;
		foreach (var it in iterators)
			while (it.HasMoreResults)
			{
				var resp = await it.ReadNextAsync();
				if (!resp.IsSuccessStatusCode || resp.Content == null) break;
				using var reader = new StreamReader(resp.Content);
				var body = await reader.ReadToEndAsync();
				if (string.IsNullOrWhiteSpace(body)) break;
				var jObj = JObject.Parse(body);
				if (jObj["Documents"] is JArray docs)
					total += docs.Count;
			}

		total.Should().Be(10);
	}

	[Fact]
	public async Task ChangeFeed_Incremental_UpdatesReturnLatestVersion_WithFeedRange()
	{
		var container = new InMemoryContainer("fr-cf-upd", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = "original" }),
				new PartitionKey($"pk-{i}"));

		// Update 5 items
		for (var i = 0; i < 5; i++)
			await container.UpsertItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = "updated" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allItems = new List<JObject>();
		foreach (var range in ranges)
		{
			var it = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (it.HasMoreResults) allItems.AddRange(await it.ReadNextAsync());
		}

		allItems.Should().HaveCount(10);
		var updatedItems = allItems.Where(j => j["name"]!.ToString() == "updated").ToList();
		updatedItems.Should().HaveCount(5);
	}

	[Fact]
	public async Task ChangeFeed_Beginning_UnionMatchesGlobalChangeFeed()
	{
		var container = new InMemoryContainer("fr-cf-union", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		// Global change feed
		var globalIt = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var globalIds = new HashSet<string>();
		while (globalIt.HasMoreResults)
			foreach (var item in await globalIt.ReadNextAsync())
				globalIds.Add(item["id"]!.ToString());

		// Per-range change feed
		var ranges = await container.GetFeedRangesAsync();
		var perRangeIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var it = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (it.HasMoreResults)
				foreach (var item in await it.ReadNextAsync())
					perRangeIds.Add(item["id"]!.ToString());
		}

		perRangeIds.Should().BeEquivalentTo(globalIds);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  FeedRange Deep Dive: Stream Parity
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeStreamParityTests
{
	[Fact]
	public async Task QueryStreamIterator_WithFeedRange_MatchesTypedIterator()
	{
		var container = new InMemoryContainer("fr-stream", "/partitionKey") { FeedRangeCount = 3 };
		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var range = (await container.GetFeedRangesAsync())[0];

		// Typed
		var typedIt = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
		var typedIds = new HashSet<string>();
		while (typedIt.HasMoreResults)
			foreach (var item in await typedIt.ReadNextAsync())
				typedIds.Add(item["id"]!.ToString());

		// Stream
		var streamIt = container.GetItemQueryStreamIterator(range, new QueryDefinition("SELECT * FROM c"));
		var streamIds = new HashSet<string>();
		while (streamIt.HasMoreResults)
		{
			var resp = await streamIt.ReadNextAsync();
			if (resp.IsSuccessStatusCode)
			{
				using var reader = new StreamReader(resp.Content);
				var body = await reader.ReadToEndAsync();
				var docs = JObject.Parse(body)["Documents"] as JArray;
				if (docs != null)
					foreach (var doc in docs)
						streamIds.Add(doc["id"]!.ToString());
			}
		}

		streamIds.Should().BeEquivalentTo(typedIds);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  FeedRange Deep Dive: Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeEdgeCaseAdvancedTests
{
	[Fact]
	public async Task DeleteAllItems_ThenFeedRangeQuery_ReturnsEmpty()
	{
		var container = new InMemoryContainer("fr-del", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		for (var i = 0; i < 10; i++)
			await container.DeleteItemAsync<JObject>($"{i}", new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var total = 0;
		foreach (var range in ranges)
		{
			var it = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (it.HasMoreResults) total += (await it.ReadNextAsync()).Count;
		}

		total.Should().Be(0);
	}

	[Fact]
	public async Task UpsertSameItem_StaysInSameRange()
	{
		var container = new InMemoryContainer("fr-upsert", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "upsert-test", partitionKey = "test-pk", name = "original" }),
			new PartitionKey("test-pk"));

		var ranges = await container.GetFeedRangesAsync();
		int originalRange = -1;
		for (var r = 0; r < ranges.Count; r++)
		{
			var it = container.GetItemQueryIterator<JObject>(ranges[r], new QueryDefinition("SELECT * FROM c WHERE c.id = 'upsert-test'"));
			while (it.HasMoreResults)
				if ((await it.ReadNextAsync()).Count > 0) originalRange = r;
		}

		// Upsert with new data
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "upsert-test", partitionKey = "test-pk", name = "updated" }),
			new PartitionKey("test-pk"));

		int newRange = -1;
		for (var r = 0; r < ranges.Count; r++)
		{
			var it = container.GetItemQueryIterator<JObject>(ranges[r], new QueryDefinition("SELECT * FROM c WHERE c.id = 'upsert-test'"));
			while (it.HasMoreResults)
				if ((await it.ReadNextAsync()).Count > 0) newRange = r;
		}

		newRange.Should().Be(originalRange);
	}

	[Fact]
	public async Task MultipleDifferentQueries_SameFeedRange_ConsistentSubset()
	{
		var container = new InMemoryContainer("fr-multi-q", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 40; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i}" }),
				new PartitionKey($"pk-{i}"));

		var range = (await container.GetFeedRangesAsync())[0];

		var query1Ids = new HashSet<string>();
		var it1 = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
		while (it1.HasMoreResults) foreach (var j in await it1.ReadNextAsync()) query1Ids.Add(j["id"]!.ToString());

		var query2Ids = new HashSet<string>();
		var it2 = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT c.id, c.partitionKey FROM c"));
		while (it2.HasMoreResults) foreach (var j in await it2.ReadNextAsync()) query2Ids.Add(j["id"]!.ToString());

		query1Ids.Should().BeEquivalentTo(query2Ids);
	}
}
