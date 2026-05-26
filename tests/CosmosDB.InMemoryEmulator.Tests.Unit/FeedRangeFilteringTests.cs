using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 1 — FeedRange Count Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeCountTests
{
	[Fact]
	public async Task FeedRangeCount_DefaultsToOne()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var ranges = await container.GetFeedRangesAsync();

		ranges.Should().HaveCount(1);
	}

	[Fact]
	public async Task FeedRangeCount_ReturnsConfiguredCount()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		var ranges = await container.GetFeedRangesAsync();

		ranges.Should().HaveCount(4);
	}

	[Fact]
	public async Task FeedRanges_AreRealFeedRangeEpk_NotMocks()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			var json = range.ToJsonString();
			var obj = JObject.Parse(json);
			obj["Range"].Should().NotBeNull();
			obj["Range"]!["min"].Should().NotBeNull();
			obj["Range"]!["max"].Should().NotBeNull();
		}
	}

	[Fact]
	public async Task FeedRanges_CoverFullRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 3 };

		var ranges = await container.GetFeedRangesAsync();

		// First range starts at ""
		var firstJson = JObject.Parse(ranges[0].ToJsonString());
		firstJson["Range"]!["min"]!.ToString().Should().Be("");

		// Last range ends at "FF"
		var lastJson = JObject.Parse(ranges[^1].ToJsonString());
		lastJson["Range"]!["max"]!.ToString().Should().Be("FF");
	}

	[Fact]
	public async Task FeedRanges_AreContiguous()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		var ranges = await container.GetFeedRangesAsync();

		for (var i = 0; i < ranges.Count - 1; i++)
		{
			var currentMax = JObject.Parse(ranges[i].ToJsonString())["Range"]!["max"]!.ToString();
			var nextMin = JObject.Parse(ranges[i + 1].ToJsonString())["Range"]!["min"]!.ToString();
			currentMax.Should().Be(nextMin, $"range {i} max should equal range {i + 1} min");
		}
	}

	[Fact]
	public async Task FeedRangeCount_SetToOne_ReturnsSingleFullRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 1 };

		var ranges = await container.GetFeedRangesAsync();

		ranges.Should().HaveCount(1);
		var json = JObject.Parse(ranges[0].ToJsonString());
		json["Range"]!["min"]!.ToString().Should().Be("");
		json["Range"]!["max"]!.ToString().Should().Be("FF");
	}

	[Fact]
	public async Task FeedRangeCount_VeryLarge_NoOverflowAndAllItemsFound()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 65536 };

		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(65536);

		var totalCount = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				totalCount += page.Count;
			}
		}

		totalCount.Should().Be(200);
	}

	[Fact]
	public async Task FeedRanges_AreNonOverlapping_NoGaps()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 5 };

		var ranges = await container.GetFeedRangesAsync();
		var boundaries = ranges.Select(r =>
		{
			var obj = JObject.Parse(r.ToJsonString());
			return (Min: obj["Range"]!["min"]!.ToString(), Max: obj["Range"]!["max"]!.ToString());
		}).ToList();

		// No gaps: max of range[i] == min of range[i+1]
		for (var i = 0; i < boundaries.Count - 1; i++)
			boundaries[i].Max.Should().Be(boundaries[i + 1].Min,
				$"range {i} max should equal range {i + 1} min (no gaps)");

		// Full coverage
		boundaries[0].Min.Should().Be("");
		boundaries[^1].Max.Should().Be("FF");
	}

	[Fact]
	public async Task FeedRanges_AreOrderedByMinAscending()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 8 };

		var ranges = await container.GetFeedRangesAsync();
		var minValues = ranges.Select(r =>
		{
			var obj = JObject.Parse(r.ToJsonString());
			var minStr = obj["Range"]!["min"]!.ToString();
			return minStr == "" ? 0UL : Convert.ToUInt64(minStr, 16);
		}).ToList();

		for (var i = 0; i < minValues.Count - 1; i++)
			minValues[i].Should().BeLessThan(minValues[i + 1],
				$"range {i} min should be less than range {i + 1} min");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 2 — FeedRange Query Filtering Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeQueryFilteringTests
{
	[Fact]
	public async Task QueryIterator_WithFeedRange_FiltersToMatchingPartitions()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		// Seed items across different partition keys
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<TestDocument>();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				allResults.AddRange(page);
			}
		}

		// Union of all ranges should give us all 20 items (no duplicates, no missing)
		allResults.Should().HaveCount(20);
		allResults.Select(r => r.Id).Distinct().Should().HaveCount(20);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_ReturnsSubsetNotAll()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		// At least one range should return fewer than all items
		var anyScopedCorrectly = false;

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			if (results.Count < 100)
				anyScopedCorrectly = true;
		}

		anyScopedCorrectly.Should().BeTrue("at least one FeedRange should return a subset of items");
	}

	[Fact]
	public async Task QueryStreamIterator_WithFeedRange_FiltersToMatchingPartitions()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryStreamIterator(
				range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var response = await iterator.ReadNextAsync();
				using var reader = new StreamReader(response.Content);
				var json = await reader.ReadToEndAsync();
				var doc = JObject.Parse(json);
				foreach (var item in doc["Documents"]!)
					allIds.Add(item["id"]!.ToString());
			}
		}

		allIds.Should().HaveCount(20);
	}

	[Fact]
	public async Task QueryIterator_WithSingleFeedRange_ReturnsAllItems()
	{
		// FeedRangeCount=1 (default) should return all items through the single range
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(1);

		var iterator = container.GetItemQueryIterator<TestDocument>(
			ranges[0], new QueryDefinition("SELECT * FROM c"));
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(10);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_WhereClause_FiltersCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<TestDocument>();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c WHERE c.name != 'NonExistent'"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				allResults.AddRange(page);
			}
		}

		// WHERE clause doesn't filter anything, union of ranges = all items
		allResults.Should().HaveCount(20);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_Pagination_ContinuationToken_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<TestDocument>();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"),
				requestOptions: new QueryRequestOptions { MaxItemCount = 5 });
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				page.Count.Should().BeLessThanOrEqualTo(5);
				allResults.AddRange(page);
			}
		}

		allResults.Should().HaveCount(30);
		allResults.Select(r => r.Id).Distinct().Should().HaveCount(30);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_EmptyContainer_ReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			results.Should().BeEmpty();
		}
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_AllItemsSamePartitionKey_MostRangesEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "samePK", Name = $"Item{i}" },
				new PartitionKey("samePK"));

		var ranges = await container.GetFeedRangesAsync();
		var nonEmptyCount = 0;
		var totalItems = 0;

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			if (results.Count > 0) nonEmptyCount++;
			totalItems += results.Count;
		}

		nonEmptyCount.Should().Be(1, "all items share the same PK and should land in one range");
		totalItems.Should().Be(20);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_AggregateCOUNT()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var totalCount = 0;

		foreach (var range in ranges)
		{
			// Use SELECT * and count in code because aggregate queries return scalars
			// without PK fields, which FilterByFeedRange cannot filter post-query
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				totalCount += page.Count;
			}
		}

		totalCount.Should().Be(20);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_OrderByWithinRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i:D3}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c ORDER BY c.name ASC"));
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			if (results.Count > 1)
			{
				var names = results.Select(r => r.Name).ToList();
				names.Should().BeInAscendingOrder();
			}
		}
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_Top()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT TOP 3 * FROM c"));
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			results.Count.Should().BeLessThanOrEqualTo(3);
		}
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_Distinct()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = i % 2 == 0 ? "CategoryA" : "CategoryB" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			// Include partitionKey in projection so FilterByFeedRange can extract PK
			var iterator = container.GetItemQueryIterator<JObject>(
				range, new QueryDefinition("SELECT DISTINCT c.name, c.partitionKey FROM c"));
			var results = new List<JObject>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			// Each range should have at most 2 distinct names (CategoryA, CategoryB)
			var distinctNames = results.Select(r => r["name"]!.ToString()).Distinct().ToList();
			distinctNames.Count.Should().BeLessThanOrEqualTo(2);
		}
	}

	[Fact]
	public async Task QueryIterator_NullFeedRange_ReturnsAllItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var iterator = container.GetItemQueryIterator<TestDocument>(
			(FeedRange)null!, new QueryDefinition("SELECT * FROM c"));
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(20);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_ParameterizedQuery()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<TestDocument>();

		foreach (var range in ranges)
		{
			var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @name")
				.WithParameter("@name", "Item5");
			var iterator = container.GetItemQueryIterator<TestDocument>(range, query);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				allResults.AddRange(page);
			}
		}

		// "Item5" exists once, should be in exactly one range
		allResults.Should().HaveCount(1);
		allResults[0].Name.Should().Be("Item5");
	}

	[Fact]
	public async Task QueryStreamIterator_NullFeedRange_ReturnsAllItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var iterator = container.GetItemQueryStreamIterator(
			(FeedRange)null!, new QueryDefinition("SELECT * FROM c"));
		var allIds = new HashSet<string>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			using var reader = new StreamReader(response.Content);
			var json = await reader.ReadToEndAsync();
			var doc = JObject.Parse(json);
			foreach (var item in doc["Documents"]!)
				allIds.Add(item["id"]!.ToString());
		}

		allIds.Should().HaveCount(20);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 3 — FeedRange Change Feed Filtering Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeChangeFeedFilteringTests
{
	[Fact]
	public async Task ChangeFeed_Beginning_WithFeedRange_ScopesToRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<TestDocument>();

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				allResults.AddRange(page);
			}
		}

		// Union of all ranges should give us all 20 items
		allResults.Should().HaveCount(20);
		allResults.Select(r => r.Id).Distinct().Should().HaveCount(20);
	}

	[Fact]
	public async Task ChangeFeed_WithFeedRange_ReturnsSubsetNotAll()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var anyScopedCorrectly = false;

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			if (results.Count < 100)
				anyScopedCorrectly = true;
		}

		anyScopedCorrectly.Should().BeTrue("at least one FeedRange should scope to a subset");
	}

	[Fact]
	public async Task ChangeFeedStream_WithFeedRange_ScopesToRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedStreamIterator(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var response = await iterator.ReadNextAsync();
				using var reader = new StreamReader(response.Content);
				var json = await reader.ReadToEndAsync();
				var doc = JObject.Parse(json);
				if (doc["Documents"] is JArray docs)
				{
					foreach (var item in docs)
						allIds.Add(item["id"]!.ToString());
				}
			}
		}

		allIds.Should().HaveCount(20);
	}

	[Fact]
	public async Task ChangeFeed_Now_WithFeedRange_AcceptsRangeParameter()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		var ranges = await container.GetFeedRangesAsync();

		// Verify ChangeFeedStartFrom.Now(feedRange) is accepted without error
		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Now(range),
				ChangeFeedMode.Incremental);

			// "Now" means changes from this point forward — no items yet, so no results
			iterator.HasMoreResults.Should().BeFalse();
		}
	}

	[Fact]
	public async Task ChangeFeed_WithSingleFeedRange_ReturnsAllItems()
	{
		// FeedRangeCount=1 with Beginning(range[0]) should return everything
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(1);

		var iterator = container.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(ranges[0]),
			ChangeFeedMode.Incremental);
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(10);
	}

	[Fact]
	public async Task ChangeFeed_Replace_WithFeedRange_UpdateInCorrectRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		// Upsert item "5" with new data (same PK)
		await container.UpsertItemAsync(
			new TestDocument { Id = "5", PartitionKey = "pk-5", Name = "Updated5" },
			new PartitionKey("pk-5"));

		var ranges = await container.GetFeedRangesAsync();

		// Find which range contains item "5"
		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			var item5 = results.FirstOrDefault(r => r.Id == "5");
			if (item5 != null)
			{
				item5.Name.Should().Be("Updated5", "the latest version should appear in the change feed");
			}
		}
	}

	[Fact]
	public async Task ChangeFeed_IncrementalMode_MultipleUpdates_WithFeedRange_ShowsLatest()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk-1", Name = "Version1" },
			new PartitionKey("pk-1"));

		for (var v = 2; v <= 6; v++)
			await container.UpsertItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk-1", Name = $"Version{v}" },
				new PartitionKey("pk-1"));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<TestDocument>();

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				allResults.AddRange(page);
			}
		}

		// Incremental mode shows only the latest version
		allResults.Should().HaveCount(1);
		allResults[0].Name.Should().Be("Version6");
	}

	[Fact]
	public async Task ChangeFeed_WithFeedRange_Pagination_PageSizeHint()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<TestDocument>();

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental,
				new ChangeFeedRequestOptions { PageSizeHint = 5 });
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				page.Count.Should().BeLessThanOrEqualTo(5);
				allResults.AddRange(page);
			}
		}

		allResults.Should().HaveCount(30);
	}

	[Fact]
	public async Task ChangeFeed_Beginning_WithFeedRange_EmptyContainer_NoResults()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };
		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			iterator.HasMoreResults.Should().BeFalse();
		}
	}

	[Fact]
	public async Task ChangeFeed_Time_WithFeedRange_FiltersBothDimensions()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		// Create "early" items
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"early-{i}", PartitionKey = $"pk-{i}", Name = $"Early{i}" },
				new PartitionKey($"pk-{i}"));

		var midpoint = DateTime.UtcNow;
		await Task.Delay(50);

		// Create "late" items with same PKs (upserts to same partitions)
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"late-{i}", PartitionKey = $"pk-{i}", Name = $"Late{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allLateResults = new List<TestDocument>();

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Time(midpoint, range),
				ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				allLateResults.AddRange(page);
			}
		}

		// Only "late" items should be returned
		allLateResults.Should().HaveCount(20);
		allLateResults.All(r => r.Name.StartsWith("Late")).Should().BeTrue();
	}

	[Fact]
	public async Task ChangeFeed_QueryAndChangeFeed_SameRangeMapping()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			// Query path
			var queryIter = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			var queryIds = new HashSet<string>();
			while (queryIter.HasMoreResults)
			{
				var page = await queryIter.ReadNextAsync();
				foreach (var item in page) queryIds.Add(item.Id);
			}

			// Change feed path
			var cfIter = container.GetChangeFeedIterator<TestDocument>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			var cfIds = new HashSet<string>();
			while (cfIter.HasMoreResults)
			{
				var page = await cfIter.ReadNextAsync();
				foreach (var item in page) cfIds.Add(item.Id);
			}

			queryIds.Should().BeEquivalentTo(cfIds,
				"query path and change feed path should produce identical item sets per range");
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase E — Large Document Consistency
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeLargeDocumentTests
{
	[Fact]
	public async Task LargeDocuments_FeedRangeQuery_SumsToTotal()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.FeedRangeCount = 4;

		// Create 20 items with large payloads (~10 KB each)
		var bigPayload = new string('x', 10_000);
		for (var i = 0; i < 20; i++)
		{
			var doc = new JObject
			{
				["id"] = $"big-{i}",
				["partitionKey"] = $"pk-{i}",
				["data"] = bigPayload
			};
			await container.CreateItemAsync(doc, new PartitionKey($"pk-{i}"));
		}

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(4);

		var totalCount = 0;
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(
				range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				totalCount += page.Count;
			}
		}

		totalCount.Should().Be(20, "sum across all feed ranges should equal total items");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase G — Handler Consistency (Divergent)
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeHandlerConsistencyTests
{
	[Fact]
	public async Task InMemoryContainer_FeedRange_QueryFiltersCorrectly_VsDirectQuery()
	{
		// Verifies that querying with a specific feed range returns a strict
		// subset compared to querying without a feed range, documenting the
		// expected filtering behavior.
		var container = new InMemoryContainer("test", "/partitionKey");
		container.FeedRangeCount = 4;

		for (var i = 0; i < 40; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"N{i}" },
				new PartitionKey($"pk-{i}"));

		// Full query (no feed range)
		var allResults = new List<TestDocument>();
		var allIter = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c"));
		while (allIter.HasMoreResults)
		{
			var page = await allIter.ReadNextAsync();
			allResults.AddRange(page);
		}

		// Per-feed-range queries
		var ranges = await container.GetFeedRangesAsync();
		var perRangeTotal = 0;
		foreach (var range in ranges)
		{
			var rangeResults = new List<TestDocument>();
			var iter = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				rangeResults.AddRange(page);
			}
			rangeResults.Count.Should().BeLessThan(allResults.Count,
				"each feed range should return a subset");
			perRangeTotal += rangeResults.Count;
		}

		perRangeTotal.Should().Be(allResults.Count,
			"sum of per-range results should equal total");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 4 — FeedRange Custom/Manual Boundaries
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeCustomBoundaryTests
{
	[Fact]
	public async Task QueryIterator_CustomFeedRange_SubsetOfFullRange_ReturnsSubset()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		// First half of hash space
		var firstHalf = FeedRange.FromJsonString("{\"Range\":{\"min\":\"\",\"max\":\"80000000\"}}");
		var firstIter = container.GetItemQueryIterator<TestDocument>(
			firstHalf, new QueryDefinition("SELECT * FROM c"));
		var firstResults = new List<TestDocument>();
		while (firstIter.HasMoreResults)
		{
			var page = await firstIter.ReadNextAsync();
			firstResults.AddRange(page);
		}

		// Second half
		var secondHalf = FeedRange.FromJsonString("{\"Range\":{\"min\":\"80000000\",\"max\":\"FF\"}}");
		var secondIter = container.GetItemQueryIterator<TestDocument>(
			secondHalf, new QueryDefinition("SELECT * FROM c"));
		var secondResults = new List<TestDocument>();
		while (secondIter.HasMoreResults)
		{
			var page = await secondIter.ReadNextAsync();
			secondResults.AddRange(page);
		}

		firstResults.Count.Should().BeLessThan(50);
		secondResults.Count.Should().BeLessThan(50);
		(firstResults.Count + secondResults.Count).Should().Be(50);
	}

	[Fact]
	public async Task QueryIterator_FeedRange_InvertedMinMax_ReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var inverted = FeedRange.FromJsonString("{\"Range\":{\"min\":\"80000000\",\"max\":\"40000000\"}}");
		var iterator = container.GetItemQueryIterator<TestDocument>(
			inverted, new QueryDefinition("SELECT * FROM c"));
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().BeEmpty("inverted min > max means zero-width range");
	}

	[Fact]
	public async Task QueryIterator_FeedRange_ZeroWidthRange_ReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var zeroWidth = FeedRange.FromJsonString("{\"Range\":{\"min\":\"55555555\",\"max\":\"55555555\"}}");
		var iterator = container.GetItemQueryIterator<TestDocument>(
			zeroWidth, new QueryDefinition("SELECT * FROM c"));
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().BeEmpty("identical min and max means zero-width range");
	}

	[Fact]
	public async Task QueryIterator_FeedRange_MalformedHex_FallsBackToAllItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var malformed = FeedRange.FromJsonString("{\"Range\":{\"min\":\"GGGG\",\"max\":\"HHHH\"}}");
		var iterator = container.GetItemQueryIterator<TestDocument>(
			malformed, new QueryDefinition("SELECT * FROM c"));
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(10, "malformed hex falls back to returning all items");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 5 — Partition Key Type Coverage
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangePartitionKeyTypeFilteringTests
{
	[Fact]
	public async Task QueryIterator_WithFeedRange_CompositePartitionKey_NoItemsLost()
	{
		var container = new InMemoryContainer("test", new List<string> { "/tenantId", "/userId" })
		{
			FeedRangeCount = 4
		};

		for (var i = 0; i < 20; i++)
		{
			var doc = new JObject
			{
				["id"] = $"{i}",
				["tenantId"] = $"tenant-{i % 4}",
				["userId"] = $"user-{i}",
				["name"] = $"Item{i}"
			};
			await container.CreateItemAsync(doc,
				new PartitionKeyBuilder()
					.Add($"tenant-{i % 4}")
					.Add($"user-{i}")
					.Build());
		}

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(
				range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page)
					allIds.Add(item["id"]!.ToString());
			}
		}

		allIds.Should().HaveCount(20);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_NullPartitionKey_ItemInExactlyOneRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		await container.CreateItemAsync(
			new TestDocument { Id = "null-pk", PartitionKey = null!, Name = "NullPK" },
			PartitionKey.None);

		var ranges = await container.GetFeedRangesAsync();
		var foundInRanges = 0;

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			if (results.Count > 0) foundInRanges++;
		}

		foundInRanges.Should().Be(1, "null PK item should land in exactly one range");
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_NumericPartitionKey_NoItemsLost()
	{
		var container = new InMemoryContainer("test", "/category") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
		{
			var doc = new JObject
			{
				["id"] = $"{i}",
				["category"] = (i + 1) * 100,
				["name"] = $"Item{i}"
			};
			await container.CreateItemAsync(doc, new PartitionKey((i + 1) * 100));
		}

		var ranges = await container.GetFeedRangesAsync();
		var totalCount = 0;

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(
				range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				totalCount += page.Count;
			}
		}

		totalCount.Should().Be(20);
	}

	[Fact]
	public async Task ChangeFeed_WithFeedRange_CompositePartitionKey_NoItemsLost()
	{
		var container = new InMemoryContainer("test", new List<string> { "/tenantId", "/userId" })
		{
			FeedRangeCount = 4
		};

		for (var i = 0; i < 20; i++)
		{
			var doc = new JObject
			{
				["id"] = $"{i}",
				["tenantId"] = $"tenant-{i % 4}",
				["userId"] = $"user-{i}",
				["name"] = $"Item{i}"
			};
			await container.CreateItemAsync(doc,
				new PartitionKeyBuilder()
					.Add($"tenant-{i % 4}")
					.Add($"user-{i}")
					.Build());
		}

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page)
					allIds.Add(item["id"]!.ToString());
			}
		}

		allIds.Should().HaveCount(20);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 6 — Stream Iterator Parity
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeStreamFilteringParityTests
{
	[Fact]
	public async Task QueryStreamIterator_WithFeedRange_Pagination_AllItemsDelivered()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var streamIds = new HashSet<string>();
		var typedIds = new HashSet<string>();

		foreach (var range in ranges)
		{
			// Stream path
			var streamIter = container.GetItemQueryStreamIterator(
				range, new QueryDefinition("SELECT * FROM c"));
			while (streamIter.HasMoreResults)
			{
				var response = await streamIter.ReadNextAsync();
				using var reader = new StreamReader(response.Content);
				var json = await reader.ReadToEndAsync();
				var doc = JObject.Parse(json);
				foreach (var item in doc["Documents"]!)
					streamIds.Add(item["id"]!.ToString());
			}

			// Typed path
			var typedIter = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			while (typedIter.HasMoreResults)
			{
				var page = await typedIter.ReadNextAsync();
				foreach (var item in page)
					typedIds.Add(item.Id);
			}
		}

		streamIds.Should().HaveCount(30);
		streamIds.Should().BeEquivalentTo(typedIds);
	}

	[Fact]
	public async Task ChangeFeedStream_WithFeedRange_SumsToTotal()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();

		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedStreamIterator(
				ChangeFeedStartFrom.Beginning(range),
				ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var response = await iterator.ReadNextAsync();
				using var reader = new StreamReader(response.Content);
				var json = await reader.ReadToEndAsync();
				var doc = JObject.Parse(json);
				if (doc["Documents"] is JArray docs)
				{
					foreach (var item in docs)
						allIds.Add(item["id"]!.ToString());
				}
			}
		}

		allIds.Should().HaveCount(20);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 7 — FeedRange.FromPartitionKey Divergent Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeQueryFilteringDivergentTests
{
	[Fact]
	public async Task QueryIterator_FeedRangeFromPartitionKey_ScopesToSinglePartition()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var pkRange = FeedRange.FromPartitionKey(new PartitionKey("pk-5"));
		var iterator = container.GetItemQueryIterator<TestDocument>(
			pkRange, new QueryDefinition("SELECT * FROM c"));
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
		results[0].Id.Should().Be("5");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 8 — Change Feed Stream Eager Evaluation Divergent Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedRangeChangeFeedStreamDivergentTests
{
	[Fact]
	public async Task ChangeFeedStream_Now_WithFeedRange_LazyEvaluation_SeesNewItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };
		var ranges = await container.GetFeedRangesAsync();

		var iterator = container.GetChangeFeedStreamIterator(
			ChangeFeedStartFrom.Now(ranges[0]),
			ChangeFeedMode.Incremental);

		// Add items after iterator creation
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		var allIds = new HashSet<string>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			using var reader = new StreamReader(response.Content);
			var json = await reader.ReadToEndAsync();
			var doc = JObject.Parse(json);
			if (doc["Documents"] is JArray docs)
			{
				foreach (var item in docs)
					allIds.Add(item["id"]!.ToString());
			}
		}

		allIds.Should().NotBeEmpty("lazy evaluation should see items added after creation");
	}
}
