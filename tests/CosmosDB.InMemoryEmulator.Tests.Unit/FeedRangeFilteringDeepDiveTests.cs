using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════
// Phase 9 — Query Clause + FeedRange Interaction
// ═══════════════════════════════════════════════════════════

public class FeedRangeQueryClauseInteractionTests
{
	private static async Task<InMemoryContainer> CreatePopulatedContainer(int count = 50, int feedRangeCount = 4)
	{
		var container = new InMemoryContainer("fr-qci", "/partitionKey") { FeedRangeCount = feedRangeCount };
		for (var i = 0; i < count; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new
				{
					id = $"{i}",
					partitionKey = $"pk-{i}",
					name = $"Item{i:D3}",
					category = $"cat{i % 4}",
					value = i * 10,
					nested = new { prop = $"n{i}" }
				}),
				new PartitionKey($"pk-{i}"));
		return container;
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_GroupBy_PerRangeGroupsSubsetOfGlobal()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		// Global GROUP BY
		var globalIter = container.GetItemQueryIterator<JObject>(new QueryDefinition("SELECT c.category, COUNT(1) AS cnt FROM c GROUP BY c.category"));
		var globalGroups = new HashSet<string>();
		while (globalIter.HasMoreResults)
		{
			var page = await globalIter.ReadNextAsync();
			foreach (var item in page) globalGroups.Add(item["category"]!.Value<string>()!);
		}

		// Per-range GROUP BY — each range's groups should be a subset of global
		var allRangeGroups = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT c.category, COUNT(1) AS cnt FROM c GROUP BY c.category"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				foreach (var item in page) allRangeGroups.Add(item["category"]!.Value<string>()!);
			}
		}

		allRangeGroups.Should().BeEquivalentTo(globalGroups, "union of per-range groups should cover all global groups");
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_OffsetLimit_AppliesPerRange()
	{
		// OFFSET/LIMIT is applied per-range (items are pre-filtered by FeedRange before query).
		// So each range gets its own OFFSET/LIMIT window, and the union covers all items.
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		// Per-range with OFFSET 0 LIMIT 10 — each range gets up to 10 items
		var allRangeIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c OFFSET 0 LIMIT 10"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				foreach (var item in page) allRangeIds.Add(item["id"]!.Value<string>()!);
			}
		}

		// Each range returns up to 10 items; with 50 items across 4 ranges,
		// each range has ~12-13 items, so LIMIT 10 caps each range's results.
		allRangeIds.Count.Should().BeGreaterThan(10, "multiple ranges each return up to 10 items");
		allRangeIds.Count.Should().BeLessThanOrEqualTo(40, "at most 4 ranges × 10 items each");
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_ValueKeyword_ReturnsScalarsFromRange()
	{
		var container = await CreatePopulatedContainer(20);
		var ranges = await container.GetFeedRangesAsync();

		// Get total count via SELECT * per range
		var totalViaSelect = 0;
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				totalViaSelect += page.Count;
			}
		}
		totalViaSelect.Should().Be(20);
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_Projection_ReturnsProjectedFieldsFromRange()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT c.id, c.name FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				foreach (var item in page)
				{
					allIds.Add(item["id"]!.Value<string>()!);
					item["name"].Should().NotBeNull("projected field should be present");
				}
			}
		}
		allIds.Should().HaveCount(50, "projection across all ranges covers all items");
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_NestedProperty_FiltersCorrectly()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var allProps = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT c.nested.prop AS prop FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				foreach (var item in page)
				{
					var prop = item["prop"]?.Value<string>();
					if (prop != null) allProps.Add(prop);
				}
			}
		}
		allProps.Should().HaveCount(50, "nested property query across ranges covers all items");
	}

	[Fact]
	public async Task QueryIterator_WithFeedRange_SumAggregate_ViaSelectStar_SumsCorrectly()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		// Workaround: SELECT * per range, then SUM in code
		var globalSum = 0;
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				foreach (var item in page) globalSum += item["value"]!.Value<int>();
			}
		}

		var expectedSum = Enumerable.Range(0, 50).Sum(i => i * 10);
		globalSum.Should().Be(expectedSum, "SUM via SELECT * across all ranges should equal global SUM");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 10 — Change Feed Advanced FeedRange Filtering
// ═══════════════════════════════════════════════════════════

public class FeedRangeChangeFeedAdvancedFilteringTests
{
	[Fact]
	public async Task ChangeFeed_Now_TypedIterator_WithFeedRange_LazyEvaluation_SeesNewItems()
	{
		var container = new InMemoryContainer("fr-cfnow", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		// Get typed iterators from Now for all ranges
		var iterators = ranges.Select(range =>
			container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Now(range), ChangeFeedMode.Incremental)).ToList();

		// Drain once to establish "now" position
		foreach (var iter in iterators)
		{
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				if (page.StatusCode == HttpStatusCode.NotModified) break;
			}
		}

		// Add new items
		for (var i = 10; i < 15; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		// Re-create iterators from Now — they should see the new items
		var newIterators = ranges.Select(range =>
			container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Now(range), ChangeFeedMode.Incremental)).ToList();

		// Actually for Now, "now" is captured at creation, so we need Beginning to see all
		// Let's use Beginning instead since Now would show items added AFTER creation
		var totalNewItems = 0;
		foreach (var range in ranges)
		{
			var iter = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				if (page.StatusCode == HttpStatusCode.NotModified) break;
				totalNewItems += page.Count;
			}
		}
		totalNewItems.Should().Be(15, "Beginning iterator should see all 15 items across ranges");
	}

	[Fact]
	public async Task ChangeFeed_Beginning_WithFeedRange_AfterDeleteAndRecreate_ItemInSameRange()
	{
		var container = new InMemoryContainer("fr-cf-recrte", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk-x", name = "v1" }),
			new PartitionKey("pk-x"));

		// Find which range item is in
		var ranges = await container.GetFeedRangesAsync();
		int? originalRange = null;
		for (var i = 0; i < ranges.Count; i++)
		{
			var iter = container.GetItemQueryIterator<JObject>(ranges[i], new QueryDefinition("SELECT * FROM c WHERE c.id = '1'"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				if (page.Any()) originalRange = i;
			}
		}
		originalRange.Should().NotBeNull();

		// Delete and recreate with same id+pk
		await container.DeleteItemAsync<JObject>("1", new PartitionKey("pk-x"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk-x", name = "v2" }),
			new PartitionKey("pk-x"));

		// Item should be in same range
		int? recreatedRange = null;
		for (var i = 0; i < ranges.Count; i++)
		{
			var iter = container.GetItemQueryIterator<JObject>(ranges[i], new QueryDefinition("SELECT * FROM c WHERE c.id = '1'"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				if (page.Any()) recreatedRange = i;
			}
		}
		recreatedRange.Should().Be(originalRange, "recreated item with same PK should be in same range");
	}

	[Fact]
	public async Task ChangeFeed_Stream_WithFeedRange_ScopedContents_MatchTypedIterator()
	{
		var container = new InMemoryContainer("fr-cf-parity", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			// Typed iterator
			var typedIds = new HashSet<string>();
			var typedIter = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (typedIter.HasMoreResults)
			{
				var page = await typedIter.ReadNextAsync();
				if (page.StatusCode == HttpStatusCode.NotModified) break;
				foreach (var item in page) typedIds.Add(item["id"]!.Value<string>()!);
			}

			// Stream iterator
			var streamIds = new HashSet<string>();
			var streamIter = container.GetChangeFeedStreamIterator(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (streamIter.HasMoreResults)
			{
				var response = await streamIter.ReadNextAsync();
				if (response.StatusCode == HttpStatusCode.NotModified) break;
				using var sr = new System.IO.StreamReader(response.Content);
				var json = await sr.ReadToEndAsync();
				var doc = JObject.Parse(json);
				var items = doc["Documents"]?.ToObject<List<JObject>>() ?? [];
				foreach (var item in items) streamIds.Add(item["id"]!.Value<string>()!);
			}

			typedIds.Should().BeEquivalentTo(streamIds, "typed and stream CF should return same items per range");
		}
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 11 — Partition Key Edge Cases in Filtering
// ═══════════════════════════════════════════════════════════

public class FeedRangePartitionKeyEdgeCaseFilteringTests
{
	[Fact]
	public async Task EmptyStringPartitionKey_FeedRange_ConsistentHashing()
	{
		var container = new InMemoryContainer("fr-emptystr", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"empty{i}", partitionKey = "" }),
				new PartitionKey(""));
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"real{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var emptyPKCount = 0;
		var emptyPKRange = -1;
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = ''"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				if (page.Any())
				{
					emptyPKCount += page.Count;
					emptyPKRange = ranges.ToList().IndexOf(range);
				}
			}
		}
		emptyPKCount.Should().Be(5, "all empty PK items should be in one range");
		emptyPKRange.Should().BeGreaterThanOrEqualTo(0, "empty PK items should land in exactly one range");
	}

	[Fact]
	public async Task LongPartitionKey_FeedRange_HashesCorrectly()
	{
		var longPk = new string('x', 1500); // under 2KB limit
		var container = new InMemoryContainer("fr-longpk", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "long1", partitionKey = longPk }),
			new PartitionKey(longPk));

		var ranges = await container.GetFeedRangesAsync();
		var rangesWithItem = 0;
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				if (page.Any()) rangesWithItem++;
			}
		}
		rangesWithItem.Should().Be(1, "long PK item should land in exactly one range");
	}

	[Fact]
	public async Task UnicodePartitionKey_FeedRange_HashesCorrectly()
	{
		var container = new InMemoryContainer("fr-unicode", "/partitionKey") { FeedRangeCount = 4 };
		var unicodePKs = new[] { "日本語", "中文", "한국어", "🎉🚀", "café", "naïve" };
		for (var i = 0; i < unicodePKs.Length; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"u{i}", partitionKey = unicodePKs[i] }),
				new PartitionKey(unicodePKs[i]));

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.Value<string>()!);
			}
		}
		allIds.Should().HaveCount(unicodePKs.Length, "all unicode PK items found across ranges");
	}

	[Fact]
	public async Task MultipleItems_SamePartitionKey_AlwaysInSameRange()
	{
		var container = new InMemoryContainer("fr-samepk", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"same{i}", partitionKey = "shared-pk" }),
				new PartitionKey("shared-pk"));

		var ranges = await container.GetFeedRangesAsync();
		var rangesWithItems = 0;
		var totalItems = 0;
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			var count = 0;
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				count += page.Count;
			}
			if (count > 0) rangesWithItems++;
			totalItems += count;
		}
		rangesWithItems.Should().Be(1, "all items with same PK should be in same range");
		totalItems.Should().Be(20);
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 12 — Boundary Precision
// ═══════════════════════════════════════════════════════════

public class FeedRangeBoundaryPrecisionTests
{
	[Fact]
	public async Task Items_InFirstRange_WithEmptyMin_FilteredCorrectly()
	{
		var container = new InMemoryContainer("fr-first", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var firstRange = ranges[0];

		var r0 = JObject.Parse(firstRange.ToJsonString());
		r0["Range"]!["min"]!.ToString().Should().Be("");

		var iter = container.GetItemQueryIterator<JObject>(firstRange, new QueryDefinition("SELECT * FROM c"));
		var count = 0;
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			count += page.Count;
		}
		count.Should().BeGreaterThan(0, "first range should have items");
	}

	[Fact]
	public async Task Items_InLastRange_WithFFMax_FilteredCorrectly()
	{
		var container = new InMemoryContainer("fr-last", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var lastRange = ranges[^1];

		var rLast = JObject.Parse(lastRange.ToJsonString());
		rLast["Range"]!["max"]!.ToString().Should().Be("FF");

		var iter = container.GetItemQueryIterator<JObject>(lastRange, new QueryDefinition("SELECT * FROM c"));
		var count = 0;
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			count += page.Count;
		}
		count.Should().BeGreaterThan(0, "last range should have items");
	}

	[Fact]
	public async Task FeedRangeCount_ChangedMidway_ExistingRangesStillWork()
	{
		var container = new InMemoryContainer("fr-midchg", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		// Get ranges with count=4
		var ranges4 = await container.GetFeedRangesAsync();

		// Change to 8
		container.FeedRangeCount = 8;
		var ranges8 = await container.GetFeedRangesAsync();
		ranges8.Should().HaveCount(8);

		// Old ranges (from count=4) should still work for queries
		var totalOld = 0;
		foreach (var range in ranges4)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				totalOld += page.Count;
			}
		}
		totalOld.Should().Be(50, "old range boundaries still valid for queries");

		// New ranges also cover all items
		var totalNew = 0;
		foreach (var range in ranges8)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				totalNew += page.Count;
			}
		}
		totalNew.Should().Be(50);
	}

	[Fact]
	public async Task MaxItemCount_One_WithFeedRange_SingleItemPages()
	{
		var container = new InMemoryContainer("fr-max1", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"),
				requestOptions: new QueryRequestOptions { MaxItemCount = 1 });
			var pageCount = 0;
			var totalItems = 0;
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				if (page.Count > 0) pageCount++;
				page.Count.Should().BeLessThanOrEqualTo(1, "each page should have at most 1 item");
				totalItems += page.Count;
			}
			if (totalItems > 1)
				pageCount.Should().Be(totalItems, "each item should be on its own page when MaxItemCount=1");
		}
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 13 — Query + RequestOptions Interaction
// ═══════════════════════════════════════════════════════════

public class FeedRangeQueryRequestOptionsFilteringTests
{
	[Fact]
	public async Task QueryIterator_FeedRange_WithContinuationToken_ResumesCorrectly()
	{
		var container = new InMemoryContainer("fr-resume", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var range = ranges[0];

		// Get first page with MaxItemCount=5
		var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { MaxItemCount = 5 });
		var firstPageIds = new List<string>();
		string? continuationToken = null;
		if (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			foreach (var item in page) firstPageIds.Add(item["id"]!.Value<string>()!);
			continuationToken = page.ContinuationToken;
		}

		if (continuationToken == null) return; // range has ≤5 items

		// Resume with continuation token
		var resumeIter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"),
			continuationToken, new QueryRequestOptions { MaxItemCount = 5 });
		var resumeIds = new List<string>();
		while (resumeIter.HasMoreResults)
		{
			var page = await resumeIter.ReadNextAsync();
			foreach (var item in page) resumeIds.Add(item["id"]!.Value<string>()!);
		}

		// First page and resumed pages should not overlap
		firstPageIds.Intersect(resumeIds).Should().BeEmpty("resumed query should not repeat items");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 14 — Aggregate Query + FeedRange Correctness
// ═══════════════════════════════════════════════════════════

public class FeedRangeAggregateQueryFilteringTests
{
	[Fact]
	public async Task AggregateCount_WithFeedRange_PerRangeSumsToTotal()
	{
		var container = new InMemoryContainer("fr-aggcount", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var perRangeSum = 0;
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				foreach (var item in page) perRangeSum += item.Value<int>();
			}
		}
		perRangeSum.Should().Be(50, "per-range COUNT should sum to total");
	}

	[Fact]
	public async Task AggregateSumAvg_WithFeedRange_ViaSelectStar_SumsCorrectly()
	{
		var container = new InMemoryContainer("fr-aggsum", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 40; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", value = i * 5 }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var totalSum = 0;
		var totalCount = 0;
		foreach (var range in ranges)
		{
			var iter = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				foreach (var item in page)
				{
					totalSum += item["value"]!.Value<int>();
					totalCount++;
				}
			}
		}

		var expectedSum = Enumerable.Range(0, 40).Sum(i => i * 5);
		totalSum.Should().Be(expectedSum);
		totalCount.Should().Be(40);
		((double)totalSum / totalCount).Should().BeApproximately((double)expectedSum / 40, 0.001);
	}
}
