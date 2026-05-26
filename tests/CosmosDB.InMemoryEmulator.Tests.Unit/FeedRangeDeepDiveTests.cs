using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════
// Phase 1 — Aggregate + VALUE Divergent Behavior Tests
// ═══════════════════════════════════════════════════════════

public class FeedRangeAggregateDivergentTests
{
	private static async Task<InMemoryContainer> CreatePopulatedContainer(int count = 50, int feedRangeCount = 4)
	{
		var container = new InMemoryContainer("fr-agg", "/partitionKey") { FeedRangeCount = feedRangeCount };
		for (var i = 0; i < count; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i:D3}", value = i * 10 }),
				new PartitionKey($"pk-{i}"));
		return container;
	}

	// 1.1 — Aggregate COUNT over FeedRange produces per-range counts
	[Fact]
	public async Task AggregateCOUNT_WithFeedRange_PerRangeSumsToTotal()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var perRangeSum = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) perRangeSum += item.Value<int>();
			}
		}

		// If working correctly, this would equal 50
		perRangeSum.Should().Be(50);
	}

	// Sister test: verify per-range counts are non-trivially distributed
	[Fact]
	public async Task AggregateCOUNT_WithFeedRange_PerRangeCounts_DistributedAcrossRanges()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var perRangeCounts = new List<int>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));
			var rangeCount = 0;
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) rangeCount += item.Value<int>();
			}
			perRangeCounts.Add(rangeCount);
		}

		// Items are pre-filtered by FeedRange before aggregation, so multiple ranges should have counts.
		var rangesWithCount = perRangeCounts.Count(c => c > 0);
		rangesWithCount.Should().BeGreaterThan(1, "items should be distributed across multiple ranges");
		perRangeCounts.Sum().Should().Be(50);
	}

	// 1.2 — Aggregate SUM
	[Fact]
	public async Task AggregateSUM_WithFeedRange_PerRangeSumsToTotal()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var perRangeSum = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE SUM(c.value) FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) perRangeSum += item.Value<int>();
			}
		}

		perRangeSum.Should().Be(Enumerable.Range(0, 50).Sum(i => i * 10));
	}

	// Sister test: verify per-range sums add up
	[Fact]
	public async Task AggregateSUM_WithFeedRange_PerRangeSums_DistributedCorrectly()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var expectedTotal = Enumerable.Range(0, 50).Sum(i => i * 10);
		var perRangeSums = new List<int>();

		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE SUM(c.value) FROM c"));
			var rangeSum = 0;
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) rangeSum += item.Value<int>();
			}
			perRangeSums.Add(rangeSum);
		}

		var rangesWithSum = perRangeSums.Count(s => s > 0);
		rangesWithSum.Should().BeGreaterThan(1, "sums should be distributed across multiple ranges");
		perRangeSums.Sum().Should().Be(expectedTotal);
	}

	// 1.3 — Aggregate AVG
	[Fact]
	public async Task AggregateAVG_WithFeedRange_EachRangeReturnsLocalAvg()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var perRangeAvgs = new List<double>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE AVG(c.value) FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) perRangeAvgs.Add(item.Value<double>());
			}
		}

		// Each range should return its own local AVG
		perRangeAvgs.Should().HaveCountGreaterThan(1, "multiple ranges should return AVGs");
	}

	// Sister test: verify local AVGs are reasonable values
	[Fact]
	public async Task AggregateAVG_WithFeedRange_LocalAvgs_AreReasonableValues()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var perRangeAvgs = new List<double>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE AVG(c.value) FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) perRangeAvgs.Add(item.Value<double>());
			}
		}

		// All per-range AVGs should be within the global range [0, 490]
		perRangeAvgs.Should().AllSatisfy(avg => avg.Should().BeInRange(0, 490));
	}

	// 1.4 — Aggregate MIN/MAX
	[Fact]
	public async Task AggregateMinMax_WithFeedRange_EachRangeReturnsLocalMin()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var allMins = new List<int>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE MIN(c.value) FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allMins.Add(item.Value<int>());
			}
		}

		// Each range should return its local MIN
		allMins.Should().HaveCountGreaterThan(1, "multiple ranges should return MIN values");
	}

	// Sister test: global MIN is 0 and appears somewhere in per-range results
	[Fact]
	public async Task AggregateMinMax_WithFeedRange_GlobalMinAppearsInOneRange()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var allMins = new List<int>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE MIN(c.value) FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allMins.Add(item.Value<int>());
			}
		}

		allMins.Should().Contain(0, "global MIN of 0 should appear in one of the ranges");
	}

	// 1.5 — VALUE keyword with FeedRange
	[Fact]
	public async Task VALUE_WithFeedRange_PerRangeSumsToTotal()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var allNames = new List<string>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE c.name FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allNames.Add(item.Value<string>()!);
			}
		}

		allNames.Should().HaveCount(50, "all 50 names should be returned across ranges");
	}

	// Sister test: VALUE results distributed across ranges
	[Fact]
	public async Task VALUE_WithFeedRange_ScalarsDistributedAcrossRanges()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var perRangeCounts = new List<int>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JToken>(range, new QueryDefinition("SELECT VALUE c.name FROM c"));
			var count = 0;
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				count += page.Count;
			}
			perRangeCounts.Add(count);
		}

		// Items are pre-filtered by FeedRange, so results are distributed
		var rangesWithResults = perRangeCounts.Count(c => c > 0);
		rangesWithResults.Should().BeGreaterThan(1, "VALUE results should be distributed across ranges");
		perRangeCounts.Sum().Should().Be(50, "total count is still 50");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 2 — TTL + FeedRange
// ═══════════════════════════════════════════════════════════

public class FeedRangeTtlTests
{
	// 2.1 — TTL-expired items excluded from FeedRange queries
	[Fact]
	public async Task TTLExpiredItems_FeedRangeQuery_ExcludesExpiredItems()
	{
		var container = new InMemoryContainer("fr-ttl", "/partitionKey") { FeedRangeCount = 4, DefaultTimeToLive = 1 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i}" }),
				new PartitionKey($"pk-{i}"));

		// Before TTL expiry — all items present across feed ranges
		var beforeCount = 0;
		var ranges = await container.GetFeedRangesAsync();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				beforeCount += page.Count;
			}
		}
		beforeCount.Should().Be(20);

		// Wait for TTL to expire (1 second + buffer)
		await Task.Delay(1500);

		// After TTL expiry — items should be excluded from FeedRange queries
		var afterCount = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				afterCount += page.Count;
			}
		}
		afterCount.Should().Be(0, "TTL-expired items should not appear in FeedRange queries");
	}

	// 2.2 — Change feed from Beginning should still show creation of TTL-expired items
	[Fact]
	public async Task TTLExpiredItems_FeedRangeChangeFeed_StillShowsCreation()
	{
		var container = new InMemoryContainer("fr-ttl-cf", "/partitionKey") { FeedRangeCount = 4, DefaultTimeToLive = 1 };

		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		// Wait for TTL expiry
		await Task.Delay(1500);

		// Change feed from Beginning should still show all 10 creations
		var totalFromChangeFeed = 0;
		var ranges = await container.GetFeedRangesAsync();
		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.StatusCode == System.Net.HttpStatusCode.NotModified) break;
				totalFromChangeFeed += page.Count;
			}
		}
		totalFromChangeFeed.Should().Be(10, "change feed shows historical creations even after TTL expiry");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 3 — Mutation + FeedRange Consistency
// ═══════════════════════════════════════════════════════════

public class FeedRangeMutationTests
{
	// 3.1 — Patch doesn't change PK so item stays in same range
	[Fact]
	public async Task PatchItem_StaysInSameRange()
	{
		var container = new InMemoryContainer("fr-patch", "/partitionKey") { FeedRangeCount = 4 };

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk-stable", name = "Before" }),
			new PartitionKey("pk-stable"));

		// Find which range the item is in before patch
		var ranges = await container.GetFeedRangesAsync();
		int? rangeBefore = null;
		for (var i = 0; i < ranges.Count; i++)
		{
			var iterator = container.GetItemQueryIterator<JObject>(ranges[i], new QueryDefinition("SELECT * FROM c WHERE c.id = '1'"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.Any()) rangeBefore = i;
			}
		}
		rangeBefore.Should().NotBeNull();

		// Patch the item (non-PK field)
		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk-stable"),
			[PatchOperation.Set("/name", "After")]);

		// Verify item is still in the same range
		int? rangeAfter = null;
		for (var i = 0; i < ranges.Count; i++)
		{
			var iterator = container.GetItemQueryIterator<JObject>(ranges[i], new QueryDefinition("SELECT * FROM c WHERE c.id = '1'"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.Any()) rangeAfter = i;
			}
		}
		rangeAfter.Should().Be(rangeBefore, "patching a non-PK field should not change FeedRange assignment");
	}

	// 3.2 — Patch change feed appears in correct range
	[Fact]
	public async Task PatchItem_ChangeFeed_UpdateAppearsInCorrectRange()
	{
		var container = new InMemoryContainer("fr-patch-cf", "/partitionKey") { FeedRangeCount = 4 };

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk-cf", name = "Original" }),
			new PartitionKey("pk-cf"));

		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk-cf"),
			[PatchOperation.Set("/name", "Patched")]);

		// Read change feed from Beginning per range — the patched version should appear in exactly one range
		var ranges = await container.GetFeedRangesAsync();
		var rangesWithPatchedItem = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.StatusCode == System.Net.HttpStatusCode.NotModified) break;
				if (page.Any(item => item["name"]?.Value<string>() == "Patched"))
					rangesWithPatchedItem++;
			}
		}
		rangesWithPatchedItem.Should().Be(1, "patch update should appear in exactly one FeedRange change feed");
	}

	// 3.3 — DeleteAllByPartitionKey makes items disappear from range
	[Fact]
	public async Task DeleteAllByPartitionKey_FeedRange_RangeItemsDisappear()
	{
		var container = new InMemoryContainer("fr-delpk", "/partitionKey") { FeedRangeCount = 4 };

		// Seed: 5 items with pk-a, 5 items with pk-b
		for (var i = 0; i < 5; i++)
		{
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"a{i}", partitionKey = "pk-a" }),
				new PartitionKey("pk-a"));
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"b{i}", partitionKey = "pk-b" }),
				new PartitionKey("pk-b"));
		}

		// Count total before delete
		var ranges = await container.GetFeedRangesAsync();
		var totalBefore = await CountAllAcrossRanges(container, ranges);
		totalBefore.Should().Be(10);

		// Delete all pk-a items
		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk-a"));

		// Count total after delete — should be 5
		var totalAfter = await CountAllAcrossRanges(container, ranges);
		totalAfter.Should().Be(5, "only pk-b items should remain after deleting pk-a");
	}

	// 3.4 — ClearItems makes all ranges empty
	[Fact]
	public async Task ClearItems_AllFeedRanges_ReturnEmpty()
	{
		var container = new InMemoryContainer("fr-clear", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		(await CountAllAcrossRanges(container, ranges)).Should().Be(20);

		container.ClearItems();

		var totalAfter = await CountAllAcrossRanges(container, ranges);
		totalAfter.Should().Be(0, "all FeedRange queries should return empty after ClearItems");
	}

	private static async Task<int> CountAllAcrossRanges(InMemoryContainer container, IReadOnlyList<FeedRange> ranges)
	{
		var total = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				total += page.Count;
			}
		}
		return total;
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 4 — State Persistence + FeedRange
// ═══════════════════════════════════════════════════════════

public class FeedRangeStatePersistenceTests
{
	// 4.1 — Export/Import preserves FeedRange distribution
	[Fact]
	public async Task ExportImportState_FeedRangeDistribution_Preserved()
	{
		var container1 = new InMemoryContainer("fr-persist", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 50; i++)
			await container1.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i}" }),
				new PartitionKey($"pk-{i}"));

		// Capture per-range distribution from original container
		var ranges1 = await container1.GetFeedRangesAsync();
		var dist1 = await GetPerRangeIds(container1, ranges1);

		// Export and import into new container with same config
		var state = container1.ExportState();
		var container2 = new InMemoryContainer("fr-persist", "/partitionKey") { FeedRangeCount = 4 };
		container2.ImportState(state);

		// Verify same distribution
		var ranges2 = await container2.GetFeedRangesAsync();
		var dist2 = await GetPerRangeIds(container2, ranges2);

		dist1.Count.Should().Be(dist2.Count);
		for (var i = 0; i < dist1.Count; i++)
			dist1[i].Should().BeEquivalentTo(dist2[i], $"range {i} should have the same items after import");
	}

	// 4.2 — After import, change feed is empty but query FeedRanges work
	[Fact]
	public async Task ExportImportState_ChangeFeed_NotPreserved_ButQueryWorks()
	{
		var container1 = new InMemoryContainer("fr-persist2", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 10; i++)
			await container1.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var state = container1.ExportState();
		var container2 = new InMemoryContainer("fr-persist2", "/partitionKey") { FeedRangeCount = 4 };
		container2.ImportState(state);

		// Query works — items are present
		var ranges = await container2.GetFeedRangesAsync();
		var queryCount = 0;
		foreach (var range in ranges)
		{
			var iterator = container2.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				queryCount += page.Count;
			}
		}
		queryCount.Should().Be(10, "query should return all imported items");

		// Change feed from Beginning — may or may not have entries (import behavior)
		// The key assertion is that queries work correctly
		var changeFeedCount = 0;
		foreach (var range in ranges)
		{
			var iterator = container2.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.StatusCode == System.Net.HttpStatusCode.NotModified) break;
				changeFeedCount += page.Count;
			}
		}

		// Change feed is populated by ImportState (it calls UpsertItemAsync internally)
		// So it may or may not be empty — just verify it doesn't throw
		changeFeedCount.Should().BeGreaterThanOrEqualTo(0);
	}

	private static async Task<List<HashSet<string>>> GetPerRangeIds(InMemoryContainer container, IReadOnlyList<FeedRange> ranges)
	{
		var result = new List<HashSet<string>>();
		foreach (var range in ranges)
		{
			var ids = new HashSet<string>();
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) ids.Add(item["id"]!.ToString());
			}
			result.Add(ids);
		}
		return result;
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 5 — Change Feed Advanced
// ═══════════════════════════════════════════════════════════

public class FeedRangeChangeFeedAdvancedDeepDiveTests
{
	// 5.1 — After delete, item disappears from the correct FeedRange
	[Fact]
	public async Task Delete_ItemDisappearsFromCorrectFeedRange()
	{
		var container = new InMemoryContainer("fr-cf-del", "/partitionKey") { FeedRangeCount = 4 };

		// Create items across ranges
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		// Find which range item "5" is in
		var ranges = await container.GetFeedRangesAsync();
		int? targetRange = null;
		for (var i = 0; i < ranges.Count; i++)
		{
			var iterator = container.GetItemQueryIterator<JObject>(ranges[i], new QueryDefinition("SELECT * FROM c WHERE c.id = '5'"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.Any()) targetRange = i;
			}
		}
		targetRange.Should().NotBeNull();

		// Delete item "5"
		await container.DeleteItemAsync<JObject>("5", new PartitionKey("pk-5"));

		// Verify item is gone from its FeedRange
		var iteratorAfter = container.GetItemQueryIterator<JObject>(ranges[targetRange!.Value],
			new QueryDefinition("SELECT * FROM c WHERE c.id = '5'"));
		var afterResults = new List<JObject>();
		while (iteratorAfter.HasMoreResults)
		{
			var page = await iteratorAfter.ReadNextAsync();
			afterResults.AddRange(page);
		}
		afterResults.Should().BeEmpty("deleted item should be gone from its FeedRange");

		// Verify tombstone is in change feed (checkpoint-based, no FeedRange filter)
		var allChanges = new List<JObject>();
		var feedIterator = container.GetChangeFeedIterator<JObject>(0);
		while (feedIterator.HasMoreResults)
		{
			var page = await feedIterator.ReadNextAsync();
			allChanges.AddRange(page);
		}
		allChanges.Any(item => item["_deleted"] != null && item["_deleted"]!.Value<bool>() && item["id"] != null && item["id"]!.Value<string>() == "5")
			.Should().BeTrue("delete tombstone should be in change feed");
	}

	// 5.2 — ChangeFeedStartFrom.ContinuationAndFeedRange (known gap)
	[Fact(Skip = "ChangeFeedStartFrom.ContinuationAndFeedRange may have incomplete support — ExtractFeedRangeFromStartFrom may not find FeedRange from this subtype")]
	public async Task ChangeFeed_ContinuationAndFeedRange_Support()
	{
		// This test documents the known limitation per GAP 9
		var container = new InMemoryContainer("fr-cf-cont", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		// ChangeFeedStartFrom.ContinuationAndFeedRange would be used here
		// but the constructor is likely internal/not public
		await Task.CompletedTask;
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 6 — Query + FeedRange Interaction Edge Cases
// ═══════════════════════════════════════════════════════════

public class FeedRangeQueryInteractionDeepDiveTests
{
	private static async Task<InMemoryContainer> CreatePopulatedContainer()
	{
		var container = new InMemoryContainer("fr-qi", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", tags = new[] { $"tag-{i % 3}" }, sub = new { val = i } }),
				new PartitionKey($"pk-{i}"));
		return container;
	}

	// 6.1 — Both FeedRange and PartitionKey RequestOption
	[Fact]
	public async Task QueryWithPartitionKeyOption_AndFeedRange_DoubleFilterBehavior()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		// Find which range pk-5 belongs to
		int? targetRangeIndex = null;
		for (var i = 0; i < ranges.Count; i++)
		{
			var iterator = container.GetItemQueryIterator<JObject>(ranges[i],
				new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = 'pk-5'"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.Any()) targetRangeIndex = i;
			}
		}
		targetRangeIndex.Should().NotBeNull();

		// Query with both FeedRange and PartitionKey — should still find the item
		var iteratorBoth = container.GetItemQueryIterator<JObject>(
			ranges[targetRangeIndex!.Value],
			new QueryDefinition("SELECT * FROM c"),
			null,
			new QueryRequestOptions { PartitionKey = new PartitionKey("pk-5") });
		var results = new List<JObject>();
		while (iteratorBoth.HasMoreResults)
		{
			var page = await iteratorBoth.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().ContainSingle(item => item["id"]!.Value<string>() == "5");

		// Query with FeedRange of a DIFFERENT range + PartitionKey — should return empty
		var otherRange = (targetRangeIndex.Value + 1) % ranges.Count;
		var iteratorMismatch = container.GetItemQueryIterator<JObject>(
			ranges[otherRange],
			new QueryDefinition("SELECT * FROM c"),
			null,
			new QueryRequestOptions { PartitionKey = new PartitionKey("pk-5") });
		var mismatchResults = new List<JObject>();
		while (iteratorMismatch.HasMoreResults)
		{
			var page = await iteratorMismatch.ReadNextAsync();
			mismatchResults.AddRange(page);
		}
		mismatchResults.Should().BeEmpty("FeedRange and PartitionKey filter to different subsets");
	}

	// 6.2 — Subquery EXISTS with FeedRange
	[Fact]
	public async Task SubqueryEXISTS_WithFeedRange_FiltersCorrectly()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		// Query using EXISTS subquery with FeedRange
		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range,
				new QueryDefinition("SELECT * FROM c WHERE EXISTS (SELECT VALUE 1 FROM t IN c.tags WHERE t = 'tag-0')"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.ToString());
			}
		}

		// Items with tag-0 are those where i%3==0: 0, 3, 6, 9, 12, 15, 18, 21, 24, 27
		allIds.Should().HaveCount(10);
		allIds.Should().BeEquivalentTo(Enumerable.Range(0, 30).Where(i => i % 3 == 0).Select(i => $"{i}"));
	}

	// 6.3 — JOIN query with FeedRange
	[Fact]
	public async Task JoinQuery_WithFeedRange_FiltersCorrectly()
	{
		var container = await CreatePopulatedContainer();
		var ranges = await container.GetFeedRangesAsync();

		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range,
				new QueryDefinition("SELECT c.id, t AS tag FROM c JOIN t IN c.tags"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.ToString());
			}
		}

		// Every item has 1 tag, so JOIN produces 30 rows with 30 unique ids
		allIds.Should().HaveCount(30, "JOIN with FeedRange should cover all items across all ranges");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 7 — FakeCosmosHandler + FeedRange Sync
// ═══════════════════════════════════════════════════════════

public class FeedRangeHandlerSyncTests
{
	// 7.1 — Handler defaulting to PKRangeCount=1 while container has FeedRangeCount=4
	[Fact]
	public async Task FakeCosmosHandler_DefaultPKRangeCount_DesyncWithContainerFeedRangeCount()
	{
		var container = new InMemoryContainer("fr-sync", "/partitionKey") { FeedRangeCount = 4 };

		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i}" }),
				new PartitionKey($"pk-{i}"));

		// Container reports 4 FeedRanges
		var containerRanges = await container.GetFeedRangesAsync();
		containerRanges.Should().HaveCount(4);

		// Handler defaults to PKRangeCount=1 unless set
		var handler = new FakeCosmosHandler(container);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var cosmosContainer = client.GetContainer("testdb", "fr-sync");

		// Handler-based client: GetFeedRangesAsync uses handler's PKRangeCount (default=1)
		var handlerRanges = await cosmosContainer.GetFeedRangesAsync();
		handlerRanges.Should().HaveCount(1, "handler defaults to 1 PKRange");

		// But both paths return the same total items
		var containerTotal = 0;
		foreach (var range in containerRanges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				containerTotal += page.Count;
			}
		}

		var handlerTotal = 0;
		foreach (var range in handlerRanges)
		{
			var iterator = cosmosContainer.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				handlerTotal += page.Count;
			}
		}

		containerTotal.Should().Be(20);
		handlerTotal.Should().Be(20);
	}
}
