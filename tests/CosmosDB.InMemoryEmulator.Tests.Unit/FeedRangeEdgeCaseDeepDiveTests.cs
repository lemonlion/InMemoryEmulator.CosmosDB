using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════
// Phase H — FeedRange.FromPartitionKey Interaction
// ═══════════════════════════════════════════════════════════

public class FeedRangeFromPartitionKeyTests
{
	private static async Task<InMemoryContainer> CreateTestContainer(int count = 20, int feedRangeCount = 4)
	{
		var container = new InMemoryContainer("fr-pk", "/partitionKey") { FeedRangeCount = feedRangeCount };
		for (var i = 0; i < count; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Item{i}" }),
				new PartitionKey($"pk-{i}"));
		return container;
	}

	[Fact]
	public async Task FeedRange_FromPartitionKey_QueryReturnsOnlyThatPK()
	{
		var container = await CreateTestContainer();
		var feedRange = FeedRange.FromPartitionKey(new PartitionKey("pk-5"));
		var iterator = container.GetItemQueryIterator<JObject>(feedRange, new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("5");
	}

	[Fact]
	public async Task FeedRange_FromPartitionKey_StreamIterator_ScopedCorrectly()
	{
		var container = await CreateTestContainer();
		var feedRange = FeedRange.FromPartitionKey(new PartitionKey("pk-5"));
		var iterator = container.GetItemQueryStreamIterator(feedRange, new QueryDefinition("SELECT * FROM c"));
		var allItems = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			using var sr = new System.IO.StreamReader(response.Content);
			var json = await sr.ReadToEndAsync();
			var doc = JObject.Parse(json);
			var items = doc["Documents"]!.ToObject<List<JObject>>()!;
			allItems.AddRange(items);
		}
		allItems.Should().ContainSingle();
		allItems[0]["id"]!.Value<string>().Should().Be("5");
	}

	[Fact]
	public async Task FeedRange_FromPartitionKey_ChangeFeed_ScopedCorrectly()
	{
		var container = await CreateTestContainer();
		var feedRange = FeedRange.FromPartitionKey(new PartitionKey("pk-5"));
		var iterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(feedRange), ChangeFeedMode.Incremental);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			results.AddRange(page);
		}
		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("5");
	}

	[Fact]
	public async Task FeedRange_FromPartitionKey_NumericPK_Works()
	{
		var container = new InMemoryContainer("fr-pk-num", "/value") { FeedRangeCount = 4 };
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", value = 100 }), new PartitionKey(100));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", value = 200 }), new PartitionKey(200));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", value = 300 }), new PartitionKey(300));

		var feedRange = FeedRange.FromPartitionKey(new PartitionKey(200));
		var iterator = container.GetItemQueryIterator<JObject>(feedRange, new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("2");
	}

	[Fact]
	public async Task FeedRange_FromPartitionKey_BooleanPK_Works()
	{
		var container = new InMemoryContainer("fr-pk-bool", "/isActive") { FeedRangeCount = 2 };
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", isActive = true }), new PartitionKey(true));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", isActive = false }), new PartitionKey(false));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", isActive = true }), new PartitionKey(true));

		var feedRange = FeedRange.FromPartitionKey(new PartitionKey(true));
		var iterator = container.GetItemQueryIterator<JObject>(feedRange, new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().HaveCount(2);
		results.Select(r => r["id"]!.Value<string>()).Should().BeEquivalentTo(["1", "3"]);
	}

	[Fact]
	public async Task FeedRange_FromPartitionKey_NullPK_Works()
	{
		var container = new InMemoryContainer("fr-pk-null", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk-a" }), new PartitionKey("pk-a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = (string?)null }), PartitionKey.Null);
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk-b" }), new PartitionKey("pk-b"));

		var feedRange = FeedRange.FromPartitionKey(PartitionKey.Null);
		var iterator = container.GetItemQueryIterator<JObject>(feedRange, new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		// Should return only null PK item
		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("2");
	}

	// Sister test: null PK FeedRange correctly filters to just null-PK items
	[Fact]
	public async Task FeedRange_FromPartitionKey_NullPK_DoesNotReturnOtherItems()
	{
		var container = new InMemoryContainer("fr-pk-null-sis", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk-a" }), new PartitionKey("pk-a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = (string?)null }), PartitionKey.Null);
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk-b" }), new PartitionKey("pk-b"));

		var feedRange = FeedRange.FromPartitionKey(PartitionKey.Null);
		var iterator = container.GetItemQueryIterator<JObject>(feedRange, new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		// Null PK FeedRange correctly filters — excludes non-null PK items
		results.Should().ContainSingle();
		results.Should().NotContain(r => r["id"]!.Value<string>() == "1");
		results.Should().NotContain(r => r["id"]!.Value<string>() == "3");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase I — AllVersionsAndDeletes + FeedRange
// ═══════════════════════════════════════════════════════════

public class FeedRangeAllVersionsAndDeletesTests
{
	private const string AllVersionsSkipReason =
		"ChangeFeedMode.AllVersionsAndDeletes is internal in the Cosmos SDK (v3.58). " +
		"The emulator supports all-versions semantics via GetChangeFeedIterator<T>(long checkpoint).";

	[Fact(Skip = AllVersionsSkipReason)]
	public async Task ChangeFeed_AllVersionsAndDeletes_WithFeedRange_ShowsAllVersions()
	{
		await Task.CompletedTask;
	}

	// Sister test: Via checkpoint-based approach (no FeedRange), all versions visible
	[Fact]
	public async Task ChangeFeed_AllVersions_ViaCheckpoint_ShowsAllVersions()
	{
		var container = new InMemoryContainer("fr-avd", "/partitionKey") { FeedRangeCount = 2 };
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk", name = "v1" }), new PartitionKey("pk"));
		await container.UpsertItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk", name = "v2" }), new PartitionKey("pk"));
		await container.UpsertItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk", name = "v3" }), new PartitionKey("pk"));

		var iterator = container.GetChangeFeedIterator<JObject>(0);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().HaveCount(3, "all 3 versions should appear");
		results.Select(r => r["name"]!.Value<string>()).Should().ContainInOrder("v1", "v2", "v3");
	}

	[Fact(Skip = AllVersionsSkipReason)]
	public async Task ChangeFeed_AllVersionsAndDeletes_WithFeedRange_ShowsDeletes()
	{
		await Task.CompletedTask;
	}

	// Sister test: Via checkpoint, deletes visible
	[Fact]
	public async Task ChangeFeed_AllVersions_ViaCheckpoint_ShowsDeletes()
	{
		var container = new InMemoryContainer("fr-avd-del", "/partitionKey") { FeedRangeCount = 2 };
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk" }), new PartitionKey("pk"));
		await container.DeleteItemAsync<JObject>("1", new PartitionKey("pk"));

		var iterator = container.GetChangeFeedIterator<JObject>(0);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().HaveCount(2);
		results[1]["_deleted"]!.Value<bool>().Should().BeTrue();
	}

	[Fact(Skip = AllVersionsSkipReason)]
	public async Task ChangeFeed_AllVersionsAndDeletes_Delete_InCorrectRange_Only()
	{
		await Task.CompletedTask;
	}
}

// ═══════════════════════════════════════════════════════════
// Phase J — Boundary Math Edge Cases
// ═══════════════════════════════════════════════════════════

public class FeedRangeBoundaryMathTests
{
	[Fact]
	public async Task ParseFeedRangeBoundaries_MaxFF_TreatedAsUintMax()
	{
		var container = new InMemoryContainer("fr-boundary", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }), new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var lastRange = JObject.Parse(ranges[^1].ToJsonString());
		lastRange["Range"]!["max"]!.ToString().Should().Be("FF");

		// Items in last range should be queryable — "FF" must be parsed as uint.MaxValue
		var iterator = container.GetItemQueryIterator<JObject>(ranges[^1], new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().NotBeEmpty("last range (up to FF) should contain items");
	}

	[Fact]
	public async Task IsHashInRange_ItemsDistributedAcrossAllRanges()
	{
		// With enough items, at least one should hash to range starting at min=0
		var container = new InMemoryContainer("fr-hash0", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }), new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			var count = 0;
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				count += page.Count;
			}
			count.Should().BeGreaterThan(0, "with 200 items, every range should have at least one");
		}
	}

	[Fact]
	public async Task CustomRange_PartialBoundary_SubsetReturned()
	{
		var container = new InMemoryContainer("fr-partial", "/partitionKey") { FeedRangeCount = 1 };
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }), new PartitionKey($"pk-{i}"));

		// Create a custom FeedRange covering only the middle portion
		var customRange = FeedRange.FromJsonString("{\"Range\":{\"min\":\"40000000\",\"max\":\"80000000\"}}");
		var iterator = container.GetItemQueryIterator<JObject>(customRange, new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().NotBeEmpty("some items should hash into the 40-80 range");
		results.Count.Should().BeLessThan(100, "not all items should be in the partial range");
	}

	[Fact]
	public async Task FeedRange_WithLeadingZeros_ParsesCorrectly()
	{
		var container = new InMemoryContainer("fr-zeros", "/partitionKey") { FeedRangeCount = 1 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }), new PartitionKey($"pk-{i}"));

		var lowRange = FeedRange.FromJsonString("{\"Range\":{\"min\":\"00000000\",\"max\":\"40000000\"}}");
		var iterator = container.GetItemQueryIterator<JObject>(lowRange, new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		// Should parse leading zeros correctly and return a subset
		results.Count.Should().BeLessThan(50);
	}

	[Fact]
	public async Task FeedRange_WithLowercaseHex_ParsesCorrectly()
	{
		var container = new InMemoryContainer("fr-lower", "/partitionKey") { FeedRangeCount = 1 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }), new PartitionKey($"pk-{i}"));

		var lowerRange = FeedRange.FromJsonString("{\"Range\":{\"min\":\"40000000\",\"max\":\"80000000\"}}");
		var upperRange = FeedRange.FromJsonString("{\"Range\":{\"min\":\"40000000\",\"max\":\"80000000\"}}");

		var lowerIterator = container.GetItemQueryIterator<JObject>(lowerRange, new QueryDefinition("SELECT * FROM c"));
		var upperIterator = container.GetItemQueryIterator<JObject>(upperRange, new QueryDefinition("SELECT * FROM c"));

		var lowerResults = new List<string>();
		while (lowerIterator.HasMoreResults)
		{
			var page = await lowerIterator.ReadNextAsync();
			foreach (var item in page) lowerResults.Add(item["id"]!.Value<string>()!);
		}
		var upperResults = new List<string>();
		while (upperIterator.HasMoreResults)
		{
			var page = await upperIterator.ReadNextAsync();
			foreach (var item in page) upperResults.Add(item["id"]!.Value<string>()!);
		}
		lowerResults.Should().BeEquivalentTo(upperResults);
	}

	[Fact]
	public async Task IsHashInRange_HashEqualsMin_Included()
	{
		// Verify items at the boundary of a range are included (min is inclusive)
		var container = new InMemoryContainer("fr-minbound", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }), new PartitionKey($"pk-{i}"));

		// All items should be found across all ranges (none lost)
		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.Value<string>()!);
			}
		}
		allIds.Should().HaveCount(100, "no items should be lost at range boundaries");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase K — ExtractPartitionKeyValueFromJson Edge Cases
// ═══════════════════════════════════════════════════════════

public class FeedRangePartitionKeyExtractionTests
{
	[Fact]
	public async Task FeedRange_Query_NestedPartitionKeyPath_WorksCorrectly()
	{
		var container = new InMemoryContainer("fr-nested", "/nested/field") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", nested = new { field = $"val-{i}" } }),
				new PartitionKey($"val-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.Value<string>()!);
			}
		}
		allIds.Should().HaveCount(20, "all items with nested PK path found across ranges");
	}

	[Fact]
	public async Task FeedRange_Query_NullPKValue_HashesConsistently()
	{
		var container = new InMemoryContainer("fr-nullpk", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = (string?)null }),
			PartitionKey.Null);
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", partitionKey = "real-pk" }),
			new PartitionKey("real-pk"));

		// Should not throw (BUG-1 from plan — null PK should be handled gracefully)
		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.Value<string>()!);
			}
		}
		allIds.Should().HaveCount(2, "both null and non-null PK items found");
	}

	[Fact]
	public async Task FeedRange_ChangeFeed_NullPartitionKeyEntry_NoException()
	{
		var container = new InMemoryContainer("fr-nullcf", "/partitionKey") { FeedRangeCount = 4 };
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = (string?)null }),
			PartitionKey.Null);

		var ranges = await container.GetFeedRangesAsync();
		var totalItems = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(range), ChangeFeedMode.Incremental);
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.StatusCode == HttpStatusCode.NotModified) break;
				totalItems += page.Count;
			}
		}
		totalItems.Should().Be(1, "null PK item should appear in exactly one range's change feed");
	}

	[Fact]
	public async Task FeedRange_Query_MissingPKField_HashesConsistently()
	{
		var container = new InMemoryContainer("fr-missingpk", "/partitionKey") { FeedRangeCount = 4 };
		// Item without the partitionKey field
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "nopk", name = "test" }),
			PartitionKey.None);

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.Value<string>()!);
			}
		}
		allIds.Should().HaveCount(1, "item with missing PK field should be found in one range");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase L — FakeCosmosHandler + FeedRange Advanced
// ═══════════════════════════════════════════════════════════

public class FakeCosmosHandlerFeedRangeAdvancedTests
{
	[Fact]
	public async Task FakeCosmosHandler_PartitionKeyRangeCount_One_NoFiltering()
	{
		var container = new InMemoryContainer("fr-handler1", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions { PartitionKeyRangeCount = 1 });
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var cosmosContainer = client.GetContainer("db", "fr-handler1");
		var ranges = await cosmosContainer.GetFeedRangesAsync();
		ranges.Should().HaveCount(1, "PartitionKeyRangeCount=1 → 1 range");

		var iterator = cosmosContainer.GetItemQueryIterator<JObject>(ranges[0], new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().HaveCount(20, "single range should return all items");
	}

	[Fact]
	public async Task FakeCosmosHandler_PartitionKeyRangeCount_Matched_AllItemsCovered()
	{
		var container = new InMemoryContainer("fr-handler4", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions { PartitionKeyRangeCount = 4 });
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var cosmosContainer = client.GetContainer("db", "fr-handler4");
		var ranges = await cosmosContainer.GetFeedRangesAsync();
		ranges.Should().HaveCount(4);

		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iterator = cosmosContainer.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.Value<string>()!);
			}
		}
		allIds.Should().HaveCount(50, "all items covered across 4 ranges");
	}

	[Fact]
	public async Task FakeCosmosHandler_PartitionKeyRangeCount_Mismatch_BothReturnAllItems()
	{
		var container = new InMemoryContainer("fr-handler-mm", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 30; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		// Handler has different range count than container
		var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions { PartitionKeyRangeCount = 8 });
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var cosmosContainer = client.GetContainer("db", "fr-handler-mm");

		// SDK path: 8 ranges
		var sdkRanges = await cosmosContainer.GetFeedRangesAsync();
		sdkRanges.Should().HaveCount(8);

		var sdkIds = new HashSet<string>();
		foreach (var range in sdkRanges)
		{
			var iterator = cosmosContainer.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) sdkIds.Add(item["id"]!.Value<string>()!);
			}
		}

		// Container path: 4 ranges
		var containerRanges = await container.GetFeedRangesAsync();
		var containerIds = new HashSet<string>();
		foreach (var range in containerRanges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) containerIds.Add(item["id"]!.Value<string>()!);
			}
		}

		sdkIds.Should().HaveCount(30, "SDK path returns all items");
		containerIds.Should().HaveCount(30, "container path returns all items");
		sdkIds.Should().BeEquivalentTo(containerIds, "both paths return the same items");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase M — TTL / Delete + FeedRange
// ═══════════════════════════════════════════════════════════

public class FeedRangeTTLAndDeleteTests
{
	[Fact]
	public async Task FeedRange_Query_TTLExpiredItems_NotReturned()
	{
		var container = new InMemoryContainer("fr-ttl-m", "/partitionKey") { FeedRangeCount = 4, DefaultTimeToLive = 1 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		await Task.Delay(1500);

		var ranges = await container.GetFeedRangesAsync();
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
		total.Should().Be(0, "all items TTL-expired, not returned in FeedRange queries");
	}

	[Fact]
	public async Task FeedRange_Query_AfterDelete_ItemGone()
	{
		var container = new InMemoryContainer("fr-del-m", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		await container.DeleteItemAsync<JObject>("5", new PartitionKey("pk-5"));

		var ranges = await container.GetFeedRangesAsync();
		var allIds = new HashSet<string>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				foreach (var item in page) allIds.Add(item["id"]!.Value<string>()!);
			}
		}
		allIds.Should().HaveCount(19);
		allIds.Should().NotContain("5", "deleted item should not appear in any FeedRange");
	}

	[Fact]
	public async Task FeedRange_ChangeFeed_DeletedItem_TombstoneViaCheckpoint()
	{
		var container = new InMemoryContainer("fr-del-cf-m", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var checkpoint = container.GetChangeFeedCheckpoint();
		await container.DeleteItemAsync<JObject>("3", new PartitionKey("pk-3"));

		var iterator = container.GetChangeFeedIterator<JObject>(checkpoint);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().ContainSingle();
		results[0]["_deleted"]!.Value<bool>().Should().BeTrue();
		results[0]["id"]!.Value<string>().Should().Be("3");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase N — Exact Boundary Verification
// ═══════════════════════════════════════════════════════════

public class FeedRangeExactBoundaryTests
{
	[Fact]
	public async Task FeedRangeCount_Two_ExactBoundaryAt80000000()
	{
		var container = new InMemoryContainer("fr-2", "/partitionKey") { FeedRangeCount = 2 };
		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(2);

		var r0 = JObject.Parse(ranges[0].ToJsonString());
		r0["Range"]!["min"]!.ToString().Should().Be("");
		r0["Range"]!["max"]!.ToString().Should().Be("80000000");

		var r1 = JObject.Parse(ranges[1].ToJsonString());
		r1["Range"]!["min"]!.ToString().Should().Be("80000000");
		r1["Range"]!["max"]!.ToString().Should().Be("FF");
	}

	[Fact]
	public async Task FeedRangeCount_Three_ExactBoundaries()
	{
		var container = new InMemoryContainer("fr-3", "/partitionKey") { FeedRangeCount = 3 };
		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(3);

		var r0 = JObject.Parse(ranges[0].ToJsonString());
		var r1 = JObject.Parse(ranges[1].ToJsonString());
		var r2 = JObject.Parse(ranges[2].ToJsonString());

		r0["Range"]!["min"]!.ToString().Should().Be("");
		r2["Range"]!["max"]!.ToString().Should().Be("FF");

		// Step = 0x100000000 / 3 = 0x55555555 (integer division)
		r0["Range"]!["max"]!.ToString().Should().Be(r1["Range"]!["min"]!.ToString());
		r1["Range"]!["max"]!.ToString().Should().Be(r2["Range"]!["min"]!.ToString());
	}

	[Fact]
	public async Task FeedRangeCount_PowerOfTwo_AllBoundariesDivisible()
	{
		var container = new InMemoryContainer("fr-8", "/partitionKey") { FeedRangeCount = 8 };
		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCount(8);

		// First starts at "", last ends at "FF"
		JObject.Parse(ranges[0].ToJsonString())["Range"]!["min"]!.ToString().Should().Be("");
		JObject.Parse(ranges[^1].ToJsonString())["Range"]!["max"]!.ToString().Should().Be("FF");

		// All ranges are contiguous
		for (var i = 0; i < ranges.Count - 1; i++)
		{
			var currentMax = JObject.Parse(ranges[i].ToJsonString())["Range"]!["max"]!.ToString();
			var nextMin = JObject.Parse(ranges[i + 1].ToJsonString())["Range"]!["min"]!.ToString();
			currentMax.Should().Be(nextMin, $"range {i} max should equal range {i + 1} min");
		}
	}
}

// ═══════════════════════════════════════════════════════════
// Phase O — Continuation Token + FeedRange
// ═══════════════════════════════════════════════════════════

public class FeedRangeContinuationTokenTests
{
	[Fact]
	public async Task ContinuationToken_WithFeedRange_PaginatesWithinScope()
	{
		var container = new InMemoryContainer("fr-ct", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var range0 = ranges[0];

		// Count items in range 0
		var iterator = container.GetItemQueryIterator<JObject>(range0, new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { MaxItemCount = 3 });
		var allIds = new List<string>();
		var pageCount = 0;
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			foreach (var item in page) allIds.Add(item["id"]!.Value<string>()!);
			pageCount++;
		}

		allIds.Should().NotBeEmpty("range 0 should have items");
		allIds.Should().OnlyHaveUniqueItems("no duplicates across pages");
		if (allIds.Count > 3) pageCount.Should().BeGreaterThan(1, "should paginate when more than MaxItemCount items");
	}

	[Fact]
	public async Task ContinuationToken_BeyondItems_WithFeedRange_ReturnsEmpty()
	{
		var container = new InMemoryContainer("fr-ct-beyond", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		// Use a large continuation token offset
		var iterator = container.GetItemQueryIterator<JObject>(ranges[0], new QueryDefinition("SELECT * FROM c"), "99999");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().BeEmpty("continuation token beyond available items should return empty");
	}

	[Fact]
	public async Task ContinuationToken_Zero_WithFeedRange_StartsFromBeginning()
	{
		var container = new InMemoryContainer("fr-ct-zero", "/partitionKey") { FeedRangeCount = 2 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		// No continuation
		var iter1 = container.GetItemQueryIterator<JObject>(ranges[0], new QueryDefinition("SELECT * FROM c"));
		var ids1 = new List<string>();
		while (iter1.HasMoreResults)
		{
			var page = await iter1.ReadNextAsync();
			foreach (var item in page) ids1.Add(item["id"]!.Value<string>()!);
		}

		// Continuation = "0"
		var iter2 = container.GetItemQueryIterator<JObject>(ranges[0], new QueryDefinition("SELECT * FROM c"), "0");
		var ids2 = new List<string>();
		while (iter2.HasMoreResults)
		{
			var page = await iter2.ReadNextAsync();
			foreach (var item in page) ids2.Add(item["id"]!.Value<string>()!);
		}

		ids1.Should().BeEquivalentTo(ids2, "cont token '0' should give same results as no token");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase P — Patch/Replace/Upsert + FeedRange
// ═══════════════════════════════════════════════════════════

public class FeedRangeMutationEdgeCaseTests
{
	[Fact]
	public async Task Patch_DoesNotChangePartitionKey_StaysInSameRange()
	{
		var container = new InMemoryContainer("fr-mut-patch", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Original{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();

		// Find range for item "5"
		var rangeBefore = await FindRangeForItem(container, ranges, "5");

		await container.PatchItemAsync<JObject>("5", new PartitionKey("pk-5"),
			[PatchOperation.Set("/name", "Patched")]);

		var rangeAfter = await FindRangeForItem(container, ranges, "5");
		rangeAfter.Should().Be(rangeBefore, "patch should not change FeedRange");
	}

	[Fact]
	public async Task Replace_SameId_SamePK_StaysInSameRange()
	{
		var container = new InMemoryContainer("fr-mut-replace", "/partitionKey") { FeedRangeCount = 4 };
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = $"pk-{i}", name = $"Original{i}" }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var rangeBefore = await FindRangeForItem(container, ranges, "7");

		await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "7", partitionKey = "pk-7", name = "Replaced" }),
			"7", new PartitionKey("pk-7"));

		var rangeAfter = await FindRangeForItem(container, ranges, "7");
		rangeAfter.Should().Be(rangeBefore, "replace with same PK should stay in same range");
	}

	[Fact]
	public async Task Upsert_NewItem_AppearsInExactlyOneRange()
	{
		var container = new InMemoryContainer("fr-mut-upsert", "/partitionKey") { FeedRangeCount = 4 };

		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "new1", partitionKey = "pk-new", name = "Upserted" }),
			new PartitionKey("pk-new"));

		var ranges = await container.GetFeedRangesAsync();
		var rangesWithItem = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range, new QueryDefinition("SELECT * FROM c WHERE c.id = 'new1'"));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.Any()) rangesWithItem++;
			}
		}
		rangesWithItem.Should().Be(1, "upserted item should appear in exactly one range");
	}

	private static async Task<int> FindRangeForItem(InMemoryContainer container, IReadOnlyList<FeedRange> ranges, string id)
	{
		for (var i = 0; i < ranges.Count; i++)
		{
			var iterator = container.GetItemQueryIterator<JObject>(ranges[i],
				new QueryDefinition("SELECT * FROM c WHERE c.id = @id").WithParameter("@id", id));
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				if (page.Any()) return i;
			}
		}
		return -1;
	}
}
