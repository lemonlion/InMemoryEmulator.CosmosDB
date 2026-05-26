using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan #26: PITR (Point-in-Time Restore) Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════


// ── Phase 11: Stream Operation Coverage ──────────────────────────────────────

public class PitrStreamOperationTests
{
	[Fact]
	public async Task RestoreToPointInTime_WithStreamUpsert_RestoresPreUpsertState()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.UpsertItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","partitionKey":"pk1","name":"StreamUpserted"}""")),
			new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);

		var read = (await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"))).Resource;
		read.Name.Should().Be("Original");
	}

	[Fact]
	public async Task RestoreToPointInTime_WithStreamReplace_RestoresPreReplaceState()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","partitionKey":"pk1","name":"StreamReplaced"}""")),
			"1", new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);

		var read = (await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"))).Resource;
		read.Name.Should().Be("Original");
	}

	[Fact]
	public async Task RestoreToPointInTime_WithPatchItemStreamAsync_RestoresPrePatchState()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")]);

		container.RestoreToPointInTime(restorePoint);

		var read = (await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"))).Resource;
		read.Name.Should().Be("Original");
	}
}


// ── Phase 12: Stored Procedure / Trigger / UDF Interaction ───────────────────

public class PitrSprocTriggerUdfTests
{
	[Fact]
	public async Task RestoreToPointInTime_StoredProcedureExecutionWorksAfterRestore()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterStoredProcedure("sproc1", (pk, args) =>
		{
			return """{"status":"ok"}""";
		});

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);

		// Sproc should still work after restore
		var result = await container.Scripts.ExecuteStoredProcedureAsync<string>(
			"sproc1", new PartitionKey("pk1"), []);
		result.Resource.Should().Contain("ok");
	}

	[Fact]
	public async Task RestoreToPointInTime_PreTriggerFiresOnPostRestoreWrites()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var triggerFired = false;

		container.RegisterTrigger("preT1", TriggerType.Pre, TriggerOperation.Create, (JObject doc) =>
		{
			triggerFired = true;
			return doc;
		});

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		container.RestoreToPointInTime(restorePoint);

		triggerFired = false; // Reset

		// Write after restore should fire the trigger
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "AfterRestore" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { PreTriggers = ["preT1"] });

		triggerFired.Should().BeTrue();
	}

	[Fact]
	public async Task RestoreToPointInTime_UdfQueryWorksAfterRestore()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterUdf("double", (args) => (int)Convert.ToInt64(args[0]) * 2);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Value = 5 },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/value", 99)]);

		container.RestoreToPointInTime(restorePoint);

		// UDF should work on restored data
		var query = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT udf.double(c.value) AS doubled FROM c"));
		var results = new List<JObject>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["doubled"]!.Value<int>().Should().Be(10);
	}
}


// ── Phase 14: FeedRange Interaction ──────────────────────────────────────────

public class PitrFeedRangeTests
{
	[Fact]
	public async Task RestoreToPointInTime_FeedRangeScopedQueryAfterRestore()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { FeedRangeCount = 2 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" },
			new PartitionKey("pk2"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);

		// Query all items — both should be restored
		var query = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c"));
		var results = new List<TestDocument>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
	}
}


// ── Phase 15: Bulk Operations ────────────────────────────────────────────────

public class PitrBulkOperationTests
{
	[Fact]
	public async Task RestoreToPointInTime_BulkCreatedItems_RestoresCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create 5 items
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		// Create 5 more
		for (var i = 5; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		container.ItemCount.Should().Be(10);

		container.RestoreToPointInTime(restorePoint);

		container.ItemCount.Should().Be(5);
	}
}


// ── Phase 16: Per-Item TTL ───────────────────────────────────────────────────

public class PitrPerItemTtlTests
{
	[Fact]
	public async Task RestoreToPointInTime_PerItemTtl_ItemRestoredWithOriginalTtlValue()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { DefaultTimeToLive = 3600 };

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","partitionKey":"pk1","name":"Test","_ttl":300}""")),
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.DeleteItemAsync<JObject>("1", new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);

		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"))).Resource;
		read["_ttl"]!.Value<int>().Should().Be(300);
	}

	[Fact]
	public async Task RestoreToPointInTime_PerItemTtl_RestoredItemGetsNewTimestamp()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { DefaultTimeToLive = 3600 };

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","partitionKey":"pk1","name":"Test","_ttl":300}""")),
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Modified")]);

		container.RestoreToPointInTime(restorePoint);

		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"))).Resource;
		// _ts should be the restore point, not the original creation time
		var ts = read["_ts"]!.Value<long>();
		ts.Should().Be(restorePoint.ToUnixTimeSeconds());
	}
}


// ── Phase 17: Export After Restore ───────────────────────────────────────────

public class PitrExportTests
{
	[Fact]
	public async Task RestoreToPointInTime_ExportState_ContainsOnlyRestoredItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Keep" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Keep" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Remove" },
			new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);

		var exported = container.ExportState();
		var jObj = JObject.Parse(exported);
		jObj["items"]!.ToObject<JArray>()!.Count.Should().Be(2);
	}

	[Fact]
	public async Task RestoreToPointInTime_ExportThenImport_RoundTrip()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);
		var exported = container.ExportState();

		// Import into a new container
		var container2 = new InMemoryContainer("test2", "/partitionKey");
		container2.ImportState(exported);

		container2.ItemCount.Should().Be(2);
		var read = (await container2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"))).Resource;
		read.Name.Should().Be("A");
	}
}


// ── Phase 18: ChangeFeedProcessor After Restore ──────────────────────────────

public class PitrChangeFeedProcessorTests
{
	[Fact]
	public async Task RestoreToPointInTime_ChangeFeedProcessorPicksUpPostRestoreWrites()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		container.RestoreToPointInTime(restorePoint);

		// Get checkpoint after restore
		var checkpoint = container.GetChangeFeedCheckpoint();

		// Write new item
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "AfterRestore" },
			new PartitionKey("pk1"));

		// Read changes since checkpoint
		var feed = container.GetChangeFeedIterator<JObject>(checkpoint);
		var changes = new List<JObject>();
		while (feed.HasMoreResults)
		{
			var page = await feed.ReadNextAsync();
			changes.AddRange(page);
		}

		changes.Should().ContainSingle().Which["id"]!.ToString().Should().Be("2");
	}
}


// ── Phase 19: ItemLocks Cleanup (BUG-1 FIX) ─────────────────────────────────

public class PitrItemLockFixTests
{
	[Fact]
	public async Task RestoreToPointInTime_PatchAfterRestore_NoDeadlock()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		// Patch to create a lock entry
		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")]);

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);

		// Patch same item again — should not deadlock
		var result = await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "PatchedAgain")]);

		result.Resource["name"]!.ToString().Should().Be("PatchedAgain");
	}

	[Fact]
	public async Task RestoreToPointInTime_DeletedItemPatch_CreatesNewLock()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Created" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		container.RestoreToPointInTime(restorePoint);

		// Replace item — should create a new lock and succeed
		var result = await container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "AfterRestore" },
			"1", new PartitionKey("pk1"));

		result.Resource.Name.Should().Be("AfterRestore");
	}
}


// ── Phase 20: Additional Edge Cases ──────────────────────────────────────────

public class PitrAdditionalEdgeCaseTests
{
	[Fact]
	public async Task RestoreToPointInTime_DateTimeOffsetMaxValue_KeepsAllItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" },
			new PartitionKey("pk1"));

		container.RestoreToPointInTime(DateTimeOffset.MaxValue);

		container.ItemCount.Should().Be(2);
	}

	[Fact]
	public async Task RestoreToPointInTime_NewWriteAfterRestore_HasCorrectETagAndTs()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		container.RestoreToPointInTime(restorePoint);

		// Create new item after restore
		var created = await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "New" },
			new PartitionKey("pk1"));

		created.ETag.Should().NotBeNullOrEmpty();
		var read = (await container.ReadItemAsync<JObject>("2", new PartitionKey("pk1"))).Resource;
		var ts = read["_ts"]!.Value<long>();
		// The new write's _ts should be current time, not the restore point
		ts.Should().BeGreaterThanOrEqualTo(restorePoint.ToUnixTimeSeconds());
	}

	[Fact]
	public async Task RestoreToPointInTime_100Items_AllRestoredCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		// Delete 50
		for (var i = 50; i < 100; i++)
			await container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));

		container.ItemCount.Should().Be(50);

		container.RestoreToPointInTime(restorePoint);

		container.ItemCount.Should().Be(100);
	}
}


// ── Phase 21: Divergent Behavior Documentation Tests ─────────────────────────

public class PitrDivergentDocTests
{
	[Fact(Skip = "PITR does not restore container properties — real Cosmos PITR creates a new account")]
	public async Task RestoreToPointInTime_DoesNotRestoreContainerProperties_Divergent()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { DefaultTimeToLive = 3600 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		container.DefaultTimeToLive = 60; // Changed after restore point

		container.RestoreToPointInTime(restorePoint);

		// Ideal: TTL should revert to 3600
		container.DefaultTimeToLive.Should().Be(3600);
	}

	[Fact]
	public async Task RestoreToPointInTime_ContainerPropertiesNotAffectedByRestore_ActualBehavior()
	{
		var container = new InMemoryContainer("test", "/partitionKey") { DefaultTimeToLive = 3600 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1" },
			new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		container.DefaultTimeToLive = 60;

		container.RestoreToPointInTime(restorePoint);

		// Actual: TTL stays at the modified value — PITR only restores items
		container.DefaultTimeToLive.Should().Be(60);
	}

	[Fact(Skip = "PITR does not restore sproc registrations — real Cosmos PITR creates a new account")]
	public async Task RestoreToPointInTime_DoesNotRestoreSprocRegistration_Divergent()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		// Register sproc after restore point
		container.RegisterStoredProcedure("sp1", (pk, args) => "result");

		container.RestoreToPointInTime(restorePoint);

		// Ideal: sproc should no longer be registered
		var act = () => container.Scripts.ExecuteStoredProcedureAsync<string>(
			"sp1", new PartitionKey("pk1"), []);
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task RestoreToPointInTime_SprocRegistrationSurvivesRestore_ActualBehavior()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		container.RegisterStoredProcedure("sp1", (pk, args) => """{"status":"ok"}""");

		container.RestoreToPointInTime(restorePoint);

		// Actual: sproc is still registered — PITR only restores items
		var result = await container.Scripts.ExecuteStoredProcedureAsync<string>(
			"sp1", new PartitionKey("pk1"), []);
		result.Resource.Should().Contain("ok");
	}
}
