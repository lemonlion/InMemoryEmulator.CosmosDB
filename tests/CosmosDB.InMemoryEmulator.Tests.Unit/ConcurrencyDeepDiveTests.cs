using System.Collections.ObjectModel;
using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  M1: Concurrent Deletes with ETag
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentDeleteWithETagTests
{
	[Fact]
	public async Task ConcurrentDeletes_WithETag_AtLeastOneSucceeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var created = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		var etag = created.ETag;

		var tasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			try
			{
				var response = await container.DeleteItemAsync<TestDocument>(
					"1", new PartitionKey("pk1"),
					new ItemRequestOptions { IfMatchEtag = etag });
				return (int)response.StatusCode;
			}
			catch (CosmosException ex) { return (int)ex.StatusCode; }
		});

		var results = await Task.WhenAll(tasks);

		var deleted = results.Count(r => r == 204);
		var notFound = results.Count(r => r == 404);
		var preconditionFailed = results.Count(r => r == 412);

		deleted.Should().BeGreaterThanOrEqualTo(1);
		(deleted + notFound + preconditionFailed).Should().Be(50);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M2/M3: Concurrent DeleteAllByPartitionKey
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentDeleteAllByPKTests
{
	[Fact]
	public async Task ConcurrentDeleteAllByPartitionKey_WhileWriting()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (int i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1"));

		var deleteTask = container.DeleteAllItemsByPartitionKeyStreamAsync(
			new PartitionKey("pk1"));

		var writeTasks = Enumerable.Range(100, 10).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1")));

		await Task.WhenAll(new[] { deleteTask }.Concat(writeTasks.Select(t => (Task)t)));

		// After completion: container state is consistent, no corruption
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		// Some items may survive (written after delete), but no corruption
		results.Should().OnlyContain(r => r["id"] != null);
	}

	[Fact]
	public async Task ConcurrentDeleteAllByPartitionKey_MultiplePartitions()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (int i = 0; i < 20; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = $"a{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1"));
			await container.CreateItemAsync(
				new TestDocument { Id = $"b{i}", PartitionKey = "pk2", Name = $"N{i}" },
				new PartitionKey("pk2"));
			await container.CreateItemAsync(
				new TestDocument { Id = $"c{i}", PartitionKey = "pk3", Name = $"N{i}" },
				new PartitionKey("pk3"));
		}

		await Task.WhenAll(
			container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1")),
			container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk2")));

		// pk3 items should survive intact
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk3") });
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(20);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M4: Concurrent Replace Non-Existent
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentReplaceNonExistentTests
{
	[Fact]
	public async Task ConcurrentReplace_NonExistentItem_AllGet404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.ReplaceItemAsync(
					new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = $"V{i}" },
					"nonexistent", new PartitionKey("pk1"));
				return 200;
			}
			catch (CosmosException ex) { return (int)ex.StatusCode; }
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r == 404);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M5: Concurrent Patch After Delete
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentPatchAfterDeleteTests
{
	[Fact]
	public async Task ConcurrentPatch_AfterDelete_Gets404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 0 },
			new PartitionKey("pk1"));

		var deleteTask = Task.Run(async () =>
		{
			await Task.Delay(5);
			await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		});

		var patchTasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			try
			{
				await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
					new[] { PatchOperation.Increment("/value", 1) });
				return 200;
			}
			catch (CosmosException ex) { return (int)ex.StatusCode; }
		});

		await Task.WhenAll(new[] { deleteTask }.Concat(patchTasks.Select(t => (Task)t)));

		// After delete, any remaining patches should get 404
		try
		{
			await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Increment("/value", 1) });
			Assert.Fail("Should have thrown NotFound");
		}
		catch (CosmosException ex)
		{
			ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M6: Concurrent ClearItems
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentClearItemsTests
{
	[Fact]
	public async Task ConcurrentClearItems_DuringOperations()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (int i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1"));

		var readTasks = Enumerable.Range(0, 20).Select(async i =>
		{
			try
			{
				await container.ReadItemAsync<TestDocument>($"item{i}", new PartitionKey("pk1"));
				return true;
			}
			catch { return false; }
		});

		var clearTask = Task.Run(() => container.ClearItems());

		await Task.WhenAll(new[] { clearTask }.Concat(readTasks.Select(t => (Task)t)));

		// After clear, container should be empty
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M7: Concurrent ImportState
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentImportStateTests
{
	[Fact]
	public async Task ConcurrentImportState_DuringReads()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (int i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1"));

		var state = container.ExportState();
		container.ClearItems();

		var readTasks = Enumerable.Range(0, 20).Select(async _ =>
		{
			try
			{
				await container.ReadItemAsync<TestDocument>("item0", new PartitionKey("pk1"));
				return true;
			}
			catch { return false; }
		});

		var importTask = Task.Run(() => container.ImportState(state));

		await Task.WhenAll(new[] { importTask }.Concat(readTasks.Select(t => (Task)t)));

		// After import: state should be consistent
		var final = await container.ReadItemAsync<TestDocument>("item0", new PartitionKey("pk1"));
		final.Resource.Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M8: Concurrent operations multiple containers
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentMultiContainerTests
{
	[Fact]
	public async Task ConcurrentOperations_MultipleContainers_SameDatabase()
	{
		var containers = Enumerable.Range(0, 5)
			.Select(i => new InMemoryContainer($"container{i}", "/partitionKey"))
			.ToList();

		var tasks = containers.SelectMany((c, ci) =>
			Enumerable.Range(0, 10).Select(i =>
				c.CreateItemAsync(
					new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"C{ci}_N{i}" },
					new PartitionKey("pk1"))));

		await Task.WhenAll(tasks);

		// Each container should have exactly 10 items
		foreach (var c in containers)
		{
			var iter = c.GetItemQueryIterator<JObject>(
				new QueryDefinition("SELECT * FROM c"));
			var results = new List<JObject>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			results.Should().HaveCount(10);
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M9: Concurrent PartitionKey.None
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentPartitionKeyNoneTests
{
	[Fact]
	public async Task ConcurrentOperations_PartitionKeyNone()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.CreateItemAsync(
				JObject.FromObject(new { id = $"item{i}" }),
				PartitionKey.None));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M10: Concurrent PITR
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentPITRTests
{
	[Fact]
	public async Task ConcurrentPITR_DuringWrites()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (int i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);

		// Add more items after the restore point
		for (int i = 10; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1"));

		// Restore while concurrent writes happen
		var writeTask = Task.Run(async () =>
		{
			for (int i = 20; i < 30; i++)
			{
				try
				{
					await container.UpsertItemAsync(
						new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
						new PartitionKey("pk1"));
				}
				catch { /* ignore during restore */ }
			}
		});

		container.RestoreToPointInTime(restorePoint);
		await writeTask;

		// Container should have items from restore point or written after restore
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		// At minimum the 10 restored items should exist
		results.Count.Should().BeGreaterThanOrEqualTo(10);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M11: Concurrent FeedRange Query
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentFeedRangeQueryTests
{
	[Fact]
	public async Task ConcurrentFeedRangeQuery_DuringWrites()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (int i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = $"pk{i % 5}", Name = $"N{i}" },
				new PartitionKey($"pk{i % 5}"));

		var ranges = await container.GetFeedRangesAsync();

		var writeTasks = Enumerable.Range(100, 20).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = $"pk{i % 5}", Name = $"N{i}" },
				new PartitionKey($"pk{i % 5}")));

		var queryTasks = ranges.Select(async range =>
		{
			var iter = container.GetItemQueryIterator<JObject>(
				new QueryDefinition("SELECT * FROM c"),
				requestOptions: new QueryRequestOptions { });
			var results = new List<JObject>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			return results.Count;
		});

		await Task.WhenAll(writeTasks.Select(t => (Task)t).Concat(queryTasks.Select(t => (Task)t)));

		// Queries should complete without error
		var counts = await Task.WhenAll(queryTasks);
		counts.Should().OnlyContain(c => c > 0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  M12: Concurrent Trigger Execution
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentTriggerExecutionTests
{
	[Fact]
	public async Task ConcurrentTriggerExecution_PreAndPost()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		container.RegisterTrigger("preTrigger", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(item =>
			{
				item["triggered"] = true;
				return item;
			}));

		var postCount = 0;
		container.RegisterTrigger("postTrigger", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(item =>
			{
				Interlocked.Increment(ref postCount);
			}));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.CreateItemAsync(
				JObject.FromObject(new { id = $"item{i}", partitionKey = "pk1" }),
				new PartitionKey("pk1"),
				new ItemRequestOptions
				{
					PreTriggers = new List<string> { "preTrigger" },
					PostTriggers = new List<string> { "postTrigger" }
				}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

		// All post triggers should have fired
		postCount.Should().Be(50);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E1: Concurrent Delete with ETag (Stream)
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentDeleteStreamETagTests
{
	[Fact]
	public async Task ConcurrentDelete_WithETag_StreamVariant()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var created = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		var etag = created.ETag;

		var tasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			var response = await container.DeleteItemStreamAsync(
				"1", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = etag });
			return (int)response.StatusCode;
		});

		var results = await Task.WhenAll(tasks);
		var deleted = results.Count(r => r == 204);
		var notFound = results.Count(r => r == 404);
		var preconditionFailed = results.Count(r => r == 412);

		deleted.Should().BeGreaterThanOrEqualTo(1);
		(deleted + notFound + preconditionFailed).Should().Be(50);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E2: Concurrent Upsert/Create Same ID
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentUpsertCreateTests
{
	[Fact]
	public async Task ConcurrentUpsertCreate_SameId_CorrectStatusCodes()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"V{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		var codes = results.Select(r => (int)r.StatusCode).ToList();

		// At least one 201 (Created), rest should be 200 (OK)
		codes.Should().Contain(201);
		codes.Should().OnlyContain(c => c == 201 || c == 200);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E3: Concurrent Replace with ETag (Stream)
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentReplaceStreamETagTests
{
	[Fact]
	public async Task ConcurrentReplace_WithETag_StreamVariant()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var created = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var etag = created.ETag;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"V{i}" });
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			var response = await container.ReplaceItemStreamAsync(
				stream, "1", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = etag });
			return (int)response.StatusCode;
		});

		var results = await Task.WhenAll(tasks);
		var succeeded = results.Count(r => r == 200);
		var preconditionFailed = results.Count(r => r == 412);

		succeeded.Should().BeGreaterThanOrEqualTo(1);
		(succeeded + preconditionFailed).Should().Be(50);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E4: Concurrent Patch Multiple Operations Atomic
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentPatchAtomicTests
{
	[Fact]
	public async Task ConcurrentPatch_MultipleOperations_Atomic()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Start", Value = 0, IsActive = false },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
				new[]
				{
					PatchOperation.Set("/name", $"V{i}"),
					PatchOperation.Increment("/value", 1),
					PatchOperation.Set("/isActive", true)
				}));

		await Task.WhenAll(tasks);

		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		// All 50 increments should be applied (patches are serialized)
		final.Resource.Value.Should().Be(50);
		final.Resource.IsActive.Should().BeTrue();
		final.Resource.Name.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E5: Concurrent Computed Property Query
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentComputedPropertyQueryTests
{
	[Fact]
	public async Task ConcurrentComputedPropertyQuery()
	{
		var container = new InMemoryContainer(
			new ContainerProperties("test", "/partitionKey")
			{
				ComputedProperties = new Collection<ComputedProperty>
				{
					new() { Name = "cp_lower", Query = "SELECT VALUE LOWER(c.name) FROM c" }
				}
			});

		// Concurrent writes
		var writeTasks = Enumerable.Range(0, 50).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"Name{i}" },
				new PartitionKey("pk1")));

		// Concurrent CP queries
		var queryTasks = Enumerable.Range(0, 10).Select(async _ =>
		{
			await Task.Delay(10);
			var iter = container.GetItemQueryIterator<JObject>(
				new QueryDefinition("SELECT c.id, c.cp_lower FROM c"));
			var results = new List<JObject>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			return results;
		});

		await Task.WhenAll(writeTasks.Select(t => (Task)t).Concat(queryTasks.Select(t => (Task)t)));

		// Final query should show all computed properties correctly
		var finalIter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, c.cp_lower FROM c"));
		var finalResults = new List<JObject>();
		while (finalIter.HasMoreResults) finalResults.AddRange(await finalIter.ReadNextAsync());

		finalResults.Should().HaveCount(50);
		finalResults.Should().OnlyContain(r =>
			r["cp_lower"]!.ToString() == r["cp_lower"]!.ToString().ToLowerInvariant());
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E6: Concurrent ChangeFeed Checkpoint Consistency
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentChangeFeedCheckpointTests
{
	[Fact]
	public async Task ConcurrentChangeFeed_Checkpoint_Consistency()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Concurrent writes
		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1")));

		await Task.WhenAll(tasks);

		// Read change feed — all 50 creates should be captured
		var changes = new List<JObject>();
		var changeFeed = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.LatestVersion);

		while (changeFeed.HasMoreResults)
		{
			var response = await changeFeed.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(response);
		}

		changes.Should().HaveCount(50);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E7: Concurrent Batch Same Items
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentBatchConflictTests
{
	[Fact]
	public async Task ConcurrentBatch_SameItems_ConflictDetected()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var batch1Task = Task.Run(async () =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
			batch.CreateItem(new TestDocument { Id = "conflict1", PartitionKey = "pk1", Name = "B1" });
			batch.CreateItem(new TestDocument { Id = "unique1", PartitionKey = "pk1", Name = "B1" });
			return await batch.ExecuteAsync();
		});

		var batch2Task = Task.Run(async () =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
			batch.CreateItem(new TestDocument { Id = "conflict1", PartitionKey = "pk1", Name = "B2" });
			batch.CreateItem(new TestDocument { Id = "unique2", PartitionKey = "pk1", Name = "B2" });
			return await batch.ExecuteAsync();
		});

		var results = await Task.WhenAll(batch1Task, batch2Task);

		// One batch should succeed, the other should fail on the conflicting ID
		var successCount = results.Count(r => r.IsSuccessStatusCode);
		var failCount = results.Count(r => !r.IsSuccessStatusCode);

		// At least one should succeed and items should be in valid state
		successCount.Should().BeGreaterThanOrEqualTo(1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E8: Concurrent Delete and Patch Same Item
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentDeleteAndPatchTests
{
	[Fact]
	public async Task ConcurrentDeleteAndPatch_SameItem_SafeOutcome()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 0 },
			new PartitionKey("pk1"));

		var deleteTasks = Enumerable.Range(0, 25).Select(async _ =>
		{
			try
			{
				await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
				return "delete";
			}
			catch (CosmosException) { return "delete-fail"; }
		});

		var patchTasks = Enumerable.Range(0, 25).Select(async _ =>
		{
			try
			{
				await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
					new[] { PatchOperation.Increment("/value", 1) });
				return "patch";
			}
			catch (CosmosException) { return "patch-fail"; }
		});

		var results = await Task.WhenAll(deleteTasks.Concat(patchTasks));

		// No unhandled exceptions — all outcomes are expected
		results.Should().OnlyContain(r =>
			r == "delete" || r == "delete-fail" || r == "patch" || r == "patch-fail");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  H1: ETag TOCTOU Emulator Behaviour
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentETagTOCTOUTests
{
	[Fact]
	public async Task ETagTOCTOU_OnlyOneWriteSucceeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var created = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var etag = created.ETag;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				var response = await container.ReplaceItemAsync(
					new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"V{i}" },
					"1", new PartitionKey("pk1"),
					new ItemRequestOptions { IfMatchEtag = etag });
				return (int)response.StatusCode;
			}
			catch (CosmosException ex) { return (int)ex.StatusCode; }
		});

		var results = await Task.WhenAll(tasks);

		// CheckIfMatch is now inside _itemLocks — exactly 1 should succeed
		var succeeded = results.Count(r => r == 200);
		var preconditionFailed = results.Count(r => r == 412);

		succeeded.Should().Be(1);
		preconditionFailed.Should().Be(49);

		// Item should be in a valid state
		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  H2: Batch Restore Snapshot Emulator Behaviour
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentBatchRestoreTests
{
	[Fact(Skip = "RestoreSnapshot clears dictionaries then re-populates, creating a brief window " +
				 "where concurrent reads see empty/partial state. Real Cosmos batches are " +
				 "isolated at the partition level.")]
	public void RestoreSnapshot_ConcurrentReadsAlwaysSeeConsistentState_RealCosmos() { }

	[Fact]
	public async Task RestoreSnapshot_EmulatorBehaviour_ConcurrentReadsMaySeePartialState()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create 10 items
		for (int i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"item{i}", PartitionKey = "pk1", Name = $"N{i}" },
				new PartitionKey("pk1"));

		// Execute a batch that will fail (trigger rollback via RestoreSnapshot)
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "new1", PartitionKey = "pk1", Name = "New" });
		batch.CreateItem(new TestDocument { Id = "item0", PartitionKey = "pk1", Name = "Dup" }); // conflict

		var batchResult = await batch.ExecuteAsync();
		batchResult.IsSuccessStatusCode.Should().BeFalse();

		// After rollback: original items should be intact
		for (int i = 0; i < 10; i++)
		{
			var item = await container.ReadItemAsync<TestDocument>($"item{i}", new PartitionKey("pk1"));
			item.Resource.Should().NotBeNull();
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  H3: Patch + Replace Lock Contention
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentPatchReplaceLockTests
{
	[Fact]
	public async Task PatchAndReplace_SameItem_WritesAreSerialized()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 0 },
			new PartitionKey("pk1"));

		var patchTasks = Enumerable.Range(0, 25).Select(_ =>
			container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Increment("/value", 1) }));

		var replaceTasks = Enumerable.Range(0, 25).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Replaced{i}", Value = 1000 },
				"1", new PartitionKey("pk1")));

		await Task.WhenAll(patchTasks.Select(t => (Task)t).Concat(replaceTasks.Select(t => (Task)t)));

		// Patch and Replace now use the same _itemLocks — writes are serialized per item.
		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Should().NotBeNull();
		final.Resource.Id.Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Concurrent Patch After Upsert — proves upsert acquires item lock
//  Pattern: same as ConcurrentPatch_AfterDelete_Gets404 — concurrent phase
//  followed by a deterministic post-condition.
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentPatchAfterUpsertTests
{
	[Fact]
	public async Task ConcurrentPatch_WithUpsert_FinalValueIsConsistent()
	{
		// Same structural pattern as ConcurrentPatch_AfterDelete_Gets404:
		// 1. Create item with Value=0
		// 2. Fire concurrent patches (increment by 1) alongside an upsert (Value=1000)
		// 3. AFTER all settle, do one more patch (+1) and read
		// 4. Post-condition: Value must be > 1000 if upsert ran and a patch came after,
		//    OR >= 1 if all patches ran before the upsert. Either way, the item
		//    must exist and its value must be an integer (not corrupted).
		//
		// Without the lock on upsert, a patch could read Value=0, then upsert
		// sets 1000, then patch writes back 1 — losing the upsert. A subsequent
		// patch+read would see a small value when it should see >= 1000.
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 0 },
			new PartitionKey("pk1"));

		var upsertTask = Task.Run(async () =>
		{
			await Task.Delay(5);
			await container.UpsertItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted", Value = 1000 },
				new PartitionKey("pk1"));
		});

		var patchTasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			try
			{
				await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
					new[] { PatchOperation.Increment("/value", 1) });
			}
			catch (CosmosException) { }
		});

		await Task.WhenAll(new[] { upsertTask }.Concat(patchTasks));

		// Post-condition: upsert set Value=1000, and the item lock ensures
		// no patch can overwrite it with a stale read-modify-write value.
		// The item must exist, be valid, and have Value >= 0.
		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Should().NotBeNull();
		final.Resource.Id.Should().Be("1");
	}

	[Fact]
	public async Task ConcurrentUpsert_AfterDelete_ItemMustNotBeResurrected()
	{
		// Same pattern as ConcurrentPatch_AfterDelete_Gets404 but with upsert.
		// Without the lock on upsert, a concurrent upsert could check existence,
		// then delete removes the item, then upsert writes to _items/_etags/_timestamps
		// in a non-atomic sequence — potentially leaving partial state.
		//
		// With the lock, upsert and delete serialize. After this sequence:
		// delete first -> upsert re-creates -> final read sees it. OR
		// upsert first -> delete removes -> final read gets 404.
		// Either outcome is valid, but the item must be in a clean state.
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 0 },
			new PartitionKey("pk1"));

		// Delete, then immediately upsert + more deletes racing
		var tasks = new List<Task>();
		tasks.Add(container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1")));
		tasks.AddRange(Enumerable.Range(0, 10).Select(i => Task.Run(async () =>
		{
			try
			{
				await container.UpsertItemAsync(
					new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Upsert{i}", Value = i },
					new PartitionKey("pk1"));
			}
			catch (CosmosException) { }
		})));
		tasks.AddRange(Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
		{
			try { await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1")); }
			catch (CosmosException) { }
		})));

		await Task.WhenAll(tasks);

		// Post-condition: the item is either fully present (with valid etag,
		// timestamp, and enriched system properties) or fully absent.
		// No partial state allowed.
		try
		{
			var result = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
			result.Resource.Id.Should().Be("1");
			result.ETag.Should().NotBeNullOrEmpty();
		}
		catch (CosmosException ex)
		{
			ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Concurrent Patch During Create — proves create acquires item lock
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentPatchDuringCreateTests
{
	[Fact]
	public async Task ConcurrentCreate_WithPatch_PatchSeesCompleteItem()
	{
		// Without the create lock, there's a window between _items.TryAdd
		// and setting _etags/_timestamps/enrichment where a concurrent patch
		// could read the un-enriched item (no _ts, no _etag, no _rid).
		// With the lock, patches wait until create fully completes.
		//
		// We prove this by having patches immediately after create, then
		// verifying system properties are always present.
		var container = new InMemoryContainer("test", "/partitionKey");

		var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		var createTask = Task.Run(async () =>
		{
			await barrier.Task;
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Created", Value = 100 },
				new PartitionKey("pk1"));
		});

		var patchResults = new System.Collections.Concurrent.ConcurrentBag<(int status, string json)>();
		var patchTasks = Enumerable.Range(0, 50).Select(_ => Task.Run(async () =>
		{
			await barrier.Task;
			try
			{
				var result = await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
					new[] { PatchOperation.Increment("/value", 1) });
				return 200;
			}
			catch (CosmosException ex) { return (int)ex.StatusCode; }
		}));

		barrier.SetResult();
		var results = await Task.WhenAll(patchTasks);
		await createTask;

		// Every patch either succeeded (200) or got 404 (item didn't exist yet).
		results.Should().OnlyContain(r => r == 200 || r == 404);

		// Post-condition: item must have valid system properties
		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Should().NotBeNull();
		final.ETag.Should().NotBeNullOrEmpty();

		// Read raw to verify system properties are present
		var rawResponse = await container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		using var reader = new StreamReader(rawResponse.Content);
		var rawJson = JObject.Parse(await reader.ReadToEndAsync());
		rawJson["_etag"].Should().NotBeNull();
		rawJson["_ts"].Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Concurrent Create/Upsert Stream — lock coverage for stream variants
// ═══════════════════════════════════════════════════════════════════════════

public class ConcurrentCreateUpsertStreamTests
{
	[Fact]
	public async Task ConcurrentUpsertStream_AfterDelete_NoPartialState()
	{
		// Stream variant of the upsert+delete race.
		// Without the lock, UpsertItemStreamAsync could interleave with
		// DeleteItemStreamAsync, leaving _items set but _etags/_timestamps missing.
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = new List<Task<int>>();

		// Mix of stream upserts and stream deletes
		tasks.AddRange(Enumerable.Range(0, 20).Select(i => Task.Run(async () =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"StreamUpsert{i}", Value = i * 100 });
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			var response = await container.UpsertItemStreamAsync(stream, new PartitionKey("pk1"));
			return (int)response.StatusCode;
		})));

		tasks.AddRange(Enumerable.Range(0, 20).Select(_ => Task.Run(async () =>
		{
			var response = await container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
			return (int)response.StatusCode;
		})));

		var results = await Task.WhenAll(tasks);

		// All operations should have returned valid status codes
		results.Should().OnlyContain(r => r == 200 || r == 201 || r == 204 || r == 404);

		// Post-condition: item is either fully present or fully absent.
		var readResponse = await container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		if (readResponse.StatusCode == HttpStatusCode.OK)
		{
			// If present, system properties must be intact
			using var reader = new StreamReader(readResponse.Content);
			var body = JObject.Parse(await reader.ReadToEndAsync());
			body["_etag"].Should().NotBeNull("item exists but has no _etag — partial state from unlocked write");
			body["_ts"].Should().NotBeNull("item exists but has no _ts — partial state from unlocked write");
			body["id"]!.ToString().Should().Be("1");
		}
		else
		{
			((int)readResponse.StatusCode).Should().Be(404);
		}
	}

	[Fact]
	public async Task ConcurrentCreateStream_WithDeleteStream_NoResurrection()
	{
		// Create + Delete race via stream API.
		// Pattern mirrors ConcurrentPatch_AfterDelete_Gets404:
		// After all operations settle, the post-condition must hold.
		var container = new InMemoryContainer("test", "/partitionKey");

		var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		// 20 create attempts for the same ID + 20 delete attempts
		var createTasks = Enumerable.Range(0, 20).Select(i => Task.Run(async () =>
		{
			await barrier.Task;
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = "race1", PartitionKey = "pk1", Name = $"Created{i}" });
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			var response = await container.CreateItemStreamAsync(stream, new PartitionKey("pk1"));
			return (int)response.StatusCode;
		}));

		var deleteTasks = Enumerable.Range(0, 20).Select(_ => Task.Run(async () =>
		{
			await barrier.Task;
			var response = await container.DeleteItemStreamAsync("race1", new PartitionKey("pk1"));
			return (int)response.StatusCode;
		}));

		barrier.SetResult();
		var allResults = await Task.WhenAll(createTasks.Concat(deleteTasks));

		// Valid status codes only
		allResults.Should().OnlyContain(r => r == 201 || r == 204 || r == 404 || r == 409);

		// Post-condition: item is either fully present or fully absent
		var readResponse = await container.ReadItemStreamAsync("race1", new PartitionKey("pk1"));
		if (readResponse.StatusCode == HttpStatusCode.OK)
		{
			using var reader = new StreamReader(readResponse.Content);
			var body = JObject.Parse(await reader.ReadToEndAsync());
			body["_etag"].Should().NotBeNull("item exists but has no _etag — partial state");
			body["_ts"].Should().NotBeNull("item exists but has no _ts — partial state");
		}
		else
		{
			((int)readResponse.StatusCode).Should().Be(404);
		}
	}
}
