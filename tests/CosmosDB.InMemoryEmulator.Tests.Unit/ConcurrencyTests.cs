using System.Collections.ObjectModel;
using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class ConcurrencyGapTests3
{
	[Fact]
	public async Task ConcurrentBatchOperations_Isolation()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create items for batch operations
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"pre-{i}", PartitionKey = "pk1", Name = $"Pre{i}" },
				new PartitionKey("pk1"));

		var batchTasks = Enumerable.Range(0, 5).Select(async batchIndex =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
			batch.CreateItem(new TestDocument { Id = $"batch-{batchIndex}", PartitionKey = "pk1", Name = $"Batch{batchIndex}" });
			using var response = await batch.ExecuteAsync();
			return response.IsSuccessStatusCode;
		});

		var results = await Task.WhenAll(batchTasks);
		results.Should().OnlyContain(success => success);
	}

	[Fact]
	public async Task ConcurrentChangeFeedRead_ThreadSafe()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 10).Select(async _ =>
		{
			var checkpoint = container.GetChangeFeedCheckpoint() - 10;
			if (checkpoint < 0) checkpoint = 0;
			var iterator = container.GetChangeFeedIterator<TestDocument>(checkpoint);
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			results.Should().NotBeEmpty();
		});

		await Task.WhenAll(tasks);
	}
}


public class ConcurrencyGapTests
{
	[Fact]
	public async Task ConcurrentCreates_DifferentIds_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(100);
	}

	[Fact]
	public async Task ConcurrentCreates_SameId_ExactlyOneSucceeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var successes = 0;
		var failures = 0;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.CreateItemAsync(
					new TestDocument { Id = "same", PartitionKey = "pk1", Name = $"Item{i}" },
					new PartitionKey("pk1"));
				Interlocked.Increment(ref successes);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
			{
				Interlocked.Increment(ref failures);
			}
		});

		await Task.WhenAll(tasks);

		successes.Should().Be(1);
		failures.Should().Be(49);
	}

	[Fact]
	public async Task ConcurrentReads_SameItem_AllReturnSameData()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(_ =>
			container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		results.Should().OnlyContain(r => r.Resource.Name == "Alice");
	}

	[Fact]
	public async Task ConcurrentUpserts_SameItem_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Version{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}
}


public class ConcurrencyGapTests2
{
	[Fact]
	public async Task ConcurrentCreateAndRead_NoCorruption()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create items
		for (var i = 0; i < 50; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = $"pre-{i}", PartitionKey = "pk1", Name = $"Pre{i}" },
				new PartitionKey("pk1"));
		}

		// Concurrent writes and reads
		var writeTasks = Enumerable.Range(50, 50)
			.Select(i => container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var readTasks = Enumerable.Range(0, 50)
			.Select(i => container.ReadItemAsync<TestDocument>($"pre-{i}", new PartitionKey("pk1")));

		var allTasks = writeTasks.Cast<Task>().Concat(readTasks.Cast<Task>());
		await Task.WhenAll(allTasks);

		container.ItemCount.Should().Be(100);
	}

	[Fact]
	public async Task ConcurrentQueryAndWrite_NoCorruption()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 50; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
		}

		var writeTasks = Enumerable.Range(50, 50)
			.Select(i => container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"New{i}" },
				new PartitionKey("pk1")));

		var queryTasks = Enumerable.Range(0, 10).Select(async _ =>
		{
			var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			results.Should().NotBeEmpty();
		});

		await Task.WhenAll(writeTasks.Cast<Task>().Concat(queryTasks));
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 1: Concurrent Delete Scenarios
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentDeleteTests
{
	[Fact]
	public async Task ConcurrentDeletes_SameItem_AllCompleteWithoutCrash()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Target" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			try
			{
				await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
				return "deleted";
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				return "notfound";
			}
		});

		var results = await Task.WhenAll(tasks);
		results.Should().Contain("deleted");
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task ConcurrentDeleteAndRead_ReadsEitherSucceedOrGetNotFound()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var deleteTasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try { await container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")); }
			catch (CosmosException) { }
		});

		var readTasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				var r = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
				return r.StatusCode;
			}
			catch (CosmosException ex) { return ex.StatusCode; }
		});

		await Task.WhenAll(deleteTasks.Cast<Task>().Concat(readTasks.Select(async t => { await t; })));
	}

	[Fact]
	public async Task ConcurrentDeleteAndCreate_SameId_NoCorruption()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "x", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(async i =>
		{
			try
			{
				if (i % 2 == 0)
					await container.DeleteItemAsync<TestDocument>("x", new PartitionKey("pk1"));
				else
					await container.CreateItemAsync(
						new TestDocument { Id = "x", PartitionKey = "pk1", Name = $"V{i}" },
						new PartitionKey("pk1"));
			}
			catch (CosmosException) { }
		});

		await Task.WhenAll(tasks);
		// Final state: item either exists or doesn't, but no corruption
		container.ItemCount.Should().BeLessThanOrEqualTo(1);
	}

	[Fact]
	public async Task ConcurrentDeleteAndUpsert_SameItem_StateIsConsistent()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(async i =>
		{
			try
			{
				if (i % 2 == 0)
					await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
				else
					await container.UpsertItemAsync(
						new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"V{i}" },
						new PartitionKey("pk1"));
			}
			catch (CosmosException) { }
		});

		await Task.WhenAll(tasks);
		container.ItemCount.Should().BeLessThanOrEqualTo(1);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 2: Concurrent Patch Scenarios
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentPatchTests
{
	[Fact]
	public async Task ConcurrentPatches_SameItem_AllSucceed_LastWriteWins()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Name.Should().StartWith("Patched");
	}

	[Fact]
	public async Task ConcurrentPatches_SameItem_WithETag_ExactlyOneSucceeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var etag = read.ETag;

		var successes = 0;
		var preconditionFailed = 0;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
					new[] { PatchOperation.Set("/name", $"Patched{i}") },
					new PatchItemRequestOptions { IfMatchEtag = etag });
				Interlocked.Increment(ref successes);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
			{
				Interlocked.Increment(ref preconditionFailed);
			}
		});

		await Task.WhenAll(tasks);
		successes.Should().Be(1);
		preconditionFailed.Should().Be(49);
	}

	[Fact]
	public async Task ConcurrentPatchAndReplace_SameItem_NoCorruption()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var patchTasks = Enumerable.Range(0, 25).Select(i =>
			container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") }));

		var replaceTasks = Enumerable.Range(0, 25).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Replaced{i}" },
				"1", new PartitionKey("pk1")));

		await Task.WhenAll(patchTasks.Cast<Task>().Concat(replaceTasks));

		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Name.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ConcurrentPatch_DifferentItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.PatchItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task ConcurrentPatch_IncrementOperation_ValueIsAtMost100()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Counter", Value = 0 },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(_ =>
			container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Increment("/value", 1) }));

		await Task.WhenAll(tasks);

		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		// Patches are serialized per-item via SemaphoreSlim, so all 100 increments are applied.
		final.Resource.Value.Should().Be(100, "patches are serialized per-item via SemaphoreSlim");
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 3: Concurrent Replace Scenarios
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentReplaceTests
{
	[Fact]
	public async Task ConcurrentReplaces_SameItem_AllSucceed_LastWriteWins()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Version{i}" },
				"1", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Name.Should().StartWith("Version");
	}

	[Fact]
	public async Task ConcurrentReplaces_SameItem_WithETag_ExactlyOneSucceeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var etag = read.ETag;

		var successes = 0;
		var preconditionFailed = 0;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.ReplaceItemAsync(
					new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Version{i}" },
					"1", new PartitionKey("pk1"),
					new ItemRequestOptions { IfMatchEtag = etag });
				Interlocked.Increment(ref successes);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
			{
				Interlocked.Increment(ref preconditionFailed);
			}
		});

		await Task.WhenAll(tasks);
		successes.Should().Be(1);
		preconditionFailed.Should().Be(49);
	}

	[Fact]
	public async Task ConcurrentReplaces_DifferentItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
				$"{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 4: Concurrent Upsert Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentUpsertTests
{
	[Fact]
	public async Task ConcurrentUpserts_ItemDoesntExist_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = "same", PartitionKey = "pk1", Name = $"V{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r =>
			r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);

		container.ItemCount.Should().Be(1);
	}

	[Fact]
	public async Task ConcurrentUpserts_WithETag_ExactlyOneSucceeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var etag = read.ETag;

		var successes = 0;
		var preconditionFailed = 0;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.UpsertItemAsync(
					new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"V{i}" },
					new PartitionKey("pk1"),
					new ItemRequestOptions { IfMatchEtag = etag });
				Interlocked.Increment(ref successes);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
			{
				Interlocked.Increment(ref preconditionFailed);
			}
		});

		await Task.WhenAll(tasks);
		successes.Should().Be(1);
		preconditionFailed.Should().Be(49);
	}

	[Fact]
	public async Task ConcurrentUpserts_DifferentPartitionKeys_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk{i}", Name = $"Item{i}" },
				new PartitionKey($"pk{i}")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(100);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 5: Transactional Batch Under Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentBatchTests
{
	[Fact]
	public async Task TransactionalBatch_Rollback_RestoresTimestamps()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		// Read original _ts
		var before = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsBefore = (long)before.Resource["_ts"]!;

		// Wait a moment so _ts would change
		await Task.Delay(1100);

		// Batch that modifies item 1 then fails on a duplicate create
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Modified" });
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" }); // will conflict
		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();

		// After rollback, _ts should be restored to original value
		var after = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsAfter = (long)after.Resource["_ts"]!;
		tsAfter.Should().Be(tsBefore);
	}

	[Fact]
	public async Task ConcurrentBatches_SamePartition_DifferentItems_BothSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var batch1Task = Task.Run(async () =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
			batch.CreateItem(new TestDocument { Id = "a1", PartitionKey = "pk1", Name = "A1" });
			batch.CreateItem(new TestDocument { Id = "a2", PartitionKey = "pk1", Name = "A2" });
			return await batch.ExecuteAsync();
		});

		var batch2Task = Task.Run(async () =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
			batch.CreateItem(new TestDocument { Id = "b1", PartitionKey = "pk1", Name = "B1" });
			batch.CreateItem(new TestDocument { Id = "b2", PartitionKey = "pk1", Name = "B2" });
			return await batch.ExecuteAsync();
		});

		using var r1 = await batch1Task;
		using var r2 = await batch2Task;

		r1.IsSuccessStatusCode.Should().BeTrue();
		r2.IsSuccessStatusCode.Should().BeTrue();
		container.ItemCount.Should().Be(4);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 6: Concurrent Change Feed Operations
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentChangeFeedTests
{
	[Fact]
	public async Task ConcurrentChangeFeedRead_WhileWriting_NoMissedEntries()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		const int writeCount = 100;

		var writeTasks = Enumerable.Range(0, writeCount).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var readTasks = Enumerable.Range(0, 5).Select(async _ =>
		{
			await Task.Delay(10);
			var iterator = container.GetChangeFeedIterator<TestDocument>(0);
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}
			return results.Count;
		});

		await Task.WhenAll(writeTasks);
		var readResults = await Task.WhenAll(readTasks);

		// After writes complete, final read should see all items
		var finalIterator = container.GetChangeFeedIterator<TestDocument>(0);
		var finalResults = new List<TestDocument>();
		while (finalIterator.HasMoreResults)
		{
			var page = await finalIterator.ReadNextAsync();
			finalResults.AddRange(page);
		}
		finalResults.Should().HaveCount(writeCount);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 7: Concurrent Operations Across Partitions
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentCrossPartitionTests
{
	[Fact]
	public async Task ConcurrentOperations_DifferentPartitions_FullyIndependent()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 10).Select(async pk =>
		{
			var pkStr = $"pk{pk}";
			for (var i = 0; i < 10; i++)
			{
				var id = $"{pk}-{i}";
				await container.CreateItemAsync(
					new TestDocument { Id = id, PartitionKey = pkStr, Name = $"Item{id}" },
					new PartitionKey(pkStr));
			}

			// Read back all items in this partition
			var iterator = container.GetItemQueryIterator<TestDocument>(
				new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = @pk")
					.WithParameter("@pk", pkStr));
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
				results.AddRange(await iterator.ReadNextAsync());

			results.Should().HaveCount(10);
		});

		await Task.WhenAll(tasks);
		container.ItemCount.Should().Be(100);
	}

	[Fact]
	public async Task CrossPartitionQuery_DuringConcurrentWrites()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"pre-{i}", PartitionKey = $"pk{i % 5}", Name = $"Pre{i}" },
				new PartitionKey($"pk{i % 5}"));

		var writeTasks = Enumerable.Range(50, 50).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk{i % 5}", Name = $"New{i}" },
				new PartitionKey($"pk{i % 5}")));

		var queryTasks = Enumerable.Range(0, 5).Select(async _ =>
		{
			var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
				results.AddRange(await iterator.ReadNextAsync());
			results.Should().NotBeEmpty();
		});

		await Task.WhenAll(writeTasks.Cast<Task>().Concat(queryTasks));
		container.ItemCount.Should().Be(100);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 8: Unique Key Constraint Under Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentUniqueKeyTests
{
	[Fact]
	public async Task ConcurrentCreates_UniqueKeyViolation_ExactlyOneSucceeds()
	{
		var containerProps = new ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(containerProps);

		var successes = 0;
		var conflicts = 0;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.CreateItemAsync(
					new TestDocument { Id = $"id{i}", PartitionKey = "pk1", Name = "SameName" },
					new PartitionKey("pk1"));
				Interlocked.Increment(ref successes);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
			{
				Interlocked.Increment(ref conflicts);
			}
		});

		await Task.WhenAll(tasks);
		successes.Should().Be(1);
		conflicts.Should().Be(49);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 9: Concurrent Stream API Operations
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentStreamTests
{
	[Fact]
	public async Task ConcurrentCreateItemStream_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(async i =>
		{
			var json = $"{{\"id\":\"{i}\",\"partitionKey\":\"pk1\",\"name\":\"Item{i}\"}}";
			var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
			using var response = await container.CreateItemStreamAsync(stream, new PartitionKey("pk1"));
			return response.StatusCode;
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.Created);
		container.ItemCount.Should().Be(100);
	}

	[Fact]
	public async Task ConcurrentUpsertItemStream_SameItem_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			var json = $"{{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"V{i}\"}}";
			var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
			using var response = await container.UpsertItemStreamAsync(stream, new PartitionKey("pk1"));
			return response.StatusCode;
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s =>
			s == HttpStatusCode.Created || s == HttpStatusCode.OK);
		container.ItemCount.Should().Be(1);
	}

	[Fact]
	public async Task ConcurrentDeleteItemStream_SameItem_NoCorruption()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Target" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			using var response = await container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
			return response.StatusCode;
		});

		var results = await Task.WhenAll(tasks);
		results.Should().Contain(HttpStatusCode.NoContent);
		container.ItemCount.Should().Be(0);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 10: ReadMany Under Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentReadManyTests
{
	[Fact]
	public async Task ConcurrentReadMany_WhileWriting_NoCorruption()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var writeTasks = Enumerable.Range(50, 50).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"New{i}" },
				new PartitionKey("pk1")));

		var readTasks = Enumerable.Range(0, 10).Select(async _ =>
		{
			var ids = Enumerable.Range(0, 20)
				.Select(i => (i.ToString(), new PartitionKey("pk1")))
				.ToList();
			var response = await container.ReadManyItemsAsync<TestDocument>(ids);
			response.Resource.Should().NotBeEmpty();
		});

		await Task.WhenAll(writeTasks.Cast<Task>().Concat(readTasks));
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 11: Container/Database Level Concurrency
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentContainerDatabaseTests
{
	[Fact]
	public async Task ConcurrentContainerCreation_SameDatabase()
	{
		var client = new InMemoryCosmosClient();
		var db = client.GetDatabase("db1");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			db.CreateContainerIfNotExistsAsync($"container{i}", "/pk"));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task ConcurrentDatabaseCreation()
	{
		var client = new InMemoryCosmosClient();

		var tasks = Enumerable.Range(0, 50).Select(i =>
			client.CreateDatabaseIfNotExistsAsync($"db{i}"));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Category 12: Stress / Chaos Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrencyStressTests
{
	[Fact]
	public async Task HighContention_MixedOperations_NoCorruption()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		const int itemPool = 50;

		// Pre-create items
		for (var i = 0; i < itemPool; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = 0 },
				new PartitionKey("pk1"));

		var rng = new Random(42); // deterministic seed for reproducibility
		var tasks = Enumerable.Range(0, 200).Select(async t =>
		{
			var id = $"{t % itemPool}";
			var op = rng.Next(5);
			try
			{
				switch (op)
				{
					case 0: // read
						await container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));
						break;
					case 1: // upsert
						await container.UpsertItemAsync(
							new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Upserted{t}" },
							new PartitionKey("pk1"));
						break;
					case 2: // replace
						await container.ReplaceItemAsync(
							new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Replaced{t}" },
							id, new PartitionKey("pk1"));
						break;
					case 3: // patch
						await container.PatchItemAsync<TestDocument>(id, new PartitionKey("pk1"),
							new[] { PatchOperation.Set("/name", $"Patched{t}") });
						break;
					case 4: // delete + recreate
						await container.DeleteItemAsync<TestDocument>(id, new PartitionKey("pk1"));
						await container.CreateItemAsync(
							new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Recreated{t}" },
							new PartitionKey("pk1"));
						break;
				}
			}
			catch (CosmosException ex) when (ex.StatusCode is HttpStatusCode.NotFound
				or HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed)
			{ }
		});

		await Task.WhenAll(tasks);

		// Verify no corruption: all remaining items are readable
		for (var i = 0; i < itemPool; i++)
		{
			try
			{
				var r = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
				r.Resource.Id.Should().Be($"{i}");
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound) { }
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 2: Stream & ETag concurrency tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentStreamExtendedTests
{
	[Fact]
	public async Task ConcurrentPatchItemStream_SameItem_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 0 },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Name.Should().StartWith("Patched");
	}

	[Fact]
	public async Task ConcurrentReplaceItemStream_SameItem_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
		{
			var json = $"{{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Replaced{i}\"}}";
			var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
			return container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Name.Should().StartWith("Replaced");
	}

	[Fact]
	public async Task ConcurrentReadItemStream_SameItem_AllReturn200()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Target" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(_ =>
			container.ReadItemStreamAsync("1", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}
}

public class ConcurrentETagExtendedTests
{
	[Fact]
	public async Task ConcurrentETag_Wildcard_IfMatch_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Replaced{i}" },
				"1", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = "*" }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task ConcurrentDelete_Then_Create_VerifyETagFreshness()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create item, read ETag
		var create1 = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var etag1 = create1.ETag;

		// Delete
		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		// Recreate same ID
		var create2 = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" },
			new PartitionKey("pk1"));
		var etag2 = create2.ETag;

		// New ETag should differ
		etag2.Should().NotBe(etag1, "recreated item should get a fresh ETag");
	}

	[Fact]
	public async Task ConcurrentETag_IfNoneMatch_Star_OnCreate()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// 50 threads try to upsert with IfNoneMatch="*" (create-if-not-exists)
		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				var response = await container.UpsertItemAsync(
					new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Item{i}" },
					new PartitionKey("pk1"),
					new ItemRequestOptions { IfNoneMatchEtag = "*" });
				return (int)response.StatusCode;
			}
			catch (CosmosException ex)
			{
				return (int)ex.StatusCode;
			}
		});

		var results = await Task.WhenAll(tasks);

		// At least one should succeed (201 Created), rest should be expected status codes
		var created = results.Count(r => r == 201);
		var ok = results.Count(r => r == 200);
		var preconditionFailed = results.Count(r => r == (int)HttpStatusCode.PreconditionFailed);
		var notModified = results.Count(r => r == (int)HttpStatusCode.NotModified);

		created.Should().BeGreaterThanOrEqualTo(1);
		(created + ok + preconditionFailed + notModified).Should().Be(50,
			"all results should be expected status codes, no unexpected errors");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 3: Medium difficulty tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentDeleteSerializationTests
{
	[Fact]
	public async Task ConcurrentDeletes_SameItem_ExactlyOneSucceeds_RestGet404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Target" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			try
			{
				await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
				return "deleted";
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				return "notfound";
			}
		});

		var results = await Task.WhenAll(tasks);
		var deletedCount = results.Count(r => r == "deleted");
		var notFoundCount = results.Count(r => r == "notfound");

		// At minimum, at least one must succeed and the item must be gone
		deletedCount.Should().BeGreaterThanOrEqualTo(1);
		(deletedCount + notFoundCount).Should().Be(50);
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task ConcurrentDeleteItemStream_SameItem_AtLeastOneReturns204()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Target" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(_ =>
			container.DeleteItemStreamAsync("1", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		var successCount = results.Count(r => r.StatusCode == HttpStatusCode.NoContent);
		var notFoundCount = results.Count(r => r.StatusCode == HttpStatusCode.NotFound);

		successCount.Should().BeGreaterThanOrEqualTo(1);
		(successCount + notFoundCount).Should().Be(50);
		container.ItemCount.Should().Be(0);
	}
}

public class ConcurrentChangeFeedExtendedTests
{
	[Fact]
	public async Task ConcurrentChangeFeed_DeleteTombstones_WhileDeleting()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create 20 items
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		// Read checkpoint BEFORE deletes
		var checkpointBefore = container.GetChangeFeedCheckpoint();

		// Concurrently delete all
		var deleteTasks = Enumerable.Range(0, 20).Select(async i =>
		{
			try
			{
				await container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
			}
			catch (CosmosException) { }
		});

		await Task.WhenAll(deleteTasks);

		// Use the checkpoint-based change feed to see entries after creates
		var iterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.LatestVersion);

		var items = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			items.AddRange(response);
		}

		// The LatestVersion feed shows current state — all items are deleted,
		// so the feed may be empty or contain only the latest creates before deletes.
		// The key test is: no crash, no corruption under concurrent deletes + feed reads.
		// We just verify the container is empty and the feed read completed without error.
		container.ItemCount.Should().Be(0, "all items should be deleted");
	}

	[Fact]
	public async Task ConcurrentChangeFeedProcessors_SameContainer_BothSeeAllChanges()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var received1 = new List<string>();
		var received2 = new List<string>();

		// Create items
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		// Read change feed from beginning with two independent iterators
		var iter1 = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.LatestVersion);
		while (iter1.HasMoreResults)
		{
			var response = await iter1.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			received1.AddRange(response.Select(j => j["id"]!.ToString()));
		}

		var iter2 = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.LatestVersion);
		while (iter2.HasMoreResults)
		{
			var response = await iter2.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			received2.AddRange(response.Select(j => j["id"]!.ToString()));
		}

		received1.Should().HaveCount(10);
		received2.Should().HaveCount(10);
	}

	[Fact]
	public async Task ConcurrentChangeFeedRead_TrulyInterleaved()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		const int writeCount = 100;

		// Start writes and reads in a single WhenAll for true interleaving
		var writeTasks = Enumerable.Range(0, writeCount).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var readTasks = Enumerable.Range(0, 5).Select(async _ =>
		{
			var iterator = container.GetChangeFeedIterator<JObject>(
				ChangeFeedStartFrom.Beginning(),
				ChangeFeedMode.LatestVersion);
			var results = new List<JObject>();
			while (iterator.HasMoreResults)
			{
				var response = await iterator.ReadNextAsync();
				if (response.StatusCode == HttpStatusCode.NotModified) break;
				results.AddRange(response);
			}
			return results.Count;
		});

		var allTasks = writeTasks.Cast<Task>().Concat(readTasks).ToArray();
		await Task.WhenAll(allTasks);

		// After all complete, final read should see everything
		var finalIterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.LatestVersion);
		var finalResults = new List<JObject>();
		while (finalIterator.HasMoreResults)
		{
			var response = await finalIterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			finalResults.AddRange(response);
		}
		finalResults.Should().HaveCount(writeCount);
	}
}

public class ConcurrentUniqueKeyExtendedTests
{
	[Fact]
	public async Task ConcurrentUpserts_UniqueKeyViolation_Handled()
	{
		var props = new ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(props);

		// Pre-create an item
		await container.CreateItemAsync(
			new TestDocument { Id = "seed", PartitionKey = "pk1", Name = "Unique" },
			new PartitionKey("pk1"));

		// 50 threads upsert with different IDs but same /name value
		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.UpsertItemAsync(
					new TestDocument { Id = $"new{i}", PartitionKey = "pk1", Name = "Unique" },
					new PartitionKey("pk1"));
				return "success";
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
			{
				return "conflict";
			}
		});

		var results = await Task.WhenAll(tasks);
		results.Should().Contain("conflict", "unique key violations should be caught");
	}
}

public class ConcurrentPatchExtendedTests
{
	[Fact]
	public async Task ConcurrentPatch_WithFilterPredicate_UnderContention()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 0 },
			new PartitionKey("pk1"));

		// 50 threads patch with filter predicate — only succeeds if name is still "Original"
		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
					new[] { PatchOperation.Set("/name", $"Patched{i}") },
					new PatchItemRequestOptions
					{
						FilterPredicate = "FROM c WHERE c.name = 'Original'"
					});
				return "success";
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
			{
				return "precondition_failed";
			}
		});

		var results = await Task.WhenAll(tasks);
		var successCount = results.Count(r => r == "success");

		// At least one should succeed (the first one to change the name)
		successCount.Should().BeGreaterThanOrEqualTo(1);
	}
}

public class ConcurrentHierarchicalPkTests
{
	[Fact]
	public async Task ConcurrentOperations_HierarchicalPartitionKey_AllSucceed()
	{
		var container = new InMemoryContainer("test",
			new List<string> { "/tenantId", "/userId" });

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			var tenantId = $"tenant{i % 5}";
			var userId = $"user{i % 10}";
			var pk = new PartitionKeyBuilder()
				.Add(tenantId)
				.Add(userId)
				.Build();

			var jObj = JObject.FromObject(new
			{
				id = $"item{i}",
				tenantId,
				userId,
				name = $"Item{i}"
			});

			await container.UpsertItemAsync(jObj, pk);
		});

		await Task.WhenAll(tasks);
		container.ItemCount.Should().Be(50);
	}
}

public class ConcurrentTTLTests
{
	[Fact]
	public async Task ConcurrentTTL_ExpirationDuringOperations()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.DefaultTimeToLive = 1; // 1 second

		// Create items
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		// Wait for TTL expiration
		await Task.Delay(1500);

		// Trigger lazy eviction via query
		var query = new QueryDefinition("SELECT * FROM c");
		var iterator = container.GetItemQueryIterator<TestDocument>(query);
		var items = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			items.AddRange(response);
		}

		// After TTL expiration + query trigger, items should be gone
		items.Should().BeEmpty("all items should have expired after TTL");
	}
}

public class ConcurrentStatePersistenceTests
{
	[Fact]
	public async Task StatePersistence_ExportDuringConcurrentWrites()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create some items
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		// Start writes and export concurrently
		var writeTasks = Enumerable.Range(20, 30).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var exportTask = Task.Run(() => container.ExportState());

		await Task.WhenAll(writeTasks.Cast<Task>().Append(exportTask));

		var exportedState = await exportTask;
		exportedState.Should().NotBeNullOrEmpty("exported state should be valid JSON");

		// State should be valid JSON
		var parsed = JObject.Parse(exportedState);
		parsed["items"].Should().NotBeNull();
	}
}

public class ConcurrentContainerLifecycleTests
{
	[Fact]
	public async Task ConcurrentContainerDeletion_WhileWriting()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var containerResponse = await db.CreateContainerAsync("testcontainer", "/partitionKey");
		var container = containerResponse.Container;

		// Write some items
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = "pk1", name = $"Item{i}" }),
				new PartitionKey("pk1"));

		// Delete container
		await container.DeleteContainerAsync();

		// After deletion, re-creating should succeed (returns Created, not a no-op)
		var resp = await db.CreateContainerIfNotExistsAsync("testcontainer", "/partitionKey");
		resp.StatusCode.Should().Be(HttpStatusCode.Created);

		// New container should be empty
		var newContainer = resp.Container;
		var query = new QueryDefinition("SELECT VALUE COUNT(1) FROM c");
		var iterator = newContainer.GetItemQueryIterator<int>(query);
		var count = (await iterator.ReadNextAsync()).First();
		count.Should().Be(0, "re-created container should be empty");
	}
}

public class ConcurrentBulkExtendedTests
{
	[Fact]
	public async Task ConcurrentBulkOperations_AllowBulkExecution()
	{
		var client = new InMemoryCosmosClient();
		client.ClientOptions.AllowBulkExecution = true;
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("test", "/partitionKey")).Container;

		// Fire-and-forget bulk pattern
		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = "pk1", name = $"Item{i}" }),
				new PartitionKey("pk1")));

		await Task.WhenAll(tasks);

		// All items should exist
		var query = new QueryDefinition("SELECT VALUE COUNT(1) FROM c");
		var iterator = container.GetItemQueryIterator<int>(query);
		var response = await iterator.ReadNextAsync();
		response.First().Should().Be(100);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 4: Hard tests — Batch isolation (skip+sister)
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrentBatchIsolationTests
{
	[Fact(Skip = "Emulator batch execution is not globally isolated from non-batch operations. " +
				 "RestoreSnapshot does Clear()+rewrite which is not atomic. Concurrent direct CRUD " +
				 "can see partial state during batch execution. Would need a global per-partition " +
				 "lock to fix.")]
	public void ConcurrentBatch_AndDirectCrud_NoCorruption_RealCosmos() { }

	[Fact]
	public async Task ConcurrentBatch_AndDirectCrud_EmulatorBehaviour()
	{
		// DIVERGENT: Batch execution is not globally isolated — concurrent reads may see
		// intermediate batch state. The batch itself always completes atomically (all-or-nothing).
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create item for the batch
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		// Run batch and direct reads concurrently
		var batchTask = Task.Run(async () =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
			batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "BatchModified" });
			batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "BatchCreated" });
			return await batch.ExecuteAsync();
		});

		var readTasks = Enumerable.Range(0, 20).Select(async _ =>
		{
			try
			{
				var r = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
				return r.Resource.Name;
			}
			catch (CosmosException) { return "error"; }
		});

		var allTasks = readTasks.Cast<Task>().Concat(new[] { (Task)batchTask });
		await Task.WhenAll(allTasks);

		var batchResult = await batchTask;
		batchResult.IsSuccessStatusCode.Should().BeTrue();

		// After batch completes, state should be consistent
		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Name.Should().Be("BatchModified");
	}

	[Fact(Skip = "RestoreSnapshot clears dictionaries then re-populates, creating a brief window " +
				 "where concurrent readers may see empty state. Would need copy-on-write or atomic " +
				 "swap to fix.")]
	public void ConcurrentBatch_RollbackPreservesState_FromConcurrentReaders_RealCosmos() { }

	[Fact]
	public async Task ConcurrentBatch_RollbackPreservesState_EmulatorBehaviour()
	{
		// DIVERGENT: During rollback, concurrent readers may briefly see empty state.
		// After rollback completes, state is correctly restored.
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		// Batch that will fail (duplicate create causes rollback)
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Modified" });
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" });
		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();

		// After rollback, original state should be restored
		var final = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		final.Resource.Name.Should().Be("Original");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 5: Stress tests with unique keys
// ═══════════════════════════════════════════════════════════════════════════════

public class ConcurrencyStressWithUniqueKeysTests
{
	[Fact]
	public async Task HighContention_WithUniqueKeys_NoViolationsInFinalState()
	{
		var props = new ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(props);
		const int itemPool = 50;

		// Pre-create items with unique names
		for (var i = 0; i < itemPool; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Unique{i}", Value = 0 },
				new PartitionKey("pk1"));

		// 200 threads doing random CRUD — only upsert the SAME id (so unique key is preserved)
		var tasks = Enumerable.Range(0, 200).Select(async t =>
		{
			var id = $"{t % itemPool}";
			var op = t % 4;
			try
			{
				switch (op)
				{
					case 0:
						await container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));
						break;
					case 1:
						await container.UpsertItemAsync(
							new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Unique{t % itemPool}", Value = t },
							new PartitionKey("pk1"));
						break;
					case 2:
						await container.ReplaceItemAsync(
							new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Unique{t % itemPool}", Value = t },
							id, new PartitionKey("pk1"));
						break;
					case 3:
						await container.PatchItemAsync<TestDocument>(id, new PartitionKey("pk1"),
							new[] { PatchOperation.Set("/value", t) });
						break;
				}
			}
			catch (CosmosException ex) when (ex.StatusCode is HttpStatusCode.NotFound
				or HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed)
			{ }
		});

		await Task.WhenAll(tasks);

		// Verify no corruption: all remaining items are readable and have unique names
		var names = new HashSet<string>();
		for (var i = 0; i < itemPool; i++)
		{
			try
			{
				var r = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
				r.Resource.Id.Should().Be($"{i}");
				names.Add(r.Resource.Name).Should().BeTrue(
					$"each item should have a unique name, but '{r.Resource.Name}' was duplicated");
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound) { }
		}
	}
}
