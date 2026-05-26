using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Reproduces a bug where concurrent transactional batch operations with ETag-based
/// optimistic concurrency don't properly serialize within the same partition.
/// Real Cosmos DB serializes batch execution within a logical partition, ensuring
/// the second batch sees effects of the first and fails on stale ETags.
/// </summary>
public class TransactionalBatchConcurrentEtagBugTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task ConcurrentBatches_SameItem_ETagConflict_OneFailsOnePasses()
	{
		// Seed the item
		var doc = JObject.FromObject(new { id = "counter", pk = "a", count = 0 });
		await _container.CreateItemAsync(doc, new PartitionKey("a"));

		// Read the item to get initial ETag
		var readResponse = await _container.ReadItemAsync<JObject>("counter", new PartitionKey("a"));
		var initialEtag = readResponse.ETag;

		// Two concurrent batches both trying to replace the same item with the same stale ETag
		var batch1 = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch1.ReplaceItem("counter",
			JObject.FromObject(new { id = "counter", pk = "a", count = 1 }),
			new TransactionalBatchItemRequestOptions { IfMatchEtag = initialEtag });

		var batch2 = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch2.ReplaceItem("counter",
			JObject.FromObject(new { id = "counter", pk = "a", count = 2 }),
			new TransactionalBatchItemRequestOptions { IfMatchEtag = initialEtag });

		// Execute both concurrently
		var results = await Task.WhenAll(batch1.ExecuteAsync(), batch2.ExecuteAsync());

		// One should succeed, the other should fail with PreconditionFailed
		var statuses = results.Select(r => r.StatusCode).OrderBy(s => s).ToArray();
		var successCount = statuses.Count(s => s == HttpStatusCode.OK);
		var failCount = statuses.Count(s => s == HttpStatusCode.PreconditionFailed);

		successCount.Should().Be(1, "exactly one batch should succeed");
		failCount.Should().Be(1, "exactly one batch should fail with PreconditionFailed (412)");
	}

	[Fact]
	public async Task ConcurrentBatches_CreateSameItem_OneConflicts()
	{
		// Two concurrent batches both trying to create the same item
		var batch1 = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch1.CreateItem(JObject.FromObject(new { id = "unique-doc", pk = "a", version = 1 }));

		var batch2 = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch2.CreateItem(JObject.FromObject(new { id = "unique-doc", pk = "a", version = 2 }));

		var results = await Task.WhenAll(batch1.ExecuteAsync(), batch2.ExecuteAsync());

		var statuses = results.Select(r => r.StatusCode).OrderBy(s => s).ToArray();
		var successCount = statuses.Count(s => s == HttpStatusCode.OK || s == HttpStatusCode.Created);
		var conflictCount = statuses.Count(s => s == HttpStatusCode.Conflict);

		successCount.Should().Be(1, "exactly one batch should succeed");
		conflictCount.Should().Be(1, "exactly one batch should fail with Conflict (409)");
	}

	[Fact]
	public async Task ConcurrentBatches_AtomicCounter_IncrementPattern()
	{
		// Seed the "latest" counter doc
		var latest = JObject.FromObject(new { id = "latest", pk = "a", attemptNumber = 0 });
		await _container.CreateItemAsync(latest, new PartitionKey("a"));

		// Simulate 3 concurrent "read-modify-write" operations via batches
		async Task<int> IncrementWithRetry()
		{
			for (var retry = 0; retry < 10; retry++)
			{
				var read = await _container.ReadItemAsync<JObject>("latest", new PartitionKey("a"));
				var currentNumber = read.Resource["attemptNumber"]!.Value<int>();
				var newNumber = currentNumber + 1;
				var etag = read.ETag;

				var batch = _container.CreateTransactionalBatch(new PartitionKey("a"));
				batch.ReplaceItem("latest",
					JObject.FromObject(new { id = "latest", pk = "a", attemptNumber = newNumber }),
					new TransactionalBatchItemRequestOptions { IfMatchEtag = etag });
				batch.CreateItem(JObject.FromObject(new { id = $"attempt-{newNumber}", pk = "a", number = newNumber }));

				var result = await batch.ExecuteAsync();
				if (result.IsSuccessStatusCode)
					return newNumber;

				// Retry on conflict/precondition failed
				if (result.StatusCode is HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed)
					continue;

				throw new Exception($"Unexpected batch status: {result.StatusCode}");
			}

			throw new Exception("Too many retries");
		}

		var tasks = Enumerable.Range(0, 3).Select(_ => IncrementWithRetry()).ToArray();
		var results = await Task.WhenAll(tasks);

		var sorted = results.OrderBy(x => x).ToArray();
		sorted.Should().BeEquivalentTo([1, 2, 3],
			"transactional batch with ETag should prevent duplicate attempt numbers");
	}
}

/// <summary>
/// Regression tests for a bug where a failing batch's RestoreSnapshot would wipe out
/// items created by concurrent successful batches. The old RestoreSnapshot did
/// _items.Clear() then re-populated from the snapshot, destroying concurrent writes.
/// Fixed by tracking which keys each batch touches and only restoring those on failure.
/// </summary>
public class TransactionalBatchConcurrentRollbackTests
{
	[Fact]
	public async Task FailingBatch_DoesNotWipeOutConcurrentSuccessfulBatchWrites()
	{
		var container = new InMemoryContainer("test", "/pk");

		// Batch A creates items successfully
		var batchA = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batchA.CreateItem(JObject.FromObject(new { id = "itemA1", pk = "pk1" }));
		batchA.CreateItem(JObject.FromObject(new { id = "itemA2", pk = "pk1" }));

		// Batch B will fail (creates itemB1, then tries to create itemA1 which conflicts)
		var batchB = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batchB.CreateItem(JObject.FromObject(new { id = "itemB1", pk = "pk1" }));
		batchB.CreateItem(JObject.FromObject(new { id = "itemA1", pk = "pk1" })); // conflict with batch A

		// Execute A first, then B
		var resultA = await batchA.ExecuteAsync();
		resultA.IsSuccessStatusCode.Should().BeTrue("Batch A should succeed");

		var resultB = await batchB.ExecuteAsync();
		resultB.IsSuccessStatusCode.Should().BeFalse("Batch B should fail due to conflict on itemA1");
		resultB.StatusCode.Should().Be(HttpStatusCode.Conflict);

		// Batch A's items should still be present after Batch B's rollback
		var readA1 = await container.ReadItemAsync<JObject>("itemA1", new PartitionKey("pk1"));
		readA1.Should().NotBeNull("itemA1 from Batch A should survive Batch B's rollback");

		var readA2 = await container.ReadItemAsync<JObject>("itemA2", new PartitionKey("pk1"));
		readA2.Should().NotBeNull("itemA2 from Batch A should survive Batch B's rollback");

		// Batch B's itemB1 should NOT exist (it was rolled back)
		var act = () => container.ReadItemAsync<JObject>("itemB1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => ex.StatusCode == HttpStatusCode.NotFound,
				"itemB1 should have been rolled back by Batch B's failure");
	}

	[Fact]
	public async Task ConcurrentBatches_FailingBatchRollback_PreservesConcurrentWrites()
	{
		var container = new InMemoryContainer("test", "/pk");

		// Pre-create a seed item to cause conflict
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "seed", pk = "pk1" }),
			new PartitionKey("pk1"));

		// Task A: creates new items in a batch (should succeed)
		var taskA = Task.Run(async () =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
			batch.CreateItem(JObject.FromObject(new { id = "fromA", pk = "pk1", source = "A" }));
			return await batch.ExecuteAsync();
		});

		// Task B: tries to create "seed" (conflict) in a batch (should fail)
		var taskB = Task.Run(async () =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
			batch.CreateItem(JObject.FromObject(new { id = "seed", pk = "pk1", source = "B" })); // conflict
			return await batch.ExecuteAsync();
		});

		var results = await Task.WhenAll(taskA, taskB);

		// Task A should succeed, Task B should fail
		var successBatch = results.First(r => r.IsSuccessStatusCode);
		var failedBatch = results.First(r => !r.IsSuccessStatusCode);

		successBatch.Should().NotBeNull("one batch should succeed");
		failedBatch.StatusCode.Should().Be(HttpStatusCode.Conflict);

		// Items from the successful batch should be preserved
		var readSeed = await container.ReadItemAsync<JObject>("seed", new PartitionKey("pk1"));
		readSeed.Should().NotBeNull("seed item should still exist");

		var readFromA = await container.ReadItemAsync<JObject>("fromA", new PartitionKey("pk1"));
		readFromA.Should().NotBeNull("items from successful batch should survive concurrent rollback");
	}

	[Fact]
	public async Task ConcurrentBatches_MultipleCreateRetryPattern_NoAttemptNumberDuplicates()
	{
		// This reproduces the exact pattern from the reported bug:
		// Two concurrent Save() calls that each read-then-batch with retry
		var container = new InMemoryContainer("test", "/runNumber");

		async Task<int> SaveWithRetry(int runNumber)
		{
			for (var retry = 0; retry < 10; retry++)
			{
				// Read the "latest" document
				int attemptNumber;
				string? latestEtag = null;
				bool latestExists;

				try
				{
					var read = await container.ReadItemAsync<JObject>("latest", new PartitionKey(runNumber.ToString()));
					attemptNumber = read.Resource["attemptNumber"]!.Value<int>() + 1;
					latestEtag = read.ETag;
					latestExists = true;
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
				{
					attemptNumber = 1;
					latestExists = false;
				}

				var runId = $"{runNumber}-{attemptNumber}";
				var batch = container.CreateTransactionalBatch(new PartitionKey(runNumber.ToString()));

				if (latestExists)
				{
					batch.ReplaceItem("latest",
						JObject.FromObject(new { id = "latest", runNumber = runNumber.ToString(), attemptNumber }),
						new TransactionalBatchItemRequestOptions { IfMatchEtag = latestEtag });
				}
				else
				{
					batch.CreateItem(JObject.FromObject(new { id = "latest", runNumber = runNumber.ToString(), attemptNumber }));
				}

				batch.CreateItem(JObject.FromObject(new { id = runId, runNumber = runNumber.ToString(), attemptNumber }));

				var result = await batch.ExecuteAsync();
				if (result.IsSuccessStatusCode)
					return attemptNumber;

				if (result.StatusCode is HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed)
					continue;

				throw new Exception($"Unexpected batch status: {result.StatusCode}");
			}

			throw new Exception("Too many retries");
		}

		// Run two concurrent saves for the SAME run number (exactly like the reported bug)
		var tasks = new[] { SaveWithRetry(602021330), SaveWithRetry(602021330) };
		var results = await Task.WhenAll(tasks);

		var sorted = results.OrderBy(x => x).ToArray();
		sorted.Should().BeEquivalentTo(new[] { 1, 2 },
			"concurrent saves should produce unique sequential attempt numbers via retry");

		// Verify the latest doc has the highest attempt number
		var latest = await container.ReadItemAsync<JObject>("latest", new PartitionKey("602021330"));
		latest.Resource["attemptNumber"]!.Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task ConcurrentBatches_MultipleRunNumbers_MaintainsIsolation()
	{
		// Same as above but with two different run numbers (different partitions)
		var container = new InMemoryContainer("test", "/runNumber");

		async Task<int> SaveWithRetry(int runNumber)
		{
			for (var retry = 0; retry < 10; retry++)
			{
				int attemptNumber;
				string? latestEtag = null;
				bool latestExists;

				try
				{
					var read = await container.ReadItemAsync<JObject>("latest", new PartitionKey(runNumber.ToString()));
					attemptNumber = read.Resource["attemptNumber"]!.Value<int>() + 1;
					latestEtag = read.ETag;
					latestExists = true;
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
				{
					attemptNumber = 1;
					latestExists = false;
				}

				var runId = $"{runNumber}-{attemptNumber}";
				var batch = container.CreateTransactionalBatch(new PartitionKey(runNumber.ToString()));

				if (latestExists)
				{
					batch.ReplaceItem("latest",
						JObject.FromObject(new { id = "latest", runNumber = runNumber.ToString(), attemptNumber }),
						new TransactionalBatchItemRequestOptions { IfMatchEtag = latestEtag });
				}
				else
				{
					batch.CreateItem(JObject.FromObject(new { id = "latest", runNumber = runNumber.ToString(), attemptNumber }));
				}

				batch.CreateItem(JObject.FromObject(new { id = runId, runNumber = runNumber.ToString(), attemptNumber }));

				var result = await batch.ExecuteAsync();
				if (result.IsSuccessStatusCode)
					return attemptNumber;

				if (result.StatusCode is HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed)
					continue;

				throw new Exception($"Unexpected batch status: {result.StatusCode}");
			}

			throw new Exception("Too many retries");
		}

		var tasks = new[]
		{
			SaveWithRetry(100), SaveWithRetry(200),
			SaveWithRetry(100), SaveWithRetry(200)
		};
		var results = await Task.WhenAll(tasks);

		var run100 = new List<int>();
		var run200 = new List<int>();

		// We need to figure out which result goes to which run number
		// Since tasks are indexed, we know: [0]=100, [1]=200, [2]=100, [3]=200
		run100.Add(results[0]);
		run100.Add(results[2]);
		run200.Add(results[1]);
		run200.Add(results[3]);

		run100.OrderBy(x => x).Should().BeEquivalentTo(new[] { 1, 2 },
			"run 100 should have attempt numbers 1 and 2");
		run200.OrderBy(x => x).Should().BeEquivalentTo(new[] { 1, 2 },
			"run 200 should have attempt numbers 1 and 2");
	}
}
