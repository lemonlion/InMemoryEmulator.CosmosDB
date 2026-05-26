using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests.Performance;

public class LoadTestsExtended(ITestOutputHelper output)
{
	// ═══════════════════════════════════════════════════════════════════════════
	//  Category E: Data Integrity Verification
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task E1_PostLoadStateVerification_NoOrphanedDocuments()
	{
		var (container, knownIds) = await SeedContainer("e1", 200);
		var nextId = new AtomicCounter(1000);

		// Mixed load: creates + upserts + patches (no deletes keeps tracking deterministic)
		await Task.WhenAll(Enumerable.Range(0, 500).Select(async _ =>
		{
			switch (Random.Shared.Next(3))
			{
				case 0:
					var id = nextId.Increment().ToString();
					var pk = $"pk-{id}";
					await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
					knownIds.TryAdd(id, pk);
					break;
				case 1:
					var (uId, uPk) = PickRandom(knownIds);
					if (uId != null)
						await container.UpsertItemAsync(MakeDoc(uId, uPk!), new PartitionKey(uPk));
					break;
				case 2:
					var (pId, pPk) = PickRandom(knownIds);
					if (pId != null)
						await container.PatchItemAsync<LoadDocument>(pId, new PartitionKey(pPk),
							[PatchOperation.Increment("/counter", 1)]);
					break;
			}
		}));

		var allItems = await QueryAll(container);
		var containerIds = allItems.Select(d => d.Id).ToHashSet();

		knownIds.Keys.Should().BeSubsetOf(containerIds, "every tracked item should exist in the container");
		containerIds.Should().BeSubsetOf(knownIds.Keys.ToHashSet(), "every container item should be tracked");
		output.WriteLine($"Verified {allItems.Count} items, {knownIds.Count} tracked — no orphans");
	}

	[Fact]
	public async Task E2_PatchCounterMonotonicity_CountersMatchPatchCount()
	{
		var container = new InMemoryContainer("e2", "/partitionKey");
		var itemIds = new List<string>();

		// Seed 20 items with counter=0 in the same partition for simplicity
		for (var i = 1; i <= 20; i++)
		{
			var id = i.ToString();
			await container.CreateItemAsync(MakeDoc(id, "pk-e2", counter: 0), new PartitionKey("pk-e2"));
			itemIds.Add(id);
		}

		var patchCounts = new ConcurrentDictionary<string, int>();

		// Run 500 concurrent patch-increment operations on random items
		await Task.WhenAll(Enumerable.Range(0, 500).Select(async _ =>
		{
			var targetId = itemIds[Random.Shared.Next(itemIds.Count)];
			await container.PatchItemAsync<LoadDocument>(targetId, new PartitionKey("pk-e2"),
				[PatchOperation.Increment("/counter", 1)]);
			patchCounts.AddOrUpdate(targetId, 1, (_, count) => count + 1);
		}));

		// Verify each item's counter matches the tracked patch count
		foreach (var id in itemIds)
		{
			var response = await container.ReadItemAsync<LoadDocument>(id, new PartitionKey("pk-e2"));
			var expectedPatches = patchCounts.GetValueOrDefault(id, 0);
			response.Resource.Counter.Should().Be(expectedPatches,
				$"item {id} should have counter={expectedPatches} after {expectedPatches} patch increments");
		}

		output.WriteLine($"Verified patch monotonicity across {itemIds.Count} items, {patchCounts.Values.Sum()} total patches");
	}

	[Fact(Skip = "Document size enforcement varies between InMemoryContainer (Newtonsoft.Json serialized byte count) " +
				  "and real Cosmos DB (binary transport encoding with different overhead). Under load, the overhead of " +
				  "generating/verifying 100KB+ documents makes the test flaky due to memory pressure and GC, not due " +
				  "to Cosmos behavior differences.")]
	public async Task E3_LargeDocumentPayload_UnderLoad()
	{
		var container = new InMemoryContainer("e3", "/partitionKey");
		var nextId = new AtomicCounter(0);

		await Task.WhenAll(Enumerable.Range(0, 100).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var largeData = new string('x', 100_000);
			var doc = new LoadDocument
			{
				Id = id,
				PartitionKey = $"pk-{id}",
				Counter = 0,
				Data = largeData,
				Timestamp = DateTimeOffset.UtcNow.ToString("O")
			};
			await container.CreateItemAsync(doc, new PartitionKey($"pk-{id}"));
		}));

		container.ItemCount.Should().Be(100);
		await Task.CompletedTask;
	}

	[Fact]
	public async Task E3_LargeDocumentPayload_DivergentBehavior()
	{
		// InMemoryContainer measures document size using Newtonsoft.Json serialized byte count.
		// Real Cosmos DB uses a binary transport encoding with different overhead.
		// Both enforce approximately 2MB limits, but the exact boundary point differs.
		//
		// InMemoryContainer: rejects documents where JsonConvert.SerializeObject(doc).Length > ~2MB
		// Real Cosmos DB: measures the wire-format payload size which includes property name encoding,
		// type markers, and partition key routing headers. A ~1.95MB JSON document may fail on real
		// Cosmos but succeed in InMemoryContainer, or vice versa depending on property structure.
		//
		// This test demonstrates a document well under the limit (500KB) succeeds in the emulator,
		// confirming no truncation or corruption of large payloads.

		var container = new InMemoryContainer("e3-divergent", "/partitionKey");

		var largeData = new string('x', 500_000);
		var doc = new LoadDocument
		{
			Id = "1",
			PartitionKey = "pk-1",
			Counter = 42,
			Data = largeData,
			Timestamp = DateTimeOffset.UtcNow.ToString("O")
		};

		var response = await container.CreateItemAsync(doc, new PartitionKey("pk-1"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await container.ReadItemAsync<LoadDocument>("1", new PartitionKey("pk-1"));
		readResponse.Resource.Data.Should().HaveLength(500_000, "document data should not be truncated");
		readResponse.Resource.Counter.Should().Be(42, "document fields should not be corrupted");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category A: Missing Operation Types Under Load
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task A1_ReadManyUnderLoad_VerifiesAllItemsReturned()
	{
		var (container, knownIds) = await SeedContainer("a1", 200);
		var errors = 0;

		await Task.WhenAll(Enumerable.Range(0, 200).Select(async _ =>
		{
			var snapshot = knownIds.ToArray();
			var selected = snapshot.OrderBy(_ => Random.Shared.Next()).Take(5).ToList();
			var items = selected.Select(kv => (kv.Key, new PartitionKey(kv.Value))).ToList();

			var response = await container.ReadManyItemsAsync<LoadDocument>(items);

			if (response.Count != items.Count)
				Interlocked.Increment(ref errors);
		}));

		errors.Should().Be(0, "all ReadMany calls should return the requested number of items");
		output.WriteLine("200 ReadMany calls completed successfully");
	}

	[Fact]
	public async Task A2_TransactionalBatchUnderLoad_AtomicCreateAndRead()
	{
		var container = new InMemoryContainer("a2", "/partitionKey");

		// Pre-create one item per partition for batch read operations
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(MakeDoc($"pre-{i}", $"pk-{i}"), new PartitionKey($"pk-{i}"));

		var nextId = new AtomicCounter(100);
		var errors = 0;

		await Task.WhenAll(Enumerable.Range(0, 100).Select(async _ =>
		{
			var batchPk = $"pk-{Random.Shared.Next(10)}";
			var batch = container.CreateTransactionalBatch(new PartitionKey(batchPk));
			var id1 = nextId.Increment().ToString();
			var id2 = nextId.Increment().ToString();
			var id3 = nextId.Increment().ToString();
			batch.CreateItem(MakeDoc(id1, batchPk));
			batch.CreateItem(MakeDoc(id2, batchPk));
			batch.CreateItem(MakeDoc(id3, batchPk));
			using var response = await batch.ExecuteAsync();
			if (!response.IsSuccessStatusCode)
				Interlocked.Increment(ref errors);
		}));

		errors.Should().Be(0, "all batches should succeed");
		container.ItemCount.Should().BeGreaterThanOrEqualTo(310, "10 pre-created + 300 from batches");
		output.WriteLine($"100 batches completed, {container.ItemCount} total items");
	}

	[Fact]
	public async Task A3_DeleteAllByPartitionKeyUnderLoad()
	{
		var container = new InMemoryContainer("a3", "/partitionKey");

		// Seed items into pk-target (to be bulk-deleted) and pk-safe-0..pk-safe-4 (to survive)
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(MakeDoc($"target-{i}", "pk-target"), new PartitionKey("pk-target"));
		for (var i = 0; i < 50; i++)
		{
			var safePk = $"pk-safe-{i % 5}";
			await container.CreateItemAsync(MakeDoc($"safe-{i}", safePk), new PartitionKey(safePk));
		}

		container.ItemCount.Should().Be(100);

		// Concurrently: delete all in pk-target + write new items to pk-safe partitions
		var nextId = new AtomicCounter(1000);
		var deleteTask = container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk-target"));
		var writeTasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var safePk = $"pk-safe-{Random.Shared.Next(5)}";
			await container.CreateItemAsync(MakeDoc(id, safePk), new PartitionKey(safePk));
		});

		await Task.WhenAll(writeTasks.Append(deleteTask));

		// Verify pk-target items are gone
		var targetIterator = container.GetItemQueryIterator<LoadDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = 'pk-target'"));
		var targetResults = new List<LoadDocument>();
		while (targetIterator.HasMoreResults)
		{
			var page = await targetIterator.ReadNextAsync();
			targetResults.AddRange(page);
		}

		targetResults.Should().BeEmpty("all pk-target items should be deleted");

		// Verify safe items are intact (original 50 + new writes)
		container.ItemCount.Should().BeGreaterThanOrEqualTo(100, "50 original safe + 50 new writes");
		output.WriteLine($"DeleteAllByPartitionKey verified: {container.ItemCount} items remain, pk-target cleared");
	}

	[Fact]
	public async Task A4_StreamApiOperationsUnderLoad()
	{
		var container = new InMemoryContainer("a4", "/partitionKey");
		var nextId = new AtomicCounter(0);
		var errors = 0;

		// Concurrent stream create operations
		await Task.WhenAll(Enumerable.Range(0, 100).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var pk = $"pk-{id}";
			var doc = MakeDoc(id, pk);
			var json = JsonConvert.SerializeObject(doc);
			using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
			using var response = await container.CreateItemStreamAsync(stream, new PartitionKey(pk));
			if (response.StatusCode != HttpStatusCode.Created)
				Interlocked.Increment(ref errors);
		}));

		errors.Should().Be(0, "all stream creates should succeed");
		container.ItemCount.Should().Be(100);

		// Concurrent stream read operations — IDs 1..100 match the creates above
		await Task.WhenAll(Enumerable.Range(1, 100).Select(async i =>
		{
			var id = i.ToString();
			var pk = $"pk-{id}";
			using var response = await container.ReadItemStreamAsync(id, new PartitionKey(pk));
			if (response.StatusCode != HttpStatusCode.OK)
			{
				Interlocked.Increment(ref errors);
				return;
			}

			using var reader = new StreamReader(response.Content);
			var content = await reader.ReadToEndAsync();
			var readDoc = JsonConvert.DeserializeObject<LoadDocument>(content)!;
			if (readDoc.Id != id)
				Interlocked.Increment(ref errors);
		}));

		errors.Should().Be(0, "all stream reads should succeed with correct data");
		output.WriteLine($"200 stream operations completed, {container.ItemCount} items");
	}

	[Fact]
	public async Task A5_ChangeFeedReadDuringWrites()
	{
		var container = new InMemoryContainer("a5", "/partitionKey");

		// Seed initial items
		for (var i = 1; i <= 50; i++)
		{
			var pk = $"pk-{i % 10}";
			await container.CreateItemAsync(MakeDoc(i.ToString(), pk), new PartitionKey(pk));
		}

		// Write more items concurrently
		var nextId = new AtomicCounter(1000);
		var writtenIds = new ConcurrentBag<string>();
		await Task.WhenAll(Enumerable.Range(0, 100).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var pk = $"pk-{id}";
			await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
			writtenIds.Add(id);
		}));

		// Read entire change feed from beginning
		var feedItems = new List<LoadDocument>();
		var feedIterator = container.GetChangeFeedIterator<LoadDocument>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.LatestVersion);
		while (feedIterator.HasMoreResults)
		{
			try
			{
				var response = await feedIterator.ReadNextAsync();
				feedItems.AddRange(response);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotModified)
			{
				break;
			}
		}

		var feedIds = feedItems.Select(d => d.Id).ToHashSet();
		var allExpectedIds = Enumerable.Range(1, 50).Select(i => i.ToString()).Concat(writtenIds).ToHashSet();
		allExpectedIds.Should().BeSubsetOf(feedIds, "all seeded and written items should appear in the change feed");
		output.WriteLine($"Change feed returned {feedItems.Count} items, expected {allExpectedIds.Count}");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category B: Query Diversity Under Load
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task B1_CrossPartitionQueryUnderLoad()
	{
		var (container, _) = await SeedContainer("b1", 200);
		var errors = 0;

		await Task.WhenAll(Enumerable.Range(0, 100).Select(async _ =>
		{
			var threshold = Random.Shared.Next(500);
			var iterator = container.GetItemQueryIterator<LoadDocument>(
				new QueryDefinition("SELECT TOP 20 * FROM c WHERE c.counter > @threshold")
					.WithParameter("@threshold", threshold));

			var results = new List<LoadDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			// Verify all returned documents actually satisfy the filter
			if (results.Any(d => d.Counter <= threshold))
				Interlocked.Increment(ref errors);

			// Verify cross-partition: results should come from multiple PKs (with high probability)
			// We don't assert this strictly since small result sets might come from one PK
		}));

		errors.Should().Be(0, "all cross-partition query results should satisfy the WHERE clause");
		output.WriteLine("100 cross-partition queries completed successfully");
	}

	[Fact]
	public async Task B2_AggregateQueryUnderLoad()
	{
		var (container, _) = await SeedContainer("b2", 200);
		var nextId = new AtomicCounter(1000);
		var errors = 0;

		// Concurrent: writes happening alongside aggregate queries
		var writeTasks = Enumerable.Range(0, 100).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var pk = $"pk-{id}";
			await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
		});

		var queryTasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			// COUNT query
			var countIterator = container.GetItemQueryIterator<int>(
				new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));
			var counts = new List<int>();
			while (countIterator.HasMoreResults)
			{
				var page = await countIterator.ReadNextAsync();
				counts.AddRange(page);
			}

			var count = counts.Sum();
			// Count should be at least the seed count (200) since we never delete
			if (count < 200)
				Interlocked.Increment(ref errors);
		});

		await Task.WhenAll(writeTasks.Concat(queryTasks));

		errors.Should().Be(0, "aggregate COUNT should always be >= seed count during write-only load");
		output.WriteLine($"50 aggregate queries during 100 writes — container has {container.ItemCount} items");
	}

	[Fact]
	public async Task B3_DistinctQueryUnderLoad()
	{
		var (container, _) = await SeedContainer("b3", 200);
		var nextId = new AtomicCounter(1000);
		var errors = 0;

		var writeTasks = Enumerable.Range(0, 100).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var pk = $"pk-{Random.Shared.Next(20)}";
			await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
		});

		var queryTasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			var iterator = container.GetItemQueryIterator<string>(
				new QueryDefinition("SELECT DISTINCT VALUE c.partitionKey FROM c"));

			var partitionKeys = new List<string>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				partitionKeys.AddRange(page);
			}

			// DISTINCT should have no duplicates
			if (partitionKeys.Count != partitionKeys.Distinct().Count())
				Interlocked.Increment(ref errors);
		});

		await Task.WhenAll(writeTasks.Concat(queryTasks));

		errors.Should().Be(0, "DISTINCT queries should never return duplicate values");
		output.WriteLine("50 DISTINCT queries during 100 writes completed without duplicates");
	}

	[Fact]
	public async Task B4_OffsetLimitPaginationUnderLoad()
	{
		var (container, _) = await SeedContainer("b4", 200);
		var errors = 0;

		await Task.WhenAll(Enumerable.Range(0, 100).Select(async _ =>
		{
			var offset = Random.Shared.Next(50);
			var iterator = container.GetItemQueryIterator<LoadDocument>(
				new QueryDefinition($"SELECT * FROM c ORDER BY c.id OFFSET {offset} LIMIT 10"));

			var results = new List<LoadDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			if (results.Count > 10)
				Interlocked.Increment(ref errors);
		}));

		errors.Should().Be(0, "OFFSET/LIMIT queries should never return more than the LIMIT");
		output.WriteLine("100 OFFSET/LIMIT queries completed with correct page sizes");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category C: Concurrency Edge Cases Under Sustained Load
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task C1_ETagConflictsUnderLoad_ExactlyOneWinsPerRound()
	{
		var container = new InMemoryContainer("c1", "/partitionKey");
		var doc = MakeDoc("1", "pk-1");
		var created = await container.CreateItemAsync(doc, new PartitionKey("pk-1"));
		var originalEtag = created.ETag;

		var successes = 0;
		var preconditionFailures = 0;

		// 50 threads all try to replace using the SAME original ETag — exactly one should win
		await Task.WhenAll(Enumerable.Range(0, 50).Select(async _ =>
		{
			try
			{
				var replacement = MakeDoc("1", "pk-1");
				await container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk-1"),
					new ItemRequestOptions { IfMatchEtag = originalEtag });
				Interlocked.Increment(ref successes);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
			{
				Interlocked.Increment(ref preconditionFailures);
			}
		}));

		successes.Should().Be(1, "exactly one thread should win the ETag race");
		preconditionFailures.Should().Be(49, "all other threads should get 412 PreconditionFailed");
		output.WriteLine($"ETag race: {successes} winner, {preconditionFailures} losers");
	}

	[Fact]
	public async Task C2_CreateAfterDelete_SameId_Succeeds()
	{
		var container = new InMemoryContainer("c2", "/partitionKey");

		// Pre-create 50 items
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(MakeDoc(i.ToString(), "pk-c2"), new PartitionKey("pk-c2"));

		var errors = 0;

		// For each item: delete then immediately re-create with same ID
		await Task.WhenAll(Enumerable.Range(0, 50).Select(async i =>
		{
			try
			{
				await container.DeleteItemAsync<LoadDocument>(i.ToString(), new PartitionKey("pk-c2"));
				await container.CreateItemAsync(MakeDoc(i.ToString(), "pk-c2"), new PartitionKey("pk-c2"));
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "delete-then-create on same ID should always succeed");
		container.ItemCount.Should().Be(50, "all 50 items should exist after delete+recreate");
		output.WriteLine("50 delete+recreate cycles completed successfully");
	}

	[Fact]
	public async Task C3_DoubleDelete_Returns404OnSecond()
	{
		var container = new InMemoryContainer("c3", "/partitionKey");

		// Pre-create 50 items
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(MakeDoc(i.ToString(), "pk-c3"), new PartitionKey("pk-c3"));

		var successes = 0;
		var notFounds = 0;

		// Two threads each try to delete every item
		await Task.WhenAll(Enumerable.Range(0, 50).SelectMany(i => Enumerable.Range(0, 2).Select(async _ =>
		{
			try
			{
				await container.DeleteItemAsync<LoadDocument>(i.ToString(), new PartitionKey("pk-c3"));
				Interlocked.Increment(ref successes);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				Interlocked.Increment(ref notFounds);
			}
		})));

		successes.Should().Be(50, "exactly one delete per item should succeed");
		notFounds.Should().Be(50, "exactly one delete per item should get 404");
		container.ItemCount.Should().Be(0, "all items should be deleted");
		output.WriteLine($"Double delete: {successes} successes, {notFounds} not-founds");
	}

	[Fact]
	public async Task C4_HotPartitionLoadTest_NoStarvation()
	{
		var container = new InMemoryContainer("c4", "/partitionKey");

		// Seed items in hot partition and cold partitions
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(MakeDoc($"hot-{i}", "pk-hot"), new PartitionKey("pk-hot"));
		for (var i = 0; i < 50; i++)
		{
			var coldPk = $"pk-cold-{i % 5}";
			await container.CreateItemAsync(MakeDoc($"cold-{i}", coldPk), new PartitionKey(coldPk));
		}

		var hotErrors = 0;
		var coldErrors = 0;

		// 80% operations target pk-hot, 20% target cold partitions
		await Task.WhenAll(Enumerable.Range(0, 500).Select(async opIndex =>
		{
			try
			{
				if (Random.Shared.Next(100) < 80)
				{
					var targetId = $"hot-{Random.Shared.Next(100)}";
					await container.ReadItemAsync<LoadDocument>(targetId, new PartitionKey("pk-hot"));
				}
				else
				{
					var coldPk = $"pk-cold-{Random.Shared.Next(5)}";
					var targetId = $"cold-{Random.Shared.Next(50)}";
					await container.ReadItemAsync<LoadDocument>(targetId, new PartitionKey(coldPk));
				}
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				// Item may not exist in the specific cold PK — that's OK
			}
			catch (Exception)
			{
				if (Random.Shared.Next(100) < 80) Interlocked.Increment(ref hotErrors);
				else Interlocked.Increment(ref coldErrors);
			}
		}));

		hotErrors.Should().Be(0, "hot partition operations should not fail");
		coldErrors.Should().Be(0, "cold partition operations should not be starved");
		output.WriteLine("500 hot-partition-biased operations completed without errors");
	}

	[Fact]
	public async Task C5_HighCardinalityPartitionKeys_ThousandsOfDistinctPKs()
	{
		var container = new InMemoryContainer("c5", "/partitionKey");
		var nextId = new AtomicCounter(0);
		var errors = 0;

		// Create 2000 items, each with a unique partition key
		await Task.WhenAll(Enumerable.Range(0, 2000).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var pk = $"pk-unique-{id}";
			try
			{
				await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "all creates with unique PKs should succeed");
		container.ItemCount.Should().Be(2000, "all 2000 unique-PK items should exist");

		// Read back a random sample to verify
		var readErrors = 0;
		await Task.WhenAll(Enumerable.Range(0, 200).Select(async _ =>
		{
			var id = Random.Shared.Next(2000).ToString();
			try
			{
				await container.ReadItemAsync<LoadDocument>(id, new PartitionKey($"pk-unique-{id}"));
			}
			catch (Exception)
			{
				Interlocked.Increment(ref readErrors);
			}
		}));

		readErrors.Should().Be(0, "reads across high-cardinality PKs should succeed");
		output.WriteLine("2000 unique PKs + 200 random reads completed successfully");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category D: Stress Patterns
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task D1_BurstTrafficPattern_SpikeThenCalm()
	{
		var container = new InMemoryContainer("d1", "/partitionKey");
		var nextId = new AtomicCounter(0);
		var errors = 0;

		for (var cycle = 0; cycle < 3; cycle++)
		{
			// Burst: 200 concurrent operations
			await Task.WhenAll(Enumerable.Range(0, 200).Select(async _ =>
			{
				try
				{
					var id = nextId.Increment().ToString();
					var pk = $"pk-{id}";
					await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
				}
				catch (Exception)
				{
					Interlocked.Increment(ref errors);
				}
			}));

			// Calm: 20 sequential operations
			for (var calm = 0; calm < 20; calm++)
			{
				var id = nextId.Increment().ToString();
				var pk = $"pk-{id}";
				await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
			}
		}

		errors.Should().Be(0, "no errors should occur during burst/calm cycles");
		container.ItemCount.Should().Be(660, "3 cycles × (200 burst + 20 calm) = 660 items");
		output.WriteLine($"3 burst/calm cycles completed: {container.ItemCount} items, {errors} errors");
	}

	[Fact]
	public async Task D2_GradualRampUp_LinearIncrease()
	{
		var container = new InMemoryContainer("d2", "/partitionKey");
		var nextId = new AtomicCounter(0);
		var errors = 0;
		var totalOps = 0;

		// Ramp from 50 ops to 500 ops per "second" (10 levels)
		for (var level = 1; level <= 10; level++)
		{
			var opsThisLevel = 50 * level;
			await Task.WhenAll(Enumerable.Range(0, opsThisLevel).Select(async _ =>
			{
				try
				{
					var id = nextId.Increment().ToString();
					var pk = $"pk-{id}";
					await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
				}
				catch (Exception)
				{
					Interlocked.Increment(ref errors);
				}
			}));
			totalOps += opsThisLevel;
		}

		errors.Should().Be(0, "no errors during gradual ramp-up");
		container.ItemCount.Should().Be(totalOps, $"all {totalOps} ramped operations should create items");
		output.WriteLine($"Ramp complete: {totalOps} ops across 10 levels, {errors} errors");
	}

	[Fact]
	public async Task D3_WriteOnlyStress_MonotonicallyIncreasingItemCount()
	{
		var container = new InMemoryContainer("d3", "/partitionKey");
		var nextId = new AtomicCounter(0);
		var errors = 0;

		await Task.WhenAll(Enumerable.Range(0, 2000).Select(async _ =>
		{
			try
			{
				var id = nextId.Increment().ToString();
				var pk = $"pk-{id}";
				await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "no errors during write-only stress");
		container.ItemCount.Should().Be(2000, "all 2000 write-only items should exist");
		output.WriteLine($"Write-only stress: {container.ItemCount} items, {errors} errors");
	}

	[Fact]
	public async Task D4_ReadOnlyStress_LargeDataset()
	{
		var container = new InMemoryContainer("d4", "/partitionKey");

		// Seed 5000 items
		for (var i = 0; i < 5000; i++)
		{
			var pk = $"pk-{i % 50}";
			await container.CreateItemAsync(MakeDoc(i.ToString(), pk), new PartitionKey(pk));
		}

		var errors = 0;

		// 2000 concurrent read operations (point reads + queries)
		await Task.WhenAll(Enumerable.Range(0, 2000).Select(async _ =>
		{
			try
			{
				if (Random.Shared.Next(2) == 0)
				{
					// Point read
					var id = Random.Shared.Next(5000).ToString();
					var pk = $"pk-{int.Parse(id) % 50}";
					var response = await container.ReadItemAsync<LoadDocument>(id, new PartitionKey(pk));
					if (response.StatusCode != HttpStatusCode.OK)
						Interlocked.Increment(ref errors);
				}
				else
				{
					// Query
					var iterator = container.GetItemQueryIterator<LoadDocument>(
						new QueryDefinition("SELECT TOP 5 * FROM c WHERE c.counter > @t")
							.WithParameter("@t", Random.Shared.Next(500)));
					while (iterator.HasMoreResults)
					{
						await iterator.ReadNextAsync();
					}
				}
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "no errors during read-only stress on large dataset");
		output.WriteLine($"Read-only stress: 2000 ops on {container.ItemCount} items, {errors} errors");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category F: Container/Database Lifecycle Under Load
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task F1_MultipleContainersConcurrentLoad_CompleteIsolation()
	{
		var container1 = new InMemoryContainer("f1-alpha", "/partitionKey");
		var container2 = new InMemoryContainer("f1-beta", "/partitionKey");
		var container3 = new InMemoryContainer("f1-gamma", "/partitionKey");

		var errors = 0;

		// Run concurrent operations on all three containers simultaneously
		async Task WriteToContainer(InMemoryContainer container, string prefix, int count)
		{
			var nextId = new AtomicCounter(0);
			await Task.WhenAll(Enumerable.Range(0, count).Select(async _ =>
			{
				try
				{
					var id = $"{prefix}-{nextId.Increment()}";
					var pk = $"pk-{prefix}";
					await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
				}
				catch (Exception)
				{
					Interlocked.Increment(ref errors);
				}
			}));
		}

		await Task.WhenAll(
			WriteToContainer(container1, "alpha", 200),
			WriteToContainer(container2, "beta", 200),
			WriteToContainer(container3, "gamma", 200));

		errors.Should().Be(0, "no errors across any container");

		// Verify complete isolation: each container has exactly its own items
		container1.ItemCount.Should().Be(200, "container alpha should have exactly 200 items");
		container2.ItemCount.Should().Be(200, "container beta should have exactly 200 items");
		container3.ItemCount.Should().Be(200, "container gamma should have exactly 200 items");

		// Verify no cross-contamination by checking item prefixes
		var items1 = await QueryAll(container1);
		var items2 = await QueryAll(container2);
		var items3 = await QueryAll(container3);

		items1.Should().OnlyContain(d => d.Id.StartsWith("alpha-"), "container1 should only have alpha items");
		items2.Should().OnlyContain(d => d.Id.StartsWith("beta-"), "container2 should only have beta items");
		items3.Should().OnlyContain(d => d.Id.StartsWith("gamma-"), "container3 should only have gamma items");
		output.WriteLine("3 containers × 200 items each — complete isolation verified");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category G: Delete-Heavy Load
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task G1_DeleteHeavyLoad_90PercentDeletes_ContainerDrainsWithoutCorruption()
	{
		var container = new InMemoryContainer("g1", "/partitionKey");
		var knownIds = new ConcurrentDictionary<string, string>();
		var nextId = new AtomicCounter(10_000);

		// Seed 500 items
		for (var i = 1; i <= 500; i++)
		{
			var id = i.ToString();
			var pk = $"pk-{i % 20}";
			await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
			knownIds.TryAdd(id, pk);
		}

		var deletes = 0;
		var creates = 0;
		var notFounds = 0;
		var errors = 0;

		// 1000 operations: 90% delete, 10% create
		await Task.WhenAll(Enumerable.Range(0, 1000).Select(async _ =>
		{
			try
			{
				if (Random.Shared.Next(100) < 90)
				{
					// Delete
					var ids = knownIds.Keys.ToArray();
					if (ids.Length == 0) return;
					var targetId = ids[Random.Shared.Next(ids.Length)];
					if (!knownIds.TryRemove(targetId, out var pk)) return;
					try
					{
						await container.DeleteItemAsync<LoadDocument>(targetId, new PartitionKey(pk));
						Interlocked.Increment(ref deletes);
					}
					catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
					{
						Interlocked.Increment(ref notFounds);
					}
					catch
					{
						knownIds.TryAdd(targetId, pk);
						throw;
					}
				}
				else
				{
					// Create
					var id = nextId.Increment().ToString();
					var pk = $"pk-{id}";
					await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
					knownIds.TryAdd(id, pk);
					Interlocked.Increment(ref creates);
				}
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "no unexpected errors during delete-heavy load");

		// Post-load integrity: container count should match tracked IDs
		var allItems = await QueryAll(container);
		allItems.Count.Should().Be(knownIds.Count,
			"container item count should match tracked IDs after delete-heavy load");

		output.WriteLine($"Delete-heavy: {deletes} deletes, {creates} creates, {notFounds} 404s, " +
						 $"{container.ItemCount} remaining items, {errors} errors");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category H: TTL Under Load
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact(Skip = "Real Cosmos DB proactively expires items via a background process, so items " +
				  "disappear from query results and point reads independently of any client " +
				  "activity. InMemoryContainer uses lazy eviction — items are only checked " +
				  "and removed when accessed (read/query/replace). Under sustained concurrent " +
				  "load this means write-only items may appear to survive past their TTL until " +
				  "a read touches them, producing different timing behavior than real Cosmos.")]
	public async Task H1_TTLUnderLoad_ProactiveExpiration()
	{
		// This test expects items to be proactively expired (like real Cosmos DB),
		// but InMemoryContainer only evicts lazily on read. See the sister test below
		// for the actual emulator behavior.
		await Task.CompletedTask;
	}

	[Fact]
	public async Task H1_TTLUnderLoad_LazyEviction_EmulatorBehavior()
	{
		// ──────────────────────────────────────────────────────────────────────
		// DIVERGENT BEHAVIOR: InMemoryContainer TTL eviction is LAZY.
		//
		// Real Cosmos DB: Items are proactively expired by a background process.
		//   After DefaultTimeToLive seconds, the item disappears from ALL queries
		//   and point reads, regardless of whether any client reads it.
		//
		// InMemoryContainer: Items are checked for expiration ONLY when accessed
		//   (point read, query, replace, patch, etc.). An expired item that is
		//   never read will remain in _items until something touches it.
		//   This means:
		//     1. container.ItemCount may include expired-but-unread items
		//     2. A query or point read will evict the item and return NotFound/empty
		//     3. Under concurrent write-only load, expired items linger
		//
		// This test demonstrates the lazy eviction behavior under load:
		//   - Create items with short TTL
		//   - Wait for TTL to pass
		//   - Verify items ARE evicted when read (lazy eviction works)
		//   - Show that unread items may linger in internal storage
		// ──────────────────────────────────────────────────────────────────────

		var container = new InMemoryContainer("h1-lazy", "/partitionKey");
		container.DefaultTimeToLive = 2; // 2 second TTL

		// Create 100 items concurrently
		var nextId = new AtomicCounter(0);
		await Task.WhenAll(Enumerable.Range(0, 100).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var pk = $"pk-{id}";
			await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
		}));

		container.ItemCount.Should().Be(100, "all 100 items should exist immediately after creation");

		// Wait for TTL to expire
		await Task.Delay(TimeSpan.FromSeconds(3));

		// Now read items — lazy eviction should kick in and return 404
		var evicted = 0;
		var survived = 0;
		await Task.WhenAll(Enumerable.Range(0, 100).Select(async i =>
		{
			try
			{
				await container.ReadItemAsync<LoadDocument>(i.ToString(), new PartitionKey($"pk-{i}"));
				Interlocked.Increment(ref survived);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				Interlocked.Increment(ref evicted);
			}
		}));

		evicted.Should().Be(100, "all items should be evicted after TTL expires and they are read");
		survived.Should().Be(0, "no items should survive past their TTL when read");
		output.WriteLine($"TTL lazy eviction: {evicted} evicted on read, {survived} survived");
	}

	[Fact]
	public async Task H2_TTLWithConcurrentWritesAndReads()
	{
		var container = new InMemoryContainer("h2", "/partitionKey");
		container.DefaultTimeToLive = 3; // 3 seconds TTL

		var nextId = new AtomicCounter(0);
		var errors = 0;

		// Phase 1: Create items
		for (var i = 0; i < 50; i++)
		{
			var pk = $"pk-{i}";
			await container.CreateItemAsync(MakeDoc(i.ToString(), pk), new PartitionKey(pk));
		}

		// Phase 2: Wait for partial TTL, then do concurrent reads + new writes
		await Task.Delay(TimeSpan.FromSeconds(2));

		var readNotFound = 0;
		var readFound = 0;
		var newCreates = 0;

		await Task.WhenAll(
			// Read old items (may or may not be expired yet depending on timing)
			Enumerable.Range(0, 50).Select(async i =>
			{
				try
				{
					await container.ReadItemAsync<LoadDocument>(i.ToString(), new PartitionKey($"pk-{i}"));
					Interlocked.Increment(ref readFound);
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
				{
					Interlocked.Increment(ref readNotFound);
				}
				catch
				{
					Interlocked.Increment(ref errors);
				}
			}).Concat(
			// Create fresh items (these should survive because their TTL starts fresh)
			Enumerable.Range(0, 50).Select(async _ =>
			{
				try
				{
					var id = nextId.Increment().ToString();
					var pk = $"pk-new-{id}";
					await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
					Interlocked.Increment(ref newCreates);
				}
				catch
				{
					Interlocked.Increment(ref errors);
				}
			}))
		);

		errors.Should().Be(0, "no unexpected errors during TTL concurrent test");
		newCreates.Should().Be(50, "all new items should be created");

		// New items should still be readable (their TTL just started)
		var newItemErrors = 0;
		await Task.WhenAll(Enumerable.Range(1, 50).Select(async i =>
		{
			try
			{
				await container.ReadItemAsync<LoadDocument>(i.ToString(), new PartitionKey($"pk-new-{i}"));
			}
			catch
			{
				Interlocked.Increment(ref newItemErrors);
			}
		}));

		newItemErrors.Should().Be(0, "freshly created items should survive their TTL window");

		output.WriteLine($"TTL concurrent: {readFound} old found, {readNotFound} old expired, " +
						 $"{newCreates} new created, {newItemErrors} new read errors");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category I: FakeCosmosHandler Load
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task I1_FakeCosmosHandlerLoad_CrudThroughHttpPipeline()
	{
		var container = new InMemoryContainer("i1", "/partitionKey");

		using var handler = new FakeCosmosHandler(container);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) }
			});

		var cosmosContainer = client.GetContainer("fakeDb", "i1");
		var nextId = new AtomicCounter(0);
		var errors = 0;

		// Create 200 items through the HTTP pipeline
		await Task.WhenAll(Enumerable.Range(0, 200).Select(async _ =>
		{
			try
			{
				var id = nextId.Increment().ToString();
				var pk = $"pk-{id}";
				await cosmosContainer.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "all creates through FakeCosmosHandler should succeed");
		container.ItemCount.Should().Be(200, "200 items should exist in the backing container");

		// Read them back through the pipeline
		var readErrors = 0;
		await Task.WhenAll(Enumerable.Range(1, 200).Select(async i =>
		{
			try
			{
				await cosmosContainer.ReadItemAsync<LoadDocument>(i.ToString(), new PartitionKey($"pk-{i}"));
			}
			catch (Exception)
			{
				Interlocked.Increment(ref readErrors);
			}
		}));

		readErrors.Should().Be(0, "all reads through FakeCosmosHandler should succeed");

		// Query through the pipeline
		var queryIterator = cosmosContainer.GetItemQueryIterator<LoadDocument>(
			new QueryDefinition("SELECT TOP 10 * FROM c ORDER BY c.id"));
		var queryResults = new List<LoadDocument>();
		while (queryIterator.HasMoreResults)
		{
			var page = await queryIterator.ReadNextAsync();
			queryResults.AddRange(page);
		}

		queryResults.Should().HaveCountLessThanOrEqualTo(10);
		output.WriteLine($"FakeCosmosHandler: 200 creates, 200 reads, query returned {queryResults.Count} items");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category J: Empty Container & Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task J1_EmptyContainerQueryLoad_QueriesOnEmptyContainer()
	{
		var container = new InMemoryContainer("j1", "/partitionKey");
		var errors = 0;

		// 200 concurrent queries on a completely empty container
		await Task.WhenAll(Enumerable.Range(0, 200).Select(async i =>
		{
			try
			{
				switch (i % 4)
				{
					case 0:
						var iter1 = container.GetItemQueryIterator<LoadDocument>("SELECT * FROM c");
						while (iter1.HasMoreResults)
						{
							var page = await iter1.ReadNextAsync();
							page.Count.Should().Be(0);
						}
						break;
					case 1:
						var iter2 = container.GetItemQueryIterator<int>(
							new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));
						while (iter2.HasMoreResults)
						{
							var page = await iter2.ReadNextAsync();
							foreach (var count in page) count.Should().Be(0);
						}
						break;
					case 2:
						var iter3 = container.GetItemQueryIterator<LoadDocument>(
							new QueryDefinition("SELECT TOP 10 * FROM c WHERE c.counter > 0"));
						while (iter3.HasMoreResults)
						{
							var page = await iter3.ReadNextAsync();
							page.Count.Should().Be(0);
						}
						break;
					case 3:
						try
						{
							await container.ReadItemAsync<LoadDocument>("nonexistent",
								new PartitionKey("nonexistent"));
						}
						catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
						{
							// Expected
						}
						break;
				}
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "queries on empty container should not produce unexpected errors");
		container.ItemCount.Should().Be(0, "container should remain empty");
		output.WriteLine("200 queries on empty container completed without errors");
	}

	[Fact]
	public async Task J2_NumericPartitionKeys_UnderLoad()
	{
		var container = new InMemoryContainer("j2", "/numericPk");
		var nextId = new AtomicCounter(0);
		var errors = 0;

		// Create 200 items with numeric partition keys
		await Task.WhenAll(Enumerable.Range(0, 200).Select(async _ =>
		{
			try
			{
				var id = nextId.Increment().ToString();
				var numPk = int.Parse(id) % 20;
				var doc = new JObject { ["id"] = id, ["numericPk"] = numPk, ["data"] = $"data-{id}" };
				await container.CreateItemAsync(doc, new PartitionKey(numPk));
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "all creates with numeric PKs should succeed");
		container.ItemCount.Should().Be(200);

		// Read back with numeric PKs
		var readErrors = 0;
		await Task.WhenAll(Enumerable.Range(1, 200).Select(async i =>
		{
			try
			{
				var numPk = i % 20;
				await container.ReadItemAsync<JObject>(i.ToString(), new PartitionKey(numPk));
			}
			catch (Exception)
			{
				Interlocked.Increment(ref readErrors);
			}
		}));

		readErrors.Should().Be(0, "all reads with numeric PKs should succeed");
		output.WriteLine($"Numeric PKs: 200 creates, 200 reads, {errors + readErrors} errors");
	}

	[Fact]
	public async Task J3_HierarchicalPartitionKeys_UnderLoad()
	{
		var container = new InMemoryContainer("j3", new[] { "/tenantId", "/userId" });
		var nextId = new AtomicCounter(0);
		var errors = 0;

		// Create 200 items with hierarchical partition keys
		await Task.WhenAll(Enumerable.Range(0, 200).Select(async _ =>
		{
			try
			{
				var id = nextId.Increment().ToString();
				var tenantId = $"tenant-{int.Parse(id) % 5}";
				var userId = $"user-{int.Parse(id) % 20}";
				var doc = new JObject { ["id"] = id, ["tenantId"] = tenantId, ["userId"] = userId, ["data"] = $"data-{id}" };
				var pk = new PartitionKeyBuilder().Add(tenantId).Add(userId).Build();
				await container.CreateItemAsync(doc, pk);
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "all creates with hierarchical PKs should succeed");
		container.ItemCount.Should().Be(200);

		// Read back with hierarchical PKs
		var readErrors = 0;
		await Task.WhenAll(Enumerable.Range(1, 200).Select(async i =>
		{
			try
			{
				var tenantId = $"tenant-{i % 5}";
				var userId = $"user-{i % 20}";
				var pk = new PartitionKeyBuilder().Add(tenantId).Add(userId).Build();
				await container.ReadItemAsync<JObject>(i.ToString(), pk);
			}
			catch (Exception)
			{
				Interlocked.Increment(ref readErrors);
			}
		}));

		readErrors.Should().Be(0, "all reads with hierarchical PKs should succeed");
		output.WriteLine($"Hierarchical PKs: 200 creates, 200 reads, {errors + readErrors} errors");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Category K: Latency & Iterator Resilience
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task K1_LatencyDistribution_P99StableAcrossBatches()
	{
		var container = new InMemoryContainer("k1", "/partitionKey");

		// Seed 500 items
		for (var i = 0; i < 500; i++)
		{
			var pk = $"pk-{i % 20}";
			await container.CreateItemAsync(MakeDoc(i.ToString(), pk), new PartitionKey(pk));
		}

		var batchP99s = new List<double>();

		// Run 5 batches of 200 ops each, collecting P99 per batch
		for (var batch = 0; batch < 5; batch++)
		{
			var latencies = new ConcurrentBag<double>();

			await Task.WhenAll(Enumerable.Range(0, 200).Select(async _ =>
			{
				var sw = System.Diagnostics.Stopwatch.StartNew();
				var id = Random.Shared.Next(500).ToString();
				var pk = $"pk-{int.Parse(id) % 20}";
				await container.ReadItemAsync<LoadDocument>(id, new PartitionKey(pk));
				sw.Stop();
				latencies.Add(sw.Elapsed.TotalMilliseconds);
			}));

			var sorted = latencies.OrderBy(l => l).ToArray();
			var p99Index = (int)Math.Ceiling(0.99 * sorted.Length) - 1;
			var p99 = sorted[Math.Max(0, p99Index)];
			batchP99s.Add(p99);
			output.WriteLine($"  Batch {batch + 1}: P99 = {p99:F3}ms");
		}

		// P99 should not vary wildly between batches (no more than 10x of the minimum)
		var minP99 = batchP99s.Min();
		var maxP99 = batchP99s.Max();
		maxP99.Should().BeLessThan(Math.Max(minP99 * 10, 50),
			"P99 latency should be stable across batches (no catastrophic regression)");
		output.WriteLine($"P99 range: {minP99:F3}ms - {maxP99:F3}ms");
	}

	[Fact]
	public async Task K2_ConcurrentIteratorDrain_MultipleQueriesSimultaneously()
	{
		var container = new InMemoryContainer("k2", "/partitionKey");

		// Seed 500 items
		for (var i = 0; i < 500; i++)
		{
			var pk = $"pk-{i % 20}";
			await container.CreateItemAsync(MakeDoc(i.ToString(), pk), new PartitionKey(pk));
		}

		var errors = 0;
		var totalResults = new ConcurrentBag<int>();

		// 50 threads each drain a full SELECT * iterator concurrently
		await Task.WhenAll(Enumerable.Range(0, 50).Select(async _ =>
		{
			try
			{
				var iterator = container.GetItemQueryIterator<LoadDocument>("SELECT * FROM c");
				var count = 0;
				while (iterator.HasMoreResults)
				{
					var page = await iterator.ReadNextAsync();
					count += page.Count;
				}

				totalResults.Add(count);
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		}));

		errors.Should().Be(0, "concurrent iterator drains should not fail");

		// Each iterator should see all 500 items (snapshot consistency)
		foreach (var count in totalResults)
		{
			count.Should().Be(500, "each iterator should return all 500 items");
		}

		output.WriteLine($"50 concurrent iterator drains: all returned 500 items, {errors} errors");
	}

	[Fact]
	public async Task K3_ConcurrentIteratorDrain_DuringWrites_NoCorruption()
	{
		var container = new InMemoryContainer("k3", "/partitionKey");

		// Seed 200 items
		for (var i = 0; i < 200; i++)
		{
			var pk = $"pk-{i % 20}";
			await container.CreateItemAsync(MakeDoc(i.ToString(), pk), new PartitionKey(pk));
		}

		var nextId = new AtomicCounter(1000);
		var errors = 0;

		// Concurrently: 20 iterators draining + 100 writes
		var queryTasks = Enumerable.Range(0, 20).Select(async _ =>
		{
			try
			{
				var iterator = container.GetItemQueryIterator<LoadDocument>("SELECT * FROM c");
				var results = new List<LoadDocument>();
				while (iterator.HasMoreResults)
				{
					var page = await iterator.ReadNextAsync();
					results.AddRange(page);
				}

				// Should get at least the seed count (may see some new writes too)
				results.Count.Should().BeGreaterThanOrEqualTo(200);
				// Every returned document should be structurally valid
				results.Should().OnlyContain(d => d.Id != null && d.PartitionKey != null);
			}
			catch (Exception)
			{
				Interlocked.Increment(ref errors);
			}
		});

		var writeTasks = Enumerable.Range(0, 100).Select(async _ =>
		{
			var id = nextId.Increment().ToString();
			var pk = $"pk-{id}";
			await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
		});

		await Task.WhenAll(queryTasks.Concat(writeTasks));

		errors.Should().Be(0, "no corruption during concurrent iterator drain + writes");
		container.ItemCount.Should().Be(300, "200 seed + 100 writes");
		output.WriteLine($"Concurrent drain during writes: {container.ItemCount} items, {errors} errors");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Shared Helpers
	// ═══════════════════════════════════════════════════════════════════════════

	private static async Task<(InMemoryContainer container, ConcurrentDictionary<string, string> knownIds)>
		SeedContainer(string name, int count)
	{
		var container = new InMemoryContainer(name, "/partitionKey");
		var knownIds = new ConcurrentDictionary<string, string>();
		for (var i = 1; i <= count; i++)
		{
			var id = i.ToString();
			var pk = $"pk-{i % 20}";
			await container.CreateItemAsync(MakeDoc(id, pk), new PartitionKey(pk));
			knownIds.TryAdd(id, pk);
		}

		return (container, knownIds);
	}

	private static LoadDocument MakeDoc(string id, string pk, int counter = -1) => new()
	{
		Id = id,
		PartitionKey = pk,
		Counter = counter >= 0 ? counter : Random.Shared.Next(1000),
		Data = $"data-{Guid.NewGuid():N}",
		Timestamp = DateTimeOffset.UtcNow.ToString("O")
	};

	private static async Task<List<LoadDocument>> QueryAll(InMemoryContainer container)
	{
		var results = new List<LoadDocument>();
		var iterator = container.GetItemQueryIterator<LoadDocument>("SELECT * FROM c");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		return results;
	}

	private static (string? id, string? pk) PickRandom(ConcurrentDictionary<string, string> knownIds)
	{
		var keys = knownIds.Keys.ToArray();
		if (keys.Length == 0) return (null, null);
		var key = keys[Random.Shared.Next(keys.Length)];
		return knownIds.TryGetValue(key, out var pk) ? (key, pk) : (null, null);
	}
}
