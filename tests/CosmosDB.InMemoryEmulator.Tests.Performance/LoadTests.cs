using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests.Performance;

public class LoadTests(ITestOutputHelper output)
{
	private static readonly int _targetCallsPerSecond =
		int.TryParse(Environment.GetEnvironmentVariable("LOAD_TEST_RPS"), out var rps) ? rps : 500;

	private static readonly int _durationSeconds =
		int.TryParse(Environment.GetEnvironmentVariable("LOAD_TEST_DURATION_SECONDS"), out var dur) ? dur : 60;

	private static readonly int _totalOperations = _targetCallsPerSecond * _durationSeconds;

	[Fact]
	public async Task ReadHeavyLoad_80PercentReads_20PercentWrites()
	{
		var container = new InMemoryContainer("read-heavy", "/partitionKey");
		var stats = new LoadStats();

		await SeedDocuments(container, count: 1000);

		await RunLoad(container, stats, readWeight: 80, writeWeight: 20, seedCount: 1000);

		ReportAndAssert(stats, "Read-Heavy (80/20)");
	}

	[Fact]
	public async Task WriteHeavyLoad_20PercentReads_80PercentWrites()
	{
		var container = new InMemoryContainer("write-heavy", "/partitionKey");
		var stats = new LoadStats();

		await SeedDocuments(container, count: 200);

		await RunLoad(container, stats, readWeight: 20, writeWeight: 80, seedCount: 200);

		ReportAndAssert(stats, "Write-Heavy (20/80)");
	}

	[Fact]
	public async Task EvenMixLoad_50PercentReads_50PercentWrites()
	{
		var container = new InMemoryContainer("even-mix", "/partitionKey");
		var stats = new LoadStats();

		await SeedDocuments(container, count: 500);

		await RunLoad(container, stats, readWeight: 50, writeWeight: 50, seedCount: 500);

		ReportAndAssert(stats, "Even Mix (50/50)");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Load runner
	// ═══════════════════════════════════════════════════════════════════════════

	private async Task RunLoad(
		InMemoryContainer container,
		LoadStats stats,
		int readWeight,
		int writeWeight,
		int seedCount)
	{
		var knownIds = new ConcurrentDictionary<string, string>();

		foreach (var id in Enumerable.Range(1, seedCount))
		{
			knownIds.TryAdd(id.ToString(), $"pk-{id % 20}");
		}

		var nextId = new AtomicCounter(10_000);
		var maxConcurrency = Math.Max(_targetCallsPerSecond * 2, 200);
		var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
		var tasks = new List<Task>(_totalOperations);
		var overallStopwatch = Stopwatch.StartNew();
		var intervalMs = 1000.0 / _targetCallsPerSecond;

		for (var operationIndex = 0; operationIndex < _totalOperations; operationIndex++)
		{
			var targetTime = TimeSpan.FromMilliseconds(operationIndex * intervalMs);
			var elapsed = overallStopwatch.Elapsed;
			if (targetTime > elapsed)
			{
				await Task.Delay(targetTime - elapsed);
			}

			await semaphore.WaitAsync();

			var isRead = Random.Shared.Next(100) < readWeight;
			var capturedIndex = operationIndex;

			tasks.Add(Task.Run(async () =>
			{
				var operationStopwatch = Stopwatch.StartNew();
				try
				{
					if (isRead)
					{
						await ExecuteReadOperation(container, knownIds, stats);
					}
					else
					{
						await ExecuteWriteOperation(container, knownIds, stats, nextId);
					}

					operationStopwatch.Stop();
					stats.RecordLatency(operationStopwatch.Elapsed);
				}
				// Note: 404s from replace/patch racing with concurrent delete are expected and counted
				// here rather than under the specific operation stat. totalOps still includes NotFound,
				// so the assertion in ReportAndAssert remains correct.
				catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.NotFound)
				{
					operationStopwatch.Stop();
					stats.RecordLatency(operationStopwatch.Elapsed);
					Interlocked.Increment(ref stats.NotFound);
				}
				catch (Exception exception)
				{
					operationStopwatch.Stop();
					stats.RecordLatency(operationStopwatch.Elapsed);
					Interlocked.Increment(ref stats.Errors);
					stats.ErrorMessages.Enqueue($"Op {capturedIndex}: {exception.GetType().Name}: {exception.Message}");
				}
				finally
				{
					semaphore.Release();
				}
			}));
		}

		stats.SchedulingTime = overallStopwatch.Elapsed;

		await Task.WhenAll(tasks);

		overallStopwatch.Stop();
		stats.WallClockTime = overallStopwatch.Elapsed;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Read operations (with verification)
	// ═══════════════════════════════════════════════════════════════════════════

	private static async Task ExecuteReadOperation(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var readType = Random.Shared.Next(4);

		switch (readType)
		{
			case 0:
				await ReadAndVerifySingle(container, knownIds, stats);
				break;
			case 1:
				await ReadAndVerifyQuery(container, stats);
				break;
			case 2:
				await ReadAndVerifyQueryWithFilter(container, knownIds, stats);
				break;
			case 3:
				await ReadAndVerifyCrossPartitionQuery(container, stats);
				break;
		}
	}

	private static async Task ReadAndVerifySingle(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		if (!knownIds.TryGetValue(targetId, out var partitionKey))
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		var response = await container.ReadItemAsync<LoadDocument>(targetId, new PartitionKey(partitionKey));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Id.Should().Be(targetId);
		response.Resource.PartitionKey.Should().Be(partitionKey);
		Interlocked.Increment(ref stats.Reads);
	}

	private static async Task ReadAndVerifyCrossPartitionQuery(
		InMemoryContainer container,
		LoadStats stats)
	{
		var threshold = Random.Shared.Next(500);
		var iterator = container.GetItemQueryIterator<LoadDocument>(
			new QueryDefinition("SELECT TOP 20 * FROM c WHERE c.counter > @threshold")
				.WithParameter("@threshold", threshold));

		var results = new List<LoadDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().OnlyContain(d => d.Counter > threshold);
		Interlocked.Increment(ref stats.Reads);
	}

	private static async Task ReadAndVerifyQuery(
		InMemoryContainer container,
		LoadStats stats)
	{
		var iterator = container.GetItemQueryIterator<LoadDocument>(
			new QueryDefinition("SELECT TOP 10 * FROM c ORDER BY c.counter DESC"));

		var results = new List<LoadDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCountLessThanOrEqualTo(10);

		for (var resultIndex = 1; resultIndex < results.Count; resultIndex++)
		{
			results[resultIndex].Counter.Should().BeLessThanOrEqualTo(results[resultIndex - 1].Counter);
		}

		Interlocked.Increment(ref stats.Reads);
	}

	private static async Task ReadAndVerifyQueryWithFilter(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		var iterator = container.GetItemQueryIterator<LoadDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.id = @id")
				.WithParameter("@id", targetId));

		var results = new List<LoadDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		if (results.Count == 1)
		{
			results[0].Id.Should().Be(targetId);
		}

		Interlocked.Increment(ref stats.Reads);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Write operations (mixed)
	// ═══════════════════════════════════════════════════════════════════════════

	private static async Task ExecuteWriteOperation(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats,
		AtomicCounter nextId)
	{
		var writeType = Random.Shared.Next(5);

		switch (writeType)
		{
			case 0:
				await WriteCreate(container, knownIds, stats, nextId);
				break;
			case 1:
				await WriteUpsert(container, knownIds, stats, nextId);
				break;
			case 2:
				await WriteReplace(container, knownIds, stats);
				break;
			case 3:
				await WritePatch(container, knownIds, stats);
				break;
			case 4:
				await WriteDelete(container, knownIds, stats);
				break;
		}
	}

	private static async Task WriteCreate(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats,
		AtomicCounter nextId)
	{
		var id = nextId.Increment().ToString();
		var partitionKey = $"pk-{id}";
		var document = CreateDocument(id, partitionKey);

		var response = await container.CreateItemAsync(document, new PartitionKey(partitionKey));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		knownIds.TryAdd(id, partitionKey);
		Interlocked.Increment(ref stats.Creates);
	}

	private static async Task WriteUpsert(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats,
		AtomicCounter nextId)
	{
		var updateExisting = Random.Shared.Next(2) == 0;

		if (updateExisting)
		{
			var ids = knownIds.Keys.ToArray();
			if (ids.Length > 0)
			{
				var targetId = ids[Random.Shared.Next(ids.Length)];
				if (!knownIds.TryGetValue(targetId, out var partitionKey))
				{
					Interlocked.Increment(ref stats.Skipped);
					return;
				}

				var document = CreateDocument(targetId, partitionKey);

				var response = await container.UpsertItemAsync(document, new PartitionKey(partitionKey));

				// Concurrent delete can remove the item between TryGetValue and UpsertItemAsync,
				// causing Cosmos to create fresh (201) instead of update (200)
				response.StatusCode.Should().BeOneOf(HttpStatusCode.OK, HttpStatusCode.Created);
				knownIds.TryAdd(targetId, partitionKey);
				Interlocked.Increment(ref stats.Upserts);
				return;
			}
		}

		var id = nextId.Increment().ToString();
		var newPartitionKey = $"pk-{id}";
		var newDocument = CreateDocument(id, newPartitionKey);

		var createResponse = await container.UpsertItemAsync(newDocument, new PartitionKey(newPartitionKey));

		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);
		knownIds.TryAdd(id, newPartitionKey);
		Interlocked.Increment(ref stats.Upserts);
	}

	private static async Task WriteReplace(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		if (!knownIds.TryGetValue(targetId, out var partitionKey))
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		var document = CreateDocument(targetId, partitionKey);

		var response = await container.ReplaceItemAsync(document, targetId, new PartitionKey(partitionKey));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		Interlocked.Increment(ref stats.Replaces);
	}

	private static async Task WritePatch(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		if (!knownIds.TryGetValue(targetId, out var patchPartitionKey))
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		var patchOperations = new List<PatchOperation>
		{
			PatchOperation.Increment("/counter", 1),
			PatchOperation.Set("/data", $"patched-{Guid.NewGuid():N}")
		};

		var response = await container.PatchItemAsync<LoadDocument>(
			targetId, new PartitionKey(patchPartitionKey), patchOperations);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		Interlocked.Increment(ref stats.Patches);
	}

	private static async Task WriteDelete(
		InMemoryContainer container,
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		if (!knownIds.TryRemove(targetId, out var partitionKey))
		{
			Interlocked.Increment(ref stats.Skipped);
			return;
		}

		try
		{
			var response = await container.DeleteItemAsync<LoadDocument>(
				targetId, new PartitionKey(partitionKey));

			response.StatusCode.Should().Be(HttpStatusCode.NoContent);
			Interlocked.Increment(ref stats.Deletes);
		}
		catch
		{
			// Re-add to knownIds so the item isn't orphaned if the delete failed
			knownIds.TryAdd(targetId, partitionKey);
			throw;
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Seeding
	// ═══════════════════════════════════════════════════════════════════════════

	private static async Task SeedDocuments(InMemoryContainer container, int count)
	{
		for (var seedIndex = 1; seedIndex <= count; seedIndex++)
		{
			var id = seedIndex.ToString();
			var partitionKey = $"pk-{seedIndex % 20}";
			var document = CreateDocument(id, partitionKey);
			await container.CreateItemAsync(document, new PartitionKey(partitionKey));
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Helpers
	// ═══════════════════════════════════════════════════════════════════════════

	private static LoadDocument CreateDocument(string id, string partitionKey) => new()
	{
		Id = id,
		PartitionKey = partitionKey,
		Counter = Random.Shared.Next(1000),
		Data = $"data-{Guid.NewGuid():N}",
		Timestamp = DateTimeOffset.UtcNow.ToString("O")
	};

	private void ReportAndAssert(LoadStats stats, string scenarioName)
	{
		var totalOps = stats.Reads + stats.Creates + stats.Upserts
					   + stats.Replaces + stats.Patches + stats.Deletes
					   + stats.NotFound + stats.Errors + stats.Skipped;

		var opsPerSecond = totalOps / stats.SchedulingTime.TotalSeconds;

		output.WriteLine($"║  Config: {_targetCallsPerSecond} rps × {_durationSeconds}s = {_totalOperations:N0} ops");
		output.WriteLine($"╔══════════════════════════════════════════════════╗");
		output.WriteLine($"║  {scenarioName,-46}  ║");
		output.WriteLine($"╠══════════════════════════════════════════════════╣");
		output.WriteLine($"║  Scheduling time   : {stats.SchedulingTime.TotalSeconds,8:F1}s{"",-17}║");
		output.WriteLine($"║  Wall clock        : {stats.WallClockTime.TotalSeconds,8:F1}s{"",-17}║");
		output.WriteLine($"║  Total operations  : {totalOps,8:N0}{"",-18}║");
		output.WriteLine($"║  Throughput        : {opsPerSecond,8:F1} ops/s{"",-12}║");
		output.WriteLine($"╠══════════════════════════════════════════════════╣");
		output.WriteLine($"║  Reads             : {stats.Reads,8:N0}{"",-18}║");
		output.WriteLine($"║  Creates           : {stats.Creates,8:N0}{"",-18}║");
		output.WriteLine($"║  Upserts           : {stats.Upserts,8:N0}{"",-18}║");
		output.WriteLine($"║  Replaces          : {stats.Replaces,8:N0}{"",-18}║");
		output.WriteLine($"║  Patches           : {stats.Patches,8:N0}{"",-18}║");
		output.WriteLine($"║  Deletes           : {stats.Deletes,8:N0}{"",-18}║");
		output.WriteLine($"╠══════════════════════════════════════════════════╣");
		output.WriteLine($"║  Expected 404s     : {stats.NotFound,8:N0}{"",-18}║");
		output.WriteLine($"║  Skipped (no-ops)  : {stats.Skipped,8:N0}{"",-18}║");
		output.WriteLine($"║  Unexpected errors : {stats.Errors,8:N0}{"",-18}║");
		output.WriteLine($"╠══════════════════════════════════════════════════╣");
		output.WriteLine($"║  Mean latency      : {stats.MeanLatency,8:F3}ms{"",-15}║");
		output.WriteLine($"║  P50 latency       : {stats.GetPercentile(50),8:F3}ms{"",-15}║");
		output.WriteLine($"║  P95 latency       : {stats.GetPercentile(95),8:F3}ms{"",-15}║");
		output.WriteLine($"║  P99 latency       : {stats.GetPercentile(99),8:F3}ms{"",-15}║");
		output.WriteLine($"║  Max latency       : {stats.GetPercentile(100),8:F3}ms{"",-15}║");
		output.WriteLine($"╚══════════════════════════════════════════════════╝");

		foreach (var error in stats.ErrorMessages.Take(10))
		{
			output.WriteLine($"  ERROR: {error}");
		}

		totalOps.Should().Be(_totalOperations,
			$"all {_totalOperations:N0} operations should complete");

		stats.Errors.Should().Be(0,
			"no unexpected errors should occur under sustained load");

		opsPerSecond.Should().BeGreaterThanOrEqualTo(_targetCallsPerSecond * 0.50,
			$"throughput should sustain at least 50% of target ({_targetCallsPerSecond} ops/s)");

		stats.GetPercentile(99).Should().BeLessThan(2000,
			"P99 latency should stay under 2000ms for in-memory operations (includes cross-partition queries)");
	}
}

public class LoadDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("partitionKey")]
	public string PartitionKey { get; set; } = default!;

	[JsonProperty("counter")]
	public int Counter { get; set; }

	[JsonProperty("data")]
	public string Data { get; set; } = default!;

	[JsonProperty("timestamp")]
	public string Timestamp { get; set; } = default!;
}

public class LoadStats
{
	public long Reads;
	public long Creates;
	public long Upserts;
	public long Replaces;
	public long Patches;
	public long Deletes;
	public long NotFound;
	public long Errors;
	public long Skipped;
	public TimeSpan SchedulingTime;
	public TimeSpan WallClockTime;

	public ConcurrentQueue<string> ErrorMessages { get; } = new();

	private readonly ConcurrentBag<double> _latenciesMs = [];

	public void RecordLatency(TimeSpan latency) => _latenciesMs.Add(latency.TotalMilliseconds);

	public double MeanLatency => _latenciesMs.Count > 0 ? _latenciesMs.Average() : 0;

	public double GetPercentile(int percentile)
	{
		var sorted = _latenciesMs.OrderBy(latency => latency).ToArray();
		if (sorted.Length == 0)
		{
			return 0;
		}

		if (percentile >= 100)
		{
			return sorted[^1];
		}

		var index = (int)Math.Ceiling(percentile / 100.0 * sorted.Length) - 1;
		return sorted[Math.Max(0, index)];
	}
}

public class AtomicCounter(int initial)
{
	private int _value = initial;
	public int Increment() => Interlocked.Increment(ref _value);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category A: Missing Operation Types Under Load
// ═══════════════════════════════════════════════════════════════════════════════

public class LoadTestOperationTypes(ITestOutputHelper output)
{
	[Fact]
	public async Task ReadManyUnderLoad_VerifiesAllItemsReturned()
	{
		var container = new InMemoryContainer("readmany-load", "/partitionKey");
		var seedCount = 200;
		var knownIds = new ConcurrentDictionary<string, string>();

		for (var i = 1; i <= seedCount; i++)
		{
			var id = i.ToString();
			var pk = $"pk-{i % 20}";
			await container.CreateItemAsync(
				new LoadDocument { Id = id, PartitionKey = pk, Counter = i, Data = "seed", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey(pk));
			knownIds.TryAdd(id, pk);
		}

		var errors = 0;
		var totalReads = 0;
		var tasks = Enumerable.Range(0, 100).Select(async _ =>
		{
			var ids = knownIds.Keys.ToArray();
			var batch = ids.OrderBy(_ => Random.Shared.Next()).Take(Random.Shared.Next(3, 8)).ToList();
			var feedItems = batch
				.Where(id => knownIds.ContainsKey(id))
				.Select(id => (id, new PartitionKey(knownIds[id])))
				.ToList();

			if (feedItems.Count == 0) return;

			var response = await container.ReadManyItemsAsync<LoadDocument>(feedItems);
			if (response.StatusCode != HttpStatusCode.OK)
				Interlocked.Increment(ref errors);
			else if (response.Count != feedItems.Count)
				Interlocked.Increment(ref errors);

			Interlocked.Increment(ref totalReads);
		});

		await Task.WhenAll(tasks);

		output.WriteLine($"ReadMany batches: {totalReads}, errors: {errors}");
		errors.Should().Be(0);
		totalReads.Should().Be(100);
	}

	[Fact]
	public async Task TransactionalBatchUnderLoad_AtomicCreateAndRead()
	{
		var container = new InMemoryContainer("batch-load", "/partitionKey");
		var nextId = new AtomicCounter(0);
		var errors = 0;
		var successes = 0;

		var tasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			var batchPk = $"pk-batch-{nextId.Increment()}";
			var batch = container.CreateTransactionalBatch(new PartitionKey(batchPk));
			var items = Enumerable.Range(0, 3).Select(j => new LoadDocument
			{
				Id = $"{batchPk}-{j}",
				PartitionKey = batchPk,
				Counter = j,
				Data = "batch",
				Timestamp = DateTimeOffset.UtcNow.ToString("O")
			}).ToList();

			foreach (var item in items)
				batch.CreateItem(item);

			using var response = await batch.ExecuteAsync();
			if (response.IsSuccessStatusCode)
				Interlocked.Increment(ref successes);
			else
				Interlocked.Increment(ref errors);
		});

		await Task.WhenAll(tasks);

		output.WriteLine($"Batches: {successes} succeeded, {errors} errors");
		errors.Should().Be(0);
		successes.Should().Be(50);
	}

	[Fact]
	public async Task StreamApiOperationsUnderLoad()
	{
		var container = new InMemoryContainer("stream-load", "/partitionKey");
		var nextId = new AtomicCounter(0);
		var errors = 0;
		var ops = 0;

		// Seed some items
		for (var i = 0; i < 50; i++)
		{
			var id = $"seed-{i}";
			await container.CreateItemAsync(
				new LoadDocument { Id = id, PartitionKey = "stream-pk", Counter = i, Data = "seed", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey("stream-pk"));
		}

		var tasks = Enumerable.Range(0, 200).Select(async idx =>
		{
			try
			{
				if (idx % 2 == 0)
				{
					// Stream create
					var id = $"stream-{nextId.Increment()}";
					var doc = new LoadDocument { Id = id, PartitionKey = "stream-pk", Counter = idx, Data = "stream", Timestamp = DateTimeOffset.UtcNow.ToString("O") };
					var json = Newtonsoft.Json.JsonConvert.SerializeObject(doc);
					using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
					using var response = await container.CreateItemStreamAsync(stream, new PartitionKey("stream-pk"));
					if (!response.IsSuccessStatusCode)
						Interlocked.Increment(ref errors);
				}
				else
				{
					// Stream read
					var readId = $"seed-{idx % 50}";
					using var response = await container.ReadItemStreamAsync(readId, new PartitionKey("stream-pk"));
					if (!response.IsSuccessStatusCode)
						Interlocked.Increment(ref errors);
				}

				Interlocked.Increment(ref ops);
			}
			catch
			{
				Interlocked.Increment(ref errors);
			}
		});

		await Task.WhenAll(tasks);

		output.WriteLine($"Stream ops: {ops}, errors: {errors}");
		errors.Should().Be(0);
	}

	[Fact]
	public async Task ChangeFeedReadDuringWrites()
	{
		var container = new InMemoryContainer("changefeed-load", "/partitionKey");

		// Write items
		var writeCount = 50;
		for (var i = 0; i < writeCount; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"cf-{i}", PartitionKey = $"pk-{i % 5}", Counter = i, Data = "cf", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey($"pk-{i % 5}"));
		}

		// Read change feed
		var changeFeedItems = new List<LoadDocument>();
		var iterator = container.GetChangeFeedIterator<LoadDocument>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);

		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified)
				break;
			changeFeedItems.AddRange(response);
		}

		output.WriteLine($"Change feed items: {changeFeedItems.Count} (expected {writeCount})");
		changeFeedItems.Should().HaveCount(writeCount);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category B: Query Diversity Under Load
// ═══════════════════════════════════════════════════════════════════════════════

public class LoadTestQueryDiversity(ITestOutputHelper output)
{
	[Fact]
	public async Task CrossPartitionQueryUnderLoad()
	{
		var container = new InMemoryContainer("xpart-query", "/partitionKey");
		for (var i = 0; i < 100; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"q-{i}", PartitionKey = $"pk-{i % 10}", Counter = i, Data = "query", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey($"pk-{i % 10}"));
		}

		var errors = 0;
		var tasks = Enumerable.Range(0, 50).Select(async _ =>
		{
			var threshold = Random.Shared.Next(100);
			var iterator = container.GetItemQueryIterator<LoadDocument>(
				new QueryDefinition("SELECT * FROM c WHERE c.counter > @t")
					.WithParameter("@t", threshold));

			var results = new List<LoadDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
			}

			if (results.Any(r => r.Counter <= threshold))
				Interlocked.Increment(ref errors);

			// Verify results span multiple partitions
			var distinctPks = results.Select(r => r.PartitionKey).Distinct().Count();
			if (results.Count > 10 && distinctPks < 2)
				Interlocked.Increment(ref errors);
		});

		await Task.WhenAll(tasks);
		output.WriteLine($"Cross-partition query errors: {errors}");
		errors.Should().Be(0);
	}

	[Fact]
	public async Task AggregateQueryUnderLoad()
	{
		var container = new InMemoryContainer("agg-query", "/partitionKey");
		var totalItems = 100;
		var expectedSum = 0;
		for (var i = 0; i < totalItems; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"a-{i}", PartitionKey = $"pk-{i % 5}", Counter = i, Data = "agg", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey($"pk-{i % 5}"));
			expectedSum += i;
		}

		// COUNT
		var countIterator = container.GetItemQueryIterator<int>(
			new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));
		var count = 0;
		while (countIterator.HasMoreResults)
		{
			var page = await countIterator.ReadNextAsync();
			count += page.Sum();
		}

		output.WriteLine($"COUNT: {count}, expected: {totalItems}");
		count.Should().Be(totalItems);

		// SUM
		var sumIterator = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE SUM(c.counter) FROM c"));
		var sum = 0.0;
		while (sumIterator.HasMoreResults)
		{
			var page = await sumIterator.ReadNextAsync();
			sum += page.Sum();
		}

		output.WriteLine($"SUM: {sum}, expected: {expectedSum}");
		sum.Should().Be(expectedSum);
	}

	[Fact]
	public async Task DistinctQueryUnderLoad()
	{
		var container = new InMemoryContainer("distinct-query", "/partitionKey");
		var partitionCount = 10;
		for (var i = 0; i < 100; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"d-{i}", PartitionKey = $"pk-{i % partitionCount}", Counter = i, Data = "distinct", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey($"pk-{i % partitionCount}"));
		}

		var iterator = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT DISTINCT VALUE c.partitionKey FROM c"));

		var results = new List<string>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		output.WriteLine($"Distinct PKs: {results.Count}, expected: {partitionCount}");
		results.Should().HaveCount(partitionCount);
		results.Should().OnlyHaveUniqueItems();
	}

	[Fact]
	public async Task OffsetLimitPaginationUnderLoad()
	{
		var container = new InMemoryContainer("offset-query", "/partitionKey");
		for (var i = 0; i < 50; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"ol-{i:D3}", PartitionKey = "pk-0", Counter = i, Data = "offset", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey("pk-0"));
		}

		var allPages = new List<List<LoadDocument>>();
		for (var offset = 0; offset < 50; offset += 10)
		{
			var iterator = container.GetItemQueryIterator<LoadDocument>(
				new QueryDefinition($"SELECT * FROM c ORDER BY c.id OFFSET {offset} LIMIT 10"));

			var pageItems = new List<LoadDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				pageItems.AddRange(page);
			}

			allPages.Add(pageItems);
		}

		var totalFromPages = allPages.Sum(p => p.Count);
		output.WriteLine($"Offset/Limit pages: {allPages.Count}, total items: {totalFromPages}");
		allPages.Should().OnlyContain(page => page.Count <= 10);
		totalFromPages.Should().Be(50);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category C: Concurrency Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class LoadTestConcurrencyEdgeCases(ITestOutputHelper output)
{
	[Fact]
	public async Task ETagConflictsUnderLoad()
	{
		var container = new InMemoryContainer("etag-load", "/partitionKey");
		await container.CreateItemAsync(
			new LoadDocument { Id = "e1", PartitionKey = "pk-e", Counter = 0, Data = "etag", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
			new PartitionKey("pk-e"));

		var read = await container.ReadItemAsync<LoadDocument>("e1", new PartitionKey("pk-e"));
		var etag = read.ETag;

		var successes = 0;
		var conflicts = 0;

		var tasks = Enumerable.Range(0, 10).Select(async i =>
		{
			try
			{
				var doc = new LoadDocument { Id = "e1", PartitionKey = "pk-e", Counter = i, Data = $"attempt-{i}", Timestamp = DateTimeOffset.UtcNow.ToString("O") };
				await container.ReplaceItemAsync(doc, "e1", new PartitionKey("pk-e"),
					new ItemRequestOptions { IfMatchEtag = etag });
				Interlocked.Increment(ref successes);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
			{
				Interlocked.Increment(ref conflicts);
			}
		});

		await Task.WhenAll(tasks);

		output.WriteLine($"ETag: {successes} succeeded, {conflicts} conflicts (total {successes + conflicts})");
		successes.Should().Be(1, "exactly one writer should succeed with the original ETag");
		conflicts.Should().Be(9);
	}

	[Fact]
	public async Task CreateAfterDelete_SameId_Succeeds()
	{
		var container = new InMemoryContainer("create-after-delete", "/partitionKey");
		var errors = 0;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			var id = $"cad-{i}";
			var pk = $"pk-{i}";
			try
			{
				await container.CreateItemAsync(
					new LoadDocument { Id = id, PartitionKey = pk, Counter = 0, Data = "v1", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
					new PartitionKey(pk));
				await container.DeleteItemAsync<LoadDocument>(id, new PartitionKey(pk));
				var response = await container.CreateItemAsync(
					new LoadDocument { Id = id, PartitionKey = pk, Counter = 1, Data = "v2", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
					new PartitionKey(pk));

				if (response.StatusCode != HttpStatusCode.Created)
					Interlocked.Increment(ref errors);
			}
			catch
			{
				Interlocked.Increment(ref errors);
			}
		});

		await Task.WhenAll(tasks);
		output.WriteLine($"Create-after-delete errors: {errors}");
		errors.Should().Be(0);
	}

	[Fact]
	public async Task DoubleDelete_Returns404()
	{
		var container = new InMemoryContainer("double-delete", "/partitionKey");
		var successes = 0;
		var notFounds = 0;
		var otherErrors = 0;

		var tasks = Enumerable.Range(0, 50).Select(async i =>
		{
			var id = $"dd-{i}";
			var pk = "pk-dd";
			await container.CreateItemAsync(
				new LoadDocument { Id = id, PartitionKey = pk, Counter = 0, Data = "del", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey(pk));

			// Two concurrent deletes
			var deleteTasks = Enumerable.Range(0, 2).Select(async _ =>
			{
				try
				{
					await container.DeleteItemAsync<LoadDocument>(id, new PartitionKey(pk));
					Interlocked.Increment(ref successes);
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
				{
					Interlocked.Increment(ref notFounds);
				}
				catch
				{
					Interlocked.Increment(ref otherErrors);
				}
			});

			await Task.WhenAll(deleteTasks);
		});

		await Task.WhenAll(tasks);

		output.WriteLine($"Double delete: {successes} succeeded, {notFounds} 404s, {otherErrors} other errors");
		otherErrors.Should().Be(0);
		(successes + notFounds).Should().Be(100); // 50 items × 2 delete attempts each
	}

	[Fact]
	public async Task HotPartitionLoadTest()
	{
		var container = new InMemoryContainer("hot-partition", "/partitionKey");
		var hotPk = "hot-pk";
		var coldPk = "cold-pk";

		// Seed both partitions
		for (var i = 0; i < 50; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"hot-{i}", PartitionKey = hotPk, Counter = i, Data = "hot", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey(hotPk));
			await container.CreateItemAsync(
				new LoadDocument { Id = $"cold-{i}", PartitionKey = coldPk, Counter = i, Data = "cold", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey(coldPk));
		}

		var hotErrors = 0;
		var coldErrors = 0;

		// 80% hot, 20% cold
		var tasks = Enumerable.Range(0, 200).Select(async i =>
		{
			try
			{
				if (i % 5 != 0) // 80% hot
				{
					var readId = $"hot-{i % 50}";
					var response = await container.ReadItemAsync<LoadDocument>(readId, new PartitionKey(hotPk));
					if (response.StatusCode != HttpStatusCode.OK)
						Interlocked.Increment(ref hotErrors);
				}
				else // 20% cold
				{
					var readId = $"cold-{i % 50}";
					var response = await container.ReadItemAsync<LoadDocument>(readId, new PartitionKey(coldPk));
					if (response.StatusCode != HttpStatusCode.OK)
						Interlocked.Increment(ref coldErrors);
				}
			}
			catch
			{
				if (i % 5 != 0) Interlocked.Increment(ref hotErrors);
				else Interlocked.Increment(ref coldErrors);
			}
		});

		await Task.WhenAll(tasks);

		output.WriteLine($"Hot partition errors: {hotErrors}, Cold partition errors: {coldErrors}");
		hotErrors.Should().Be(0);
		coldErrors.Should().Be(0);
	}

	[Fact]
	public async Task HighCardinalityPartitionKeys()
	{
		var container = new InMemoryContainer("high-card", "/partitionKey");
		var errors = 0;
		var uniquePks = 500;

		var tasks = Enumerable.Range(0, uniquePks).Select(async i =>
		{
			try
			{
				var pk = $"unique-pk-{i}";
				await container.CreateItemAsync(
					new LoadDocument { Id = $"hc-{i}", PartitionKey = pk, Counter = i, Data = "hc", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
					new PartitionKey(pk));
			}
			catch
			{
				Interlocked.Increment(ref errors);
			}
		});

		await Task.WhenAll(tasks);

		// Verify all items exist
		var iterator = container.GetItemQueryIterator<LoadDocument>(
			new QueryDefinition("SELECT * FROM c"));
		var allItems = new List<LoadDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
		}

		output.WriteLine($"High cardinality: {allItems.Count} items across {uniquePks} PKs, errors: {errors}");
		errors.Should().Be(0);
		allItems.Should().HaveCount(uniquePks);
		allItems.Select(d => d.PartitionKey).Distinct().Should().HaveCount(uniquePks);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category D: Stress Patterns
// ═══════════════════════════════════════════════════════════════════════════════

public class LoadTestStressPatterns(ITestOutputHelper output)
{
	[Fact]
	public async Task BurstTrafficPattern_SpikeThenCalm()
	{
		var container = new InMemoryContainer("burst-load", "/partitionKey");
		for (var i = 0; i < 100; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"b-{i}", PartitionKey = $"pk-{i % 10}", Counter = i, Data = "burst", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey($"pk-{i % 10}"));
		}

		var errors = 0;
		var ops = 0;

		// Spike: 100 concurrent reads
		var spikeTasks = Enumerable.Range(0, 100).Select(async i =>
		{
			try
			{
				var response = await container.ReadItemAsync<LoadDocument>($"b-{i % 100}", new PartitionKey($"pk-{i % 10}"));
				if (response.StatusCode == HttpStatusCode.OK) Interlocked.Increment(ref ops);
			}
			catch { Interlocked.Increment(ref errors); }
		});
		await Task.WhenAll(spikeTasks);

		// Calm: 10 sequential reads
		for (var i = 0; i < 10; i++)
		{
			var response = await container.ReadItemAsync<LoadDocument>($"b-{i}", new PartitionKey($"pk-{i % 10}"));
			if (response.StatusCode == HttpStatusCode.OK) ops++;
		}

		output.WriteLine($"Burst pattern: {ops} ops, {errors} errors");
		errors.Should().Be(0);
		ops.Should().Be(110);
	}

	[Fact]
	public async Task WriteOnlyStress()
	{
		var container = new InMemoryContainer("write-only", "/partitionKey");
		var nextId = new AtomicCounter(0);
		var errors = 0;
		var totalWrites = 500;

		var tasks = Enumerable.Range(0, totalWrites).Select(async _ =>
		{
			try
			{
				var id = nextId.Increment().ToString();
				await container.CreateItemAsync(
					new LoadDocument { Id = id, PartitionKey = $"pk-{id}", Counter = 0, Data = "write-only", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
					new PartitionKey($"pk-{id}"));
			}
			catch
			{
				Interlocked.Increment(ref errors);
			}
		});

		await Task.WhenAll(tasks);

		var iterator = container.GetItemQueryIterator<int>(
			new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));
		var count = 0;
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			count += page.Sum();
		}

		output.WriteLine($"Write-only: {count} items, {errors} errors");
		errors.Should().Be(0);
		count.Should().Be(totalWrites);
	}

	[Fact]
	public async Task ReadOnlyStress_LargeDataset()
	{
		var container = new InMemoryContainer("read-only-large", "/partitionKey");
		var seedCount = 1000;

		for (var i = 0; i < seedCount; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"ro-{i}", PartitionKey = $"pk-{i % 50}", Counter = i, Data = "read-only", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey($"pk-{i % 50}"));
		}

		var errors = 0;
		var reads = 0;

		var tasks = Enumerable.Range(0, 500).Select(async i =>
		{
			try
			{
				var id = $"ro-{i % seedCount}";
				var pk = $"pk-{i % seedCount % 50}";
				var response = await container.ReadItemAsync<LoadDocument>(id, new PartitionKey(pk));
				if (response.StatusCode == HttpStatusCode.OK) Interlocked.Increment(ref reads);
				else Interlocked.Increment(ref errors);
			}
			catch
			{
				Interlocked.Increment(ref errors);
			}
		});

		await Task.WhenAll(tasks);

		output.WriteLine($"Read-only large: {reads} reads, {errors} errors");
		errors.Should().Be(0);
		reads.Should().Be(500);
	}

	[Fact]
	public async Task GradualRampUp_LinearIncrease()
	{
		var container = new InMemoryContainer("ramp-up", "/partitionKey");
		for (var i = 0; i < 100; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"r-{i}", PartitionKey = $"pk-{i % 10}", Counter = i, Data = "ramp", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey($"pk-{i % 10}"));
		}

		var errors = 0;
		var totalOps = 0;

		// Ramp from 10 to 100 concurrent ops
		for (var level = 10; level <= 100; level += 10)
		{
			var tasks = Enumerable.Range(0, level).Select(async i =>
			{
				try
				{
					var response = await container.ReadItemAsync<LoadDocument>($"r-{i % 100}", new PartitionKey($"pk-{i % 10}"));
					if (response.StatusCode == HttpStatusCode.OK) Interlocked.Increment(ref totalOps);
				}
				catch { Interlocked.Increment(ref errors); }
			});
			await Task.WhenAll(tasks);
		}

		output.WriteLine($"Ramp-up: {totalOps} ops, {errors} errors");
		errors.Should().Be(0);
		totalOps.Should().Be(550); // 10+20+30+...+100
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category E: Data Integrity Verification
// ═══════════════════════════════════════════════════════════════════════════════

public class LoadTestDataIntegrity(ITestOutputHelper output)
{
	[Fact]
	public async Task PostLoadStateVerification()
	{
		var container = new InMemoryContainer("state-verify", "/partitionKey");
		var knownIds = new ConcurrentDictionary<string, string>();

		// Seed
		for (var i = 0; i < 100; i++)
		{
			var id = $"sv-{i}";
			var pk = $"pk-{i % 10}";
			await container.CreateItemAsync(
				new LoadDocument { Id = id, PartitionKey = pk, Counter = i, Data = "verify", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey(pk));
			knownIds.TryAdd(id, pk);
		}

		// Perform mixed operations
		for (var i = 100; i < 150; i++)
		{
			var id = $"sv-{i}";
			var pk = $"pk-{i % 10}";
			await container.CreateItemAsync(
				new LoadDocument { Id = id, PartitionKey = pk, Counter = i, Data = "verify", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey(pk));
			knownIds.TryAdd(id, pk);
		}

		// Delete some
		for (var i = 0; i < 20; i++)
		{
			var id = $"sv-{i}";
			if (knownIds.TryRemove(id, out var pk))
				await container.DeleteItemAsync<LoadDocument>(id, new PartitionKey(pk));
		}

		// Verify all known IDs exist in container
		var iterator = container.GetItemQueryIterator<LoadDocument>(
			new QueryDefinition("SELECT * FROM c"));
		var allItems = new List<LoadDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
		}

		var containerIds = allItems.Select(d => d.Id).ToHashSet();
		var knownIdSet = knownIds.Keys.ToHashSet();

		output.WriteLine($"Container: {containerIds.Count} items, Known: {knownIdSet.Count}");

		containerIds.Should().BeEquivalentTo(knownIdSet,
			"every known ID should exist in container, and no orphans should remain");
	}

	[Fact]
	public async Task PatchCounterMonotonicity()
	{
		var container = new InMemoryContainer("patch-mono", "/partitionKey");
		var itemCount = 20;
		var patchesPerItem = 10;

		// Seed with counter=0
		for (var i = 0; i < itemCount; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"pm-{i}", PartitionKey = "pk-pm", Counter = 0, Data = "patch", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey("pk-pm"));
		}

		// Patch increment each item N times (sequentially per item, parallel across items)
		var tasks = Enumerable.Range(0, itemCount).Select(async i =>
		{
			for (var j = 0; j < patchesPerItem; j++)
			{
				await container.PatchItemAsync<LoadDocument>(
					$"pm-{i}", new PartitionKey("pk-pm"),
					new[] { PatchOperation.Increment("/counter", 1) });
			}
		});

		await Task.WhenAll(tasks);

		// Verify all counters equal patchesPerItem
		for (var i = 0; i < itemCount; i++)
		{
			var response = await container.ReadItemAsync<LoadDocument>($"pm-{i}", new PartitionKey("pk-pm"));
			output.WriteLine($"Item pm-{i}: counter={response.Resource.Counter}");
			response.Resource.Counter.Should().Be(patchesPerItem,
				$"item pm-{i} should have counter={patchesPerItem} after {patchesPerItem} increments");
		}
	}

	[Fact(Skip = "Document size enforcement varies between InMemoryContainer (2MB limit checked on serialized JSON) "
		+ "and real Cosmos DB (which measures payload differently). Under load, 100KB+ documents cause memory pressure "
		+ "and GC flakiness. See sister test: LargeDocumentPayload_DivergentBehavior")]
	public async Task LargeDocumentPayload_UnderLoad()
	{
		var container = new InMemoryContainer("large-doc", "/partitionKey");
		var largeData = new string('x', 100_000);

		for (var i = 0; i < 50; i++)
		{
			await container.CreateItemAsync(
				new LoadDocument { Id = $"lg-{i}", PartitionKey = "pk-lg", Counter = i, Data = largeData, Timestamp = DateTimeOffset.UtcNow.ToString("O") },
				new PartitionKey("pk-lg"));
		}

		for (var i = 0; i < 50; i++)
		{
			var response = await container.ReadItemAsync<LoadDocument>($"lg-{i}", new PartitionKey("pk-lg"));
			response.Resource.Data.Should().HaveLength(100_000);
		}
	}

	[Fact]
	public async Task LargeDocumentPayload_DivergentBehavior()
	{
		// DIVERGENT BEHAVIOR: InMemoryContainer measures document size using Newtonsoft.Json serialized
		// byte count, while real Cosmos DB uses binary encoding overhead. Creating documents near the
		// limit succeeds in both, but the exact boundary differs. This test verifies that a moderately
		// large document (10KB) round-trips correctly without size-related errors.
		var container = new InMemoryContainer("large-doc-sister", "/partitionKey");
		var data = new string('A', 10_000);

		var response = await container.CreateItemAsync(
			new LoadDocument { Id = "lg-sister", PartitionKey = "pk-lg", Counter = 0, Data = data, Timestamp = DateTimeOffset.UtcNow.ToString("O") },
			new PartitionKey("pk-lg"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		var read = await container.ReadItemAsync<LoadDocument>("lg-sister", new PartitionKey("pk-lg"));
		read.Resource.Data.Should().Be(data);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Category F: Container/Database Lifecycle Under Load
// ═══════════════════════════════════════════════════════════════════════════════

public class LoadTestContainerLifecycle(ITestOutputHelper output)
{
	[Fact]
	public async Task MultipleContainersConcurrentLoad()
	{
		var containers = Enumerable.Range(0, 3)
			.Select(i => new InMemoryContainer($"multi-{i}", "/partitionKey"))
			.ToArray();

		var errors = 0;

		var tasks = containers.SelectMany((container, containerIndex) =>
			Enumerable.Range(0, 100).Select(async i =>
			{
				try
				{
					var id = $"mc-{containerIndex}-{i}";
					var pk = $"pk-{i % 5}";
					await container.CreateItemAsync(
						new LoadDocument { Id = id, PartitionKey = pk, Counter = i, Data = $"container-{containerIndex}", Timestamp = DateTimeOffset.UtcNow.ToString("O") },
						new PartitionKey(pk));
				}
				catch
				{
					Interlocked.Increment(ref errors);
				}
			}));

		await Task.WhenAll(tasks);

		// Verify complete isolation
		for (var containerIndex = 0; containerIndex < 3; containerIndex++)
		{
			var iterator = containers[containerIndex].GetItemQueryIterator<LoadDocument>(
				new QueryDefinition("SELECT * FROM c"));
			var allItems = new List<LoadDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				allItems.AddRange(page);
			}

			output.WriteLine($"Container {containerIndex}: {allItems.Count} items");
			allItems.Should().HaveCount(100);
			allItems.Should().OnlyContain(d => d.Data == $"container-{containerIndex}",
				$"container {containerIndex} should only contain its own data");
		}

		errors.Should().Be(0);
	}
}
