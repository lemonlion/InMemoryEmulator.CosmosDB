using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests.Performance;

public class EmulatorLoadTests(ITestOutputHelper output) : IAsyncLifetime
{
	private const string EmulatorEndpoint = "https://localhost:8081";

	private const string EmulatorKey =
		"C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

	private const string DatabaseName = "perf-test-db";
	private const string ContainerName = "perf-test-container";

	private static readonly int _targetCallsPerSecond =
		int.TryParse(Environment.GetEnvironmentVariable("EMULATOR_LOAD_TEST_RPS"), out var rps) ? rps : 50;

	private static readonly int _durationSeconds =
		int.TryParse(Environment.GetEnvironmentVariable("EMULATOR_LOAD_TEST_DURATION_SECONDS"), out var dur)
			? dur
			: 30;

	private static readonly int _totalOperations = _targetCallsPerSecond * _durationSeconds;

	private CosmosClient _client = null!;
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_client = new CosmosClient(EmulatorEndpoint, EmulatorKey, new CosmosClientOptions
		{
			HttpClientFactory = () =>
			{
				var handler = new HttpClientHandler
				{
					ServerCertificateCustomValidationCallback =
						HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
				};
				return new HttpClient(handler);
			},
			ConnectionMode = ConnectionMode.Gateway
		});

		var databaseResponse = await _client.CreateDatabaseIfNotExistsAsync(DatabaseName);
		var containerResponse = await databaseResponse.Database.CreateContainerIfNotExistsAsync(
			ContainerName, "/partitionKey");
		_container = containerResponse.Container;
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			await _client.GetDatabase(DatabaseName).DeleteAsync();
		}
		catch
		{
			// best-effort cleanup
		}

		_client.Dispose();
	}

	[Fact]
	public async Task ReadHeavyLoad_80PercentReads_20PercentWrites()
	{
		var stats = new LoadStats();

		await SeedDocuments(count: 1000);

		await RunLoad(stats, readWeight: 80, writeWeight: 20, seedCount: 1000);

		ReportAndAssert(stats, "Emulator Read-Heavy (80/20)");
	}

	[Fact]
	public async Task WriteHeavyLoad_20PercentReads_80PercentWrites()
	{
		var stats = new LoadStats();

		await SeedDocuments(count: 200);

		await RunLoad(stats, readWeight: 20, writeWeight: 80, seedCount: 200);

		ReportAndAssert(stats, "Emulator Write-Heavy (20/80)");
	}

	[Fact]
	public async Task EvenMixLoad_50PercentReads_50PercentWrites()
	{
		var stats = new LoadStats();

		await SeedDocuments(count: 500);

		await RunLoad(stats, readWeight: 50, writeWeight: 50, seedCount: 500);

		ReportAndAssert(stats, "Emulator Even Mix (50/50)");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Load runner
	// ═══════════════════════════════════════════════════════════════════════════

	private async Task RunLoad(
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
		var maxConcurrency = Math.Max(_targetCallsPerSecond * 2, 100);
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
						await ExecuteReadOperation(knownIds, stats);
					}
					else
					{
						await ExecuteWriteOperation(knownIds, stats, nextId);
					}

					operationStopwatch.Stop();
					stats.RecordLatency(operationStopwatch.Elapsed);
				}
				// Note: 404s from replace/patch racing with concurrent delete are expected and counted
				// here rather than under the specific operation stat. totalOps still includes NotFound,
				// so the assertion in ReportAndAssert remains correct.
				catch (CosmosException cosmosException)
					when (cosmosException.StatusCode == HttpStatusCode.NotFound)
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
					stats.ErrorMessages.Enqueue(
						$"Op {capturedIndex}: {exception.GetType().Name}: {exception.Message}");
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

	private async Task ExecuteReadOperation(
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var readType = Random.Shared.Next(4);

		switch (readType)
		{
			case 0:
				await ReadAndVerifySingle(knownIds, stats);
				break;
			case 1:
				await ReadAndVerifyQuery(stats);
				break;
			case 2:
				await ReadAndVerifyQueryWithFilter(knownIds, stats);
				break;
			case 3:
				await ReadAndVerifyCrossPartitionQuery(stats);
				break;
		}
	}

	private async Task ReadAndVerifySingle(
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Reads);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		if (!knownIds.TryGetValue(targetId, out var partitionKey))
		{
			Interlocked.Increment(ref stats.Reads);
			return;
		}

		var response = await _container.ReadItemAsync<LoadDocument>(targetId, new PartitionKey(partitionKey));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Id.Should().Be(targetId);
		response.Resource.PartitionKey.Should().Be(partitionKey);
		Interlocked.Increment(ref stats.Reads);
	}

	private async Task ReadAndVerifyCrossPartitionQuery(LoadStats stats)
	{
		var threshold = Random.Shared.Next(500);
		var iterator = _container.GetItemQueryIterator<LoadDocument>(
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

	private async Task ReadAndVerifyQuery(LoadStats stats)
	{
		var iterator = _container.GetItemQueryIterator<LoadDocument>(
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

	private async Task ReadAndVerifyQueryWithFilter(
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Reads);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		var iterator = _container.GetItemQueryIterator<LoadDocument>(
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

	private async Task ExecuteWriteOperation(
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats,
		AtomicCounter nextId)
	{
		var writeType = Random.Shared.Next(5);

		switch (writeType)
		{
			case 0:
				await WriteCreate(knownIds, stats, nextId);
				break;
			case 1:
				await WriteUpsert(knownIds, stats, nextId);
				break;
			case 2:
				await WriteReplace(knownIds, stats);
				break;
			case 3:
				await WritePatch(knownIds, stats);
				break;
			case 4:
				await WriteDelete(knownIds, stats);
				break;
		}
	}

	private async Task WriteCreate(
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats,
		AtomicCounter nextId)
	{
		var id = nextId.Increment().ToString();
		var partitionKey = $"pk-{id}";
		var document = CreateDocument(id, partitionKey);

		var response = await _container.CreateItemAsync(document, new PartitionKey(partitionKey));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		knownIds.TryAdd(id, partitionKey);
		Interlocked.Increment(ref stats.Creates);
	}

	private async Task WriteUpsert(
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
					Interlocked.Increment(ref stats.Upserts);
					return;
				}

				var document = CreateDocument(targetId, partitionKey);

				var response = await _container.UpsertItemAsync(document, new PartitionKey(partitionKey));

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

		var createResponse = await _container.UpsertItemAsync(newDocument, new PartitionKey(newPartitionKey));

		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);
		knownIds.TryAdd(id, newPartitionKey);
		Interlocked.Increment(ref stats.Upserts);
	}

	private async Task WriteReplace(
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Replaces);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		if (!knownIds.TryGetValue(targetId, out var partitionKey))
		{
			Interlocked.Increment(ref stats.Replaces);
			return;
		}

		var document = CreateDocument(targetId, partitionKey);

		var response = await _container.ReplaceItemAsync(document, targetId, new PartitionKey(partitionKey));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		Interlocked.Increment(ref stats.Replaces);
	}

	private async Task WritePatch(
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Patches);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		if (!knownIds.TryGetValue(targetId, out var patchPartitionKey))
		{
			Interlocked.Increment(ref stats.Patches);
			return;
		}

		var patchOperations = new List<PatchOperation>
		{
			PatchOperation.Increment("/counter", 1),
			PatchOperation.Set("/data", $"patched-{Guid.NewGuid():N}")
		};

		var response = await _container.PatchItemAsync<LoadDocument>(
			targetId, new PartitionKey(patchPartitionKey), patchOperations);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		Interlocked.Increment(ref stats.Patches);
	}

	private async Task WriteDelete(
		ConcurrentDictionary<string, string> knownIds,
		LoadStats stats)
	{
		var ids = knownIds.Keys.ToArray();
		if (ids.Length == 0)
		{
			Interlocked.Increment(ref stats.Deletes);
			return;
		}

		var targetId = ids[Random.Shared.Next(ids.Length)];
		if (!knownIds.TryRemove(targetId, out var partitionKey))
		{
			Interlocked.Increment(ref stats.Deletes);
			return;
		}

		var response = await _container.DeleteItemAsync<LoadDocument>(
			targetId, new PartitionKey(partitionKey));

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
		Interlocked.Increment(ref stats.Deletes);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Seeding
	// ═══════════════════════════════════════════════════════════════════════════

	private async Task SeedDocuments(int count)
	{
		for (var seedIndex = 1; seedIndex <= count; seedIndex++)
		{
			var id = seedIndex.ToString();
			var partitionKey = $"pk-{seedIndex % 20}";
			var document = CreateDocument(id, partitionKey);
			await _container.CreateItemAsync(document, new PartitionKey(partitionKey));
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
					   + stats.NotFound + stats.Errors;

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

		stats.GetPercentile(99).Should().BeLessThan(5000,
			"P99 latency should stay under 5000ms even for the real emulator to catch hangs");
	}
}
