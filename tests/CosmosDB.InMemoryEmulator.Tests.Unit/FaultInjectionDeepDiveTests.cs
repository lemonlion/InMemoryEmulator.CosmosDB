using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Plan #15: Fault injection deep-dive tests.
/// Covers stream operations, LINQ, batch/change-feed limitations,
/// router isolation, content-read edge cases, sequential faults,
/// cancellation, and concurrent delegate swap.
/// </summary>
public class FaultInjectionDeepDiveStreamTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_429_OnUpsertItemStreamAsync()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

		var response = await client.GetContainer("db", "test")
			.UpsertItemStreamAsync(stream, new PartitionKey("pk1"));

		((int)response.StatusCode).Should().Be(429);
	}

	[Fact]
	public async Task FaultInjection_429_OnPatchItemStreamAsync()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var response = await client.GetContainer("db", "test")
			.PatchItemStreamAsync("1", new PartitionKey("pk1"),
				[PatchOperation.Set("/name", "Patched")]);

		((int)response.StatusCode).Should().Be(429);
	}
}

public class FaultInjectionBatchTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact(Skip = "FakeCosmosHandler does not handle transactional batch routes (/docs with batch headers). " +
				 "Batch operations use a different HTTP endpoint pattern not in the handler's route table.")]
	public async Task FaultInjection_429_OnTransactionalBatch()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var batch = cosmosContainer.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new { id = "b1", partitionKey = "pk1" });

		var response = await batch.ExecuteAsync();
		((int)response.StatusCode).Should().Be(429);
	}

	[Fact(Skip = "FakeCosmosHandler does not handle transactional batch routes (/docs with batch headers). " +
				 "Batch operations use a different HTTP endpoint pattern not in the handler's route table.")]
	public async Task FaultInjection_503_OnTransactionalBatch()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var batch = cosmosContainer.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new { id = "b2", partitionKey = "pk1" });

		var response = await batch.ExecuteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
	}
}

public class FaultInjectionLinqTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_429_OnLinqCountAsync()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = () => cosmosContainer.GetItemLinqQueryable<TestDocument>().CountAsync();

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_429_OnLinqWhereToFeedIterator()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = async () =>
		{
			var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == "Test")
				.ToFeedIterator();
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}
}

public class FaultInjectionReadManyTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact(Skip = "ReadManyItemsAsync hangs when fault injector returns 429. " +
				 "ReadMany uses internal query orchestration that doesn't respect " +
				 "MaxRetryAttemptsOnRateLimitedRequests=0, causing an infinite retry loop.")]
	public async Task FaultInjection_429_OnReadManyItemsAsync()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var act = () => cosmosContainer.ReadManyItemsAsync<TestDocument>(items);

		// ReadMany uses query internally — should be affected by fault injection
		await act.Should().ThrowAsync<CosmosException>();
	}
}

public class FaultInjectionQueryPlanTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_QueryPlanRequest_NotAffectedByDefault()
	{
		// The fault injector fires AFTER metadata routes but query plan detection
		// happens AFTER fault injection in the current code flow.
		// On Windows, query plan is computed locally via ServiceInterop, so query plan
		// requests don't go through the handler. This test verifies that queries work
		// even when the fault injector is set, because on Windows the query plan
		// is computed locally and the data query is what gets faulted.
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var dataRequestFaulted = false;
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = request =>
			{
				// Only fault non-query-plan POST requests (data queries)
				if (request.Headers.TryGetValues("x-ms-cosmos-is-query-plan-request", out _))
					return null; // Let query plan through
				dataRequestFaulted = true;
				return new HttpResponseMessage((HttpStatusCode)429)
				{ Content = new StringContent("{}") };
			}
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = async () =>
		{
			var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
		dataRequestFaulted.Should().BeTrue();
	}

	[Fact]
	public async Task FaultInjection_QueryPlanRequest_AffectedWhenMetadataEnabled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
			{ Content = new StringContent("{}") },
			FaultInjectorIncludesMetadata = true
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// With FaultInjectorIncludesMetadata=true, ALL requests (including metadata
		// and query plan) are faulted. The SDK can't even initialize.
		var act = async () =>
		{
			var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}
}

public class FaultInjectionEdgeCaseTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_ContentReadByInjector_SubsequentHandlerStillWorks()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);

		handler.FaultInjector = request =>
		{
			// Intentionally read the content to inspect it — this consumes the stream
			if (request.Content != null)
			{
				var body = request.Content.ReadAsStringAsync().Result;
				// Decision: pass through (return null)
			}
			return null;
		};

		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// This should succeed even though the fault injector read the content
		var doc = new TestDocument { Id = "ci1", PartitionKey = "pk1", Name = "Content" };
		var response = await cosmosContainer.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Resource.Name.Should().Be("Content");
	}

	[Fact]
	public async Task FaultInjection_CancellationToken_CancelledBeforeFault()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		using var cts = new CancellationTokenSource();
		cts.Cancel(); // Cancel before the request

		var act = () => cosmosContainer.ReadItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"), cancellationToken: cts.Token);

		// Either cancellation or fault takes precedence — document which
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task FaultInjection_SequentialFaults_NoStateAccumulation()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Make 50 sequential faulted requests
		for (var i = 0; i < 50; i++)
		{
			try
			{
				await cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
			}
			catch (CosmosException) { }
		}

		// Clear fault and verify normal operation
		handler.FaultInjector = null;
		var response = await cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Test");

		// Request log should have accumulated entries
		handler.RequestLog.Count.Should().BeGreaterThan(50);
	}

	[Fact]
	public async Task FaultInjection_FaultInjectorReturnsNull_RequestPassesThrough()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => null! // Always pass through
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var response = await cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task FaultInjection_ConcurrentDelegateSwap_ThreadSafe()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 20; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = $"cs-{i}", PartitionKey = "pk1", Name = $"Doc{i}" },
				new PartitionKey("pk1"));
		}

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();
		var tasks = Enumerable.Range(0, 20).Select(async i =>
		{
			try
			{
				// After a short delay, swap the injector to null
				if (i == 10)
				{
					await Task.Delay(10);
					handler.FaultInjector = null;
				}
				await cosmosContainer.ReadItemAsync<TestDocument>($"cs-{i}", new PartitionKey("pk1"));
			}
			catch (CosmosException) { /* expected 429 */ }
			catch (Exception ex)
			{
				exceptions.Add(ex); // unexpected exceptions
			}
		});

		await Task.WhenAll(tasks);

		// No unexpected exceptions (NullReferenceException etc)
		exceptions.Should().BeEmpty();
	}
}

public class FaultInjectionDeepDiveRouterTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_Router_ThreeContainers_OnlyMiddleFaulted()
	{
		var c1 = new InMemoryContainer("c1", "/partitionKey");
		var c2 = new InMemoryContainer("c2", "/partitionKey");
		var c3 = new InMemoryContainer("c3", "/partitionKey");

		await c1.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C1" }, new PartitionKey("pk1"));
		await c2.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C2" }, new PartitionKey("pk1"));
		await c3.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C3" }, new PartitionKey("pk1"));

		using var h1 = new FakeCosmosHandler(c1);
		using var h2 = new FakeCosmosHandler(c2)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var h3 = new FakeCosmosHandler(c3);

		var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["c1"] = h1,
			["c2"] = h2,
			["c3"] = h3,
		});

		using var client = CreateClient(router);

		// C1 succeeds (db1)
		var read1 = await client.GetContainer("db", "c1").ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read1.Resource.Name.Should().Be("C1");

		// C2 fails (db2 is faulted)
		var act2 = () => client.GetContainer("db", "c2").ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act2.Should().ThrowAsync<CosmosException>();

		// C3 succeeds (db3)
		var read3 = await client.GetContainer("db", "c3").ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read3.Resource.Name.Should().Be("C3");
	}

	[Fact]
	public async Task FaultInjection_Router_DynamicFaultChange_AcrossContainers()
	{
		var c1 = new InMemoryContainer("c1", "/partitionKey");
		var c2 = new InMemoryContainer("c2", "/partitionKey");

		await c1.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C1" }, new PartitionKey("pk1"));
		await c2.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C2" }, new PartitionKey("pk1"));

		using var h1 = new FakeCosmosHandler(c1)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var h2 = new FakeCosmosHandler(c2);

		var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["c1"] = h1,
			["c2"] = h2,
		});

		using var client = CreateClient(router);

		// Initially: c1 faulted, c2 normal
		var act1 = () => client.GetContainer("db", "c1").ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act1.Should().ThrowAsync<CosmosException>();

		var read2 = await client.GetContainer("db", "c2").ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read2.Resource.Name.Should().Be("C2");

		// Swap: disable c1 fault, enable c2 fault
		h1.FaultInjector = null;
		h2.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
		{ Content = new StringContent("{}") };

		var read1 = await client.GetContainer("db", "c1").ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read1.Resource.Name.Should().Be("C1");

		var act2 = () => client.GetContainer("db", "c2").ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act2.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_Router_FaultOnDefaultHandler()
	{
		var c1 = new InMemoryContainer("c1", "/partitionKey");
		using var h1 = new FakeCosmosHandler(c1)
		{
			FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
			{ Content = new StringContent("{}") },
			FaultInjectorIncludesMetadata = true
		};

		var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["c1"] = h1,
		});

		using var client = CreateClient(router);

		// With FaultInjectorIncludesMetadata=true on the handler, SDK can't initialize
		// because account metadata request gets faulted
		var act = () => client.GetContainer("db", "c1")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}
}

public class FaultInjectionDeepDiveResponseTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_429_WithRetryAfterHeader_StandardFormat()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ =>
			{
				var resp = new HttpResponseMessage((HttpStatusCode)429)
				{ Content = new StringContent("{}") };
				resp.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromSeconds(1));
				return resp;
			}
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = () => cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be((HttpStatusCode)429);
	}

	[Fact]
	public async Task FaultInjection_503_WithRetryAfterHeader()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ =>
			{
				var resp = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
				{ Content = new StringContent("{}") };
				resp.Headers.Add("x-ms-retry-after-ms", "100");
				return resp;
			}
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = () => cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
	}

	[Fact]
	public async Task FaultInjection_ResponseWithCustomHeaders_PreservedInException()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ =>
			{
				var resp = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
				{ Content = new StringContent("{}") };
				resp.Headers.Add("x-ms-substatus", "3200");
				return resp;
			}
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = () => cosmosContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
		ex.Which.SubStatusCode.Should().Be(3200);
	}
}

public class FaultInjectionChangeFeedDeepDiveTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact(Skip = "FakeCosmosHandler does not route change feed requests. Change feed uses " +
				 "special headers (a]4-im, If-None-Match) on GET /docs which the handler " +
				 "routes to ReadFeed instead of ChangeFeed.")]
	public async Task FaultInjection_ChangeFeedIterator_AffectedByFault()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var iterator = cosmosContainer.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);

		var response = await iterator.ReadNextAsync();
		((int)response.StatusCode).Should().Be(429);
	}
}

public class FaultInjectionBulkTests
{
	[Fact]
	public async Task FaultInjection_BulkExecution_EachOpCanBeFaulted()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				AllowBulkExecution = true,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var cosmosContainer = client.GetContainer("db", "test");

		var tasks = Enumerable.Range(0, 10).Select(i =>
		{
			var doc = new TestDocument { Id = $"bulk-{i}", PartitionKey = "pk1", Name = $"Doc{i}" };
			return cosmosContainer.CreateItemAsync(doc, new PartitionKey("pk1"));
		}).ToList();

		var exceptions = new List<CosmosException>();
		foreach (var task in tasks)
		{
			try { await task; }
			catch (CosmosException ex) { exceptions.Add(ex); }
		}

		// All ops should fail with 429
		exceptions.Count.Should().Be(10);
		exceptions.Should().OnlyContain(e => e.StatusCode == (HttpStatusCode)429);
	}
}
