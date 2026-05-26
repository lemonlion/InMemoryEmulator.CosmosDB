using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class FaultInjectionGapTests4
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
	public async Task FaultInjection_Timeout_408()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)408)
			{
				Content = new StringContent("{}")
			}
		};
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = async () =>
		{
			var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (iterator.HasMoreResults)
				await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_SelectiveByPath_OnlyFailsWrites()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = request =>
			{
				// Only fail POST requests (writes/queries), allow GET requests (reads)
				if (request.Method == HttpMethod.Post)
					return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
					{
						Content = new StringContent("{}")
					};
				return null; // Allow through
			}
		};

		using var httpClient = new HttpClient(handler);

		// GET should succeed (read feed)
		var getResponse = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/test/docs");
		getResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		// POST should fail (query)
		var postRequest = new HttpRequestMessage(HttpMethod.Post,
			"https://localhost:9999/dbs/db/colls/test/docs");
		postRequest.Headers.Add("x-ms-documentdb-query", "True");
		postRequest.Headers.Add("x-ms-documentdb-isquery", "True");
		postRequest.Content = new StringContent(
			"""{"query":"SELECT * FROM c"}""", Encoding.UTF8, "application/query+json");

		var postResponse = await httpClient.SendAsync(postRequest);
		postResponse.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
	}
}


public class FaultInjectionTests
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
	public async Task FaultInjection_429_ClientReceivesThrottle()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
		{
			Content = new StringContent("""{"message":"Too many requests"}""",
				Encoding.UTF8, "application/json")
		};

		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = async () =>
		{
			var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_503_ClientReceivesServiceUnavailable()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
		{
			Content = new StringContent("""{"message":"Service unavailable"}""",
				Encoding.UTF8, "application/json")
		};

		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var act = async () =>
		{
			var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_SkipsMetadata_ByDefault()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var queryCallCount = 0;
		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjector = request =>
		{
			// Only intercept data requests, not metadata
			Interlocked.Increment(ref queryCallCount);
			return new HttpResponseMessage((HttpStatusCode)429)
			{
				Content = new StringContent("""{"message":"throttled"}""",
					Encoding.UTF8, "application/json")
			};
		};
		// FaultInjectorIncludesMetadata defaults to false

		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// The SDK should still be able to initialize (metadata requests bypass fault injector)
		// but actual queries should fail
		var act = async () =>
		{
			var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_IncludesMetadata_WhenEnabled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjectorIncludesMetadata = true;
		handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
		{
			Content = new StringContent("""{"message":"unavailable"}""",
				Encoding.UTF8, "application/json")
		};

		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Even metadata routes should fail now
		var act = async () =>
		{
			var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task FaultInjection_Intermittent_EventuallySucceeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var callCount = 0;
		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjector = _ =>
		{
			var count = Interlocked.Increment(ref callCount);
			if (count <= 2)
			{
				var response = new HttpResponseMessage((HttpStatusCode)429)
				{
					Content = new StringContent("""{"message":"throttled"}""",
						Encoding.UTF8, "application/json")
				};
				// Real Cosmos DB always includes x-ms-retry-after-ms on 429 responses.
				// Without it the SDK uses its own backoff with random jitter, which can
				// exceed MaxRetryWaitTimeOnRateLimitedRequests and cause flaky failures.
				response.Headers.Add("x-ms-retry-after-ms", "10");
				return response;
			}

			return null; // Allow normal processing after first 2 calls
		};

		// Use client with retries enabled
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 5,
				MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) }
			});

		var cosmosContainer = client.GetContainer("db", "test");

		var results = new List<TestDocument>();
		var iterator = cosmosContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().NotBeEmpty();
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category A: Operation-specific fault injection
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionOperationTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler, int maxRetries = 0) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = maxRetries,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_429_OnPointRead()
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

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => (int)ex.StatusCode == 429);
	}

	[Fact]
	public async Task FaultInjection_429_OnReplace()
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

		var act = () => client.GetContainer("db", "test")
			.ReplaceItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
				"1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_429_OnUpsert()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.UpsertItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
				new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_429_OnDelete()
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

		var act = () => client.GetContainer("db", "test")
			.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_429_OnPatch()
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

		var act = () => client.GetContainer("db", "test")
			.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "Updated") });

		await act.Should().ThrowAsync<CosmosException>();
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category B: HTTP status code coverage
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionStatusCodeTests
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

	[Theory]
	[InlineData(500, "InternalServerError")]
	[InlineData(403, "Forbidden")]
	[InlineData(400, "BadRequest")]
	public async Task FaultInjection_VariousStatusCodes_ClientReceivesError(int statusCode, string description)
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)statusCode)
			{ Content = new StringContent($"{{\"message\":\"{description}\"}}") }
		};
		using var client = CreateClient(handler);

		var act = async () =>
		{
			var iterator = client.GetContainer("db", "test")
				.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_404_ViaFaultInjection_DistinctFromNatural404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		// No items — reading "1" would naturally return 404
		// But we're injecting 404 at the handler level (different code path)

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.NotFound)
			{ Content = new StringContent("{\"message\":\"Injected 404\"}") }
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => ex.StatusCode == HttpStatusCode.NotFound);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category D: Dynamic / stateful fault injection patterns
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionDynamicTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler, int maxRetries = 0) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = maxRetries,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_SetToNull_DisablesMidway()
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

		// Should fail with fault injector active
		var act1 = async () =>
		{
			var it = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (it.HasMoreResults) await it.ReadNextAsync();
		};
		await act1.Should().ThrowAsync<CosmosException>();

		// Disable fault injection
		handler.FaultInjector = null;

		// Should succeed now
		var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().NotBeEmpty();
	}

	[Fact]
	public async Task FaultInjection_CountBased_FailsFirstNThenSucceeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var callCount = 0;
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ =>
			{
				if (Interlocked.Increment(ref callCount) <= 3)
				{
					var resp = new HttpResponseMessage((HttpStatusCode)429)
					{ Content = new StringContent("{}") };
					resp.Headers.Add("x-ms-retry-after-ms", "10");
					return resp;
				}
				return null;
			}
		};

		using var client = CreateClient(handler, maxRetries: 5);
		var cosmosContainer = client.GetContainer("db", "test");

		// With retries, should eventually succeed after N failures
		var iterator = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().NotBeEmpty();
		callCount.Should().BeGreaterThan(3);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category E: Infrastructure & edge cases
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionInfrastructureTests
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
	public async Task FaultInjection_DelegateThrows_ExceptionPropagates()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => throw new InvalidOperationException("injector crashed")
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<InvalidOperationException>()
			.WithMessage("*injector crashed*");
	}

	[Fact]
	public async Task FaultInjection_RequestLogRecords_FaultedRequests()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		try
		{
			var iterator = client.GetContainer("db", "test")
				.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		}
		catch (CosmosException) { }

		handler.RequestLog.Should().NotBeEmpty("request log should capture faulted request paths");
	}

	[Fact]
	public async Task FaultInjection_ConcurrentRequests_ThreadSafe()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var callCount = 0;
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ =>
			{
				Interlocked.Increment(ref callCount);
				return new HttpResponseMessage((HttpStatusCode)429)
				{ Content = new StringContent("{}") };
			}
		};

		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		var tasks = Enumerable.Range(0, 20).Select(async _ =>
		{
			try
			{
				var it = cosmosContainer.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
				while (it.HasMoreResults) await it.ReadNextAsync();
			}
			catch (CosmosException) { }
		});

		await Task.WhenAll(tasks);
		callCount.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task FaultInjection_DirectContainerOps_BypassFaultInjection()
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

		// Direct container operations bypass the handler entirely
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.StatusCode.Should().Be(HttpStatusCode.OK);
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task FaultInjection_WithCreateRouter_IndependentPerContainer()
	{
		var container1 = new InMemoryContainer("c1", "/partitionKey");
		var container2 = new InMemoryContainer("c2", "/partitionKey");

		await container1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C1" },
			new PartitionKey("pk1"));
		await container2.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C2" },
			new PartitionKey("pk1"));

		using var handler1 = new FakeCosmosHandler(container1)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var handler2 = new FakeCosmosHandler(container2);
		// handler2 has NO fault injector — should work normally

		// container1 is faulted, container2 is not
		// Direct access to the non-faulted container should work
		var read = await container2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("C2");
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category A+: Operation-specific and additional status code tests
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionOperationDeepDiveTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler, int maxRetries = 0) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = maxRetries,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	// ── A: Operation gaps ─────────────────────────────────────────────────

	[Fact]
	public async Task FaultInjection_429_OnCreate()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
				new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => (int)ex.StatusCode == 429);
	}

	[Fact]
	public async Task FaultInjection_429_OnQueryIterator()
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

		var act = async () =>
		{
			var iter = client.GetContainer("db", "test")
				.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (iter.HasMoreResults) await iter.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => (int)ex.StatusCode == 429);
	}

	[Fact]
	public async Task FaultInjection_429_OnReadFeed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var httpClient = new HttpClient(handler);

		handler.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
		{ Content = new StringContent("{}") };

		var response = await httpClient.GetAsync("https://localhost:9999/dbs/db/colls/test/docs");
		response.StatusCode.Should().Be((HttpStatusCode)429);
	}

	[Fact]
	public async Task FaultInjection_503_OnPointRead()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => ex.StatusCode == HttpStatusCode.ServiceUnavailable);
	}

	[Fact]
	public async Task FaultInjection_SelectiveFault_ReadSucceeds_WriteFailsViaSdk()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = req =>
				req.Method == HttpMethod.Get ? null
				: new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
				{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var c = client.GetContainer("db", "test");

		// Read (GET) should succeed
		var read = await c.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.StatusCode.Should().Be(HttpStatusCode.OK);

		// Write (POST) should fail
		var act = () => c.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "New" },
			new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	// ── B: Additional HTTP status codes ─────────────────────────────────────

	[Theory]
	[InlineData(401, "Unauthorized")]
	[InlineData(409, "Conflict")]
	[InlineData(412, "PreconditionFailed")]
	[InlineData(413, "EntityTooLarge")]
	public async Task FaultInjection_AdditionalStatusCodes_ClientReceivesError(int statusCode, string description)
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)statusCode)
			{ Content = new StringContent($"{{\"message\":\"{description}\"}}") }
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => (int)ex.StatusCode == statusCode);
	}

	[Fact]
	public async Task FaultInjection_410_Gone()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.Gone)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_449_RetryWith()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)449)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category C: Fault response fidelity & headers
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionResponseFidelityTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler, int maxRetries = 0) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = maxRetries,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_429_WithRetryAfterMs_SDKRespectsDelay()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var callCount = 0;
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ =>
			{
				if (Interlocked.Increment(ref callCount) <= 1)
				{
					var resp = new HttpResponseMessage((HttpStatusCode)429)
					{ Content = new StringContent("{}") };
					resp.Headers.Add("x-ms-retry-after-ms", "10");
					return resp;
				}
				return null;
			}
		};
		using var client = CreateClient(handler, maxRetries: 3);
		var c = client.GetContainer("db", "test");

		var read = await c.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.StatusCode.Should().Be(HttpStatusCode.OK);
		callCount.Should().BeGreaterThanOrEqualTo(2);
	}

	[Fact]
	public async Task FaultInjection_Response_WithEmptyContent()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("") }
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_Response_WithNullContent()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)500)
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_Response_WithActivityId()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var activityId = Guid.NewGuid().ToString();
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ =>
			{
				var resp = new HttpResponseMessage((HttpStatusCode)429)
				{ Content = new StringContent("{}") };
				resp.Headers.Add("x-ms-activity-id", activityId);
				return resp;
			}
		};
		using var client = CreateClient(handler);

		try
		{
			await client.GetContainer("db", "test")
				.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		}
		catch (CosmosException ex)
		{
			ex.ActivityId.Should().Be(activityId);
			return;
		}

		throw new Xunit.Sdk.XunitException("Expected CosmosException");
	}

	[Fact]
	public async Task FaultInjection_Response_WithRequestCharge()
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
				resp.Headers.Add("x-ms-request-charge", "42.5");
				return resp;
			}
		};
		using var client = CreateClient(handler);

		try
		{
			await client.GetContainer("db", "test")
				.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		}
		catch (CosmosException ex)
		{
			ex.RequestCharge.Should().Be(42.5);
			return;
		}

		throw new Xunit.Sdk.XunitException("Expected CosmosException");
	}

	[Fact]
	public async Task FaultInjection_Response_MissingContentType()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ =>
			{
				var resp = new HttpResponseMessage((HttpStatusCode)429);
				resp.Content = new ByteArrayContent(Array.Empty<byte>());
				resp.Content.Headers.ContentType = null;
				return resp;
			}
		};
		using var client = CreateClient(handler);

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_429_WithSubstatus_CollectionThrottle()
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
				resp.Headers.Add("x-ms-substatus", "3200");
				return resp;
			}
		};
		using var client = CreateClient(handler);

		try
		{
			await client.GetContainer("db", "test")
				.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		}
		catch (CosmosException ex)
		{
			((int)ex.StatusCode).Should().Be(429);
			return;
		}

		throw new Xunit.Sdk.XunitException("Expected CosmosException");
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category D+: Additional dynamic pattern tests
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionDynamicDeepDiveTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler, int maxRetries = 0) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = maxRetries,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task FaultInjection_ToggleOnOff_DynamicBehavior()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var c = client.GetContainer("db", "test");

		// Phase 1: No fault — succeeds
		var read1 = await c.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read1.StatusCode.Should().Be(HttpStatusCode.OK);

		// Phase 2: Enable fault — fails
		handler.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
		{ Content = new StringContent("{}") };

		var act = () => c.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();

		// Phase 3: Disable — succeeds again
		handler.FaultInjector = null;
		var read3 = await c.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read3.StatusCode.Should().Be(HttpStatusCode.OK);

		// Phase 4: Re-enable — fails again
		handler.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)503)
		{ Content = new StringContent("{}") };

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_MethodBased_OnlyFailsWrites_ViaSdk()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = req =>
				req.Method == HttpMethod.Get ? null
				: new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
				{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);
		var c = client.GetContainer("db", "test");

		// GET (read) succeeds
		var read = await c.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.StatusCode.Should().Be(HttpStatusCode.OK);

		// DELETE fails
		var deleteAct = () => c.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await deleteAct.Should().ThrowAsync<CosmosException>();

		// PATCH fails
		var patchAct = () => c.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Set("/name", "X") });
		await patchAct.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_DocumentIdBased_OnlyFailsSpecificDoc()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "target-doc", PartitionKey = "pk1", Name = "Target" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "safe-doc", PartitionKey = "pk1", Name = "Safe" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = req =>
				req.RequestUri?.AbsolutePath.Contains("target-doc") == true
					? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
					{ Content = new StringContent("{}") }
					: null
		};
		using var client = CreateClient(handler);
		var c = client.GetContainer("db", "test");

		// Target doc fails
		var act = () => c.ReadItemAsync<TestDocument>("target-doc", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();

		// Safe doc succeeds
		var read = await c.ReadItemAsync<TestDocument>("safe-doc", new PartitionKey("pk1"));
		read.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category E+: Infrastructure deep dive
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionInfrastructureDeepDiveTests
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
	public async Task FaultInjection_QueryLogNotPopulated_WhenFaulted()
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

		try
		{
			var iter = client.GetContainer("db", "test")
				.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (iter.HasMoreResults) await iter.ReadNextAsync();
		}
		catch (CosmosException) { }

		// QueryLog populated at line 661 (inside query handler), but fault fires before that
		handler.QueryLog.Should().BeEmpty("fault fires before query SQL is logged");
	}

	[Fact]
	public async Task FaultInjection_RequestLog_CapturesMetadataRequests_EvenWhenFaulted()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		using var handler = new FakeCosmosHandler(container);
		handler.FaultInjectorIncludesMetadata = true;
		handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
		{ Content = new StringContent("{}") };

		using var httpClient = new HttpClient(handler);
		var _ = await httpClient.GetAsync("https://localhost:9999/");

		// RequestLog is written at line 161 BEFORE the fault check at line 163
		handler.RequestLog.Should().NotBeEmpty();
	}

	[Fact]
	public async Task FaultInjection_EmptyDatabase_StillFaults()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		// No items at all

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var act = async () =>
		{
			var iter = client.GetContainer("db", "test")
				.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			while (iter.HasMoreResults) await iter.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task FaultInjection_WithCreateRouter_RoutingIsolation_ViaSdk()
	{
		var container1 = new InMemoryContainer("c1", "/partitionKey");
		var container2 = new InMemoryContainer("c2", "/partitionKey");
		await container1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C1" },
			new PartitionKey("pk1"));
		await container2.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C2" },
			new PartitionKey("pk1"));

		using var handler1 = new FakeCosmosHandler(container1)
		{
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var handler2 = new FakeCosmosHandler(container2);

		using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["c1"] = handler1,
			["c2"] = handler2,
		});
		using var client = CreateClient(router);

		// Container c1 is faulted — should fail
		var act = () => client.GetContainer("db", "c1")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();

		// Container c2 is not faulted — should succeed
		var read = await client.GetContainer("db", "c2")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("C2");
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category F: SDK retry behaviour verification
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionRetryTests
{
	[Fact]
	public async Task FaultInjection_429_NoRetries_ExactlyOneDataCall()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var dataCallCount = 0;
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = req =>
			{
				// Only count non-metadata requests
				if (req.RequestUri?.AbsolutePath.Contains("/docs") == true)
				{
					Interlocked.Increment(ref dataCallCount);
					return new HttpResponseMessage((HttpStatusCode)429)
					{ Content = new StringContent("{}") };
				}
				return null;
			}
		};

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

		try
		{
			await client.GetContainer("db", "test")
				.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		}
		catch (CosmosException) { }

		dataCallCount.Should().Be(1, "with 0 retries, exactly 1 data call should be made");
	}

	[Fact]
	public async Task FaultInjection_429_MaxRetriesExhausted_StillThrows()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var callCount = 0;
		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = req =>
			{
				if (req.RequestUri?.AbsolutePath.Contains("/docs") == true)
				{
					Interlocked.Increment(ref callCount);
					var resp = new HttpResponseMessage((HttpStatusCode)429)
					{ Content = new StringContent("{}") };
					resp.Headers.Add("x-ms-retry-after-ms", "1");
					return resp;
				}
				return null;
			}
		};

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 2,
				MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var act = () => client.GetContainer("db", "test")
			.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
		callCount.Should().BeGreaterThanOrEqualTo(2, "SDK should retry at least 2 times");
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Category G: Stream API fault injection
// ═══════════════════════════════════════════════════════════════════════════

public class FaultInjectionStreamTests
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
	public async Task FaultInjection_429_OnCreateItemStreamAsync()
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
			.CreateItemStreamAsync(stream, new PartitionKey("pk1"));

		((int)response.StatusCode).Should().Be(429);
	}

	[Fact]
	public async Task FaultInjection_429_OnReadItemStreamAsync()
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
			.ReadItemStreamAsync("1", new PartitionKey("pk1"));

		// Stream API returns status code instead of throwing
		((int)response.StatusCode).Should().Be(429);
	}

	[Fact]
	public async Task FaultInjection_429_OnReplaceItemStreamAsync()
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

		var json = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
		using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

		var response = await client.GetContainer("db", "test")
			.ReplaceItemStreamAsync(stream, "1", new PartitionKey("pk1"));

		((int)response.StatusCode).Should().Be(429);
	}

	[Fact]
	public async Task FaultInjection_429_OnDeleteItemStreamAsync()
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
			.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

		((int)response.StatusCode).Should().Be(429);
	}

	[Fact]
	public async Task FaultInjection_503_OnGetItemQueryStreamIterator()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		using var handler = new FakeCosmosHandler(container)
		{
			FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var iter = client.GetContainer("db", "test")
			.GetItemQueryStreamIterator("SELECT * FROM c");

		var response = await iter.ReadNextAsync();
		response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
	}
}
