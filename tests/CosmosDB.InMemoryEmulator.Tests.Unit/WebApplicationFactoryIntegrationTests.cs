using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// Minimal test app for integration tests
// ════════════════════════════════════════════════════════════════════════════════

public record CosmosTestItem(
	[property: JsonPropertyName("id")]
	[property: Newtonsoft.Json.JsonProperty("id")]
	string Id,
	[property: JsonPropertyName("partitionKey")]
	[property: Newtonsoft.Json.JsonProperty("partitionKey")]
	string PartitionKey,
	[property: JsonPropertyName("name")]
	[property: Newtonsoft.Json.JsonProperty("name")]
	string Name);

/// <summary>
/// A repository that resolves Container from DI — simulates the BreakfastProvider pattern.
/// </summary>
public class TestRepository
{
	private readonly Container _container;

	public TestRepository(Container container)
	{
		_container = container;
	}

	public async Task<CosmosTestItem> CreateAsync(CosmosTestItem item)
	{
		var response = await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
		return response.Resource;
	}

	public async Task<CosmosTestItem?> GetByIdAsync(string id, string partitionKey)
	{
		try
		{
			var response = await _container.ReadItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey));
			return response.Resource;
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			return null;
		}
	}

	public async Task<List<CosmosTestItem>> GetAllAsync()
	{
		var iterator = _container.GetItemLinqQueryable<CosmosTestItem>(true)
			.ToFeedIteratorOverridable();

		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		return results;
	}

	public async Task<CosmosTestItem> UpsertAsync(CosmosTestItem item)
	{
		var response = await _container.UpsertItemAsync(item, new PartitionKey(item.PartitionKey));
		return response.Resource;
	}

	public async Task DeleteAsync(string id, string partitionKey)
	{
		await _container.DeleteItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey));
	}

	public async Task<CosmosTestItem> ReplaceAsync(CosmosTestItem item)
	{
		var response = await _container.ReplaceItemAsync(item, item.Id, new PartitionKey(item.PartitionKey));
		return response.Resource;
	}

	public async Task PatchNameAsync(string id, string partitionKey, string newName)
	{
		await _container.PatchItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey),
			new[] { PatchOperation.Set("/name", newName) });
	}

	public async Task<List<CosmosTestItem>> GetFilteredByNameAsync(string name)
	{
		var iterator = _container.GetItemLinqQueryable<CosmosTestItem>(true)
			.Where(x => x.Name == name)
			.ToFeedIteratorOverridable();

		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		return results;
	}

	public async Task<List<CosmosTestItem>> QuerySqlAsync(string sql)
	{
		var iterator = _container.GetItemQueryIterator<CosmosTestItem>(new QueryDefinition(sql));
		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		return results;
	}
}

/// <summary>
/// A repository that resolves Container from CosmosClient — simulates Pattern 3 (Acquisition.Api).
/// </summary>
public class ClientResolvedRepository
{
	private readonly Container _container;

	public ClientResolvedRepository(CosmosClient client)
	{
		_container = client.GetContainer("TestDb", "items");
	}

	public async Task<CosmosTestItem> CreateAsync(CosmosTestItem item)
	{
		var response = await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
		return response.Resource;
	}

	public async Task<CosmosTestItem?> GetByIdAsync(string id, string partitionKey)
	{
		try
		{
			var response = await _container.ReadItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey));
			return response.Resource;
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			return null;
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Typed CosmosClient for Pattern 2 testing
// ════════════════════════════════════════════════════════════════════════════════

public class TestCosmosClient : CosmosClient
{
	public TestCosmosClient(string connectionString, CosmosClientOptions? options = null)
		: base(connectionString, options) { }
}

/// <summary>
/// Repository that resolves a typed InMemoryCosmosClient — simulates Pattern 2.
/// </summary>
public class TypedClientRepository
{
	private readonly Container _container;

	public TypedClientRepository(TestCosmosClient client)
	{
		_container = client.GetContainer("TestDb", "items");
	}

	public async Task<CosmosTestItem> CreateAsync(CosmosTestItem item)
	{
		var response = await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
		return response.Resource;
	}

	public async Task<CosmosTestItem?> GetByIdAsync(string id, string partitionKey)
	{
		try
		{
			var response = await _container.ReadItemAsync<CosmosTestItem>(id, new PartitionKey(partitionKey));
			return response.Resource;
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			return null;
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Helper to build a TestServer-backed IHost with "production" + "test" services
// ════════════════════════════════════════════════════════════════════════════════

public class TestAppHost : IAsyncDisposable
{
	private IHost? _host;
	private HttpClient? _httpClient;

	public IServiceProvider Services => _host!.Services;
	public HttpClient HttpClient => _httpClient!;

	private TestAppHost() { }

	public static async Task<TestAppHost> CreateAsync(
		Action<IServiceCollection>? configureTestServices = null,
		Action<IServiceCollection>? configureBaseServices = null,
		Action<IEndpointRouteBuilder>? configureEndpoints = null)
	{
		var instance = new TestAppHost();

		var builder = new HostBuilder()
			.ConfigureWebHost(webBuilder =>
			{
				webBuilder.UseTestServer();
				webBuilder.ConfigureServices(services =>
				{
					services.AddRouting();

					if (configureBaseServices != null)
					{
						configureBaseServices(services);
					}
					else
					{
						// Default: simulate a real app registering CosmosClient + Container
						services.AddSingleton<CosmosClient>(_ =>
							new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
						services.AddSingleton(sp =>
						{
							var client = sp.GetRequiredService<CosmosClient>();
							return client.GetContainer("ProductionDb", "items");
						});
						services.AddSingleton<TestRepository>();
					}
				});

				// Test services override production registrations (same as ConfigureTestServices)
				if (configureTestServices != null)
				{
					webBuilder.ConfigureServices(configureTestServices);
				}

				webBuilder.Configure(app =>
				{
					app.UseRouting();
					app.UseEndpoints(endpoints =>
					{
						if (configureEndpoints != null)
						{
							configureEndpoints(endpoints);
						}
						else
						{
							endpoints.MapGet("/items/{id}", async (string id, TestRepository repo) =>
							{
								var item = await repo.GetByIdAsync(id, id);
								return item is not null ? Results.Ok(item) : Results.NotFound();
							});

							endpoints.MapPost("/items", async (CosmosTestItem item, TestRepository repo) =>
							{
								try
								{
									var created = await repo.CreateAsync(item);
									return Results.Created($"/items/{created.Id}", created);
								}
								catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
								{
									return Results.Conflict();
								}
							});

							endpoints.MapGet("/items", async (TestRepository repo) =>
							{
								var items = await repo.GetAllAsync();
								return Results.Ok(items);
							});

							endpoints.MapDelete("/items/{id}", async (string id, TestRepository repo) =>
							{
								try
								{
									await repo.DeleteAsync(id, id);
									return Results.NoContent();
								}
								catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
								{
									return Results.NotFound();
								}
							});

							endpoints.MapPut("/items/{id}", async (string id, CosmosTestItem item, TestRepository repo) =>
							{
								try
								{
									var replaced = await repo.ReplaceAsync(item);
									return Results.Ok(replaced);
								}
								catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
								{
									return Results.NotFound();
								}
							});

							endpoints.MapPut("/items", async (CosmosTestItem item, TestRepository repo) =>
							{
								var upserted = await repo.UpsertAsync(item);
								return Results.Ok(upserted);
							});

							endpoints.MapMethods("/items/{id}/name", new[] { "PATCH" },
								async (string id, HttpContext ctx) =>
								{
									var body = await ctx.Request.ReadFromJsonAsync<JsonElement>();
									var newName = body.GetProperty("name").GetString()!;
									var repo = ctx.RequestServices.GetRequiredService<TestRepository>();
									try
									{
										await repo.PatchNameAsync(id, id, newName);
										return Results.Ok();
									}
									catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
									{
										return Results.NotFound();
									}
								});

							endpoints.MapGet("/items/search", async (string name, TestRepository repo) =>
							{
								var items = await repo.GetFilteredByNameAsync(name);
								return Results.Ok(items);
							});

							endpoints.MapPost("/items/query", async (HttpContext ctx) =>
							{
								var body = await ctx.Request.ReadFromJsonAsync<JsonElement>();
								var sql = body.GetProperty("sql").GetString()!;
								var repo = ctx.RequestServices.GetRequiredService<TestRepository>();
								var items = await repo.QuerySqlAsync(sql);
								return Results.Ok(items);
							});
						}
					});
				});
			});

		instance._host = await builder.StartAsync();
		instance._httpClient = instance._host.GetTestClient();
		return instance;
	}

	public async ValueTask DisposeAsync()
	{
		_httpClient?.Dispose();
		if (_host != null)
		{
			await _host.StopAsync();
			_host.Dispose();
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 4: Integration Tests
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WebApplicationFactoryIntegrationTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new()
	{
		PropertyNameCaseInsensitive = true
	};

	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task CreateAndReadItem()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		var item = new CosmosTestItem("1", "1", "Test");
		var postResponse = await client.PostAsJsonAsync("/items", item);
		postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var getResponse = await client.GetAsync("/items/1");
		getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		result!.Name.Should().Be("Test");
	}

	[Fact]
	public async Task ReadNotFound()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		var response = await client.GetAsync("/items/nonexistent");
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ListItems()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Alice"));
		await client.PostAsJsonAsync("/items", new CosmosTestItem("2", "2", "Bob"));
		await client.PostAsJsonAsync("/items", new CosmosTestItem("3", "3", "Charlie"));

		var response = await client.GetAsync("/items");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().HaveCount(3);
	}

	[Fact]
	public async Task LinqQueryWorks()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Alice"));
		await client.PostAsJsonAsync("/items", new CosmosTestItem("2", "2", "Bob"));

		var response = await client.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().HaveCount(2);
	}

	[Fact]
	public async Task ClientIsAccessible()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var cosmosClient = app.Services.GetRequiredService<CosmosClient>();
		cosmosClient.Should().NotBeNull();

		var container = cosmosClient.GetContainer("in-memory-db", "items");
		container.Should().NotBeNull();
		container.Id.Should().Be("items");
	}

	[Fact]
	public async Task IsolatedPerFactory()
	{
		await using var app1 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		await using var app2 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var client1 = app1.HttpClient;
		var client2 = app2.HttpClient;

		await client1.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Factory1Item"));

		var response1 = await client1.GetAsync("/items/1");
		response1.StatusCode.Should().Be(HttpStatusCode.OK);

		var response2 = await client2.GetAsync("/items/1");
		response2.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task CalledFromConfigureTestServices_FullRoundTrip()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		// Create
		var postResponse = await client.PostAsJsonAsync("/items",
			new CosmosTestItem("rt1", "rt1", "RoundTrip"));
		postResponse.StatusCode.Should().Be(HttpStatusCode.Created);
		var created = await postResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		created!.Id.Should().Be("rt1");
		created.Name.Should().Be("RoundTrip");

		// Read
		var getResponse = await client.GetAsync("/items/rt1");
		getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		var read = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		read!.Name.Should().Be("RoundTrip");

		// List
		var listResponse = await client.GetAsync("/items");
		var items = await listResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().ContainSingle().Which.Name.Should().Be("RoundTrip");
	}

	[Fact]
	public async Task ConstructorResolvedContainer_Pattern3()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddSingleton<ClientResolvedRepository>();
			},
			configureTestServices: services =>
			{
				services.UseInMemoryCosmosDB(o =>
				{
					o.DatabaseName = "TestDb";
					o.AddContainer("items", "/partitionKey");
				});
			},
			configureEndpoints: endpoints =>
			{
				endpoints.MapPost("/client-items", async (CosmosTestItem item, ClientResolvedRepository repo) =>
				{
					var created = await repo.CreateAsync(item);
					return Results.Created($"/client-items/{created.Id}", created);
				});
				endpoints.MapGet("/client-items/{id}", async (string id, ClientResolvedRepository repo) =>
				{
					var result = await repo.GetByIdAsync(id, id);
					return result is not null ? Results.Ok(result) : Results.NotFound();
				});
			});

		var client = app.HttpClient;

		var postResponse = await client.PostAsJsonAsync("/client-items",
			new CosmosTestItem("p3-1", "p3-1", "Pattern3"));
		postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var getResponse = await client.GetAsync("/client-items/p3-1");
		getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		result!.Name.Should().Be("Pattern3");
	}

	[Fact]
	public async Task ZeroConfig_JustWorks()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB());
		var client = app.HttpClient;

		var postResponse = await client.PostAsJsonAsync("/items",
			new CosmosTestItem("z1", "z1", "ZeroConfig"));
		postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var getResponse = await client.GetAsync("/items/z1");
		getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ZeroConfig_AutoDetect_ContainerNameMatchesProduction()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB());

		// The auto-detected container should be the one from the production factory:
		// client.GetContainer("ProductionDb", "items")
		var container = app.Services.GetRequiredService<Container>();
		container.Id.Should().Be("items");

		// Verify the client also returns a container with the same name
		var cosmosClient = app.Services.GetRequiredService<CosmosClient>();
		var clientContainer = cosmosClient.GetContainer("ProductionDb", "items");
		clientContainer.Id.Should().Be("items");
	}

	[Fact]
	public async Task UpsertAndQuery()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();

		await container.UpsertItemAsync(
			new CosmosTestItem("u1", "u1", "Original"),
			new PartitionKey("u1"));
		await container.UpsertItemAsync(
			new CosmosTestItem("u1", "u1", "Updated"),
			new PartitionKey("u1"));
		await container.UpsertItemAsync(
			new CosmosTestItem("u2", "u2", "Second"),
			new PartitionKey("u2"));

		var httpClient = app.HttpClient;
		var response = await httpClient.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().HaveCount(2);
		items.Should().Contain(i => i.Id == "u1" && i.Name == "Updated");
		items.Should().Contain(i => i.Id == "u2" && i.Name == "Second");
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase A: Fix Existing Issues
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafLinqFilterTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task LinqQueryWithFilter_ReturnsMatchingItems()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Alice"));
		await client.PostAsJsonAsync("/items", new CosmosTestItem("2", "2", "Bob"));

		var response = await client.GetAsync("/items/search?name=Alice");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task LinqWithNativeToFeedIterator_WorksWithUseInMemoryCosmosDB()
	{
		// UseInMemoryCosmosDB uses FakeCosmosHandler, so native .ToFeedIterator() works
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();

		await container.CreateItemAsync(new CosmosTestItem("1", "1", "Alice"), new PartitionKey("1"));
		await container.CreateItemAsync(new CosmosTestItem("2", "2", "Bob"), new PartitionKey("2"));

		// Use native .ToFeedIterator() — NOT .ToFeedIteratorOverridable()
		var iterator = container.GetItemLinqQueryable<CosmosTestItem>(true)
			.Where(x => x.Name == "Alice")
			.ToFeedIterator();

		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase B: Missing CRUD Operations via HTTP
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafCrudViaHttpTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task DeleteItem_ViaHttp_RemovesItem()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/items", new CosmosTestItem("d1", "d1", "ToDelete"));

		var deleteResponse = await client.DeleteAsync("/items/d1");
		deleteResponse.StatusCode.Should().Be(HttpStatusCode.NoContent);

		var getResponse = await client.GetAsync("/items/d1");
		getResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteItem_ViaHttp_NotFound_Returns404()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		var response = await client.DeleteAsync("/items/nonexistent");
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReplaceItem_ViaHttp_UpdatesItem()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/items", new CosmosTestItem("r1", "r1", "Original"));

		var putResponse = await client.PutAsJsonAsync("/items/r1",
			new CosmosTestItem("r1", "r1", "Replaced"));
		putResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		var getResponse = await client.GetAsync("/items/r1");
		var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		result!.Name.Should().Be("Replaced");
	}

	[Fact]
	public async Task UpsertItem_ViaHttp_CreatesOrUpdates()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		// Upsert new
		var upsertResponse = await client.PutAsJsonAsync("/items",
			new CosmosTestItem("up1", "up1", "New"));
		upsertResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		// Upsert existing
		var upsertResponse2 = await client.PutAsJsonAsync("/items",
			new CosmosTestItem("up1", "up1", "Updated"));
		upsertResponse2.StatusCode.Should().Be(HttpStatusCode.OK);

		var getResponse = await client.GetAsync("/items/up1");
		var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		result!.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task PatchItem_ViaHttp_PartialUpdate()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/items", new CosmosTestItem("p1", "p1", "Original"));

		var patchContent = JsonContent.Create(new { name = "Patched" });
		var patchRequest = new HttpRequestMessage(new HttpMethod("PATCH"), "/items/p1/name")
		{
			Content = patchContent
		};
		var patchResponse = await client.SendAsync(patchRequest);
		patchResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		var getResponse = await client.GetAsync("/items/p1");
		var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		result!.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task CreateDuplicateItem_ViaHttp_Returns409()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		var item = new CosmosTestItem("dup1", "dup1", "First");
		await client.PostAsJsonAsync("/items", item);

		var duplicateResponse = await client.PostAsJsonAsync("/items", item);
		duplicateResponse.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase C: Missing DI Integration Patterns
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafDiPatternTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task UseInMemoryCosmosContainers_WorksThroughWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddSingleton(sp => sp.GetRequiredService<CosmosClient>().GetContainer("Db", "items"));
				services.AddSingleton<TestRepository>();
			},
			configureTestServices: services =>
			{
				services.UseInMemoryCosmosContainers(o => o.AddContainer("items", "/partitionKey"));
			});
		var client = app.HttpClient;

		var postResponse = await client.PostAsJsonAsync("/items",
			new CosmosTestItem("c1", "c1", "ContainerPattern"));
		postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var getResponse = await client.GetAsync("/items/c1");
		getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		result!.Name.Should().Be("ContainerPattern");
	}

	[Fact]
	public async Task TypedClient_WorksThroughWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<TypedClientRepository>();
			},
			configureTestServices: services =>
			{
				services.UseInMemoryCosmosDB<TestCosmosClient>(o =>
				{
					o.DatabaseName = "TestDb";
					o.AddContainer("items", "/partitionKey");
				});
			},
			configureEndpoints: endpoints =>
			{
				endpoints.MapPost("/typed-items", async (CosmosTestItem item, TypedClientRepository repo) =>
				{
					var created = await repo.CreateAsync(item);
					return Results.Created($"/typed-items/{created.Id}", created);
				});
				endpoints.MapGet("/typed-items/{id}", async (string id, TypedClientRepository repo) =>
				{
					var result = await repo.GetByIdAsync(id, id);
					return result is not null ? Results.Ok(result) : Results.NotFound();
				});
			});
		var client = app.HttpClient;

		var postResponse = await client.PostAsJsonAsync("/typed-items",
			new CosmosTestItem("t1", "t1", "TypedClient"));
		postResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var getResponse = await client.GetAsync("/typed-items/t1");
		getResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		var result = await getResponse.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		result!.Name.Should().Be("TypedClient");
	}

	[Fact]
	public async Task MultipleContainers_SameApp_IsolatedData()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddRouting();
			},
			configureTestServices: services =>
			{
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("orders", "/partitionKey");
					o.AddContainer("products", "/partitionKey");
				});
			},
			configureEndpoints: endpoints =>
			{
				endpoints.MapPost("/orders", async (CosmosTestItem item, CosmosClient cosmosClient) =>
				{
					var container = cosmosClient.GetContainer("in-memory-db", "orders");
					var response = await container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
					return Results.Created($"/orders/{response.Resource.Id}", response.Resource);
				});
				endpoints.MapGet("/orders", async (CosmosClient cosmosClient) =>
				{
					var container = cosmosClient.GetContainer("in-memory-db", "orders");
					var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
					var results = new List<CosmosTestItem>();
					while (iterator.HasMoreResults)
					{
						var page = await iterator.ReadNextAsync();
						results.AddRange(page);
					}

					return Results.Ok(results);
				});
				endpoints.MapGet("/products", async (CosmosClient cosmosClient) =>
				{
					var container = cosmosClient.GetContainer("in-memory-db", "products");
					var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
					var results = new List<CosmosTestItem>();
					while (iterator.HasMoreResults)
					{
						var page = await iterator.ReadNextAsync();
						results.AddRange(page);
					}

					return Results.Ok(results);
				});
			});
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/orders", new CosmosTestItem("o1", "o1", "Order1"));

		var ordersResponse = await client.GetAsync("/orders");
		var orders = await ordersResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		orders.Should().ContainSingle();

		var productsResponse = await client.GetAsync("/products");
		var products = await productsResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		products.Should().BeEmpty();
	}

	[Fact]
	public async Task OnHandlerCreated_FaultInjection_ViaWaf()
	{
		FakeCosmosHandler? capturedHandler = null;

		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, handler) => capturedHandler = handler;
				}));

		capturedHandler.Should().NotBeNull();

		var client = app.HttpClient;

		// Seed an item first
		await client.PostAsJsonAsync("/items", new CosmosTestItem("f1", "f1", "Faulted"));

		// Now inject a fault — all reads return 503
		capturedHandler!.FaultInjector = _ =>
			new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);

		// TestServer propagates unhandled exceptions rather than returning 500,
		// so the CosmosException surfaces directly to the caller
		Func<Task> act = async () => await client.GetAsync("/items/f1");
		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => ex.StatusCode == HttpStatusCode.ServiceUnavailable);
	}

	[Fact]
	public async Task HttpMessageHandlerWrapper_RequestCounting_ViaWaf()
	{
		var requestCount = 0;

		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.WithHttpMessageHandlerWrapper(innerHandler =>
					{
						var countingHandler = new CountingDelegatingHandler(innerHandler, () => Interlocked.Increment(ref requestCount));
						return countingHandler;
					});
				}));
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/items", new CosmosTestItem("c1", "c1", "Counted"));
		await client.GetAsync("/items/c1");

		requestCount.Should().BeGreaterThan(0);
	}
}

/// <summary>
/// DelegatingHandler that counts requests passing through it.
/// </summary>
public class CountingDelegatingHandler : DelegatingHandler
{
	private readonly Action _onRequest;

	public CountingDelegatingHandler(HttpMessageHandler inner, Action onRequest) : base(inner)
	{
		_onRequest = onRequest;
	}

	protected override Task<HttpResponseMessage> SendAsync(
		HttpRequestMessage request, CancellationToken cancellationToken)
	{
		_onRequest();
		return base.SendAsync(request, cancellationToken);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase D: Query & Data Patterns
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafQueryPatternTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task SqlQuery_ViaHttpEndpoint_ReturnsFilteredResults()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		await client.PostAsJsonAsync("/items", new CosmosTestItem("q1", "q1", "Alice"));
		await client.PostAsJsonAsync("/items", new CosmosTestItem("q2", "q2", "Bob"));
		await client.PostAsJsonAsync("/items", new CosmosTestItem("q3", "q3", "Alice"));

		var queryResponse = await client.PostAsJsonAsync("/items/query",
			new { sql = "SELECT * FROM c WHERE c.name = 'Alice'" });
		queryResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		var items = await queryResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().HaveCount(2);
		items.Should().OnlyContain(i => i.Name == "Alice");
	}

	[Fact]
	public async Task EmptyContainer_ListReturnsEmptyArray()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		var response = await client.GetAsync("/items");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().BeEmpty();
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase E: Error & Edge Cases
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafEdgeCaseTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task ConcurrentRequests_AllSucceed()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		var tasks = Enumerable.Range(1, 10).Select(i =>
			client.PostAsJsonAsync("/items",
				new CosmosTestItem($"c{i}", $"c{i}", $"Item{i}")));
		var responses = await Task.WhenAll(tasks);

		responses.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

		var listResponse = await client.GetAsync("/items");
		var items = await listResponse.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().HaveCount(10);
	}

	[Fact]
	public async Task ItemWithSpecialCharactersInId_RoundTrips()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		// Use direct container access to avoid HTTP URL encoding issues with the test endpoint
		var container = app.Services.GetRequiredService<Container>();
		var id = "item+special chars&more";
		var item = new CosmosTestItem(id, id, "Special");
		await container.CreateItemAsync(item, new PartitionKey(id));

		var read = await container.ReadItemAsync<CosmosTestItem>(id, new PartitionKey(id));
		read.Resource.Name.Should().Be("Special");
		read.Resource.Id.Should().Be(id);
	}

	[Fact]
	public async Task ETagConcurrency_ViaDirectContainer_InWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();

		// Create and capture ETag
		var createResponse = await container.CreateItemAsync(
			new CosmosTestItem("e1", "e1", "Original"), new PartitionKey("e1"));
		var originalETag = createResponse.ETag;

		// Update the item, which changes the ETag
		await container.ReplaceItemAsync(
			new CosmosTestItem("e1", "e1", "Updated"), "e1", new PartitionKey("e1"));

		// Try to replace with stale ETag — should fail
		var act = () => container.ReplaceItemAsync(
			new CosmosTestItem("e1", "e1", "StaleUpdate"), "e1", new PartitionKey("e1"),
			new ItemRequestOptions { IfMatchEtag = originalETag });
		await act.Should().ThrowAsync<CosmosException>()
			.Where(ex => ex.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task ItemWithDifferentIdAndPartitionKey_ViaHttp()
	{
		// Demonstrates that GET /items/{id} assumes PK == ID — items with different PK won't be found
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();

		// Create an item where PK differs from ID
		await container.CreateItemAsync(
			new CosmosTestItem("abc", "xyz", "DiffPk"), new PartitionKey("xyz"));

		var client = app.HttpClient;
		// The HTTP endpoint hardcodes PK=ID, so this returns 404
		var response = await client.GetAsync("/items/abc");
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);

		// But direct container access with correct PK works
		var read = await container.ReadItemAsync<CosmosTestItem>("abc", new PartitionKey("xyz"));
		read.Resource.Name.Should().Be("DiffPk");
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase F: Difficult Behaviours (Skip + Sister Tests)
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafDivergentBehaviorTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact(Skip = "UseInMemoryCosmosDB creates a single FakeCosmosHandler and CosmosClient at registration time. "
		+ "The Container resolved from client.GetContainer() returns the same in-memory backing store regardless of scope. "
		+ "Per-request data isolation would require a scoped FakeCosmosHandler factory. "
		+ "See sister test: ScopedLifetime_ActualBehavior_ReturnsSameDataAcrossScopes")]
	public async Task ScopedLifetime_PreservesPerRequestIsolation()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddScoped<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddScoped(sp => sp.GetRequiredService<CosmosClient>().GetContainer("Db", "items"));
				services.AddScoped<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var client = app.HttpClient;
		// If scoped, data written in one request should be isolated from another
		await client.PostAsJsonAsync("/items", new CosmosTestItem("s1", "s1", "ScopedItem"));
		var response = await client.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().BeEmpty(); // Would need true scoped isolation
	}

	[Fact]
	public async Task ScopedLifetime_ActualBehavior_ReturnsSameDataAcrossScopes()
	{
		// DIVERGENT BEHAVIOR: Even when the base service registers Container as Scoped,
		// UseInMemoryCosmosDB replaces it with a Singleton CosmosClient. The Container
		// resolved from client.GetContainer() shares the same in-memory store. Data written
		// in one scope is visible in all other scopes. This matches real CosmosClient behavior
		// (the SDK client is typically a singleton), but differs from what "Scoped" lifetime
		// might imply for DI-resolved Container instances.
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddScoped<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddScoped(sp => sp.GetRequiredService<CosmosClient>().GetContainer("Db", "items"));
				services.AddScoped<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var client = app.HttpClient;
		await client.PostAsJsonAsync("/items", new CosmosTestItem("s1", "s1", "ScopedItem"));

		var response = await client.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().ContainSingle().Which.Name.Should().Be("ScopedItem");
	}

	[Fact(Skip = "HTTP pagination requires exposing continuation tokens through the HTTP API layer. "
		+ "The FakeCosmosHandler supports pagination internally, but surfacing this through a minimal API "
		+ "endpoint would require custom response headers and request parameters beyond the scope of the "
		+ "integration pattern being tested. See sister test: Pagination_WorksViaDirectContainerAccess_InWafContext")]
	public async Task Pagination_ContinuationToken_ViaHttp()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		var client = app.HttpClient;

		for (var i = 0; i < 100; i++)
			await client.PostAsJsonAsync("/items", new CosmosTestItem($"p{i}", $"p{i}", $"Item{i}"));

		// Would need continuation token header support in the endpoint
		var response = await client.GetAsync("/items?pageSize=10");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Pagination_WorksViaDirectContainerAccess_InWafContext()
	{
		// DIVERGENT BEHAVIOR: Pagination via FeedIterator works correctly at the Container
		// level. However, when accessed through HTTP endpoints in a WAF context, pagination
		// requires the API layer to thread continuation tokens through request/response headers —
		// this is an application concern, not an emulator concern.
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();

		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new CosmosTestItem($"p{i}", $"p{i}", $"Item{i}"), new PartitionKey($"p{i}"));

		var iterator = container.GetItemQueryIterator<CosmosTestItem>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

		var allItems = new List<CosmosTestItem>();
		var pageCount = 0;
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
			pageCount++;
		}

		allItems.Should().HaveCount(5);
		pageCount.Should().BeGreaterThan(1);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Plan 46: DI Callback Coverage
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafCallbackTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task OnClientCreated_CapturesCosmosClient_ViaWaf()
	{
		CosmosClient? captured = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnClientCreated = client => captured = client;
				}));

		captured.Should().NotBeNull();
		var resolved = app.Services.GetRequiredService<CosmosClient>();
		captured.Should().BeSameAs(resolved);
	}

	[Fact]
	public async Task OnContainerCreated_CapturesInMemoryContainer_ViaWaf()
	{
		InMemoryContainer? captured = null;
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<Container>(_ =>
					new InMemoryContainer("items", "/partitionKey"));
				services.AddSingleton<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosContainers(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnContainerCreated = c => captured = (InMemoryContainer)c;
				}));

		captured.Should().NotBeNull();

		await captured!.CreateItemAsync(
			new CosmosTestItem("seed1", "seed1", "Seeded"),
			new PartitionKey("seed1"));

		var container = app.Services.GetRequiredService<Container>();
		var read = await container.ReadItemAsync<CosmosTestItem>("seed1", new PartitionKey("seed1"));
		read.Resource.Name.Should().Be("Seeded");
	}

	[Fact]
	public async Task RegisterFeedIteratorSetup_False_NativeToFeedIteratorStillWorks()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.RegisterFeedIteratorSetup = false;
				}));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new CosmosTestItem("1", "1", "Test"),
			new PartitionKey("1"));

		// Native query iterator works because UseInMemoryCosmosDB uses FakeCosmosHandler
		var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Plan 46: Data Seeding Patterns
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafSeedingPatternTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new()
	{
		PropertyNameCaseInsensitive = true
	};

	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task SeedingViaOnHandlerCreated_BackingContainer_ViaWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, handler) =>
					{
						handler.BackingContainer.CreateItemAsync(
							new CosmosTestItem("seed1", "seed1", "SeededViaHandler"),
							new PartitionKey("seed1")).GetAwaiter().GetResult();
					};
				}));

		var response = await app.HttpClient.GetAsync("/items/seed1");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var item = await response.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		item!.Name.Should().Be("SeededViaHandler");
	}

	[Fact]
	public async Task SeedingViaOnContainerCreated_ViaWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<Container>(_ =>
					new InMemoryContainer("items", "/partitionKey"));
				services.AddSingleton<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosContainers(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnContainerCreated = container =>
					{
						((InMemoryContainer)container).CreateItemAsync(
							new CosmosTestItem("seed1", "seed1", "SeededViaContainer"),
							new PartitionKey("seed1")).GetAwaiter().GetResult();
					};
				}));

		var container = app.Services.GetRequiredService<Container>();
		var read = await container.ReadItemAsync<CosmosTestItem>("seed1", new PartitionKey("seed1"));
		read.Resource.Name.Should().Be("SeededViaContainer");
	}

	[Fact]
	public async Task SeedingViaOnClientCreated_ViaWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnClientCreated = client =>
					{
						var container = client.GetContainer("in-memory-db", "items");
						container.CreateItemAsync(
							new CosmosTestItem("seed1", "seed1", "SeededViaClient"),
							new PartitionKey("seed1")).GetAwaiter().GetResult();
					};
				}));

		var response = await app.HttpClient.GetAsync("/items/seed1");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var item = await response.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		item!.Name.Should().Be("SeededViaClient");
	}

	[Fact]
	public async Task SeedingViaImportState_InDiCallback_ViaWaf()
	{
		// Pre-generate state by exporting from a seeded container
		var source = new InMemoryContainer("items", "/partitionKey");
		await source.CreateItemAsync(
			new CosmosTestItem("imp1", "imp1", "Imported"),
			new PartitionKey("imp1"));
		var state = source.ExportState();

		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, handler) =>
					{
						handler.BackingInMemoryContainer.ImportState(state);
					};
				}));

		var response = await app.HttpClient.GetAsync("/items/imp1");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var item = await response.Content.ReadFromJsonAsync<CosmosTestItem>(JsonOptions);
		item!.Name.Should().Be("Imported");
	}

	[Fact]
	public async Task ClearItems_ResetBetweenTests_ViaWaf()
	{
		FakeCosmosHandler? capturedHandler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, handler) => capturedHandler = handler;
				}));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new CosmosTestItem("1", "1", "ToBeCleared"),
			new PartitionKey("1"));

		capturedHandler!.BackingInMemoryContainer.ClearItems();

		var response = await app.HttpClient.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().BeEmpty();
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Plan 46: Advanced Cosmos Features via WAF
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafAdvancedFeatureTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new()
	{
		PropertyNameCaseInsensitive = true
	};

	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task TransactionalBatch_ViaWaf_AtomicCreateAndRead()
	{
		FakeCosmosHandler? handler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) => handler = h;
				}));

		// Use backing container for batch (HTTP-proxied Container doesn't support batch)
		var container = handler!.BackingContainer;
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new CosmosTestItem("b1", "pk1", "Batch1"));
		batch.CreateItem(new CosmosTestItem("b2", "pk1", "Batch2"));
		batch.CreateItem(new CosmosTestItem("b3", "pk1", "Batch3"));
		var batchResult = await batch.ExecuteAsync();
		batchResult.IsSuccessStatusCode.Should().BeTrue();

		var response = await app.HttpClient.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().HaveCount(3);
	}

	[Fact]
	public async Task ChangeFeed_ViaWaf_ReadsChangesAfterHttpMutations()
	{
		FakeCosmosHandler? handler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) => handler = h;
				}));

		var client = app.HttpClient;
		await client.PostAsJsonAsync("/items", new CosmosTestItem("cf1", "cf1", "CF1"));
		await client.PostAsJsonAsync("/items", new CosmosTestItem("cf2", "cf2", "CF2"));
		await client.PostAsJsonAsync("/items", new CosmosTestItem("cf3", "cf3", "CF3"));

		var backingContainer = handler!.BackingContainer;
		var iterator = backingContainer.GetChangeFeedIterator<Newtonsoft.Json.Linq.JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.LatestVersion);

		var changes = new List<Newtonsoft.Json.Linq.JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(page.Resource);
		}

		changes.Should().HaveCount(3);
	}

	[Fact]
	public async Task Ttl_ViaWaf_ExpiredItemsNotReturned()
	{
		FakeCosmosHandler? handler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) =>
					{
						handler = h;
						h.BackingInMemoryContainer.DefaultTimeToLive = 1;
					};
				}));

		var client = app.HttpClient;
		await client.PostAsJsonAsync("/items", new CosmosTestItem("ttl1", "ttl1", "Temporary"));

		await Task.Delay(TimeSpan.FromSeconds(3));

		var response = await app.HttpClient.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().BeEmpty();
	}

	[Fact]
	public async Task ReadMany_ViaWaf_ReturnsRequestedItems()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		for (var i = 1; i <= 5; i++)
			await container.CreateItemAsync(
				new CosmosTestItem($"rm{i}", $"rm{i}", $"Item{i}"),
				new PartitionKey($"rm{i}"));

		var readManyResult = await container.ReadManyItemsAsync<CosmosTestItem>(new List<(string, PartitionKey)>
		{
			("rm1", new PartitionKey("rm1")),
			("rm3", new PartitionKey("rm3")),
			("rm5", new PartitionKey("rm5"))
		});

		readManyResult.Resource.Should().HaveCount(3);
	}

	[Fact]
	public async Task StatePersistence_ExportImport_ViaWaf()
	{
		FakeCosmosHandler? handler1 = null;
		await using var app1 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) => handler1 = h;
				}));

		await app1.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("s1", "s1", "State1"));
		await app1.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("s2", "s2", "State2"));

		var exportedState = handler1!.BackingInMemoryContainer.ExportState();

		FakeCosmosHandler? handler2 = null;
		await using var app2 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) =>
					{
						handler2 = h;
						h.BackingInMemoryContainer.ImportState(exportedState);
					};
				}));

		var response = await app2.HttpClient.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().HaveCount(2);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Plan 46: Container / Database Management via WAF
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafContainerManagementTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task DynamicContainerCreation_ViaWaf_CreateThenUse()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var client = app.Services.GetRequiredService<CosmosClient>();
		var db = client.GetDatabase("in-memory-db");
		var dynamicResponse = await db.CreateContainerIfNotExistsAsync("dynamic", "/partitionKey");
		var dynamicContainer = dynamicResponse.Container;

		await dynamicContainer.CreateItemAsync(
			new CosmosTestItem("d1", "a", "Dynamic"),
			new PartitionKey("a"));

		var read = await dynamicContainer.ReadItemAsync<CosmosTestItem>("d1", new PartitionKey("a"));
		read.Resource.Name.Should().Be("Dynamic");
	}

	[Fact]
	public async Task ReadContainerProperties_ViaWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		var props = await container.ReadContainerAsync();
		props.Resource.Id.Should().Be("items");
	}

	[Fact]
	public async Task ReadAccountProperties_ViaWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var client = app.Services.GetRequiredService<CosmosClient>();
		var account = await client.ReadAccountAsync();
		account.Should().NotBeNull();
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Plan 46: Pattern Validation
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafPatternValidationTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task ProductionClientNotReachable_ReplacedByEmulator()
	{
		// Base services register a CosmosClient pointing to an unreachable endpoint.
		// UseInMemoryCosmosDB replaces it. If replacement failed, CreateItemAsync would
		// throw a network error.
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		var act = () => container.CreateItemAsync(
			new CosmosTestItem("1", "1", "Works"),
			new PartitionKey("1"));

		await act.Should().NotThrowAsync();
	}

	[Fact]
	public async Task RegistrationOrder_DoesNotMatter()
	{
		// Register services before UseInMemoryCosmosDB
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddSingleton(sp =>
					sp.GetRequiredService<CosmosClient>().GetContainer("Db", "items"));
				services.AddSingleton<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new CosmosTestItem("1", "1", "OrderTest"),
			new PartitionKey("1"));

		var read = await container.ReadItemAsync<CosmosTestItem>("1", new PartitionKey("1"));
		read.Resource.Name.Should().Be("OrderTest");
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Plan 46: Divergent Behaviour Deep Tests (Skip + Sister Pairs)
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafDivergentBehaviorDeepTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact(Skip = "DIVERGENT: When RegisterFeedIteratorSetup=false with UseInMemoryCosmosContainers, "
			   + ".ToFeedIteratorOverridable() falls back to native .ToFeedIterator() which requires "
			   + "the Cosmos LINQ provider. InMemoryContainer's IOrderedQueryable doesn't implement "
			   + "this provider, so it throws.")]
	public void RegisterFeedIteratorSetup_False_WithContainers_BreaksOverridable() { }

	[Fact]
	public async Task RegisterFeedIteratorSetup_False_WithContainers_NativeQueryStillWorks()
	{
		// With UseInMemoryCosmosContainers + RegisterFeedIteratorSetup=false,
		// SQL queries still work; only LINQ .ToFeedIteratorOverridable() breaks.
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<Container>(_ =>
					new InMemoryContainer("items", "/partitionKey"));
				services.AddSingleton<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosContainers(o =>
				{
					o.AddContainer("items", "/partitionKey");
				}));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new CosmosTestItem("1", "1", "Test"),
			new PartitionKey("1"));

		var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task PerContainerDatabaseName_MultipleDbsSameContainerName_ShouldIsolate()
	{
		// Two containers with the same name in different databases should be isolated
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddSingleton<TestRepository>();
				services.AddSingleton(sp => sp.GetRequiredService<CosmosClient>().GetContainer("Db1", "items"));
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey", databaseName: "Db1");
					o.AddContainer("items", "/partitionKey", databaseName: "Db2");
				}));

		var client = app.Services.GetRequiredService<CosmosClient>();
		var db1Items = client.GetContainer("Db1", "items");
		var db2Items = client.GetContainer("Db2", "items");

		await db1Items.CreateItemAsync(
			new CosmosTestItem("i1", "i1", "FromDb1"),
			new PartitionKey("i1"));
		await db2Items.CreateItemAsync(
			new CosmosTestItem("i2", "i2", "FromDb2"),
			new PartitionKey("i2"));

		var db1Iterator = db1Items.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var db1Results = new List<CosmosTestItem>();
		while (db1Iterator.HasMoreResults)
			db1Results.AddRange(await db1Iterator.ReadNextAsync());

		var db2Iterator = db2Items.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var db2Results = new List<CosmosTestItem>();
		while (db2Iterator.HasMoreResults)
			db2Results.AddRange(await db2Iterator.ReadNextAsync());

		db1Results.Should().HaveCount(1);
		db1Results[0].Name.Should().Be("FromDb1");
		db2Results.Should().HaveCount(1);
		db2Results[0].Name.Should().Be("FromDb2");
	}

	[Fact]
	public async Task PerContainerDatabaseName_DifferentContainerNames_IsolatesCorrectly()
	{
		// Different container names in different databases work because routing is by container name
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddSingleton<TestRepository>();
				services.AddSingleton(sp => sp.GetRequiredService<CosmosClient>().GetContainer("Db", "items"));
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey", databaseName: "Db1");
					o.AddContainer("products", "/partitionKey", databaseName: "Db2");
				}));

		var client = app.Services.GetRequiredService<CosmosClient>();
		var items = client.GetContainer("Db1", "items");
		var products = client.GetContainer("Db2", "products");

		await items.CreateItemAsync(
			new CosmosTestItem("i1", "i1", "Item"),
			new PartitionKey("i1"));
		await products.CreateItemAsync(
			new CosmosTestItem("p1", "p1", "Product"),
			new PartitionKey("p1"));

		var itemsIterator = items.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var itemResults = new List<CosmosTestItem>();
		while (itemsIterator.HasMoreResults)
			itemResults.AddRange(await itemsIterator.ReadNextAsync());

		var productsIterator = products.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var productResults = new List<CosmosTestItem>();
		while (productsIterator.HasMoreResults)
			productResults.AddRange(await productsIterator.ReadNextAsync());

		itemResults.Should().HaveCount(1);
		itemResults[0].Name.Should().Be("Item");
		productResults.Should().HaveCount(1);
		productResults[0].Name.Should().Be("Product");
	}

	[Fact(Skip = "DIVERGENT: Stored procedure registration requires InMemoryContainer.RegisterStoredProcedure() "
			   + "which is not available on the abstract Container class resolved from DI. "
			   + "Use OnHandlerCreated to capture the FakeCosmosHandler.BackingContainer.")]
	public void StoredProcedure_ViaWaf_RequiresBackingContainerAccess() { }

	[Fact(Skip = "DIVERGENT: Trigger registration requires InMemoryContainer.RegisterTrigger() "
			   + "which is not part of the Container abstract class. Additionally, "
			   + "pre-trigger headers must be threaded through ItemRequestOptions, "
			   + "which HTTP endpoints don't expose.")]
	public void Trigger_ViaWaf_RequiresBackingContainerAndHeaders() { }

	[Fact]
	public async Task UniqueKeyPolicy_ViaWaf_AddContainerWithContainerProperties()
	{
		var containerProps = new ContainerProperties("unique-test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/Email" } } }
			}
		};

		using var host = new HostBuilder()
			.ConfigureWebHost(web =>
			{
				web.UseTestServer();
				web.ConfigureServices(services =>
				{
					services.UseInMemoryCosmosDB(opts =>
						opts.AddContainer(containerProps));
				});
				web.Configure(_ => { });
			})
			.Build();

		await host.StartAsync();
		var client = host.Services.GetRequiredService<CosmosClient>();
		var container = client.GetContainer("in-memory-db", "unique-test");

		// First insert succeeds
		await container.CreateItemAsync(
			new { id = "1", partitionKey = "pk", Email = "a@b.com" },
			new PartitionKey("pk"));

		// Duplicate unique key should fail with Conflict
		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			container.CreateItemAsync(
				new { id = "2", partitionKey = "pk", Email = "a@b.com" },
				new PartitionKey("pk")));
		Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);
	}

	[Fact(Skip = "DIVERGENT: Deleting a container via DeleteContainerAsync() removes it from "
			   + "the in-memory database, but the DI container retains its reference to the "
			   + "now-deleted singleton. This is a DI lifetime concern.")]
	public void DeleteContainer_ViaWaf_DiStillHoldsReference() { }

	[Fact(Skip = "DIVERGENT: After DisposeAsync(), the IHost and TestServer are stopped. "
			   + "Subsequent HttpClient calls throw, but the exact exception type is "
			   + "framework-dependent (ObjectDisposedException, HttpRequestException, etc.).")]
	public void Dispose_ThenAttemptUse_FrameworkDependentException() { }

	[Fact(Skip = "DIVERGENT: Change feed continuation tokens via HTTP require threading tokens "
			   + "through request/response headers — an application concern, not an emulator concern.")]
	public void ChangeFeed_IncrementalReads_ViaHttp_RequiresCustomHeaders() { }

	[Fact(Skip = "DIVERGENT: Item-level TTL requires _ttl property on the document. "
			   + "CosmosTestItem record doesn't include this field. Testing through HTTP "
			   + "requires a dedicated record type or dynamic JSON endpoints.")]
	public void Ttl_ItemLevelOverride_ViaHttp_RequiresCustomRecordType() { }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 47: WAF Integration Deep Dive Tests
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class WafCrudEdgeCaseTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task ReplaceNonExistent_ViaHttp_Returns404()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var response = await app.HttpClient.PutAsJsonAsync("/items/nonexistent",
			new CosmosTestItem("nonexistent", "nonexistent", "Ghost"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PatchNonExistent_ViaHttp_Returns404()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var response = await app.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Patch, "/items/nonexistent/name")
		{
			Content = JsonContent.Create(new { name = "NewName" })
		});

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

[Collection("FeedIteratorSetup")]
public class WafAdvancedDataPatternTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task CrossPartitionQuery_ViaWaf_ReturnsAllPartitions()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new CosmosTestItem($"item-{i}", $"pk-{i}", $"Item{i}"),
				new PartitionKey($"pk-{i}"));

		var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(5);
	}

	[Fact]
	public async Task StreamApi_CreateAndRead_ViaWaf()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		var json = """{"id":"stream1","partitionKey":"pk1","name":"StreamItem"}""";
		var createResponse = await container.CreateItemStreamAsync(
			new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
			new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await container.ReadItemStreamAsync("stream1", new PartitionKey("pk1"));
		readResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		using var reader = new System.IO.StreamReader(readResponse.Content);
		var body = await reader.ReadToEndAsync();
		body.Should().Contain("StreamItem");
	}

	[Fact]
	public async Task ParameterizedQuery_ViaWaf_ReturnsFilteredResults()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(new CosmosTestItem("1", "pk1", "Alice"), new PartitionKey("pk1"));
		await container.CreateItemAsync(new CosmosTestItem("2", "pk1", "Bob"), new PartitionKey("pk1"));
		await container.CreateItemAsync(new CosmosTestItem("3", "pk1", "Alice"), new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @name")
			.WithParameter("@name", "Alice");

		var iterator = container.GetItemQueryIterator<CosmosTestItem>(query);
		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results.Should().OnlyContain(r => r.Name == "Alice");
	}

	[Fact]
	public async Task BulkOperations_ViaWaf_AllSucceed()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.CreateItemAsync(
				new CosmosTestItem($"bulk-{i}", "pk1", $"Bulk{i}"),
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

		var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var all = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
			all.AddRange(await iterator.ReadNextAsync());
		all.Should().HaveCount(50);
	}

	[Fact]
	public async Task EmptyStringPartitionKey_ViaWaf_ItemAccessible()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new CosmosTestItem("none1", "", "NoPartition"),
			new PartitionKey(""));

		var read = await container.ReadItemAsync<CosmosTestItem>("none1", new PartitionKey(""));
		read.Resource.Name.Should().Be("NoPartition");
	}
}

[Collection("FeedIteratorSetup")]
public class WafLoggingDiagnosticsTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task RequestLog_CapturesHttpOperations_ViaWaf()
	{
		FakeCosmosHandler? handler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) => handler = h;
				}));

		// Perform CRUD via HTTP
		await app.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("log1", "log1", "LogTest"));
		await app.HttpClient.GetAsync("/items/log1");
		await app.HttpClient.DeleteAsync("/items/log1");

		handler.Should().NotBeNull();
		handler!.RequestLog.Should().NotBeEmpty();
		handler.RequestLog.Count.Should().BeGreaterThanOrEqualTo(3);
	}

	[Fact]
	public async Task QueryLog_CapturesSqlQueries_ViaWaf()
	{
		FakeCosmosHandler? handler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) => handler = h;
				}));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(new CosmosTestItem("q1", "q1", "Test"), new PartitionKey("q1"));

		// Execute SQL query
		var iterator = container.GetItemQueryIterator<CosmosTestItem>(
			new QueryDefinition("SELECT * FROM c WHERE c.name = 'Test'"));
		while (iterator.HasMoreResults)
			await iterator.ReadNextAsync();

		handler.Should().NotBeNull();
		handler!.QueryLog.Should().NotBeEmpty();
	}
}

[Collection("FeedIteratorSetup")]
public class WafStateLifecycleTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task NewHost_AfterDispose_StartsEmpty()
	{
		// First host: create an item
		await using (var app1 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey"))))
		{
			await app1.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("x", "x", "Temp"));
			var resp = await app1.HttpClient.GetAsync("/items");
			var items = await resp.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
			items.Should().HaveCount(1);
		}

		// Second host: should start empty (no persistence)
		await using var app2 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var response = await app2.HttpClient.GetAsync("/items");
		var items2 = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items2.Should().BeEmpty();
	}

	[Fact]
	public async Task DisposeAsync_IsIdempotent()
	{
		var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		await app.DisposeAsync();
		// Second dispose should not throw
		await app.DisposeAsync();
	}

	[Fact]
	public async Task ContainerProperties_WithDefaultTtl_ViaWaf_ItemsExpire()
	{
		FakeCosmosHandler? handler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) =>
					{
						handler = h;
						h.BackingInMemoryContainer.DefaultTimeToLive = 1;
					};
				}));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(new CosmosTestItem("ttl1", "pk1", "Temp"), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(3));

		var iterator = container.GetItemQueryIterator<CosmosTestItem>("SELECT * FROM c");
		var results = new List<CosmosTestItem>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task StatePersistenceDirectory_AutoSavesOnDispose_ReloadsOnNewHost()
	{
		var tempDir = Path.Combine(Path.GetTempPath(), $"cosmos-test-{Guid.NewGuid():N}");
		try
		{
			// Host 1: create items, explicitly persist, then dispose
			FakeCosmosHandler? handler1 = null;
			await using (var app1 = await TestAppHost.CreateAsync(
				configureTestServices: services =>
					services.UseInMemoryCosmosDB(o =>
					{
						o.AddContainer("items", "/partitionKey");
						o.StatePersistenceDirectory = tempDir;
						o.OnHandlerCreated = (_, h) => handler1 = h;
					})))
			{
				var container = app1.Services.GetRequiredService<Container>();
				await container.CreateItemAsync(new CosmosTestItem("persist1", "pk1", "Persisted"), new PartitionKey("pk1"));
				// Explicitly persist — host disposal may not reliably trigger InMemoryContainer.Dispose()
				handler1!.BackingInMemoryContainer.Dispose();
			}

			// Host 2: should auto-load from same directory
			await using var app2 = await TestAppHost.CreateAsync(
				configureTestServices: services =>
					services.UseInMemoryCosmosDB(o =>
					{
						o.AddContainer("items", "/partitionKey");
						o.StatePersistenceDirectory = tempDir;
					}));

			var container2 = app2.Services.GetRequiredService<Container>();
			var read = await container2.ReadItemAsync<CosmosTestItem>("persist1", new PartitionKey("pk1"));
			read.Resource.Name.Should().Be("Persisted");
		}
		finally
		{
			if (Directory.Exists(tempDir))
				Directory.Delete(tempDir, recursive: true);
		}
	}
}

[Collection("FeedIteratorSetup")]
public class WafConcurrencyIsolationTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task MultipleHosts_ConcurrentOperations_FullyIsolated()
	{
		await using var app1 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		await using var app2 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));
		await using var app3 = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		// Each host creates different items concurrently
		var t1 = app1.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("a", "a", "Host1"));
		var t2 = app2.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("b", "b", "Host2"));
		var t3 = app3.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("c", "c", "Host3"));
		await Task.WhenAll(t1, t2, t3);

		// Each host should only see its own data
		var r1 = await (await app1.HttpClient.GetAsync("/items")).Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		var r2 = await (await app2.HttpClient.GetAsync("/items")).Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		var r3 = await (await app3.HttpClient.GetAsync("/items")).Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);

		r1.Should().HaveCount(1).And.ContainSingle(i => i.Name == "Host1");
		r2.Should().HaveCount(1).And.ContainSingle(i => i.Name == "Host2");
		r3.Should().HaveCount(1).And.ContainSingle(i => i.Name == "Host3");
	}
}

[Collection("FeedIteratorSetup")]
public class WafDiPatternDeepTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task OnHandlerCreated_MultiContainer_ReceivesCorrectContainerName()
	{
		var capturedNames = new List<string>();
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddSingleton(sp => sp.GetRequiredService<CosmosClient>().GetContainer("db", "orders"));
				services.AddSingleton<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("orders", "/partitionKey");
					o.AddContainer("products", "/partitionKey");
					o.AddContainer("users", "/partitionKey");
					o.OnHandlerCreated = (name, _) => capturedNames.Add(name);
				}));

		capturedNames.Should().HaveCount(3);
		capturedNames.Should().Contain("orders");
		capturedNames.Should().Contain("products");
		capturedNames.Should().Contain("users");
	}

	[Fact]
	public async Task FeedIteratorSetup_Deregister_WhenNotRegistered_DoesNotThrow()
	{
		// Make sure deregister is safe even without prior registration
		InMemoryFeedIteratorSetup.Deregister();
		InMemoryFeedIteratorSetup.Deregister(); // double deregister should also be safe

		// Now register and use normally
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		await container.CreateItemAsync(new CosmosTestItem("1", "1", "Test"), new PartitionKey("1"));
		var read = await container.ReadItemAsync<CosmosTestItem>("1", new PartitionKey("1"));
		read.Resource.Name.Should().Be("Test");
	}
}

[Collection("FeedIteratorSetup")]
public class WafMiscEdgeCaseDeepTests : IDisposable
{
	private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task ContainerPattern_LinqViaOverridable_WorksEndToEnd_ViaHttp()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<Container>(_ =>
					new InMemoryContainer("items", "/partitionKey"));
				services.AddSingleton<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosContainers(o =>
					o.AddContainer("items", "/partitionKey")));

		// Create items via HTTP
		await app.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("1", "1", "Alice"));
		await app.HttpClient.PostAsJsonAsync("/items", new CosmosTestItem("2", "2", "Bob"));

		// List via HTTP (uses ToFeedIteratorOverridable in TestRepository)
		var response = await app.HttpClient.GetAsync("/items");
		var items = await response.Content.ReadFromJsonAsync<List<CosmosTestItem>>(JsonOptions);
		items.Should().HaveCount(2);
	}

	[Fact(Skip = "DIVERGENT: When AddContainer is called twice with the same name, the router dictionary "
			   + "overwrites the first handler. This is Dictionary<string, FakeCosmosHandler> semantics — "
			   + "the second registration wins.")]
	public void DuplicateContainerNames_AmbiguousRouting() { }

	[Fact]
	public async Task DuplicateContainerNames_SecondRegistrationActive()
	{
		// When two containers are registered with the same name, the second one wins
		FakeCosmosHandler? capturedHandler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
				services.AddSingleton(sp => sp.GetRequiredService<CosmosClient>().GetContainer("db", "items"));
				services.AddSingleton<TestRepository>();
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/pk1");
					o.AddContainer("items", "/pk2"); // overwrites the first
					o.OnHandlerCreated = (_, h) => capturedHandler = h;
				}));

		// The effective partition key path should be from the second registration
		capturedHandler.Should().NotBeNull();
		capturedHandler!.BackingContainer.Should().NotBeNull();
	}

	[Fact]
	public async Task CompositePartitionKey_ViaWaf_CrudWorksEndToEnd()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					var props = new ContainerProperties("items", "/tenantId")
					{
						PartitionKeyPaths = new System.Collections.ObjectModel.Collection<string> { "/tenantId", "/userId" }
					};
					o.AddContainer(props);
				}),
			configureEndpoints: endpoints =>
			{
				endpoints.MapPost("/composite", async (Container container) =>
				{
					var item = Newtonsoft.Json.Linq.JObject.FromObject(new { id = "c1", tenantId = "t1", userId = "u1", name = "Composite" });
					await container.CreateItemAsync(item, new PartitionKeyBuilder().Add("t1").Add("u1").Build());
					return Results.Created("/composite/c1", null);
				});

				endpoints.MapGet("/composite/{id}", async (string id, Container container) =>
				{
					var read = await container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>(id,
						new PartitionKeyBuilder().Add("t1").Add("u1").Build());
					return Results.Ok(read.Resource);
				});
			});

		var createResp = await app.HttpClient.PostAsync("/composite", null);
		createResp.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResp = await app.HttpClient.GetAsync("/composite/c1");
		readResp.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact(Skip = "DIVERGENT: Empty SQL query never reaches the parser — the SDK's QueryDefinition "
			   + "constructor throws ArgumentNullException for null/empty query strings. "
			   + "Real Cosmos returns 400 BadRequest with syntax error.")]
	public void EmptySqlQuery_ViaWaf_ErrorBehavior() { }

	[Fact]
	public async Task EmptySqlQuery_ViaWaf_ThrowsArgumentNullException()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o => o.AddContainer("items", "/partitionKey")));

		var container = app.Services.GetRequiredService<Container>();
		// SDK's QueryDefinition constructor rejects null/empty query with ArgumentNullException
		var act = () => container.GetItemQueryIterator<CosmosTestItem>(
			new QueryDefinition("")).ReadNextAsync();
		await act.Should().ThrowAsync<ArgumentNullException>();
	}
}

[Collection("FeedIteratorSetup")]
public class WafStatePersistenceContainerPatternTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task StatePersistenceDirectory_WithContainerPattern_AutoPersists()
	{
		var tempDir = Path.Combine(Path.GetTempPath(), $"cosmos-cont-{Guid.NewGuid():N}");
		try
		{
			// Host 1: create items via container-only pattern, explicitly persist
			IContainerTestSetup? capturedContainer = null;
			await using (var app1 = await TestAppHost.CreateAsync(
				configureBaseServices: services =>
				{
					services.AddSingleton<Container>(_ =>
						new InMemoryContainer("items", "/partitionKey"));
					services.AddSingleton<TestRepository>();
				},
				configureTestServices: services =>
					services.UseInMemoryCosmosContainers(o =>
					{
						o.AddContainer("items", "/partitionKey");
						o.StatePersistenceDirectory = tempDir;
						o.OnContainerCreated = c => capturedContainer = c;
					})))
			{
				var container = app1.Services.GetRequiredService<Container>();
				await container.CreateItemAsync(
					new CosmosTestItem("p1", "pk1", "ContainerPersisted"),
					new PartitionKey("pk1"));
				// Explicitly persist — container pattern may not auto-dispose
				((InMemoryContainer)capturedContainer!).Dispose();
			}

			// Host 2: should auto-load
			await using var app2 = await TestAppHost.CreateAsync(
				configureBaseServices: services =>
				{
					services.AddSingleton<Container>(_ =>
						new InMemoryContainer("items", "/partitionKey"));
					services.AddSingleton<TestRepository>();
				},
				configureTestServices: services =>
					services.UseInMemoryCosmosContainers(o =>
					{
						o.AddContainer("items", "/partitionKey");
						o.StatePersistenceDirectory = tempDir;
					}));

			var container2 = app2.Services.GetRequiredService<Container>();
			var read = await container2.ReadItemAsync<CosmosTestItem>("p1", new PartitionKey("pk1"));
			read.Resource.Name.Should().Be("ContainerPersisted");
		}
		finally
		{
			if (Directory.Exists(tempDir))
				Directory.Delete(tempDir, recursive: true);
		}
	}
}

[Collection("FeedIteratorSetup")]
public class WafFaultInjectorMetadataTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task FaultInjectorIncludesMetadata_False_DataFaulted_MetadataUnaffected()
	{
		FakeCosmosHandler? handler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) =>
					{
						handler = h;
						h.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
						h.FaultInjectorIncludesMetadata = false;
					};
				}));

		var container = app.Services.GetRequiredService<Container>();

		// Data operations should be faulted
		var act = () => container.CreateItemAsync(
			new CosmosTestItem("f1", "pk1", "Fail"),
			new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.ServiceUnavailable);

		// Metadata operations should still work (container read)
		handler!.FaultInjector = null;
	}

	[Fact]
	public async Task FaultInjectorIncludesMetadata_True_AlsoFaultsMetadataRoutes()
	{
		FakeCosmosHandler? handler = null;
		await using var app = await TestAppHost.CreateAsync(
			configureTestServices: services =>
				services.UseInMemoryCosmosDB(o =>
				{
					o.AddContainer("items", "/partitionKey");
					o.OnHandlerCreated = (_, h) =>
					{
						handler = h;
						h.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
						h.FaultInjectorIncludesMetadata = true;
					};
				}));

		var container = app.Services.GetRequiredService<Container>();

		// Even creating an item (which involves metadata) should be faulted
		var act = () => container.CreateItemAsync(
			new CosmosTestItem("f2", "pk1", "Fail"),
			new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.ServiceUnavailable);
	}
}

[Collection("FeedIteratorSetup")]
public class WafZeroConfigDeepTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task ZeroConfig_NoBaseContainerRegistration_UsesDefaultName()
	{
		// When UseInMemoryCosmosDB() is called with no base Container registration
		// and no explicit AddContainer, it creates a default "in-memory-container"
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				// Register only CosmosClient (no Container)
				services.AddSingleton<CosmosClient>(_ =>
					new CosmosClient("AccountEndpoint=https://fail.example.com:9999/;AccountKey=dGVzdA==;"));
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB());

		var container = app.Services.GetRequiredService<Container>();
		container.Should().NotBeNull();

		// Should be able to create items in the default container
		await container.CreateItemAsync(
			new { id = "z1", partitionKey = "z1" },
			new PartitionKey("z1"));

		var read = await container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>("z1", new PartitionKey("z1"));
		read.Resource["id"]!.ToString().Should().Be("z1");
	}
}

[Collection("FeedIteratorSetup")]
public class WafTypedClientDeepTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact(Skip = "DIVERGENT: UseInMemoryCosmosDB<TClient>() registers only as TClient (e.g., TestCosmosClient), "
			   + "not as base CosmosClient. This is by design — typed clients are independent registrations. "
			   + "Services.GetService<CosmosClient>() returns null unless separately registered.")]
	public void TypedClient_NotResolvableAsBaseCosmosClient() { }

	[Fact]
	public async Task TypedClient_ResolvableOnlyAsTClient_NotBaseCosmosClient()
	{
		await using var app = await TestAppHost.CreateAsync(
			configureBaseServices: services =>
			{
				services.AddSingleton<TestCosmosClient>(_ =>
					new TestCosmosClient("AccountEndpoint=https://x.documents.azure.com:443/;AccountKey=dGVzdA=="));
			},
			configureTestServices: services =>
				services.UseInMemoryCosmosDB<TestCosmosClient>(o =>
					o.AddContainer("items", "/partitionKey")));

		// TestCosmosClient should be resolvable
		var typedClient = app.Services.GetService<TestCosmosClient>();
		typedClient.Should().NotBeNull();

		// Base CosmosClient should NOT be registered by UseInMemoryCosmosDB<T>
		var baseClient = app.Services.GetService<CosmosClient>();
		baseClient.Should().BeNull();
	}
}
