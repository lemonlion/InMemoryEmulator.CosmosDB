using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// Phase 1: UseInMemoryCosmosContainers
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class UseInMemoryCosmosContainersTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void Default_RegistersSingleContainer()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers();

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
		container.Id.Should().Be("in-memory-container");
	}

	[Fact]
	public void Container_IsRealSdkContainer_NotInMemoryContainer()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers(o => o.AddContainer("orders", "/pk"));

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		// 4.0: UseInMemoryCosmosContainers now returns real SDK Container backed by FakeCosmosHandler
		container.Should().NotBeOfType<InMemoryContainer>();
		container.Id.Should().Be("orders");
	}

	[Fact]
	public async Task Container_SupportsCrud_ThroughSdkPipeline()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers(o => o.AddContainer("orders", "/pk"));

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// Real SDK pipeline — create and read back
		await container.CreateItemAsync(new { id = "1", pk = "a" }, new PartitionKey("a"));
		var response = await container.ReadItemAsync<dynamic>("1", new PartitionKey("a"));
		((string)response.Resource.id).Should().Be("1");
	}

	[Fact]
	public async Task Container_ToFeedIterator_WorksNatively()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers(o => o.AddContainer("orders", "/pk"));

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		await container.CreateItemAsync(new { id = "1", pk = "a" }, new PartitionKey("a"));

		// .ToFeedIterator() works via FakeCosmosHandler — no ProductionExtensions needed
		var query = container.GetItemLinqQueryable<dynamic>().ToFeedIterator();
		var results = new List<dynamic>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().ContainSingle();
	}

	[Fact]
	public void RemovesExistingContainerRegistration()
	{
		var services = new ServiceCollection();
		services.AddSingleton<Container>(new InMemoryContainer("old-container", "/old"));

		services.UseInMemoryCosmosContainers();

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Id.Should().Be("in-memory-container");
	}

	[Fact]
	public void DoesNotRemoveCosmosClient()
	{
		var services = new ServiceCollection();
		var originalClient = new InMemoryCosmosClient();
		services.AddSingleton<CosmosClient>(originalClient);

		services.UseInMemoryCosmosContainers();

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		client.Should().BeSameAs(originalClient);
	}

	[Fact]
	public void CustomContainerName()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers(o => o.AddContainer("orders"));

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Id.Should().Be("orders");
	}

	[Fact]
	public void OnHandlerCreatedCallback()
	{
		var services = new ServiceCollection();
		FakeCosmosHandler? captured = null;

		services.UseInMemoryCosmosContainers(o =>
		{
			o.AddContainer("orders", "/pk");
			o.OnHandlerCreated = (_, h) => captured = h;
		});

		captured.Should().NotBeNull();
		captured!.BackingContainer.Id.Should().Be("orders");
	}

	[Fact]
	public void OnHandlerCreated_FiresForEachContainer()
	{
		var services = new ServiceCollection();
		var captured = new Dictionary<string, FakeCosmosHandler>();

		services.UseInMemoryCosmosContainers(o =>
		{
			o.OnHandlerCreated = (name, h) => captured[name] = h;
			o.AddContainer("orders", "/pk");
			o.AddContainer("events", "/partitionKey");
			o.AddContainer("logs", "/category");
		});

		captured.Should().HaveCount(3);
		captured.Keys.Should().BeEquivalentTo(["orders", "events", "logs"]);
	}

	[Fact]
	public async Task FaultInjection_ViaOnHandlerCreated()
	{
		var services = new ServiceCollection();
		FakeCosmosHandler? handler = null;

		services.UseInMemoryCosmosContainers(o =>
		{
			o.AddContainer("orders", "/pk");
			o.OnHandlerCreated = (_, h) => handler = h;
		});

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		handler!.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);

		var act = () => container.ReadItemAsync<dynamic>("1", new PartitionKey("a"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.ServiceUnavailable);
	}

	[Fact]
	public void WithHttpMessageHandlerWrapper()
	{
		var services = new ServiceCollection();
		var wrapperInvoked = false;

		services.UseInMemoryCosmosContainers(o =>
		{
			o.AddContainer("orders", "/pk");
			o.WithHttpMessageHandlerWrapper(h =>
			{
				wrapperInvoked = true;
				return h; // pass-through
			});
		});

		wrapperInvoked.Should().BeTrue();
	}

	[Fact]
	public void OnContainerCreatedCallback_StillWorks()
	{
		var services = new ServiceCollection();
		InMemoryContainer? captured = null;

		services.UseInMemoryCosmosContainers(o =>
		{
			o.AddContainer("orders", "/pk");
			o.OnContainerCreated = c => captured = (InMemoryContainer)c;
		});

		captured.Should().NotBeNull();
		captured!.Id.Should().Be("orders");
	}

	[Fact]
	public void MatchesExistingLifetime_Scoped()
	{
		var services = new ServiceCollection();
		services.AddScoped<Container>(_ => new InMemoryContainer("old", "/id"));

		services.UseInMemoryCosmosContainers();

		var descriptor = services.First(d => d.ServiceType == typeof(Container));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
	}

	[Fact]
	public void MultipleContainers()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers(o =>
		{
			o.AddContainer("orders", "/pk");
			o.AddContainer("events", "/partitionKey");
			o.AddContainer("logs", "/category");
		});

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		containers.Should().HaveCount(3);
		containers.Select(c => c.Id).Should().BeEquivalentTo(["orders", "events", "logs"]);
	}

	[Fact]
	public void Idempotent_CalledTwice()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers();
		services.UseInMemoryCosmosContainers();

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		containers.Should().ContainSingle();
	}

	[Fact]
	public void MatchesExistingLifetime_Transient()
	{
		var services = new ServiceCollection();
		services.AddTransient<Container>(_ => new InMemoryContainer("old", "/id"));

		services.UseInMemoryCosmosContainers();

		var descriptor = services.First(d => d.ServiceType == typeof(Container));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
	}

	[Fact]
	public void MatchesExistingLifetime_DefaultsSingleton()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers();

		var descriptor = services.First(d => d.ServiceType == typeof(Container));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
	}

	[Fact]
	public void ReturnsSameServiceCollection_Fluent()
	{
		var services = new ServiceCollection();

		var result = services.UseInMemoryCosmosContainers();

		result.Should().BeSameAs(services);
	}

	[Fact]
	public void CustomDatabaseName()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers(o =>
		{
			o.DatabaseName = "TestDb";
			o.AddContainer("orders", "/pk");
		});

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 2: UseInMemoryCosmosDB
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class UseInMemoryCosmosDBTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void Default_RegistersCosmosClient()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		client.Should().NotBeNull();
	}

	[Fact]
	public void Default_RegistersContainer()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
		container.Id.Should().Be("in-memory-container");
	}

	[Fact]
	public void RemovesExistingCosmosClient()
	{
		var services = new ServiceCollection();
		// Register a factory that would fail if ever invoked
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should have been removed"));

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		client.Should().NotBeNull();
	}

	[Fact]
	public void RemovesExistingContainer()
	{
		var services = new ServiceCollection();
		services.AddSingleton<Container>(_ =>
			throw new InvalidOperationException("Should have been removed"));

		// Explicit AddContainer removes existing registrations
		services.UseInMemoryCosmosDB(o => o.AddContainer("orders", "/pk"));

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
		container.Id.Should().Be("orders");
	}

	[Fact]
	public void ContainerIsFromClient()
	{
		var services = new ServiceCollection();
		FakeCosmosHandler? capturedHandler = null;

		services.UseInMemoryCosmosDB(o =>
		{
			o.DatabaseName = "mydb";
			o.AddContainer("orders", "/pk");
			o.OnHandlerCreated = (_, h) => capturedHandler = h;
		});

		var provider = services.BuildServiceProvider();
		var diContainer = provider.GetRequiredService<Container>();

		// DI container and client both route through the same FakeCosmosHandler
		diContainer.Id.Should().Be("orders");
		capturedHandler.Should().NotBeNull();
		capturedHandler!.BackingContainer.Id.Should().Be("orders");
	}

	[Fact]
	public void CustomDatabaseName()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB(o => o.DatabaseName = "TestDb");

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		var db = client.GetDatabase("TestDb");
		db.Id.Should().Be("TestDb");
	}

	[Fact]
	public void DefaultDatabaseName_IsInMemoryDb()
	{
		var services = new ServiceCollection();
		FakeCosmosHandler? capturedHandler = null;

		services.UseInMemoryCosmosDB(o =>
		{
			o.OnHandlerCreated = (_, h) => capturedHandler = h;
		});

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// The default database and container names are used
		container.Id.Should().Be("in-memory-container");
		capturedHandler.Should().NotBeNull();
		capturedHandler!.BackingContainer.Id.Should().Be("in-memory-container");
	}

	[Fact]
	public void MultipleContainers()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/pk");
			o.AddContainer("events", "/partitionKey");
		});

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		containers.Should().HaveCount(2);
		containers.Select(c => c.Id).Should().BeEquivalentTo(["orders", "events"]);
	}

	[Fact]
	public void DoesNotNeedFeedIteratorSetup()
	{
		InMemoryFeedIteratorSetup.Deregister();
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB();

		// FakeCosmosHandler handles .ToFeedIterator() natively — no setup needed
		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull();
	}

	[Fact]
	public void OnClientCreatedCallback()
	{
		var services = new ServiceCollection();
		CosmosClient? captured = null;

		services.UseInMemoryCosmosDB(o => o.OnClientCreated = c => captured = c);

		captured.Should().NotBeNull();
	}

	[Fact]
	public void IsSuperset_ContainerBehavior()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should have been removed"));
		services.AddSingleton<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("db", "items"));

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		provider.GetRequiredService<CosmosClient>().Should().NotBeNull();
		// Auto-detect: existing factory resolves against the in-memory client
		provider.GetRequiredService<Container>().Should().NotBeNull();
	}

	[Fact]
	public void ContainerPerDatabaseOverride()
	{
		var services = new ServiceCollection();
		var capturedHandlers = new Dictionary<string, FakeCosmosHandler>();

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("c1", "/pk", databaseName: "db1");
			o.AddContainer("c2", "/pk", databaseName: "db2");
			o.OnHandlerCreated = (name, h) => capturedHandlers[name] = h;
		});

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();

		containers.Should().HaveCount(2);
		containers.Select(c => c.Id).Should().BeEquivalentTo(["c1", "c2"]);

		// Each container backed by its own FakeCosmosHandler
		capturedHandlers.Should().HaveCount(2);
		capturedHandlers["c1"].BackingContainer.Id.Should().Be("c1");
		capturedHandlers["c2"].BackingContainer.Id.Should().Be("c2");
	}

	[Fact]
	public void MatchesExistingContainerLifetime()
	{
		var services = new ServiceCollection();
		services.AddScoped<Container>(_ => new InMemoryContainer("old", "/id"));

		services.UseInMemoryCosmosDB();

		var descriptor = services.First(d => d.ServiceType == typeof(Container));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
	}

	[Fact]
	public void MatchesExistingLifetime_Transient()
	{
		var services = new ServiceCollection();
		services.AddTransient<Container>(_ => new InMemoryContainer("old", "/id"));

		services.UseInMemoryCosmosDB();

		var descriptor = services.First(d => d.ServiceType == typeof(Container));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
	}

	[Fact]
	public void MatchesExistingLifetime_DefaultsSingleton()
	{
		var services = new ServiceCollection();
		// No existing Container registration — defaults to Singleton

		services.UseInMemoryCosmosDB();

		var descriptor = services.First(d => d.ServiceType == typeof(Container));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
	}

	[Fact]
	public void Idempotent_CalledTwice()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB();
		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		containers.Should().ContainSingle();
		var clients = provider.GetServices<CosmosClient>().ToList();
		clients.Should().ContainSingle();
	}

	[Fact]
	public void OnHandlerCreatedCallback_SingleContainer()
	{
		var services = new ServiceCollection();
		string? capturedName = null;
		FakeCosmosHandler? capturedHandler = null;

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/pk");
			o.OnHandlerCreated = (name, handler) =>
			{
				capturedName = name;
				capturedHandler = handler;
			};
		});

		capturedName.Should().Be("orders");
		capturedHandler.Should().NotBeNull();
		capturedHandler!.BackingContainer.Id.Should().Be("orders");
	}

	[Fact]
	public void DuplicateContainerNames_LastHandlerWins()
	{
		// When duplicate container names are used with UseInMemoryCosmosDB,
		// the handlers dictionary uses the container name as key — the last one wins.
		// Both are still registered in DI as separate Container instances, but
		// the FakeCosmosHandler routing only sees the last one.
		var services = new ServiceCollection();
		var capturedHandlers = new List<(string Name, FakeCosmosHandler Handler)>();

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/pk");
			o.AddContainer("orders", "/pk2");
			o.OnHandlerCreated = (name, h) => capturedHandlers.Add((name, h));
		});

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		// Both containers are registered in DI
		containers.Should().HaveCount(2);

		// OnHandlerCreated fires for each, but second overwrites first in the handler dictionary
		capturedHandlers.Should().HaveCount(2);
	}

	[Fact]
	public void CosmosClientAlwaysSingleton_EvenIfExistingWasScoped()
	{
		var services = new ServiceCollection();
		services.AddScoped<CosmosClient>(_ =>
			throw new InvalidOperationException("Should have been removed"));

		services.UseInMemoryCosmosDB();

		// CosmsosClient is always registered as Singleton regardless of existing registration
		var descriptor = services.First(d => d.ServiceType == typeof(CosmosClient));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
	}

	[Fact]
	public void ReturnsSameServiceCollection_Fluent()
	{
		var services = new ServiceCollection();

		var result = services.UseInMemoryCosmosDB();

		result.Should().BeSameAs(services);
	}

	[Fact]
	public void OnClientCreated_ReturnsInstanceDIResolves()
	{
		var services = new ServiceCollection();
		CosmosClient? captured = null;

		services.UseInMemoryCosmosDB(o => o.OnClientCreated = c => captured = c);

		var provider = services.BuildServiceProvider();
		var resolved = provider.GetRequiredService<CosmosClient>();

		// The callback receives the exact same instance that DI will resolve
		captured.Should().BeSameAs(resolved);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 3: Edge Cases
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionExtensionEdgeCaseTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void UseInMemoryCosmosDB_NoExistingRegistrations_StillWorks()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		provider.GetRequiredService<CosmosClient>().Should().NotBeNull();
		provider.GetRequiredService<Container>().Should().NotBeNull();
	}

	[Fact]
	public void UseInMemoryCosmosContainers_NoExistingRegistrations_StillWorks()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers();

		var provider = services.BuildServiceProvider();
		provider.GetRequiredService<Container>().Should().NotBeNull();
	}

	[Fact]
	public async Task UseInMemoryCosmosDB_ContainerUsableForCrud()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o => o.AddContainer("test", "/partitionKey"));
		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// Create
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await container.CreateItemAsync(item, new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		// Read
		var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task UseInMemoryCosmosContainers_ContainerUsableForCrud()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers(o => o.AddContainer("test", "/partitionKey"));
		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// Create
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await container.CreateItemAsync(item, new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		// Read
		var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task UseInMemoryCosmosDB_ContainerUsableForLinqQuery()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o => o.AddContainer("test", "/partitionKey"));
		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// Seed
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		// Query via LINQ + ToFeedIteratorOverridable
		var iterator = container.GetItemLinqQueryable<TestDocument>(true)
			.Where(d => d.PartitionKey == "pk1")
			.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
	}

	[Fact]
	public void UseInMemoryCosmosDB_ClientGetContainerReturnsRegisteredContainer()
	{
		var services = new ServiceCollection();
		FakeCosmosHandler? capturedHandler = null;
		services.UseInMemoryCosmosDB(o =>
		{
			o.DatabaseName = "db";
			o.AddContainer("orders", "/pk");
			o.OnHandlerCreated = (_, h) => capturedHandler = h;
		});
		var provider = services.BuildServiceProvider();

		var client = provider.GetRequiredService<CosmosClient>();
		var diContainer = provider.GetRequiredService<Container>();
		var clientContainer = client.GetContainer("db", "orders");

		// Both route through the same FakeCosmosHandler backing
		diContainer.Id.Should().Be("orders");
		clientContainer.Id.Should().Be("orders");
		capturedHandler.Should().NotBeNull();
		capturedHandler!.BackingContainer.Id.Should().Be("orders");
	}

	[Fact]
	public void BothMethodsCalled_UseInMemoryCosmosDB_ThenContainers()
	{
		// When UseInMemoryCosmosDB is called first and UseInMemoryCosmosContainers second,
		// the Container registration from UseInMemoryCosmosDB gets removed and replaced.
		// CosmosClient from UseInMemoryCosmosDB remains.
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB(o => o.AddContainer("fromDB", "/pk"));
		services.UseInMemoryCosmosContainers(o => o.AddContainer("fromContainers", "/pk"));

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		containers.Should().ContainSingle();
		containers[0].Id.Should().Be("fromContainers");
		// CosmosClient from UseInMemoryCosmosDB should still be present
		provider.GetRequiredService<CosmosClient>().Should().NotBeNull();
	}

	[Fact]
	public void BothMethodsCalled_UseInMemoryCosmosContainers_ThenCosmosDB()
	{
		// When UseInMemoryCosmosContainers is called first and UseInMemoryCosmosDB second,
		// Container from Containers gets removed and replaced by DB's container.
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers(o => o.AddContainer("fromContainers", "/pk"));
		services.UseInMemoryCosmosDB(o => o.AddContainer("fromDB", "/pk"));

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		containers.Should().ContainSingle();
		containers[0].Id.Should().Be("fromDB");
	}

	[Fact]
	public async Task ScopedLifetime_DisposalWorks()
	{
		var services = new ServiceCollection();
		services.AddScoped<Container>(_ => new InMemoryContainer("old", "/id"));

		services.UseInMemoryCosmosDB(o => o.AddContainer("test", "/partitionKey"));

		var provider = services.BuildServiceProvider();

		Container container;
		using (var scope = provider.CreateScope())
		{
			container = scope.ServiceProvider.GetRequiredService<Container>();
			container.Should().NotBeNull();
			container.Id.Should().Be("test");

			// Container is usable within the scope
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Scoped" },
				new PartitionKey("pk1"));
		}

		// After scope disposal, a new scope should get a fresh factory invocation
		using var scope2 = provider.CreateScope();
		var container2 = scope2.ServiceProvider.GetRequiredService<Container>();
		container2.Should().NotBeNull();
	}

	[Fact]
	public void FeedIteratorSetup_Asymmetry_UseInMemoryCosmosDB_DoesNotRegister()
	{
		// UseInMemoryCosmosDB does NOT register FeedIteratorSetup (FakeCosmosHandler handles it natively)
		InMemoryFeedIteratorSetup.Deregister();
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB();

		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull(
			"UseInMemoryCosmosDB uses FakeCosmosHandler which handles .ToFeedIterator() natively");
	}

	[Fact]
	public void FeedIteratorSetup_Asymmetry_UseInMemoryCosmosContainers_DoesNotRegister()
	{
		// v4.0: UseInMemoryCosmosContainers now uses FakeCosmosHandler — no FeedIteratorSetup needed
		InMemoryFeedIteratorSetup.Deregister();
		var services = new ServiceCollection();

		services.UseInMemoryCosmosContainers();

		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull(
			"UseInMemoryCosmosContainers uses FakeCosmosHandler which handles .ToFeedIterator() natively");
	}

	[Fact]
	public void FeedIteratorSetup_NotRegistered_TypedClient()
	{
		// UseInMemoryCosmosDB<T> now uses FakeCosmosHandler, so FeedIteratorSetup is not needed
		InMemoryFeedIteratorSetup.Deregister();
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull(
			"UseInMemoryCosmosDB<T> uses FakeCosmosHandler which handles .ToFeedIterator() natively");
	}

	[Fact]
	public void FluentChaining_InMemoryCosmosOptions_AddContainer()
	{
		var options = new InMemoryCosmosOptions();

		var result = options.AddContainer("a", "/pk").AddContainer("b", "/pk2");

		result.Should().BeSameAs(options);
		options.Containers.Should().HaveCount(2);
		options.Containers[0].ContainerName.Should().Be("a");
		options.Containers[1].ContainerName.Should().Be("b");
	}

	[Fact]
	public void FluentChaining_InMemoryContainerOptions_AddContainer()
	{
		var options = new InMemoryContainerOptions();

		var result = options.AddContainer("a", "/pk").AddContainer("b", "/pk2");

		result.Should().BeSameAs(options);
		options.Containers.Should().HaveCount(2);
		options.Containers[0].ContainerName.Should().Be("a");
		options.Containers[1].ContainerName.Should().Be("b");
	}

	[Fact]
	public void ContainerConfig_RecordEquality()
	{
		var a = new ContainerConfig("orders", "/pk", "db1");
		var b = new ContainerConfig("orders", "/pk", "db1");
		var c = new ContainerConfig("events", "/pk", "db1");

		a.Should().Be(b);
		a.Should().NotBe(c);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Pattern 2: Typed CosmosClient Subclasses (UseInMemoryCosmosDB<TClient>)
// ════════════════════════════════════════════════════════════════════════════════

// Simulated PRODUCTION typed clients (extend CosmosClient, NOT InMemoryCosmosClient).
// These mirror the real SCA.Common pattern — each domain gets its own CosmosClient subclass.
// With the new Castle.Core proxy approach, these are used DIRECTLY in UseInMemoryCosmosDB<T>()
// — no shadow types needed.
public class EmployeeCosmosClient : CosmosClient
{
	public EmployeeCosmosClient(string connectionString, CosmosClientOptions? options = null)
		: base(connectionString, options) { }
}

public class CustomerCosmosClient : CosmosClient
{
	public CustomerCosmosClient(string connectionString, CosmosClientOptions? options = null)
		: base(connectionString, options) { }
}

// Simulated repos that take typed clients — exactly as in production code.
public class BiometricRepository
{
	public Container Container { get; }

	public BiometricRepository(EmployeeCosmosClient client)
	{
		Container = client.GetContainer("BiometricDb", "biometrics");
	}
}

public class OOBRepository
{
	public Container Container { get; }

	public OOBRepository(CustomerCosmosClient client)
	{
		Container = client.GetContainer("OOBDb", "oob-events");
	}
}

[Collection("FeedIteratorSetup")]
public class UseInMemoryTypedCosmosDBTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void RegistersTypedClient_ResolvableFromDI()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
			o.AddContainer("biometrics", "/partitionKey"));

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		client.Should().BeAssignableTo<EmployeeCosmosClient>();
	}

	[Fact]
	public void RemovesExistingTypedRegistration()
	{
		var services = new ServiceCollection();
		services.AddSingleton<EmployeeCosmosClient>(_ =>
			throw new InvalidOperationException("Should have been removed"));

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
			o.AddContainer("biometrics", "/partitionKey"));

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		client.Should().NotBeNull();
	}

	[Fact]
	public async Task DefaultDatabase_IsInMemoryDb()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("in-memory-db", "in-memory-container");
		container.Should().NotBeNull();

		// Verify it's functional (backed by FakeCosmosHandler)
		var item = new TestDocument { Id = "1", PartitionKey = "1", Name = "Test" };
		var response = await container.CreateItemAsync(item, new PartitionKey("1"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CustomDatabaseName()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.DatabaseName = "BiometricDb";
			o.AddContainer("biometrics", "/pk");
		});

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("BiometricDb", "biometrics");
		container.Should().NotBeNull();

		// Verify it's functional
		var response = await container.CreateItemAsync(
			new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public void MultipleTypedClients_Independent()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.DatabaseName = "BiometricDb";
			o.AddContainer("biometrics", "/pk");
		});
		services.UseInMemoryCosmosDB<CustomerCosmosClient>(o =>
		{
			o.DatabaseName = "OOBDb";
			o.AddContainer("oob-events", "/pk");
		});

		var provider = services.BuildServiceProvider();
		var bioClient = provider.GetRequiredService<EmployeeCosmosClient>();
		var oobClient = provider.GetRequiredService<CustomerCosmosClient>();

		bioClient.Should().NotBeSameAs(oobClient);
	}

	[Fact]
	public async Task TypedClient_ContainerUsableForCrud()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.DatabaseName = "BiometricDb";
			o.AddContainer("biometrics", "/partitionKey");
		});

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("BiometricDb", "biometrics");

		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Fingerprint" };
		var createResponse = await container.CreateItemAsync(item, new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Fingerprint");
	}

	[Fact]
	public void RepositoryPattern_Constructor_ResolvesTypedClient()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.DatabaseName = "BiometricDb";
			o.AddContainer("biometrics", "/partitionKey");
		});
		services.AddTransient<BiometricRepository>();

		var provider = services.BuildServiceProvider();
		var repo = provider.GetRequiredService<BiometricRepository>();

		repo.Container.Should().NotBeNull();
		repo.Container.Id.Should().Be("biometrics");
	}

	[Fact]
	public void MultipleTypedClients_WithRepos_IsolatedData()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.DatabaseName = "BiometricDb";
			o.AddContainer("biometrics", "/partitionKey");
		});
		services.UseInMemoryCosmosDB<CustomerCosmosClient>(o =>
		{
			o.DatabaseName = "OOBDb";
			o.AddContainer("oob-events", "/partitionKey");
		});
		services.AddTransient<BiometricRepository>();
		services.AddTransient<OOBRepository>();

		var provider = services.BuildServiceProvider();
		var bioRepo = provider.GetRequiredService<BiometricRepository>();
		var oobRepo = provider.GetRequiredService<OOBRepository>();

		// Each repo's container should be independent
		bioRepo.Container.Should().NotBeSameAs(oobRepo.Container);
		bioRepo.Container.Id.Should().Be("biometrics");
		oobRepo.Container.Id.Should().Be("oob-events");
	}

	[Fact]
	public void DoesNotRegisterAsBaseCosmosClient()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		var provider = services.BuildServiceProvider();
		// Should NOT be resolvable as base CosmosClient unless explicitly registered
		var baseClient = provider.GetService<CosmosClient>();
		baseClient.Should().BeNull();
	}

	[Fact]
	public void DoesNotRegisterContainer_InDI()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o => o.AddContainer("bio", "/pk"));

		var provider = services.BuildServiceProvider();
		// Pattern 2 never has Container in DI — repos call client.GetContainer()
		var container = provider.GetService<Container>();
		container.Should().BeNull();
	}

	[Fact]
	public void FeedIteratorSetup_NotNeeded_TypedClient()
	{
		InMemoryFeedIteratorSetup.Deregister();
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		// FakeCosmosHandler handles .ToFeedIterator() natively — no FeedIteratorSetup needed
		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull(
			"UseInMemoryCosmosDB<T> now uses FakeCosmosHandler which handles .ToFeedIterator() natively");
	}

	[Fact]
	public void RegisterFeedIteratorSetupOption_Ignored_TypedClient()
	{
		InMemoryFeedIteratorSetup.Deregister();
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o => o.RegisterFeedIteratorSetup = true);

		// Even with RegisterFeedIteratorSetup = true, it's ignored — FakeCosmosHandler handles natively
		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull();
	}

	[Fact]
	public void OnClientCreatedCallback()
	{
		var services = new ServiceCollection();
		CosmosClient? captured = null;

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o => o.OnClientCreated = c => captured = c);

		// The callback receives a Castle proxy assignable to the typed client
		captured.Should().NotBeNull();
		captured.Should().BeAssignableTo<EmployeeCosmosClient>();
	}

	[Fact]
	public void MatchesExistingLifetime_Scoped()
	{
		var services = new ServiceCollection();
		services.AddScoped<EmployeeCosmosClient>(_ => new EmployeeCosmosClient("AccountEndpoint=https://x.documents.azure.com:443/;AccountKey=dGVzdA=="));

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		var descriptor = services.First(d => d.ServiceType == typeof(EmployeeCosmosClient));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
	}

	[Fact]
	public void MatchesExistingLifetime_DefaultsSingleton()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		var descriptor = services.First(d => d.ServiceType == typeof(EmployeeCosmosClient));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
	}

	[Fact]
	public async Task FullEndToEnd_MultipleTypedClients()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.DatabaseName = "BiometricDb";
			o.AddContainer("biometrics", "/partitionKey");
		});
		services.UseInMemoryCosmosDB<CustomerCosmosClient>(o =>
		{
			o.DatabaseName = "OOBDb";
			o.AddContainer("oob-events", "/partitionKey");
		});
		services.AddTransient<BiometricRepository>();
		services.AddTransient<OOBRepository>();

		var provider = services.BuildServiceProvider();

		// Write via biometric repo
		var bioClient = provider.GetRequiredService<EmployeeCosmosClient>();
		var bioContainer = bioClient.GetContainer("BiometricDb", "biometrics");
		await bioContainer.CreateItemAsync(
			new TestDocument { Id = "bio-1", PartitionKey = "pk1", Name = "Fingerprint" },
			new PartitionKey("pk1"));

		// Write via OOB repo
		var oobClient = provider.GetRequiredService<CustomerCosmosClient>();
		var oobContainer = oobClient.GetContainer("OOBDb", "oob-events");
		await oobContainer.CreateItemAsync(
			new TestDocument { Id = "oob-1", PartitionKey = "pk1", Name = "SMS" },
			new PartitionKey("pk1"));

		// Read back — each client's data is isolated
		var bioRead = await bioContainer.ReadItemAsync<TestDocument>("bio-1", new PartitionKey("pk1"));
		bioRead.Resource.Name.Should().Be("Fingerprint");

		var oobRead = await oobContainer.ReadItemAsync<TestDocument>("oob-1", new PartitionKey("pk1"));
		oobRead.Resource.Name.Should().Be("SMS");

		// Cross-contamination check: bio container should not have OOB data
		var act = () => bioContainer.ReadItemAsync<TestDocument>("oob-1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public void Idempotent_CalledTwice()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		var provider = services.BuildServiceProvider();
		var clients = provider.GetServices<EmployeeCosmosClient>().ToList();
		clients.Should().ContainSingle();
	}

	[Fact]
	public void MatchesExistingLifetime_Transient()
	{
		var services = new ServiceCollection();
		services.AddTransient<EmployeeCosmosClient>(_ => new EmployeeCosmosClient("AccountEndpoint=https://x.documents.azure.com:443/;AccountKey=dGVzdA=="));

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		var descriptor = services.First(d => d.ServiceType == typeof(EmployeeCosmosClient));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
	}

	[Fact]
	public async Task MultipleContainersOnSameTypedClient()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.DatabaseName = "BiometricDb";
			o.AddContainer("biometrics", "/pk");
			o.AddContainer("audit-logs", "/pk");
		});

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();

		// Both containers should be accessible via GetContainer
		var bioContainer = client.GetContainer("BiometricDb", "biometrics");
		bioContainer.Should().NotBeNull();
		bioContainer.Id.Should().Be("biometrics");

		var auditContainer = client.GetContainer("BiometricDb", "audit-logs");
		auditContainer.Should().NotBeNull();
		auditContainer.Id.Should().Be("audit-logs");

		// They should be independent — writing to one doesn't affect the other
		await bioContainer.CreateItemAsync(new { id = "1", pk = "a" }, new PartitionKey("a"));
		var act = () => auditContainer.ReadItemAsync<dynamic>("1", new PartitionKey("a"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task PerContainerDatabaseOverride_TypedClient()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.AddContainer("c1", "/pk", databaseName: "db1");
			o.AddContainer("c2", "/pk", databaseName: "db2");
		});

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();

		var c1 = client.GetContainer("db1", "c1");
		c1.Should().NotBeNull();
		c1.Id.Should().Be("c1");

		var c2 = client.GetContainer("db2", "c2");
		c2.Should().NotBeNull();
		c2.Id.Should().Be("c2");

		// Verify both are functional
		await c1.CreateItemAsync(new { id = "1", pk = "a" }, new PartitionKey("a"));
		await c2.CreateItemAsync(new { id = "1", pk = "a" }, new PartitionKey("a"));
	}

	[Fact]
	public void OnHandlerCreated_Fires_TypedClient()
	{
		var services = new ServiceCollection();
		var callbackInvoked = false;
		string? callbackContainerName = null;

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.OnHandlerCreated = (name, handler) =>
			{
				callbackInvoked = true;
				callbackContainerName = name;
			};
			o.AddContainer("bio", "/pk");
		});

		callbackInvoked.Should().BeTrue(
			"UseInMemoryCosmosDB<T> now uses FakeCosmosHandler, so OnHandlerCreated should fire");
		callbackContainerName.Should().Be("bio");
	}

	[Fact]
	public void HttpMessageHandlerWrapper_Invoked_TypedClient()
	{
		var services = new ServiceCollection();
		var wrapperInvoked = false;

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.HttpMessageHandlerWrapper = handler =>
			{
				wrapperInvoked = true;
				return handler;
			};
			o.AddContainer("bio", "/pk");
		});

		wrapperInvoked.Should().BeTrue(
			"UseInMemoryCosmosDB<T> now uses FakeCosmosHandler, so HttpMessageHandlerWrapper should fire");

		// Client should still work fine
		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		client.Should().NotBeNull();
	}

	[Fact]
	public void ReturnsSameServiceCollection_Fluent()
	{
		var services = new ServiceCollection();

		var result = services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		result.Should().BeSameAs(services);
	}

	[Fact]
	public async Task TypedClient_FaultInjection_Works()
	{
		var services = new ServiceCollection();
		FakeCosmosHandler? capturedHandler = null;

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.AddContainer("bio", "/partitionKey");
			o.OnHandlerCreated = (_, handler) =>
			{
				capturedHandler = handler;
				handler.FaultInjector = req =>
				{
					if (req.Method == HttpMethod.Post)
						return new HttpResponseMessage(HttpStatusCode.TooManyRequests);
					return null;
				};
			};
		});

		capturedHandler.Should().NotBeNull();

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("in-memory-db", "bio");

		var act = () => container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => (int)e.StatusCode == 429);
	}

	[Fact]
	public async Task TypedClient_QueryLogging_Works()
	{
		var services = new ServiceCollection();
		FakeCosmosHandler? capturedHandler = null;

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.AddContainer("bio", "/partitionKey");
			o.OnHandlerCreated = (_, handler) => capturedHandler = handler;
		});

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("in-memory-db", "bio");

		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.name = 'Test'"));
		while (iterator.HasMoreResults)
			await iterator.ReadNextAsync();

		capturedHandler!.QueryLog.Should().Contain("SELECT * FROM c WHERE c.name = 'Test'");
	}

	[Fact]
	public async Task TypedClient_LinqToFeedIterator_WorksNatively()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
			o.AddContainer("bio", "/partitionKey"));

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("in-memory-db", "bio");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		// LINQ .ToFeedIterator() should work without FeedIteratorSetup
		var iterator = container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "Alice")
			.ToFeedIterator();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task TypedClient_HierarchicalPkPrefix_ScopesCorrectly()
	{
		var services = new ServiceCollection();

		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
			o.AddContainer(new ContainerProperties("events", new List<string> { "/tenantId", "/category", "/region" })));

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("in-memory-db", "events");

		await container.CreateItemAsync(
			new { id = "1", tenantId = "t1", category = "A", region = "eu" },
			new PartitionKeyBuilder().Add("t1").Add("A").Add("eu").Build());
		await container.CreateItemAsync(
			new { id = "2", tenantId = "t2", category = "B", region = "us" },
			new PartitionKeyBuilder().Add("t2").Add("B").Add("us").Build());

		// Query with 1-component prefix PK should scope correctly
		var prefixPk = new PartitionKeyBuilder().Add("t1").Build();
		var iterator = container.GetItemQueryIterator<dynamic>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = prefixPk });

		var results = new List<dynamic>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 5: Auto-Detect Mode — UseInMemoryCosmosDB() with no explicit containers
// ════════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Tests for the auto-detect behavior: when <c>UseInMemoryCosmosDB()</c> is called
/// without explicit <c>AddContainer()</c> calls, existing <c>Container</c> factory
/// registrations are preserved. They naturally resolve against the in-memory client.
/// </summary>
[Collection("FeedIteratorSetup")]
public class AutoDetectContainerTests
{

	[Fact]
	public void AutoDetect_PreservesExistingContainerFactory()
	{
		var services = new ServiceCollection();

		// Simulate production: register CosmosClient + Container factory
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Production client should be replaced"));
		services.AddSingleton<Container>(sp =>
		{
			var client = sp.GetRequiredService<CosmosClient>();
			return client.GetContainer("ProductionDb", "orders");
		});

		// Zero-config swap — no AddContainer needed
		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		client.Should().NotBeNull();

		// Container resolves via the existing factory, which calls GetContainer on the real client
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
		container.Id.Should().Be("orders");
	}

	[Fact]
	public async Task AutoDetect_ContainerIsFullyFunctional()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));
		services.AddSingleton<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("MyDb", "items"));

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// CRUD works — the auto-created InMemoryContainer is fully usable
		var item = new TestDocument { Id = "1", PartitionKey = "1", Name = "AutoDetected" };
		var response = await container.CreateItemAsync(item, new PartitionKey("1"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("1"));
		read.Resource.Name.Should().Be("AutoDetected");
	}

	[Fact]
	public void AutoDetect_MultipleContainerFactories_AllPreserved()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));
		services.AddSingleton<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("db", "orders"));
		services.AddSingleton<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("db", "customers"));

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		containers.Should().HaveCount(2);
		containers.Select(c => c.Id).Should().BeEquivalentTo(["orders", "customers"]);
	}

	[Fact]
	public void AutoDetect_ClientGetContainer_SharesSameBackingStore()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));
		services.AddSingleton<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("MyDb", "orders"));

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		var diContainer = provider.GetRequiredService<Container>();
		var clientContainer = provider.GetRequiredService<CosmosClient>().GetContainer("MyDb", "orders");

		// Both route through the same FakeCosmosHandler backing
		diContainer.Id.Should().Be("orders");
		clientContainer.Id.Should().Be("orders");
	}

	[Fact]
	public void AutoDetect_NoExistingContainers_CreatesDefault()
	{
		var services = new ServiceCollection();
		// No Container registered — only CosmosClient
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));

		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		// Default container should still be created when no existing registrations
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
		container.Id.Should().Be("in-memory-container");
	}

	[Fact]
	public void AutoDetect_PreservesExistingLifetime()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));
		// Register as Scoped — should be preserved
		services.AddScoped<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("db", "orders"));

		services.UseInMemoryCosmosDB();

		var descriptor = services.First(d => d.ServiceType == typeof(Container));
		descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
	}

	[Fact]
	public void ExplicitContainers_StillRemovesExistingRegistrations()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));
		services.AddSingleton<Container>(_ =>
			throw new InvalidOperationException("Should have been removed by explicit AddContainer"));

		// Explicit AddContainer — should remove existing and replace
		services.UseInMemoryCosmosDB(o => o.AddContainer("orders", "/pk"));

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
		container.Id.Should().Be("orders");
	}

	[Fact]
	public void AutoDetect_DoesNotNeedFeedIteratorSetup()
	{
		InMemoryFeedIteratorSetup.Deregister();
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));
		services.AddSingleton<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("db", "items"));

		services.UseInMemoryCosmosDB();

		// FakeCosmosHandler handles .ToFeedIterator() natively — no setup needed
		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull();
	}

	[Fact]
	public void AutoDetect_OnClientCreatedCallback_StillWorks()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));
		services.AddSingleton<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("db", "items"));
		CosmosClient? captured = null;

		services.UseInMemoryCosmosDB(o => o.OnClientCreated = c => captured = c);

		captured.Should().NotBeNull();
	}

	[Fact]
	public void AutoDetect_MixedExistingLifetimes_UsesFirst()
	{
		// When existing Container registrations have mixed lifetimes,
		// the code uses FirstOrDefault()?.Lifetime — so the first registration's lifetime wins.
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(_ =>
			throw new InvalidOperationException("Should be replaced"));
		services.AddScoped<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("db", "orders"));
		services.AddSingleton<Container>(sp =>
			sp.GetRequiredService<CosmosClient>().GetContainer("db", "customers"));

		services.UseInMemoryCosmosDB();

		// In auto-detect mode, existing registrations are preserved as-is.
		// The first descriptor's lifetime (Scoped) was captured for informational purposes,
		// but since auto-detect doesn't replace, both registrations keep their original lifetimes.
		var descriptors = services.Where(d => d.ServiceType == typeof(Container)).ToList();
		descriptors[0].Lifetime.Should().Be(ServiceLifetime.Scoped);
		descriptors[1].Lifetime.Should().Be(ServiceLifetime.Singleton);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Phase 6: HttpMessageHandlerWrapper
// ════════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class HttpMessageHandlerWrapperTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	/// <summary>
	/// A test DelegatingHandler that counts requests passing through it.
	/// </summary>
	private class CountingHandler : DelegatingHandler
	{
		public int RequestCount { get; private set; }

		public CountingHandler(HttpMessageHandler innerHandler) : base(innerHandler) { }

		protected override Task<HttpResponseMessage> SendAsync(
			HttpRequestMessage request, CancellationToken cancellationToken)
		{
			RequestCount++;
			return base.SendAsync(request, cancellationToken);
		}
	}

	[Fact]
	public void Wrapper_IsInvoked_WithHandler()
	{
		var services = new ServiceCollection();
		HttpMessageHandler? capturedHandler = null;

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/pk");
			o.HttpMessageHandlerWrapper = handler =>
			{
				capturedHandler = handler;
				return handler; // pass through unchanged
			};
		});

		capturedHandler.Should().NotBeNull();
		capturedHandler.Should().BeOfType<FakeCosmosHandler>();
	}

	[Fact]
	public async Task Wrapper_ReturnValue_IsUsed_DelegatingHandlerSendAsyncCalled()
	{
		var services = new ServiceCollection();
		CountingHandler? countingHandler = null;

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("test", "/partitionKey");
			o.HttpMessageHandlerWrapper = handler =>
			{
				countingHandler = new CountingHandler(handler);
				return countingHandler;
			};
		});

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// Perform a CRUD operation — this should flow through the CountingHandler
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		countingHandler.Should().NotBeNull();
		countingHandler!.RequestCount.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task NullWrapper_Default_ExistingBehaviourPreserved()
	{
		var services = new ServiceCollection();

		// No HttpMessageHandlerWrapper set — default null
		services.UseInMemoryCosmosDB(o => o.AddContainer("test", "/partitionKey"));

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// CRUD should work exactly as before
		var createResponse = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public void MultiContainer_Wrapper_ReceivesRouter_NotIndividualHandler()
	{
		var services = new ServiceCollection();
		HttpMessageHandler? capturedHandler = null;

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/pk");
			o.AddContainer("events", "/pk");
			o.HttpMessageHandlerWrapper = handler =>
			{
				capturedHandler = handler;
				return handler;
			};
		});

		capturedHandler.Should().NotBeNull();
		// With multiple containers, the handler is the router — NOT a FakeCosmosHandler
		capturedHandler.Should().NotBeOfType<FakeCosmosHandler>();
	}

	[Fact]
	public void FluentMethod_WithHttpMessageHandlerWrapper_Works()
	{
		var services = new ServiceCollection();
		HttpMessageHandler? capturedHandler = null;

		services.UseInMemoryCosmosDB(o => o
			.AddContainer("orders", "/pk")
			.WithHttpMessageHandlerWrapper(handler =>
			{
				capturedHandler = handler;
				return handler;
			}));

		capturedHandler.Should().NotBeNull();
		capturedHandler.Should().BeOfType<FakeCosmosHandler>();
	}

	[Fact]
	public async Task FullDelegatingHandlerChaining_CrudAndQueryThroughWrapper()
	{
		var services = new ServiceCollection();
		CountingHandler? countingHandler = null;

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("test", "/partitionKey");
			o.HttpMessageHandlerWrapper = handler =>
			{
				countingHandler = new CountingHandler(handler);
				return countingHandler;
			};
		});

		var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		// Create
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		// Read
		var readResponse = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Alice");

		// Query
		var iterator = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'"));
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().ContainSingle().Which.Name.Should().Be("Alice");

		// All requests went through the counting handler
		countingHandler.Should().NotBeNull();
		countingHandler!.RequestCount.Should().BeGreaterThanOrEqualTo(3,
			"Create + Read + Query should produce at least 3 HTTP requests through the wrapper");
	}

	[Fact]
	public void OnHandlerCreated_And_HttpWrapper_BothFire()
	{
		// Both callbacks should fire: OnHandlerCreated fires per-container during
		// handler creation, then HttpMessageHandlerWrapper fires once with the
		// final handler (or router).
		var services = new ServiceCollection();
		var handlerCreatedNames = new List<string>();
		HttpMessageHandler? wrappedHandler = null;

		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/pk");
			o.OnHandlerCreated = (name, _) => handlerCreatedNames.Add(name);
			o.HttpMessageHandlerWrapper = handler =>
			{
				wrappedHandler = handler;
				return handler;
			};
		});

		// OnHandlerCreated fires first, per container
		handlerCreatedNames.Should().ContainSingle().Which.Should().Be("orders");

		// HttpMessageHandlerWrapper fires after, with the handler
		wrappedHandler.Should().NotBeNull();
		wrappedHandler.Should().BeOfType<FakeCosmosHandler>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase A: Bug Investigation
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionBugInvestigationTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void Idempotent_CalledTwice_NoExistingRegistrations_VerifyBehavior()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB();
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		var clients = provider.GetServices<CosmosClient>().ToList();
		clients.Should().ContainSingle();

		var containers = provider.GetServices<Container>().ToList();
		containers.Should().ContainSingle();
	}

	[Fact]
	public void UseInMemoryCosmosDB_RegisterFeedIteratorSetup_HasNoEffect()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o => o.RegisterFeedIteratorSetup = true);

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		client.Should().NotBeNull();
		// FeedIteratorSetup is NOT registered by UseInMemoryCosmosDB — it's only
		// relevant for UseInMemoryCosmosDB<T> and UseInMemoryCosmosContainers
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase B: Callback Coverage
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionCallbackTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void OnContainerCreated_DefaultContainer_StillFires()
	{
		InMemoryContainer? captured = null;
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers(o =>
		{
			o.OnContainerCreated = c => captured = (InMemoryContainer)c;
		});

		using var provider = services.BuildServiceProvider();
		_ = provider.GetRequiredService<Container>();

		captured.Should().NotBeNull();
	}

	[Fact]
	public void OnHandlerCreated_DefaultContainer_StillFires()
	{
		FakeCosmosHandler? captured = null;
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o =>
		{
			o.OnHandlerCreated = (name, handler) => captured = handler;
		});

		using var provider = services.BuildServiceProvider();
		_ = provider.GetRequiredService<Container>();

		captured.Should().NotBeNull();
	}

	[Fact]
	public void OnHandlerCreated_AutoDetectMode_StillFires()
	{
		FakeCosmosHandler? captured = null;
		var services = new ServiceCollection();
		services.AddSingleton<Container>(sp =>
		{
			var client = sp.GetRequiredService<CosmosClient>();
			return client.GetContainer("db", "existing");
		});
		services.UseInMemoryCosmosDB(o =>
		{
			o.OnHandlerCreated = (name, handler) => captured = handler;
		});

		using var provider = services.BuildServiceProvider();
		_ = provider.GetRequiredService<Container>();

		captured.Should().NotBeNull();
	}

	[Fact]
	public void OnHandlerCreated_MultipleContainers_FiresForEach()
	{
		var names = new List<string>();
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/pk")
			 .AddContainer("events", "/pk")
			 .AddContainer("users", "/pk");
			o.OnHandlerCreated = (name, handler) => names.Add(name);
		});

		using var provider = services.BuildServiceProvider();
		_ = provider.GetServices<Container>().ToList();

		names.Should().HaveCount(3);
		names.Should().Contain("orders");
		names.Should().Contain("events");
		names.Should().Contain("users");
	}

	[Fact]
	public void TypedClient_OnClientCreated_ReturnsDIInstance()
	{
		EmployeeCosmosClient? captured = null;
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.OnClientCreated = c => captured = (EmployeeCosmosClient)c;
		});

		using var provider = services.BuildServiceProvider();
		var resolved = provider.GetRequiredService<EmployeeCosmosClient>();

		captured.Should().BeSameAs(resolved);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase C: Idempotency & Multiple Calls
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionIdempotencyTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void Idempotent_CalledTwice_AutoDetectMode()
	{
		var services = new ServiceCollection();
		services.AddSingleton<Container>(sp =>
		{
			var client = sp.GetRequiredService<CosmosClient>();
			return client.GetContainer("db", "existing");
		});
		services.UseInMemoryCosmosDB();
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		var clients = provider.GetServices<CosmosClient>().ToList();
		clients.Should().ContainSingle();
	}

	[Fact]
	public void CalledTwice_DifferentContainers_SecondWins()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o => o.AddContainer("orders", "/pk"));
		services.UseInMemoryCosmosDB(o => o.AddContainer("events", "/pk"));

		using var provider = services.BuildServiceProvider();
		var containers = provider.GetServices<Container>().ToList();
		// Second call removes all containers and adds its own
		containers.Should().ContainSingle();
	}

	[Fact]
	public void CalledTwice_DifferentDatabaseNames()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o =>
		{
			o.DatabaseName = "db1";
			o.AddContainer("orders", "/pk");
		});
		services.UseInMemoryCosmosDB(o =>
		{
			o.DatabaseName = "db2";
			o.AddContainer("events", "/pk");
		});

		using var provider = services.BuildServiceProvider();
		// Second call wins — client is replaced
		var client = provider.GetRequiredService<CosmosClient>();
		client.Should().NotBeNull();
	}

	[Fact]
	public void UseInMemoryCosmosContainers_CalledTwice_DifferentContainers()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers(o => o.AddContainer("orders", "/pk"));
		services.UseInMemoryCosmosContainers(o => o.AddContainer("events", "/pk"));

		using var provider = services.BuildServiceProvider();
		// Both registrations add; last is resolved for "Container"
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase 1: AddContainer(ContainerProperties) Overload Tests
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ContainerPropertiesOverloadTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task UseInMemoryCosmosDB_AddContainerViaContainerProperties_ContainerCreated()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o =>
			o.AddContainer(new ContainerProperties("orders", "/partitionKey")));

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();

		// Verify CRUD works
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task UseInMemoryCosmosDB_AddContainerViaContainerProperties_UniqueKeyPolicyEnforced()
	{
		var props = new ContainerProperties("orders-uk", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o => o.AddContainer(props));

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" }, new PartitionKey("pk"));

		var act = () => container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice" }, new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task UseInMemoryCosmosDB_AddContainerViaContainerProperties_DefaultTtlHonored()
	{
		var props = new ContainerProperties("orders-ttl", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o => o.AddContainer(props));

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		await Task.Delay(1500);

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(0, "item should expire after TTL");
	}

	[Fact]
	public async Task UseInMemoryCosmosContainers_AddContainerViaContainerProperties_ContainerCreated()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers(o =>
			o.AddContainer(new ContainerProperties("orders-cont", "/partitionKey")));

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task UseInMemoryCosmosContainers_AddContainerViaContainerProperties_UniqueKeyPolicyEnforced()
	{
		var props = new ContainerProperties("orders-cont-uk", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers(o => o.AddContainer(props));

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" }, new PartitionKey("pk"));

		var act = () => container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice" }, new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task TypedClient_AddContainerViaContainerProperties_ContainerCreated()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
		{
			o.DatabaseName = "EmpDb";
			o.AddContainer(new ContainerProperties("employees", "/partitionKey"), "EmpDb");
		});

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("EmpDb", "employees");

		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public void FluentChaining_InMemoryCosmosOptions_AddContainerProperties()
	{
		var options = new InMemoryCosmosOptions();
		var props = new ContainerProperties("chained", "/pk");
		var result = options.AddContainer(props, "myDb");

		result.Should().BeSameAs(options, "AddContainer should support fluent chaining");
		options.Containers.Should().ContainSingle();
		options.Containers[0].ContainerName.Should().Be("chained");
		options.Containers[0].DatabaseName.Should().Be("myDb");
		options.Containers[0].ContainerProperties.Should().BeSameAs(props);
	}

	[Fact]
	public void FluentChaining_InMemoryContainerOptions_AddContainerProperties()
	{
		var options = new InMemoryContainerOptions();
		var props = new ContainerProperties("chained-cont", "/pk");
		var result = options.AddContainer(props);

		result.Should().BeSameAs(options, "AddContainer should support fluent chaining");
		options.Containers.Should().ContainSingle();
		options.Containers[0].ContainerProperties.Should().BeSameAs(props);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase 2: StatePersistenceDirectory Tests
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class StatePersistenceDirectoryTests : IDisposable
{
	private readonly string _tempDir;

	public StatePersistenceDirectoryTests()
	{
		_tempDir = Path.Combine(Path.GetTempPath(), $"cosmos-test-{Guid.NewGuid():N}");
		Directory.CreateDirectory(_tempDir);
	}

	public void Dispose()
	{
		InMemoryFeedIteratorSetup.Deregister();
		if (Directory.Exists(_tempDir))
			Directory.Delete(_tempDir, true);
	}

	[Fact]
	public async Task UseInMemoryCosmosDB_StatePersistenceDirectory_NonExistentDir_StartsEmpty()
	{
		var emptyDir = Path.Combine(Path.GetTempPath(), $"cosmos-empty-{Guid.NewGuid():N}");
		try
		{
			var services = new ServiceCollection();
			services.UseInMemoryCosmosDB(o =>
			{
				o.AddContainer("orders", "/partitionKey");
				o.StatePersistenceDirectory = emptyDir;
			});

			using var provider = services.BuildServiceProvider();
			var container = provider.GetRequiredService<Container>();
			var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			var results = await iter.ReadNextAsync();
			results.Count.Should().Be(0);
		}
		finally
		{
			if (Directory.Exists(emptyDir)) Directory.Delete(emptyDir, true);
		}
	}

	[Fact]
	public async Task UseInMemoryCosmosDB_StatePersistenceDirectory_LoadsSavedState()
	{
		// Pre-create state file in expected format
		var stateContainer = new InMemoryContainer("mycontainer", "/partitionKey");
		await stateContainer.CreateItemAsync(
			new TestDocument { Id = "seeded", PartitionKey = "pk", Name = "Seeded" }, new PartitionKey("pk"));
		var state = stateContainer.ExportState();

		// UseInMemoryCosmosDB uses "{db}_{container}.json" naming
		var stateFile = Path.Combine(_tempDir, "mydb_mycontainer.json");
		await File.WriteAllTextAsync(stateFile, state);

		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o =>
		{
			o.DatabaseName = "mydb";
			o.AddContainer("mycontainer", "/partitionKey", "mydb");
			o.StatePersistenceDirectory = _tempDir;
		});

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		var read = await container.ReadItemAsync<TestDocument>("seeded", new PartitionKey("pk"));
		read.Resource.Name.Should().Be("Seeded");
	}

	[Fact]
	public async Task UseInMemoryCosmosContainers_StatePersistenceDirectory_NonExistentDir_StartsEmpty()
	{
		var emptyDir = Path.Combine(Path.GetTempPath(), $"cosmos-empty-{Guid.NewGuid():N}");
		try
		{
			var services = new ServiceCollection();
			services.UseInMemoryCosmosContainers(o =>
			{
				o.AddContainer("orders", "/partitionKey");
				o.StatePersistenceDirectory = emptyDir;
			});

			using var provider = services.BuildServiceProvider();
			var container = provider.GetRequiredService<Container>();
			var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			var results = await iter.ReadNextAsync();
			results.Count.Should().Be(0);
		}
		finally
		{
			if (Directory.Exists(emptyDir)) Directory.Delete(emptyDir, true);
		}
	}

	[Fact]
	public async Task UseInMemoryCosmosContainers_StatePersistenceDirectory_LoadsSavedState()
	{
		// UseInMemoryCosmosContainers uses "{container}.json" naming (no db prefix)
		var stateContainer = new InMemoryContainer("events", "/partitionKey");
		await stateContainer.CreateItemAsync(
			new TestDocument { Id = "seeded", PartitionKey = "pk", Name = "Loaded" }, new PartitionKey("pk"));
		var state = stateContainer.ExportState();

		var stateFile = Path.Combine(_tempDir, "events.json");
		await File.WriteAllTextAsync(stateFile, state);

		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers(o =>
		{
			o.AddContainer("events", "/partitionKey");
			o.StatePersistenceDirectory = _tempDir;
		});

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		var read = await container.ReadItemAsync<TestDocument>("seeded", new PartitionKey("pk"));
		read.Resource.Name.Should().Be("Loaded");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase 3: Multi-Database Container Name Collision Tests
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class MultiDatabaseContainerCollisionTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task MultiDatabase_SameContainerName_DataIsolation()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/partitionKey", "db1");
			o.AddContainer("orders", "/partitionKey", "db2");
		});

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();

		var db1Container = client.GetContainer("db1", "orders");
		var db2Container = client.GetContainer("db2", "orders");

		await db1Container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "DB1Item" }, new PartitionKey("pk"));

		// db2 should not see db1's item
		var act = () => db2Container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task MultiDatabase_DifferentContainerNames_NoCollision()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/partitionKey", "db1");
			o.AddContainer("customers", "/partitionKey", "db2");
		});

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();

		var orders = client.GetContainer("db1", "orders");
		var customers = client.GetContainer("db2", "customers");

		await orders.CreateItemAsync(
			new TestDocument { Id = "o1", PartitionKey = "pk", Name = "Order" }, new PartitionKey("pk"));
		await customers.CreateItemAsync(
			new TestDocument { Id = "c1", PartitionKey = "pk", Name = "Customer" }, new PartitionKey("pk"));

		var orderRead = await orders.ReadItemAsync<TestDocument>("o1", new PartitionKey("pk"));
		orderRead.Resource.Name.Should().Be("Order");

		var customerRead = await customers.ReadItemAsync<TestDocument>("c1", new PartitionKey("pk"));
		customerRead.Resource.Name.Should().Be("Customer");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase 4: Callback Exception Propagation Tests
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class CallbackExceptionPropagationTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void OnHandlerCreated_ThrowsException_PropagatesUnwrapped()
	{
		var services = new ServiceCollection();
		var act = () => services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("test", "/pk");
			o.OnHandlerCreated = (_, _) => throw new InvalidOperationException("Handler callback failed");
		});
		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*Handler callback failed*");
	}

	[Fact]
	public void OnClientCreated_ThrowsException_PropagatesUnwrapped()
	{
		var services = new ServiceCollection();
		var act = () => services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("test", "/pk");
			o.OnClientCreated = _ => throw new InvalidOperationException("Client callback failed");
		});
		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*Client callback failed*");
	}

	[Fact]
	public void OnContainerCreated_ThrowsException_PropagatesUnwrapped()
	{
		var services = new ServiceCollection();
		var act = () => services.UseInMemoryCosmosContainers(o =>
		{
			o.AddContainer("test", "/pk");
			o.OnContainerCreated = _ => throw new InvalidOperationException("Container callback failed");
		});
		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*Container callback failed*");
	}

	[Fact]
	public void HttpMessageHandlerWrapper_ThrowsException_PropagatesUnwrapped()
	{
		var services = new ServiceCollection();
		var act = () => services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("test", "/pk");
			o.WithHttpMessageHandlerWrapper(_ => throw new InvalidOperationException("Wrapper failed"));
		});
		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*Wrapper failed*");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase 7: Auto-Detect Mode Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class AutoDetectModeEdgeCaseTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task AutoDetect_FactoryThatUsesGetContainer_DifferentNames()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(sp => new CosmosClient("AccountEndpoint=https://localhost;AccountKey=dGVzdGtleQ==;"));
		services.AddSingleton(sp =>
		{
			var client = sp.GetRequiredService<CosmosClient>();
			return client.GetContainer("CustomDb", "custom-container");
		});

		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();

		// Verify it's functional even with custom db/container names
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "1", Name = "A" }, new PartitionKey("1"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task AutoDetect_NoCosmosClient_NoContainer_RegistersBoth()
	{
		var services = new ServiceCollection();
		// No existing registrations at all
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		client.Should().NotBeNull();

		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();

		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "1", Name = "A" }, new PartitionKey("1"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase D: Null / Edge Case / Validation
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionEdgeCaseDeepDiveTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void UseInMemoryCosmosDB_ExplicitNullConfigure()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(null);

		using var provider = services.BuildServiceProvider();
		provider.GetRequiredService<CosmosClient>().Should().NotBeNull();
		provider.GetRequiredService<Container>().Should().NotBeNull();
	}

	[Fact]
	public void UseInMemoryCosmosContainers_ExplicitNullConfigure()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers(null);

		using var provider = services.BuildServiceProvider();
		provider.GetRequiredService<Container>().Should().NotBeNull();
	}

	[Fact]
	public void UseInMemoryCosmosDB_TypedClient_ExplicitNullConfigure()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(null);

		using var provider = services.BuildServiceProvider();
		provider.GetRequiredService<EmployeeCosmosClient>().Should().NotBeNull();
	}

	[Fact]
	public void ContainerConfig_DefaultValues()
	{
		var config = new ContainerConfig("my-container");
		config.ContainerName.Should().Be("my-container");
		config.PartitionKeyPath.Should().Be("/id");
		config.DatabaseName.Should().BeNull();
	}

	[Fact]
	public void ContainerConfig_WithDeconstruction()
	{
		var config = new ContainerConfig("my-container", "/pk", "mydb");
		var (name, pkPath, dbName, _) = config;
		name.Should().Be("my-container");
		pkPath.Should().Be("/pk");
		dbName.Should().Be("mydb");
	}

	[Fact]
	public void ContainerConfig_EmptyContainerName()
	{
		var config = new ContainerConfig("");
		config.ContainerName.Should().BeEmpty();
	}

	[Fact]
	public void ContainerConfig_PartitionKeyPathWithoutLeadingSlash()
	{
		var config = new ContainerConfig("test", "id");
		config.PartitionKeyPath.Should().Be("id");
		// InMemoryContainer handles the path normalization
	}

	[Fact]
	public void DoesNotAffectUnrelatedServices()
	{
		var services = new ServiceCollection();
		services.AddSingleton("hello-world");
		services.AddSingleton<object>(42);
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		provider.GetRequiredService<string>().Should().Be("hello-world");
		provider.GetRequiredService<object>().Should().Be(42);
		provider.GetRequiredService<CosmosClient>().Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase E: Query & CRUD Integration Through DI
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionQueryIntegrationTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public async Task UseInMemoryCosmosContainers_LinqQuery_ViaToFeedIteratorOverridable()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers();

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "1", Name = "Alice" }, new PartitionKey("1"));

		var iter = container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "Alice")
			.ToFeedIteratorOverridable();
		var page = await iter.ReadNextAsync();
		page.Count.Should().Be(1);
	}

	[Fact]
	public async Task UseInMemoryCosmosDB_SqlQuery()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "1", Name = "A" }, new PartitionKey("1"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(1);
	}

	[Fact]
	public async Task UseInMemoryCosmosContainers_SqlQuery()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers();

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "1", Name = "A" }, new PartitionKey("1"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(1);
	}

	[Fact]
	public async Task TypedClient_SqlQuery()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
			o.AddContainer("biometrics", "/partitionKey", "BiometricDb"));

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("BiometricDb", "biometrics");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = await iter.ReadNextAsync();
		results.Count.Should().Be(1);
	}

	[Fact]
	public async Task TypedClient_LinqQuery_ViaToFeedIteratorOverridable()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>(o =>
			o.AddContainer("biometrics", "/partitionKey", "BiometricDb"));

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		var container = client.GetContainer("BiometricDb", "biometrics");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" }, new PartitionKey("pk"));

		var iter = container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "Alice")
			.ToFeedIteratorOverridable();
		var page = await iter.ReadNextAsync();
		page.Count.Should().Be(1);
	}

	[Fact]
	public async Task MultiContainer_DataIsolation_UseInMemoryCosmosDB()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o =>
		{
			o.AddContainer("orders", "/partitionKey");
			o.AddContainer("events", "/partitionKey");
		});

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		var orders = client.GetContainer("in-memory-db", "orders");
		var events = client.GetContainer("in-memory-db", "events");

		await orders.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Order1" }, new PartitionKey("pk"));

		var ordersIter = orders.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var orderResults = await ordersIter.ReadNextAsync();
		orderResults.Count.Should().Be(1);

		var eventsIter = events.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var eventResults = await eventsIter.ReadNextAsync();
		eventResults.Count.Should().Be(0);
	}

	[Fact]
	public async Task UseInMemoryCosmosDB_StreamCrud()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();

		var createResp = await container.CreateItemStreamAsync(
			new MemoryStream(System.Text.Encoding.UTF8.GetBytes(
				"""{"id":"1","partitionKey":"1","name":"A"}""")),
			new PartitionKey("1"));
		createResp.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResp = await container.ReadItemStreamAsync("1", new PartitionKey("1"));
		readResp.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase F: DI Patterns & Auto-Detect Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class AutoDetectEdgeCaseTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void AutoDetect_ExistingInstanceRegistration_IsPreserved()
	{
		var instance = new InMemoryContainer("standalone", "/id");
		var services = new ServiceCollection();
		services.AddSingleton<Container>(instance);
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		// Auto-detect preserves existing registrations
		var containers = provider.GetServices<Container>().ToList();
		containers.Should().NotBeEmpty();
	}

	[Fact]
	public void AutoDetect_FactoryWithAdditionalDependency_Resolves()
	{
		var services = new ServiceCollection();
		services.AddSingleton("test-config-value");
		services.AddSingleton<Container>(sp =>
		{
			var config = sp.GetRequiredService<string>();
			var client = sp.GetRequiredService<CosmosClient>();
			return client.GetContainer("db", config);
		});
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		container.Should().NotBeNull();
	}

	[Fact]
	public void UseInMemoryCosmosContainers_ExistingCosmosClientFactory_StillResolvable()
	{
		var services = new ServiceCollection();
		services.AddSingleton<CosmosClient>(new InMemoryCosmosClient());
		services.UseInMemoryCosmosContainers();

		using var provider = services.BuildServiceProvider();
		// CosmosClient is NOT removed by UseInMemoryCosmosContainers
		provider.GetRequiredService<CosmosClient>().Should().NotBeNull();
		provider.GetRequiredService<Container>().Should().NotBeNull();
	}

	[Fact]
	public void UseInMemoryCosmosContainers_ContainerIsRealSdkType()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers();

		using var provider = services.BuildServiceProvider();
		var container = provider.GetRequiredService<Container>();
		// v4.0: Container is a real SDK Container backed by FakeCosmosHandler
		container.Should().NotBeOfType<InMemoryContainer>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase G: Lifecycle & Thread Safety
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionLifecycleTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void ServiceProviderDisposal_DoesNotThrow()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB();

		var provider = services.BuildServiceProvider();
		_ = provider.GetRequiredService<CosmosClient>();
		_ = provider.GetRequiredService<Container>();

		var act = () => provider.Dispose();
		act.Should().NotThrow();
	}

	[Fact]
	public void ServiceProviderDisposal_TypedClient_DoesNotThrow()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		var provider = services.BuildServiceProvider();
		_ = provider.GetRequiredService<EmployeeCosmosClient>();

		var act = () => provider.Dispose();
		act.Should().NotThrow();
	}

	[Fact]
	public void ServiceProviderDisposal_ContainersOnly_DoesNotThrow()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosContainers();

		var provider = services.BuildServiceProvider();
		_ = provider.GetRequiredService<Container>();

		var act = () => provider.Dispose();
		act.Should().NotThrow();
	}

	[Fact]
	public async Task ConcurrentScopeResolution_SameBackingData()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB(o => o.AddContainer("shared", "/partitionKey"));

		using var provider = services.BuildServiceProvider();

		// Write from main scope
		var container = provider.GetRequiredService<Container>();
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Shared" }, new PartitionKey("pk"));

		// Read from concurrent "scopes" (singletons share the same instance)
		var tasks = Enumerable.Range(0, 5).Select(async _ =>
		{
			var c = provider.GetRequiredService<Container>();
			var iter = c.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			var page = await iter.ReadNextAsync();
			return page.Count;
		});

		var counts = await Task.WhenAll(tasks);
		counts.Should().AllSatisfy(c => c.Should().Be(1));
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase H: Fluent API & Options Model
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionFluentApiTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void FluentChaining_InMemoryCosmosOptions_AddContainer_WithDatabaseName()
	{
		var options = new InMemoryCosmosOptions();
		options.AddContainer("orders", "/pk", "db1")
			   .AddContainer("events", "/pk", "db2");

		options.Containers.Should().HaveCount(2);
		options.Containers[0].DatabaseName.Should().Be("db1");
		options.Containers[1].DatabaseName.Should().Be("db2");
	}

	[Fact]
	public void FluentChaining_WithHttpMessageHandlerWrapper_ChainedWithAddContainer()
	{
		HttpMessageHandler? captured = null;
		var options = new InMemoryCosmosOptions();
		var result = options
			.AddContainer("orders", "/pk")
			.WithHttpMessageHandlerWrapper(h => { captured = h; return h; })
			.AddContainer("events", "/pk");

		result.Containers.Should().HaveCount(2);
		result.HttpMessageHandlerWrapper.Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 36 — Phase I: CosmosClient Properties
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class ServiceCollectionClientPropertyTests : IDisposable
{
	public void Dispose() => InMemoryFeedIteratorSetup.Deregister();

	[Fact]
	public void CosmosClient_Endpoint()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB();

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<CosmosClient>();
		client.Endpoint.Should().NotBeNull();
	}

	[Fact]
	public void TypedClient_IsCastleProxy()
	{
		var services = new ServiceCollection();
		services.UseInMemoryCosmosDB<EmployeeCosmosClient>();

		using var provider = services.BuildServiceProvider();
		var client = provider.GetRequiredService<EmployeeCosmosClient>();
		client.Should().BeAssignableTo<EmployeeCosmosClient>();
		client.GetType().Should().NotBe(typeof(EmployeeCosmosClient), "Castle proxy creates a subclass");
	}
}
