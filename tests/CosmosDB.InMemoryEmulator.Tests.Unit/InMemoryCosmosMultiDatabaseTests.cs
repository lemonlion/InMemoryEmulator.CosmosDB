using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class InMemoryCosmosMultiDatabaseTests
{
	[Fact]
	public async Task AddDatabase_CreatesContainersInSeparateDatabases()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db =>
			{
				db.AddContainer("events", "/userId");
				db.AddContainer("profiles", "/userId");
			})
			.AddDatabase("orders-db", db =>
			{
				db.AddContainer("events", "/orderId");
				db.AddContainer("products", "/categoryId");
			})
			.Build();

		// Database-scoped access
		var userEvents = cosmos.Database("users-db").Containers["events"];
		var orderEvents = cosmos.Database("orders-db").Containers["events"];

		userEvents.Should().NotBeSameAs(orderEvents);

		// Write to users-db/events
		await userEvents.CreateItemAsync(
			new { id = "1", userId = "u1", type = "login" },
			new PartitionKey("u1"));

		// Write to orders-db/events
		await orderEvents.CreateItemAsync(
			new { id = "1", orderId = "o1", type = "placed" },
			new PartitionKey("o1"));

		// Read back from each — separate data
		var userEvent = await userEvents.ReadItemAsync<JObject>("1", new PartitionKey("u1"));
		userEvent.Resource["type"]!.ToString().Should().Be("login");

		var orderEvent = await orderEvents.ReadItemAsync<JObject>("1", new PartitionKey("o1"));
		orderEvent.Resource["type"]!.ToString().Should().Be("placed");
	}

	[Fact]
	public void Database_ReturnsCorrectResult()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db =>
			{
				db.AddContainer("profiles", "/userId");
			})
			.Build();

		var dbResult = cosmos.Database("users-db");
		dbResult.Should().NotBeNull();
		dbResult.DatabaseName.Should().Be("users-db");
		dbResult.Containers.Should().ContainKey("profiles");
	}

	[Fact]
	public void Database_NonexistentName_Throws()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db =>
			{
				db.AddContainer("profiles", "/userId");
			})
			.Build();

		var act = () => cosmos.Database("nonexistent");

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*Database 'nonexistent' not found*Available databases*users-db*");
	}

	[Fact]
	public void Databases_Dictionary_HasAllDatabases()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("profiles", "/userId"))
			.AddDatabase("orders-db", db => db.AddContainer("orders", "/orderId"))
			.Build();

		cosmos.Databases.Should().HaveCount(2);
		cosmos.Databases.Should().ContainKey("users-db");
		cosmos.Databases.Should().ContainKey("orders-db");
	}

	[Fact]
	public void DatabaseResult_SetupContainer_Works()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("profiles", "/userId"))
			.Build();

		var setup = cosmos.Database("users-db").SetupContainer("profiles");
		setup.Should().NotBeNull();
		setup.Should().BeAssignableTo<IContainerTestSetup>();
	}

	[Fact]
	public void DatabaseResult_GetHandler_Works()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("profiles", "/userId"))
			.Build();

		var handler = cosmos.Database("users-db").GetHandler("profiles");
		handler.Should().NotBeNull();
	}

	[Fact]
	public void DatabaseResult_SetFaultInjector_SetsOnDatabaseHandlers()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db =>
			{
				db.AddContainer("profiles", "/userId");
				db.AddContainer("events", "/userId");
			})
			.AddDatabase("orders-db", db => db.AddContainer("orders", "/orderId"))
			.Build();

		Func<HttpRequestMessage, HttpResponseMessage?> injector =
			req => new HttpResponseMessage((HttpStatusCode)503);

		// Set only on users-db
		cosmos.Database("users-db").SetFaultInjector(injector);

		cosmos.Database("users-db").Handlers["profiles"].FaultInjector.Should().BeSameAs(injector);
		cosmos.Database("users-db").Handlers["events"].FaultInjector.Should().BeSameAs(injector);
		cosmos.Database("orders-db").Handlers["orders"].FaultInjector.Should().BeNull();
	}
}

public class InMemoryCosmosMultiDatabaseMixingTests
{
	[Fact]
	public void MixAddContainerAndAddDatabase_UniqueNames_FlatAccessWorks()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddContainer("orders", "/customerId")
			.AddDatabase("audit-db", db =>
			{
				db.AddContainer("events", "/eventId");
			})
			.Build();

		// Flat access works — names are globally unique
		cosmos.Containers.Should().ContainKey("orders");
		cosmos.Containers.Should().ContainKey("events");
	}

	[Fact]
	public void MixAddContainerAndAddDatabase_SetupContainer_Works()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddContainer("orders", "/customerId")
			.AddDatabase("audit-db", db =>
			{
				db.AddContainer("events", "/eventId");
			})
			.Build();

		cosmos.SetupContainer("orders").Should().NotBeNull();
		cosmos.SetupContainer("events").Should().NotBeNull();
	}
}

public class InMemoryCosmosAmbiguityTests
{
	[Fact]
	public void Containers_AmbiguousName_ThrowsWithGuidance()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("events", "/userId"))
			.AddDatabase("orders-db", db => db.AddContainer("events", "/orderId"))
			.Build();

		var act = () => cosmos.Containers["events"];

		act.Should().Throw<KeyNotFoundException>()
			.WithMessage("*events*multiple databases*");
	}

	[Fact]
	public void SetupContainer_AmbiguousName_ThrowsWithGuidance()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("events", "/userId"))
			.AddDatabase("orders-db", db => db.AddContainer("events", "/orderId"))
			.Build();

		var act = () => cosmos.SetupContainer("events");

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*events*multiple databases*");
	}

	[Fact]
	public void GetHandler_AmbiguousName_ThrowsWithGuidance()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("events", "/userId"))
			.AddDatabase("orders-db", db => db.AddContainer("events", "/orderId"))
			.Build();

		var act = () => cosmos.GetHandler("events");

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*events*multiple databases*");
	}

	[Fact]
	public void Database_ScopedAccess_WorksForAmbiguousNames()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("events", "/userId"))
			.AddDatabase("orders-db", db => db.AddContainer("events", "/orderId"))
			.Build();

		// Database-scoped access works fine
		cosmos.Database("users-db").Containers["events"].Should().NotBeNull();
		cosmos.Database("orders-db").Containers["events"].Should().NotBeNull();
		cosmos.Database("users-db").SetupContainer("events").Should().NotBeNull();
		cosmos.Database("orders-db").GetHandler("events").Should().NotBeNull();
	}

	[Fact]
	public void Containers_UniqueNamesAcrossDatabases_FlatAccessWorks()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("profiles", "/userId"))
			.AddDatabase("orders-db", db => db.AddContainer("orders", "/orderId"))
			.Build();

		// Unique names — flat access works
		cosmos.Containers.Should().ContainKey("profiles");
		cosmos.Containers.Should().ContainKey("orders");
	}
}

public class InMemoryCosmosMultiDatabaseValidationTests
{
	[Fact]
	public void AddDatabase_DuplicateName_Throws()
	{
		var act = () => InMemoryCosmos.Builder()
			.AddDatabase("users-db", db => db.AddContainer("profiles", "/userId"))
			.AddDatabase("users-db", db => db.AddContainer("other", "/id"));

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*Database 'users-db' has already been added*");
	}

	[Fact]
	public void AddDatabase_EmptyCallback_Throws()
	{
		var act = () => InMemoryCosmos.Builder()
			.AddDatabase("empty-db", db => { })
			.Build();

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*Database 'empty-db' must have at least one container*");
	}

	[Fact]
	public void AddDatabase_DuplicateContainerWithinDatabase_Throws()
	{
		var act = () => InMemoryCosmos.Builder()
			.AddDatabase("users-db", db =>
			{
				db.AddContainer("events", "/userId");
				db.AddContainer("events", "/id");
			});

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*Container 'events' has already been added*");
	}
}
