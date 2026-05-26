using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class InMemoryCosmosDynamicContainerTests
{
	[Fact]
	public async Task CreateContainerAsync_CreatesUsableContainer()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);

		var props = new ContainerProperties("products", "/categoryId");
		var response = await db.CreateContainerAsync(props);
		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var productsContainer = cosmos.Client.GetContainer(InMemoryCosmos.DefaultDatabaseName, "products");
		await productsContainer.CreateItemAsync(
			new { id = "p1", categoryId = "c1", name = "Widget" },
			new PartitionKey("c1"));

		var readResponse = await productsContainer.ReadItemAsync<JObject>("p1", new PartitionKey("c1"));
		readResponse.Resource["name"]!.ToString().Should().Be("Widget");
	}

	[Fact]
	public async Task CreateContainerAsync_AppearsInContainersDictionary()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);

		await db.CreateContainerAsync(new ContainerProperties("products", "/categoryId"));

		cosmos.Containers.Should().ContainKey("products");
		cosmos.Containers.Should().HaveCount(2); // orders + products
	}

	[Fact]
	public async Task CreateContainerAsync_AppearsInHandlersDictionary()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);

		await db.CreateContainerAsync(new ContainerProperties("products", "/categoryId"));

		cosmos.Handlers.Should().ContainKey("products");
		cosmos.Handlers.Should().HaveCount(2);
	}

	[Fact]
	public async Task CreateContainerAsync_SetupContainerWorksForDynamic()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);

		await db.CreateContainerAsync(new ContainerProperties("products", "/categoryId"));

		var setup = cosmos.SetupContainer("products");
		setup.Should().NotBeNull();
	}

	[Fact]
	public async Task CreateContainerAsync_GetHandlerWorksForDynamic()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);

		await db.CreateContainerAsync(new ContainerProperties("products", "/categoryId"));

		var handler = cosmos.GetHandler("products");
		handler.Should().NotBeNull();
	}

	[Fact]
	public async Task CreateContainerIfNotExistsAsync_CreatesWhenNew()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);

		var response = await db.CreateContainerIfNotExistsAsync(
			new ContainerProperties("products", "/categoryId"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateContainerIfNotExistsAsync_ReturnsOkWhenExists()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);

		await db.CreateContainerAsync(new ContainerProperties("products", "/categoryId"));
		var response = await db.CreateContainerIfNotExistsAsync(
			new ContainerProperties("products", "/categoryId"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteContainerAsync_RemovesContainer()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddContainer("orders", "/partitionKey")
			.AddContainer("products", "/categoryId")
			.Build();

		var productsContainer = cosmos.Client.GetContainer(InMemoryCosmos.DefaultDatabaseName, "products");
		await productsContainer.DeleteContainerAsync();

		cosmos.Containers.Should().NotContainKey("products");
		cosmos.Containers.Should().HaveCount(1);
		cosmos.Handlers.Should().NotContainKey("products");
	}

	[Fact]
	public async Task DeleteContainerAsync_SubsequentOperationsReturn404()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddContainer("orders", "/partitionKey")
			.AddContainer("products", "/categoryId")
			.Build();

		var productsContainer = cosmos.Client.GetContainer(InMemoryCosmos.DefaultDatabaseName, "products");
		await productsContainer.DeleteContainerAsync();

		var act = () => productsContainer.CreateItemAsync(
			new { id = "p1", categoryId = "c1" }, new PartitionKey("c1"));
		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteContainerAsync_SingularContainerPropertyStillWorks()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);

		await db.CreateContainerAsync(new ContainerProperties("products", "/categoryId"));

		// Delete the dynamically created container
		var productsContainer = cosmos.Client.GetContainer(InMemoryCosmos.DefaultDatabaseName, "products");
		await productsContainer.DeleteContainerAsync();

		// Original Container property still works
		await cosmos.Container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
	}

	[Fact]
	public async Task ReplaceContainerAsync_UpdatesMutableProperties()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

		var container = cosmos.Client.GetContainer(InMemoryCosmos.DefaultDatabaseName, "orders");
		var readResponse = await container.ReadContainerAsync();
		var props = readResponse.Resource;
		props.DefaultTimeToLive = 3600;

		var replaceResponse = await container.ReplaceContainerAsync(props);
		replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceContainerAsync_RejectsPkPathChange()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

		var container = cosmos.Client.GetContainer(InMemoryCosmos.DefaultDatabaseName, "orders");
		var readResponse = await container.ReadContainerAsync();
		var props = readResponse.Resource;
		props.PartitionKeyPath = "/differentKey";

		var act = () => container.ReplaceContainerAsync(props);
		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

public class InMemoryCosmosDatabaseCrudTests
{
	[Fact]
	public async Task CreateDatabaseAsync_Succeeds()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

		var response = await cosmos.Client.CreateDatabaseAsync("mydb");
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateDatabaseIfNotExistsAsync_Succeeds()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

		// Database always "exists" in the stub handler, so both calls return OK
		var response = await cosmos.Client.CreateDatabaseIfNotExistsAsync("mydb");
		response.StatusCode.Should().BeOneOf(HttpStatusCode.Created, HttpStatusCode.OK);

		var response2 = await cosmos.Client.CreateDatabaseIfNotExistsAsync("mydb");
		response2.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReadDatabaseAsync_Returns200()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);
		var response = await db.ReadAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteDatabaseAsync_Returns204()
	{
		using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

		await cosmos.Client.CreateDatabaseAsync("temp-db");
		var db = cosmos.Client.GetDatabase("temp-db");
		var response = await db.DeleteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task GetContainerQueryIterator_ListsContainers()
	{
		using var cosmos = InMemoryCosmos.Builder()
			.AddContainer("orders", "/partitionKey")
			.AddContainer("customers", "/id")
			.Build();

		var db = cosmos.Client.GetDatabase(InMemoryCosmos.DefaultDatabaseName);
		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var containers = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			containers.AddRange(page);
		}

		containers.Select(c => c.Id).Should().Contain("orders");
		containers.Select(c => c.Id).Should().Contain("customers");
	}
}
