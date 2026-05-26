using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Bug: FakeCosmosHandler does not support database and container management HTTP routes.
///
/// Found while migrating 5 open-source projects to use InMemoryEmulator:
/// - ASOS/SimpleEventStore
/// - Azure/Microsoft.Extensions.Caching.Cosmos
/// - MassTransit/MassTransit
/// - Avanade/Beef
/// - Particular/NServiceBus.Persistence.CosmosDB
///
/// Every real-world project calls CreateDatabaseIfNotExistsAsync and/or
/// CreateContainerIfNotExistsAsync before using containers. FakeCosmosHandler
/// throws "unrecognised route POST /dbs" for these operations, forcing all
/// 5 projects to fall back to InMemoryCosmosClient (which doesn't support
/// custom serializers via CosmosClientOptions) instead of the recommended
/// FakeCosmosHandler approach (which preserves the full SDK pipeline).
/// Tests verify FakeCosmosHandler-specific route stubs and cannot meaningfully
/// parity-validate against the real emulator (which supports these natively).
/// </summary>
public class FakeCosmosHandlerDatabaseRoutesBugReproduction
{
	[Fact]
	public async Task FakeCosmosHandler_ShouldSupportCreateDatabaseIfNotExistsAsync()
	{
		var inMemoryContainer = new InMemoryContainer("test-container", "/partitionKey");
		using var handler = new FakeCosmosHandler(inMemoryContainer);

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var act = async () => await client.CreateDatabaseIfNotExistsAsync("test-db");
		await act.Should().NotThrowAsync(
			"FakeCosmosHandler should support CreateDatabaseIfNotExistsAsync since nearly all real projects need it");
	}

	[Fact]
	public async Task FakeCosmosHandler_ShouldSupportCreateContainerIfNotExistsAsync()
	{
		var inMemoryContainer = new InMemoryContainer("test-container", "/partitionKey");
		using var handler = new FakeCosmosHandler(inMemoryContainer);

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler)
			});

		// Projects typically create a database first, then a container
		var act = async () =>
		{
			var database = client.GetDatabase("test-db");
			await database.CreateContainerIfNotExistsAsync("my-container", "/partitionKey");
		};
		await act.Should().NotThrowAsync(
			"FakeCosmosHandler should support CreateContainerIfNotExistsAsync since nearly all real projects need it");
	}

	[Fact]
	public async Task FakeCosmosHandler_ShouldSupportReadDatabaseAsync()
	{
		var inMemoryContainer = new InMemoryContainer("test-container", "/partitionKey");
		using var handler = new FakeCosmosHandler(inMemoryContainer);

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var database = client.GetDatabase("test-db");
		var act = async () => await database.ReadAsync();
		await act.Should().NotThrowAsync(
			"FakeCosmosHandler should support ReadDatabaseAsync");
	}

	[Fact]
	public async Task FakeCosmosHandler_ShouldSupportDeleteDatabaseAsync()
	{
		var inMemoryContainer = new InMemoryContainer("test-container", "/partitionKey");
		using var handler = new FakeCosmosHandler(inMemoryContainer);

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var database = client.GetDatabase("test-db");
		var act = async () => await database.DeleteAsync();
		await act.Should().NotThrowAsync(
			"FakeCosmosHandler should support DeleteDatabaseAsync");
	}

	[Fact]
	public async Task FakeCosmosHandler_ShouldSupportFullSetupFlow()
	{
		// This is the most common real-world pattern:
		// 1. Create database if not exists
		// 2. Create container if not exists
		// 3. Use the container
		var inMemoryContainer = new InMemoryContainer("my-container", "/partitionKey");
		using var handler = new FakeCosmosHandler(inMemoryContainer);

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler)
			});

		// Step 1: Create database
		var dbResponse = await client.CreateDatabaseIfNotExistsAsync("test-db");
		dbResponse.StatusCode.Should().BeOneOf(
			System.Net.HttpStatusCode.Created,
			System.Net.HttpStatusCode.OK);

		// Step 2: Create container
		var database = client.GetDatabase("test-db");
		var containerResponse = await database.CreateContainerIfNotExistsAsync("my-container", "/partitionKey");
		containerResponse.StatusCode.Should().BeOneOf(
			System.Net.HttpStatusCode.Created,
			System.Net.HttpStatusCode.OK);

		// Step 3: Use the container
		var container = client.GetContainer("test-db", "my-container");
		await container.CreateItemAsync(
			new { id = "1", partitionKey = "pk1", name = "test" },
			new PartitionKey("pk1"));

		var readResponse = await container.ReadItemAsync<dynamic>("1", new PartitionKey("pk1"));
		((string)readResponse.Resource.name).Should().Be("test");
	}
}
