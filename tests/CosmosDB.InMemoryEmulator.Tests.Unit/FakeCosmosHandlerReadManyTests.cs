using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for ReadMany operations through FakeCosmosHandler.
/// ReadMany is a direct method on Container — verify it works when the
/// Container is backed by FakeCosmosHandler/InMemoryContainer.
/// All tests use BackingContainer.ReadManyItemsAsync directly and cannot run
/// against the real emulator.
/// </summary>
public class FakeCosmosHandlerReadManyTests : IDisposable
{
	private readonly InMemoryContainer _inMemoryContainer;
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _container;

	public FakeCosmosHandlerReadManyTests()
	{
		_inMemoryContainer = new InMemoryContainer("test-readmany", "/partitionKey");
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_container = _client.GetContainer("db", "test-readmany");
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
	}

	private async Task SeedItems()
	{
		var docs = new[]
		{
			new TestDocument { Id = "1", PartitionKey = "pkA", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pkA", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pkB", Name = "Charlie", Value = 30 },
			new TestDocument { Id = "4", PartitionKey = "pkB", Name = "Diana", Value = 40 },
			new TestDocument { Id = "5", PartitionKey = "pkC", Name = "Eve", Value = 50 },
		};
		foreach (var doc in docs)
			await _container.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
	}

	[Fact]
	public async Task ReadMany_MultipleItems_ReturnsAll()
	{
		await SeedItems();

		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pkA")),
			("3", new PartitionKey("pkB")),
			("5", new PartitionKey("pkC")),
		};

		var response = await _handler.BackingContainer.ReadManyItemsAsync<TestDocument>(itemsToRead);
		response.Resource.Should().HaveCount(3);
		response.Resource.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie", "Eve");
	}

	[Fact]
	public async Task ReadMany_SingleItem_ReturnsOne()
	{
		await SeedItems();

		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("2", new PartitionKey("pkA")),
		};

		var response = await _handler.BackingContainer.ReadManyItemsAsync<TestDocument>(itemsToRead);
		response.Resource.Should().HaveCount(1);
		response.Resource.First().Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ReadMany_MissingItem_SkipsSilently()
	{
		await SeedItems();

		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pkA")),
			("nonexistent", new PartitionKey("pkA")),
			("3", new PartitionKey("pkB")),
		};

		var response = await _handler.BackingContainer.ReadManyItemsAsync<TestDocument>(itemsToRead);
		response.Resource.Should().HaveCount(2);
		response.Resource.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
	}

	[Fact]
	public async Task ReadMany_AcrossPartitionKeys_ReturnsAll()
	{
		await SeedItems();

		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pkA")),
			("4", new PartitionKey("pkB")),
		};

		var response = await _handler.BackingContainer.ReadManyItemsAsync<TestDocument>(itemsToRead);
		response.Resource.Should().HaveCount(2);
		response.Resource.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Diana");
	}

	[Fact]
	public async Task ReadMany_EmptyList_ReturnsEmpty()
	{
		await SeedItems();

		var response = await _handler.BackingContainer.ReadManyItemsAsync<TestDocument>(
			new List<(string id, PartitionKey pk)>());
		response.Resource.Should().BeEmpty();
	}

	[Fact]
	public async Task ReadMany_AfterDeleteThroughHandler_ReflectsDeletion()
	{
		await SeedItems();

		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pkA"));

		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pkA")),
			("2", new PartitionKey("pkA")),
		};

		var response = await _handler.BackingContainer.ReadManyItemsAsync<TestDocument>(itemsToRead);
		response.Resource.Should().HaveCount(1);
		response.Resource.First().Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ReadMany_AfterUpsertThroughHandler_ReflectsUpdate()
	{
		await SeedItems();

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pkA", Name = "Updated", Value = 999 },
			new PartitionKey("pkA"));

		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pkA")),
		};

		var response = await _handler.BackingContainer.ReadManyItemsAsync<TestDocument>(itemsToRead);
		response.Resource.Should().HaveCount(1);
		response.Resource.First().Name.Should().Be("Updated");
		response.Resource.First().Value.Should().Be(999);
	}

	[Fact]
	public async Task ReadMany_Stream_ReturnsSuccessStatus()
	{
		await SeedItems();

		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pkA")),
			("3", new PartitionKey("pkB")),
		};

		var response = await _handler.BackingContainer.ReadManyItemsStreamAsync(itemsToRead);
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().NotBeNull();
	}
}
