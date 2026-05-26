using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for change feed through FakeCosmosHandler.
/// CRUD operations go through the SDK HTTP pipeline, then change feed is verified
/// via backing container's checkpoint-based API and SDK iterators.
/// Note: SDK's Container.GetChangeFeedIterator doesn't work through FakeCosmosHandler
/// because the handler doesn't implement the A-IM change feed HTTP protocol.
/// All tests in this class use InMemoryContainer-specific APIs (BackingContainer,
/// checkpoint, change feed processor) and cannot run against the real emulator.
/// </summary>
public class FakeCosmosHandlerChangeFeedTests : IDisposable
{
	private readonly InMemoryContainer _inMemoryContainer;
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _container;

	public FakeCosmosHandlerChangeFeedTests()
	{
		_inMemoryContainer = new InMemoryContainer("test-changefeed", "/partitionKey");
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
		_container = _client.GetContainer("db", "test-changefeed");
	}

	public void Dispose()
	{
		_client.Dispose();
		_handler.Dispose();
	}

	private async Task<List<T>> DrainIterator<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}

	private async Task<List<T>> DrainChangeFeed<T>(FeedIterator<T> iterator, int maxPages = 10)
	{
		var results = new List<T>();
		int pages = 0;
		while (iterator.HasMoreResults && pages < maxPages)
		{
			try
			{
				var page = await iterator.ReadNextAsync();
				results.AddRange(page);
				pages++;
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotModified)
			{
				break;
			}
		}
		return results;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  CRUD through handler -> change feed via backing container
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ChangeFeed_CreateThroughHandler_VisibleInBackingContainer()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(0);
		var items = await DrainIterator(iterator);
		items.Should().HaveCount(2);
	}

	[Fact]
	public async Task ChangeFeed_Incremental_TracksNewItemsAfterCheckpoint()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var checkpoint = _inMemoryContainer.GetChangeFeedCheckpoint();

		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(checkpoint);
		var newItems = await DrainIterator(iterator);
		newItems.Should().HaveCount(1);
		newItems[0].Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ChangeFeed_UpsertThroughHandler_RecordsChange()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
			new PartitionKey("pk1"));

		var checkpoint = _inMemoryContainer.GetChangeFeedCheckpoint();

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "After" },
			new PartitionKey("pk1"));

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(checkpoint);
		var changes = await DrainIterator(iterator);
		changes.Should().HaveCount(1);
		changes[0].Name.Should().Be("After");
	}

	[Fact]
	public async Task ChangeFeed_DeleteThroughHandler_AdvancesCheckpoint()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
			new PartitionKey("pk1"));

		var checkpoint = _inMemoryContainer.GetChangeFeedCheckpoint();

		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var newCheckpoint = _inMemoryContainer.GetChangeFeedCheckpoint();
		newCheckpoint.Should().BeGreaterThan(checkpoint);
	}

	[Fact]
	public async Task ChangeFeed_PatchThroughHandler_RecordsUpdatedDocument()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before", Value = 10 },
			new PartitionKey("pk1"));

		var checkpoint = _inMemoryContainer.GetChangeFeedCheckpoint();

		await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "After")]);

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(checkpoint);
		var changes = await DrainIterator(iterator);
		changes.Should().HaveCount(1);
		changes[0].Name.Should().Be("After");
	}

	[Fact]
	public async Task ChangeFeed_ReplaceThroughHandler_RecordsFullDocument()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Before" },
			new PartitionKey("pk1"));

		var checkpoint = _inMemoryContainer.GetChangeFeedCheckpoint();

		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "After", Value = 99 },
			"1", new PartitionKey("pk1"));

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(checkpoint);
		var changes = await DrainIterator(iterator);
		changes.Should().HaveCount(1);
		changes[0].Name.Should().Be("After");
		changes[0].Value.Should().Be(99);
	}

	[Fact]
	public async Task ChangeFeed_BatchThroughHandler_TracksAllOperations()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "b1", PartitionKey = "pk1", Name = "Batch1" });
		batch.CreateItem(new TestDocument { Id = "b2", PartitionKey = "pk1", Name = "Batch2" });
		await batch.ExecuteAsync();

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(0);
		var changes = await DrainIterator(iterator);
		changes.Should().HaveCount(2);
		changes.Select(c => c.Name).Should().BeEquivalentTo("Batch1", "Batch2");
	}

	[Fact]
	public async Task ChangeFeed_ManyItemsThroughHandler_AllTracked()
	{
		for (int i = 0; i < 20; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"item-{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
		}

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(0);
		var allChanges = await DrainIterator(iterator);
		allChanges.Should().HaveCount(20);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Change feed via backing container's SDK iterator
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ChangeFeed_BackingContainerSdkIterator_Beginning_ReturnsAllItems()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob" },
			new PartitionKey("pk2"));

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);

		var results = await DrainChangeFeed(iterator);
		results.Should().HaveCount(2);
		results.Select(r => r.Name).Should().Contain("Alice");
		results.Select(r => r.Name).Should().Contain("Bob");
	}

	[Fact]
	public async Task ChangeFeed_BackingContainerSdkIterator_EmptyContainer_Returns304()
	{
		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);

		var results = await DrainChangeFeed(iterator);
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ChangeFeed_BackingContainerSdkIterator_WithPartitionKey_ScopesToPartition()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob" },
			new PartitionKey("pk2"));

		var iterator = _inMemoryContainer.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(FeedRange.FromPartitionKey(new PartitionKey("pk1"))),
			ChangeFeedMode.Incremental);

		var results = await DrainChangeFeed(iterator);
		results.Should().OnlyContain(r => r.PartitionKey == "pk1");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Change feed processor — uses backing container
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ChangeFeed_Processor_ReceivesItemsCreatedThroughHandler()
	{
		var received = new List<TestDocument>();
		var processor = _inMemoryContainer.GetChangeFeedProcessorBuilder<TestDocument>(
				"test-processor",
				(context, changes, ct) =>
				{
					received.AddRange(changes);
					return Task.CompletedTask;
				})
			.WithInMemoryLeaseContainer()
			.WithInstanceName("instance1")
			.WithStartTime(DateTime.MinValue.ToUniversalTime())
			.Build();

		await processor.StartAsync();

		// Create through the handler (HTTP pipeline)
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ProcessorItem" },
			new PartitionKey("pk1"));

		// Allow processor time to pick up the item
		await Task.Delay(1000);
		await processor.StopAsync();

		received.Should().Contain(r => r.Name == "ProcessorItem");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  All Versions and Deletes (via checkpoint-based API)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ChangeFeed_AllVersions_ShowsCreateAndUpdate()
	{
		var checkpoint = _inMemoryContainer.GetChangeFeedCheckpoint();

		await _container.CreateItemAsync(
			new TestDocument { Id = "v1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		await _container.UpsertItemAsync(
			new TestDocument { Id = "v1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"));

		var changes = new List<JObject>();
		var iterator = _inMemoryContainer.GetChangeFeedIterator<JObject>(checkpoint);
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			changes.AddRange(page);
		}

		// Should see both the create and the update as separate entries
		changes.Should().HaveCount(2);
		changes[0]["name"]!.Value<string>().Should().Be("Original");
		changes[1]["name"]!.Value<string>().Should().Be("Updated");
	}

	[Fact]
	public async Task ChangeFeed_AllVersions_ShowsDeleteTombstone()
	{
		var checkpoint = _inMemoryContainer.GetChangeFeedCheckpoint();

		await _container.CreateItemAsync(
			new TestDocument { Id = "d1", PartitionKey = "pk1", Name = "WillBeDeleted" },
			new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("d1", new PartitionKey("pk1"));

		var changes = new List<JObject>();
		var iterator = _inMemoryContainer.GetChangeFeedIterator<JObject>(checkpoint);
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			changes.AddRange(page);
		}

		changes.Should().HaveCount(2);
		changes[0]["name"]!.Value<string>().Should().Be("WillBeDeleted");
		changes[1]["_deleted"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task ChangeFeed_AllVersions_MultipleOps_OrderIsPreserved()
	{
		var checkpoint = _inMemoryContainer.GetChangeFeedCheckpoint();

		await _container.CreateItemAsync(
			new TestDocument { Id = "m1", PartitionKey = "pk1", Name = "V1" },
			new PartitionKey("pk1"));
		await _container.UpsertItemAsync(
			new TestDocument { Id = "m1", PartitionKey = "pk1", Name = "V2" },
			new PartitionKey("pk1"));
		await _container.UpsertItemAsync(
			new TestDocument { Id = "m1", PartitionKey = "pk1", Name = "V3" },
			new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("m1", new PartitionKey("pk1"));

		var changes = new List<JObject>();
		var iterator = _inMemoryContainer.GetChangeFeedIterator<JObject>(checkpoint);
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			changes.AddRange(page);
		}

		changes.Should().HaveCount(4);
		changes[0]["name"]!.Value<string>().Should().Be("V1");
		changes[1]["name"]!.Value<string>().Should().Be("V2");
		changes[2]["name"]!.Value<string>().Should().Be("V3");
		changes[3]["_deleted"]!.Value<bool>().Should().BeTrue();
	}
}
