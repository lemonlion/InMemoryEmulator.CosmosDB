using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for concurrent/bulk operations through FakeCosmosHandler.
/// Exercises concurrent SDK operations through the handler pipeline.
/// Parity-validated: runs against both FakeCosmosHandler (in-memory) and real emulator.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerBulkTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("test-bulk", "/partitionKey");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	[Fact]
	public async Task Bulk_ConcurrentCreates_AllSucceed()
	{
		var tasks = Enumerable.Range(0, 50).Select(i =>
			_container.CreateItemAsync(
				new TestDocument { Id = $"bulk-{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

		var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var items = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			items.AddRange(page);
		}
		items.Should().HaveCount(50);
	}

	[Fact]
	public async Task Bulk_ConcurrentUpserts_AllSucceed()
	{
		// Create initial items
		for (int i = 0; i < 10; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"upsert-{i}", PartitionKey = "pk1", Name = $"Original{i}", Value = i },
				new PartitionKey("pk1"));
		}

		// Concurrent upserts to update them all
		var tasks = Enumerable.Range(0, 10).Select(i =>
			_container.UpsertItemAsync(
				new TestDocument { Id = $"upsert-{i}", PartitionKey = "pk1", Name = $"Updated{i}", Value = i * 10 },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task Bulk_MixedCreateAndUpsert_DifferentPartitions_AllSucceed()
	{
		// Seed items in different partitions
		await _container.CreateItemAsync(
			new TestDocument { Id = "u1", PartitionKey = "pk-u", Name = "Original", Value = 1 },
			new PartitionKey("pk-u"));

		// Concurrent creates across different partitions + upsert in separate partition
		var tasks = new List<Task<ItemResponse<TestDocument>>>
		{
			_container.CreateItemAsync(
				new TestDocument { Id = "new1", PartitionKey = "pk-a", Name = "New1", Value = 100 },
				new PartitionKey("pk-a")),
			_container.CreateItemAsync(
				new TestDocument { Id = "new2", PartitionKey = "pk-b", Name = "New2", Value = 200 },
				new PartitionKey("pk-b")),
			_container.UpsertItemAsync(
				new TestDocument { Id = "u1", PartitionKey = "pk-u", Name = "Updated", Value = 999 },
				new PartitionKey("pk-u")),
		};

		await Task.WhenAll(tasks);

		var read = await _container.ReadItemAsync<TestDocument>("new1", new PartitionKey("pk-a"));
		read.Resource.Name.Should().Be("New1");

		var readUpdated = await _container.ReadItemAsync<TestDocument>("u1", new PartitionKey("pk-u"));
		readUpdated.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task Bulk_ConcurrentCreates_DifferentPartitions_AllSucceed()
	{
		var tasks = Enumerable.Range(0, 30).Select(i =>
			_container.CreateItemAsync(
				new TestDocument { Id = $"pk-{i}", PartitionKey = $"pk-{i % 5}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i % 5}")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
	}
}
