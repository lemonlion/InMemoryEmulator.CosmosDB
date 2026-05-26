using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Change Feed Cap — eviction of oldest entries when MaxChangeFeedSize is hit
// ═══════════════════════════════════════════════════════════════════════════

public class ChangeFeedCapTests
{
	[Fact]
	public void DefaultMaxChangeFeedSize_Is1000()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.MaxChangeFeedSize.Should().Be(1000);
	}

	[Fact]
	public void MaxChangeFeedSize_CanBeSetToCustomValue()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			MaxChangeFeedSize = 50
		};
		container.MaxChangeFeedSize.Should().Be(50);
	}

	[Fact]
	public async Task ChangeFeed_EvictsOldestEntries_WhenCapExceeded()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			MaxChangeFeedSize = 5
		};

		// Write 8 items — should keep only the last 5
		for (var i = 1; i <= 8; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = i.ToString(), PartitionKey = "pk", Name = $"Item{i}" },
				new PartitionKey("pk"));
		}

		container.GetChangeFeedCheckpoint().Should().Be(5);

		// Read all entries — should be items 4-8 (oldest 3 evicted)
		var iterator = container.GetChangeFeedIterator<TestDocument>(0);
		var items = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			items.AddRange(response);
		}

		items.Should().HaveCount(5);
		items[0].Id.Should().Be("4");
		items[4].Id.Should().Be("8");
	}

	[Fact]
	public async Task ChangeFeed_DoesNotEvict_WhenUnderCap()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			MaxChangeFeedSize = 10
		};

		for (var i = 1; i <= 5; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = i.ToString(), PartitionKey = "pk", Name = $"Item{i}" },
				new PartitionKey("pk"));
		}

		container.GetChangeFeedCheckpoint().Should().Be(5);

		var iterator = container.GetChangeFeedIterator<TestDocument>(0);
		var items = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			items.AddRange(response);
		}

		items.Should().HaveCount(5);
		items[0].Id.Should().Be("1");
	}

	[Fact]
	public async Task ChangeFeed_EvictsCorrectly_WithDeleteTombstones()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			MaxChangeFeedSize = 5
		};

		// Create 3 items, delete 1, create 3 more = 7 entries total, cap at 5
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "C" },
			new PartitionKey("pk"));
		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));
		// 4 entries so far, all under cap
		await container.CreateItemAsync(
			new TestDocument { Id = "4", PartitionKey = "pk", Name = "D" },
			new PartitionKey("pk"));
		// 5 entries — at cap
		await container.CreateItemAsync(
			new TestDocument { Id = "5", PartitionKey = "pk", Name = "E" },
			new PartitionKey("pk"));
		// 6th entry — evicts oldest (create "1")
		await container.CreateItemAsync(
			new TestDocument { Id = "6", PartitionKey = "pk", Name = "F" },
			new PartitionKey("pk"));
		// 7th entry — evicts 2nd oldest (create "2")

		container.GetChangeFeedCheckpoint().Should().Be(5);
	}

	[Fact]
	public async Task ChangeFeedIterator_WorksCorrectly_AfterEviction()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			MaxChangeFeedSize = 3
		};

		// Get checkpoint before adding items
		var checkpoint = container.GetChangeFeedCheckpoint();
		checkpoint.Should().Be(0);

		// Write 5 items, cap at 3 → last 3 survive
		for (var i = 1; i <= 5; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = i.ToString(), PartitionKey = "pk", Name = $"Item{i}" },
				new PartitionKey("pk"));
		}

		// Read from beginning (checkpoint 0)
		var iterator = container.GetChangeFeedIterator<TestDocument>(0);
		var items = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			items.AddRange(response);
		}

		items.Should().HaveCount(3);
		items[0].Id.Should().Be("3");
		items[1].Id.Should().Be("4");
		items[2].Id.Should().Be("5");
	}

	[Fact]
	public async Task ClearItems_ResetsChangeFeed_RegardlessOfCap()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			MaxChangeFeedSize = 5
		};

		for (var i = 1; i <= 3; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = i.ToString(), PartitionKey = "pk", Name = $"Item{i}" },
				new PartitionKey("pk"));
		}

		container.ClearItems();

		container.GetChangeFeedCheckpoint().Should().Be(0);
	}

	[Fact]
	public async Task ChangeFeed_EvictsCorrectly_WithUpserts()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			MaxChangeFeedSize = 4
		};

		// Create 2 items, upsert 1, create 2 more = 5 entries
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" },
			new PartitionKey("pk"));
		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A-updated" },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "C" },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "4", PartitionKey = "pk", Name = "D" },
			new PartitionKey("pk"));

		// 5 entries, cap 4 → oldest evicted (create "1")
		container.GetChangeFeedCheckpoint().Should().Be(4);

		var iterator = container.GetChangeFeedIterator<TestDocument>(0);
		var items = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			items.AddRange(response);
		}

		items.Should().HaveCount(4);
		// First remaining entry should be create "2"
		items[0].Id.Should().Be("2");
	}

	[Fact]
	public async Task MaxChangeFeedSize_SetToZero_DisablesEviction()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			MaxChangeFeedSize = 0
		};

		for (var i = 1; i <= 100; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = i.ToString(), PartitionKey = "pk", Name = $"Item{i}" },
				new PartitionKey("pk"));
		}

		container.GetChangeFeedCheckpoint().Should().Be(100);
	}
}
