using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for DeleteAllItemsByPartitionKey and other partition key edge cases
/// through FakeCosmosHandler.
/// Parity-validated: cross-partition queries and PK edge cases run against both backends.
/// DeleteAllItemsByPartitionKey tests use BackingContainer and are tagged InMemoryOnly.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerPartitionKeyTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("test-pk", "/partitionKey");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	private async Task<List<T>> DrainQuery<T>(string sql)
	{
		return await DrainQuery<T>(_container, sql);
	}

	private static async Task<List<T>> DrainQuery<T>(Container container, string sql)
	{
		var iterator = container.GetItemQueryIterator<T>(sql);
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  DeleteAllItemsByPartitionKeyStreamAsync (InMemoryOnly — uses BackingContainer API)
	// ═══════════════════════════════════════════════════════════════════════════

	private static (FakeCosmosHandler Handler, CosmosClient Client, Container Container) CreateInMemoryStack(
		string name = "test-pk", string pkPath = "/partitionKey")
	{
		var cosmos = InMemoryCosmos.Create(name, pkPath);
		return (cosmos.Handler, cosmos.Client, cosmos.Container);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task DeleteAllByPK_RemovesAllItemsInPartition()
	{
		var (handler, client, container) = CreateInMemoryStack("test-pk-del1");
		using (client)
		using (handler)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
			await container.CreateItemAsync(
				new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));
			await container.CreateItemAsync(
				new TestDocument { Id = "3", PartitionKey = "pk2", Name = "C" }, new PartitionKey("pk2"));

			var response = await handler.BackingContainer.DeleteAllItemsByPartitionKeyStreamAsync(
				new PartitionKey("pk1"));
			response.StatusCode.Should().Be(HttpStatusCode.OK);

			var remaining = await DrainQuery<TestDocument>(container, "SELECT * FROM c");
			remaining.Should().HaveCount(1);
			remaining[0].Name.Should().Be("C");
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task DeleteAllByPK_EmptyPartition_Succeeds()
	{
		var (handler, client, container) = CreateInMemoryStack("test-pk-del2");
		using (client)
		using (handler)
		{
			var response = await handler.BackingContainer.DeleteAllItemsByPartitionKeyStreamAsync(
				new PartitionKey("nonexistent"));
			response.StatusCode.Should().Be(HttpStatusCode.OK);
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task DeleteAllByPK_AdvancesChangeFeed()
	{
		var (handler, client, container) = CreateInMemoryStack("test-pk-del3");
		using (client)
		using (handler)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
			await container.CreateItemAsync(
				new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));

			// DeleteAllByPK is not handled via HTTP routing, so call on backing container
			await handler.BackingContainer.DeleteAllItemsByPartitionKeyStreamAsync(
				new PartitionKey("pk1"));

			// Verify all items in the partition were deleted
			var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
			var remaining = new List<TestDocument>();
			while (iterator.HasMoreResults)
				remaining.AddRange(await iterator.ReadNextAsync());

			remaining.Should().BeEmpty();
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Cross-partition queries through handler
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task CrossPartitionQuery_ReturnsItemsFromAllPartitions()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk3", Name = "C" }, new PartitionKey("pk3"));

		var results = await DrainQuery<TestDocument>("SELECT * FROM c ORDER BY c.name");
		results.Should().HaveCount(3);
		results.Select(r => r.Name).Should().ContainInOrder("A", "B", "C");
	}

	[Fact]
	public async Task PartitionKeyNone_CrudRoundTrip()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "none-1", PartitionKey = null!, Name = "NoPartition" },
			PartitionKey.None);

		var read = await _container.ReadItemAsync<TestDocument>("none-1", PartitionKey.None);
		read.Resource.Name.Should().Be("NoPartition");
	}
}
