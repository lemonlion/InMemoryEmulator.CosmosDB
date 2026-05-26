using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Reproduces a bug where querying a partition that has no documents returns zero pages.
/// <c>FeedIterator.HasMoreResults</c> is immediately <c>false</c>, but real Cosmos DB
/// always returns at least one (empty) response page.
/// </summary>
public class EmptyQueryZeroPagesBugTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task QueryIterator_EmptyContainer_ShouldReturnAtLeastOnePage()
	{
		// Query an empty container - no items have been inserted
		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"));

		// Real Cosmos DB: HasMoreResults is true on the first check, even if no documents match
		iterator.HasMoreResults.Should().BeTrue(
			"real Cosmos DB always returns at least one page, even when empty");

		var page = await iterator.ReadNextAsync();
		page.Count.Should().Be(0, "the page should be empty since no documents exist");
	}

	[Fact]
	public async Task QueryIterator_NonexistentPartition_ShouldReturnAtLeastOnePage()
	{
		// Insert an item in one partition
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		// Query a different, nonexistent partition
		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = 'nonexistent'"));

		// Real Cosmos DB: HasMoreResults is true on the first check, even if no documents match
		iterator.HasMoreResults.Should().BeTrue(
			"real Cosmos DB always returns at least one page, even when no documents match");

		var page = await iterator.ReadNextAsync();
		page.Count.Should().Be(0, "the page should be empty since no documents match the filter");
	}

	[Fact]
	public async Task QueryIterator_WithQueryRequestOptions_EmptyResult_ShouldReturnAtLeastOnePage()
	{
		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = 'nonexistent'"),
			requestOptions: new QueryRequestOptions { MaxItemCount = 10 });

		// Real Cosmos DB: HasMoreResults is true on the first check, even if no documents match
		iterator.HasMoreResults.Should().BeTrue(
			"real Cosmos DB always returns at least one page, even when empty with MaxItemCount set");

		var page = await iterator.ReadNextAsync();
		page.Count.Should().Be(0, "the page should be empty since no documents match the filter");
	}

	[Fact]
	public async Task QueryIterator_StringQuery_EmptyResult_ShouldReturnAtLeastOnePage()
	{
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.partitionKey = 'nonexistent'");

		// Real Cosmos DB: HasMoreResults is true on the first check, even if no documents match
		iterator.HasMoreResults.Should().BeTrue(
			"real Cosmos DB always returns at least one page, even when using string query overload");

		var page = await iterator.ReadNextAsync();
		page.Count.Should().Be(0, "the page should be empty since no documents match the filter");
	}

	[Fact]
	public async Task QueryIterator_EmptyResult_SecondReadShouldHaveNoMoreResults()
	{
		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"));

		// First page should be available
		iterator.HasMoreResults.Should().BeTrue();
		var page = await iterator.ReadNextAsync();
		page.Count.Should().Be(0);

		// After reading the empty page, no more results
		iterator.HasMoreResults.Should().BeFalse();
	}

	[Fact]
	public async Task QueryIterator_WithItems_ThenEmptyQuery_ShouldReturnOnePage()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		// Query that matches nothing
		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name = 'ZZZ'"));

		iterator.HasMoreResults.Should().BeTrue();
		var page = await iterator.ReadNextAsync();
		page.Count.Should().Be(0);
		iterator.HasMoreResults.Should().BeFalse();
	}
}
