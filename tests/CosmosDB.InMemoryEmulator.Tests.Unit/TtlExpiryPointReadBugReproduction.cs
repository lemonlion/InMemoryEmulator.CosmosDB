using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Bug: Items with expired TTL are not filtered out on point reads when the per-item
/// TTL is set via the standard "ttl" property (no underscore prefix).
///
/// Real Cosmos DB uses "ttl" (without underscore) as the per-item TTL property.
/// The emulator's IsExpired only checks "_ttl" (with underscore), so items stored
/// with the standard "ttl" property name are never detected as expired.
/// </summary>
public class TtlExpiryPointReadBugReproduction
{
	[Fact]
	public async Task ItemWithTTL_ShouldReturn404AfterExpiry()
	{
		var containerProps = new ContainerProperties("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1 // Container TTL enabled, per-item TTL controls
		};
		var container = new InMemoryContainer(containerProps);

		// Create item with 1-second TTL using standard "ttl" property name (no underscore)
		var item = new TtlTestDocument { Id = "expiring-item", PartitionKey = "pk1", Ttl = 1, Data = "test" };
		await container.CreateItemAsync(item, new PartitionKey("pk1"));

		// Verify it exists immediately
		var readBefore = await container.ReadItemAsync<TtlTestDocument>("expiring-item", new PartitionKey("pk1"));
		readBefore.StatusCode.Should().Be(HttpStatusCode.OK);

		// Wait for TTL to expire
		await Task.Delay(2000);

		// Point read should return 404 for expired items
		var act = async () => await container.ReadItemAsync<TtlTestDocument>("expiring-item", new PartitionKey("pk1"));
		var exception = (await act.Should().ThrowAsync<CosmosException>()).Which;
		exception.StatusCode.Should().Be(HttpStatusCode.NotFound,
			"Item with expired TTL should return 404 on point read after expiry");
	}

	[Fact]
	public async Task ItemWithTTL_StreamRead_ShouldReturn404AfterExpiry()
	{
		var containerProps = new ContainerProperties("ttl-stream-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};
		var container = new InMemoryContainer(containerProps);

		var item = new TtlTestDocument { Id = "stream-expiring", PartitionKey = "pk1", Ttl = 1, Data = "test" };
		await container.CreateItemAsync(item, new PartitionKey("pk1"));

		await Task.Delay(2000);

		var response = await container.ReadItemStreamAsync("stream-expiring", new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.NotFound,
			"Stream point read should also return 404 for expired TTL items");
	}

	[Fact]
	public async Task ItemWithTTL_Query_ShouldFilterExpiredItems()
	{
		var containerProps = new ContainerProperties("ttl-query-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};
		var container = new InMemoryContainer(containerProps);

		var item = new TtlTestDocument { Id = "query-expiring", PartitionKey = "pk1", Ttl = 1, Data = "test" };
		await container.CreateItemAsync(item, new PartitionKey("pk1"));

		await Task.Delay(2000);

		var iterator = container.GetItemQueryIterator<TtlTestDocument>(
			new QueryDefinition("SELECT * FROM c WHERE c.id = 'query-expiring'"));
		var results = new List<TtlTestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().BeEmpty("expired TTL items should be filtered from query results");
	}
}

public class TtlTestDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("partitionKey")]
	public string PartitionKey { get; set; } = default!;

	[JsonProperty("ttl")]
	public int Ttl { get; set; }

	[JsonProperty("data")]
	public string Data { get; set; } = default!;
}
