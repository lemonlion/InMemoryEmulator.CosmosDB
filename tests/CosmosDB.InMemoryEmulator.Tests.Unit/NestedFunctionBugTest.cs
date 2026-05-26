using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class NestedFunctionBugTest
{
	[Fact]
	public async Task ContainsLower_DirectContainer_Works()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			new { id = "1", pk = "p1", name = "T1000" },
			new PartitionKey("p1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(LOWER(c.name), @val)")
			.WithParameter("@val", "t1000");
		var iterator = container.GetItemQueryIterator<dynamic>(query);
		var results = new List<dynamic>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task ContainsLower_ViaFakeCosmosHandler_Works()
	{
		using var cosmos = InMemoryCosmos.Create("test-nested", "/pk");
		var container = cosmos.Container;
		var handler = cosmos.Handler;

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p1", name = "T1000" }),
			new PartitionKey("p1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(LOWER(c.name), @val)")
			.WithParameter("@val", "t1000");
		var results = new List<JObject>();
		using var iterator = container.GetItemQueryIterator<JObject>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("p1") });
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		// Debug: show the query that was actually sent
		var queryLog = handler.QueryLog;
		results.Should().HaveCount(1, $"Query log: [{string.Join("; ", queryLog)}]");
	}
}
