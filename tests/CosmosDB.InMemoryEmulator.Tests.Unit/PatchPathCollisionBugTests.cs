using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class PatchPathCollisionBugTests
{
	public class ParentDoc
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("partitionKey")] public string PartitionKey { get; set; } = "";
		[JsonProperty("transactions")] public Dictionary<Guid, ItemValue> Transactions { get; set; } = new();
		[JsonProperty("runs")] public List<RunEntry> Runs { get; set; } = new();
	}

	public class RunEntry
	{
		[JsonProperty("status")] public string Status { get; set; } = "";
		[JsonProperty("transactions")] public List<ItemValue> Transactions { get; set; } = new();
	}

	public class ItemValue
	{
		[JsonProperty("name")] public string Name { get; set; } = "";
	}

	[Fact]
	public async Task Set_RootAndNestedWithSameTerminalName_DoesNotCorruptRoot()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create doc with empty transactions dict and empty runs list
		var doc = new ParentDoc
		{
			Id = "doc-1",
			PartitionKey = "pk-1",
			Transactions = new Dictionary<Guid, ItemValue>(),
			Runs = new List<RunEntry>()
		};
		await container.CreateItemAsync(doc, new PartitionKey("pk-1"));

		// Add a run entry
		var run = new RunEntry { Status = "InProgress", Transactions = new List<ItemValue>() };
		await container.PatchItemAsync<ParentDoc>(
			"doc-1", new PartitionKey("pk-1"),
			[PatchOperation.Add("/runs/0", run)]);

		// Patch both root /transactions AND nested /runs/0/transactions
		var txId = Guid.NewGuid();
		var allTransactions = new Dictionary<Guid, ItemValue>
		{
			[txId] = new ItemValue { Name = "Tx1" }
		};

		await container.PatchItemAsync<ParentDoc>(
			"doc-1", new PartitionKey("pk-1"),
			[
				PatchOperation.Set("/transactions", allTransactions),
				PatchOperation.Set("/runs/0/status", "Completed"),
				PatchOperation.Set("/runs/0/transactions", new List<ItemValue>
				{
					new() { Name = "Tx1" }
				})
			]);

		// Read back — should not throw
		var readResponse = await container.ReadItemAsync<ParentDoc>(
			"doc-1", new PartitionKey("pk-1"));

		// Root transactions should be a dictionary, not a list
		readResponse.Resource.Transactions.Should().HaveCount(1);
		readResponse.Resource.Transactions.Values.First().Name.Should().Be("Tx1");

		// Nested transactions should be a list
		readResponse.Resource.Runs.Should().HaveCount(1);
		readResponse.Resource.Runs[0].Status.Should().Be("Completed");
		readResponse.Resource.Runs[0].Transactions.Should().HaveCount(1);
		readResponse.Resource.Runs[0].Transactions[0].Name.Should().Be("Tx1");
	}
}
