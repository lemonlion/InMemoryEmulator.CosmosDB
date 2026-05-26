using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Reproduction tests for GitHub Issue #18:
/// "Bug: InMemoryCosmosException not assignable to CosmosException"
/// </summary>
public class Issue18ReproductionTests
{
	[Fact]
	public async Task ReadItemAsync_NonExistent_CatchCosmosException_Works()
	{
		// Arrange — exact scenario from issue
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		// Act & Assert — catch (CosmosException) with when clause
		bool caughtAsCosmosException = false;
		try
		{
			await container.ReadItemAsync<dynamic>("nonexistent", new PartitionKey("pk1"));
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			caughtAsCosmosException = true;
		}

		caughtAsCosmosException.Should().BeTrue("InMemoryCosmosException should be catchable as CosmosException");
	}

	[Fact]
	public async Task ReadItemAsync_NonExistent_AssertThrowsCosmosException_Works()
	{
		// Arrange
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		// Act & Assert — Assert.ThrowsAsync<CosmosException> pattern from issue
		var exception = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.ReadItemAsync<dynamic>("nonexistent", new PartitionKey("pk1")));

		exception.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task CreateItemAsync_Duplicate_CatchCosmosException_Works()
	{
		// Arrange
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		await container.CreateItemAsync(new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));

		// Act & Assert — duplicate create should throw 409
		bool caughtConflict = false;
		try
		{
			await container.CreateItemAsync(new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
		{
			caughtConflict = true;
		}

		caughtConflict.Should().BeTrue("Duplicate create should be catchable as CosmosException with 409");
	}

	[Fact]
	public async Task DeleteItemAsync_NonExistent_CatchCosmosException_Works()
	{
		// Arrange
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		// Act & Assert
		bool caughtNotFound = false;
		try
		{
			await container.DeleteItemAsync<dynamic>("nonexistent", new PartitionKey("pk1"));
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			caughtNotFound = true;
		}

		caughtNotFound.Should().BeTrue("Delete of nonexistent item should be catchable as CosmosException with 404");
	}

	[Fact]
	public async Task ReplaceItemAsync_NonExistent_CatchCosmosException_Works()
	{
		// Arrange
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		// Act & Assert
		bool caughtNotFound = false;
		try
		{
			await container.ReplaceItemAsync(new { id = "nonexistent", pk = "pk1" }, "nonexistent", new PartitionKey("pk1"));
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			caughtNotFound = true;
		}

		caughtNotFound.Should().BeTrue("Replace of nonexistent item should be catchable as CosmosException with 404");
	}

	[Fact]
	public async Task PatchItemAsync_NonExistent_CatchCosmosException_Works()
	{
		// Arrange
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		// Act & Assert
		bool caughtNotFound = false;
		try
		{
			await container.PatchItemAsync<dynamic>("nonexistent", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "test") });
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			caughtNotFound = true;
		}

		caughtNotFound.Should().BeTrue("Patch of nonexistent item should be catchable as CosmosException with 404");
	}

	[Fact]
	public void InMemoryCosmosException_Creates_CosmosException()
	{
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);
		(ex is CosmosException).Should().BeTrue();
		ex.GetType().Should().Be(typeof(CosmosException), "thrown exception should be exactly CosmosException, not a subclass");
		ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
		ex.Diagnostics.Should().BeNull("Diagnostics is null because the SDK does not expose a public way to set it");
	}

	[Fact]
	public async Task CreateDatabaseAsync_Duplicate_CatchCosmosException_Works()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseAsync("testdb");

		bool caughtConflict = false;
		try
		{
			await client.CreateDatabaseAsync("testdb");
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
		{
			caughtConflict = true;
		}

		caughtConflict.Should().BeTrue("Duplicate database creation should throw CosmosException with 409");
	}
}
