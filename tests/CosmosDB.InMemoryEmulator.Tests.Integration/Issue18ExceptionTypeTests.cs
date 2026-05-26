using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Integration-level reproduction tests for GitHub Issue #18:
/// "Bug: InMemoryCosmosException not assignable to CosmosException"
///
/// These verify the fix through the full SDK HTTP pipeline via FakeCosmosHandler,
/// ensuring exceptions are exactly <see cref="CosmosException"/> (not a subclass)
/// regardless of the test target (in-memory or real emulator).
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Issue18ExceptionTypeTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("issue18", "/pk");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	[Fact]
	public async Task ReadItem_NotFound_CatchCosmosException_Works()
	{
		bool caughtAsCosmosException = false;
		try
		{
			await _container.ReadItemAsync<dynamic>("nonexistent", new PartitionKey("pk1"));
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			caughtAsCosmosException = true;
		}

		caughtAsCosmosException.Should().BeTrue(
			"CosmosException should be catchable with status code filter");
	}

	[Fact]
	public async Task ReadItem_NotFound_AssertThrowsExactType()
	{
		var exception = await Assert.ThrowsAsync<CosmosException>(async () =>
			await _container.ReadItemAsync<dynamic>("nonexistent", new PartitionKey("pk1")));

		exception.StatusCode.Should().Be(HttpStatusCode.NotFound);
		exception.GetType().Should().Be(typeof(CosmosException),
			"thrown exception must be exactly CosmosException, not a subclass");
	}

	[Fact]
	public async Task ReadItem_NotFound_FluentThrowAsync()
	{
		Func<Task> act = () => _container.ReadItemAsync<dynamic>("nonexistent", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.And.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task CreateItem_Duplicate_CatchCosmosException_Works()
	{
		await _container.CreateItemAsync(new { id = "dup1", pk = "pk1" }, new PartitionKey("pk1"));

		bool caughtConflict = false;
		try
		{
			await _container.CreateItemAsync(new { id = "dup1", pk = "pk1" }, new PartitionKey("pk1"));
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
		{
			caughtConflict = true;
		}

		caughtConflict.Should().BeTrue(
			"Duplicate create should throw CosmosException with 409 Conflict");
	}

	[Fact]
	public async Task CreateItem_Duplicate_AssertThrowsExactType()
	{
		await _container.CreateItemAsync(new { id = "dup2", pk = "pk1" }, new PartitionKey("pk1"));

		var exception = await Assert.ThrowsAsync<CosmosException>(async () =>
			await _container.CreateItemAsync(new { id = "dup2", pk = "pk1" }, new PartitionKey("pk1")));

		exception.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task DeleteItem_NotFound_CatchCosmosException_Works()
	{
		bool caughtNotFound = false;
		try
		{
			await _container.DeleteItemAsync<dynamic>("nonexistent", new PartitionKey("pk1"));
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			caughtNotFound = true;
		}

		caughtNotFound.Should().BeTrue(
			"Delete of nonexistent item should throw CosmosException with 404");
	}

	[Fact]
	public async Task ReplaceItem_NotFound_CatchCosmosException_Works()
	{
		bool caughtNotFound = false;
		try
		{
			await _container.ReplaceItemAsync(
				new { id = "nonexistent", pk = "pk1" }, "nonexistent", new PartitionKey("pk1"));
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			caughtNotFound = true;
		}

		caughtNotFound.Should().BeTrue(
			"Replace of nonexistent item should throw CosmosException with 404");
	}

	[Fact]
	public async Task PatchItem_NotFound_CatchCosmosException_Works()
	{
		bool caughtNotFound = false;
		try
		{
			await _container.PatchItemAsync<dynamic>(
				"nonexistent", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "test") });
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			caughtNotFound = true;
		}

		caughtNotFound.Should().BeTrue(
			"Patch of nonexistent item should throw CosmosException with 404");
	}
}
