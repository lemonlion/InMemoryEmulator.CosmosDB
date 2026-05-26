using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Validates that the SDK pipeline returns 400 / SubStatus 1001 when the
/// partition key in the request header doesn't match the value in the document body.
/// Parity-validated: runs against both FakeCosmosHandler (in-memory) and real emulator.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerPkMismatchTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("test-pk-mismatch", "/partitionKey");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	[Fact]
	public async Task Create_PkMismatchWithBody_Returns400SubStatus1001()
	{
		var act = () => _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "body-pk", Name = "A" },
			new PartitionKey("header-pk"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest && e.SubStatusCode == 1001);
	}

	[Fact]
	public async Task Upsert_PkMismatchWithBody_Returns400SubStatus1001()
	{
		var act = () => _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "body-pk", Name = "A" },
			new PartitionKey("header-pk"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest && e.SubStatusCode == 1001);
	}

	[Fact]
	public async Task Replace_PkMismatchWithBody_Returns400SubStatus1001()
	{
		// Create a valid item first
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "a", Name = "Original" },
			new PartitionKey("a"));

		// Replace: header says "a", body says "different"
		var act = () => _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "different", Name = "Replaced" },
			"1", new PartitionKey("a"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest && e.SubStatusCode == 1001);
	}

	[Fact]
	public async Task Create_PkMatchesBody_Succeeds()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "match1", PartitionKey = "same", Name = "OK" },
			new PartitionKey("same"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Upsert_PkMatchesBody_Succeeds()
	{
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "match2", PartitionKey = "same", Name = "OK" },
			new PartitionKey("same"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Create_PkFieldMissingFromBody_Succeeds()
	{
		// When the PK field is absent from the body, Cosmos accepts the document
		var doc = new JObject { ["id"] = "no-pk-field", ["name"] = "no pk" };
		var response = await _container.CreateItemAsync(doc, new PartitionKey("any-value"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}
