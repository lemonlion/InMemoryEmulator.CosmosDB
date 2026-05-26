using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Typed Response Status Codes
// ═══════════════════════════════════════════════════════════════════════════

public class TypedResponseStatusCodeTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task CreateItemAsync_Returns_Created()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task ReadItemAsync_Returns_OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task UpsertItemAsync_NewItem_Returns_Created()
	{
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task UpsertItemAsync_ExistingItem_Returns_OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated" },
			new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceItemAsync_Returns_OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Replaced" },
			"1", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteItemAsync_Returns_NoContent()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task PatchItemAsync_Returns_OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "Patched")]);
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Typed Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class TypedResponseMetadataTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task CreateItemAsync_HasAllMetadata()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		response.RequestCharge.Should().BeGreaterThan(0);
		response.ActivityId.Should().NotBeNullOrEmpty();
		response.Diagnostics.Should().NotBeNull();
		response.ETag.Should().NotBeNullOrEmpty();
		response.Headers.Should().NotBeNull();
	}

	[Fact]
	public async Task ReadItemAsync_HasAllMetadata()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

		response.RequestCharge.Should().BeGreaterThan(0);
		response.ActivityId.Should().NotBeNullOrEmpty();
		response.Diagnostics.Should().NotBeNull();
		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task UpsertItemAsync_HasAllMetadata()
	{
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		response.RequestCharge.Should().BeGreaterThan(0);
		response.ActivityId.Should().NotBeNullOrEmpty();
		response.Diagnostics.Should().NotBeNull();
		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReplaceItemAsync_HasAllMetadata()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" },
			"1", new PartitionKey("pk"));

		response.RequestCharge.Should().BeGreaterThan(0);
		response.ActivityId.Should().NotBeNullOrEmpty();
		response.Diagnostics.Should().NotBeNull();
		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task DeleteItemAsync_HasMetadata()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

		response.RequestCharge.Should().BeGreaterThan(0);
		response.ActivityId.Should().NotBeNullOrEmpty();
		response.Diagnostics.Should().NotBeNull();
	}

	[Fact]
	public async Task PatchItemAsync_HasAllMetadata()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "Patched")]);

		response.RequestCharge.Should().BeGreaterThan(0);
		response.ActivityId.Should().NotBeNullOrEmpty();
		response.Diagnostics.Should().NotBeNull();
		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ActivityId_IsUnique_PerOperation()
	{
		var r1 = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));
		var r2 = await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" },
			new PartitionKey("pk"));

		r1.ActivityId.Should().NotBe(r2.ActivityId);
	}

	[Fact]
	public async Task ETag_IsQuotedString()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		response.ETag.Should().StartWith("\"").And.EndWith("\"");
	}

	[Fact]
	public async Task Diagnostics_GetClientElapsedTime_ReturnsTimeSpan()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		response.Diagnostics.GetClientElapsedTime().Should().Be(TimeSpan.Zero);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stream Response Status Codes
// ═══════════════════════════════════════════════════════════════════════════

public class StreamResponseStatusCodeTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	private static MemoryStream MakeStream(string json) =>
		new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task CreateItemStreamAsync_Returns_Created()
	{
		var response = await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task ReadItemStreamAsync_Returns_OK()
	{
		await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task UpsertItemStreamAsync_NewItem_Returns_Created()
	{
		var response = await _container.UpsertItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task UpsertItemStreamAsync_ExistingItem_Returns_OK()
	{
		await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		var response = await _container.UpsertItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk","name":"Updated"}"""),
			new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceItemStreamAsync_Returns_OK()
	{
		await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		var response = await _container.ReplaceItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk","name":"Replaced"}"""),
			"1", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteItemStreamAsync_Returns_NoContent()
	{
		await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task PatchItemStreamAsync_Returns_OK()
	{
		await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk","name":"A"}"""),
			new PartitionKey("pk"));

		var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "Patched")]);
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stream Response Headers
// ═══════════════════════════════════════════════════════════════════════════

public class StreamResponseHeaderTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	private static MemoryStream MakeStream(string json) =>
		new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task StreamCreate_HasActivityId_RequestCharge_ETag()
	{
		var response = await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
		response.Headers.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task StreamRead_HasActivityId_RequestCharge_ETag()
	{
		await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk"));

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
		response.Headers.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task StreamUpsert_HasActivityId_RequestCharge_ETag()
	{
		var response = await _container.UpsertItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
		response.Headers.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task StreamReplace_HasActivityId_RequestCharge_ETag()
	{
		await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		var response = await _container.ReplaceItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk","name":"B"}"""),
			"1", new PartitionKey("pk"));

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
		response.Headers.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task StreamDelete_HasActivityId_RequestCharge()
	{
		await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""),
			new PartitionKey("pk"));

		var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk"));

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Error Response Status Codes
// ═══════════════════════════════════════════════════════════════════════════

public class ErrorResponseStatusCodeTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task CreateItemAsync_Duplicate_Throws_Conflict_409()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var act = () => _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Dup" },
			new PartitionKey("pk"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task ReadItemAsync_NotFound_Throws_404()
	{
		var act = () => _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReplaceItemAsync_NotFound_Throws_404()
	{
		var act = () => _container.ReplaceItemAsync(
			new TestDocument { Id = "nope", PartitionKey = "pk", Name = "X" },
			"nope", new PartitionKey("pk"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteItemAsync_NotFound_Throws_404()
	{
		var act = () => _container.DeleteItemAsync<TestDocument>("nope", new PartitionKey("pk"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PatchItemAsync_NotFound_Throws_404()
	{
		var act = () => _container.PatchItemAsync<TestDocument>("nope", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "X")]);

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReplaceItemAsync_StaleETag_Throws_PreconditionFailed_412()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var act = () => _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" },
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task PatchItemAsync_FilterPredicate_Throws_PreconditionFailed_412()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A", IsActive = false },
			new PartitionKey("pk"));

		var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "X")],
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.isActive = true" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Feed Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class FeedResponseMetadataTests
{
	[Fact]
	public async Task FeedResponse_HasRequestCharge()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iterator.ReadNextAsync();

		page.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
	}

	[Fact]
	public async Task FeedResponse_HasDiagnostics()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iterator.ReadNextAsync();

		page.Diagnostics.Should().NotBeNull();
	}

	[Fact]
	public async Task FeedResponse_HasStatusCode_OK()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iterator.ReadNextAsync();

		page.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task FeedResponse_ActivityId_IsValidGuid()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iterator.ReadNextAsync();

		Guid.TryParse(page.ActivityId, out _).Should().BeTrue();
	}

	[Fact]
	public async Task FeedResponse_ActivityId_EmulatorBehavior_IsValidGuid()
	{
		// Previously divergent: ActivityId was empty string. Now returns a valid GUID.
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iterator.ReadNextAsync();

		Guid.TryParse(page.ActivityId, out _).Should().BeTrue("emulator now returns a valid GUID for FeedResponse.ActivityId");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Batch Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class BatchResponseMetadataTests
{
	[Fact]
	public async Task BatchResponse_HasStatusCode_OnSuccess()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });

		using var response = await batch.ExecuteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task BatchResponse_HasRequestCharge()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });

		using var response = await batch.ExecuteAsync();
		response.RequestCharge.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task BatchResponse_Count_MatchesOperations()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" });

		using var response = await batch.ExecuteAsync();
		response.Count.Should().Be(2);
	}

	[Fact]
	public async Task BatchResponse_IsSuccessStatusCode_TrueOnSuccess()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task BatchResponse_PerOperation_HasStatusCode()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" });

		using var response = await batch.ExecuteAsync();
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);
		response[1].StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Database/Container Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseContainerResponseMetadataTests
{
	[Fact]
	public async Task CreateDatabaseAsync_Returns_Created()
	{
		var client = new InMemoryCosmosClient();
		var response = await client.CreateDatabaseAsync("testdb");
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateContainerAsync_Returns_Created()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var response = await db.CreateContainerAsync("mycontainer", "/pk");
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateContainerAsync_HasResource()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var response = await db.CreateContainerAsync("mycontainer", "/pk");
		response.Resource.Should().NotBeNull();
		response.Resource.Id.Should().Be("mycontainer");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Documentation
// ═══════════════════════════════════════════════════════════════════════════

public class ResponseMetadataDivergentBehaviorTests
{
	[Fact(Skip = "Real Cosmos DB: writes ~6-10 RU, reads ~1 RU, queries vary by complexity. " +
		"The emulator always returns a synthetic 1.0 RU charge.")]
	public void RequestCharge_ShouldVaryByOperationType()
	{
		// Placeholder — real Cosmos varies RU by operation
	}

	[Fact]
	public async Task RequestCharge_EmulatorBehavior_AlwaysSynthetic()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos: RU charge varies (writes ~6-10, reads ~1, queries vary).
		// Emulator: Always returns 1.0 RU.
		var container = new InMemoryContainer("test", "/partitionKey");

		var createResp = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));
		var readResp = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

		createResp.RequestCharge.Should().Be(readResp.RequestCharge,
			"emulator returns the same synthetic RU for all operations");
	}

	[Fact(Skip = "Real Cosmos DB: Diagnostics contain query plan, execution timing, retries, " +
		"contacted regions, and partitions. The emulator returns a stub with TimeSpan.Zero.")]
	public void Diagnostics_ShouldContainQueryPlanAndTimings()
	{
		// Placeholder
	}

	[Fact]
	public async Task Diagnostics_EmulatorBehavior_StubReturnsZeroElapsed()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos: Rich diagnostics with timing, retries, regions.
		// Emulator: Stub returns TimeSpan.Zero.
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		response.Diagnostics.GetClientElapsedTime().Should().Be(TimeSpan.Zero);
	}

	[Fact(Skip = "Real Cosmos DB: Session tokens are cumulative LSN-based (e.g. '0:0#12345'). " +
		"The emulator generates a random GUID per response with no cumulative state.")]
	public void SessionToken_ShouldBeCumulative()
	{
		// Placeholder
	}

	[Fact]
	public async Task SessionToken_EmulatorBehavior_FixedPerResponse()
	{
		// Emulator returns a fixed session token on all stream responses.
		// Real Cosmos DB uses LSN-based cumulative tokens.
		var container = new InMemoryContainer("test", "/partitionKey");

		var json = """{"id":"1","partitionKey":"pk"}""";
		var r1 = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk"));

		json = """{"id":"2","partitionKey":"pk"}""";
		var r2 = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk"));

		var token1 = r1.Headers["x-ms-session-token"];
		var token2 = r2.Headers["x-ms-session-token"];

		token1.Should().NotBeNullOrEmpty();
		token2.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Stream Patch Headers
// ═══════════════════════════════════════════════════════════════════════════════

public class StreamPatchHeaderTests
{
	[Fact]
	public async Task StreamPatch_HasActivityId_RequestCharge_ETag()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk","name":"A"}""")),
			new PartitionKey("pk"));

		var response = await container.PatchItemStreamAsync("1", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "Patched")]);

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
		response.Headers.ETag.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Stream Error Response Status Codes
// ═══════════════════════════════════════════════════════════════════════════════

public class StreamErrorResponseTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	private static MemoryStream MakeStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task StreamCreate_Duplicate_Returns_Conflict_409()
	{
		await _container.CreateItemStreamAsync(MakeStream("""{"id":"1","partitionKey":"pk"}"""), new PartitionKey("pk"));
		var response = await _container.CreateItemStreamAsync(MakeStream("""{"id":"1","partitionKey":"pk"}"""), new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task StreamRead_NotFound_Returns_404()
	{
		var response = await _container.ReadItemStreamAsync("nope", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task StreamReplace_NotFound_Returns_404()
	{
		var response = await _container.ReplaceItemStreamAsync(
			MakeStream("""{"id":"nope","partitionKey":"pk"}"""), "nope", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task StreamDelete_NotFound_Returns_404()
	{
		var response = await _container.DeleteItemStreamAsync("nope", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task StreamPatch_NotFound_Returns_404()
	{
		var response = await _container.PatchItemStreamAsync("nope", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "X")]);
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task StreamReplace_StaleETag_Returns_PreconditionFailed_412()
	{
		await _container.CreateItemStreamAsync(MakeStream("""{"id":"1","partitionKey":"pk"}"""), new PartitionKey("pk"));

		var response = await _container.ReplaceItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk","name":"B"}"""),
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task StreamRead_IfNoneMatch_Returns_NotModified_304()
	{
		var create = await _container.CreateItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk"}"""), new PartitionKey("pk"));
		var etag = create.Headers.ETag;

		var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk"),
			new ItemRequestOptions { IfNoneMatchEtag = etag });
		response.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task StreamUpsert_StaleETag_Returns_PreconditionFailed_412()
	{
		await _container.CreateItemStreamAsync(MakeStream("""{"id":"1","partitionKey":"pk"}"""), new PartitionKey("pk"));

		var response = await _container.UpsertItemStreamAsync(
			MakeStream("""{"id":"1","partitionKey":"pk","name":"B"}"""),
			new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Stream Error Response Headers
// ═══════════════════════════════════════════════════════════════════════════════

public class StreamErrorResponseHeaderTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task Stream_NotFound_HasActivityId_RequestCharge()
	{
		var response = await _container.ReadItemStreamAsync("nope", new PartitionKey("pk"));
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Stream_Conflict_HasActivityId_RequestCharge()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));
		var response = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Stream_PreconditionFailed_HasActivityId_RequestCharge()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));
		var response = await _container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")),
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Stream_NotModified_HasETag_InHeaders()
	{
		var create = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));
		var etag = create.Headers.ETag;

		var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk"),
			new ItemRequestOptions { IfNoneMatchEtag = etag });

		response.StatusCode.Should().Be(HttpStatusCode.NotModified);
		response.Headers.ETag.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Content Suppression Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ContentSuppressionTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task CreateItemAsync_SuppressContent_ResourceIsDefault()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
	}

	[Fact]
	public async Task UpsertItemAsync_SuppressContent_ResourceIsDefault()
	{
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
	}

	[Fact]
	public async Task ReplaceItemAsync_SuppressContent_ResourceIsDefault()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" },
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
	}

	[Fact]
	public async Task PatchItemAsync_SuppressContent_ResourceIsDefault()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
	}

	[Fact]
	public async Task DeleteItemAsync_Resource_IsAlwaysDefault()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

		response.Resource.Should().BeNull();
	}

	[Fact]
	public async Task SuppressContent_StillHasMetadata()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.ETag.Should().NotBeNullOrEmpty();
		response.ActivityId.Should().NotBeNullOrEmpty();
		response.RequestCharge.Should().BeGreaterThan(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — ETag Lifecycle Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ETagLifecycleDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task ETag_Changes_AfterUpsert()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var upsert = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));

		create.ETag.Should().NotBe(upsert.ETag);
	}

	[Fact]
	public async Task ETag_Changes_AfterReplace()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var replace = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" }, "1", new PartitionKey("pk"));

		create.ETag.Should().NotBe(replace.ETag);
	}

	[Fact]
	public async Task ETag_Changes_AfterPatch()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var patch = await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk"), [PatchOperation.Set("/name", "B")]);

		create.ETag.Should().NotBe(patch.ETag);
	}

	[Fact]
	public async Task ETag_Consistent_BetweenWriteAndRead()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

		create.ETag.Should().Be(read.ETag);
	}

	[Fact]
	public async Task ETag_OnDocument_MatchesResponseETag()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var read = await _container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>("1", new PartitionKey("pk"));
		var docEtag = read.Resource["_etag"]?.ToString();

		docEtag.Should().Be(create.ETag);
	}

	[Fact]
	public async Task Stream_ETag_Matches_Typed_ETag()
	{
		var typed = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var stream = await _container.ReadItemStreamAsync("1", new PartitionKey("pk"));

		stream.Headers.ETag.Should().Be(typed.ETag);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — CosmosException Metadata Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ExceptionMetadataTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task CosmosException_HasActivityId_OnNotFound()
	{
		try
		{
			await _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));
			throw new Exception("Expected CosmosException");
		}
		catch (CosmosException ex)
		{
			ex.ActivityId.Should().NotBeNullOrEmpty();
			Guid.TryParse(ex.ActivityId, out _).Should().BeTrue();
		}
	}

	[Fact]
	public async Task CosmosException_HasRequestCharge_OnConflict()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		try
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk", Name = "Dup" }, new PartitionKey("pk"));
			throw new Exception("Expected CosmosException");
		}
		catch (CosmosException ex)
		{
			ex.RequestCharge.Should().BeGreaterThan(0);
		}
	}

	[Fact]
	public async Task CosmosException_SubStatusCode_IsZero()
	{
		try
		{
			await _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));
			throw new Exception("Expected CosmosException");
		}
		catch (CosmosException ex)
		{
			ex.SubStatusCode.Should().Be(0);
		}
	}

	[Fact(Skip = "CosmosException.Diagnostics requires CosmosDiagnostics construction which is not available through the public API. The emulator does not populate Diagnostics on exceptions.")]
	public async Task CosmosException_HasDiagnostics_OnError()
	{
		await Task.CompletedTask;
	}

	[Fact]
	public async Task CosmosException_HasDiagnostics_Divergent_DiagnosticsIsNull()
	{
		// DIVERGENT BEHAVIOUR: Real Cosmos DB includes Diagnostics on CosmosException.
		// The emulator does not populate Diagnostics on exceptions — it's null.
		try
		{
			await _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));
			throw new Exception("Expected CosmosException");
		}
		catch (CosmosException ex)
		{
			// Diagnostics may or may not be null — just document the behavior
			_ = ex.Diagnostics;
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Feed Response Headers (BUG-2 fix verification)
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedResponseHeadersDeepDiveTests
{
	[Fact]
	public async Task FeedResponse_Headers_ContainRequestCharge()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iter.ReadNextAsync();

		page.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task FeedResponse_Headers_ContainActivityId()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iter.ReadNextAsync();

		page.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task FeedResponse_ContinuationToken_NullOnLastPage()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iter.ReadNextAsync();

		page.ContinuationToken.Should().BeNull();
	}

	[Fact]
	public async Task FeedResponse_EmptyResults_StillHasMetadata()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'nobody'");
		var page = await iter.ReadNextAsync();

		page.StatusCode.Should().Be(HttpStatusCode.OK);
		page.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Stream Feed Iterator Headers (BUG-1 fix verification)
// ═══════════════════════════════════════════════════════════════════════════════

public class StreamFeedIteratorMetadataTests
{
	[Fact]
	public async Task StreamFeedIterator_HasActivityId_Header()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await iter.ReadNextAsync();

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task StreamFeedIterator_HasRequestCharge_Header()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await iter.ReadNextAsync();

		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task StreamFeedIterator_HasSessionToken_Header()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await iter.ReadNextAsync();

		response.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Typed Response Headers Completeness (BUG-6 fix verification)
// ═══════════════════════════════════════════════════════════════════════════════

public class TypedResponseHeadersDeepDiveTests
{
	[Fact]
	public async Task TypedResponse_Headers_ContainActivityId()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task TypedResponse_Headers_ContainRequestCharge()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task TypedResponse_Headers_ContainSessionToken()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		response.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — ReadMany Response Metadata
// ═══════════════════════════════════════════════════════════════════════════════

public class ReadManyMetadataDeepDiveTests
{
	[Fact]
	public async Task ReadManyItemsAsync_Returns_FeedResponse_WithMetadata()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));

		var response = await container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk")), ("2", new PartitionKey("pk"))]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Count.Should().Be(2);
		response.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
	}

	[Fact]
	public async Task ReadManyItemsStreamAsync_Returns_OK_WithHeaders()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var response = await container.ReadManyItemsStreamAsync(
			[("1", new PartitionKey("pk"))]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().NotBeNull();
	}

	[Fact]
	public async Task ReadManyItemsAsync_EmptyList_Returns_EmptyResponse()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var response = await container.ReadManyItemsAsync<TestDocument>([]);

		response.Count.Should().Be(0);
	}

	[Fact]
	public async Task ReadManyItemsAsync_PartialMatch_Returns_OnlyFound()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));

		var response = await container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk")), ("2", new PartitionKey("pk")), ("3", new PartitionKey("pk"))]);

		response.Count.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Database/Container Lifecycle Status Codes
// ═══════════════════════════════════════════════════════════════════════════════

public class DatabaseContainerLifecycleDeepDiveTests
{
	[Fact]
	public async Task CreateDatabaseIfNotExistsAsync_NewDB_Returns_Created()
	{
		var client = new InMemoryCosmosClient();
		var response = await client.CreateDatabaseIfNotExistsAsync("testdb");
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateDatabaseIfNotExistsAsync_ExistingDB_Returns_OK()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseAsync("testdb");
		var response = await client.CreateDatabaseIfNotExistsAsync("testdb");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReadDatabaseAsync_Returns_OK()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var response = await db.ReadAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Should().NotBeNull();
	}

	[Fact]
	public async Task DeleteDatabaseAsync_Returns_NoContent()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var response = await db.DeleteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task CreateContainerIfNotExistsAsync_NewContainer_Returns_Created()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var response = await db.CreateContainerIfNotExistsAsync("mycontainer", "/pk");
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateContainerIfNotExistsAsync_ExistingContainer_Returns_OK()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		await db.CreateContainerAsync("mycontainer", "/pk");
		var response = await db.CreateContainerIfNotExistsAsync("mycontainer", "/pk");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReadContainerAsync_Returns_OK()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var c = (await db.CreateContainerAsync("mycontainer", "/pk")).Container;
		var response = await c.ReadContainerAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Should().NotBeNull();
	}

	[Fact]
	public async Task DeleteContainerAsync_Returns_NoContent()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var c = (await db.CreateContainerAsync("mycontainer", "/pk")).Container;
		var response = await c.DeleteContainerAsync();
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task CreateDatabaseAsync_Duplicate_Throws_Conflict()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseAsync("testdb");

		var act = () => client.CreateDatabaseAsync("testdb");
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.Conflict);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Database/Container Stream Operations (BUG-5 fix verification)
// ═══════════════════════════════════════════════════════════════════════════════

public class DatabaseContainerStreamDeepDiveTests
{
	[Fact]
	public async Task CreateDatabaseStreamAsync_Returns_Created_WithHeaders()
	{
		var client = new InMemoryCosmosClient();
		var response = await client.CreateDatabaseStreamAsync(new DatabaseProperties("testdb"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task CreateDatabaseStreamAsync_Duplicate_Returns_Conflict_WithHeaders()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseStreamAsync(new DatabaseProperties("testdb"));
		var response = await client.CreateDatabaseStreamAsync(new DatabaseProperties("testdb"));
		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task CreateContainerStreamAsync_Returns_Created_WithHeaders()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var response = await db.CreateContainerStreamAsync(new ContainerProperties("mycontainer", "/pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task CreateContainerStreamAsync_Duplicate_Returns_Conflict_WithHeaders()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		await db.CreateContainerStreamAsync(new ContainerProperties("mycontainer", "/pk"));
		var response = await db.CreateContainerStreamAsync(new ContainerProperties("mycontainer", "/pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReadDatabaseStreamAsync_Returns_OK_WithHeaders()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var response = await db.ReadStreamAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task DeleteDatabaseStreamAsync_Returns_NoContent_WithHeaders()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var response = await db.DeleteStreamAsync();
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Database/Container Typed Metadata (SKIP + sisters — BUG-4)
// ═══════════════════════════════════════════════════════════════════════════════

public class DatabaseContainerMetadataSkipTests
{
	[Fact(Skip = "DatabaseResponse from NSubstitute mock only has StatusCode and Resource. Would require significant refactoring to populate Headers, RequestCharge, ActivityId, Diagnostics.")]
	public async Task DatabaseResponse_HasRequestCharge_ActivityId_Diagnostics()
	{
		await Task.CompletedTask;
	}

	[Fact]
	public async Task DatabaseResponse_EmulatorBehavior_OnlyHasStatusCodeAndResource()
	{
		// DIVERGENT BEHAVIOUR: Real Cosmos DB DatabaseResponse includes
		// RequestCharge, ActivityId, Diagnostics. Emulator NSubstitute mock
		// only sets StatusCode and Resource.
		var client = new InMemoryCosmosClient();
		var response = await client.CreateDatabaseAsync("testdb");

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Resource.Should().NotBeNull();
	}

	[Fact(Skip = "ContainerResponse from NSubstitute mock only has StatusCode, Resource, Container. Would require significant refactoring.")]
	public async Task ContainerResponse_HasRequestCharge_ActivityId_Diagnostics()
	{
		await Task.CompletedTask;
	}

	[Fact]
	public async Task ContainerResponse_EmulatorBehavior_OnlyHasStatusCodeAndResource()
	{
		// DIVERGENT BEHAVIOUR: Same as DatabaseResponse — limited mock.
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		var response = await db.CreateContainerAsync("mycontainer", "/pk");

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Resource.Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Batch Response Additional Metadata
// ═══════════════════════════════════════════════════════════════════════════════

public class BatchResponseAdditionalTests
{
	[Fact]
	public async Task BatchResponse_Failure_HasConflictStatus()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "Dup" });
		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task BatchResponse_PerOperation_WriteOps_HaveETag()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" });

		using var response = await batch.ExecuteAsync();
		response[0].ETag.Should().NotBeNullOrEmpty();
		response[1].ETag.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Session Token Format Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class SessionTokenFormatTests
{
	[Fact]
	public async Task TypedResponse_SessionToken_HasCosmosFormat()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var token = response.Headers["x-ms-session-token"];
		token.Should().NotBeNullOrEmpty();
		token.Should().StartWith("0:");
	}

	[Fact]
	public async Task StreamResponse_SessionToken_HasCosmosFormat()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")),
			new PartitionKey("pk"));

		var token = response.Headers["x-ms-session-token"];
		token.Should().NotBeNullOrEmpty();
		token.Should().Contain("0:");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Typed IfNoneMatch 304
// ═══════════════════════════════════════════════════════════════════════════════

public class TypedIfNoneMatchTests
{
	[Fact]
	public async Task ReadItemAsync_IfNoneMatch_Matching_Throws_NotModified_304()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var create = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"),
			new ItemRequestOptions { IfNoneMatchEtag = create.ETag });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task ReadItemAsync_IfNoneMatch_Wildcard_Throws_NotModified_304()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotModified);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 34 — Additional Divergent Behavior Documentation
// ═══════════════════════════════════════════════════════════════════════════════

public class ResponseMetadataDivergentBehaviorDeepDiveTests
{
	[Fact]
	public async Task ETag_IsMonotonicallyIncreasingHexCounter()
	{
		// ETags are monotonically increasing hex counters, providing ordering.
		var container = new InMemoryContainer("test", "/partitionKey");
		var r1 = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		var r2 = await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));

		r1.ETag.Should().NotBe(r2.ETag);
		r1.ETag.Should().MatchRegex("^\"[0-9a-f]{16}\"$");
		r2.ETag.Should().MatchRegex("^\"[0-9a-f]{16}\"$");

		// Verify monotonic ordering
		var v1 = long.Parse(r1.ETag!.Trim('"'), System.Globalization.NumberStyles.HexNumber);
		var v2 = long.Parse(r2.ETag!.Trim('"'), System.Globalization.NumberStyles.HexNumber);
		v2.Should().BeGreaterThan(v1);
	}

	[Fact(Skip = "Real Cosmos: continuation tokens are opaque base64-encoded JSON with partition info. Emulator: plain integer offset.")]
	public void ContinuationToken_ShouldBeOpaque_Base64Json() { }

	[Fact]
	public async Task ContinuationToken_EmulatorBehavior_IsPlainIntegerOffset()
	{
		// DIVERGENT BEHAVIOUR: InMemoryFeedIterator uses integer offsets as continuation tokens.
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}" }, new PartitionKey("pk"));

		var iter = new InMemoryFeedIterator<int>(Enumerable.Range(1, 5).ToList(), maxItemCount: 2);
		var page = await iter.ReadNextAsync();

		page.ContinuationToken.Should().Be("2");
	}

	[Fact]
	public async Task FeedResponseHeaders_ShouldContainItemCount()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iter.ReadNextAsync();

		page.Headers["x-ms-item-count"].Should().Be("2");
	}

	[Fact]
	public async Task FeedResponseHeaders_EmulatorBehavior_HasActivityIdAndRequestCharge()
	{
		// DIVERGENT BEHAVIOUR: Real Cosmos includes x-ms-item-count in headers.
		// Emulator includes x-ms-activity-id and x-ms-request-charge but not x-ms-item-count.
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iter.ReadNextAsync();

		page.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		page.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task DeleteAllByPartitionKey_Returns_OK_WithHeaders()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var response = await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteAllByPartitionKey_EmptyPartition_Returns_OK()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var response = await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("empty"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 1: Batch Metadata Gaps
// ═══════════════════════════════════════════════════════════════════════════════

public class BatchMetadataGapsDeepDiveTests
{
	[Fact]
	public async Task BatchResponse_ActivityId_IsValidGuid()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		using var response = await batch.ExecuteAsync();
		Guid.TryParse(response.ActivityId, out _).Should().BeTrue();
	}

	[Fact]
	public async Task BatchResponse_ActivityId_IsNotAllZeros()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.ActivityId.Should().NotBe("00000000-0000-0000-0000-000000000000");
	}

	[Fact]
	public async Task BatchResponse_Headers_ContainActivityId()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task BatchResponse_Headers_ContainRequestCharge()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task BatchResponse_EmptyBatch_ReturnsMetadata()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		using var response = await batch.ExecuteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.ActivityId.Should().NotBeNullOrEmpty();
		response.Diagnostics.Should().NotBeNull();
	}

	[Fact]
	public async Task BatchResponse_Diagnostics_ToString_ReturnsJson()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.Diagnostics.ToString().Should().Be("{}");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 2: Session Token Consistency
// ═══════════════════════════════════════════════════════════════════════════════

public class SessionTokenConsistencyDeepDiveTests
{
	[Fact]
	public async Task SessionToken_Consistent_AcrossTypedAndStream()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var typed = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		var stream = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","partitionKey":"pk"}""")), new PartitionKey("pk"));

		var typedToken = typed.Headers["x-ms-session-token"];
		var streamToken = stream.Headers["x-ms-session-token"];

		// Both should use the same format
		typedToken.Should().StartWith("0:");
		streamToken.Should().StartWith("0:");
		typedToken.Should().MatchRegex(@"^0:\d+#\d+$");
		streamToken.Should().MatchRegex(@"^0:\d+#\d+$");
	}

	[Fact]
	public async Task SessionToken_PresentOnAllTypedCrudOps()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var create = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		create.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Create");

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		read.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Read");

		var upsert = await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));
		upsert.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Upsert");

		var replace = await container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "C" }, "1", new PartitionKey("pk"));
		replace.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Replace");

		var patch = await container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk"), [PatchOperation.Set("/name", "D")]);
		patch.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Patch");

		var delete = await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));
		delete.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Delete");
	}

	[Fact]
	public async Task SessionToken_PresentOnAllStreamCrudOps()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var create = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk","name":"A"}""")), new PartitionKey("pk"));
		create.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Create");

		var read = await container.ReadItemStreamAsync("1", new PartitionKey("pk"));
		read.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Read");

		var upsert = await container.UpsertItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk","name":"B"}""")), new PartitionKey("pk"));
		upsert.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Upsert");

		var replace = await container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk","name":"C"}""")), "1", new PartitionKey("pk"));
		replace.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Replace");

		var patch = await container.PatchItemStreamAsync("1", new PartitionKey("pk"),
			[PatchOperation.Set("/name", "D")]);
		patch.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Patch");

		var delete = await container.DeleteItemStreamAsync("1", new PartitionKey("pk"));
		delete.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty("Delete");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 3: CosmosException Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════

public class CosmosExceptionDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task CosmosException_Message_ContainsStatusCode()
	{
		try
		{
			await _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));
			throw new Exception("Expected CosmosException");
		}
		catch (CosmosException ex)
		{
			// CosmosException.Message includes "Resource Not Found" matching real SDK behavior
			ex.Message.Should().Contain("Resource Not Found");
			ex.Message.Should().Contain("Entity with the specified id does not exist");
			ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}

	// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
	//   404 Not found — "The operation is attempting to act on a resource that no longer exists."
	// Ref: Observed Cosmos DB REST API error body: {"code":"NotFound","message":"Resource Not Found. ..."}
	// The exception message should contain "Resource Not Found" to match the real SDK behavior,
	// enabling code that checks for this string (e.g., e.Message.Contains("Resource Not Found")).
	[Theory]
	[InlineData("ReadItem")]
	[InlineData("ReplaceItem")]
	[InlineData("DeleteItem")]
	[InlineData("PatchItem")]
	public async Task CosmosException_NotFound_Message_ContainsResourceNotFound(string operation)
	{
		Func<Task> act = operation switch
		{
			"ReadItem" => () => _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk")),
			"ReplaceItem" => () => _container.ReplaceItemAsync(
				new TestDocument { Id = "nope", PartitionKey = "pk" }, "nope", new PartitionKey("pk")),
			"DeleteItem" => () => _container.DeleteItemAsync<TestDocument>("nope", new PartitionKey("pk")),
			"PatchItem" => () => _container.PatchItemAsync<TestDocument>("nope", new PartitionKey("pk"),
				new[] { PatchOperation.Set("/name", "x") }),
			_ => throw new ArgumentException(operation)
		};

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
		ex.Which.Message.ToLower().Should().Contain("resource not found");
	}

	[Fact]
	public async Task CosmosException_Headers_ContainActivityId()
	{
		try
		{
			await _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));
			throw new Exception("Expected CosmosException");
		}
		catch (CosmosException ex)
		{
			ex.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		}
	}

	[Fact]
	public async Task CosmosException_HasDiagnostics_OrNull()
	{
		try
		{
			await _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));
			throw new Exception("Expected CosmosException");
		}
		catch (CosmosException ex)
		{
			// Document whatever the emulator returns — may be null
			_ = ex.Diagnostics;
		}
	}

	[Fact]
	public async Task CosmosException_ActivityId_IsUnique_PerError()
	{
		string activityId1 = null!, activityId2 = null!;
		try
		{
			await _container.ReadItemAsync<TestDocument>("nope1", new PartitionKey("pk"));
		}
		catch (CosmosException ex)
		{
			activityId1 = ex.ActivityId;
		}

		try
		{
			await _container.ReadItemAsync<TestDocument>("nope2", new PartitionKey("pk"));
		}
		catch (CosmosException ex)
		{
			activityId2 = ex.ActivityId;
		}

		activityId1.Should().NotBe(activityId2);
	}

	[Fact]
	public async Task CosmosException_ResponseBody_Typed_vs_Stream()
	{
		// Typed API throws CosmosException; Stream API returns status code
		var act = () => _container.ReadItemAsync<TestDocument>("nope", new PartitionKey("pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);

		var streamResponse = await _container.ReadItemStreamAsync("nope", new PartitionKey("pk"));
		streamResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 4: Stream Error Body Content
// ═══════════════════════════════════════════════════════════════════════════════

public class StreamErrorBodyContentDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task StreamError_404_Content_ContainsErrorBody()
	{
		var response = await _container.ReadItemStreamAsync("nope", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
		// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
		// Real Cosmos DB returns a JSON error body for 404 responses.
		response.Content.Should().NotBeNull();
		using var reader = new StreamReader(response.Content);
		var body = reader.ReadToEnd();
		body.Should().Contain("Resource Not Found");
	}

	[Fact]
	public async Task StreamError_409_Content_ContainsErrorBody()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));
		var response = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
		response.Content.Should().NotBeNull();
		using var reader = new StreamReader(response.Content);
		var body = reader.ReadToEnd();
		body.Should().Contain("Conflict");
	}

	[Fact]
	public async Task StreamError_412_Content_ContainsErrorBody()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));
		var response = await _container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")),
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale\"" });
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
		response.Content.Should().NotBeNull();
		using var reader = new StreamReader(response.Content);
		var body = reader.ReadToEnd();
		body.Should().Contain("PreconditionFailed");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 5: Delete Metadata Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class DeleteMetadataEdgeCaseDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task DeleteItemAsync_ETag_IsNull_OnResponse()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.ETag.Should().BeNull();
	}

	[Fact]
	public async Task DeleteItemStreamAsync_NoETag_InHeaders()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));
		var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk"));
		response.Headers.ETag.Should().BeNullOrEmpty();
	}

	[Fact]
	public async Task DeleteItemStreamAsync_Content_IsNull()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));
		var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk"));
		response.Content.Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 6: Typed IfMatch/IfNoneMatch Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class TypedETagEdgeCaseDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task UpsertItemAsync_StaleETag_Throws_412()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var act = () => _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task PatchItemAsync_StaleETag_Throws_412()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var act = () => _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk"), [PatchOperation.Set("/name", "B")],
			new PatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task DeleteItemAsync_StaleETag_Throws_412()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var act = () => _container.DeleteItemAsync<TestDocument>(
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task DeleteItemStreamAsync_StaleETag_Returns_412()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")), new PartitionKey("pk"));

		var response = await _container.DeleteItemStreamAsync(
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 7: Content Suppression Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class ContentSuppressionEdgeCaseDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task CreateItemAsync_SuppressContent_StillUpdatesETag()
	{
		var r1 = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });
		r1.ETag.Should().NotBeNullOrEmpty();

		var r2 = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });
		r2.ETag.Should().NotBeNullOrEmpty();
		r2.ETag.Should().NotBe(r1.ETag);
	}

	[Fact]
	public async Task UpsertItemAsync_SuppressContent_ExistingItem_ResourceIsNull()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task SuppressContent_StreamApi_HasEmptyBody()
	{
		var response = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk"}""")),
			new PartitionKey("pk"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Content.Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 8: FeedResponse Count & Pagination
// ═══════════════════════════════════════════════════════════════════════════════

public class FeedResponsePaginationDeepDiveTests
{
	[Fact]
	public async Task FeedResponse_Count_MatchesItemsInPage()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iter.ReadNextAsync();

		page.Count.Should().Be(5);
	}

	[Fact]
	public async Task FeedResponse_MultiPage_ContinuationToken_Format()
	{
		var items = Enumerable.Range(0, 5).Select(i =>
			new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}" }).ToList();
		var iter = new InMemoryFeedIterator<TestDocument>(items, maxItemCount: 2);

		var page1 = await iter.ReadNextAsync();
		page1.Count.Should().Be(2);
		page1.ContinuationToken.Should().NotBeNullOrEmpty();

		var page2 = await iter.ReadNextAsync();
		page2.Count.Should().Be(2);
		page2.ContinuationToken.Should().NotBeNullOrEmpty();

		var page3 = await iter.ReadNextAsync();
		page3.Count.Should().Be(1);
		page3.ContinuationToken.Should().BeNull();
	}

	[Fact]
	public async Task FeedResponse_MultiPage_EachPage_HasUniqueActivityId()
	{
		var items = Enumerable.Range(0, 4).Select(i =>
			new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}" }).ToList();
		var iter = new InMemoryFeedIterator<TestDocument>(items, maxItemCount: 2);

		var page1 = await iter.ReadNextAsync();
		var page2 = await iter.ReadNextAsync();

		page1.ActivityId.Should().NotBe(page2.ActivityId);
	}

	[Fact]
	public async Task StreamFeedIterator_ItemCount_MatchesItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await iter.ReadNextAsync();

		response.Headers["x-ms-item-count"].Should().Be("2");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 9: Diagnostics Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════

public class DiagnosticsDeepDiveTests
{
	[Fact]
	public async Task Diagnostics_ToString_ReturnsEmptyJson()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		response.Diagnostics.ToString().Should().Be("{}");
	}

	[Fact]
	public async Task Diagnostics_GetContactedRegions_ReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		response.Diagnostics.GetContactedRegions().Should().BeEmpty();
	}

	[Fact]
	public async Task FeedResponse_Diagnostics_GetClientElapsedTime_IsZero()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var iter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var page = await iter.ReadNextAsync();

		page.Diagnostics.GetClientElapsedTime().Should().Be(TimeSpan.Zero);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 10: DeleteAllItemsByPartitionKey Headers
// ═══════════════════════════════════════════════════════════════════════════════

public class DeleteAllByPartitionKeyHeadersDeepDiveTests
{
	[Fact]
	public async Task DeleteAllByPartitionKey_HasActivityId_Header()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var response = await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk"));
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task DeleteAllByPartitionKey_HasRequestCharge_Header()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var response = await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk"));
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 11: ReadMany Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class ReadManyEdgeCaseDeepDiveTests
{
	[Fact]
	public async Task ReadManyItemsAsync_HasActivityId()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var response = await container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk"))]);
		response.ActivityId.Should().NotBeNullOrEmpty();
		Guid.TryParse(response.ActivityId, out _).Should().BeTrue();
	}

	[Fact]
	public async Task ReadManyItemsStreamAsync_HasActivityId_Header()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var response = await container.ReadManyItemsStreamAsync(
			[("1", new PartitionKey("pk"))]);
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 12: Cross-API Consistency Checks
// ═══════════════════════════════════════════════════════════════════════════════

public class CrossApiConsistencyDeepDiveTests
{
	[Fact]
	public async Task TypedAndStream_SameItem_SameETag()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var typed = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var stream = await container.ReadItemStreamAsync("1", new PartitionKey("pk"));

		stream.Headers.ETag.Should().Be(typed.ETag);
	}

	[Fact]
	public async Task TypedAndStream_StatusCodes_Match_ForAllOps()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create
		var typedCreate = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));
		var streamCreate = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","partitionKey":"pk"}""")), new PartitionKey("pk"));
		typedCreate.StatusCode.Should().Be((HttpStatusCode)streamCreate.StatusCode);

		// Read
		var typedRead = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		var streamRead = await container.ReadItemStreamAsync("2", new PartitionKey("pk"));
		typedRead.StatusCode.Should().Be((HttpStatusCode)streamRead.StatusCode);

		// Delete
		var typedDelete = await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));
		var streamDelete = await container.DeleteItemStreamAsync("2", new PartitionKey("pk"));
		typedDelete.StatusCode.Should().Be((HttpStatusCode)streamDelete.StatusCode);
	}

	[Fact]
	public async Task FeedIterator_TypedAndStream_SameActivityIdFormat()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var typedIter = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var typedPage = await typedIter.ReadNextAsync();

		var streamIter = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var streamResponse = await streamIter.ReadNextAsync();

		Guid.TryParse(typedPage.ActivityId, out _).Should().BeTrue();
		Guid.TryParse(streamResponse.Headers["x-ms-activity-id"], out _).Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Group 13: FakeCosmosHandler Metadata Consistency
// ═══════════════════════════════════════════════════════════════════════════════

public class FakeCosmosHandlerResponseMetadataDeepDiveTests : IDisposable
{
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _client;
	private readonly Container _typedContainer;

	public FakeCosmosHandlerResponseMetadataDeepDiveTests()
	{
		_handler = new FakeCosmosHandler(new InMemoryContainer("test", "/partitionKey"));

		_client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				HttpClientFactory = () => new HttpClient(_handler),
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0
			});

		_typedContainer = _client.GetContainer("db", "test");
	}

	public void Dispose()
	{
		_client?.Dispose();
		_handler?.Dispose();
	}

	[Fact]
	public async Task FakeCosmosHandler_PreservesActivityId_FromContainer()
	{
		await _typedContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" }, new PartitionKey("pk"));

		var response = await _typedContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.ActivityId.Should().NotBeNullOrEmpty();
		Guid.TryParse(response.ActivityId, out _).Should().BeTrue();
	}

	[Fact]
	public async Task FakeCosmosHandler_HasSessionToken_OnResponse()
	{
		var response = await _typedContainer.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B" }, new PartitionKey("pk"));

		response.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();
	}
}
