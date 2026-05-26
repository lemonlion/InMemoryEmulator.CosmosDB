using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// TDD tests for CRUD route handling through the SDK HTTP pipeline.
/// Parity-validated: runs against both FakeCosmosHandler (in-memory) and real emulator.
/// Tests tagged [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] are excluded from emulator runs.
/// Patch filter predicate tests are tagged InMemoryOnly — Windows emulator (v2.14.0) does not support filterPredicate syntax (see #53).
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerCrudTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("test-crud", "/partitionKey");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	/// <summary>
	/// Helper for in-memory-only tests that need direct access to FakeCosmosHandler APIs
	/// (RequestLog, FaultInjector, etc.). Creates an isolated handler stack.
	/// </summary>
	private static (FakeCosmosHandler Handler, CosmosClient Client, Container Container) CreateInMemoryStack(
		string name = "test", string pkPath = "/partitionKey")
	{
		var cosmos = InMemoryCosmos.Create(name, pkPath);
		return (cosmos.Handler, cosmos.Client, cosmos.Container);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1A. Create Item
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_ReturnsCreated()
	{
		var doc = new TestDocument { Id = "c1", PartitionKey = "pk1", Name = "Alice", Value = 10 };

		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Resource.Should().NotBeNull();
		response.Resource.Id.Should().Be("c1");
		response.Resource.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Handler_CreateItem_DuplicateId_ReturnsConflict()
	{
		var doc = new TestDocument { Id = "c2", PartitionKey = "pk1", Name = "Alice" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task Handler_CreateItem_ThenQuery_ReturnsItem()
	{
		var doc = new TestDocument { Id = "c3", PartitionKey = "pk1", Name = "Bob", Value = 20 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var results = new List<TestDocument>();
		var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'Bob'");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Id.Should().Be("c3");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1B. Read Item
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ReadItem_ReturnsDocument()
	{
		var doc = new TestDocument { Id = "r1", PartitionKey = "pk1", Name = "Alice", Value = 10 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("r1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Alice");
		response.Resource.Value.Should().Be(10);
	}

	[Fact]
	public async Task Handler_ReadItem_NotFound_Throws404()
	{
		var act = () => _container.ReadItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb
	//   404 Not found — "The operation is attempting to act on a resource that no longer exists."
	// Ref: Observed Cosmos DB REST API error body: {"code":"NotFound","message":"Resource Not Found. ..."}
	// The exception message should contain "Resource Not Found" to match the real SDK behavior.
	[Theory]
	[InlineData("ReadItem")]
	[InlineData("ReplaceItem")]
	[InlineData("DeleteItem")]
	[InlineData("PatchItem")]
	public async Task Handler_NotFound_Message_ContainsResourceNotFound(string operation)
	{
		Func<Task> act = operation switch
		{
			"ReadItem" => () => _container.ReadItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1")),
			"ReplaceItem" => () => _container.ReplaceItemAsync(
				new TestDocument { Id = "nonexistent", PartitionKey = "pk1" }, "nonexistent", new PartitionKey("pk1")),
			"DeleteItem" => () => _container.DeleteItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1")),
			"PatchItem" => () => _container.PatchItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "x") }),
			_ => throw new ArgumentException(operation)
		};

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
		ex.Which.Message.ToLower().Should().Contain("resource not found");
	}

	[Fact]
	public async Task Handler_ReadItem_WithPartitionKey_ReturnsCorrectItem()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "r3", PartitionKey = "pkA", Name = "FromA" },
			new PartitionKey("pkA"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "r3", PartitionKey = "pkB", Name = "FromB" },
			new PartitionKey("pkB"));

		var responseA = await _container.ReadItemAsync<TestDocument>("r3", new PartitionKey("pkA"));
		var responseB = await _container.ReadItemAsync<TestDocument>("r3", new PartitionKey("pkB"));

		responseA.Resource.Name.Should().Be("FromA");
		responseB.Resource.Name.Should().Be("FromB");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1C. Upsert Item
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_UpsertItem_NewItem_ReturnsCreated()
	{
		var doc = new TestDocument { Id = "u1", PartitionKey = "pk1", Name = "Alice" };

		var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Resource.Id.Should().Be("u1");
	}

	[Fact]
	public async Task Handler_UpsertItem_ExistingItem_ReturnsOk()
	{
		var doc = new TestDocument { Id = "u2", PartitionKey = "pk1", Name = "Alice", Value = 10 };
		await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "Updated";
		doc.Value = 99;
		var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Updated");
		response.Resource.Value.Should().Be(99);
	}

	[Fact]
	public async Task Handler_UpsertItem_ThenQuery_ReturnsUpdatedItem()
	{
		var doc = new TestDocument { Id = "u3", PartitionKey = "pk1", Name = "Original" };
		await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "Modified";
		await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		var results = new List<TestDocument>();
		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c WHERE c.id = 'u3'");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Modified");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1D. Replace Item
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ReplaceItem_ReturnsOk()
	{
		var doc = new TestDocument { Id = "rp1", PartitionKey = "pk1", Name = "Before", Value = 1 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "After";
		doc.Value = 2;
		var response = await _container.ReplaceItemAsync(doc, "rp1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("After");
		response.Resource.Value.Should().Be(2);
	}

	[Fact]
	public async Task Handler_ReplaceItem_NotFound_Throws404()
	{
		var doc = new TestDocument { Id = "rp2", PartitionKey = "pk1", Name = "Ghost" };

		var act = () => _container.ReplaceItemAsync(doc, "rp2", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Handler_ReplaceItem_WithETag_StaleETag_ThrowsPreconditionFailed()
	{
		var doc = new TestDocument { Id = "rp3", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		// Modify to change the ETag
		doc.Name = "Modified";
		await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		// Try to replace with a stale ETag
		doc.Name = "StaleReplace";
		var act = () => _container.ReplaceItemAsync(doc, "rp3", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1E. Delete Item
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_DeleteItem_ReturnsNoContent()
	{
		var doc = new TestDocument { Id = "d1", PartitionKey = "pk1", Name = "ToDelete" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("d1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task Handler_DeleteItem_NotFound_Throws404()
	{
		var act = () => _container.DeleteItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Handler_DeleteItem_ThenRead_Throws404()
	{
		var doc = new TestDocument { Id = "d3", PartitionKey = "pk1", Name = "Ephemeral" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("d3", new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("d3", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1F. Patch Item
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_PatchItem_SetOperation_ReturnsOk()
	{
		var doc = new TestDocument { Id = "p1", PartitionKey = "pk1", Name = "Before", Value = 1 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "After")]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("After");
		response.Resource.Value.Should().Be(1); // unchanged
	}

	[Fact]
	public async Task Handler_PatchItem_MultipleOperations_AllApplied()
	{
		var doc = new TestDocument { Id = "p2", PartitionKey = "pk1", Name = "Start", Value = 10 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p2", new PartitionKey("pk1"),
		[
			PatchOperation.Set("/name", "Patched"),
			PatchOperation.Increment("/value", 5)
		]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Patched");
		response.Resource.Value.Should().Be(15);
	}

	[Fact]
	public async Task Handler_PatchItem_NotFound_Throws404()
	{
		var act = () => _container.PatchItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Ghost")]);

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// Windows emulator (v2.14.0) returns 400 "Syntax error near 'value'" for patch filter predicates.
	// Tracked: https://github.com/lemonlion/CosmosDB.InMemoryEmulator/issues/53
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	[Fact]
	public async Task Handler_PatchItem_WithFilterPredicate_MatchingCondition_Succeeds()
	{
		var doc = new TestDocument { Id = "p4", PartitionKey = "pk1", Name = "Conditional", Value = 100 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p4", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Updated")],
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.value = 100" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Updated");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1G. Integration / Multi-Container
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CrudThenLinqQuery_RoundTrip()
	{
		// Create items via CRUD
		await _container.CreateItemAsync(
			new TestDocument { Id = "lq1", PartitionKey = "pk1", Name = "Active", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "lq2", PartitionKey = "pk1", Name = "Inactive", Value = 5 },
			new PartitionKey("pk1"));

		// Query via LINQ .ToFeedIterator() — the whole point of this feature
		var results = new List<TestDocument>();
		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Value > 7)
			.ToFeedIterator();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Active");
	}

	[Fact]
	public async Task Handler_MultiContainer_CrudIsolated()
	{
		var cA = _container; // reuse per-test container to stay within CI's 3-partition limit
		var cB = await _fixture.CreateContainerAsync("containerB", "/partitionKey");

		// Create in A
		await cA.CreateItemAsync(
			new TestDocument { Id = "iso1", PartitionKey = "pk1", Name = "InA" },
			new PartitionKey("pk1"));

		// Read from A succeeds
		var readA = await cA.ReadItemAsync<TestDocument>("iso1", new PartitionKey("pk1"));
		readA.Resource.Name.Should().Be("InA");

		// Read from B fails — item doesn't exist there
		var act = () => cB.ReadItemAsync<TestDocument>("iso1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_RequestLog_RecordsCrudOperations()
	{
		var (handler, client, container) = CreateInMemoryStack("log-crud");
		using var _h = handler;
		using var _c = client;

		var doc = new TestDocument { Id = "log1", PartitionKey = "pk1", Name = "Logged" };
		await container.CreateItemAsync(doc, new PartitionKey("pk1"));
		await container.ReadItemAsync<TestDocument>("log1", new PartitionKey("pk1"));
		await container.DeleteItemAsync<TestDocument>("log1", new PartitionKey("pk1"));

		// POST for create, GET for read, DELETE for delete
		handler.RequestLog.Should().Contain(e => e.StartsWith("POST") && e.Contains("/docs"));
		handler.RequestLog.Should().Contain(e => e.StartsWith("GET") && e.Contains("/docs/"));
		handler.RequestLog.Should().Contain(e => e.StartsWith("DELETE") && e.Contains("/docs/"));
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1H. SDK Compatibility
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_VerifySdkCompatibility_IncludesCrudCheck()
	{
		var act = FakeCosmosHandler.VerifySdkCompatibilityAsync;

		await act.Should().NotThrowAsync();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1I. Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_ThrottlesCreateRequest()
	{
		var (handler, client, container) = CreateInMemoryStack("fault-throttle");
		using var _h = handler;
		using var _c = client;

		handler.FaultInjector = _ =>
			new HttpResponseMessage((HttpStatusCode)429)
			{
				Headers = { RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromMilliseconds(1)) }
			};
		handler.FaultInjectorIncludesMetadata = false;

		var doc = new TestDocument { Id = "fi1", PartitionKey = "pk1", Name = "Throttled" };
		var act = () => container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be((HttpStatusCode)429);
	}

	// Windows emulator (v2.14.0) returns 400 BadRequest instead of 412 PreconditionFailed for non-matching patch filter predicates.
	// Tracked: https://github.com/lemonlion/CosmosDB.InMemoryEmulator/issues/53
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	[Fact]
	public async Task Handler_PatchItem_WithFilterPredicate_NonMatchingCondition_ThrowsPreconditionFailed()
	{
		var doc = new TestDocument { Id = "p5", PartitionKey = "pk1", Name = "Conditional", Value = 50 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.PatchItemAsync<TestDocument>("p5", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Updated")],
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.value > 100" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Handler_ReadItem_WithUrlEncodedId_Succeeds()
	{
		var id = "doc with spaces";
		var doc = new TestDocument { Id = id, PartitionKey = "pk1", Name = "Encoded" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));

		response.Resource.Name.Should().Be("Encoded");
		response.Resource.Id.Should().Be(id);
	}

	[Fact]
	public async Task Handler_Crud_WithCompositePartitionKey_RoundTrip()
	{
		var container = await _fixture.CreateContainerAsync("composite-test", ["/tenantId", "/userId"]);

		var pk = new PartitionKeyBuilder().Add("t1").Add("u1").Build();
		var doc = new { id = "cpk1", tenantId = "t1", userId = "u1", name = "Composite" };
		var createResponse = await container.CreateItemAsync(doc, pk);
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await container.ReadItemAsync<dynamic>("cpk1", pk);
		((string)readResponse.Resource.name).Should().Be("Composite");

		await container.DeleteItemAsync<dynamic>("cpk1", pk);
		var act = () => container.ReadItemAsync<dynamic>("cpk1", pk);
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  A1. Special Character IDs
	// ═══════════════════════════════════════════════════════════════════════════

	[Theory]
	[InlineData("a.b.c")]
	[InlineData("hello world")]
	[InlineData("日本語")]
	[InlineData("a+b")]
	public async Task Handler_CreateItem_WithSpecialCharactersInId_Succeeds(string id)
	{
		var doc = new TestDocument { Id = id, PartitionKey = "pk1", Name = "Special" };
		var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await _container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));
		readResponse.Resource.Id.Should().Be(id);
		readResponse.Resource.Name.Should().Be("Special");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  A2. Replace with matching ETag
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ReplaceItem_WithMatchingETag_Succeeds()
	{
		var doc = new TestDocument { Id = "etag-rp1", PartitionKey = "pk1", Name = "Original" };
		var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		var etag = createResponse.ETag;

		doc.Name = "Updated";
		var replaceResponse = await _container.ReplaceItemAsync(doc, "etag-rp1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });

		replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		replaceResponse.Resource.Name.Should().Be("Updated");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  A3/A4. Delete with ETag
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_DeleteItem_WithStaleETag_ThrowsPreconditionFailed()
	{
		var doc = new TestDocument { Id = "etag-d1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		// Modify to change the ETag
		doc.Name = "Modified";
		await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.DeleteItemAsync<TestDocument>("etag-d1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Handler_DeleteItem_WithMatchingETag_Succeeds()
	{
		var doc = new TestDocument { Id = "etag-d2", PartitionKey = "pk1", Name = "ToDelete" };
		var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		var etag = createResponse.ETag;

		var deleteResponse = await _container.DeleteItemAsync<TestDocument>("etag-d2", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });

		deleteResponse.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  A5/A6. Patch Remove + Add operations
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_PatchItem_RemoveOperation_Succeeds()
	{
		var doc = new TestDocument { Id = "prm1", PartitionKey = "pk1", Name = "HasName", Value = 42 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("prm1", new PartitionKey("pk1"),
			[PatchOperation.Remove("/name")]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().BeNull();
		response.Resource.Value.Should().Be(42); // unchanged
	}

	[Fact]
	public async Task Handler_PatchItem_AddOperation_Succeeds()
	{
		var doc = new TestDocument { Id = "padd1", PartitionKey = "pk1", Name = "Before" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("padd1", new PartitionKey("pk1"),
			[PatchOperation.Add("/tags", new[] { "alpha", "beta" })]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Tags.Should().Contain("alpha");
		response.Resource.Tags.Should().Contain("beta");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  A7-A10. ETag in CRUD responses
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_ReturnsETagInResponse()
	{
		var doc = new TestDocument { Id = "etag-c1", PartitionKey = "pk1", Name = "Alice" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Handler_UpsertItem_ReturnsETagInResponse()
	{
		var doc = new TestDocument { Id = "etag-u1", PartitionKey = "pk1", Name = "Alice" };

		var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Handler_ReplaceItem_ReturnsUpdatedETag()
	{
		var doc = new TestDocument { Id = "etag-rp2", PartitionKey = "pk1", Name = "Before" };
		var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		var originalETag = createResponse.ETag;

		doc.Name = "After";
		var replaceResponse = await _container.ReplaceItemAsync(doc, "etag-rp2", new PartitionKey("pk1"));

		replaceResponse.ETag.Should().NotBeNullOrEmpty();
		replaceResponse.ETag.Should().NotBe(originalETag);
	}

	[Fact]
	public async Task Handler_ReadItem_ReturnsETagInResponse()
	{
		var doc = new TestDocument { Id = "etag-r1", PartitionKey = "pk1", Name = "Alice" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("etag-r1", new PartitionKey("pk1"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B3. PartitionKey.None round-trip
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ReadItem_WithPartitionKeyNone_Succeeds()
	{
		var container = await _fixture.CreateContainerAsync("pknone-test", "/partitionKey");

		var doc = new { id = "none1", name = "NoPK" };
		await container.CreateItemAsync(doc, PartitionKey.None);

		var response = await container.ReadItemAsync<dynamic>("none1", PartitionKey.None);
		((string)response.Resource.name).Should().Be("NoPK");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D1-D3. Response headers on CRUD operations
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CrudResponse_ContainsRequestCharge()
	{
		var doc = new TestDocument { Id = "hdr1", PartitionKey = "pk1", Name = "Alice" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.RequestCharge.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task Handler_CrudResponse_ContainsActivityId()
	{
		var doc = new TestDocument { Id = "hdr2", PartitionKey = "pk1", Name = "Alice" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.ActivityId.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Handler_CrudResponse_ContainsSessionToken()
	{
		var doc = new TestDocument { Id = "hdr3", PartitionKey = "pk1", Name = "Alice" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C: Create Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_WithNullPartitionKey_Succeeds()
	{
		var doc = new { id = "c-null-pk", partitionKey = (string?)null, name = "NullPK" };

		var response = await _container.CreateItemAsync(doc, PartitionKey.Null);

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Handler_CreateItem_WithEmptyStringPartitionKey_Succeeds()
	{
		var doc = new TestDocument { Id = "c-empty-pk", PartitionKey = "", Name = "EmptyPK" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey(""));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		var read = await _container.ReadItemAsync<TestDocument>("c-empty-pk", new PartitionKey(""));
		read.Resource.Name.Should().Be("EmptyPK");
	}

	[Fact]
	public async Task Handler_CreateItem_SameIdDifferentPartitionKey_BothExist()
	{
		var docA = new TestDocument { Id = "c-dup", PartitionKey = "pkA", Name = "InA" };
		var docB = new TestDocument { Id = "c-dup", PartitionKey = "pkB", Name = "InB" };

		await _container.CreateItemAsync(docA, new PartitionKey("pkA"));
		await _container.CreateItemAsync(docB, new PartitionKey("pkB"));

		var readA = await _container.ReadItemAsync<TestDocument>("c-dup", new PartitionKey("pkA"));
		var readB = await _container.ReadItemAsync<TestDocument>("c-dup", new PartitionKey("pkB"));
		readA.Resource.Name.Should().Be("InA");
		readB.Resource.Name.Should().Be("InB");
	}

	[Fact]
	public async Task Handler_CreateItem_ReturnsResourceWithSystemProperties()
	{
		var doc = new TestDocument { Id = "c-sys", PartitionKey = "pk1", Name = "SystemProps" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.ETag.Should().NotBeNullOrEmpty();
		// Read back as JObject to check system properties
		var read = await _container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>("c-sys", new PartitionKey("pk1"));
		read.Resource["_ts"].Should().NotBeNull();
		read.Resource["_etag"].Should().NotBeNull();
	}

	[Fact]
	public async Task Handler_CreateItem_WithNestedObject_RoundTrips()
	{
		var doc = new TestDocument
		{
			Id = "c-nested",
			PartitionKey = "pk1",
			Name = "Nested",
			Nested = new NestedObject { Description = "Deep", Score = 3.14 }
		};

		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("c-nested", new PartitionKey("pk1"));
		read.Resource.Nested.Should().NotBeNull();
		read.Resource.Nested!.Description.Should().Be("Deep");
		read.Resource.Nested.Score.Should().Be(3.14);
	}

	[Fact]
	public async Task Handler_CreateItem_WithNumericPartitionKey_Succeeds()
	{
		var container = await _fixture.CreateContainerAsync("num-pk-test", "/numericPk");

		var doc = new { id = "num1", numericPk = 42, name = "NumericPK" };
		var response = await container.CreateItemAsync(doc, new PartitionKey(42));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await container.ReadItemAsync<dynamic>("num1", new PartitionKey(42));
		((string)read.Resource.name).Should().Be("NumericPK");
	}

	[Fact]
	public async Task Handler_CreateItem_WithBooleanPartitionKey_Succeeds()
	{
		var container = await _fixture.CreateContainerAsync("bool-pk-test", "/boolPk");

		var doc = new { id = "bool1", boolPk = true, name = "BoolPK" };
		var response = await container.CreateItemAsync(doc, new PartitionKey(true));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await container.ReadItemAsync<dynamic>("bool1", new PartitionKey(true));
		((string)read.Resource.name).Should().Be("BoolPK");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  R: Read Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ReadItem_ReturnsResourceWithSystemProperties()
	{
		var doc = new TestDocument { Id = "r-sys", PartitionKey = "pk1", Name = "SysRead" };
		var create = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>("r-sys", new PartitionKey("pk1"));

		read.Resource["_ts"].Should().NotBeNull();
		read.Resource["_etag"]!.ToString().Should().Be(create.ETag);
	}

	[Fact]
	public async Task Handler_ReadItem_WithIfNoneMatch_MatchingETag_ReturnsNotModified()
	{
		var doc = new TestDocument { Id = "r-inm1", PartitionKey = "pk1", Name = "Conditional" };
		var create = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("r-inm1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = create.ETag });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task Handler_ReadItem_WithIfNoneMatch_StaleETag_ReturnsDocument()
	{
		var doc = new TestDocument { Id = "r-inm2", PartitionKey = "pk1", Name = "Fresh" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		// Modify to change the ETag
		doc.Name = "Modified";
		await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("r-inm2", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "\"stale-etag\"" });

		read.StatusCode.Should().Be(HttpStatusCode.OK);
		read.Resource.Name.Should().Be("Modified");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  U: Upsert Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_UpsertItem_WithIfMatchETag_MatchingETag_Succeeds()
	{
		var doc = new TestDocument { Id = "u-ifm1", PartitionKey = "pk1", Name = "Original" };
		var create = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "Updated";
		var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = create.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task Handler_UpsertItem_WithIfMatchETag_StaleETag_ThrowsPreconditionFailed()
	{
		var doc = new TestDocument { Id = "u-ifm2", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "StaleUpsert";
		var act = () => _container.UpsertItemAsync(doc, new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Handler_UpsertItem_ChangesETag_BetweenCreateAndUpdate()
	{
		var doc = new TestDocument { Id = "u-etag", PartitionKey = "pk1", Name = "V1" };
		var create = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));
		var firstETag = create.ETag;

		doc.Name = "V2";
		var update = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		update.ETag.Should().NotBe(firstETag);
	}

	[Fact]
	public async Task Handler_UpsertItem_ExistingItem_FullyReplacesDocument()
	{
		var doc = new TestDocument { Id = "u-full", PartitionKey = "pk1", Name = "HasName", Value = 99, Tags = ["tag1"] };
		await _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		// Upsert with only some fields set — should fully replace
		var replacement = new TestDocument { Id = "u-full", PartitionKey = "pk1", Name = "OnlyName" };
		await _container.UpsertItemAsync(replacement, new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("u-full", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("OnlyName");
		read.Resource.Value.Should().Be(0); // default int, not 99
		read.Resource.Tags.Should().BeNullOrEmpty();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  RP: Replace Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ReplaceItem_FullyReplacesDocument_OldFieldsGone()
	{
		var original = new TestDocument { Id = "rp-full", PartitionKey = "pk1", Name = "Full", Value = 42, Tags = ["x"] };
		await _container.CreateItemAsync(original, new PartitionKey("pk1"));

		var replacement = new TestDocument { Id = "rp-full", PartitionKey = "pk1", Name = "Replaced" };
		await _container.ReplaceItemAsync(replacement, "rp-full", new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("rp-full", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Replaced");
		read.Resource.Value.Should().Be(0); // default, not 42
		read.Resource.Tags.Should().BeNullOrEmpty();
	}

	[Fact]
	public async Task Handler_ReplaceItem_DoubleReplace_SecondReflectsLatest()
	{
		var doc = new TestDocument { Id = "rp-dbl", PartitionKey = "pk1", Name = "V1" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "V2";
		await _container.ReplaceItemAsync(doc, "rp-dbl", new PartitionKey("pk1"));

		doc.Name = "V3";
		await _container.ReplaceItemAsync(doc, "rp-dbl", new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("rp-dbl", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("V3");
	}

	[Fact]
	public async Task Handler_ReplaceItem_ReturnsResourceWithUpdatedSystemProperties()
	{
		var doc = new TestDocument { Id = "rp-sys", PartitionKey = "pk1", Name = "Before" };
		var create = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		var originalETag = create.ETag;

		doc.Name = "After";
		var replace = await _container.ReplaceItemAsync(doc, "rp-sys", new PartitionKey("pk1"));

		replace.ETag.Should().NotBe(originalETag);

		var read = await _container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>("rp-sys", new PartitionKey("pk1"));
		read.Resource["_ts"].Should().NotBeNull();
		read.Resource["_etag"]!.ToString().Should().Be(replace.ETag);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D: Delete Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_DeleteItem_ThenCreateSameId_Succeeds()
	{
		var doc = new TestDocument { Id = "d-recreate", PartitionKey = "pk1", Name = "First" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("d-recreate", new PartitionKey("pk1"));

		doc.Name = "Recreated";
		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		var read = await _container.ReadItemAsync<TestDocument>("d-recreate", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Recreated");
	}

	[Fact]
	public async Task Handler_DeleteItem_DoubleDelete_SecondThrows404()
	{
		var doc = new TestDocument { Id = "d-dbl", PartitionKey = "pk1", Name = "DeleteMe" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("d-dbl", new PartitionKey("pk1"));

		var act = () => _container.DeleteItemAsync<TestDocument>("d-dbl", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  P: Patch Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_PatchItem_WithIfMatchETag_MatchingETag_Succeeds()
	{
		var doc = new TestDocument { Id = "p-ifm1", PartitionKey = "pk1", Name = "Original", Value = 10 };
		var create = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p-ifm1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { IfMatchEtag = create.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task Handler_PatchItem_WithIfMatchETag_StaleETag_ThrowsPreconditionFailed()
	{
		var doc = new TestDocument { Id = "p-ifm2", PartitionKey = "pk1", Name = "Original", Value = 10 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.PatchItemAsync<TestDocument>("p-ifm2", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Stale")],
			new PatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Handler_PatchItem_IncrementOperation_Standalone()
	{
		var doc = new TestDocument { Id = "p-inc", PartitionKey = "pk1", Name = "Counter", Value = 100 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p-inc", new PartitionKey("pk1"),
			[PatchOperation.Increment("/value", 10)]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Value.Should().Be(110);
	}

	[Fact]
	public async Task Handler_PatchItem_SetOnNestedPath_Succeeds()
	{
		var doc = new TestDocument
		{
			Id = "p-nest",
			PartitionKey = "pk1",
			Name = "Nested",
			Nested = new NestedObject { Description = "Original", Score = 1.0 }
		};
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p-nest", new PartitionKey("pk1"),
			[PatchOperation.Set("/nested/description", "Updated")]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Nested!.Description.Should().Be("Updated");
		response.Resource.Nested.Score.Should().Be(1.0); // unchanged
	}

	[Fact]
	public async Task Handler_PatchItem_ReplaceOperationType_MappedToSet()
	{
		// In real Cosmos, "replace" fails if path doesn't exist. The handler maps "replace" → Set,
		// which creates the path if missing. This test documents the mapping works end-to-end.
		var doc = new TestDocument { Id = "p-repl", PartitionKey = "pk1", Name = "Before", Value = 1 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p-repl", new PartitionKey("pk1"),
			[PatchOperation.Replace("/name", "Replaced")]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Replaced");
	}

	[Fact]
	public async Task Handler_PatchItem_ReturnsUpdatedETag()
	{
		var doc = new TestDocument { Id = "p-etag", PartitionKey = "pk1", Name = "Before", Value = 1 };
		var create = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var patch = await _container.PatchItemAsync<TestDocument>("p-etag", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "After")]);

		patch.ETag.Should().NotBeNullOrEmpty();
		patch.ETag.Should().NotBe(create.ETag);
	}

	[Fact]
	public async Task Handler_PatchItem_MultipleSetOnSamePath_LastWins()
	{
		var doc = new TestDocument { Id = "p-last", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p-last", new PartitionKey("pk1"),
		[
			PatchOperation.Set("/name", "First"),
			PatchOperation.Set("/name", "Second")
		]);

		response.Resource.Name.Should().Be("Second");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  PK: Partition Key Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_WithLargePartitionKey_Succeeds()
	{
		var largePk = new string('x', 1000);
		var doc = new TestDocument { Id = "pk-large", PartitionKey = largePk, Name = "LargePK" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey(largePk));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		var read = await _container.ReadItemAsync<TestDocument>("pk-large", new PartitionKey(largePk));
		read.Resource.Name.Should().Be("LargePK");
	}

	[Fact]
	public async Task Handler_CrudRoundTrip_WithSpecialCharsInPartitionKey()
	{
		var specialPk = "it's a \"quoted\" value / with \\ slashes";
		var doc = new TestDocument { Id = "pk-special", PartitionKey = specialPk, Name = "SpecialPK" };

		await _container.CreateItemAsync(doc, new PartitionKey(specialPk));
		var read = await _container.ReadItemAsync<TestDocument>("pk-special", new PartitionKey(specialPk));

		read.Resource.Name.Should().Be("SpecialPK");
		read.Resource.PartitionKey.Should().Be(specialPk);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  FI: Fault Injection on Other CRUD Ops
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_ThrottlesReadRequest()
	{
		using var cosmos = InMemoryCosmos.Create("fault-read", "/partitionKey",
			configureOptions: o => o.MaxRetryAttemptsOnRateLimitedRequests = 0);
		var handler = cosmos.Handler;
		var container = cosmos.Container;

		var doc = new TestDocument { Id = "fi-read", PartitionKey = "pk1", Name = "Throttled" };
		await container.CreateItemAsync(doc, new PartitionKey("pk1"));

		handler.FaultInjector = _ =>
			new HttpResponseMessage((HttpStatusCode)429)
			{
				Headers = { RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromMilliseconds(1)) }
			};
		handler.FaultInjectorIncludesMetadata = false;

		var act = () => container.ReadItemAsync<TestDocument>("fi-read", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be((HttpStatusCode)429);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_ReturnsServiceUnavailable_OnDelete()
	{
		var (handler, client, container) = CreateInMemoryStack("fault-del");
		using var _h = handler;
		using var _c = client;

		var doc = new TestDocument { Id = "fi-del", PartitionKey = "pk1", Name = "Unavailable" };
		await container.CreateItemAsync(doc, new PartitionKey("pk1"));

		handler.FaultInjector = _ =>
			new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
		handler.FaultInjectorIncludesMetadata = false;

		var act = () => container.DeleteItemAsync<TestDocument>("fi-del", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_NullReturn_ProceedsNormally()
	{
		var (handler, client, container) = CreateInMemoryStack("fault-null");
		using var _h = handler;
		using var _c = client;

		handler.FaultInjector = _ => null!;

		var doc = new TestDocument { Id = "fi-null", PartitionKey = "pk1", Name = "Normal" };
		var response = await container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  RL: Request Log Edge Cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_RequestLog_RecordsPatchOperation()
	{
		var (handler, client, container) = CreateInMemoryStack("log-patch");
		using var _h = handler;
		using var _c = client;

		var doc = new TestDocument { Id = "rl-patch", PartitionKey = "pk1", Name = "ToPatch" };
		await container.CreateItemAsync(doc, new PartitionKey("pk1"));

		await container.PatchItemAsync<TestDocument>("rl-patch", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")]);

		handler.RequestLog.Should().Contain(e => e.Contains("PATCH") || e.Contains("patch"));
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_RequestLog_RecordsUpsertOperation()
	{
		var (handler, client, container) = CreateInMemoryStack("log-upsert");
		using var _h = handler;
		using var _c = client;

		var doc = new TestDocument { Id = "rl-upsert", PartitionKey = "pk1", Name = "ToUpsert" };

		await container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		handler.RequestLog.Should().Contain(e => e.StartsWith("POST") && e.Contains("/docs"));
	}

	// ── IX: Error Path Edge Cases ───────────────────────────────────────────

	[Fact]
	public async Task Handler_EmptyBodyCreate_Returns400()
	{
		var act = () => _container.CreateItemAsync<JObject>(null!, new PartitionKey("pk1"));

		// Null body should result in an error
		await act.Should().ThrowAsync<Exception>();
	}
}
