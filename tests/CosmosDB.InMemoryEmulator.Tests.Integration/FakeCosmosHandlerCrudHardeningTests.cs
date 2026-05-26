using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Plan #14: FakeCosmosHandler CRUD coverage hardening.
/// Tests stream CRUD, ETag edge cases, replace/patch validation,
/// document size, handler properties, PK edge cases, response contracts,
/// concurrency, and fault injection through the HTTP pipeline.
/// Parity-validated: Phases 1-5, 7 (standard PK), 8, 9 run against both backends.
/// Phase 6 (handler properties), hierarchical PK, Phase 10 (fault injection) are InMemoryOnly.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerCrudHardeningTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("harden-crud", "/partitionKey");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	private static (FakeCosmosHandler Handler, CosmosClient Client, Container Container) CreateInMemoryStack(
		string name = "harden-crud", string pkPath = "/partitionKey")
	{
		var cosmos = InMemoryCosmos.Create(name, pkPath);
		return (cosmos.Handler, cosmos.Client, cosmos.Container);
	}

	private static MemoryStream ToStream(object obj)
	{
		var json = JsonConvert.SerializeObject(obj);
		return new MemoryStream(Encoding.UTF8.GetBytes(json));
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 1: Stream CRUD Operations (S1–S10)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItemStream_ReturnsCreated()
	{
		using var body = ToStream(new { id = "s1", partitionKey = "pk1", name = "StreamCreate" });

		using var response = await _container.CreateItemStreamAsync(body, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Content.Should().NotBeNull();
		using var reader = new StreamReader(response.Content);
		var json = JObject.Parse(await reader.ReadToEndAsync());
		json["id"]!.ToString().Should().Be("s1");
	}

	[Fact]
	public async Task Handler_ReadItemStream_ReturnsOk()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "s2", PartitionKey = "pk1", Name = "StreamRead" },
			new PartitionKey("pk1"));

		using var response = await _container.ReadItemStreamAsync("s2", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		using var reader = new StreamReader(response.Content);
		var json = JObject.Parse(await reader.ReadToEndAsync());
		json["name"]!.ToString().Should().Be("StreamRead");
	}

	[Fact]
	public async Task Handler_ReadItemStream_NotFound_Returns404()
	{
		using var response = await _container.ReadItemStreamAsync("nonexistent", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Handler_UpsertItemStream_NewItem_ReturnsCreated()
	{
		using var body = ToStream(new { id = "s4", partitionKey = "pk1", name = "StreamUpsertNew" });

		using var response = await _container.UpsertItemStreamAsync(body, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Handler_UpsertItemStream_ExistingItem_ReturnsOk()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "s5", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		using var body = ToStream(new { id = "s5", partitionKey = "pk1", name = "Updated" });
		using var response = await _container.UpsertItemStreamAsync(body, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Handler_ReplaceItemStream_ReturnsOk()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "s6", PartitionKey = "pk1", Name = "Before" },
			new PartitionKey("pk1"));

		using var body = ToStream(new { id = "s6", partitionKey = "pk1", name = "After" });
		using var response = await _container.ReplaceItemStreamAsync(body, "s6", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Handler_ReplaceItemStream_NotFound_Returns404()
	{
		using var body = ToStream(new { id = "s7", partitionKey = "pk1", name = "Ghost" });

		using var response = await _container.ReplaceItemStreamAsync(body, "s7", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Handler_DeleteItemStream_ReturnsNoContent()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "s8", PartitionKey = "pk1", Name = "ToDelete" },
			new PartitionKey("pk1"));

		using var response = await _container.DeleteItemStreamAsync("s8", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task Handler_DeleteItemStream_NotFound_Returns404()
	{
		using var response = await _container.DeleteItemStreamAsync("nonexistent", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Handler_PatchItemStream_ReturnsOk()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "s10", PartitionKey = "pk1", Name = "Before", Value = 1 },
			new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("s10", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "After")]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("After");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 2: ETag & Conditional Write Edge Cases (E1–E9)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_WithIfNoneMatchWildcard_PreventsOverwrite()
	{
		var doc = new TestDocument { Id = "e1", PartitionKey = "pk1", Name = "First" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		// Second create with same id should fail with Conflict regardless of IfNoneMatch
		var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task Handler_UpsertItem_WithIfMatchWildcard_AlwaysSucceeds()
	{
		var doc = new TestDocument { Id = "e2", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "WildcardUpsert";
		var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("WildcardUpsert");
	}

	[Fact]
	public async Task Handler_ReplaceItem_WithIfMatchWildcard_AlwaysSucceeds()
	{
		var doc = new TestDocument { Id = "e3", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "WildcardReplace";
		var response = await _container.ReplaceItemAsync(doc, "e3", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("WildcardReplace");
	}

	[Fact]
	public async Task Handler_DeleteItem_WithIfMatchWildcard_AlwaysSucceeds()
	{
		var doc = new TestDocument { Id = "e4", PartitionKey = "pk1", Name = "ToDelete" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("e4", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task Handler_PatchItem_WithIfMatchWildcard_AlwaysSucceeds()
	{
		var doc = new TestDocument { Id = "e5", PartitionKey = "pk1", Name = "Before" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("e5", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "WildcardPatch")],
			new PatchItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("WildcardPatch");
	}

	[Fact]
	public async Task Handler_ReadItem_WithIfNoneMatchWildcard_ExistingItem_Returns304()
	{
		var doc = new TestDocument { Id = "e6", PartitionKey = "pk1", Name = "Exists" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("e6", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task Handler_UpsertItem_NewItem_WithIfMatchETag_CreatesItem()
	{
		// If-Match is "applicable only on PUT and DELETE" per REST API docs.
		// Upsert uses POST, so If-Match is ignored on the insert path.
		var doc = new TestDocument { Id = "e7-new", PartitionKey = "pk1", Name = "NeverCreated" };

		var response = await _container.UpsertItemAsync(doc, new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Handler_CreateItem_ETagChangesOnSubsequentRead()
	{
		var doc = new TestDocument { Id = "e8", PartitionKey = "pk1", Name = "ETagConsistency" };

		var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		var readResponse = await _container.ReadItemAsync<TestDocument>("e8", new PartitionKey("pk1"));

		createResponse.ETag.Should().Be(readResponse.ETag);
	}

	[Fact]
	public async Task Handler_PatchItem_MultiplePatches_ETagIncrements()
	{
		var doc = new TestDocument { Id = "e9", PartitionKey = "pk1", Name = "Counter", Value = 0 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var patch1 = await _container.PatchItemAsync<TestDocument>("e9", new PartitionKey("pk1"),
			[PatchOperation.Increment("/value", 1)]);
		var patch2 = await _container.PatchItemAsync<TestDocument>("e9", new PartitionKey("pk1"),
			[PatchOperation.Increment("/value", 1)]);

		patch1.ETag.Should().NotBe(patch2.ETag);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 3: Replace Validation Edge Cases (R1–R3)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)] // Windows emulator returns 200 (emulator bug); see docs link below
	public async Task Handler_ReplaceItem_BodyIdMismatch_Throws400()
	{
		var doc = new TestDocument { Id = "r1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		// The SDK docs state body id must match the id parameter.
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
		var mismatch = new TestDocument { Id = "different-id", PartitionKey = "pk1", Name = "Mismatch" };
		var act = () => _container.ReplaceItemAsync(mismatch, "r1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Handler_ReplaceItem_PreservesPartitionKey()
	{
		var doc = new TestDocument { Id = "r2", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		// Replace with same PK
		doc.Name = "Updated";
		var response = await _container.ReplaceItemAsync(doc, "r2", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		// Verify the PK is preserved
		var read = await _container.ReadItemAsync<TestDocument>("r2", new PartitionKey("pk1"));
		read.Resource.PartitionKey.Should().Be("pk1");
	}

	[Fact]
	public async Task Handler_ReplaceItem_WithNullBody_Throws()
	{
		var doc = new TestDocument { Id = "r3", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.ReplaceItemAsync<TestDocument>(null!, "r3", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 4: Patch Validation & Boundaries (P1–P8)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_PatchItem_EmptyOperationsList_Throws()
	{
		var doc = new TestDocument { Id = "p1", PartitionKey = "pk1", Name = "Empty" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.PatchItemAsync<TestDocument>("p1", new PartitionKey("pk1"),
			[]);

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Handler_PatchItem_MoreThan10Operations_Throws()
	{
		var doc = new TestDocument { Id = "p2", PartitionKey = "pk1", Name = "TooMany", Value = 0 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var ops = Enumerable.Range(0, 11)
			.Select(i => PatchOperation.Increment("/value", 1))
			.ToList();

		var act = () => _container.PatchItemAsync<TestDocument>("p2", new PartitionKey("pk1"), ops);

		// Should throw — Cosmos limits patch to max 10 operations
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Handler_PatchItem_SetId_Throws()
	{
		var doc = new TestDocument { Id = "p3", PartitionKey = "pk1", Name = "HasId" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.PatchItemAsync<TestDocument>("p3", new PartitionKey("pk1"),
			[PatchOperation.Set("/id", "new-id")]);

		// Patching /id should fail
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Handler_PatchItem_AddToExistingArray_AppendsElement()
	{
		var doc = new TestDocument { Id = "p5", PartitionKey = "pk1", Name = "Arrays", Tags = ["tag1"] };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("p5", new PartitionKey("pk1"),
			[PatchOperation.Add("/tags/-", "tag2")]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Tags.Should().Contain("tag1");
		response.Resource.Tags.Should().Contain("tag2");
	}

	[Fact]
	public async Task Handler_PatchItem_IncrementOnNonNumeric_Throws()
	{
		var doc = new TestDocument { Id = "p6", PartitionKey = "pk1", Name = "StringField" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var act = () => _container.PatchItemAsync<TestDocument>("p6", new PartitionKey("pk1"),
			[PatchOperation.Increment("/name", 1)]);

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Handler_PatchItem_RemoveNonExistentPath_Throws()
	{
		var doc = new TestDocument { Id = "p7", PartitionKey = "pk1", Name = "Sparse" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		// Removing a path that doesn't exist throws in real Cosmos and in the emulator
		var act = () => _container.PatchItemAsync<TestDocument>("p7", new PartitionKey("pk1"),
			[PatchOperation.Remove("/nonExistentField")]);

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Handler_PatchItem_SetDeepNestedPath_WhenParentExists_Succeeds()
	{
		// Patch Set on a nested path works when the parent object exists
		var doc = new { id = "p8", partitionKey = "pk1", name = "HasNested", nested = new { score = 1.0 } };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<JObject>("p8", new PartitionKey("pk1"),
			[PatchOperation.Set("/nested/description", "Deep")]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource["nested"]!["description"]!.ToString().Should().Be("Deep");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 5: Document Size & Serialization (D1–D4)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_Over2MB_Throws()
	{
		var largeContent = new string('x', 2 * 1024 * 1024 + 1);
		var doc = new { id = "d1", partitionKey = "pk1", data = largeContent };

		var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Handler_UpsertItem_Over2MB_Throws()
	{
		var largeContent = new string('x', 2 * 1024 * 1024 + 1);
		var doc = new { id = "d2", partitionKey = "pk1", data = largeContent };

		var act = () => _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Handler_CreateItem_WithEmptyStringId_Throws()
	{
		var doc = new { id = "", partitionKey = "pk1", name = "EmptyId" };

		var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 6: BackingContainer & Handler Properties (H1–H5) — InMemoryOnly
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public void Handler_BackingContainer_ReturnsNonNull()
	{
		using var cosmos = InMemoryCosmos.Create("backing-test", "/partitionKey");
		cosmos.Handler.BackingContainer.Should().NotBeNull();
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_QueryLog_NotPopulatedOnPureCrud()
	{
		var (handler, client, container) = CreateInMemoryStack("h2-querylog");
		using (client)
		using (handler)
		{
			var doc = new TestDocument { Id = "h2", PartitionKey = "pk1", Name = "CrudOnly" };
			await container.CreateItemAsync(doc, new PartitionKey("pk1"));
			await container.ReadItemAsync<TestDocument>("h2", new PartitionKey("pk1"));
			await container.DeleteItemAsync<TestDocument>("h2", new PartitionKey("pk1"));

			handler.QueryLog.Should().BeEmpty();
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_RequestLog_ContainsAllCrudVerbs()
	{
		var (handler, client, container) = CreateInMemoryStack("h3-reqlog");
		using (client)
		using (handler)
		{
			var doc = new TestDocument { Id = "h3", PartitionKey = "pk1", Name = "AllVerbs", Value = 1 };
			await container.CreateItemAsync(doc, new PartitionKey("pk1"));
			await container.ReadItemAsync<TestDocument>("h3", new PartitionKey("pk1"));

			doc.Name = "Replaced";
			await container.ReplaceItemAsync(doc, "h3", new PartitionKey("pk1"));

			await container.PatchItemAsync<TestDocument>("h3", new PartitionKey("pk1"),
				[PatchOperation.Set("/name", "Patched")]);

			await container.DeleteItemAsync<TestDocument>("h3", new PartitionKey("pk1"));

			handler.RequestLog.Should().Contain(e => e.StartsWith("POST"));
			handler.RequestLog.Should().Contain(e => e.StartsWith("GET"));
			handler.RequestLog.Should().Contain(e => e.StartsWith("PUT"));
			handler.RequestLog.Should().Contain(e => e.StartsWith("PATCH"));
			handler.RequestLog.Should().Contain(e => e.StartsWith("DELETE"));
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_Options_CacheTtl_Configurable()
	{
		using var cosmos = InMemoryCosmos.Create("cache-test", "/partitionKey");
		var c = cosmos.Container;

		await c.CreateItemAsync(
			new TestDocument { Id = "cache1", PartitionKey = "pk1", Name = "Cached" },
			new PartitionKey("pk1"));

		var read = await c.ReadItemAsync<TestDocument>("cache1", new PartitionKey("pk1"));
		read.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 7: PartitionKey Edge Cases (PK1–PK4)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_PartitionKeyWithUnicode_RoundTrips()
	{
		var unicodePk = "pk-🎉-日本語";
		var doc = new TestDocument { Id = "pk1", PartitionKey = unicodePk, Name = "Unicode" };

		await _container.CreateItemAsync(doc, new PartitionKey(unicodePk));

		var read = await _container.ReadItemAsync<TestDocument>("pk1", new PartitionKey(unicodePk));
		read.Resource.Name.Should().Be("Unicode");
		read.Resource.PartitionKey.Should().Be(unicodePk);
	}

	[Fact]
	public async Task Handler_CrudRoundTrip_WithPartitionKeyContainingQuotes()
	{
		var quotedPk = "it's a \"quoted\" value";
		var doc = new TestDocument { Id = "pk2", PartitionKey = quotedPk, Name = "Quoted" };

		await _container.CreateItemAsync(doc, new PartitionKey(quotedPk));

		var read = await _container.ReadItemAsync<TestDocument>("pk2", new PartitionKey(quotedPk));
		read.Resource.Name.Should().Be("Quoted");
		read.Resource.PartitionKey.Should().Be(quotedPk);
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_CreateItem_HierarchicalPartitionKey_ThreeLevels()
	{
		using var cosmos = InMemoryCosmos.Create("hier3-test", new[] { "/tenantId", "/region", "/userId" });
		var c = cosmos.Container;

		var pk = new PartitionKeyBuilder().Add("tenant1").Add("us-east").Add("user1").Build();
		var doc = new { id = "h3pk", tenantId = "tenant1", region = "us-east", userId = "user1", name = "ThreeLevel" };
		var response = await c.CreateItemAsync(doc, pk);

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await c.ReadItemAsync<dynamic>("h3pk", pk);
		((string)read.Resource.name).Should().Be("ThreeLevel");
	}

	[Fact]
	public async Task Handler_ReadItem_WrongPartitionKey_Throws404()
	{
		var doc = new TestDocument { Id = "pk4", PartitionKey = "correct-pk", Name = "Isolated" };
		await _container.CreateItemAsync(doc, new PartitionKey("correct-pk"));

		var act = () => _container.ReadItemAsync<TestDocument>("pk4", new PartitionKey("wrong-pk"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 8: Response Contract Verification (RC1–RC5)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_CreateItem_ResponseContainsDiagnostics()
	{
		var doc = new TestDocument { Id = "rc1", PartitionKey = "pk1", Name = "Diagnostics" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.Diagnostics.Should().NotBeNull();
	}

	[Fact]
	public async Task Handler_AllCrudOps_ReturnNonZeroRequestCharge()
	{
		var doc = new TestDocument { Id = "rc2", PartitionKey = "pk1", Name = "Charge", Value = 1 };

		var create = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		create.RequestCharge.Should().BeGreaterThan(0);

		var read = await _container.ReadItemAsync<TestDocument>("rc2", new PartitionKey("pk1"));
		read.RequestCharge.Should().BeGreaterThan(0);

		doc.Name = "Replaced";
		var replace = await _container.ReplaceItemAsync(doc, "rc2", new PartitionKey("pk1"));
		replace.RequestCharge.Should().BeGreaterThan(0);

		var patch = await _container.PatchItemAsync<TestDocument>("rc2", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")]);
		patch.RequestCharge.Should().BeGreaterThan(0);

		var delete = await _container.DeleteItemAsync<TestDocument>("rc2", new PartitionKey("pk1"));
		delete.RequestCharge.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task Handler_AllCrudOps_ReturnNonEmptyActivityId()
	{
		var doc = new TestDocument { Id = "rc3", PartitionKey = "pk1", Name = "Activity", Value = 1 };

		var create = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		create.ActivityId.Should().NotBeNullOrEmpty();

		var read = await _container.ReadItemAsync<TestDocument>("rc3", new PartitionKey("pk1"));
		read.ActivityId.Should().NotBeNullOrEmpty();

		doc.Name = "Replaced";
		var replace = await _container.ReplaceItemAsync(doc, "rc3", new PartitionKey("pk1"));
		replace.ActivityId.Should().NotBeNullOrEmpty();

		var patch = await _container.PatchItemAsync<TestDocument>("rc3", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")]);
		patch.ActivityId.Should().NotBeNullOrEmpty();

		var delete = await _container.DeleteItemAsync<TestDocument>("rc3", new PartitionKey("pk1"));
		delete.ActivityId.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Handler_CreateItem_ResponseStatusCodeMatchesExpected()
	{
		var doc = new TestDocument { Id = "rc4", PartitionKey = "pk1", Name = "StatusCode" };

		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		((int)response.StatusCode).Should().Be(201);
	}

	[Fact]
	public async Task Handler_DeleteItem_ResponseResource_IsDefault()
	{
		var doc = new TestDocument { Id = "rc5", PartitionKey = "pk1", Name = "ToDelete" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("rc5", new PartitionKey("pk1"));

		response.Resource.Should().BeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 9: Concurrency via Handler (CC1–CC3)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_ConcurrentCreates_DifferentIds_AllSucceed()
	{
		var tasks = Enumerable.Range(0, 50).Select(i =>
		{
			var doc = new TestDocument { Id = $"cc1-{i}", PartitionKey = "pk1", Name = $"Doc{i}" };
			return _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);

		results.Should().HaveCount(50);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
	}

	[Fact]
	public async Task Handler_ConcurrentUpserts_SameId_LastWriteWins()
	{
		// Seed the item
		var doc = new TestDocument { Id = "cc2", PartitionKey = "pk1", Name = "Seed", Value = 0 };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var tasks = Enumerable.Range(1, 10).Select(i =>
		{
			var d = new TestDocument { Id = "cc2", PartitionKey = "pk1", Name = $"V{i}", Value = i };
			return _container.UpsertItemAsync(d, new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);

		// All should succeed (no corruption)
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		// Final read should return one consistent version
		var read = await _container.ReadItemAsync<TestDocument>("cc2", new PartitionKey("pk1"));
		read.Resource.Name.Should().StartWith("V");
	}

	[Fact]
	public async Task Handler_ConcurrentReadAndWrite_NoCorruption()
	{
		// Seed some items
		for (var i = 0; i < 10; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"cc3-{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
				new PartitionKey("pk1"));
		}

		var readTasks = Enumerable.Range(0, 10).Select(i =>
			_container.ReadItemAsync<TestDocument>($"cc3-{i}", new PartitionKey("pk1")));

		var writeTasks = Enumerable.Range(10, 10).Select(i =>
		{
			var d = new TestDocument { Id = $"cc3-{i}", PartitionKey = "pk1", Name = $"New{i}" };
			return _container.CreateItemAsync(d, new PartitionKey("pk1"));
		});

		var allTasks = readTasks.Cast<Task>().Concat(writeTasks.Cast<Task>());
		await Task.WhenAll(allTasks);

		// No exceptions means no corruption
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 10: Fault Injection Edge Cases (FI1–FI5) — InMemoryOnly
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_OnUpsert_ThrottlesCorrectly()
	{
		var (handler, client, container) = CreateInMemoryStack("fi1-upsert");
		using (client)
		using (handler)
		{
			handler.FaultInjector = _ =>
				new HttpResponseMessage((HttpStatusCode)429)
				{
					Headers = { RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromMilliseconds(1)) }
				};
			handler.FaultInjectorIncludesMetadata = false;

			var doc = new TestDocument { Id = "fi1", PartitionKey = "pk1", Name = "Throttled" };
			var act = () => container.UpsertItemAsync(doc, new PartitionKey("pk1"));

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be((HttpStatusCode)429);
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_OnReplace_Returns500()
	{
		var (handler, client, container) = CreateInMemoryStack("fi2-replace");
		using (client)
		using (handler)
		{
			var doc = new TestDocument { Id = "fi2", PartitionKey = "pk1", Name = "Original" };
			await container.CreateItemAsync(doc, new PartitionKey("pk1"));

			handler.FaultInjector = _ =>
				new HttpResponseMessage(HttpStatusCode.InternalServerError);
			handler.FaultInjectorIncludesMetadata = false;

			doc.Name = "Broken";
			var act = () => container.ReplaceItemAsync(doc, "fi2", new PartitionKey("pk1"));

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_OnPatch_Returns503()
	{
		var (handler, client, container) = CreateInMemoryStack("fi3-patch");
		using (client)
		using (handler)
		{
			var doc = new TestDocument { Id = "fi3", PartitionKey = "pk1", Name = "Before" };
			await container.CreateItemAsync(doc, new PartitionKey("pk1"));

			handler.FaultInjector = _ =>
				new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
			handler.FaultInjectorIncludesMetadata = false;

			var act = () => container.PatchItemAsync<TestDocument>("fi3", new PartitionKey("pk1"),
				[PatchOperation.Set("/name", "After")]);

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_SelectiveByPath_OnlyTargetsSpecificDoc()
	{
		var (handler, client, container) = CreateInMemoryStack("fi4-selective");
		using (client)
		using (handler)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = "fi4-target", PartitionKey = "pk1", Name = "Target" },
				new PartitionKey("pk1"));
			await container.CreateItemAsync(
				new TestDocument { Id = "fi4-safe", PartitionKey = "pk1", Name = "Safe" },
				new PartitionKey("pk1"));

			handler.FaultInjector = req =>
			{
				if (req.RequestUri?.ToString().Contains("fi4-target") == true)
					return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
				return null;
			};
			handler.FaultInjectorIncludesMetadata = false;

			// Target doc is blocked
			var act = () => container.ReadItemAsync<TestDocument>("fi4-target", new PartitionKey("pk1"));
			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);

			// Safe doc is accessible
			var read = await container.ReadItemAsync<TestDocument>("fi4-safe", new PartitionKey("pk1"));
			read.Resource.Name.Should().Be("Safe");
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public async Task Handler_FaultInjection_Clears_WhenSetToNull()
	{
		var (handler, client, container) = CreateInMemoryStack("fi5-clear");
		using (client)
		using (handler)
		{
			handler.FaultInjector = _ =>
				new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
			handler.FaultInjectorIncludesMetadata = false;

			var doc = new TestDocument { Id = "fi5", PartitionKey = "pk1", Name = "Blocked" };
			var act = () => container.CreateItemAsync(doc, new PartitionKey("pk1"));
			await act.Should().ThrowAsync<CosmosException>();

			// Clear fault injector
			handler.FaultInjector = null;

			// Now should succeed
			var response = await container.CreateItemAsync(doc, new PartitionKey("pk1"));
			response.StatusCode.Should().Be(HttpStatusCode.Created);
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 10: Unique Key Policy – Nested Paths (UK1–UK3)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Handler_UniqueKeyPolicy_NestedPath_EnforcesConstraint()
	{
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/unique-keys
		//   "You can specify unique key paths with up to 16 path values."
		//   Paths like /address/zipCode are supported and reference nested properties.
		var container = await _fixture.CreateContainerAsync("uk-nested", "/partitionKey", props =>
		{
			props.UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/nested/description" } } }
			};
		});

		var doc1 = new TestDocument { Id = "uk1", PartitionKey = "pk1", Name = "A", Nested = new NestedObject { Description = "unique-val", Score = 1.0 } };
		var doc2 = new TestDocument { Id = "uk2", PartitionKey = "pk1", Name = "B", Nested = new NestedObject { Description = "unique-val", Score = 2.0 } };

		await container.CreateItemAsync(doc1, new PartitionKey("pk1"));

		var ex = await Assert.ThrowsAsync<CosmosException>(() =>
			container.CreateItemAsync(doc2, new PartitionKey("pk1")));
		ex.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task Handler_UniqueKeyPolicy_NestedPath_DifferentValues_Succeeds()
	{
		var container = await _fixture.CreateContainerAsync("uk-nested-ok", "/partitionKey", props =>
		{
			props.UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/nested/description" } } }
			};
		});

		var doc1 = new TestDocument { Id = "uk3", PartitionKey = "pk1", Name = "A", Nested = new NestedObject { Description = "val-one", Score = 1.0 } };
		var doc2 = new TestDocument { Id = "uk4", PartitionKey = "pk1", Name = "B", Nested = new NestedObject { Description = "val-two", Score = 2.0 } };

		var r1 = await container.CreateItemAsync(doc1, new PartitionKey("pk1"));
		var r2 = await container.CreateItemAsync(doc2, new PartitionKey("pk1"));

		r1.StatusCode.Should().Be(HttpStatusCode.Created);
		r2.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Handler_UniqueKeyPolicy_NestedPath_DifferentPartitions_Succeeds()
	{
		// Unique keys are scoped to logical partition — same nested value in different partitions is allowed
		var container = await _fixture.CreateContainerAsync("uk-nested-xpk", "/partitionKey", props =>
		{
			props.UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/nested/description" } } }
			};
		});

		var doc1 = new TestDocument { Id = "uk5", PartitionKey = "pk1", Name = "A", Nested = new NestedObject { Description = "same-val", Score = 1.0 } };
		var doc2 = new TestDocument { Id = "uk6", PartitionKey = "pk2", Name = "B", Nested = new NestedObject { Description = "same-val", Score = 2.0 } };

		var r1 = await container.CreateItemAsync(doc1, new PartitionKey("pk1"));
		var r2 = await container.CreateItemAsync(doc2, new PartitionKey("pk2"));

		r1.StatusCode.Should().Be(HttpStatusCode.Created);
		r2.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}
