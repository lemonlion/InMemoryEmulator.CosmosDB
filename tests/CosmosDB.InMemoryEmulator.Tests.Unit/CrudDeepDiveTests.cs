using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Phase A: ReadManyItems Coverage
// ═══════════════════════════════════════════════════════════════════════════

public class ReadManyItemsDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task ReadMany_HappyPath_ReturnsAllItems()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "b" }), new PartitionKey("b"));

		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a")),
			("2", new PartitionKey("a")),
			("3", new PartitionKey("b"))
		});

		result.Count.Should().Be(3);
	}

	[Fact]
	public async Task ReadMany_EmptyList_ReturnsEmptyFeedResponse()
	{
		var result = await _container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)>());

		result.Count.Should().Be(0);
	}

	[Fact]
	public async Task ReadMany_MissingItems_ReturnsPartialResults()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a" }), new PartitionKey("a"));

		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a")),
			("2", new PartitionKey("a")),
			("3", new PartitionKey("a")),
			("99", new PartitionKey("a")) // doesn't exist
        });

		result.Count.Should().Be(3);
	}

	[Fact]
	public async Task ReadMany_AllMissing_ReturnsEmptyFeedResponse()
	{
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("99", new PartitionKey("a")),
			("100", new PartitionKey("b"))
		});

		result.Count.Should().Be(0);
	}

	[Fact]
	public async Task ReadMany_WrongPartitionKey_ItemNotReturned()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("b")) // wrong PK
        });

		result.Count.Should().Be(0);
	}

	[Fact]
	public async Task ReadMany_NullInput_ThrowsArgumentNullException()
	{
		var act = () => _container.ReadManyItemsAsync<JObject>(null!);

		await act.Should().ThrowAsync<ArgumentNullException>();
	}

	[Fact]
	public async Task ReadMany_DuplicateIds_ReturnsDuplicates()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a")),
			("1", new PartitionKey("a"))
		});

		result.Count.Should().Be(2);
	}

	[Fact]
	public async Task ReadMany_AcrossPartitions_ReturnsAllMatching()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "b" }), new PartitionKey("b"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "c" }), new PartitionKey("c"));

		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a")),
			("3", new PartitionKey("c"))
		});

		result.Count.Should().Be(2);
	}

	[Fact]
	public async Task ReadMany_ResponseContainsETag()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a"))
		});

		result.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReadMany_WithCancelledToken_ThrowsOperationCancelled()
	{
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => _container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)>(), cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task ReadManyStream_HappyPath_ReturnsOk()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		using var response = await _container.ReadManyItemsStreamAsync(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a"))
		});

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().NotBeNull();
	}

	[Fact]
	public async Task ReadManyStream_EmptyList_ReturnsOk()
	{
		using var response = await _container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)>());

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase B: Patch CRUD Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class PatchItemCrudEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<JObject> CreateTestItem()
	{
		var item = JObject.FromObject(new { id = "item1", pk = "a", value = 10, name = "test" });
		await _container.CreateItemAsync(item, new PartitionKey("a"));
		return item;
	}

	[Fact]
	public async Task Patch_EmptyOperations_ThrowsBadRequest()
	{
		await CreateTestItem();

		var act = () => _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation>());

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Patch_Over10Operations_ThrowsBadRequest()
	{
		await CreateTestItem();

		var ops = Enumerable.Range(0, 11)
			.Select(i => PatchOperation.Set($"/field{i}", i))
			.ToList();

		var act = () => _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"), ops);

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Patch_OnSystemProperty_Ts_ThrowsBadRequest()
	{
		await CreateTestItem();

		var act = () => _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/_ts", 0) });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Patch_OnSystemProperty_Etag_ThrowsBadRequest()
	{
		await CreateTestItem();

		var act = () => _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/_etag", "x") });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Patch_OnId_ThrowsBadRequest()
	{
		await CreateTestItem();

		var act = () => _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/id", "newid") });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Patch_OnPartitionKey_ThrowsBadRequest()
	{
		await CreateTestItem();

		var act = () => _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/pk", "newpk") });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Patch_WithFilterPredicate_MatchesFalse_ThrowsPreconditionFailed()
	{
		await CreateTestItem();

		var act = () => _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/value", 99) },
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.value = 999" });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Patch_WithFilterPredicate_MatchesTrue_Succeeds()
	{
		await CreateTestItem();

		var result = await _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/value", 99) },
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.value = 10" });

		result.StatusCode.Should().Be(HttpStatusCode.OK);
		result.Resource["value"]!.Value<int>().Should().Be(99);
	}

	[Fact]
	public async Task Patch_NonExistent_ThrowsNotFound()
	{
		var act = () => _container.PatchItemAsync<JObject>("nonexistent", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/value", 99) });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Patch_WithIfMatchStaleETag_ThrowsPreconditionFailed()
	{
		await CreateTestItem();

		var act = () => _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/value", 99) },
			new PatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Patch_ChangesETag()
	{
		await CreateTestItem();
		var before = await _container.ReadItemAsync<JObject>("item1", new PartitionKey("a"));
		var etagBefore = before.ETag;

		await _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/value", 99) });

		var after = await _container.ReadItemAsync<JObject>("item1", new PartitionKey("a"));
		after.ETag.Should().NotBe(etagBefore);
	}

	[Fact]
	public async Task Patch_RecordsInChangeFeed()
	{
		var checkpoint = _container.GetChangeFeedCheckpoint();
		await CreateTestItem();
		var afterCreate = _container.GetChangeFeedCheckpoint();
		afterCreate.Should().BeGreaterThan(checkpoint);

		await _container.PatchItemAsync<JObject>("item1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/value", 99) });

		var afterPatch = _container.GetChangeFeedCheckpoint();
		afterPatch.Should().BeGreaterThan(afterCreate);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase C: Replace Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ReplaceItemExtendedEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Replace_WithIfMatchWildcard_NonExistentItem_ThrowsNotFound()
	{
		var act = () => _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "notexist", pk = "a" }),
			"notexist", new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Replace_ReplacesEntireDocument_NotMerge()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", field1 = "value1", field2 = "value2" }),
			new PartitionKey("a"));

		await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", field3 = "value3" }),
			"1", new PartitionKey("a"));

		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		item.Resource["field1"].Should().BeNull();
		item.Resource["field2"].Should().BeNull();
		item.Resource["field3"]!.Value<string>().Should().Be("value3");
	}

	[Fact]
	public async Task Replace_ChangesETag()
	{
		var created = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var etagBefore = created.ETag;

		var replaced = await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", newField = true }),
			"1", new PartitionKey("a"));

		replaced.ETag.Should().NotBe(etagBefore);
	}

	[Fact]
	public async Task Replace_ResponseResource_ContainsUpdatedData()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", value = "old" }),
			new PartitionKey("a"));

		var response = await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", value = "new" }),
			"1", new PartitionKey("a"));

		response.Resource["value"]!.Value<string>().Should().Be("new");
	}

	[Fact]
	public async Task Replace_WithNullPartitionKey_ExtractsFromDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var response = await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", updated = true }),
			"1");

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase D: Upsert Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class UpsertItemExtendedEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Upsert_WithIfNoneMatchEtag_IsIgnoredOnWrites()
	{
		// IfNoneMatch is a read-only concept, upsert should ignore it
		var response = await _container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		// Upsert should succeed or return appropriate status
		response.Should().NotBeNull();
	}

	[Fact]
	public async Task Upsert_WithEnableContentResponseOnWrite_True_ContainsResource()
	{
		var response = await _container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", value = 42 }),
			new PartitionKey("a"),
			new ItemRequestOptions { EnableContentResponseOnWrite = true });

		response.Resource.Should().NotBeNull();
		response.Resource["value"]!.Value<int>().Should().Be(42);
	}

	[Fact]
	public async Task Upsert_WithMissingPartitionKeyPath_InBody_FallsBackToId()
	{
		// container has PK /pk, but item body has no "pk" property
		var container = new InMemoryContainer("test", "/id");
		var response = await container.UpsertItemAsync(
			JObject.FromObject(new { id = "myid" }),
			new PartitionKey("myid"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase E: Create Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class CreateItemExtendedEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Create_WithWhitespaceOnlyId_Succeeds()
	{
		var response = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "   ", pk = "a" }),
			new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Create_JustUnder2MB_Succeeds()
	{
		var largeValue = new string('x', 1_900_000); // well under 2MB
		var item = JObject.FromObject(new { id = "large", pk = "a", data = largeValue });

		var response = await _container.CreateItemAsync(item, new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Create_ResponseContainsSessionToken()
	{
		var response = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		response.Headers.Session.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Create_ManyItemsSamePartition_AllRetrievable()
	{
		for (var i = 0; i < 100; i++)
		{
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"item-{i}", pk = "same" }),
				new PartitionKey("same"));
		}

		for (var i = 0; i < 100; i++)
		{
			var response = await _container.ReadItemAsync<JObject>($"item-{i}", new PartitionKey("same"));
			response.StatusCode.Should().Be(HttpStatusCode.OK);
		}
	}

	[Fact]
	public async Task Create_WithNumericId_Succeeds()
	{
		var response = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "12345", pk = "a" }),
			new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await _container.ReadItemAsync<JObject>("12345", new PartitionKey("a"));
		read.Resource["id"]!.Value<string>().Should().Be("12345");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase F: Delete Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class DeleteItemExtendedEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Delete_DoubleDelete_SecondCallThrows404()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		await _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));

		var act = () => _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Delete_ReturnedRequestCharge_IsOnePointZero()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var response = await _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));

		response.RequestCharge.Should().Be(1.0);
	}

	[Fact]
	public async Task Delete_ResponseContainsHeaders()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var response = await _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));

		response.Headers.Session.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase G: Read Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ReadItemExtendedEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Read_WithIfNoneMatch_WildcardStar_NonExistentItem_Throws404()
	{
		var act = () => _container.ReadItemAsync<JObject>("nonexistent", new PartitionKey("a"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Read_MultipleReads_SameETag()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var r1 = await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		var r2 = await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));

		r1.ETag.Should().Be(r2.ETag);
	}

	[Fact]
	public async Task Read_ResponseHasDiagnostics_NonNull()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));

		response.Diagnostics.Should().NotBeNull();
	}

	[Fact]
	public async Task Read_ResponseHasActivityId_NonNull()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));

		response.ActivityId.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase H: DeleteAllByPK Extended
// ═══════════════════════════════════════════════════════════════════════════

public class DeleteAllByPKDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task DeleteAll_EmptyPartition_ReturnsOk()
	{
		using var response = await _container.DeleteAllItemsByPartitionKeyStreamAsync(
			new PartitionKey("nonexistent"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteAll_WithPartitionKeyNone_DeletesNullPkItems()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1" }), PartitionKey.None);
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a" }),
			new PartitionKey("a"));

		using var response = await _container.DeleteAllItemsByPartitionKeyStreamAsync(PartitionKey.None);

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		// Item with PK.None should be gone
		var act = () => _container.ReadItemAsync<JObject>("1", PartitionKey.None);
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);

		// Item with other PK should remain
		var r = await _container.ReadItemAsync<JObject>("2", new PartitionKey("a"));
		r.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteAll_LargePartition_100Items_AllDeleted()
	{
		for (var i = 0; i < 100; i++)
		{
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"item-{i}", pk = "bulk" }),
				new PartitionKey("bulk"));
		}

		using var response = await _container.DeleteAllItemsByPartitionKeyStreamAsync(
			new PartitionKey("bulk"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		// All should be gone
		for (var i = 0; i < 5; i++) // spot check
		{
			var act = () => _container.ReadItemAsync<JObject>($"item-{i}", new PartitionKey("bulk"));
			(await act.Should().ThrowAsync<CosmosException>()).Which
				.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase I: Cross-Cutting / Interaction Tests
// ═══════════════════════════════════════════════════════════════════════════

public class CrudInteractionTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Create_Replace_Delete_Create_FullLifecycle()
	{
		// Create
		var created = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", state = "created" }),
			new PartitionKey("a"));
		created.StatusCode.Should().Be(HttpStatusCode.Created);

		// Replace
		var replaced = await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", state = "replaced" }),
			"1", new PartitionKey("a"));
		replaced.StatusCode.Should().Be(HttpStatusCode.OK);

		// Delete
		var deleted = await _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));
		deleted.StatusCode.Should().Be(HttpStatusCode.NoContent);

		// Re-create
		var recreated = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", state = "recreated" }),
			new PartitionKey("a"));
		recreated.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		read.Resource["state"]!.Value<string>().Should().Be("recreated");
	}

	[Fact]
	public async Task Upsert_Then_ReadMany_ReturnsLatestVersion()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", version = 1 }),
			new PartitionKey("a"));

		await _container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", version = 2 }),
			new PartitionKey("a"));

		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a"))
		});

		result.Count.Should().Be(1);
		result.First()["version"]!.Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task Patch_Then_Replace_ReplacesAllIncludingPatchedFields()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", field1 = "original", field2 = "keep" }),
			new PartitionKey("a"));

		await _container.PatchItemAsync<JObject>("1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/field1", "patched") });

		await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", field3 = "replaced" }),
			"1", new PartitionKey("a"));

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		read.Resource["field1"].Should().BeNull(); // gone after replace
		read.Resource["field2"].Should().BeNull(); // gone after replace
		read.Resource["field3"]!.Value<string>().Should().Be("replaced");
	}

	[Fact]
	public async Task Create_WithPreTrigger_ThenReadMany_TriggerFieldsPresent()
	{
		_container.RegisterTrigger("addField", Microsoft.Azure.Cosmos.Scripts.TriggerType.Pre,
			Microsoft.Azure.Cosmos.Scripts.TriggerOperation.All,
			(Func<JObject, JObject>)(doc =>
			{
				doc["triggerField"] = "added-by-trigger";
				return doc;
			}));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a"))
		});

		result.First()["triggerField"]!.Value<string>().Should().Be("added-by-trigger");
	}
}
