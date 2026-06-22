using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Red-team edge case tests for ETag/IfMatchEtag implementation.
/// Designed to find subtle bugs in concurrency, format handling,
/// cross-API consistency, and state management.
/// </summary>
public class ETagRedTeamEdgeCaseTests : IDisposable
{
    private readonly InMemoryCosmosResult _cosmos = InMemoryCosmos.Create("etag-redteam", "/partitionKey");

    public void Dispose() => _cosmos.Dispose();

    // ═══════════════════════════════════════════════════════════════════════════
    //  1. ETag Format Edge Cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IfMatchEtag_EmptyString_IsIgnoredByPipeline()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "fmt-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        // Through the SDK pipeline, empty string is not a valid HTTP ETag header value.
        // The SDK/HTTP layer drops it, so the operation proceeds without an ETag check.
        var response = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "fmt-1", PartitionKey = "pk1", Name = "Updated" },
            "fmt-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task IfMatchEtag_WhitespaceOnly_IsIgnoredByPipeline()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "fmt-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        // Through the SDK pipeline, whitespace-only is not a valid HTTP ETag header value.
        // The SDK/HTTP layer drops it, so the operation proceeds without an ETag check.
        var response = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "fmt-2", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "   " });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task IfMatchEtag_WithoutQuotes_IsIgnoredByPipeline()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "fmt-3", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        // ETags are generated as quoted hex strings; unquoted version is not a valid
        // HTTP ETag format. Through the SDK pipeline, the HTTP layer drops it,
        // so the operation proceeds without an ETag check.
        var quotedEtag = create.ETag;
        var unquotedEtag = quotedEtag.Trim('"');

        var response = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "fmt-3", PartitionKey = "pk1", Name = "Updated" },
            "fmt-3", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = unquotedEtag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task IfMatchEtag_VeryLongString_ShouldReject()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "fmt-4", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var longEtag = "\"" + new string('a', 10000) + "\"";

        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "fmt-4", PartitionKey = "pk1", Name = "Updated" },
            "fmt-4", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = longEtag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task IfMatchEtag_SpecialCharacters_ShouldReject()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "fmt-5", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "fmt-5", PartitionKey = "pk1", Name = "Updated" },
            "fmt-5", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"<script>alert('xss')</script>\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task IfMatchEtag_CaseSensitivity_ETagsShouldBeCaseSensitive()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "fmt-6", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var etag = create.ETag;
        var upperCaseEtag = etag.ToUpperInvariant();

        // If the generated etag has lowercase hex, upper-casing it should fail
        if (etag != upperCaseEtag)
        {
            var act = () => _cosmos.Container.ReplaceItemAsync(
                new TestDocument { Id = "fmt-6", PartitionKey = "pk1", Name = "Updated" },
                "fmt-6", new PartitionKey("pk1"),
                new ItemRequestOptions { IfMatchEtag = upperCaseEtag });

            var ex = await act.Should().ThrowAsync<CosmosException>();
            ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  2. Wildcard Edge Cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IfMatchEtag_WildcardStar_OnNonExistentItem_ReplaceShouldFail()
    {
        // Wildcard "*" means "match any version" but the item must still exist for Replace
        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "wild-1", PartitionKey = "pk1", Name = "New" },
            "wild-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "*" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task IfMatchEtag_WildcardStar_OnDeletedItem_ReplaceShouldFail()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "wild-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        await _cosmos.Container.DeleteItemAsync<TestDocument>("wild-2", new PartitionKey("pk1"));

        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "wild-2", PartitionKey = "pk1", Name = "Updated" },
            "wild-2", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "*" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task IfMatchEtag_WildcardStar_OnDeletedItem_DeleteShouldFail()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "wild-3", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        await _cosmos.Container.DeleteItemAsync<TestDocument>("wild-3", new PartitionKey("pk1"));

        var act = () => _cosmos.Container.DeleteItemAsync<TestDocument>(
            "wild-3", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "*" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task IfMatchEtag_QuotedWildcard_ShouldNotBeWildcard()
    {
        // "\"*\"" is a literal etag value, not the wildcard * 
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "wild-4", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "wild-4", PartitionKey = "pk1", Name = "Updated" },
            "wild-4", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"*\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task IfMatchEtag_WildcardStar_UpsertNonExistentItem_ShouldCreateItem()
    {
        // Per REST API docs: IfMatch applies to PUT/DELETE only.
        // Upsert on a non-existent item uses POST semantics — IfMatchEtag (including wildcard) is ignored.
        var response = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "wild-5", PartitionKey = "pk1", Name = "New" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "*" });

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  3. Delete-then-Write Edge Cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DeleteThenReplace_WithOldETag_ShouldFail()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "del-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var originalEtag = create.ETag;

        await _cosmos.Container.DeleteItemAsync<TestDocument>("del-1", new PartitionKey("pk1"));

        // Replace with old ETag on deleted item should be NotFound (item doesn't exist)
        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "del-1", PartitionKey = "pk1", Name = "Resurrected" },
            "del-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = originalEtag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DeleteThenUpsert_WithOldETag_CreatesItem()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "del-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var originalEtag = create.ETag;

        await _cosmos.Container.DeleteItemAsync<TestDocument>("del-2", new PartitionKey("pk1"));

        // Per REST API docs: IfMatch applies to PUT/DELETE only. Upsert uses POST semantics on
        // the insert path, so IfMatchEtag is ignored when the item doesn't exist.
        var response = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "del-2", PartitionKey = "pk1", Name = "Resurrected" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = originalEtag });
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task DeleteThenRecreate_OldETag_ShouldNotMatchNewItem()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "del-3", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var originalEtag = create.ETag;

        await _cosmos.Container.DeleteItemAsync<TestDocument>("del-3", new PartitionKey("pk1"));

        // Recreate same item
        var recreate = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "del-3", PartitionKey = "pk1", Name = "Recreated" },
            new PartitionKey("pk1"));

        // Old ETag should not match new item's ETag
        recreate.ETag.Should().NotBe(originalEtag);

        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "del-3", PartitionKey = "pk1", Name = "Update with old etag" },
            "del-3", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = originalEtag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task DeleteWithMatchingETag_ShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "del-4", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var etag = create.ETag;

        // Delete with current ETag should succeed
        var response = await _cosmos.Container.DeleteItemAsync<TestDocument>(
            "del-4", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task DeleteWithStaleETag_ShouldFail()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "del-5", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var staleEtag = create.ETag;

        // Modify the item to change its ETag
        await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "del-5", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));

        var act = () => _cosmos.Container.DeleteItemAsync<TestDocument>(
            "del-5", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = staleEtag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  4. Cross-Partition Key Scoping
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ETags_AreScopedToPartitionKey_SameIdDifferentPK()
    {
        var create1 = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "xpk-1", PartitionKey = "pkA", Name = "Item A" },
            new PartitionKey("pkA"));

        var create2 = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "xpk-1", PartitionKey = "pkB", Name = "Item B" },
            new PartitionKey("pkB"));

        // ETags should be different even for same ID with different partition keys
        create1.ETag.Should().NotBe(create2.ETag);

        // Using ETag from pkA should NOT work for pkB
        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "xpk-1", PartitionKey = "pkB", Name = "Updated B" },
            "xpk-1", new PartitionKey("pkB"),
            new ItemRequestOptions { IfMatchEtag = create1.ETag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task ETags_AreScopedToPartitionKey_UpdateOneDoesNotAffectOther()
    {
        var createA = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "xpk-2", PartitionKey = "pkA", Name = "Item A" },
            new PartitionKey("pkA"));

        var createB = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "xpk-2", PartitionKey = "pkB", Name = "Item B" },
            new PartitionKey("pkB"));

        var etagB = createB.ETag;

        // Update item in pkA
        await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "xpk-2", PartitionKey = "pkA", Name = "Updated A" },
            "xpk-2", new PartitionKey("pkA"));

        // ETag for pkB should still be valid
        var replaceB = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "xpk-2", PartitionKey = "pkB", Name = "Updated B" },
            "xpk-2", new PartitionKey("pkB"),
            new ItemRequestOptions { IfMatchEtag = etagB });

        replaceB.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  5. Patch Operations with IfMatchEtag
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Patch_WithMatchingETag_ShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "patch-1", PartitionKey = "pk1", Name = "Original", Value = 1 },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.PatchItemAsync<TestDocument>(
            "patch-1", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Patched") },
            new PatchItemRequestOptions { IfMatchEtag = create.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Resource.Name.Should().Be("Patched");
    }

    [Fact]
    public async Task Patch_WithStaleETag_ShouldFail()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "patch-2", PartitionKey = "pk1", Name = "Original", Value = 1 },
            new PartitionKey("pk1"));
        var staleEtag = create.ETag;

        await _cosmos.Container.PatchItemAsync<TestDocument>(
            "patch-2", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Updated") });

        var act = () => _cosmos.Container.PatchItemAsync<TestDocument>(
            "patch-2", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Second Update") },
            new PatchItemRequestOptions { IfMatchEtag = staleEtag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Patch_WithWildcardETag_ShouldSucceed()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "patch-3", PartitionKey = "pk1", Name = "Original", Value = 1 },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.PatchItemAsync<TestDocument>(
            "patch-3", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Patched") },
            new PatchItemRequestOptions { IfMatchEtag = "*" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Patch_OnNonExistentItem_WithETag_ShouldReturnNotFound()
    {
        var act = () => _cosmos.Container.PatchItemAsync<TestDocument>(
            "patch-nope", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Patched") },
            new PatchItemRequestOptions { IfMatchEtag = "\"some-fake-etag\"" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Patch_ETagChangesAfterPatch()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "patch-4", PartitionKey = "pk1", Name = "Original", Value = 1 },
            new PartitionKey("pk1"));

        var patchResult = await _cosmos.Container.PatchItemAsync<TestDocument>(
            "patch-4", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Patched") });

        patchResult.ETag.Should().NotBe(create.ETag);

        // Old ETag should no longer work
        var act = () => _cosmos.Container.PatchItemAsync<TestDocument>(
            "patch-4", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Second") },
            new PatchItemRequestOptions { IfMatchEtag = create.ETag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  6. Stream vs Typed API Consistency
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StreamReplace_WithMatchingETag_ShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "stream-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(
            "{\"id\":\"stream-1\",\"partitionKey\":\"pk1\",\"name\":\"Stream Updated\"}"));
        var response = await _cosmos.Container.ReplaceItemStreamAsync(
            stream, "stream-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = create.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task StreamUpsert_WithMatchingETag_ShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "stream-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(
            "{\"id\":\"stream-2\",\"partitionKey\":\"pk1\",\"name\":\"Stream Updated\"}"));
        var response = await _cosmos.Container.UpsertItemStreamAsync(
            stream, new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = create.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task StreamDelete_WithMatchingETag_ShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "stream-3", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.DeleteItemStreamAsync(
            "stream-3", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = create.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task StreamDelete_WithStaleETag_ShouldReturn412()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "stream-4", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var staleEtag = create.ETag;

        await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "stream-4", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.DeleteItemStreamAsync(
            "stream-4", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = staleEtag });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task StreamUpsert_OnNonExistentItem_WithETag_CreatesItem()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(
            "{\"id\":\"stream-nope\",\"partitionKey\":\"pk1\",\"name\":\"New\"}"));
        var response = await _cosmos.Container.UpsertItemStreamAsync(
            stream, new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"fake-etag\"" });

        // Per REST API docs: IfMatch is ignored on the Upsert insert path (POST semantics).
        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task StreamReplace_OnDeletedItem_WithOldETag_ShouldReturnNotFound()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "stream-5", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var etag = create.ETag;

        await _cosmos.Container.DeleteItemAsync<TestDocument>("stream-5", new PartitionKey("pk1"));

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(
            "{\"id\":\"stream-5\",\"partitionKey\":\"pk1\",\"name\":\"Resurrected\"}"));
        var response = await _cosmos.Container.ReplaceItemStreamAsync(
            stream, "stream-5", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task StreamPatch_WithStaleETag_ShouldReturn412()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "stream-6", PartitionKey = "pk1", Name = "Original", Value = 1 },
            new PartitionKey("pk1"));
        var staleEtag = create.ETag;

        await _cosmos.Container.PatchItemAsync<TestDocument>(
            "stream-6", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Updated") });

        var response = await _cosmos.Container.PatchItemStreamAsync(
            "stream-6", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Should fail") },
            new PatchItemRequestOptions { IfMatchEtag = staleEtag });

        response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task StreamPatch_WithMatchingETag_ShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "stream-7", PartitionKey = "pk1", Name = "Original", Value = 1 },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.PatchItemStreamAsync(
            "stream-7", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Patched via stream") },
            new PatchItemRequestOptions { IfMatchEtag = create.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  7. Stream Wildcard Edge Cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StreamReplace_Wildcard_OnNonExistentItem_ShouldReturn404()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(
            "{\"id\":\"sw-1\",\"partitionKey\":\"pk1\",\"name\":\"New\"}"));
        var response = await _cosmos.Container.ReplaceItemStreamAsync(
            stream, "sw-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "*" });

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task StreamDelete_Wildcard_OnNonExistentItem_ShouldReturn404()
    {
        var response = await _cosmos.Container.DeleteItemStreamAsync(
            "sw-2", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "*" });

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task StreamUpsert_Wildcard_OnNonExistentItem_CreatesItem()
    {
        // Per REST API docs: IfMatch is ignored on the Upsert insert path (POST semantics, even wildcard).
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(
            "{\"id\":\"sw-3\",\"partitionKey\":\"pk1\",\"name\":\"New\"}"));
        var response = await _cosmos.Container.UpsertItemStreamAsync(
            stream, new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "*" });

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  8. Concurrency / Read-Write Interleaving
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrentReplaces_WithSameETag_OnlyOneShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "conc-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var etag = create.ETag;

        var tasks = Enumerable.Range(0, 10).Select(i =>
            _cosmos.Container.ReplaceItemAsync(
                new TestDocument { Id = "conc-1", PartitionKey = "pk1", Name = $"Writer-{i}" },
                "conc-1", new PartitionKey("pk1"),
                new ItemRequestOptions { IfMatchEtag = etag })
        ).Select(async t =>
        {
            try
            {
                var r = await t;
                return (Success: true, StatusCode: r.StatusCode);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                return (Success: false, StatusCode: ex.StatusCode);
            }
        }).ToList();

        var results = await Task.WhenAll(tasks);
        results.Count(r => r.Success).Should().Be(1, "exactly one concurrent writer should succeed");
        results.Count(r => !r.Success).Should().Be(9, "remaining writers should get 412");
    }

    [Fact]
    public async Task ConcurrentUpserts_WithSameETag_OnlyOneShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "conc-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var etag = create.ETag;

        var tasks = Enumerable.Range(0, 10).Select(i =>
            _cosmos.Container.UpsertItemAsync(
                new TestDocument { Id = "conc-2", PartitionKey = "pk1", Name = $"Writer-{i}" },
                new PartitionKey("pk1"),
                new ItemRequestOptions { IfMatchEtag = etag })
        ).Select(async t =>
        {
            try
            {
                var r = await t;
                return (Success: true, StatusCode: r.StatusCode);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                return (Success: false, StatusCode: ex.StatusCode);
            }
        }).ToList();

        var results = await Task.WhenAll(tasks);
        results.Count(r => r.Success).Should().Be(1, "exactly one concurrent writer should succeed");
    }

    [Fact]
    public async Task ReadAfterReplace_ShouldReturnNewETag()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "conc-3", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var replace = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "conc-3", PartitionKey = "pk1", Name = "Updated" },
            "conc-3", new PartitionKey("pk1"));

        var read = await _cosmos.Container.ReadItemAsync<TestDocument>("conc-3", new PartitionKey("pk1"));

        read.ETag.Should().Be(replace.ETag);
        read.ETag.Should().NotBe(create.ETag);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  9. IfNoneMatch Interactions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IfNoneMatch_WithCurrentETag_Read_ShouldReturn304()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "inm-1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _cosmos.Container.ReadItemAsync<TestDocument>(
            "inm-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = create.ETag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }

    [Fact]
    public async Task IfNoneMatch_WithStaleETag_Read_ShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "inm-2", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));
        var staleEtag = create.ETag;

        await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "inm-2", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));

        var read = await _cosmos.Container.ReadItemAsync<TestDocument>(
            "inm-2", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = staleEtag });

        read.StatusCode.Should().Be(HttpStatusCode.OK);
        read.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task IfNoneMatch_Wildcard_OnExistingItem_Read_ShouldReturn304()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "inm-3", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => _cosmos.Container.ReadItemAsync<TestDocument>(
            "inm-3", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = "*" });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }

    [Fact]
    public async Task IfNoneMatch_Wildcard_Patch_OnExistingItem_SucceedsThroughPipeline()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "inm-4", PartitionKey = "pk1", Name = "Test", Value = 1 },
            new PartitionKey("pk1"));

        // Through the SDK pipeline, the Patch handler only propagates IfMatch headers,
        // not IfNoneMatch. So IfNoneMatchEtag is effectively ignored for Patch operations.
        var response = await _cosmos.Container.PatchItemAsync<TestDocument>(
            "inm-4", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Patched") },
            new PatchItemRequestOptions { IfNoneMatchEtag = "*" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task IfNoneMatch_StreamRead_WithCurrentETag_ShouldReturn304()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "inm-5", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.ReadItemStreamAsync(
            "inm-5", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = create.ETag });

        response.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }

    [Fact]
    public async Task IfNoneMatch_StreamRead_WithStaleETag_ShouldReturn200()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "inm-6", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));
        var staleEtag = create.ETag;

        await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "inm-6", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.ReadItemStreamAsync(
            "inm-6", new PartitionKey("pk1"),
            new ItemRequestOptions { IfNoneMatchEtag = staleEtag });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  10. Batch / TransactionalBatch Edge Cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Batch_MixedOperations_OneStaleETag_ShouldRollbackAll()
    {
        // Create two items
        var create1 = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "batch-1", PartitionKey = "bpk", Name = "Item 1" },
            new PartitionKey("bpk"));
        var create2 = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "batch-2", PartitionKey = "bpk", Name = "Item 2" },
            new PartitionKey("bpk"));

        // Update item 2 to invalidate its ETag
        await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "batch-2", PartitionKey = "bpk", Name = "Item 2 Updated" },
            new PartitionKey("bpk"));

        // Batch: valid replace on item 1, stale replace on item 2
        var batch = _cosmos.Container.CreateTransactionalBatch(new PartitionKey("bpk"));
        batch.ReplaceItem("batch-1",
            new TestDocument { Id = "batch-1", PartitionKey = "bpk", Name = "Batch Replace 1" },
            new TransactionalBatchItemRequestOptions { IfMatchEtag = create1.ETag });
        batch.ReplaceItem("batch-2",
            new TestDocument { Id = "batch-2", PartitionKey = "bpk", Name = "Batch Replace 2" },
            new TransactionalBatchItemRequestOptions { IfMatchEtag = create2.ETag }); // stale

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();

        // Item 1 should NOT have been updated (rollback)
        var read1 = await _cosmos.Container.ReadItemAsync<TestDocument>("batch-1", new PartitionKey("bpk"));
        read1.Resource.Name.Should().Be("Item 1", "batch should have rolled back item 1 changes");
    }

    [Fact]
    public async Task Batch_Delete_WithStaleETag_ShouldRollback()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "batch-3", PartitionKey = "bpk", Name = "Original" },
            new PartitionKey("bpk"));
        var staleEtag = create.ETag;

        await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "batch-3", PartitionKey = "bpk", Name = "Updated" },
            new PartitionKey("bpk"));

        var batch = _cosmos.Container.CreateTransactionalBatch(new PartitionKey("bpk"));
        batch.DeleteItem("batch-3",
            new TransactionalBatchItemRequestOptions { IfMatchEtag = staleEtag });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();

        // Item should still exist
        var read = await _cosmos.Container.ReadItemAsync<TestDocument>("batch-3", new PartitionKey("bpk"));
        read.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task Batch_CreateThenReplaceWithETag_ShouldWork()
    {
        // Batch that creates an item and then tries to use the etag from the create
        // This is tricky because the create ETag isn't known until execution
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "batch-4", PartitionKey = "bpk", Name = "Existing" },
            new PartitionKey("bpk"));

        var batch = _cosmos.Container.CreateTransactionalBatch(new PartitionKey("bpk"));
        batch.ReplaceItem("batch-4",
            new TestDocument { Id = "batch-4", PartitionKey = "bpk", Name = "Batch Updated" },
            new TransactionalBatchItemRequestOptions { IfMatchEtag = create.ETag });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    [Fact]
    public async Task Batch_WildcardETag_Replace_ShouldSucceed()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "batch-5", PartitionKey = "bpk", Name = "Original" },
            new PartitionKey("bpk"));

        var batch = _cosmos.Container.CreateTransactionalBatch(new PartitionKey("bpk"));
        batch.ReplaceItem("batch-5",
            new TestDocument { Id = "batch-5", PartitionKey = "bpk", Name = "Wildcard Updated" },
            new TransactionalBatchItemRequestOptions { IfMatchEtag = "*" });

        using var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeTrue();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  11. Import/Export State Edge Cases
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ImportState_RegeneratesETags_OldETagsShouldNotWork()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "import-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var originalEtag = create.ETag;

        // Export and reimport
        var state = _cosmos.ExportState();
        _cosmos.ImportState(state);

        // Old ETag should not work after reimport
        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "import-1", PartitionKey = "pk1", Name = "Updated" },
            "import-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = originalEtag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task ImportState_NewETagsShouldWork()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "import-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var state = _cosmos.ExportState();
        _cosmos.ImportState(state);

        // Read to get new ETag
        var read = await _cosmos.Container.ReadItemAsync<TestDocument>("import-2", new PartitionKey("pk1"));
        var newEtag = read.ETag;
        newEtag.Should().NotBeNullOrEmpty();

        // New ETag should work
        var replace = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "import-2", PartitionKey = "pk1", Name = "Updated after import" },
            "import-2", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = newEtag });

        replace.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ImportState_EmptyImport_ShouldClearEverything()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "import-3", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        _cosmos.ImportState("{\"items\":[]}");

        var act = () => _cosmos.Container.ReadItemAsync<TestDocument>("import-3", new PartitionKey("pk1"));
        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  12. ETag in Response Consistency
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ETag_ReturnedFromCreate_MatchesSubsequentRead()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "resp-1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var read = await _cosmos.Container.ReadItemAsync<TestDocument>("resp-1", new PartitionKey("pk1"));

        read.ETag.Should().Be(create.ETag);
    }

    [Fact]
    public async Task ETag_ReturnedFromUpsert_MatchesSubsequentRead()
    {
        var upsert = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "resp-2", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var read = await _cosmos.Container.ReadItemAsync<TestDocument>("resp-2", new PartitionKey("pk1"));

        read.ETag.Should().Be(upsert.ETag);
    }

    [Fact]
    public async Task ETag_ReturnedFromReplace_MatchesSubsequentRead()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "resp-3", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var replace = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "resp-3", PartitionKey = "pk1", Name = "Replaced" },
            "resp-3", new PartitionKey("pk1"));

        var read = await _cosmos.Container.ReadItemAsync<TestDocument>("resp-3", new PartitionKey("pk1"));

        read.ETag.Should().Be(replace.ETag);
    }

    [Fact]
    public async Task ETag_ReturnedFromPatch_MatchesSubsequentRead()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "resp-4", PartitionKey = "pk1", Name = "Original", Value = 1 },
            new PartitionKey("pk1"));

        var patch = await _cosmos.Container.PatchItemAsync<TestDocument>(
            "resp-4", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Patched") });

        var read = await _cosmos.Container.ReadItemAsync<TestDocument>("resp-4", new PartitionKey("pk1"));

        read.ETag.Should().Be(patch.ETag);
    }

    [Fact]
    public async Task ETag_StreamRead_MatchesTypedRead()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "resp-5", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var typedRead = await _cosmos.Container.ReadItemAsync<TestDocument>("resp-5", new PartitionKey("pk1"));
        var streamRead = await _cosmos.Container.ReadItemStreamAsync("resp-5", new PartitionKey("pk1"));

        streamRead.Headers.ETag.Should().Be(typedRead.ETag);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  13. ETag Uniqueness / Monotonicity
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ETags_AreUniqueAcrossMultipleOperations()
    {
        var etags = new HashSet<string>();

        for (int i = 0; i < 50; i++)
        {
            var resp = await _cosmos.Container.UpsertItemAsync(
                new TestDocument { Id = "unique-1", PartitionKey = "pk1", Name = $"Version-{i}" },
                new PartitionKey("pk1"));
            etags.Add(resp.ETag).Should().BeTrue($"ETag should be unique at iteration {i}");
        }
    }

    [Fact]
    public async Task ETags_AreUniqueAcrossMultipleItems()
    {
        var etags = new HashSet<string>();

        for (int i = 0; i < 50; i++)
        {
            var resp = await _cosmos.Container.CreateItemAsync(
                new TestDocument { Id = $"unique-item-{i}", PartitionKey = "pk1", Name = $"Item-{i}" },
                new PartitionKey("pk1"));
            etags.Add(resp.ETag).Should().BeTrue($"ETag should be unique for item {i}");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  14. Double-Delete and Repeat Operations
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DoubleDelete_WithSameETag_SecondShouldFail()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "dd-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var etag = create.ETag;

        // First delete succeeds
        await _cosmos.Container.DeleteItemAsync<TestDocument>(
            "dd-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        // Second delete with same ETag should fail (item gone)
        var act = () => _cosmos.Container.DeleteItemAsync<TestDocument>(
            "dd-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task DoubleReplace_WithFirstETag_SecondShouldFail()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "dd-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var etag = create.ETag;

        // First replace succeeds and changes ETag
        var replace1 = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "dd-2", PartitionKey = "pk1", Name = "First Replace" },
            "dd-2", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        replace1.ETag.Should().NotBe(etag);

        // Second replace with original ETag should fail
        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "dd-2", PartitionKey = "pk1", Name = "Second Replace" },
            "dd-2", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = etag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  15. Upsert Semantics: Create-via-Upsert with IfMatchEtag
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Upsert_AsCreate_WithIfMatchEtag_CreatesItem()
    {
        // Upsert that would create a new item, but IfMatchEtag is set
        // Per REST API docs: IfMatch is ignored on the Upsert insert path (POST semantics).
        // When the item doesn't exist, IfMatchEtag is not evaluated - item is created.
        var response = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "upsert-new-1", PartitionKey = "pk1", Name = "Brand New" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"definitely-not-a-real-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Upsert_AsUpdate_WithMatchingEtag_ShouldSucceed()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "upsert-upd-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var upsert = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "upsert-upd-1", PartitionKey = "pk1", Name = "Upserted" },
            new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = create.ETag });

        upsert.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Upsert_WithNoEtag_ShouldAlwaysSucceedAsCreate()
    {
        var upsert = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "upsert-no-etag", PartitionKey = "pk1", Name = "Created via Upsert" },
            new PartitionKey("pk1"));

        upsert.StatusCode.Should().Be(HttpStatusCode.Created);
        upsert.ETag.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Upsert_WithNoEtag_ShouldAlwaysSucceedAsUpdate()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "upsert-no-etag-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var upsert = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "upsert-no-etag-2", PartitionKey = "pk1", Name = "Updated" },
            new PartitionKey("pk1"));

        upsert.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  16. Rapid Read-Modify-Write Cycle (Optimistic Concurrency Pattern)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task OptimisticConcurrency_ReadModifyWrite_ShouldWork()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "occ-1", PartitionKey = "pk1", Name = "v0", Value = 0 },
            new PartitionKey("pk1"));

        // Simulate optimistic concurrency loop
        for (int i = 0; i < 10; i++)
        {
            var read = await _cosmos.Container.ReadItemAsync<TestDocument>("occ-1", new PartitionKey("pk1"));
            var item = read.Resource;
            item.Value++;
            item.Name = $"v{item.Value}";

            var response = await _cosmos.Container.ReplaceItemAsync(item, "occ-1", new PartitionKey("pk1"),
                new ItemRequestOptions { IfMatchEtag = read.ETag });
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }

        var final = await _cosmos.Container.ReadItemAsync<TestDocument>("occ-1", new PartitionKey("pk1"));
        final.Resource.Value.Should().Be(10);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  17. ETag in System Properties (_etag field in document)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ETag_InDocument_MatchesResponseHeader()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "sysprop-1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var streamRead = await _cosmos.Container.ReadItemStreamAsync("sysprop-1", new PartitionKey("pk1"));
        using var reader = new StreamReader(streamRead.Content);
        var json = await reader.ReadToEndAsync();
        var doc = JObject.Parse(json);

        var documentEtag = doc["_etag"]?.ToString();
        documentEtag.Should().NotBeNull();
        // The response ETag should match the _etag in the document body
        create.ETag.Should().Be(documentEtag);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  18. Null RequestOptions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Replace_WithNullRequestOptions_ShouldSucceed()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "null-opts-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "null-opts-1", PartitionKey = "pk1", Name = "Updated" },
            "null-opts-1", new PartitionKey("pk1"),
            requestOptions: null);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Upsert_WithNullRequestOptions_ShouldSucceed()
    {
        var response = await _cosmos.Container.UpsertItemAsync(
            new TestDocument { Id = "null-opts-2", PartitionKey = "pk1", Name = "Created" },
            new PartitionKey("pk1"),
            requestOptions: null);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Delete_WithNullRequestOptions_ShouldSucceed()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "null-opts-3", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.DeleteItemAsync<TestDocument>(
            "null-opts-3", new PartitionKey("pk1"),
            requestOptions: null);

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  19. IfMatchEtag with RequestOptions where IfMatchEtag is null
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Replace_WithRequestOptions_ButNullIfMatchEtag_ShouldSucceed()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "nulletag-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "nulletag-1", PartitionKey = "pk1", Name = "Updated" },
            "nulletag-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = null });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  20. Mixed IfMatch + IfNoneMatch (unusual but possible)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Patch_WithBothIfMatchAndIfNoneMatch_MatchingETag_SucceedsThroughPipeline()
    {
        // Through the SDK pipeline, the Patch handler only propagates IfMatch headers,
        // not IfNoneMatch. So IfNoneMatchEtag is effectively ignored for Patch operations.
        // Since IfMatch matches the current ETag, the operation succeeds.
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "both-1", PartitionKey = "pk1", Name = "Test", Value = 1 },
            new PartitionKey("pk1"));

        var response = await _cosmos.Container.PatchItemAsync<TestDocument>(
            "both-1", new PartitionKey("pk1"),
            new[] { PatchOperation.Replace("/name", "Patched") },
            new PatchItemRequestOptions
            {
                IfMatchEtag = create.ETag,
                IfNoneMatchEtag = create.ETag
            });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  21. Replace Item that was replaced by another operation - chain of ETags
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ETag_Chain_ReplaceReplace_ShouldTrackCorrectly()
    {
        var v1 = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "chain-1", PartitionKey = "pk1", Name = "v1" },
            new PartitionKey("pk1"));

        var v2 = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "chain-1", PartitionKey = "pk1", Name = "v2" },
            "chain-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = v1.ETag });

        var v3 = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "chain-1", PartitionKey = "pk1", Name = "v3" },
            "chain-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = v2.ETag });

        v3.StatusCode.Should().Be(HttpStatusCode.OK);

        // v1 ETag should not work anymore
        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "chain-1", PartitionKey = "pk1", Name = "v4 with v1 etag" },
            "chain-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = v1.ETag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

        // v2 ETag should not work anymore either
        var act2 = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "chain-1", PartitionKey = "pk1", Name = "v4 with v2 etag" },
            "chain-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = v2.ETag });

        var ex2 = await act2.Should().ThrowAsync<CosmosException>();
        ex2.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);

        // v3 ETag should work
        var v4 = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "chain-1", PartitionKey = "pk1", Name = "v4" },
            "chain-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = v3.ETag });

        v4.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  22. ETag after Restore to Point in Time
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task RestoreToPointInTime_RegeneratesETags_OldETagsShouldNotWork()
    {
        var create = await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "pitr-1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var originalEtag = create.ETag;

        // Wait briefly so timestamp differs
        await Task.Delay(10);

        // Restore to the current point in time
        _cosmos.RestoreToPointInTime(DateTimeOffset.UtcNow);

        // Old ETag should NOT work after PITR (etags are regenerated)
        var act = () => _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "pitr-1", PartitionKey = "pk1", Name = "After PITR" },
            "pitr-1", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = originalEtag });

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task RestoreToPointInTime_NewETags_ShouldWork()
    {
        await _cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "pitr-2", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        await Task.Delay(10);
        _cosmos.RestoreToPointInTime(DateTimeOffset.UtcNow);

        // Read to get new ETag
        var read = await _cosmos.Container.ReadItemAsync<TestDocument>("pitr-2", new PartitionKey("pk1"));

        // New ETag should work
        var replace = await _cosmos.Container.ReplaceItemAsync(
            new TestDocument { Id = "pitr-2", PartitionKey = "pk1", Name = "After PITR" },
            "pitr-2", new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = read.ETag });

        replace.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
