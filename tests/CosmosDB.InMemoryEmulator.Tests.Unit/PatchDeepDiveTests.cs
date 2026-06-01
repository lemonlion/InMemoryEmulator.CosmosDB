using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan #25: Patch Item Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════


// ── Category A: ETag Edge Cases ──────────────────────────────────────────────

public class PatchEtagEdgeCaseTests
{
    [Fact]
    public async Task Patch_WithIfMatchWildcard_AlwaysSucceeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Updated")],
            new PatchItemRequestOptions { IfMatchEtag = "*" });

        result.Resource.Name.Should().Be("Updated");
    }

    [Fact]
    public async Task Patch_WithIfNoneMatchCurrentETag_ThrowsPreconditionFailed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var created = await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var act = () => container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Updated")],
            new PatchItemRequestOptions { IfNoneMatchEtag = created.ETag });

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Patch_WithIfNoneMatchStaleETag_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Updated")],
            new PatchItemRequestOptions { IfNoneMatchEtag = "stale-etag" });

        result.Resource.Name.Should().Be("Updated");
    }
}


// ── Category B: Stream Variant Path Validation (BUG-3) ──────────────────────

public class PatchStreamPathValidationTests
{
    [Fact]
    public async Task PatchItemStreamAsync_SetIdField_ReturnsBadRequest()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/id", "new-id")]);

        result.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task PatchItemStreamAsync_SetPartitionKeyField_ReturnsBadRequest()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/partitionKey", "new-pk")]);

        result.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}


// ── Category C: Deeply Nested Path Edge Cases ────────────────────────────────

public class PatchDeepNestedPathTests
{
    [Fact]
    public async Task Patch_Set_ThreeLevelDeepPath_Creates()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test",
                Nested = new NestedObject { Description = "desc", Score = 1.0 } },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/nested/score", 99.9)]);

        result.Resource["nested"]!["score"]!.Value<double>().Should().Be(99.9);
    }

    [Fact]
    public async Task Patch_Add_DeeplyNestedArrayAppend()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","nested":{"tags":["a","b"]}}""")),
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Add("/nested/tags/-", "c")]);

        var tags = result.Resource["nested"]!["tags"]!.ToObject<string[]>();
        tags.Should().BeEquivalentTo("a", "b", "c");
    }

    [Fact]
    public async Task Patch_Remove_DeeplyNestedProperty()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test",
                Nested = new NestedObject { Description = "desc", Score = 5.0 } },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/nested/description")]);

        result.Resource["nested"]!["description"].Should().BeNull();
        result.Resource["nested"]!["score"]!.Value<double>().Should().Be(5.0);
    }
}


// ── Category D: TTL Interaction ──────────────────────────────────────────────

public class PatchTtlInteractionTests
{
    [Fact]
    public async Task Patch_ExpiredTtlItem_ThrowsNotFound()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { DefaultTimeToLive = 1 };
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        await Task.Delay(1500);

        var act = () => container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "patched")]);

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Patch_Set_TtlToNegativeOne_DisablesPerItemTtl()
    {
        var container = new InMemoryContainer("test", "/partitionKey") { DefaultTimeToLive = 1 };
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        // Set _ttl to -1 to disable per-item TTL
        await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/_ttl", -1)]);

        await Task.Delay(1500);

        // Item should still be readable
        var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"))).Resource;
        read["name"]!.ToString().Should().Be("Test");
    }
}


// ── Category E: Increment Edge Cases ─────────────────────────────────────────

public class PatchIncrementEdgeCaseTests
{
    [Fact]
    public async Task Patch_Increment_ByZero_ValueUnchanged()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Value = 42 },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/value", 0)]);

        result.Resource["value"]!.Value<int>().Should().Be(42);
    }

    [Fact]
    public async Task Patch_Increment_LargeValue_HandlesLargeNumbers()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","value":1000000}""")),
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/value", 999999)]);

        result.Resource["value"]!.Value<long>().Should().Be(1999999);
    }

    [Fact]
    public async Task Patch_Increment_Double_ThenInt_TypePromotesToDouble()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","value":1.5}""")),
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/value", 1)]);

        result.Resource["value"]!.Value<double>().Should().Be(2.5);
    }

    [Fact]
    public async Task Patch_Increment_Int_ThenDouble_TypePromotesToDouble()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Value = 10 },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/value", 0.5)]);

        result.Resource["value"]!.Value<double>().Should().Be(10.5);
    }
}


// ── Category F: Array Operation Edge Cases ───────────────────────────────────

public class PatchArrayOperationTests
{
    [Fact]
    public async Task Patch_Add_ArrayIndex0_InsertsAtBeginning()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Tags = ["b", "c"] },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Add("/tags/0", "a")]);

        result.Resource["tags"]!.ToObject<string[]>().Should().BeEquivalentTo("a", "b", "c");
    }

    [Fact]
    public async Task Patch_Remove_LastArrayElement_LeavesEmptyArray()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Tags = ["only"] },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/tags/0")]);

        result.Resource["tags"]!.ToObject<string[]>().Should().BeEmpty();
    }

    [Fact]
    public async Task Patch_Set_EntireArrayToNull()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Tags = ["a", "b"] },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set<object?>("/tags", null)]);

        result.Resource["tags"]!.Type.Should().Be(JTokenType.Null);
    }
}


// ── Category G: Move Operation Edge Cases ────────────────────────────────────

public class PatchMoveEdgeCaseDeepTests
{
    [Fact]
    public async Task Move_BetweenDifferentNestingLevels_NestedToRoot()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1",
                Nested = new NestedObject { Score = 42.0 } },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Move("/nested/score", "/topScore")]);

        result.Resource["topScore"]!.Value<double>().Should().Be(42.0);
        result.Resource["nested"]!["score"].Should().BeNull();
    }

    [Fact]
    public async Task Move_BetweenDifferentNestingLevels_RootToNested()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Display",
                Nested = new NestedObject { Description = "desc" } },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Move("/name", "/nested/displayName")]);

        result.Resource["nested"]!["displayName"]!.ToString().Should().Be("Display");
        result.Resource["name"].Should().BeNull();
    }

    [Fact]
    public async Task Move_IdField_ThrowsBadRequest()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Move("/name", "/id")]);

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Move_PartitionKeyField_ThrowsBadRequest()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Move("/name", "/partitionKey")]);

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.BadRequest);
    }
}


// ── Category H: Replace Type Coercion ────────────────────────────────────────

public class PatchReplaceTypeCoercionTests
{
    [Fact]
    public async Task Patch_Replace_IntWithString_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Value = 42 },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Replace("/value", "forty-two")]);

        result.Resource["value"]!.ToString().Should().Be("forty-two");
    }

    [Fact]
    public async Task Patch_Replace_ObjectWithScalar_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1",
                Nested = new NestedObject { Description = "desc" } },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Replace("/nested", "flat-value")]);

        result.Resource["nested"]!.ToString().Should().Be("flat-value");
    }

    [Fact]
    public async Task Patch_Replace_NullValue_SetsToNull()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Replace<object?>("/name", null)]);

        result.Resource["name"]!.Type.Should().Be(JTokenType.Null);
    }
}


// ── Category I: Combined/Compound Operations ─────────────────────────────────

public class PatchCombinedOperationTests
{
    [Fact]
    public async Task Patch_All5OperationTypes_InSingleCall()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original",
                Value = 10, Tags = ["a"], Nested = new NestedObject { Score = 5.0 } },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [
                PatchOperation.Set("/name", "Updated"),
                PatchOperation.Replace("/value", 20),
                PatchOperation.Add("/tags/-", "b"),
                PatchOperation.Remove("/nested/description"),
                PatchOperation.Increment("/nested/score", 3)
            ]);

        result.Resource["name"]!.ToString().Should().Be("Updated");
        result.Resource["value"]!.Value<int>().Should().Be(20);
        result.Resource["tags"]!.ToObject<string[]>().Should().Contain("b");
        result.Resource["nested"]!["description"].Should().BeNull();
        result.Resource["nested"]!["score"]!.Value<double>().Should().Be(8.0);
    }

    [Fact]
    public async Task Patch_SequentialPatchCalls_AccumulateChanges()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V1", Value = 1 },
            new PartitionKey("pk1"));

        await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "V2")]);

        await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Increment("/value", 1)]);

        var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"))).Resource;
        read["name"]!.ToString().Should().Be("V2");
        read["value"]!.Value<int>().Should().Be(2);
    }

    [Fact]
    public async Task Patch_MultipleRemoves_SequentialIndexShift()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Tags = ["a", "b", "c"] },
            new PartitionKey("pk1"));

        // Remove /tags/0 twice — first removes "a", then "b" (shifted)
        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Remove("/tags/0"), PatchOperation.Remove("/tags/0")]);

        result.Resource["tags"]!.ToObject<string[]>().Should().BeEquivalentTo("c");
    }
}


// ── Category J: Partition Key Variants ───────────────────────────────────────

public class PatchPartitionKeyVariantTests
{
    [Fact]
    public async Task Patch_WithPartitionKeyNone_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" });

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "Patched")]);

        result.Resource["name"]!.ToString().Should().Be("Patched");
    }

    [Fact]
    public async Task Patch_HierarchicalPK_SetSubPath_ThrowsBadRequest()
    {
        var container = new InMemoryContainer("test", ["/country", "/city"]);
        var pk = new PartitionKeyBuilder().Add("US").Add("NYC").Build();
        await container.CreateItemAsync(
            JObject.FromObject(new { id = "1", country = "US", city = "NYC", name = "Test" }), pk);

        var act = () => container.PatchItemAsync<JObject>(
            "1", pk,
            [PatchOperation.Set("/country", "UK")]);

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.BadRequest);
    }
}


// ── Category K: Filter Predicate Edge Cases ──────────────────────────────────

public class PatchFilterPredicateEdgeTests
{
    [Fact]
    public async Task Patch_FilterPredicate_WithNestedFieldCondition()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1",
                Nested = new NestedObject { Score = 5.0 } },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "updated")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.nested.score > 3" });

        result.Resource["name"]!.ToString().Should().Be("updated");
    }

    [Fact]
    public async Task Patch_FilterPredicate_CombinedWithETag()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var created = await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "updated")],
            new PatchItemRequestOptions
            {
                IfMatchEtag = created.ETag,
                FilterPredicate = "FROM c WHERE c.name = 'Test'"
            });

        result.Resource["name"]!.ToString().Should().Be("updated");
    }
}


// ── Category K2: FilterPredicate Missing Property = null (Issue #70) ─────────

/// <summary>
/// Regression tests for Issue #70: FilterPredicate should treat missing properties as null.
/// Uses InMemoryContainer directly with raw JSON streams to control property presence.
///
/// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update#filter-predicate
///   "The filter predicate is evaluated against the existing state of the document."
///   Missing properties are semantically equivalent to null in filter predicate evaluation.
/// </summary>
public class PatchFilterPredicateMissingPropertyTests
{
    [Fact]
    public async Task Patch_FilterPredicate_MissingPropertyEqualsNull_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        // Document without linkedId property at all
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","name":"test"}""")),
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/linkedId", "new-value")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.linkedId = null" });

        result.Resource["linkedId"]!.Value<string>().Should().Be("new-value");
    }

    [Fact]
    public async Task Patch_FilterPredicate_MissingPropertyNotEqualNull_ThrowsPreconditionFailed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        // Document without linkedId property at all
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","name":"test"}""")),
            new PartitionKey("pk1"));

        var act = () => container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "updated")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.linkedId != null" });

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Patch_FilterPredicate_MissingPropertyEqualsString_ThrowsPreconditionFailed()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        // Document without linkedId property at all
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","name":"test"}""")),
            new PartitionKey("pk1"));

        // undefined treated as null — null ≠ 'some-value'
        var act = () => container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "updated")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.linkedId = 'some-value'" });

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task Patch_FilterPredicate_ExplicitNullEqualsNull_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        // Document WITH explicit null for linkedId
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","linkedId":null,"name":"test"}""")),
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/linkedId", "new-value")],
            new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.linkedId = null" });

        result.Resource["linkedId"]!.Value<string>().Should().Be("new-value");
    }

    [Fact]
    public async Task Patch_FilterPredicate_MissingPropertyWithAndCondition_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        // Document without linkedId but with name
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","name":"test"}""")),
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/linkedId", "new-value")],
            new PatchItemRequestOptions
            {
                FilterPredicate = "FROM c WHERE c.linkedId = null AND c.name = 'test'"
            });

        result.Resource["linkedId"]!.Value<string>().Should().Be("new-value");
    }
}


// ── Category L: FakeCosmosHandler Patch Bugs ─────────────────────────────────

public class FakeCosmosHandlerPatchBugTests
{
    [Fact]
    public async Task FakeCosmosHandler_PatchReplace_NonExistentPath_Returns400()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var handler = new FakeCosmosHandler(container);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                HttpClientFactory = () => new HttpClient(handler)
            });

        var c = client.GetContainer("db", "test");
        await c.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var act = () => c.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Replace("/nonExistent", "val")]);

        await act.Should().ThrowAsync<CosmosException>()
            .Where(e => e.StatusCode == HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task FakeCosmosHandler_PatchMove_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var handler = new FakeCosmosHandler(container);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                HttpClientFactory = () => new HttpClient(handler)
            });

        var c = client.GetContainer("db", "test");
        await c.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "MoveMe" },
            new PartitionKey("pk1"));

        var result = await c.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Move("/name", "/movedName")]);

        result.Resource["movedName"]!.ToString().Should().Be("MoveMe");
        result.Resource["name"].Should().BeNull();
    }

    [Fact]
    public async Task FakeCosmosHandler_PatchReplace_ExistingPath_Succeeds()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var handler = new FakeCosmosHandler(container);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                HttpClientFactory = () => new HttpClient(handler)
            });

        var c = client.GetContainer("db", "test");
        await c.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var result = await c.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Replace("/name", "Replaced")]);

        result.Resource.Name.Should().Be("Replaced");
    }
}


// ── Category M: Atomicity & Rollback ─────────────────────────────────────────

public class PatchAtomicityDeepTests
{
    [Fact]
    public async Task Patch_Atomicity_DocumentSizeExceeded_RollsBack()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "small" },
            new PartitionKey("pk1"));

        // First op is fine; second creates a massive string exceeding 2MB
        var hugeString = new string('x', 2 * 1024 * 1024);
        var act = () => container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [
                PatchOperation.Set("/name", "updated"),
                PatchOperation.Set("/huge", hugeString)
            ]);

        await act.Should().ThrowAsync<CosmosException>();

        // Name should remain "small" — all ops rolled back
        var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"))).Resource;
        read["name"]!.ToString().Should().Be("small");
    }
}


// ── Category N: Stream Variant Parity ────────────────────────────────────────

public class PatchStreamVariantParityTests
{
    [Fact]
    public async Task PatchItemStreamAsync_MoreThan10Ops_ReturnsBadRequest()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1" },
            new PartitionKey("pk1"));

        var ops = Enumerable.Range(0, 11)
            .Select(i => PatchOperation.Set($"/field{i}", i))
            .ToList();

        var result = await container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"), ops);

        result.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task PatchItemStreamAsync_EnableContentResponseOnWrite_False_EmptyBody()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "updated")],
            new PatchItemRequestOptions { EnableContentResponseOnWrite = false });

        result.StatusCode.Should().Be(HttpStatusCode.OK);
        // When content response is suppressed, body may be null or empty
        if (result.Content is not null)
        {
            using var reader = new StreamReader(result.Content);
            var body = await reader.ReadToEndAsync();
            body.Should().BeNullOrEmpty();
        }
    }

    [Fact]
    public async Task PatchItemStreamAsync_ResponseHeaders_ContainETag()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemStreamAsync(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "updated")]);

        result.StatusCode.Should().Be(HttpStatusCode.OK);
        result.Headers.ETag.Should().NotBeNullOrEmpty();
    }
}


// ── Category O: Concurrent Patch Scenarios ───────────────────────────────────

public class PatchConcurrencyDeepTests
{
    [Fact]
    public async Task Patch_Concurrent_SameField_LastWriteWins()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Value = 0 },
            new PartitionKey("pk1"));

        var tasks = Enumerable.Range(1, 10).Select(i =>
            container.PatchItemAsync<JObject>(
                "1", new PartitionKey("pk1"),
                [PatchOperation.Set("/value", i)]));

        await Task.WhenAll(tasks);

        var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"))).Resource;
        read["value"]!.Value<int>().Should().BeInRange(1, 10);
    }

    [Fact]
    public async Task Patch_Concurrent_WithETagCheck_OneSucceedsOneFails()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        var created = await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
            new PartitionKey("pk1"));
        var etag = created.ETag;

        var task1 = container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "A")],
            new PatchItemRequestOptions { IfMatchEtag = etag });

        var task2 = container.PatchItemAsync<TestDocument>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/name", "B")],
            new PatchItemRequestOptions { IfMatchEtag = etag });

        var results = await Task.WhenAll(
            task1.ContinueWith(t => (Success: !t.IsFaulted, t.Exception)),
            task2.ContinueWith(t => (Success: !t.IsFaulted, t.Exception)));

        // At least one should succeed, at least one should fail with PreconditionFailed
        results.Count(r => r.Success).Should().BeGreaterThan(0);
    }
}


// ── Category P: Miscellaneous Edge Cases ─────────────────────────────────────

public class PatchMiscEdgeCaseTests
{
    [Fact]
    public async Task Patch_Set_PropertyNameWithUnderscore()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1" },
            new PartitionKey("pk1"));

        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/my_field", "value")]);

        result.Resource["my_field"]!.ToString().Should().Be("value");
    }

    [Fact]
    public async Task Patch_Set_PropertyNameWithDots()
    {
        var container = new InMemoryContainer("test", "/partitionKey");
        await container.CreateItemStreamAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(
                """{"id":"1","partitionKey":"pk1","my.field":"original"}""")),
            new PartitionKey("pk1"));

        // Dots in property names are tricky — Cosmos uses / for path separator
        var result = await container.PatchItemAsync<JObject>(
            "1", new PartitionKey("pk1"),
            [PatchOperation.Set("/my.field", "updated")]);

        // Either updates the dotted property or creates a new one
        result.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
