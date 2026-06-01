using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Regression tests for GitHub Issue #70 — FilterPredicate does not treat missing
/// properties as null when NullValueHandling.Ignore is used.
///
/// When a document is serialized with NullValueHandling.Ignore, null properties are
/// omitted entirely. Real Cosmos DB treats these missing properties as null during
/// FilterPredicate evaluation, so "FROM c WHERE c.prop = null" should match.
///
/// Tagged InMemoryOnly because the Windows Cosmos DB Emulator (v2.14.0) does not
/// support FilterPredicate syntax — it returns 400 BadRequest (tracked: #53).
///
/// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update#filter-predicate
///   "The filter predicate is evaluated against the existing state of the document."
///   Missing properties are semantically equivalent to null in filter predicate evaluation.
/// </summary>
public class Issue70FilterPredicateMissingPropertyTests : IAsyncLifetime
{
    private InMemoryCosmosResult _cosmos = null!;
    private Container _container = null!;

    public ValueTask InitializeAsync()
    {
        _cosmos = InMemoryCosmos.Create("issue70", "/partitionKey",
            configureOptions: opts => opts.Serializer = new CosmosJsonDotNetSerializer(new JsonSerializerSettings
            {
                ContractResolver = new DefaultContractResolver
                {
                    NamingStrategy = new CamelCaseNamingStrategy()
                },
                NullValueHandling = NullValueHandling.Ignore
            }));
        _container = _cosmos.Container;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _cosmos.Dispose();
        return ValueTask.CompletedTask;
    }

    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task PatchWithFilterPredicate_MissingPropertyEqualsNull_Succeeds()
    {
        // linkedId is null → will NOT be serialized due to NullValueHandling.Ignore
        var document = new
        {
            id = Guid.NewGuid().ToString(),
            partitionKey = "pk-1",
            linkedId = (string?)null,
            name = "test"
        };
        await _container.CreateItemAsync(document, new PartitionKey("pk-1"));

        // This FilterPredicate should match because the missing property should be treated as null
        var patchOperations = new[] { PatchOperation.Set("/linkedId", Guid.NewGuid().ToString()) };
        var options = new PatchItemRequestOptions
        {
            FilterPredicate = "FROM c WHERE c.linkedId = null"
        };

        var response = await _container.PatchItemAsync<dynamic>(
            document.id, new PartitionKey("pk-1"), patchOperations, options);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task PatchWithFilterPredicate_MissingPropertyNotEqualNull_FailsPrecondition()
    {
        // linkedId is null → will NOT be serialized due to NullValueHandling.Ignore
        var document = new
        {
            id = Guid.NewGuid().ToString(),
            partitionKey = "pk-1",
            linkedId = (string?)null,
            name = "test"
        };
        await _container.CreateItemAsync(document, new PartitionKey("pk-1"));

        // WHERE c.linkedId != null should NOT match when property is missing (treated as null)
        var patchOperations = new[] { PatchOperation.Set("/name", "updated") };
        var options = new PatchItemRequestOptions
        {
            FilterPredicate = "FROM c WHERE c.linkedId != null"
        };

        var act = () => _container.PatchItemAsync<dynamic>(
            document.id, new PartitionKey("pk-1"), patchOperations, options);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task PatchWithFilterPredicate_ExplicitNullPropertyEqualsNull_Succeeds()
    {
        // Use stream to explicitly write null (bypassing NullValueHandling.Ignore)
        var id = Guid.NewGuid().ToString();
        var json = $$"""{"id":"{{id}}","partitionKey":"pk-1","linkedId":null,"name":"test"}""";
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        await _container.CreateItemStreamAsync(stream, new PartitionKey("pk-1"));

        var patchOperations = new[] { PatchOperation.Set("/linkedId", "new-value") };
        var options = new PatchItemRequestOptions
        {
            FilterPredicate = "FROM c WHERE c.linkedId = null"
        };

        var response = await _container.PatchItemAsync<dynamic>(
            id, new PartitionKey("pk-1"), patchOperations, options);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task PatchWithFilterPredicate_MissingPropertyEqualsValue_FailsPrecondition()
    {
        // linkedId is null → will NOT be serialized due to NullValueHandling.Ignore
        var document = new
        {
            id = Guid.NewGuid().ToString(),
            partitionKey = "pk-1",
            linkedId = (string?)null,
            name = "test"
        };
        await _container.CreateItemAsync(document, new PartitionKey("pk-1"));

        // WHERE c.linkedId = 'some-value' should NOT match — missing property treated as null ≠ 'some-value'
        var patchOperations = new[] { PatchOperation.Set("/name", "updated") };
        var options = new PatchItemRequestOptions
        {
            FilterPredicate = "FROM c WHERE c.linkedId = 'some-value'"
        };

        var act = () => _container.PatchItemAsync<dynamic>(
            document.id, new PartitionKey("pk-1"), patchOperations, options);

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }
}

/// <summary>
/// A simple Newtonsoft.Json-based CosmosSerializer for test use.
/// The SDK's built-in one is internal, so we provide a minimal implementation.
/// </summary>
internal sealed class CosmosJsonDotNetSerializer : CosmosSerializer
{
    private readonly JsonSerializer _serializer;

    public CosmosJsonDotNetSerializer(JsonSerializerSettings settings)
    {
        _serializer = JsonSerializer.Create(settings);
    }

    public override T FromStream<T>(Stream stream)
    {
        using var sr = new StreamReader(stream);
        using var jr = new JsonTextReader(sr);
        return _serializer.Deserialize<T>(jr)!;
    }

    public override Stream ToStream<T>(T input)
    {
        var ms = new MemoryStream();
        using (var sw = new StreamWriter(ms, leaveOpen: true))
        using (var jw = new JsonTextWriter(sw))
        {
            _serializer.Serialize(jw, input);
            jw.Flush();
        }
        ms.Position = 0;
        return ms;
    }
}
