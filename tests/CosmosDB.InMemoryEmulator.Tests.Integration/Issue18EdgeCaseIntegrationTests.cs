using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Integration-level edge-case tests for the InMemoryCosmosException factory fix (Issue #18).
/// These exercise error paths through the full SDK HTTP pipeline via FakeCosmosHandler,
/// verifying that exceptions are exactly <see cref="CosmosException"/> and carry the
/// expected status codes.
///
/// The concurrent test uses adaptive concurrency: lower volume when targeting a real
/// emulator to avoid overwhelming its partition service with 503 "high demand" errors,
/// while still validating the same thread-safety behaviour.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class Issue18EdgeCaseIntegrationTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("issue18-edge", "/pk");
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ETAG / PRECONDITION FAILED
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ETagMismatch_Replace_ThrowsCosmosException_412()
    {
        await _container.CreateItemAsync(
            new { id = "etag1", pk = "pk1", value = "a" }, new PartitionKey("pk1"));

        var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
            await _container.ReplaceItemAsync(
                new { id = "etag1", pk = "pk1", value = "b" }, "etag1", new PartitionKey("pk1"),
                new ItemRequestOptions { IfMatchEtag = "\"wrong-etag\"" }));

        ex.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    [Fact]
    public async Task IfNoneMatch_Star_OnExistingItem_ThrowsCosmosException_304()
    {
        await _container.CreateItemAsync(
            new { id = "nm1", pk = "pk1" }, new PartitionKey("pk1"));

        var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
            await _container.ReadItemAsync<dynamic>("nm1", new PartitionKey("pk1"),
                new ItemRequestOptions { IfNoneMatchEtag = "*" }));

        ex.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }

    [Fact]
    public async Task IfNoneMatch_MatchingEtag_ThrowsCosmosException_304()
    {
        var created = await _container.CreateItemAsync(
            new { id = "nm2", pk = "pk1" }, new PartitionKey("pk1"));
        var etag = created.ETag;

        var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
            await _container.ReadItemAsync<dynamic>("nm2", new PartitionKey("pk1"),
                new ItemRequestOptions { IfNoneMatchEtag = etag }));

        ex.StatusCode.Should().Be(HttpStatusCode.NotModified);
    }

    [Fact]
    public async Task ETagMismatch_Patch_ThrowsCosmosException_412()
    {
        await _container.CreateItemAsync(
            new { id = "etag2", pk = "pk1", name = "test" }, new PartitionKey("pk1"));

        var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
            await _container.PatchItemAsync<dynamic>("etag2", new PartitionKey("pk1"),
                new[] { PatchOperation.Set("/name", "new") },
                new PatchItemRequestOptions { IfMatchEtag = "\"wrong-etag\"" }));

        ex.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MULTIPLE SEQUENTIAL ERRORS
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MultipleSequentialErrors_AllCatchableAsCosmosException()
    {
        await _container.CreateItemAsync(new { id = "seq1", pk = "pk1" }, new PartitionKey("pk1"));

        var errors = new List<HttpStatusCode>();

        // Error 1: Read non-existent → 404
        try { await _container.ReadItemAsync<dynamic>("nope", new PartitionKey("pk1")); }
        catch (CosmosException ex) { errors.Add(ex.StatusCode); }

        // Error 2: Duplicate create → 409
        try { await _container.CreateItemAsync(new { id = "seq1", pk = "pk1" }, new PartitionKey("pk1")); }
        catch (CosmosException ex) { errors.Add(ex.StatusCode); }

        // Error 3: Delete non-existent → 404
        try { await _container.DeleteItemAsync<dynamic>("nope", new PartitionKey("pk1")); }
        catch (CosmosException ex) { errors.Add(ex.StatusCode); }

        errors.Should().HaveCount(3);
        errors[0].Should().Be(HttpStatusCode.NotFound);
        errors[1].Should().Be(HttpStatusCode.Conflict);
        errors[2].Should().Be(HttpStatusCode.NotFound);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // VALIDATION ERRORS (400 BadRequest)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EmptyId_ThrowsCosmosException_400()
    {
        var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
            await _container.CreateItemAsync(new { id = "", pk = "pk1" }, new PartitionKey("pk1")));

        ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task PatchTooManyOps_ThrowsCosmosException_400()
    {
        await _container.CreateItemAsync(new { id = "patch1", pk = "pk1", a = 0 }, new PartitionKey("pk1"));

        var ops = Enumerable.Range(0, 11)
            .Select(i => PatchOperation.Set($"/field{i}", i))
            .ToArray();

        var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
            await _container.PatchItemAsync<dynamic>("patch1", new PartitionKey("pk1"), ops));

        ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task PatchEmptyOps_ThrowsException()
    {
        await _container.CreateItemAsync(new { id = "patch2", pk = "pk1" }, new PartitionKey("pk1"));

        // SDK validates empty patch operations before reaching the handler,
        // throwing ArgumentException rather than CosmosException
        var act = () => _container.PatchItemAsync<dynamic>("patch2", new PartitionKey("pk1"),
                Array.Empty<PatchOperation>());

        await act.Should().ThrowAsync<Exception>();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // TRANSACTIONAL BATCH ERRORS
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BatchConflict_ReturnsFailedResponse()
    {
        await _container.CreateItemAsync(new { id = "bdup", pk = "pk1" }, new PartitionKey("pk1"));

        var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"))
            .CreateItem(new { id = "bdup", pk = "pk1" });

        var response = await batch.ExecuteAsync();
        response.IsSuccessStatusCode.Should().BeFalse();
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CONCURRENT CONTAINER OPERATIONS RAISING EXCEPTIONS
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrentReadsOfNonExistent_AllThrowCosmosException()
    {
        // Use lower concurrency on emulators to avoid overwhelming the partition
        // service (503 "high demand in this region"). The behavioural assertion is
        // the same — all reads of non-existent items throw CosmosException(404).
        var concurrency = session.IsEmulator ? 10 : 50;
        var tasks = Enumerable.Range(0, concurrency).Select(async i =>
        {
            var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
                await _container.ReadItemAsync<dynamic>($"nope-{i}", new PartitionKey("pk1")));

            ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
            return ex;
        });

        var results = await Task.WhenAll(tasks);
        results.Should().HaveCount(concurrency);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // AGGREGATE EXCEPTION SCENARIOS
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AggregateException_ContainingCosmosException_CanBeUnwrapped()
    {
        var task1 = Task.Run(() =>
            _container.ReadItemAsync<dynamic>("agg1", new PartitionKey("pk1")));
        var task2 = Task.Run(() =>
            _container.ReadItemAsync<dynamic>("agg2", new PartitionKey("pk1")));

        var allTask = Task.WhenAll(task1, task2);
        try
        {
            await allTask;
            Assert.Fail("Should have thrown");
        }
        catch (CosmosException firstEx)
        {
            firstEx.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }

        task1.IsFaulted.Should().BeTrue();
        task2.IsFaulted.Should().BeTrue();

        allTask.Exception.Should().NotBeNull();
        allTask.Exception!.InnerExceptions.Should().HaveCount(2);
        foreach (var inner in allTask.Exception.InnerExceptions)
        {
            inner.Should().BeOfType<CosmosException>();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // REPLACE WITH WRONG BODY ID
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] // Windows emulator returns 200 (emulator bug); see docs link below
    public async Task ReplaceItem_BodyIdMismatch_ThrowsBadRequest()
    {
        // SDK docs state body id must match the id parameter.
        // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
        await _container.CreateItemAsync(new { id = "rep1", pk = "pk1" }, new PartitionKey("pk1"));

        var act = () => _container.ReplaceItemAsync(
            new { id = "DIFFERENT", pk = "pk1" }, "rep1", new PartitionKey("pk1"));

        var ex = await act.Should().ThrowAsync<CosmosException>();
        ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UPSERT WITH IF-MATCH ON NON-EXISTENT ITEM
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Upsert_IfMatch_NonExistent_CreatesItem()
    {
        // Real Cosmos DB creates the item. If-Match is "applicable only on PUT and DELETE"
        // per the REST API common headers docs. Upsert uses POST, so If-Match is ignored
        // on the insert path.
        var response = await _container.UpsertItemAsync(
            new { id = "noexist", pk = "pk1" }, new PartitionKey("pk1"),
            new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // EXCEPTION PRESERVES PROPERTIES AFTER ASYNC STATE MACHINE TRAVERSAL
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ExceptionProperties_Preserved_AfterAsyncAwait()
    {
        CosmosException? caught = null;
        try
        {
            await NestedAsyncCall(_container);
        }
        catch (CosmosException ex)
        {
            caught = ex;
        }

        caught.Should().NotBeNull();
        caught!.StatusCode.Should().Be(HttpStatusCode.NotFound);
        // Through the SDK pipeline, Diagnostics is populated by DiagnosticsHandler
        caught.Diagnostics.Should().NotBeNull();
    }

    private static async Task NestedAsyncCall(Container container)
    {
        await Task.Yield();
        await container.ReadItemAsync<dynamic>("nonexistent", new PartitionKey("pk1"));
    }
}
