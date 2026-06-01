using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests that queries without an ORDER BY clause return documents in insertion order,
/// matching the observed behavior of real Cosmos DB and the Windows Cosmos Emulator.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerInsertionOrderTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("test-insertion-order", "/partitionKey");
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    private async Task<List<T>> DrainQuery<T>(string sql, string? partitionKey = null)
    {
        var options = partitionKey is not null
            ? new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) }
            : null;
        var iterator = _container.GetItemQueryIterator<T>(sql, requestOptions: options);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    private async Task<List<T>> DrainQuery<T>(QueryDefinition queryDef, string? partitionKey = null)
    {
        var options = partitionKey is not null
            ? new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) }
            : null;
        var iterator = _container.GetItemQueryIterator<T>(queryDef, requestOptions: options);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Insertion order — queries without ORDER BY
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Verifies that documents queried within a single partition are returned in
    /// insertion order when no ORDER BY clause is specified.
    /// Ref: Observed behavior on Windows Cosmos DB Emulator — documents return
    ///   in the order they were created when no ORDER BY is applied.
    /// </summary>
    [Fact]
    public async Task Query_WithoutOrderBy_ReturnsSinglePartitionDocsInInsertionOrder()
    {
        // Arrange — insert 10 documents sequentially
        var insertedIds = new List<string>();
        for (var i = 1; i <= 10; i++)
        {
            var id = $"order-{i:D4}";
            insertedIds.Add(id);
            await _container.CreateItemAsync(
                new TestDocument { Id = id, PartitionKey = "pk-order", Name = $"Doc {i}", Value = i },
                new PartitionKey("pk-order"));
        }

        // Act — query without ORDER BY
        var results = await DrainQuery<TestDocument>(
            "SELECT * FROM c WHERE c.partitionKey = 'pk-order'",
            "pk-order");

        // Assert — returned IDs match insertion order
        var returnedIds = results.Select(r => r.Id).ToList();
        returnedIds.Should().Equal(insertedIds);
    }

    /// <summary>
    /// Verifies that a cross-partition query without ORDER BY returns documents
    /// in insertion order across all partitions.
    /// </summary>
    [Fact]
    public async Task Query_WithoutOrderBy_ReturnsCrossPartitionDocsInInsertionOrder()
    {
        // Arrange — insert documents across two partitions, interleaved
        var insertedIds = new List<string>();
        for (var i = 1; i <= 8; i++)
        {
            var pk = i % 2 == 0 ? "pk-even" : "pk-odd";
            var id = $"cross-{i:D4}";
            insertedIds.Add(id);
            await _container.CreateItemAsync(
                new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc {i}", Value = i },
                new PartitionKey(pk));
        }

        // Act — cross-partition query without ORDER BY
        var results = await DrainQuery<TestDocument>("SELECT * FROM c");

        // Assert — returned IDs match insertion order
        var returnedIds = results.Select(r => r.Id).ToList();
        returnedIds.Should().Equal(insertedIds);
    }

    /// <summary>
    /// Verifies that replacing a document does not change its position in the
    /// insertion order (replace preserves original creation position).
    /// </summary>
    [Fact]
    public async Task Query_AfterReplace_PreservesInsertionOrder()
    {
        // Arrange — insert 5 documents
        var insertedIds = new List<string>();
        for (var i = 1; i <= 5; i++)
        {
            var id = $"replace-{i:D4}";
            insertedIds.Add(id);
            await _container.CreateItemAsync(
                new TestDocument { Id = id, PartitionKey = "pk-replace", Name = $"Doc {i}", Value = i },
                new PartitionKey("pk-replace"));
        }

        // Replace the 2nd document
        await _container.ReplaceItemAsync(
            new TestDocument { Id = "replace-0002", PartitionKey = "pk-replace", Name = "Updated", Value = 99 },
            "replace-0002",
            new PartitionKey("pk-replace"));

        // Act
        var results = await DrainQuery<TestDocument>(
            "SELECT * FROM c WHERE c.partitionKey = 'pk-replace'",
            "pk-replace");

        // Assert — order unchanged
        var returnedIds = results.Select(r => r.Id).ToList();
        returnedIds.Should().Equal(insertedIds);
    }

    /// <summary>
    /// Verifies that deleting a document and re-creating it places the new
    /// document at the end (new insertion position).
    /// </summary>
    [Fact]
    public async Task Query_AfterDeleteAndRecreate_NewDocAppearsAtEnd()
    {
        // Arrange — insert 5 documents
        for (var i = 1; i <= 5; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"delrec-{i:D4}", PartitionKey = "pk-delrec", Name = $"Doc {i}", Value = i },
                new PartitionKey("pk-delrec"));
        }

        // Delete the 2nd document
        await _container.DeleteItemAsync<TestDocument>("delrec-0002", new PartitionKey("pk-delrec"));

        // Re-create with same ID
        await _container.CreateItemAsync(
            new TestDocument { Id = "delrec-0002", PartitionKey = "pk-delrec", Name = "Recreated", Value = 200 },
            new PartitionKey("pk-delrec"));

        // Act
        var results = await DrainQuery<TestDocument>(
            "SELECT * FROM c WHERE c.partitionKey = 'pk-delrec'",
            "pk-delrec");

        // Assert — recreated doc is now at the end
        var returnedIds = results.Select(r => r.Id).ToList();
        returnedIds.Should().Equal(["delrec-0001", "delrec-0003", "delrec-0004", "delrec-0005", "delrec-0002"]);
    }

    /// <summary>
    /// Verifies that upsert of a new document places it at the end of
    /// the insertion order (same as create).
    /// </summary>
    [Fact]
    public async Task Query_UpsertNewDoc_AppearsAtEnd()
    {
        // Arrange — insert 3 documents
        for (var i = 1; i <= 3; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"upsert-{i:D4}", PartitionKey = "pk-upsert", Name = $"Doc {i}", Value = i },
                new PartitionKey("pk-upsert"));
        }

        // Upsert a new document
        await _container.UpsertItemAsync(
            new TestDocument { Id = "upsert-0004", PartitionKey = "pk-upsert", Name = "New", Value = 4 },
            new PartitionKey("pk-upsert"));

        // Act
        var results = await DrainQuery<TestDocument>(
            "SELECT * FROM c WHERE c.partitionKey = 'pk-upsert'",
            "pk-upsert");

        // Assert — upserted new doc at end
        var returnedIds = results.Select(r => r.Id).ToList();
        returnedIds.Should().Equal(["upsert-0001", "upsert-0002", "upsert-0003", "upsert-0004"]);
    }

    /// <summary>
    /// Verifies that upsert of an existing document preserves its original
    /// insertion position (same as replace).
    /// </summary>
    [Fact]
    public async Task Query_UpsertExistingDoc_PreservesInsertionOrder()
    {
        // Arrange — insert 3 documents
        for (var i = 1; i <= 3; i++)
        {
            await _container.CreateItemAsync(
                new TestDocument { Id = $"upsert-ex-{i:D4}", PartitionKey = "pk-upsert-ex", Name = $"Doc {i}", Value = i },
                new PartitionKey("pk-upsert-ex"));
        }

        // Upsert an existing document (update)
        await _container.UpsertItemAsync(
            new TestDocument { Id = "upsert-ex-0002", PartitionKey = "pk-upsert-ex", Name = "Updated", Value = 99 },
            new PartitionKey("pk-upsert-ex"));

        // Act
        var results = await DrainQuery<TestDocument>(
            "SELECT * FROM c WHERE c.partitionKey = 'pk-upsert-ex'",
            "pk-upsert-ex");

        // Assert — order preserved
        var returnedIds = results.Select(r => r.Id).ToList();
        returnedIds.Should().Equal(["upsert-ex-0001", "upsert-ex-0002", "upsert-ex-0003"]);
    }
}
