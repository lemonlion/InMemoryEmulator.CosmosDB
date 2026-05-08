using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.Tests.Infrastructure;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for LINQ queries through FakeCosmosHandler (SDK translates LINQ → SQL → HTTP → handler).
/// Parity-validated: runs against both FakeCosmosHandler (in-memory) and real emulator.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerLinqTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("test-linq", "/partitionKey");
        await SeedData();
    }

    private async Task SeedData()
    {
        var docs = new[]
        {
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 30, IsActive = true, Tags = ["admin", "user"] },
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["user"] },
            new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 50, IsActive = true, Tags = ["admin"] },
            new TestDocument { Id = "4", PartitionKey = "pk2", Name = "Diana", Value = 10, IsActive = true, Tags = ["user", "moderator"] },
            new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = 40, IsActive = false, Tags = [] },
        };
        foreach (var doc in docs)
            await _container.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    private async Task<List<T>> DrainIterator<T>(FeedIterator<T> iterator)
    {
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LINQ Where
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_Where_FiltersByProperty()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.Name == "Alice")
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
    }

    [Fact]
    public async Task Linq_Where_BooleanFilter()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.IsActive)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie", "Diana");
    }

    [Fact]
    public async Task Linq_Where_ComparisonOperator()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.Value > 25)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(3);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie", "Eve");
    }

    [Fact]
    public async Task Linq_Where_CombinedConditions()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.IsActive && d.Value > 25)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LINQ Select Projections
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_Select_AnonymousProjection()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.Name == "Alice")
            .Select(d => new { d.Name, d.Value })
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
        results[0].Value.Should().Be(30);
    }

    [Fact]
    public async Task Linq_Select_SingleProperty()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Select(d => d.Name)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(5);
        results.Should().Contain("Alice");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LINQ OrderBy
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_OrderBy_Ascending()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .OrderBy(d => d.Value)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Select(r => r.Value).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Linq_OrderByDescending()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .OrderByDescending(d => d.Value)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Select(r => r.Value).Should().BeInDescendingOrder();
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)] // Real Cosmos requires composite index for multi-field ORDER BY
    public async Task Linq_OrderBy_ThenBy()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .OrderBy(d => d.PartitionKey)
            .ThenBy(d => d.Name)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Select(r => r.Name).Should().ContainInOrder("Alice", "Bob", "Eve", "Charlie", "Diana");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LINQ Take / Skip
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_Take_LimitsResults()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .OrderBy(d => d.Name)
            .Take(2)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(2);
        results[0].Name.Should().Be("Alice");
        results[1].Name.Should().Be("Bob");
    }

    [Fact]
    public async Task Linq_Skip_Take_Pagination()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .OrderBy(d => d.Name)
            .Skip(1)
            .Take(2)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(2);
        results[0].Name.Should().Be("Bob");
        results[1].Name.Should().Be("Charlie");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LINQ Count / Aggregates
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_CountAsync_ReturnsCorrectCount()
    {
        var count = await _container.GetItemLinqQueryable<TestDocument>()
            .CountAsync();

        count.Resource.Should().Be(5);
    }

    [Fact]
    public async Task Linq_CountAsync_WithFilter()
    {
        var count = await _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.IsActive)
            .CountAsync();

        count.Resource.Should().Be(3);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LINQ with PartitionKey scoping
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_WithPartitionKey_ScopesQuery()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>(
                requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") })
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(3);
        results.Should().OnlyContain(d => d.PartitionKey == "pk1");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  LINQ complex expressions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Linq_Where_StringContains()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.Name.Contains("li"))
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo("Alice", "Charlie");
    }

    [Fact]
    public async Task Linq_Where_NestedPropertyAccess()
    {
        // Add a doc with nested object
        await _container.CreateItemAsync(
            new TestDocument { Id = "n1", PartitionKey = "pk1", Name = "Nested", Nested = new NestedObject { Description = "test", Score = 99.5 } },
            new PartitionKey("pk1"));

        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.Nested != null && d.Nested.Score > 50)
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Nested");
    }

    [Fact]
    public async Task Linq_Where_OrCondition()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .Where(d => d.Name == "Alice" || d.Name == "Bob")
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(2);
    }

    [Fact]
    public async Task Linq_Select_WithOrderByAndTake()
    {
        var iterator = _container.GetItemLinqQueryable<TestDocument>()
            .OrderByDescending(d => d.Value)
            .Take(3)
            .Select(d => new { d.Name, d.Value })
            .ToFeedIterator();

        var results = await DrainIterator(iterator);
        results.Should().HaveCount(3);
        results[0].Value.Should().Be(50);
        results[1].Value.Should().Be(40);
        results[2].Value.Should().Be(30);
    }
}
