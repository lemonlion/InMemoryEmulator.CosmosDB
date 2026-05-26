using System.Net;
using System.Text;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests that verify LINQ-to-FeedIterator integration with InMemoryContainer,
/// covering both the official <c>CosmosLinqExtensions.ToFeedIterator()</c> and
/// the <c>ToFeedIteratorOverridable()</c> workaround.
/// </summary>
[Collection("FeedIteratorSetup")]
public class LinqToFeedIteratorTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task ToFeedIterator_ThrowsWhenUsedWithInMemoryContainer()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Name == "Alice");

		var act = () => queryable.ToFeedIterator();

		act.Should().Throw<ArgumentOutOfRangeException>()
			.WithMessage("*ToFeedIterator is only supported on Cosmos LINQ query operations*");
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_WorksWithInMemoryContainer()
	{
		InMemoryFeedIteratorSetup.Register();

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Value > 10)
			.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(2);
		results.Select(document => document.Name).Should().BeEquivalentTo(["Bob", "Charlie"]);
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_WithOrderBy_ReturnsOrderedResults()
	{
		InMemoryFeedIteratorSetup.Register();

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.OrderBy(document => document.Name)
			.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Bob", "Charlie");
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_WithSelect_ReturnsProjectedResults()
	{
		InMemoryFeedIteratorSetup.Register();

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.Select(document => document.Name)
			.ToFeedIteratorOverridable();

		var results = new List<string>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().ContainSingle().Which.Should().Be("Alice");
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_WithNoMatchingItems_ReturnsEmpty()
	{
		InMemoryFeedIteratorSetup.Register();

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Name == "Nobody")
			.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().BeEmpty();
	}

	[Fact]
	public void Deregister_ClearsFactory()
	{
		InMemoryFeedIteratorSetup.Register();
		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().NotBeNull();

		InMemoryFeedIteratorSetup.Deregister();
		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().BeNull();
	}

	[Fact]
	public void Deregister_AllowsReRegister()
	{
		InMemoryFeedIteratorSetup.Register();
		InMemoryFeedIteratorSetup.Deregister();
		InMemoryFeedIteratorSetup.Register();

		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().NotBeNull();
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_WithRegister_WorksWithFilter()
	{
		InMemoryFeedIteratorSetup.Register();

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Value > 10)
			.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_WithRegister_OrderByWorks()
	{
		InMemoryFeedIteratorSetup.Register();

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.OrderBy(document => document.Name)
			.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Charlie");
	}
}


public class LinqGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedItems()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 30, IsActive = true, Tags = ["urgent"] }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 10, IsActive = true, Tags = ["urgent", "important"] }, new PartitionKey("pk2"));
	}

	[Fact]
	public async Task Linq_Where_CompoundConditions()
	{
		await SeedItems();

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var results = queryable.Where(doc => doc.Value > 15 && doc.IsActive).ToList();

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Linq_OrderBy_ThenByDescending()
	{
		await SeedItems();

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var results = queryable.OrderBy(doc => doc.IsActive).ThenByDescending(doc => doc.Value).ToList();

		// false sorts before true; within active=false, highest value first
		results[0].Name.Should().Be("Bob");
	}

	[Fact]
	public async Task Linq_Skip_Take_Pagination()
	{
		await SeedItems();

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var results = queryable.OrderBy(doc => doc.Value).Skip(1).Take(1).ToList();

		results.Should().ContainSingle().Which.Value.Should().Be(20);
	}

	[Fact]
	public async Task Linq_Count_Aggregate()
	{
		await SeedItems();

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var count = queryable.Count();

		count.Should().Be(3);
	}

	[Fact]
	public async Task Linq_FirstOrDefault()
	{
		await SeedItems();

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var result = queryable.FirstOrDefault(doc => doc.Name == "Charlie");

		result.Should().NotBeNull();
		result!.Value.Should().Be(10);
	}

	[Fact]
	public async Task Linq_Any_ExistenceCheck()
	{
		await SeedItems();

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var hasUrgent = queryable.Any(doc => doc.Tags.Contains("urgent"));

		hasUrgent.Should().BeTrue();
	}
}


/// <summary>
/// Validates that GetItemLinqQueryable can accept a continuation token and
/// CosmosLinqSerializerOptions without throwing, even though InMemoryContainer
/// ignores both parameters.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.getitemlinqqueryable
/// </summary>
public class LinqQueryableParameterTests5
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Linq_WithContinuationToken_DoesNotThrow()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Should not throw when passing a continuation token
		var queryable = _container.GetItemLinqQueryable<TestDocument>(
			allowSynchronousQueryExecution: true,
			continuationToken: "some-token");

		queryable.ToList().Should().ContainSingle();
	}

	[Fact]
	public async Task Linq_WithLinqSerializerOptions_DoesNotThrow()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var options = new CosmosLinqSerializerOptions
		{
			PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
		};

		// Should not throw when passing serializer options
		var queryable = _container.GetItemLinqQueryable<TestDocument>(
			allowSynchronousQueryExecution: true,
			linqSerializerOptions: options);

		queryable.ToList().Should().ContainSingle();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 23 — LINQ Operator Coverage
// ═══════════════════════════════════════════════════════════════════════════════

public class LinqOperatorTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedItems()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 30, IsActive = true }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 10, IsActive = true }, new PartitionKey("pk2"));
		await _container.CreateItemAsync(new TestDocument { Id = "4", PartitionKey = "pk2", Name = "Diana", Value = 40, IsActive = true }, new PartitionKey("pk2"));
	}

	[Fact]
	public async Task Linq_OrderByDescending()
	{
		await SeedItems();
		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		var results = q.OrderByDescending(d => d.Value).ToList();

		results.Select(d => d.Value).Should().BeInDescendingOrder();
	}

	[Fact]
	public async Task Linq_ThenBy_AfterOrderBy()
	{
		await SeedItems();
		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		var results = q.OrderBy(d => d.IsActive).ThenBy(d => d.Value).ToList();

		// false < true, then ascending value
		results[0].Name.Should().Be("Bob"); // IsActive=false, Value=20
		results[1].Value.Should().BeLessThanOrEqualTo(results[2].Value);
	}

	[Fact]
	public async Task Linq_Sum_Aggregate()
	{
		await SeedItems();
		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		var sum = q.Sum(d => d.Value);

		sum.Should().Be(100); // 30+20+10+40
	}

	[Fact]
	public async Task Linq_Average_Aggregate()
	{
		await SeedItems();
		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		var avg = q.Average(d => d.Value);

		avg.Should().Be(25.0); // 100/4
	}

	[Fact]
	public async Task Linq_Min_Max_Aggregates()
	{
		await SeedItems();
		var q = _container.GetItemLinqQueryable<TestDocument>(true);

		q.Min(d => d.Value).Should().Be(10);
		q.Max(d => d.Value).Should().Be(40);
	}

	[Fact]
	public async Task Linq_Distinct_RemovesDuplicateProjections()
	{
		await SeedItems();
		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		var activeFlags = q.Select(d => d.IsActive).Distinct().ToList();

		activeFlags.Should().HaveCount(2);
		activeFlags.Should().Contain(true);
		activeFlags.Should().Contain(false);
	}

	[Fact]
	public async Task Linq_GroupBy_ByPartitionKey()
	{
		await SeedItems();
		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		var groups = q.GroupBy(d => d.PartitionKey).ToList();

		groups.Should().HaveCount(2);
		groups.SelectMany(g => g).Should().HaveCount(4);
	}

	[Fact]
	public async Task Linq_SelectMany_FlattenTags()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A", Tags = ["x", "y"] },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B", Tags = ["y", "z"] },
			new PartitionKey("pk1"));

		var q = container.GetItemLinqQueryable<TestDocument>(true);
		var allTags = q.SelectMany(d => d.Tags).ToList();

		allTags.Should().HaveCount(4);
		allTags.Should().Contain("x");
		allTags.Should().Contain("z");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 23 — Registration Lifecycle
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqRegistrationLifecycleTests
{
	[Fact]
	public void Register_SetsStaticFallbackFactory()
	{
		InMemoryFeedIteratorSetup.Register();
		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().NotBeNull();
	}

	[Fact]
	public void Register_IsIdempotent_CallingTwiceDoesNotThrow()
	{
		InMemoryFeedIteratorSetup.Register();
		InMemoryFeedIteratorSetup.Register(); // second call should not throw

		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().NotBeNull();
	}

	[Fact]
	public void ToFeedIteratorOverridable_WithoutRegister_ThrowsOrReturnsNull()
	{
		InMemoryFeedIteratorSetup.Deregister();
		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 23 — ToFeedIteratorOverridable Integration
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqFeedIteratorIntegrationTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task ToFeedIteratorOverridable_WithSkipTake_ReturnsPaginatedResults()
	{
		InMemoryFeedIteratorSetup.Register();

		for (var i = 0; i < 10; i++)
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
				new PartitionKey("pk1"));

		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.OrderBy(d => d.Value)
			.Skip(3)
			.Take(4)
			.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(4);
		results.Select(d => d.Value).Should().ContainInConsecutiveOrder(3, 4, 5, 6);
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_MultipleEnumerations_ReturnSameResults()
	{
		InMemoryFeedIteratorSetup.Register();

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		// First enumeration
		var iter1 = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "Alice")
			.ToFeedIteratorOverridable();
		var results1 = new List<TestDocument>();
		while (iter1.HasMoreResults)
		{
			var page = await iter1.ReadNextAsync();
			results1.AddRange(page);
		}

		// Second enumeration
		var iter2 = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "Alice")
			.ToFeedIteratorOverridable();
		var results2 = new List<TestDocument>();
		while (iter2.HasMoreResults)
		{
			var page = await iter2.ReadNextAsync();
			results2.AddRange(page);
		}

		results1.Should().HaveCount(1);
		results2.Should().HaveCount(1);
		results1[0].Id.Should().Be(results2[0].Id);
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_OrderByDescending_ReturnsDescendingResults()
	{
		InMemoryFeedIteratorSetup.Register();

		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 30 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 20 },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemLinqQueryable<TestDocument>()
			.OrderByDescending(d => d.Value)
			.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Select(d => d.Value).Should().ContainInConsecutiveOrder(30, 20, 10);
	}
}


public class LinqGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedItems()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Tags = ["urgent"] }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30, Tags = ["important"] }, new PartitionKey("pk2"));
	}

	[Fact]
	public async Task Linq_WithPartitionKey_FiltersCorrectly()
	{
		await SeedItems();

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
		var results = queryable.ToList();

		results.Should().HaveCount(2);
		results.Should().OnlyContain(item => item.PartitionKey == "pk1");
	}

	[Fact]
	public async Task Linq_Contains_OnCollection_InStyleQuery()
	{
		await SeedItems();

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var validNames = new[] { "Alice", "Charlie" };
		var results = queryable.Where(doc => validNames.Contains(doc.Name)).ToList();

		results.Should().HaveCount(2);
	}
}


public class LinqGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Linq_Where_EqualityFilter()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var results = queryable.Where(d => d.Name == "Alice").ToList();

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Linq_OrderBy()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bravo", Value = 2 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alpha", Value = 1 },
			new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var results = queryable.OrderBy(d => d.Value).ToList();

		results[0].Name.Should().Be("Alpha");
		results[1].Name.Should().Be("Bravo");
	}

	[Fact]
	public async Task Linq_Select_Projection()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 },
			new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var results = queryable.Select(d => new { d.Name, d.Value }).ToList();

		results.Should().ContainSingle();
		results[0].Name.Should().Be("Alice");
		results[0].Value.Should().Be(42);
	}

	[Fact]
	public async Task Linq_Count()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var count = queryable.Count();

		count.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group A: LINQ Operator Gaps
// ═══════════════════════════════════════════════════════════════════════════════

public class LinqOperatorGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedTestData()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 30, IsActive = true, Tags = ["a", "b"], Nested = new NestedObject { Description = "D1", Score = 8.5 } }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["c"], Nested = null }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 40, IsActive = true, Tags = ["a"] }, new PartitionKey("pk2"));
	}

	[Fact]
	public async Task Linq_Where_NullComparison_FiltersNullNested()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.Nested == null).ToList();
		results.Should().HaveCount(2); // Bob and Charlie (Nested default is null)
		results.Should().Contain(r => r.Name == "Bob");
	}

	[Fact]
	public async Task Linq_Where_NestedPropertyAccess()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.Nested != null && d.Nested.Score > 5.0).ToList();
		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Linq_Where_StringContains()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.Name.Contains("li")).ToList();
		results.Should().HaveCount(2); // Alice and Charlie both contain "li"
		results.Should().Contain(r => r.Name == "Alice");
	}

	[Fact]
	public async Task Linq_Where_StringStartsWith()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.Name.StartsWith("A")).ToList();
		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Linq_Where_StringEndsWith()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.Name.EndsWith("ce")).ToList();
		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Linq_Where_OrCondition()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.Name == "Alice" || d.Name == "Bob").ToList();
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Linq_Where_NotCondition()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => !d.IsActive).ToList();
		results.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public async Task Linq_Where_ArithmeticExpression()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.Value * 2 > 50).ToList();
		results.Should().HaveCount(2); // Alice (60) and Charlie (80)
	}

	[Fact]
	public async Task Linq_Where_MultipleChainedWhereClauses()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.IsActive).Where(d => d.Value > 25).ToList();
		results.Should().HaveCount(2); // Alice(30) and Charlie(40)
	}

	[Fact]
	public async Task Linq_Single_WithOneMatch()
	{
		await SeedTestData();
		var result = _container.GetItemLinqQueryable<TestDocument>(true).Single(d => d.Name == "Alice");
		result.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Linq_SingleOrDefault_WithNoMatch_ReturnsNull()
	{
		await SeedTestData();
		var result = _container.GetItemLinqQueryable<TestDocument>(true).SingleOrDefault(d => d.Name == "Nobody");
		result.Should().BeNull();
	}

	[Fact]
	public async Task Linq_Single_WithMultipleMatches_Throws()
	{
		await SeedTestData();
		var act = () => _container.GetItemLinqQueryable<TestDocument>(true).Single(d => d.IsActive);
		act.Should().Throw<InvalidOperationException>();
	}

	[Fact]
	public async Task Linq_All_PredicateCheck()
	{
		await SeedTestData();
		_container.GetItemLinqQueryable<TestDocument>(true).All(d => d.Value > 0).Should().BeTrue();
		_container.GetItemLinqQueryable<TestDocument>(true).All(d => d.IsActive).Should().BeFalse();
	}

	[Fact]
	public async Task Linq_Take_Zero_ReturnsEmpty()
	{
		await SeedTestData();
		_container.GetItemLinqQueryable<TestDocument>(true).Take(0).ToList().Should().BeEmpty();
	}

	[Fact]
	public async Task Linq_Take_MoreThanAvailable_ReturnsAll()
	{
		await SeedTestData();
		_container.GetItemLinqQueryable<TestDocument>(true).Take(100).ToList().Should().HaveCount(3);
	}

	[Fact]
	public async Task Linq_Skip_Zero_ReturnsAll()
	{
		await SeedTestData();
		_container.GetItemLinqQueryable<TestDocument>(true).Skip(0).ToList().Should().HaveCount(3);
	}

	[Fact]
	public async Task Linq_Select_ToAnonymousType_WithComputed()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true)
			.Select(d => new { d.Name, d.Value }).ToList();
		results.Should().Contain(r => r.Name == "Alice" && r.Value == 30);
	}

	[Fact]
	public async Task Linq_Select_ToDtoType()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true)
			.AsEnumerable() // materialize to avoid expression tree limitations
			.Select(d => (d.Name, d.Value)).ToList();
		results.Should().Contain(("Alice", 30));
	}

	[Fact]
	public async Task Linq_Where_BooleanProperty_DirectCheck()
	{
		await SeedTestData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.IsActive).ToList();
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Linq_CountWithPredicate()
	{
		await SeedTestData();
		_container.GetItemLinqQueryable<TestDocument>(true).Count(d => d.IsActive).Should().Be(2);
	}

	[Fact]
	public async Task Linq_LongCount()
	{
		await SeedTestData();
		_container.GetItemLinqQueryable<TestDocument>(true).LongCount().Should().Be(3L);
	}

	[Fact]
	public async Task Linq_CaseSensitive_StringComparison()
	{
		await SeedTestData();
		// LINQ-to-Objects is case-sensitive; "alice" != "Alice"
		_container.GetItemLinqQueryable<TestDocument>(true).Where(d => d.Name == "alice").ToList().Should().BeEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group B: ToFeedIteratorOverridable Integration
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqFeedIteratorIntegrationGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task ToFeedIteratorOverridable_EmptyContainer_NoResults()
	{
		InMemoryFeedIteratorSetup.Register();
		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var iterator = queryable.ToFeedIteratorOverridable();
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_WithPartitionKeyFilter_RespectsPartition()
	{
		InMemoryFeedIteratorSetup.Register();
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") });
		var iterator = queryable.ToFeedIteratorOverridable();
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			results.AddRange(await iterator.ReadNextAsync());
		}
		results.Should().ContainSingle().Which.Name.Should().Be("A");
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_Response_HasCorrectMetadata()
	{
		InMemoryFeedIteratorSetup.Register();
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var iterator = queryable.ToFeedIteratorOverridable();
		var page = await iterator.ReadNextAsync();

		page.StatusCode.Should().Be(HttpStatusCode.OK);
		page.RequestCharge.Should().Be(1);
		page.Count.Should().Be(1);
		page.Diagnostics.Should().NotBeNull();
		page.ActivityId.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_Response_ContinuationToken_NullOnLastPage()
	{
		InMemoryFeedIteratorSetup.Register();
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var iterator = queryable.ToFeedIteratorOverridable();
		var page = await iterator.ReadNextAsync();
		page.ContinuationToken.Should().BeNull();
		iterator.HasMoreResults.Should().BeFalse();
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_CalledTwiceOnSameQueryable_EachIteratorIndependent()
	{
		InMemoryFeedIteratorSetup.Register();
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);
		var iter1 = queryable.ToFeedIteratorOverridable();
		var iter2 = queryable.ToFeedIteratorOverridable();

		var results1 = new List<TestDocument>();
		while (iter1.HasMoreResults) results1.AddRange(await iter1.ReadNextAsync());
		var results2 = new List<TestDocument>();
		while (iter2.HasMoreResults) results2.AddRange(await iter2.ReadNextAsync());

		results1.Should().HaveCount(1);
		results2.Should().HaveCount(1);
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_WithDistinct_ReturnsUniqueItems()
	{
		InMemoryFeedIteratorSetup.Register();
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true).Select(d => d.Name).Distinct();
		var iterator = queryable.ToFeedIteratorOverridable();
		var results = new List<string>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().BeEquivalentTo(["A", "B"]);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group C: Registration Lifecycle Gaps
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqRegistrationLifecycleGapTests
{
	[Fact]
	public async Task Deregister_ThenRegister_StillWorks()
	{
		InMemoryFeedIteratorSetup.Register();
		InMemoryFeedIteratorSetup.Deregister();
		InMemoryFeedIteratorSetup.Register();

		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var iterator = container.GetItemLinqQueryable<TestDocument>(true).ToFeedIteratorOverridable();
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		results.Should().ContainSingle();

		InMemoryFeedIteratorSetup.Deregister();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group D: Edge Cases & Error Handling
// ═══════════════════════════════════════════════════════════════════════════════

public class LinqEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public void Linq_EmptyContainer_AllOperators_ReturnEmptyOrDefaults()
	{
		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		q.Where(d => d.IsActive).ToList().Should().BeEmpty();
		q.Select(d => d.Name).ToList().Should().BeEmpty();
		q.OrderBy(d => d.Name).ToList().Should().BeEmpty();
		q.Count().Should().Be(0);
		q.FirstOrDefault().Should().BeNull();
		q.Any().Should().BeFalse();
		q.All(d => d.Value > 0).Should().BeTrue(); // vacuous truth
	}

	[Fact]
	public async Task Linq_SingleItem_AllOperatorsWork()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
			new PartitionKey("pk1"));

		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		q.Where(d => d.IsActive).ToList().Should().ContainSingle();
		q.First().Name.Should().Be("Alice");
		q.Single().Name.Should().Be("Alice");
		q.Count().Should().Be(1);
		q.Any().Should().BeTrue();
		q.Sum(d => d.Value).Should().Be(10);
		q.Min(d => d.Value).Should().Be(10);
		q.Max(d => d.Value).Should().Be(10);
		q.Average(d => d.Value).Should().Be(10);
	}

	[Fact]
	public async Task Linq_LargeDataset_1000Items_HandlesCorrectly()
	{
		for (int i = 0; i < 1000; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
				new PartitionKey("pk1"));
		}

		var q = _container.GetItemLinqQueryable<TestDocument>(true);
		q.Count().Should().Be(1000);
		q.Where(d => d.Value >= 500).ToList().Should().HaveCount(500);
		q.Sum(d => d.Value).Should().Be(499500);
	}

	[Fact]
	public async Task Linq_AfterDelete_ItemNotReturnedInQuery()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		_container.GetItemLinqQueryable<TestDocument>(true).ToList().Should().BeEmpty();
	}

	[Fact]
	public async Task Linq_AfterUpsert_QueryReturnsUpdatedItem()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"));

		var result = _container.GetItemLinqQueryable<TestDocument>(true).Single();
		result.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task Linq_AfterReplace_QueryReturnsReplacedItem()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "v1" },
			new PartitionKey("pk1"));
		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "v2" }, "1",
			new PartitionKey("pk1"));

		var result = _container.GetItemLinqQueryable<TestDocument>(true).Single();
		result.Name.Should().Be("v2");
	}

	[Fact]
	public async Task Linq_WithJObjectType_ReturnsJObjects()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var results = _container.GetItemLinqQueryable<JObject>(true).ToList();
		results.Should().ContainSingle();
		results[0]["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task Linq_ConcurrentReads_DontCorrupt()
	{
		for (int i = 0; i < 20; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
		}

		var tasks = Enumerable.Range(0, 10)
			.Select(_ => Task.Run(() => _container.GetItemLinqQueryable<TestDocument>(true).ToList()))
			.ToArray();

		var allResults = await Task.WhenAll(tasks);
		foreach (var results in allResults)
		{
			results.Should().HaveCount(20);
		}
	}

	[Fact]
	public async Task Linq_MultiPartitionKey_CrossPartitionQuery()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

		// No partition key in request options → cross-partition
		var results = _container.GetItemLinqQueryable<TestDocument>(true).ToList();
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Linq_WithRequestOptions_MaxItemCount_IsIgnored()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "C" }, new PartitionKey("pk1"));

		// MaxItemCount=1 has no effect on LINQ queryable — all items returned
		var results = _container.GetItemLinqQueryable<TestDocument>(true,
			requestOptions: new QueryRequestOptions { MaxItemCount = 1 })
			.ToList();
		results.Should().HaveCount(3);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group E: Divergent Behavior Tests (Skipped + Sister)
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqDivergentBehaviorTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedData()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30 }, new PartitionKey("pk2"));
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects, where " +
		"GroupBy is natively supported by System.Linq. Real Cosmos SDK's CosmosLinqQueryProvider " +
		"cannot translate GroupBy to SQL and throws NotSupportedException. This divergence only " +
		"affects Approach 1 (direct InMemoryContainer). With Approach 3 (CosmosClient + " +
		"FakeCosmosHandler), the real SDK correctly rejects GroupBy — see " +
		"RealToFeedIteratorLinqDeepDiveTests.ToFeedIterator_GroupBy_SdkRejectsTranslation.")]
	public async Task Linq_GroupBy_RealCosmos_ShouldThrowNotSupported()
	{
		await SeedData();
		var act = () => _container.GetItemLinqQueryable<TestDocument>(true)
			.GroupBy(d => d.PartitionKey).ToList();
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public async Task Linq_GroupBy_WorksInMemory_DivergentBehavior()
	{
		// DIVERGENT: GroupBy works in-memory via LINQ-to-Objects. Real Cosmos rejects it.
		await SeedData();
		var groups = _container.GetItemLinqQueryable<TestDocument>(true)
			.GroupBy(d => d.PartitionKey).ToList();
		groups.Should().HaveCount(2);
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects, where " +
		"Last()/LastOrDefault() work natively. Cosmos SQL has no LAST equivalent, so the real " +
		"SDK rejects these. This divergence only affects Approach 1 (direct InMemoryContainer). " +
		"With Approach 3 (CosmosClient + FakeCosmosHandler), the real SDK correctly rejects " +
		"Last() — see RealToFeedIteratorLinqDeepDiveTests.ToFeedIterator_Last_SdkRejectsTranslation.")]
	public async Task Linq_Last_RealCosmos_ShouldThrowNotSupported()
	{
		await SeedData();
		var act = () => _container.GetItemLinqQueryable<TestDocument>(true).Last();
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public async Task Linq_Last_WorksInMemory_DivergentBehavior()
	{
		// DIVERGENT: Last() works in-memory. Real Cosmos rejects it.
		await SeedData();
		var result = _container.GetItemLinqQueryable<TestDocument>(true).Last();
		result.Should().NotBeNull();
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects, where " +
		"custom Aggregate() works natively. The real SDK only translates Sum/Average/Count/Min/Max " +
		"to Cosmos SQL. This divergence only affects Approach 1 (direct InMemoryContainer). " +
		"With Approach 3 (CosmosClient + FakeCosmosHandler), the real SDK correctly rejects " +
		"Aggregate() — see RealToFeedIteratorLinqDeepDiveTests.ToFeedIterator_Aggregate_SdkRejectsTranslation.")]
	public async Task Linq_Aggregate_RealCosmos_ShouldThrowNotSupported()
	{
		await SeedData();
		var act = () => _container.GetItemLinqQueryable<TestDocument>(true)
			.Aggregate(0, (acc, d) => acc + d.Value);
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public async Task Linq_Aggregate_WorksInMemory_DivergentBehavior()
	{
		// DIVERGENT: Custom Aggregate works in-memory. Real Cosmos rejects it.
		await SeedData();
		var total = _container.GetItemLinqQueryable<TestDocument>(true)
			.Aggregate(0, (acc, d) => acc + d.Value);
		total.Should().Be(60);
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects, where " +
		"Reverse() works natively. Cosmos SQL has no REVERSE ordering clause, so the real SDK " +
		"rejects it. This divergence only affects Approach 1 (direct InMemoryContainer). " +
		"With Approach 3 (CosmosClient + FakeCosmosHandler), the real SDK correctly rejects " +
		"Reverse() — see RealToFeedIteratorLinqDeepDiveTests.ToFeedIterator_Reverse_SdkRejectsTranslation.")]
	public async Task Linq_Reverse_RealCosmos_ShouldThrowNotSupported()
	{
		await SeedData();
		var act = () => _container.GetItemLinqQueryable<TestDocument>(true).Reverse().ToList();
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public async Task Linq_Reverse_WorksInMemory_DivergentBehavior()
	{
		// DIVERGENT: Reverse() works in-memory. Real Cosmos rejects it.
		await SeedData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true).Reverse().ToList();
		results.Should().HaveCount(3);
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer ignores allowSynchronousQueryExecution " +
		"because the queryable is always synchronously enumerable via LINQ-to-Objects. The real " +
		"SDK enforces this flag by throwing on ToList()/foreach, forcing ToFeedIterator(). This " +
		"divergence only affects Approach 1 (direct InMemoryContainer). With Approach 3 " +
		"(CosmosClient + FakeCosmosHandler), the real SDK correctly enforces async — see " +
		"RealToFeedIteratorLinqDeepDiveTests.ToFeedIterator_AllowSynchronousQueryExecution_False_SdkEnforcesAsync.")]
	public async Task Linq_AllowSynchronousQueryExecution_False_ShouldThrow()
	{
		await SeedData();
		var act = () => _container.GetItemLinqQueryable<TestDocument>(false).ToList();
		act.Should().Throw<InvalidOperationException>();
	}

	[Fact]
	public async Task Linq_AllowSynchronousQueryExecution_False_StillWorksInMemory_DivergentBehavior()
	{
		// DIVERGENT: allowSynchronousQueryExecution=false is ignored. Sync enumeration always works.
		await SeedData();
		var results = _container.GetItemLinqQueryable<TestDocument>(false).ToList();
		results.Should().HaveCount(3);
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS (L4): InMemoryContainer accepts continuationToken " +
		"on GetItemLinqQueryable but ignores it — all items are always materialized via " +
		"LINQ-to-Objects. Real Cosmos uses continuation tokens to resume from a server-side " +
		"checkpoint. This divergence only affects Approach 1 (direct InMemoryContainer). " +
		"With Approach 3 (CosmosClient + FakeCosmosHandler), the real SDK handles continuation " +
		"tokens through the HTTP pipeline.")]
	public async Task Linq_ContinuationToken_ShouldResumeFromCheckpoint()
	{
		await SeedData();
		// With a continuation token, should resume from midpoint
		var results = _container.GetItemLinqQueryable<TestDocument>(true, "some-token").ToList();
		results.Should().HaveCountLessThan(3);
	}

	[Fact]
	public async Task Linq_ContinuationToken_IsIgnored_DivergentBehavior()
	{
		// DIVERGENT (L4): continuationToken is ignored — all items always returned
		await SeedData();
		var results = _container.GetItemLinqQueryable<TestDocument>(true, "some-token").ToList();
		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task ToFeedIteratorOverridable_MaxItemCount_ShouldPaginate()
	{
		await SeedData();
		InMemoryFeedIteratorSetup.Register();
		var q = _container.GetItemLinqQueryable<TestDocument>(true,
			requestOptions: new QueryRequestOptions { MaxItemCount = 1 });
		var iterator = q.ToFeedIteratorOverridable();
		var pageCount = 0;
		while (iterator.HasMoreResults) { await iterator.ReadNextAsync(); pageCount++; }
		pageCount.Should().Be(3, "should page through 3 items one at a time");
		InMemoryFeedIteratorSetup.Deregister();
	}
}
