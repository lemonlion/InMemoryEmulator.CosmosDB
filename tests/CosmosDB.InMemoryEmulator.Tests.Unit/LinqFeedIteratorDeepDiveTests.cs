using System.Net;
using System.Text;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Group 1: FeedIterator Pagination Deep Dive
// ═══════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqPaginationDeepTests : IDisposable
{
	private readonly InMemoryContainer _container = new("pag-container", "/partitionKey");

	public LinqPaginationDeepTests()
	{
		InMemoryFeedIteratorSetup.Register();
	}

	public void Dispose()
	{
		InMemoryFeedIteratorSetup.Deregister();
	}

	private async Task SeedItems(int count)
	{
		for (int i = 1; i <= count; i++)
			await _container.CreateItemAsync(
				new TestDocument { Id = i.ToString(), PartitionKey = "pk", Name = $"Item{i}", Value = i },
				new PartitionKey("pk"));
	}

	[Fact]
	public async Task Pagination_MultiPage_ContinuationTokensFlowCorrectly()
	{
		await SeedItems(5);
		var queryable = _container.GetItemLinqQueryable<TestDocument>(requestOptions: new QueryRequestOptions { MaxItemCount = 2 });
		var iterator = queryable.ToFeedIteratorOverridable();

		var allItems = new List<TestDocument>();
		int pageCount = 0;
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			allItems.AddRange(page);
			pageCount++;
		}

		allItems.Should().HaveCount(5);
		pageCount.Should().BeGreaterThanOrEqualTo(2);
	}

	[Fact]
	public async Task Pagination_WithWhereFilter_PagesFilteredResults()
	{
		await SeedItems(6);
		var queryable = _container.GetItemLinqQueryable<TestDocument>(requestOptions: new QueryRequestOptions { MaxItemCount = 2 })
			.Where(d => d.Value > 3);
		var iterator = queryable.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(3); // Items 4, 5, 6
	}

	[Fact]
	public async Task Pagination_WithOrderBy_MaintainsOrderAcrossPages()
	{
		await SeedItems(5);
		var queryable = _container.GetItemLinqQueryable<TestDocument>(requestOptions: new QueryRequestOptions { MaxItemCount = 2 })
			.OrderByDescending(d => d.Value);
		var iterator = queryable.ToFeedIteratorOverridable();

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Select(r => r.Value).Should().BeInDescendingOrder();
	}

	[Fact]
	public async Task Pagination_MaxItemCountLargerThanTotal_SinglePage()
	{
		await SeedItems(3);
		var queryable = _container.GetItemLinqQueryable<TestDocument>(requestOptions: new QueryRequestOptions { MaxItemCount = 100 });
		var iterator = queryable.ToFeedIteratorOverridable();

		var page = await iterator.ReadNextAsync();
		page.Should().HaveCount(3);
		iterator.HasMoreResults.Should().BeFalse();
	}

	[Fact]
	public async Task Pagination_MaxItemCountZero_ReturnsAllInOnePage()
	{
		// Divergent: MaxItemCount=0 treated same as null (all items)
		await SeedItems(3);
		var queryable = _container.GetItemLinqQueryable<TestDocument>(requestOptions: new QueryRequestOptions { MaxItemCount = 0 });
		var iterator = queryable.ToFeedIteratorOverridable();

		var page = await iterator.ReadNextAsync();
		page.Should().HaveCount(3);
	}

	[Fact]
	public async Task Pagination_MaxItemCountNegative_ReturnsAllInOnePage()
	{
		await SeedItems(3);
		var queryable = _container.GetItemLinqQueryable<TestDocument>(requestOptions: new QueryRequestOptions { MaxItemCount = -1 });
		var iterator = queryable.ToFeedIteratorOverridable();

		var page = await iterator.ReadNextAsync();
		page.Should().HaveCount(3);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Group 2: FeedIterator Response Shape
// ═══════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqResponseShapeTests : IDisposable
{
	private readonly InMemoryContainer _container = new("resp-container", "/partitionKey");

	public LinqResponseShapeTests()
	{
		InMemoryFeedIteratorSetup.Register();
	}

	public void Dispose()
	{
		InMemoryFeedIteratorSetup.Deregister();
	}

	[Fact]
	public async Task Response_Resource_MatchesEnumeration()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test", Value = 1 },
			new PartitionKey("pk"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>();
		var iterator = queryable.ToFeedIteratorOverridable();
		var page = await iterator.ReadNextAsync();

		var enumerated = page.ToList();
		var fromResource = page.Resource.ToList();
		enumerated.Should().HaveCount(fromResource.Count);
	}

	[Fact]
	public async Task Response_CancellationToken_IsAccepted()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test", Value = 1 },
			new PartitionKey("pk"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>();
		var iterator = queryable.ToFeedIteratorOverridable();

		var page = await iterator.ReadNextAsync(CancellationToken.None);
		page.Should().HaveCount(1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Group 5: Dispose Safety
// ═══════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqDisposeSafetyTests : IDisposable
{
	private readonly InMemoryContainer _container = new("disp-container", "/partitionKey");

	public LinqDisposeSafetyTests()
	{
		InMemoryFeedIteratorSetup.Register();
	}

	public void Dispose()
	{
		InMemoryFeedIteratorSetup.Deregister();
	}

	[Fact]
	public async Task FeedIterator_Dispose_DoesNotThrow()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test", Value = 1 },
			new PartitionKey("pk"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>();
		var iterator = queryable.ToFeedIteratorOverridable();
		await iterator.ReadNextAsync();

		var act = () => iterator.Dispose();
		act.Should().NotThrow();
	}

	[Fact]
	public async Task FeedIterator_DoubleDispose_DoesNotThrow()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test", Value = 1 },
			new PartitionKey("pk"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>();
		var iterator = queryable.ToFeedIteratorOverridable();
		await iterator.ReadNextAsync();

		iterator.Dispose();
		var act = () => iterator.Dispose();
		act.Should().NotThrow();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Group 6: Divergent Operator Tests
// ═══════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqDivergentOperatorTests : IDisposable
{
	private readonly InMemoryContainer _container = new("div-ops", "/partitionKey");

	public LinqDivergentOperatorTests()
	{
		InMemoryFeedIteratorSetup.Register();
	}

	public void Dispose()
	{
		InMemoryFeedIteratorSetup.Deregister();
	}

	private async Task SeedItems()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk", Name = "A", Value = 1 }, new PartitionKey("pk"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk", Name = "B", Value = 2 }, new PartitionKey("pk"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk", Name = "C", Value = 3 }, new PartitionKey("pk"));
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects where Concat is natively supported. Real Cosmos SDK throws NotSupportedException.")]
	public async Task Linq_Concat_RealCosmos_ShouldThrowNotSupported()
	{
		await SeedItems();
		var q1 = _container.GetItemLinqQueryable<TestDocument>().Where(d => d.Value == 1);
		var q2 = _container.GetItemLinqQueryable<TestDocument>().Where(d => d.Value == 2);
		var act = () => q1.Concat(q2).ToList();
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public async Task Linq_Concat_WorksInMemory_DivergentBehavior()
	{
		await SeedItems();
		var q1 = _container.GetItemLinqQueryable<TestDocument>().Where(d => d.Value == 1);
		var q2 = _container.GetItemLinqQueryable<TestDocument>().Where(d => d.Value == 2);
		var result = q1.Concat(q2).ToList();
		result.Should().HaveCount(2);
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects where Union is natively supported. Real Cosmos SDK throws NotSupportedException.")]
	public async Task Linq_Union_RealCosmos_ShouldThrowNotSupported()
	{
		await SeedItems();
		var q1 = _container.GetItemLinqQueryable<TestDocument>().Where(d => d.Value <= 2);
		var q2 = _container.GetItemLinqQueryable<TestDocument>().Where(d => d.Value >= 2);
		var act = () => q1.Union(q2).ToList();
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public async Task Linq_Union_WorksInMemory_DivergentBehavior()
	{
		await SeedItems();
		var q1 = _container.GetItemLinqQueryable<TestDocument>().Where(d => d.Value <= 2);
		var q2 = _container.GetItemLinqQueryable<TestDocument>().Where(d => d.Value >= 2);
		var result = q1.Union(q2).ToList();
		result.Should().HaveCount(4); // LINQ-to-Objects Union uses reference equality, so objects aren't deduped
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects where DefaultIfEmpty is natively supported. Real Cosmos SDK throws NotSupportedException.")]
	public void Linq_DefaultIfEmpty_RealCosmos_ShouldThrowNotSupported()
	{
		var act = () => _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Value > 100).DefaultIfEmpty().ToList();
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public void Linq_DefaultIfEmpty_WorksInMemory_DivergentBehavior()
	{
		var result = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Value > 100).DefaultIfEmpty().ToList();
		result.Should().ContainSingle().Which.Should().BeNull();
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects where ElementAt is natively supported. Real Cosmos SDK throws NotSupportedException.")]
	public async Task Linq_ElementAt_RealCosmos_ShouldThrowNotSupported()
	{
		await SeedItems();
		var act = () => _container.GetItemLinqQueryable<TestDocument>().ElementAt(0);
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public async Task Linq_ElementAt_WorksInMemory_DivergentBehavior()
	{
		await SeedItems();
		var item = _container.GetItemLinqQueryable<TestDocument>()
			.OrderBy(d => d.Value).ElementAt(1);
		item.Value.Should().Be(2);
	}

	[Fact(Skip = "APPROACH 1 PERMISSIVENESS: InMemoryContainer uses LINQ-to-Objects where ElementAtOrDefault is natively supported. Real Cosmos SDK throws NotSupportedException.")]
	public async Task Linq_ElementAtOrDefault_RealCosmos_ShouldThrowNotSupported()
	{
		await SeedItems();
		var act = () => _container.GetItemLinqQueryable<TestDocument>().ElementAtOrDefault(100);
		act.Should().Throw<NotSupportedException>();
	}

	[Fact]
	public async Task Linq_ElementAtOrDefault_WorksInMemory_DivergentBehavior()
	{
		await SeedItems();
		var item = _container.GetItemLinqQueryable<TestDocument>().ElementAtOrDefault(100);
		item.Should().BeNull();
	}

	[Fact]
	public async Task Linq_ToDictionary_WorksInMemory()
	{
		await SeedItems();
		var dict = _container.GetItemLinqQueryable<TestDocument>()
			.ToDictionary(d => d.Id);
		dict.Should().HaveCount(3);
		dict.Should().ContainKey("1");
	}

	[Fact]
	public async Task Linq_ToHashSet_WorksInMemory()
	{
		await SeedItems();
		var set = _container.GetItemLinqQueryable<TestDocument>()
			.Select(d => d.Name).ToHashSet();
		set.Should().HaveCount(3);
		set.Should().Contain("A");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Group 7: Numeric & Type Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class LinqNumericEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("num-edge", "/partitionKey");

	[Fact]
	public void Linq_Min_OnEmpty_ThrowsInvalidOperationException()
	{
		var act = () => _container.GetItemLinqQueryable<TestDocument>()
			.Select(d => d.Value).Min();
		act.Should().Throw<InvalidOperationException>();
	}

	[Fact]
	public void Linq_Max_OnEmpty_ThrowsInvalidOperationException()
	{
		var act = () => _container.GetItemLinqQueryable<TestDocument>()
			.Select(d => d.Value).Max();
		act.Should().Throw<InvalidOperationException>();
	}

	[Fact]
	public async Task Linq_NegativeValues_InAggregates()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk", Value = -10 }, new PartitionKey("pk"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk", Value = -20 }, new PartitionKey("pk"));
		await _container.CreateItemAsync(new TestDocument { Id = "3", PartitionKey = "pk", Value = 5 }, new PartitionKey("pk"));

		var min = _container.GetItemLinqQueryable<TestDocument>().Select(d => d.Value).Min();
		var max = _container.GetItemLinqQueryable<TestDocument>().Select(d => d.Value).Max();
		var sum = _container.GetItemLinqQueryable<TestDocument>().Select(d => d.Value).Sum();

		min.Should().Be(-20);
		max.Should().Be(5);
		sum.Should().Be(-25);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Group 8: Serialization Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class LinqSerializationEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("ser-edge", "/partitionKey");

	[Fact]
	public async Task Linq_UnicodeCharacters_InValues_Queryable()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk", Name = "こんにちは" }, new PartitionKey("pk"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk", Name = "🎉" }, new PartitionKey("pk"));

		var results = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "こんにちは").ToList();
		results.Should().ContainSingle().Which.Id.Should().Be("1");
	}

	[Fact]
	public async Task Linq_EmptyStringProperty_Queryable()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk", Name = "" }, new PartitionKey("pk"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk", Name = "test" }, new PartitionKey("pk"));

		var results = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "").ToList();
		results.Should().ContainSingle().Which.Id.Should().Be("1");
	}

	[Fact]
	public async Task Linq_NullStringProperty_Queryable()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk", Name = null! }, new PartitionKey("pk"));
		await _container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk", Name = "test" }, new PartitionKey("pk"));

		var results = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == null).ToList();
		results.Should().ContainSingle().Which.Id.Should().Be("1");
	}

	[Fact]
	public async Task Linq_SpecialJsonCharacters_Queryable()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "he said \"hello\"" },
			new PartitionKey("pk"));

		var results = _container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name.Contains("hello")).ToList();
		results.Should().ContainSingle();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Group 9: Partition Key Edge Cases in LINQ
// ═══════════════════════════════════════════════════════════════════════════

public class LinqPartitionKeyEdgeCaseTests
{
	[Fact]
	public async Task Linq_WithPartitionKeyNone_ReturnsAllItems()
	{
		var container = new InMemoryContainer("pk-none", "/partitionKey");
		await container.CreateItemAsync(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" }, new PartitionKey("pk2"));

		var results = container.GetItemLinqQueryable<TestDocument>(
			requestOptions: new QueryRequestOptions { PartitionKey = PartitionKey.None })
			.ToList();
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task Linq_WithHierarchicalPartitionKey_FiltersCorrectly()
	{
		var container = new InMemoryContainer("pk-hier", new List<string> { "/region", "/city" });
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", region = "US", city = "NYC", val = 1 }),
			new PartitionKeyBuilder().Add("US").Add("NYC").Build());
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", region = "US", city = "LA", val = 2 }),
			new PartitionKeyBuilder().Add("US").Add("LA").Build());
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", region = "UK", city = "LON", val = 3 }),
			new PartitionKeyBuilder().Add("UK").Add("LON").Build());

		var results = container.GetItemLinqQueryable<JObject>(
			requestOptions: new QueryRequestOptions
			{
				PartitionKey = new PartitionKeyBuilder().Add("US").Add("NYC").Build()
			}).ToList();
		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Group 10: Stream FeedIterator
// ═══════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class LinqStreamFeedIteratorTests : IDisposable
{
	private readonly InMemoryContainer _container = new("stream-linq", "/partitionKey");

	public LinqStreamFeedIteratorTests()
	{
		InMemoryFeedIteratorSetup.Register();
	}

	public void Dispose()
	{
		InMemoryFeedIteratorSetup.Deregister();
	}

	[Fact]
	public async Task StreamFeedIterator_ReturnsStreamResponse()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test", Value = 42 },
			new PartitionKey("pk"));

		// Use GetItemQueryStreamIterator directly since ToStreamIterator requires real SDK
		var iterator = _container.GetItemQueryStreamIterator("SELECT * FROM c");

		iterator.HasMoreResults.Should().BeTrue();
		var response = await iterator.ReadNextAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().NotBeNull();

		using var reader = new StreamReader(response.Content);
		var json = await reader.ReadToEndAsync();
		json.Should().Contain("Test");
	}

	[Fact]
	public async Task StreamFeedIterator_HasCorrectStatusCode()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "X" },
			new PartitionKey("pk"));

		var iterator = _container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await iterator.ReadNextAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.IsSuccessStatusCode.Should().BeTrue();
	}
}
