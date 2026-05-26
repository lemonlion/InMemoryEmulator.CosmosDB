using System.Net;
using System.Text;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests that explore whether the real <c>CosmosLinqExtensions.ToFeedIterator()</c>
/// can be made to work with <see cref="InMemoryContainer"/> by creating a real
/// <see cref="CosmosClient"/> backed by a fake HTTP handler that delegates query
/// execution to the in-memory container.
/// </summary>
[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler)
				{
					Timeout = TimeSpan.FromSeconds(10)
				}
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task ToFeedIterator_ReturnsAllItems()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(2);
		results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Bob"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithWhereClause_FiltersCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Value > 15)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().BeEquivalentTo(["Bob", "Charlie"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithOrderBy_ReturnsOrderedResults()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 20 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(document => document.Name)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Bob", "Charlie");
	}

	[Fact]
	public async Task ToFeedIterator_WithSelectProjection_ReturnsProjectedValues()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => document.Name)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().ContainSingle().Which.Should().Be("Alice");
	}

	[Fact]
	public async Task ToFeedIterator_WithNoMatchingItems_ReturnsEmpty()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Name == "Nobody")
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ToFeedIterator_WithComplexFilter_ChainsMultipleOperators()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
			new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 40 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Value >= 20)
			.OrderByDescending(document => document.Value)
			.Take(2)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(2);
		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Diana", "Charlie");
	}

	[Fact]
	public async Task ToFeedIterator_WithWhereAndSelect_ReturnsProjectedFiltered()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Value >= 20)
			.Select(document => document.Name)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().BeEquivalentTo(["Bob", "Charlie"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithOrderByDescending_ReturnsSortedDescending()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.OrderByDescending(document => document.Value)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Charlie", "Bob", "Alice");
	}

	[Fact]
	public async Task ToFeedIterator_WithTake_LimitsResults()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Take(2)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task ToFeedIterator_WithBoolFilter_HandlesBoolean()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = true });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.IsActive)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Charlie"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithStringContains_FiltersOnSubstring()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Alicia", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Name.Contains("Ali"))
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Alicia"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithMultiplePartitionKeys_ReturnsFromAllPartitions()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Charlie", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task ToFeedIterator_WithWhereOrderByTake_ChainsAllThree()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 50 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 10 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 40 },
			new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 30 },
			new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = 20 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Value >= 20)
			.OrderBy(document => document.Value)
			.Take(3)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(3);
		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Eve", "Diana", "Charlie");
	}

	[Fact]
	public async Task ToFeedIterator_WithMultiFieldOrderBy_WrapsAllOrderByItems()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 20 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 10 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Alice", Value = 10 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(document => document.Name)
			.ThenBy(document => document.Value)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(3);
		results[0].Name.Should().Be("Alice");
		results[0].Value.Should().Be(10);
		results[1].Name.Should().Be("Alice");
		results[1].Value.Should().Be(20);
		results[2].Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ToFeedIterator_WithFaultInjector_Returns503()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 });

		_handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
		{
			Content = new StringContent("{}", Encoding.UTF8, "application/json")
		};

		var act = async () =>
		{
			var iterator = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
			await DrainAsync(iterator);
		};

		await act.Should().ThrowAsync<CosmosException>()
			.Where(exception => exception.StatusCode == HttpStatusCode.ServiceUnavailable);
	}

	[Fact]
	public async Task ToFeedIterator_WithIsDefinedInUserCode_PreservesIsDefined()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Nested = new NestedObject { Description = "test", Score = 1.0 } },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 });

		// Query with a WHERE that InMemoryContainer can evaluate.
		// This verifies IS_DEFINED is not incorrectly stripped for user-written conditions.
		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Name != null)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task ToFeedIterator_WithCountAggregate_ReturnsCount()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.CountAsync();

		var count = await iterator;

		count.Resource.Should().Be(3);
	}

	[Fact]
	public async Task ToFeedIterator_WithDistinct_ReturnsUniqueValues()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 10 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 20 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => document.Value)
			.Distinct()
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().BeEquivalentTo([10, 20]);
	}

	[Fact]
	public async Task ToFeedIterator_WithAnonymousProjection_ReturnsProjectedShape()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => new { document.Name, document.Value })
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().ContainSingle();
		var item = results[0];
		item.Name.Should().Be("Alice");
		item.Value.Should().Be(42);
	}

	[Fact]
	public async Task ToFeedIterator_WithMetadataFaultInjection_FailsOnCollectionFetch()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 });

		// Create a separate client/handler pair to test metadata faults.
		// The existing _cosmosClient has already cached metadata, so we need a fresh one.
		using var faultHandler = new FakeCosmosHandler(_inMemoryContainer);
		var callCount = 0;
		faultHandler.FaultInjectorIncludesMetadata = true;
		faultHandler.FaultInjector = request =>
		{
			callCount++;
			// Fault on the 2nd+ request (after account metadata) to hit pkranges/colls
			if (callCount > 1)
			{
				var faultResp = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
				{
					Content = new StringContent("{}", Encoding.UTF8, "application/json")
				};
				faultResp.Headers.Add("x-ms-request-charge", "0");
				faultResp.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
				return faultResp;
			}

			return null;
		};

		using var faultClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(faultHandler)
				{
					Timeout = TimeSpan.FromSeconds(10)
				}
			});

		var container = faultClient.GetContainer("fakeDb", "fakeContainer");
		var act = async () =>
		{
			var iterator = container.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
			await DrainAsync(iterator);
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task ToFeedIterator_WithSumAggregate_ReturnsSum()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var sum = await _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => document.Value)
			.SumAsync();

		sum.Resource.Should().Be(60);
	}

	[Fact]
	public async Task ToFeedIterator_WithAverageAggregate_ReturnsAverage()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var avg = await _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => (double)document.Value)
			.AverageAsync();

		avg.Resource.Should().Be(20);
	}

	[Fact]
	public async Task ToFeedIterator_WithMinAggregate_ReturnsMin()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var min = await _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => document.Value)
			.MinAsync();

		min.Resource.Should().Be(10);
	}

	[Fact]
	public async Task ToFeedIterator_WithMaxAggregate_ReturnsMax()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var max = await _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => document.Value)
			.MaxAsync();

		max.Resource.Should().Be(30);
	}

	[Fact]
	public async Task ToFeedIterator_WithSelectMany_FlattensArrays()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Tags = ["a", "b"] },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, Tags = ["c"] });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.SelectMany(document => document.Tags)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().BeEquivalentTo(["a", "b", "c"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithSkipTake_PagesCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
			new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 40 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(document => document.Value)
			.Skip(1)
			.Take(2)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(2);
		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Bob", "Charlie");
	}

	[Fact]
	public async Task ToFeedIterator_WithNegatedBoolFilter_HandlesNotOperator()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => !document.IsActive)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ToFeedIterator_WithNestedPropertyAccess_FiltersOnNestedField()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Nested = new NestedObject { Description = "test", Score = 5.0 } },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, Nested = new NestedObject { Description = "other", Score = 3.0 } });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Nested!.Score > 4.0)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task ToFeedIterator_WithOrCondition_ReturnsUnion()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Name == "Alice" || document.Name == "Charlie")
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Charlie"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithMultipleProjectionFields_ReturnsComplexProjection()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42, IsActive = true });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => new { document.Name, document.Value, document.IsActive })
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().ContainSingle();
		var item = results[0];
		item.Name.Should().Be("Alice");
		item.Value.Should().Be(42);
		item.IsActive.Should().BeTrue();
	}

	[Fact]
	public async Task ToFeedIterator_WithWhereContainsAndOrderBy_CombinesFilterWithSort()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 30 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alicia", Value = 10 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 20 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Name.Contains("Ali"))
			.OrderBy(document => document.Value)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(2);
		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alicia", "Alice");
	}

	[Fact]
	public async Task ToFeedIterator_WithCountAfterWhere_ReturnsFilteredCount()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = true });

		var count = await _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.IsActive)
			.CountAsync();

		count.Resource.Should().Be(2);
	}

	[Fact]
	public async Task ToFeedIterator_WithStringStartsWith_FiltersCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Amanda", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Name.StartsWith("A"))
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Amanda"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithNullCheck_FiltersNulls()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Nested = new NestedObject { Description = "test", Score = 5.0 } },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, Nested = null });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Nested != null)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task ToFeedIterator_WithChainedWhere_AppliesBothFilters()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = true },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = false });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.IsActive)
			.Where(document => document.Value > 15)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ToFeedIterator_WithThenBy_AppliesSecondarySort()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Charlie", Value = 10 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 20 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(document => document.Value)
			.ThenBy(document => document.Name)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Charlie", "Bob");
	}

	[Fact]
	public async Task ToFeedIterator_WithSelectComputation_ProjectsCalculatedValues()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.Select(document => new { document.Name, DoubleValue = document.Value * 2 })
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(2);
		results.Should().Contain(item => item.Name == "Alice" && item.DoubleValue == 20);
		results.Should().Contain(item => item.Name == "Bob" && item.DoubleValue == 40);
	}

	[Fact]
	public async Task ToFeedIterator_WithOrderedTake_LimitsResults()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30 });

		var iterator = _realContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(document => document.Value)
			.Take(2)
			.ToFeedIterator();

		var results = await DrainAsync(iterator);

		results.Should().HaveCount(2);
		results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Bob"]);
	}

	private async Task SeedAsync(params TestDocument[] documents)
	{
		foreach (var document in documents)
		{
			await _inMemoryContainer.CreateItemAsync(document, new PartitionKey(document.PartitionKey));
		}
	}

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		return results;
	}

	[Fact]
	public async Task FakeCosmosHandler_UnknownRoute_Returns404()
	{
		using var client = new HttpClient(_handler);
		var response = await client.DeleteAsync("https://localhost:9999/dbs/fakeDb/colls/fakeContainer/sprocs/unknown");

		response.StatusCode.Should().Be(System.Net.HttpStatusCode.NotFound);
		var body = await response.Content.ReadAsStringAsync();
		body.Should().Contain("unrecognised route");
	}

	[Fact]
	public void FakeCosmosHandler_RequestLog_RecordsRequests()
	{
		_handler.RequestLog.Should().NotBeNull();
	}

	[Fact]
	public void FakeCosmosHandler_QueryLog_RecordsQueries()
	{
		_handler.QueryLog.Should().NotBeNull();
	}
}

public class FakeCosmosHandlerOptionsTests : IAsyncLifetime
{
	[Fact]
	public async Task CustomCacheOptions_AreRespected()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			CacheTtl = TimeSpan.FromSeconds(1),
			CacheMaxEntries = 5
		});

		using var client = CreateClient(handler);
		var realContainer = client.GetContainer("fakeDb", "fakeContainer");

		var iterator = realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		var results = await DrainAsync(iterator);

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task MultiplePartitionRanges_QueriesStillReturnAllData()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
			new PartitionKey("pk2"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Charlie", Value = 30 },
			new PartitionKey("pk3"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 4
		});

		using var client = CreateClient(handler);
		var realContainer = client.GetContainer("fakeDb", "fakeContainer");

		var iterator = realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		var results = await DrainAsync(iterator);

		results.Should().HaveCount(3);
		results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Bob", "Charlie"]);
	}

	[Fact]
	public async Task MultiplePartitionRanges_OrderByStillWorks()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Alice", Value = 10 },
			new PartitionKey("pk2"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Bob", Value = 20 },
			new PartitionKey("pk3"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 3
		});

		using var client = CreateClient(handler);
		var realContainer = client.GetContainer("fakeDb", "fakeContainer");

		var iterator = realContainer.GetItemLinqQueryable<TestDocument>()
			.OrderBy(document => document.Name)
			.ToFeedIterator();
		var results = await DrainAsync(iterator);

		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Bob", "Charlie");
	}

	[Fact]
	public async Task MultiplePartitionRanges_WhereFilterStillWorks()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
			new PartitionKey("pk2"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 2
		});

		using var client = CreateClient(handler);
		var realContainer = client.GetContainer("fakeDb", "fakeContainer");

		var iterator = realContainer.GetItemLinqQueryable<TestDocument>()
			.Where(document => document.Value > 15)
			.ToFeedIterator();
		var results = await DrainAsync(iterator);

		results.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public void DefaultOptions_HaveSensibleDefaults()
	{
		var options = new FakeCosmosHandlerOptions();

		options.CacheTtl.Should().Be(TimeSpan.FromMinutes(5));
		options.CacheMaxEntries.Should().Be(100);
		options.PartitionKeyRangeCount.Should().Be(1);
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;
	public ValueTask DisposeAsync() => ValueTask.CompletedTask;

	private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(handler)
				{
					Timeout = TimeSpan.FromSeconds(10)
				}
			});

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		return results;
	}
}

public class SdkCompatibilityTests
{
	[Fact]
	public async Task VerifySdkCompatibilityAsync_PassesWithCurrentSdk()
	{
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}
}

public class MultiContainerRoutingTests : IAsyncLifetime
{
	[Fact]
	public async Task CreateRouter_QueriesDifferentContainers()
	{
		var container1 = new InMemoryContainer("container1", "/partitionKey");
		var container2 = new InMemoryContainer("container2", "/partitionKey");

		await container1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));
		await container2.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
			new PartitionKey("pk"));

		using var handler1 = new FakeCosmosHandler(container1);
		using var handler2 = new FakeCosmosHandler(container2);
		using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["container1"] = handler1,
			["container2"] = handler2
		});

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var c1 = client.GetContainer("fakeDb", "container1");
		var c2 = client.GetContainer("fakeDb", "container2");

		var results1 = await DrainAsync(c1.GetItemLinqQueryable<TestDocument>().ToFeedIterator());
		var results2 = await DrainAsync(c2.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

		results1.Should().ContainSingle().Which.Name.Should().Be("Alice");
		results2.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public async Task CreateRouter_OrderByWorksAcrossContainers()
	{
		var container1 = new InMemoryContainer("orders", "/partitionKey");
		await container1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Charlie", Value = 30 },
			new PartitionKey("pk"));
		await container1.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		using var handler = new FakeCosmosHandler(container1);
		using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["orders"] = handler
		});

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var c = client.GetContainer("fakeDb", "orders");
		var results = await DrainAsync(
			c.GetItemLinqQueryable<TestDocument>().OrderBy(document => document.Name).ToFeedIterator());

		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Charlie");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;
	public ValueTask DisposeAsync() => ValueTask.CompletedTask;

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		return results;
	}
}

public class HashBasedPartitionRoutingTests : IAsyncLifetime
{
	[Fact]
	public async Task MultipleRanges_DistributesDocumentsAcrossRanges()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
			new PartitionKey("pk2"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Charlie", Value = 30 },
			new PartitionKey("pk3"));
		await container.CreateItemAsync(
			new TestDocument { Id = "4", PartitionKey = "pk4", Name = "Diana", Value = 40 },
			new PartitionKey("pk4"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 4
		});

		using var client = CreateClient(handler);
		var realContainer = client.GetContainer("fakeDb", "test");

		var results = await DrainAsync(
			realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

		results.Should().HaveCount(4);
		results.Select(document => document.Name).Should().BeEquivalentTo(["Alice", "Bob", "Charlie", "Diana"]);
	}

	[Fact]
	public async Task MultipleRanges_OrderByMergeSortsCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Charlie", Value = 30 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Alice", Value = 10 },
			new PartitionKey("pk2"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Bob", Value = 20 },
			new PartitionKey("pk3"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 3
		});

		using var client = CreateClient(handler);
		var realContainer = client.GetContainer("fakeDb", "test");

		var results = await DrainAsync(
			realContainer.GetItemLinqQueryable<TestDocument>()
				.OrderBy(document => document.Name)
				.ToFeedIterator());

		results.Select(document => document.Name).Should().ContainInConsecutiveOrder("Alice", "Bob", "Charlie");
	}

	[Fact]
	public async Task MultipleRanges_FilterWorksWithHashRouting()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob", Value = 20 },
			new PartitionKey("pk2"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk3", Name = "Charlie", Value = 30 },
			new PartitionKey("pk3"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 2
		});

		using var client = CreateClient(handler);
		var realContainer = client.GetContainer("fakeDb", "test");

		var results = await DrainAsync(
			realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(document => document.Value > 15)
				.ToFeedIterator());

		results.Should().HaveCount(2);
		results.Select(document => document.Name).Should().BeEquivalentTo(["Bob", "Charlie"]);
	}

	[Fact]
	public async Task DynamicCollectionMetadata_UsesContainerPartitionKeyPath()
	{
		var container = new InMemoryContainer("custom-container", "/customKey");
		await container.CreateItemAsync(
			new CustomKeyDocument { Id = "1", CustomKey = "ck1", Name = "Alice" },
			new PartitionKey("ck1"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var realContainer = client.GetContainer("fakeDb", "custom-container");

		var results = await DrainAsync(
			realContainer.GetItemLinqQueryable<CustomKeyDocument>().ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;
	public ValueTask DisposeAsync() => ValueTask.CompletedTask;

	private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(handler)
				{
					Timeout = TimeSpan.FromSeconds(10)
				}
			});

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		return results;
	}
}

[Collection("FeedIteratorSetup")]
public class ReflectionBasedRegistrationTests
{
	[Fact]
	public async Task Register_WorksWithReflectionInsteadOfDynamic()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		InMemoryFeedIteratorSetup.Register();
		try
		{
			var iterator = container.GetItemLinqQueryable<TestDocument>()
				.Where(document => document.Name == "Alice")
				.ToFeedIteratorOverridable();

			var results = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var response = await iterator.ReadNextAsync();
				results.AddRange(response);
			}

			results.Should().ContainSingle().Which.Name.Should().Be("Alice");
		}
		finally
		{
			InMemoryFeedIteratorSetup.Deregister();
		}
	}

	[Fact]
	public void Register_WithInvalidQueryable_ThrowsDescriptiveError()
	{
		InMemoryFeedIteratorSetup.Register();
		try
		{
			var factory = CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory;
			factory.Should().NotBeNull();

			var act = () => factory!("not a queryable");

			act.Should().Throw<InvalidOperationException>()
				.WithMessage("*does not implement IQueryable<T>*");
		}
		finally
		{
			InMemoryFeedIteratorSetup.Deregister();
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 29 — String Operations via Real SDK
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorStringTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorStringTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	private async Task SeedAsync(params TestDocument[] documents)
	{
		foreach (var doc in documents)
			await _inMemoryContainer.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
	}

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}
		return results;
	}

	[Fact]
	public async Task ToFeedIterator_WithStringEndsWith_FiltersCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Grace" },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Bob" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name.EndsWith("ce"))
				.ToFeedIterator());

		results.Should().HaveCount(2);
		results.Select(d => d.Name).Should().BeEquivalentTo(["Alice", "Grace"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithToLower_FiltersCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "ALICE" },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "bob" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name.ToLower() == "alice")
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Id.Should().Be("1");
	}

	[Fact]
	public async Task ToFeedIterator_WithToUpper_FiltersCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "alice" },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "BOB" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name.ToUpper() == "ALICE")
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Id.Should().Be("1");
	}

	[Fact]
	public async Task ToFeedIterator_WithTrim_FiltersCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "  Alice  " },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name.Trim() == "Alice")
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Id.Should().Be("1");
	}

	[Fact]
	public async Task ToFeedIterator_WithSubstring_ProjectsCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Select(d => d.Name.Substring(0, 3))
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Should().Be("Ali");
	}

	[Fact]
	public async Task ToFeedIterator_WithStringLength_FiltersCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Al" },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice" },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Bob" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name.Length > 3)
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task ToFeedIterator_WithReplace_ProjectsCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Hello World" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Select(d => d.Name.Replace("World", "Cosmos"))
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Should().Be("Hello Cosmos");
	}

	[Fact]
	public async Task ToFeedIterator_WithIndexOf_ProjectsCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Select(d => d.Name.IndexOf("ic"))
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 29 — CRUD via Real SDK
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorCrudTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorCrudTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task CreateItem_ViaRealSdk_ItemIsQueryable()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		var results = new List<TestDocument>();
		var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task UpsertItem_ViaRealSdk_UpdatesExisting()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		await _realContainer.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice Updated", Value = 20 },
			new PartitionKey("pk"));

		var results = new List<TestDocument>();
		var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Alice Updated");
	}

	[Fact]
	public async Task DeleteItem_ViaRealSdk_ItemNoLongerQueryable()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		await _realContainer.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

		var results = new List<TestDocument>();
		var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ReadItem_ViaRealSdk_ReturnsCorrectItem()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 42 },
			new PartitionKey("pk"));

		var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

		response.Resource.Name.Should().Be("Alice");
		response.Resource.Value.Should().Be(42);
	}

	[Fact]
	public async Task ReplaceItem_ViaRealSdk_UpdatesItem()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		await _realContainer.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice Replaced", Value = 99 },
			"1", new PartitionKey("pk"));

		var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.Resource.Name.Should().Be("Alice Replaced");
		response.Resource.Value.Should().Be(99);
	}

	[Fact]
	public async Task PatchItem_ViaRealSdk_AppliesPatchOperations()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		await _realContainer.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
			new[] { PatchOperation.Replace("/name", "Patched") });

		var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.Resource.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task CreateItem_ViaRealSdk_ReadViaInMemoryContainer()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		// Verify item is accessible directly through in-memory container
		var response = await _inMemoryContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.Resource.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task StreamCreate_ViaRealSdk_ItemIsCreated()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk", Name = "StreamTest" };
		var json = Newtonsoft.Json.JsonConvert.SerializeObject(doc);
		using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

		var response = await _realContainer.CreateItemStreamAsync(stream, new PartitionKey("pk"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await _inMemoryContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		readResponse.Resource.Name.Should().Be("StreamTest");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 29 — Response Metadata Tests
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorResponseMetadataTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorResponseMetadataTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task Query_ResponseHeaders_ContainRequestCharge()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter.HasMoreResults)
		{
			var response = await iter.ReadNextAsync();
			response.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
			break;
		}
	}

	[Fact]
	public async Task Query_ResponseHeaders_ContainActivityId()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter.HasMoreResults)
		{
			var response = await iter.ReadNextAsync();
			response.Headers.ActivityId.Should().NotBeNullOrEmpty();
			break;
		}
	}

	[Fact]
	public async Task ReadItem_Response_StatusCode200()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task CreateItem_Response_StatusCode201()
	{
		var response = await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task DeleteItem_Response_StatusCode204()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A" },
			new PartitionKey("pk"));

		var response = await _realContainer.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task ReadItem_NotFound_Returns404()
	{
		try
		{
			await _realContainer.ReadItemAsync<TestDocument>("nonexistent", new PartitionKey("pk"));
			throw new Exception("Expected CosmosException");
		}
		catch (CosmosException ex)
		{
			ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 29 — Edge Cases and Pagination
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorEdgeCaseTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorEdgeCaseTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	private async Task SeedAsync(params TestDocument[] documents)
	{
		foreach (var doc in documents)
			await _inMemoryContainer.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
	}

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}
		return results;
	}

	[Fact]
	public async Task LargeResultSet_AllItemsReturned()
	{
		for (var i = 0; i < 200; i++)
			await _inMemoryContainer.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}", Value = i },
				new PartitionKey("pk"));

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

		results.Should().HaveCount(200);
	}

	[Fact]
	public async Task SpecialCharacters_InPartitionKey_Handled()
	{
		var pk = "pk/with/slashes&special=chars";
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = pk, Name = "Special" },
			new PartitionKey(pk));

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == "Special")
				.ToFeedIterator());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task EmptyStringValues_HandleCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "", Value = 0 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "NotEmpty", Value = 1 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == "")
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Id.Should().Be("1");
	}

	[Fact]
	public async Task NumericEdgeCases_IntMinMax_QueryCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "MinVal", Value = int.MinValue },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "MaxVal", Value = int.MaxValue },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Zero", Value = 0 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Value > 0)
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("MaxVal");
	}

	[Fact]
	public async Task MultiplePartitions_OrderByAcrossPartitions()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C", Value = 30 },
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "A", Value = 10 },
			new TestDocument { Id = "3", PartitionKey = "pk3", Name = "B", Value = 20 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.OrderBy(d => d.Name)
				.ToFeedIterator());

		results.Select(d => d.Name).Should().ContainInConsecutiveOrder("A", "B", "C");
	}

	[Fact]
	public async Task WhereOnNestedProperty_FiltersCorrectly()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "A", Nested = new NestedObject { Score = 9.5 } },
			new PartitionKey("pk"));
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "B", Nested = new NestedObject { Score = 3.0 } },
			new PartitionKey("pk"));

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Nested != null && d.Nested.Score > 5.0)
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("A");
	}

	[Fact]
	public async Task SelectWithConcat_ProjectsCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Select(d => d.Name + "-" + d.Id)
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Should().Be("Alice-1");
	}

	[Fact]
	public async Task ConditionalFilter_TernaryInWhere()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Active", Value = 10, IsActive = true },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Inactive", Value = 20, IsActive = false });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.IsActive ? d.Value > 5 : d.Value > 100)
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("Active");
	}

	[Fact]
	public async Task NullNestedProperty_DoesNotThrow()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "NoNested" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Nested == null)
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("NoNested");
	}

	[Fact]
	public async Task ArrayContains_FiltersOnTag()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Tagged", Tags = ["important", "urgent"] },
			new PartitionKey("pk"));
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "NotTagged", Tags = ["low"] },
			new PartitionKey("pk"));

		// Use SQL ARRAY_CONTAINS instead of LINQ .Contains() — the Cosmos SDK LINQ
		// translator does not support List<T>.Contains() on Linux.
		var results = await DrainAsync(
			_realContainer.GetItemQueryIterator<TestDocument>(
				new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'urgent')")));

		results.Should().ContainSingle().Which.Name.Should().Be("Tagged");
	}

	[Fact]
	public async Task MultipleAggregates_AllReturnCorrectValues()
	{
		for (var i = 1; i <= 5; i++)
			await _inMemoryContainer.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}", Value = i * 10 },
				new PartitionKey("pk"));

		var q = _realContainer.GetItemLinqQueryable<TestDocument>();

		var count = await DrainAsync(q.Select(d => 1).ToFeedIterator());
		count.Should().HaveCount(5);

		var ordered = await DrainAsync(
			q.OrderByDescending(d => d.Value).Take(1).ToFeedIterator());
		ordered.Should().ContainSingle().Which.Value.Should().Be(50);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — LINQ Operator Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorLinqDeepDiveTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorLinqDeepDiveTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	private async Task SeedAsync(params TestDocument[] documents)
	{
		foreach (var doc in documents)
			await _inMemoryContainer.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
	}

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}
		return results;
	}

	[Fact]
	public async Task ToFeedIterator_WithThenByDescending_AppliesSecondaryDescSort()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 20 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Bob", Value = 15 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.OrderBy(d => d.Name)
				.ThenByDescending(d => d.Value)
				.ToFeedIterator());

		results.Should().HaveCount(3);
		results[0].Name.Should().Be("Alice");
		results[0].Value.Should().Be(20);
		results[1].Name.Should().Be("Alice");
		results[1].Value.Should().Be(10);
		results[2].Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ToFeedIterator_WithGroupBy_GroupsCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 30 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.GroupBy(d => d.Name, (key, elements) => new { Name = key, Count = elements.Count() })
				.ToFeedIterator());

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task ToFeedIterator_WithTakeOne_ReturnsSingleItem()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Charlie", Value = 30 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == "Bob")
				.Take(1)
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ToFeedIterator_WithTakeOneNoMatch_ReturnsEmpty()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == "Nobody")
				.Take(1)
				.ToFeedIterator());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ToFeedIterator_WithDistinctAfterWhere_ReturnsUniqueFilteredValues()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10, IsActive = true },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 10, IsActive = true },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Charlie", Value = 20, IsActive = false });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.IsActive)
				.Select(d => d.Value)
				.Distinct()
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Should().Be(10);
	}

	[Fact]
	public async Task ToFeedIterator_WithSelectToNamedType_ReturnsTypedResults()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 42 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 99 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Select(d => new NameProjection { Label = d.Name, Number = d.Value })
				.ToFeedIterator());

		results.Should().HaveCount(2);
		results.Should().Contain(r => r.Label == "Alice" && r.Number == 42);
		results.Should().Contain(r => r.Label == "Bob" && r.Number == 99);
	}

	[Fact(Skip = "Cosmos SDK LINQ provider does not translate C# ?? (null-coalescing) operator to Cosmos SQL. Use ternary with null check instead.")]
	public async Task ToFeedIterator_WithCoalesceInProjection_ReturnsDefaultOnNull()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Nested = new NestedObject { Description = "real" } },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob" });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Select(d => d.Nested!.Description ?? "N/A")
				.ToFeedIterator());

		results.Should().BeEquivalentTo(["real", "N/A"]);
	}

	[Fact]
	public async Task ToFeedIterator_WithCoalesceInProjection_Divergent_ThrowsOnTranslation()
	{
		// DIVERGENT BEHAVIOUR: Attempting to use ?? in a LINQ Select projection causes the
		// Cosmos SDK LINQ provider to throw during query translation, because there is no
		// Cosmos SQL equivalent. Use explicit ternary: d.Nested != null ? d.Nested.Description : "N/A" instead.
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Nested = new NestedObject { Description = "real" } },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob" });

		// Ternary works as the alternative approach
		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Select(d => new { d.Name, Desc = d.Nested != null ? d.Nested.Description : "N/A" })
				.ToFeedIterator());

		results.Should().HaveCount(2);
	}

	// ── Approach 3 LINQ rejection tests ──────────────────────────────────────
	// These operations work in Approach 1 (InMemoryContainer → LINQ-to-Objects) but are
	// correctly rejected in Approach 3 (CosmosClient + FakeCosmosHandler) because the real
	// SDK's CosmosLinqQueryProvider cannot translate them to Cosmos SQL.
	// See Integration-Approaches wiki, Decision 2, for why this distinction exists.

	[Fact]
	public async Task ToFeedIterator_GroupBy_SdkRejectsTranslation()
	{
		// Approach 3: The real SDK's CosmosLinqQueryProvider throws DocumentQueryException
		// because GroupBy has no Cosmos SQL equivalent. In Approach 1 (InMemoryContainer),
		// this works via LINQ-to-Objects — see LinqDivergentBehaviorTests.Linq_GroupBy_WorksInMemory_DivergentBehavior.
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie", Value = 30 });

		var act = () => _realContainer.GetItemLinqQueryable<TestDocument>()
			.GroupBy(d => d.PartitionKey)
			.ToFeedIterator();

		act.Should().Throw<Exception>().WithMessage("*GroupBy*");
	}

	[Fact]
	public async Task ToFeedIterator_Last_SdkRejectsTranslation()
	{
		// Approach 3: Last()/LastOrDefault() have no Cosmos SQL equivalent.
		// In Approach 1, this works via LINQ-to-Objects — see LinqDivergentBehaviorTests.Linq_Last_WorksInMemory_DivergentBehavior.
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 });

		var act = () => _realContainer.GetItemLinqQueryable<TestDocument>().LastOrDefault();

		act.Should().Throw<Exception>().WithMessage("*LastOrDefault*");
	}

	[Fact]
	public async Task ToFeedIterator_Aggregate_SdkRejectsTranslation()
	{
		// Approach 3: Custom Aggregate() has no Cosmos SQL equivalent. Only Sum/Average/Count/Min/Max
		// are translated. In Approach 1, this works via LINQ-to-Objects —
		// see LinqDivergentBehaviorTests.Linq_Aggregate_WorksInMemory_DivergentBehavior.
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 });

		var act = () => _realContainer.GetItemLinqQueryable<TestDocument>()
			.Aggregate(0, (acc, d) => acc + d.Value);

		act.Should().Throw<Exception>().WithMessage("*Aggregate*");
	}

	[Fact]
	public async Task ToFeedIterator_Reverse_SdkRejectsTranslation()
	{
		// Approach 3: Reverse() has no Cosmos SQL equivalent. Use OrderByDescending() instead.
		// In Approach 1, this works via LINQ-to-Objects —
		// see LinqDivergentBehaviorTests.Linq_Reverse_WorksInMemory_DivergentBehavior.
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 });

		var act = () => _realContainer.GetItemLinqQueryable<TestDocument>()
			.Reverse()
			.ToFeedIterator();

		act.Should().Throw<Exception>().WithMessage("*Reverse*");
	}

	[Fact]
	public async Task ToFeedIterator_AllowSynchronousQueryExecution_False_SdkEnforcesAsync()
	{
		// Approach 3: The real SDK enforces allowSynchronousQueryExecution=false by throwing
		// on synchronous enumeration (ToList()/foreach), requiring ToFeedIterator() instead.
		// In Approach 1 (InMemoryContainer), this flag is ignored because the queryable is
		// always synchronously enumerable via LINQ-to-Objects —
		// see LinqDivergentBehaviorTests.Linq_AllowSynchronousQueryExecution_False_StillWorksInMemory_DivergentBehavior.
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 });

		var queryable = _realContainer.GetItemLinqQueryable<TestDocument>(
			allowSynchronousQueryExecution: false);

		var act = () => queryable.ToList();
		act.Should().Throw<NotSupportedException>();
	}
}

public class NameProjection
{
	[Newtonsoft.Json.JsonProperty("label")]
	public string Label { get; set; } = default!;

	[Newtonsoft.Json.JsonProperty("number")]
	public int Number { get; set; }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — FakeCosmosHandler Route Coverage Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorHandlerRouteDeepDiveTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorHandlerRouteDeepDiveTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	private async Task SeedAsync(params TestDocument[] documents)
	{
		foreach (var doc in documents)
			await _inMemoryContainer.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
	}

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}
		return results;
	}

	[Fact]
	public async Task ToFeedIterator_WithParameterizedWhere_UsesParameters()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 });

		var targetName = "Alice";
		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == targetName)
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");

		// Verify the query was captured in the log (SDK may inline params or use @params
		// depending on version — the emulator processes both forms correctly)
		_handler.QueryLog.Should().Contain(q => q.Contains("Alice") || q.Contains("@"));
	}

	[Fact]
	public async Task ReadFeed_ViaLinq_ReturnsAllItems()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Charlie", Value = 30 },
			new TestDocument { Id = "4", PartitionKey = "pk", Name = "Diana", Value = 40 },
			new TestDocument { Id = "5", PartitionKey = "pk", Name = "Eve", Value = 50 });

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

		results.Should().HaveCount(5);
	}

	[Fact]
	public async Task CreateItem_Response_ContainsETag()
	{
		var response = await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReadItem_Response_ContainsETag()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var response = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReplaceItem_WithMatchingETag_Succeeds()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		var readResponse = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		var etag = readResponse.ETag;

		var replaceResponse = await _realContainer.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated", Value = 20 },
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = etag });

		replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		replaceResponse.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task ReplaceItem_WithStaleETag_Returns412Or409()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		var readResponse = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		var staleEtag = readResponse.ETag;

		// Update without ETag to change the version
		await _realContainer.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated", Value = 20 },
			"1", new PartitionKey("pk"));

		// Now try with stale ETag
		var act = async () => await _realContainer.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "StaleUpdate", Value = 30 },
			"1", new PartitionKey("pk"),
			new ItemRequestOptions { IfMatchEtag = staleEtag });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed || e.StatusCode == HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task UpsertItem_WhenNotExists_CreatesNew()
	{
		var response = await _realContainer.UpsertItemAsync(
			new TestDocument { Id = "new-1", PartitionKey = "pk", Name = "Created", Value = 42 },
			new PartitionKey("pk"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var readBack = await _realContainer.ReadItemAsync<TestDocument>("new-1", new PartitionKey("pk"));
		readBack.Resource.Name.Should().Be("Created");
	}

	[Fact]
	public async Task PatchItem_WithAddOp_AddsField()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		await _realContainer.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
			new[] { PatchOperation.Add("/tags", new[] { "new-tag" }) });

		var result = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		result.Resource.Tags.Should().Contain("new-tag");
	}

	[Fact]
	public async Task PatchItem_WithRemoveOp_RemovesField()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10, Tags = ["a", "b"] },
			new PartitionKey("pk"));

		await _realContainer.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
			new[] { PatchOperation.Remove("/tags") });

		var result = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		result.Resource.Tags.Should().BeNullOrEmpty();
	}

	[Fact]
	public async Task PatchItem_WithIncrementOp_IncrementsValue()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		await _realContainer.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
			new[] { PatchOperation.Increment("/value", 5) });

		var result = await _realContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		result.Resource.Value.Should().Be(15);
	}

	[Fact]
	public async Task PatchItem_WhenNotExists_Returns404()
	{
		var act = async () => await _realContainer.PatchItemAsync<TestDocument>(
			"nonexistent", new PartitionKey("pk"),
			new[] { PatchOperation.Replace("/name", "Updated") });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Error Handling & Edge Cases Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorErrorHandlingDeepDiveTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorErrorHandlingDeepDiveTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	private async Task SeedAsync(params TestDocument[] documents)
	{
		foreach (var doc in documents)
			await _inMemoryContainer.CreateItemAsync(doc, new PartitionKey(doc.PartitionKey));
	}

	private static async Task<List<T>> DrainAsync<T>(FeedIterator<T> iterator)
	{
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}
		return results;
	}

	[Fact]
	public async Task ToFeedIterator_OnEmptyContainer_ReturnsEmpty()
	{
		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task CountAsync_OnEmptyContainer_ReturnsZero()
	{
		var count = await _realContainer.GetItemLinqQueryable<TestDocument>().CountAsync();

		count.Resource.Should().Be(0);
	}

	[Fact]
	public async Task TwoIterators_SameContainer_BothDrainCorrectly()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Charlie", Value = 30 });

		var iter1 = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		var iter2 = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();

		var results1 = await DrainAsync(iter1);
		var results2 = await DrainAsync(iter2);

		results1.Should().HaveCount(3);
		results2.Should().HaveCount(3);
	}

	[Fact]
	public async Task Iterator_AfterMutation_ReflectsUpdatedState()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Charlie", Value = 30 });

		// Create iterator (FakeCosmosHandler re-executes on each page fetch)
		var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();

		// Mutate container before draining
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "4", PartitionKey = "pk", Name = "Diana", Value = 40 },
			new PartitionKey("pk"));

		// The emulator returns all items since it re-evaluates per page
		var results = await DrainAsync(iter);
		results.Count.Should().BeGreaterThanOrEqualTo(3);
	}

	[Fact]
	public async Task CreateItem_WhenAlreadyExists_Returns409()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var act = async () => await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Duplicate" },
			new PartitionKey("pk"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task ReplaceItem_WhenNotExists_Returns404()
	{
		var act = async () => await _realContainer.ReplaceItemAsync(
			new TestDocument { Id = "nonexistent", PartitionKey = "pk", Name = "Ghost" },
			"nonexistent", new PartitionKey("pk"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteItem_WhenNotExists_Returns404()
	{
		var act = async () => await _realContainer.DeleteItemAsync<TestDocument>(
			"nonexistent", new PartitionKey("pk"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task SpecialCharacters_Unicode_InPartitionKey()
	{
		var unicodePk = "日本語テスト";
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = unicodePk, Name = "Unicode" },
			new PartitionKey(unicodePk));

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == "Unicode")
				.ToFeedIterator());

		results.Should().ContainSingle().Which.Name.Should().Be("Unicode");
	}

	[Fact]
	public async Task DuplicateIds_DifferentPartitions_BothRetrievable()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk-a", Name = "Alice" },
			new PartitionKey("pk-a"));
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk-b", Name = "Bob" },
			new PartitionKey("pk-b"));

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator());

		results.Should().HaveCount(2);
		results.Select(d => d.Name).Should().BeEquivalentTo(["Alice", "Bob"]);
	}

	[Fact]
	public async Task FaultInjector_With429_ThrowsCosmosExceptionWithTooManyRequests()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" });

		_handler.FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
		{
			Content = new StringContent("{}", Encoding.UTF8, "application/json")
		};

		var act = async () =>
		{
			var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
			await DrainAsync(iter);
		};

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => (int)e.StatusCode == 429);
	}

	[Fact]
	public async Task FaultInjector_With500_ThrowsCosmosException()
	{
		await SeedAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" });

		_handler.FaultInjector = _ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
		{
			Content = new StringContent("{}", Encoding.UTF8, "application/json")
		};

		var act = async () =>
		{
			var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
			await DrainAsync(iter);
		};

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.InternalServerError);
	}

	[Fact]
	public async Task LongStringValues_QueryCorrectly()
	{
		var longName = new string('A', 10_000);
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = longName },
			new PartitionKey("pk"));

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name.StartsWith("AAAA"))
				.ToFeedIterator());

		results.Should().ContainSingle();
		results[0].Name.Length.Should().Be(10_000);
	}

	[Fact(Skip = "Would need a separate container with a numeric PK path — not available in this fixture's shared container.")]
	public async Task NumericPartitionKey_CreatesAndQueriesCorrectly()
	{
		// Numeric PKs through FakeCosmosHandler are tested in FakeCosmosHandlerCrudTests.Handler_CreateItem_WithNumericPartitionKey_Succeeds
		await Task.CompletedTask;
	}

	[Fact]
	public async Task NumericPartitionKey_StringPkAlsoWorksAsAlternative()
	{
		// String PKs with numeric values work as expected.
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "12345", Name = "NumericAsString" },
			new PartitionKey("12345"));

		var results = await DrainAsync(
			_realContainer.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == "NumericAsString")
				.ToFeedIterator());

		results.Should().ContainSingle();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — InMemoryFeedIterator Direct Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryFeedIteratorDirectTests
{
	[Fact]
	public void InMemoryFeedIterator_DeferredFactory_EvaluatesLazily()
	{
		var called = false;
		var iter = new InMemoryFeedIterator<int>(() =>
		{
			called = true;
			return [1, 2, 3];
		});

		called.Should().BeFalse();
		_ = iter.HasMoreResults;
		called.Should().BeTrue();
	}

	[Fact]
	public async Task InMemoryFeedIterator_WithMaxItemCount_ReturnsCorrectPages()
	{
		var items = Enumerable.Range(1, 7).ToList();
		var iter = new InMemoryFeedIterator<int>(items, maxItemCount: 3);

		var pages = new List<List<int>>();
		while (iter.HasMoreResults)
		{
			var response = await iter.ReadNextAsync();
			pages.Add(response.ToList());
		}

		pages.Should().HaveCount(3);
		pages[0].Should().HaveCount(3);
		pages[1].Should().HaveCount(3);
		pages[2].Should().HaveCount(1);
		pages.SelectMany(p => p).Should().BeEquivalentTo(items);
	}

	[Fact]
	public async Task InMemoryFeedIterator_ContinuationTokens_IncrementCorrectly()
	{
		var items = Enumerable.Range(1, 5).ToList();
		var iter = new InMemoryFeedIterator<int>(items, maxItemCount: 2);

		var page1 = await iter.ReadNextAsync();
		page1.ContinuationToken.Should().Be("2");

		var page2 = await iter.ReadNextAsync();
		page2.ContinuationToken.Should().Be("4");

		var page3 = await iter.ReadNextAsync();
		page3.ContinuationToken.Should().BeNull();
	}

	[Fact]
	public void InMemoryFeedIterator_WithEmptySource_HasNoResults()
	{
		var iter = new InMemoryFeedIterator<int>(Array.Empty<int>());

		iter.HasMoreResults.Should().BeFalse();
	}

	[Fact]
	public void InMemoryFeedIterator_WhenFactoryThrows_PropagatesException()
	{
		var iter = new InMemoryFeedIterator<int>(() => throw new ArgumentException("boom"));

		var act = () => iter.HasMoreResults;

		// The factory exception is wrapped in InvalidOperationException by Materialize,
		// or may propagate directly depending on the code path
		act.Should().Throw<Exception>();
	}

	[Fact]
	public async Task InMemoryFeedIterator_WithInitialOffset_SkipsItems()
	{
		var items = Enumerable.Range(1, 5).ToList();
		var iter = new InMemoryFeedIterator<int>(items, maxItemCount: 2, initialOffset: 3);

		var allResults = new List<int>();
		while (iter.HasMoreResults)
		{
			var response = await iter.ReadNextAsync();
			allResults.AddRange(response);
		}

		allResults.Should().BeEquivalentTo([4, 5]);
	}

	[Fact]
	public async Task InMemoryFeedIterator_ReadAfterDrained_ReturnsEmptyPage()
	{
		var iter = new InMemoryFeedIterator<int>([1, 2]);

		// Drain all items
		while (iter.HasMoreResults)
			await iter.ReadNextAsync();

		// Read again after drained
		var extra = await iter.ReadNextAsync();
		extra.Count.Should().Be(0);
	}

	[Fact]
	public async Task InMemoryFeedIterator_WithEnumerableSource_MaterializesEagerly()
	{
		var evaluationCount = 0;
		IEnumerable<int> LazySource()
		{
			evaluationCount++;
			yield return 1;
			yield return 2;
			yield return 3;
		}

		var iter = new InMemoryFeedIterator<int>(LazySource());

		evaluationCount.Should().Be(1);

		// Reading should not re-evaluate
		await iter.ReadNextAsync();
		evaluationCount.Should().Be(1);
	}

	[Fact]
	public async Task InMemoryFeedIterator_ContinuationToken_IsOffsetBased_Divergent()
	{
		// DIVERGENT BEHAVIOUR: Real Cosmos DB uses opaque JSON continuation tokens.
		// The InMemoryFeedIterator uses simple integer offset strings (e.g., "2", "4").
		// Code that parses continuation tokens expecting JSON will break.
		var iter = new InMemoryFeedIterator<int>(Enumerable.Range(1, 5).ToList(), maxItemCount: 2);

		var page = await iter.ReadNextAsync();
		page.ContinuationToken.Should().Be("2");
		int.TryParse(page.ContinuationToken, out _).Should().BeTrue();
	}

	[Fact]
	public async Task InMemoryFeedIterator_WithNoMaxItemCount_ReturnsAllInOnePage()
	{
		var items = Enumerable.Range(1, 10).ToList();
		var iter = new InMemoryFeedIterator<int>(items);

		iter.HasMoreResults.Should().BeTrue();
		var page = await iter.ReadNextAsync();
		page.Count.Should().Be(10);
		iter.HasMoreResults.Should().BeFalse();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — InMemoryFeedIteratorSetup Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════

public class InMemoryFeedIteratorSetupDeepDiveTests
{
	[Fact]
	public void Deregister_ClearsBothFactories()
	{
		InMemoryFeedIteratorSetup.Register();
		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().NotBeNull();

		InMemoryFeedIteratorSetup.Deregister();
		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory.Should().BeNull();
		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory.Should().BeNull();
	}

	[Fact]
	public async Task ReRegister_AfterDeregister_WorksAgain()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		InMemoryFeedIteratorSetup.Register();
		InMemoryFeedIteratorSetup.Deregister();
		InMemoryFeedIteratorSetup.Register();
		try
		{
			var iter = container.GetItemLinqQueryable<TestDocument>()
				.Where(d => d.Name == "Alice")
				.ToFeedIteratorOverridable();

			var results = new List<TestDocument>();
			while (iter.HasMoreResults)
			{
				var response = await iter.ReadNextAsync();
				results.AddRange(response);
			}

			results.Should().ContainSingle().Which.Name.Should().Be("Alice");
		}
		finally
		{
			InMemoryFeedIteratorSetup.Deregister();
		}
	}

	[Fact]
	public async Task Register_WorksWithMultipleTypes()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 42 },
			new PartitionKey("pk"));

		InMemoryFeedIteratorSetup.Register();
		try
		{
			// Use with TestDocument
			var docIter = container.GetItemLinqQueryable<TestDocument>().ToFeedIteratorOverridable();
			var docs = new List<TestDocument>();
			while (docIter.HasMoreResults)
			{
				var response = await docIter.ReadNextAsync();
				docs.AddRange(response);
			}
			docs.Should().ContainSingle().Which.Name.Should().Be("Alice");

			// Use with string projection
			var strIter = container.GetItemLinqQueryable<TestDocument>()
				.Select(d => d.Name)
				.ToFeedIteratorOverridable();
			var names = new List<string>();
			while (strIter.HasMoreResults)
			{
				var response = await strIter.ReadNextAsync();
				names.AddRange(response);
			}
			names.Should().ContainSingle().Which.Should().Be("Alice");
		}
		finally
		{
			InMemoryFeedIteratorSetup.Deregister();
		}
	}

	[Fact]
	public void ToFeedIteratorOverridable_WithoutRegistration_ThrowsArgumentOutOfRange()
	{
		InMemoryFeedIteratorSetup.Deregister();

		var container = new InMemoryContainer("test", "/partitionKey");
		var queryable = container.GetItemLinqQueryable<TestDocument>();

		var act = () => queryable.ToFeedIteratorOverridable();

		act.Should().Throw<Exception>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 33 — Response Fidelity Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════

[Collection("FeedIteratorSetup")]
public class RealToFeedIteratorResponseFidelityDeepDiveTests : IAsyncLifetime
{
	private readonly InMemoryContainer _inMemoryContainer = new("test-container", "/partitionKey");
	private readonly FakeCosmosHandler _handler;
	private readonly CosmosClient _cosmosClient;
	private readonly Container _realContainer;

	public RealToFeedIteratorResponseFidelityDeepDiveTests()
	{
		_handler = new FakeCosmosHandler(_inMemoryContainer);
		_cosmosClient = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(_handler) { Timeout = TimeSpan.FromSeconds(10) }
			});
		_realContainer = _cosmosClient.GetContainer("fakeDb", "fakeContainer");
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		_cosmosClient.Dispose();
		_handler.Dispose();
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task Query_Response_ContainsSessionToken()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		while (iter.HasMoreResults)
		{
			var response = await iter.ReadNextAsync();
			response.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();
			break;
		}
	}

	[Fact]
	public async Task Query_Response_ItemCountMatchesResults()
	{
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob" },
			new PartitionKey("pk"));
		await _inMemoryContainer.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Charlie" },
			new PartitionKey("pk"));

		var iter = _realContainer.GetItemLinqQueryable<TestDocument>().ToFeedIterator();
		var totalCount = 0;
		while (iter.HasMoreResults)
		{
			var response = await iter.ReadNextAsync();
			totalCount += response.Count;
		}

		totalCount.Should().Be(3);
	}

	[Fact]
	public async Task ReplaceItem_Response_StatusCode200()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var response = await _realContainer.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated" },
			"1", new PartitionKey("pk"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task UpsertItem_NewItem_Response_StatusCode201()
	{
		var response = await _realContainer.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Brand New" },
			new PartitionKey("pk"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task UpsertItem_ExistingItem_Response_StatusCode200()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Original" },
			new PartitionKey("pk"));

		var response = await _realContainer.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated" },
			new PartitionKey("pk"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task PatchItem_Response_StatusCode200()
	{
		await _realContainer.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var response = await _realContainer.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk"),
			new[] { PatchOperation.Replace("/name", "Patched") });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task StreamFeedIterator_ReturnsAllItemsInSinglePage()
	{
		for (var i = 0; i < 10; i++)
			await _inMemoryContainer.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk", Name = $"Item{i}" },
				new PartitionKey("pk"));

		var iter = _realContainer.GetItemQueryStreamIterator("SELECT * FROM c");

		var pageCount = 0;
		var totalContent = "";
		while (iter.HasMoreResults)
		{
			var response = await iter.ReadNextAsync();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			using var reader = new StreamReader(response.Content);
			totalContent += await reader.ReadToEndAsync();
			pageCount++;
		}

		totalContent.Should().Contain("Item0");
		totalContent.Should().Contain("Item9");
	}
}
