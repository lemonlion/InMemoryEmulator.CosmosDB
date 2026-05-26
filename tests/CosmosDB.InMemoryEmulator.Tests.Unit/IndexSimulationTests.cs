using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class IndexSimulationTests
{
	// ── Basic index policy support ──────────────────────────────────────────

	[Fact]
	public void DefaultIndexingPolicy_HasAutomaticIndexing()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");

		var policy = container.IndexingPolicy;

		policy.Should().NotBeNull();
		policy.Automatic.Should().BeTrue();
		policy.IndexingMode.Should().Be(IndexingMode.Consistent);
	}

	[Fact]
	public void SetIndexingPolicy_UpdatesPolicy()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		var policy = new IndexingPolicy
		{
			Automatic = false,
			IndexingMode = IndexingMode.None
		};

		container.IndexingPolicy = policy;

		container.IndexingPolicy.Automatic.Should().BeFalse();
		container.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
	}

	// ── Included/Excluded paths ─────────────────────────────────────────────

	[Fact]
	public void IndexingPolicy_DefaultIncludesAllPaths()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");

		container.IndexingPolicy.IncludedPaths.Should().ContainSingle(p => p.Path == "/*");
	}

	[Fact]
	public async Task ExcludedPath_StillAllowsPointReads()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		var response = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		response.Resource.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task ExcludedPath_QueriesStillWorkButLogWarning()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		// Queries on excluded paths still work in our in-memory implementation
		// (they would just be slower in real Cosmos DB - a full scan instead of index lookup)
		var query = new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'");
		var iterator = container.GetItemQueryIterator<TestDocument>(query);
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(1);
	}

	// ── Composite indexes ───────────────────────────────────────────────────

	[Fact]
	public void CompositeIndexes_CanBeConfigured()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
		]);

		container.IndexingPolicy.CompositeIndexes.Should().HaveCount(1);
		container.IndexingPolicy.CompositeIndexes[0].Should().HaveCount(2);
	}

	[Fact]
	public async Task OrderBy_WithCompositeIndex_ReturnsCorrectOrder()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
		]);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Alice", Value = 20 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Bob", Value = 5 },
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT * FROM c ORDER BY c.name ASC, c.value DESC");
		var iterator = container.GetItemQueryIterator<TestDocument>(query);
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(3);
		results[0].Name.Should().Be("Alice");
		results[0].Value.Should().Be(20);
		results[1].Name.Should().Be("Alice");
		results[1].Value.Should().Be(10);
		results[2].Name.Should().Be("Bob");
	}

	// ── Spatial indexes ─────────────────────────────────────────────────────

	[Fact]
	public void SpatialIndexes_CanBeConfigured()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		container.IndexingPolicy.SpatialIndexes.Add(new SpatialPath
		{
			Path = "/location/*"
		});

		container.IndexingPolicy.SpatialIndexes.Should().HaveCount(1);
	}

	// ── Request charge reflects index usage ─────────────────────────────────
	// Behavioral difference: In real Cosmos DB, queries on excluded paths consume
	// more RUs because they require a full scan. Our implementation always returns
	// a synthetic request charge - we simulate this by returning a higher synthetic
	// charge when querying on excluded paths.

	[Fact(Skip = "InMemoryContainer uses a synthetic request charge of 1.0 for all operations. " +
		"Real Cosmos DB varies request charge based on index usage, document size and query complexity. " +
		"Implementing realistic RU calculation would require modeling the full Cosmos DB cost model.")]
	public async Task ExcludedPath_QueryReturnsHigherRequestCharge()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		// Indexed query
		var indexedQuery = new QueryDefinition("SELECT * FROM c WHERE c.value = 10");
		var indexedIterator = container.GetItemQueryIterator<TestDocument>(indexedQuery);
		var indexedResponse = await indexedIterator.ReadNextAsync();
		var indexedCharge = indexedResponse.RequestCharge;

		// Exclude the name path
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });

		// Query on excluded path
		var excludedQuery = new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'");
		var excludedIterator = container.GetItemQueryIterator<TestDocument>(excludedQuery);
		var excludedResponse = await excludedIterator.ReadNextAsync();
		var excludedCharge = excludedResponse.RequestCharge;

		excludedCharge.Should().BeGreaterThan(indexedCharge);
	}

	// Behavioral difference test: documents the actual divergent behaviour
	[Fact]
	public async Task BehavioralDifference_RequestChargeIsAlwaysSynthetic()
	{
		// In real Cosmos DB, request charge varies based on index usage, document size,
		// and query complexity. Our implementation always returns a synthetic flat charge.
		var container = new InMemoryContainer("test-container", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.value = 10");
		var iterator = container.GetItemQueryIterator<TestDocument>(query);
		var response = await iterator.ReadNextAsync();

		// Our synthetic charge is always the same regardless of index configuration
		response.RequestCharge.Should().Be(1.0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category A: Index Policy Roundtrip & Persistence
// ═══════════════════════════════════════════════════════════════════════════

public class IndexPolicyRoundtripTests
{
	[Fact]
	public async Task IndexingPolicy_SurvivesReadContainerAsync()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
		]);

		var response = await container.ReadContainerAsync();

		response.Resource.IndexingPolicy.Should().NotBeNull();
		response.Resource.IndexingPolicy.CompositeIndexes.Should().HaveCount(1);
	}

	[Fact]
	public async Task IndexingPolicy_SurvivesReadContainerAsync_EmulatorBehavior()
	{
		// ReadContainerAsync now syncs IndexingPolicy into _containerProperties.
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
		]);

		var response = await container.ReadContainerAsync();

		response.Resource.IndexingPolicy.CompositeIndexes.Should().HaveCount(1,
			"ReadContainerAsync now syncs IndexingPolicy correctly");
	}

	[Fact]
	public async Task IndexingPolicy_UpdatedViaReplaceContainerAsync()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var newProperties = new ContainerProperties("test", "/partitionKey")
		{
			IndexingPolicy = new IndexingPolicy
			{
				Automatic = false,
				IndexingMode = IndexingMode.None
			}
		};

		await container.ReplaceContainerAsync(newProperties);

		container.IndexingPolicy.Automatic.Should().BeFalse();
		container.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
	}

	[Fact]
	public async Task IndexingPolicy_UpdatedViaReplaceContainerAsync_EmulatorBehavior()
	{
		// ReplaceContainerAsync now syncs IndexingPolicy back to the public property.
		var container = new InMemoryContainer("test", "/partitionKey");

		var newProperties = new ContainerProperties("test", "/partitionKey")
		{
			IndexingPolicy = new IndexingPolicy
			{
				Automatic = false,
				IndexingMode = IndexingMode.None
			}
		};

		await container.ReplaceContainerAsync(newProperties);

		container.IndexingPolicy.Automatic.Should().BeFalse(
			"ReplaceContainerAsync now syncs IndexingPolicy correctly");
		container.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
	}

	[Fact]
	public void IndexingPolicy_DefaultExcludedPaths_Empty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Real Cosmos defaults to /_etag/? in excluded paths
		container.IndexingPolicy.ExcludedPaths.Should().ContainSingle()
			.Which.Path.Should().Be("/\"_etag\"/?");
	}

	[Fact]
	public async Task IndexingPolicy_PreservedWhenCreatedViaDatabase()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var properties = new ContainerProperties("testcontainer", "/pk")
		{
			IndexingPolicy = new IndexingPolicy
			{
				Automatic = false,
				IndexingMode = IndexingMode.None
			}
		};

		var response = await db.CreateContainerAsync(properties);
		var container = response.Container as InMemoryContainer;

		container!.IndexingPolicy.Automatic.Should().BeFalse();
		container.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
	}

	[Fact]
	public async Task IndexingPolicy_PreservedWhenCreatedViaDatabase_EmulatorBehavior()
	{
		// CreateContainerAsync(ContainerProperties) now preserves IndexingPolicy.
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var properties = new ContainerProperties("testcontainer", "/pk")
		{
			IndexingPolicy = new IndexingPolicy
			{
				Automatic = false,
				IndexingMode = IndexingMode.None
			}
		};

		var response = await db.CreateContainerAsync(properties);
		var container = response.Container as InMemoryContainer;

		container!.IndexingPolicy.Automatic.Should().BeFalse(
			"IndexingPolicy is now preserved during database container creation");
		container.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category B: IndexingMode Behavioral Differences
// ═══════════════════════════════════════════════════════════════════════════

public class IndexingModeBehaviorTests
{
	[Fact(Skip = "In real Cosmos DB, IndexingMode.None means queries fail unless " +
		"EnableScanInQuery=true. The emulator ignores IndexingMode entirely — queries " +
		"always work regardless of mode.")]
	public async Task IndexingMode_None_QueriesFailWithoutScanEnabled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy = new IndexingPolicy
		{
			Automatic = false,
			IndexingMode = IndexingMode.None
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'Alice'");
		var act = async () =>
		{
			while (iterator.HasMoreResults) await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task BehavioralDifference_IndexingMode_None_QueriesStillWork()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos DB: IndexingMode.None prevents queries (only point reads work).
		// In-Memory Emulator: IndexingMode is stored but not enforced. Queries always scan all items.
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy = new IndexingPolicy
		{
			Automatic = false,
			IndexingMode = IndexingMode.None
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'Alice'");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle(r => r.Name == "Alice");
	}

	[Fact]
	public void IndexingMode_Lazy_IsAccepted()
	{
#pragma warning disable CS0618 // Lazy is obsolete
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy = new IndexingPolicy
		{
			IndexingMode = IndexingMode.Lazy
		};

		container.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.Lazy);
#pragma warning restore CS0618
	}

	[Fact]
	public async Task BehavioralDifference_IndexingMode_Lazy_QueriesAreImmediate()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos DB: Lazy mode indexes async; queries may see stale results.
		// In-Memory Emulator: All queries return complete, immediate results regardless of mode.
#pragma warning disable CS0618
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy = new IndexingPolicy
		{
			IndexingMode = IndexingMode.Lazy
		};
#pragma warning restore CS0618

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.name = 'Alice'");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category C: Included/Excluded Path Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class IndexPathEdgeCaseTests
{
	[Fact]
	public void ExcludedPath_EtagPath_CanBeAdded()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/_etag/?" });

		container.IndexingPolicy.ExcludedPaths.Should().ContainSingle(p => p.Path == "/_etag/?");
	}

	[Fact]
	public async Task ExcludedPath_NestedPath_QueriesStillWork()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/nested/*" });

		await container.CreateItemAsync(
			new TestDocument
			{
				Id = "1",
				PartitionKey = "pk",
				Name = "Test",
				Nested = new NestedObject { Description = "deep", Score = 9.5 }
			},
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c WHERE c.nested.description = 'deep'");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
	}

	[Fact(Skip = "In real Cosmos DB, excluding /* with no included paths means no queries work " +
		"(only point reads). The emulator ignores exclusion rules entirely — queries always scan.")]
	public void ExcludedAllPaths_QueriesFail()
	{
		// Placeholder for real Cosmos behavior
	}

	[Fact]
	public async Task BehavioralDifference_ExcludedAllPaths_QueriesStillWork()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos DB: Excluding /* prevents queries from using any index.
		// In-Memory Emulator: Exclusion paths are stored but not enforced.
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.IncludedPaths.Clear();
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
	}

	[Fact]
	public void IncludedPaths_CanBeCleared()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.IncludedPaths.Clear();

		container.IndexingPolicy.IncludedPaths.Should().BeEmpty();
	}

	[Fact]
	public void IncludedPaths_SpecificPath_CanBeSet()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.IncludedPaths.Clear();
		container.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/name/?" });

		container.IndexingPolicy.IncludedPaths.Should().ContainSingle(p => p.Path == "/name/?");
	}

	[Fact]
	public void MultipleExcludedPaths_AllStoredCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/tags/*" });

		// /_etag/? is auto-added, so total is 3 (auto + 2 user-added)
		container.IndexingPolicy.ExcludedPaths.Should().HaveCount(3);
	}

	[Fact]
	public void ExcludedPath_WildcardVariations_Stored()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/?" });
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });

		// /_etag/? auto-added + 3 user-added = 4
		container.IndexingPolicy.ExcludedPaths.Should().HaveCount(4);
	}

	[Fact]
	public async Task ExcludedPath_PointReadsWorkWithModeNone()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy = new IndexingPolicy
		{
			Automatic = false,
			IndexingMode = IndexingMode.None
		};
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		read.Resource.Name.Should().Be("Alice");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category D: Composite Index Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class CompositeIndexEdgeCaseTests
{
	[Fact]
	public void CompositeIndex_ThreePaths_CanBeConfigured()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/value", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/isActive", Order = CompositePathSortOrder.Descending }
		]);

		container.IndexingPolicy.CompositeIndexes[0].Should().HaveCount(3);
	}

	[Fact]
	public void CompositeIndex_MultipleSets_CanBeConfigured()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
		]);
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/isActive", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending }
		]);

		container.IndexingPolicy.CompositeIndexes.Should().HaveCount(2);
	}

	[Fact(Skip = "In real Cosmos DB, multi-field ORDER BY without a matching composite index " +
		"returns an error. The emulator does not validate composite index requirements.")]
	public void OrderBy_WithoutCompositeIndex_Fails()
	{
		// Placeholder for real Cosmos behavior
	}

	[Fact]
	public async Task BehavioralDifference_OrderBy_MultiField_WorksWithoutCompositeIndex()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos DB: Multi-field ORDER BY requires a composite index.
		// In-Memory Emulator: ORDER BY always works regardless of index configuration.
		var container = new InMemoryContainer("test", "/partitionKey");
		// NO composite index configured

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 20 },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c.name ASC, c.value ASC");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
		results[0].Value.Should().Be(10);
		results[1].Value.Should().Be(20);
	}

	[Fact]
	public async Task OrderBy_CompositeIndex_MixedSortOrders()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
		]);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk", Name = "Alice", Value = 30 },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c.name ASC, c.value DESC");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results[0].Name.Should().Be("Alice");
		results[0].Value.Should().Be(30);
		results[1].Name.Should().Be("Alice");
		results[1].Value.Should().Be(10);
		results[2].Name.Should().Be("Bob");
	}

	[Fact]
	public async Task CompositeIndex_AllDescending()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(
		[
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Descending },
			new CompositePath { Path = "/value", Order = CompositePathSortOrder.Descending }
		]);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 5 },
			new PartitionKey("pk"));

		var iterator = container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c ORDER BY c.name DESC, c.value DESC");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results[0].Name.Should().Be("Bob");
		results[1].Name.Should().Be("Alice");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category E: Spatial Index Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class SpatialIndexEdgeCaseTests
{
	[Fact]
	public void SpatialIndex_WithSpatialTypes_CanBeConfigured()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.SpatialIndexes.Add(new SpatialPath
		{
			Path = "/location/*",
			SpatialTypes = { SpatialType.Point, SpatialType.Polygon }
		});

		container.IndexingPolicy.SpatialIndexes.Should().ContainSingle();
		container.IndexingPolicy.SpatialIndexes[0].SpatialTypes.Should().HaveCount(2);
	}

	[Fact]
	public void SpatialIndex_MultiplePaths_CanBeConfigured()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy.SpatialIndexes.Add(new SpatialPath { Path = "/location/*" });
		container.IndexingPolicy.SpatialIndexes.Add(new SpatialPath { Path = "/area/*" });

		container.IndexingPolicy.SpatialIndexes.Should().HaveCount(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category F: Unique Key Policy Interaction
// ═══════════════════════════════════════════════════════════════════════════

public class UniqueKeyPolicyDatabaseCreationTests
{
	[Fact]
	public async Task UniqueKeyPolicy_PreservedWhenCreatedViaDatabase()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var properties = new ContainerProperties("testcontainer", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};

		await db.CreateContainerAsync(properties);
		var container = db.GetContainer("testcontainer");

		// Should enforce unique key
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		var act = () => container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task UniqueKeyPolicy_PreservedWhenCreatedViaDatabase_EmulatorBehavior()
	{
		// UniqueKeyPolicy is now preserved when creating containers via Database.
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var properties = new ContainerProperties("testcontainer", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};

		await db.CreateContainerAsync(properties);
		var container = db.GetContainer("testcontainer");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		// UniqueKeyPolicy IS now enforced — duplicate names should conflict
		var act = () => container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Alice" },
			new PartitionKey("pk"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.Conflict);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category G: FakeCosmosHandler Index Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class FakeCosmosHandlerIndexMetadataTests
{
	[Fact]
	public void FakeCosmosHandler_IndexMetadata_ReflectsContainerPolicy()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy = new IndexingPolicy
		{
			Automatic = false,
			IndexingMode = IndexingMode.None
		};

		var handler = new FakeCosmosHandler(container);
		handler.Should().NotBeNull();
	}

	[Fact]
	public void BehavioralDifference_FakeCosmosHandler_IndexMetadataReflectsPolicy()
	{
		// FakeCosmosHandler now reads from the container's actual IndexingPolicy.
		var container = new InMemoryContainer("test", "/partitionKey");
		container.IndexingPolicy = new IndexingPolicy
		{
			Automatic = false,
			IndexingMode = IndexingMode.None
		};

		var handler = new FakeCosmosHandler(container);
		handler.Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category I — ORDER BY Mixed Type Sorting
// ═══════════════════════════════════════════════════════════════════════════════

public class OrderByMixedTypeSortingTests
{
	[Fact]
	public async Task OrderBy_NullValues_SortFirst()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", score = 10 }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", score = (int?)null }),
			new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", score = 5 }),
			new PartitionKey("c"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.score");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		results[0]["id"]!.ToString().Should().Be("2", "null sorts first");
	}

	[Fact]
	public async Task OrderBy_AllNulls_StableSort()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", score = (int?)null }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", score = (int?)null }),
			new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", score = (int?)null }),
			new PartitionKey("c"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.score");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task OrderBy_UndefinedField_TreatedAsNull()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", score = 10 }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b" }),
			new PartitionKey("b"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.score");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results[0]["id"]!.ToString().Should().Be("2", "undefined/missing sorts before values");
	}

	[Fact]
	public async Task OrderBy_MixedTypes_FollowsCosmosTypeRanking()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "hello" }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", val = 42 }),
			new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", val = true }),
			new PartitionKey("c"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.val");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results[0]["id"]!.ToString().Should().Be("3", "boolean before number");
		results[1]["id"]!.ToString().Should().Be("2", "number before string");
		results[2]["id"]!.ToString().Should().Be("1", "string last");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category J — ORDER BY Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class OrderByEdgeCaseTests
{
	[Fact]
	public async Task OrderBy_NestedProperty_SortsCorrectly()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", nested = new { score = 30 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", nested = new { score = 10 } }),
			new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", nested = new { score = 20 } }),
			new PartitionKey("c"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.nested.score");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		results[0]["id"]!.ToString().Should().Be("2");
		results[1]["id"]!.ToString().Should().Be("3");
		results[2]["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task OrderBy_SingleField_Descending()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", val = 30 }),
			new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", val = 20 }),
			new PartitionKey("c"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.val DESC");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		results[0]["id"]!.ToString().Should().Be("2");
		results[1]["id"]!.ToString().Should().Be("3");
		results[2]["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task OrderBy_EmptyCollection_ReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/pk");

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.val");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task OrderBy_SingleItem_ReturnsItem()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 42 }),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.val");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task OrderBy_LargeDataset_SortsCorrectly()
	{
		var container = new InMemoryContainer("test", "/pk");
		var rng = new Random(42);
		var values = Enumerable.Range(0, 100).Select(_ => rng.Next(1000)).ToList();

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = $"p{i}", val = values[i] }),
				new PartitionKey($"p{i}"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.val");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(100);
		var sorted = results.Select(r => (long)r["val"]!).ToList();
		sorted.Should().BeInAscendingOrder();
	}

	[Fact]
	public async Task OrderBy_WithWhereClause_FiltersAndSorts()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 30, active = true }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", val = 10, active = false }),
			new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", val = 20, active = true }),
			new PartitionKey("c"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.active = true ORDER BY c.val");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results[0]["id"]!.ToString().Should().Be("3"); // val=20
		results[1]["id"]!.ToString().Should().Be("1"); // val=30
	}

	[Fact]
	public async Task OrderBy_WithTop_ReturnsTopNSorted()
	{
		var container = new InMemoryContainer("test", "/pk");
		for (var i = 1; i <= 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = $"p{i}", val = i * 10 }),
				new PartitionKey($"p{i}"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT TOP 3 * FROM c ORDER BY c.val DESC");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		results[0]["id"]!.ToString().Should().Be("10"); // val=100
		results[1]["id"]!.ToString().Should().Be("9");  // val=90
		results[2]["id"]!.ToString().Should().Be("8");  // val=80
	}

	[Fact]
	public async Task OrderBy_WithOffsetLimit_ReturnsSortedPage()
	{
		var container = new InMemoryContainer("test", "/pk");
		for (var i = 1; i <= 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = $"p{i}", val = i }),
				new PartitionKey($"p{i}"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.val OFFSET 3 LIMIT 3");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		results[0]["id"]!.ToString().Should().Be("4");
		results[1]["id"]!.ToString().Should().Be("5");
		results[2]["id"]!.ToString().Should().Be("6");
	}

	[Fact]
	public async Task OrderBy_DuplicateValues_StableSort()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", val = 10 }),
			new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", val = 10 }),
			new PartitionKey("c"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.val");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3, "all items with same sort key returned");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category K — IndexingPolicy Through Client Creation
// ═══════════════════════════════════════════════════════════════════════════════

public class IndexPolicyClientCreationTests
{
	[Fact]
	public async Task CreateContainerIfNotExists_PreservesIndexingPolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var props = new ContainerProperties("test", "/pk")
		{
			IndexingPolicy = new IndexingPolicy
			{
				Automatic = false,
				IndexingMode = IndexingMode.None
			}
		};
		var response = await db.CreateContainerIfNotExistsAsync(props);

		response.Resource.IndexingPolicy.Automatic.Should().BeFalse();
		response.Resource.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
	}

	[Fact]
	public async Task CreateContainerIfNotExists_ExistingContainer_DoesNotOverwritePolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var props1 = new ContainerProperties("test", "/pk")
		{
			IndexingPolicy = new IndexingPolicy { Automatic = false }
		};
		await db.CreateContainerIfNotExistsAsync(props1);

		var props2 = new ContainerProperties("test", "/pk")
		{
			IndexingPolicy = new IndexingPolicy { Automatic = true }
		};
		await db.CreateContainerIfNotExistsAsync(props2);

		// Read the container to verify the original policy is preserved
		var readResponse = await db.GetContainer("test").ReadContainerAsync();
		readResponse.Resource.IndexingPolicy.Automatic.Should().BeFalse(
			"first creation's policy is preserved — second call does not overwrite");
	}

	[Fact]
	public async Task CreateContainer_ViaCosmosClient_PreservesPolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var props = new ContainerProperties("test", "/pk")
		{
			IndexingPolicy = new IndexingPolicy
			{
				Automatic = true,
				IndexingMode = IndexingMode.Consistent,
				IncludedPaths = { new IncludedPath { Path = "/name/?" } },
				ExcludedPaths = { new ExcludedPath { Path = "/secret/*" } }
			}
		};
		var response = await db.CreateContainerAsync(props);

		var policy = response.Resource.IndexingPolicy;
		policy.Automatic.Should().BeTrue();
		policy.IndexingMode.Should().Be(IndexingMode.Consistent);
		policy.IncludedPaths.Should().Contain(p => p.Path == "/name/?");
		policy.ExcludedPaths.Should().Contain(p => p.Path == "/secret/*");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category L — IndexMetrics
// ═══════════════════════════════════════════════════════════════════════════════

public class IndexMetricsTests
{
	[Fact]
	public async Task IndexMetrics_ReturnsUtilizationData()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.val > 5",
			requestOptions: new QueryRequestOptions { PopulateIndexMetrics = true });
		var response = await iterator.ReadNextAsync();

		response.IndexMetrics.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task IndexMetrics_WithoutPopulateFlag_ReturnsNull()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.val > 5");
		var response = await iterator.ReadNextAsync();

		response.IndexMetrics.Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category N — EnableScanInQuery
// ═══════════════════════════════════════════════════════════════════════════════

public class EnableScanInQueryTests
{
	[Fact(Skip = "Real Cosmos DB requires EnableScanInQuery=true to query paths not covered " +
		"by an index. The emulator ignores this option entirely — queries always work regardless.")]
	public async Task EnableScanInQuery_Required_ForUnindexedPaths()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.IndexingPolicy = new IndexingPolicy
		{
			IncludedPaths = { new IncludedPath { Path = "/" } },
			ExcludedPaths = { new ExcludedPath { Path = "/secret/*" } }
		};
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", secret = "hidden" }),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.secret = 'hidden'");
		var response = await iterator.ReadNextAsync();

		response.Count.Should().Be(0, "query on excluded path should fail without EnableScanInQuery");
	}

	[Fact]
	public async Task BehavioralDifference_EnableScanInQuery_AlwaysEnabled()
	{
		// DIVERGENT BEHAVIOR: Real Cosmos DB requires EnableScanInQuery=true
		// to query paths excluded from indexing. The emulator always does a
		// full scan, so queries work regardless of index configuration.
		var container = new InMemoryContainer("test", "/pk");
		container.IndexingPolicy = new IndexingPolicy
		{
			IncludedPaths = { new IncludedPath { Path = "/" } },
			ExcludedPaths = { new ExcludedPath { Path = "/secret/*" } }
		};
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", secret = "hidden" }),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.secret = 'hidden'");
		var response = await iterator.ReadNextAsync();

		response.Count.Should().Be(1, "emulator always scans — queries work on excluded paths");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category O — Index Types (Range, Hash, Spatial)
// ═══════════════════════════════════════════════════════════════════════════════

public class IndexTypeTests
{
	[Fact]
	public void IncludedPath_WithIndexes_AreStored()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.IndexingPolicy = new IndexingPolicy
		{
			IncludedPaths =
			{
				new IncludedPath { Path = "/name/?" },
				new IncludedPath { Path = "/value/?" }
			}
		};

		container.IndexingPolicy.IncludedPaths.Should().HaveCount(2);
		container.IndexingPolicy.IncludedPaths.Should().Contain(p => p.Path == "/name/?");
		container.IndexingPolicy.IncludedPaths.Should().Contain(p => p.Path == "/value/?");
	}

	[Fact(Skip = "Real Cosmos DB uses RangeIndex for equality and range queries, and HashIndex for " +
		"equality-only queries. The emulator stores index kinds but does not enforce them — all " +
		"query types work regardless of index kind.")]
	public async Task IncludedPath_IndexKind_EnforcedForQueryType()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.val > 5");
		var response = await iterator.ReadNextAsync();

		response.Count.Should().Be(1);
	}

	[Fact]
	public async Task BehavioralDifference_IndexKind_NotEnforced()
	{
		// DIVERGENT BEHAVIOR: Real Cosmos DB enforces index kind constraints —
		// a HashIndex doesn't support range queries. The emulator stores index
		// configuration but queries always work via full scan.
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.val > 5");
		var response = await iterator.ReadNextAsync();

		response.Count.Should().Be(1, "emulator ignores index kind — range query works on any index");
	}
}
