using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class ContainerManagementTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public void Id_ReturnsContainerName()
	{
		_container.Id.Should().Be("test-container");
	}

	[Fact]
	public async Task ReadContainerAsync_ReturnsContainerProperties()
	{
		var response = await _container.ReadContainerAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Should().NotBeNull();
		response.Resource.Id.Should().Be("test-container");
		response.Resource.PartitionKeyPath.Should().Be("/partitionKey");
	}

	[Fact]
	public async Task ReadContainerStreamAsync_ReturnsOk()
	{
		using var response = await _container.ReadContainerStreamAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteContainerAsync_ReturnsNoContent()
	{
		var response = await _container.DeleteContainerAsync();

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task DeleteContainerStreamAsync_ReturnsNoContent()
	{
		using var response = await _container.DeleteContainerStreamAsync();

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task GetFeedRangesAsync_ReturnsNonEmptyList()
	{
		var feedRanges = await _container.GetFeedRangesAsync();

		feedRanges.Should().NotBeEmpty();
	}

	[Fact]
	public async Task DeleteAllItemsByPartitionKeyStreamAsync_RemovesItemsInPartition()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie" },
			new PartitionKey("pk2"));

		using var response = await _container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);

		var remaining = await _container.ReadItemAsync<TestDocument>("3", new PartitionKey("pk2"));
		remaining.Resource.Name.Should().Be("Charlie");
	}

	[Fact]
	public async Task DeleteAllItemsByPartitionKeyStreamAsync_EmptyPartition_ReturnsOk()
	{
		using var response = await _container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("nonexistent"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}


public class ContainerManagementEdgeCaseTests
{
	[Fact]
	public async Task ReplaceContainerStreamAsync_ReturnsOk()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		var properties = new ContainerProperties("test-container", "/partitionKey");

		var response = await container.ReplaceContainerStreamAsync(properties);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceContainerAsync_UpdatesIndexingPolicy()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");

		var properties = new ContainerProperties("test-container", "/partitionKey")
		{
			IndexingPolicy = new IndexingPolicy
			{
				Automatic = false,
				IndexingMode = IndexingMode.None
			}
		};

		var response = await container.ReplaceContainerAsync(properties);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceContainerAsync_UpdatesDefaultTimeToLive()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");

		var properties = new ContainerProperties("test-container", "/partitionKey")
		{
			DefaultTimeToLive = 3600
		};

		var response = await container.ReplaceContainerAsync(properties);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task CreateContainerAsync_WithNullId_Throws()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseAsync("test-db");

		var act = () => db.Database.CreateContainerAsync(null!, "/pk");
		await act.Should().ThrowAsync<ArgumentNullException>();
	}

	[Fact]
	public async Task CreateContainerAsync_WithNullPartitionKeyPath_Throws()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseAsync("test-db");

		var act = () => db.Database.CreateContainerAsync("c", null!);
		await act.Should().ThrowAsync<ArgumentNullException>();
	}

	[Fact]
	public async Task DeleteContainerAsync_ThenReadContainer_Returns404()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await container.DeleteContainerAsync();

		// After delete, ReadContainerAsync returns 404 (matches real Cosmos DB)
		var act = () => container.ReadContainerAsync();
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public void GetContainer_ReturnsProxyRef_DoesNotValidateExistence()
	{
		var client = new InMemoryCosmosClient();
		var db = client.GetDatabase("test-db");

		// GetContainer returns a proxy reference without validating existence
		var container = db.GetContainer("nonexistent");
		container.Should().NotBeNull();
		container.Id.Should().Be("nonexistent");
	}

	[Fact]
	public async Task CreateContainerAsync_WithUniqueKeyPolicy_SetsProperties()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseAsync("test-db");

		var properties = new ContainerProperties("unique-container", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};

		var response = await db.Database.CreateContainerAsync(properties);
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}


/// <summary>
/// ReplaceContainerStreamAsync should persist property changes so that
/// subsequent ReadContainerAsync calls return the updated values.
/// </summary>
public class ContainerStreamReplacePersistenceTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task ReplaceContainerStream_PersistsPropertyChanges()
	{
		var newProperties = new ContainerProperties("test-container", "/partitionKey")
		{
			DefaultTimeToLive = 600
		};
		await _container.ReplaceContainerStreamAsync(newProperties);

		var readResponse = await _container.ReadContainerAsync();
		readResponse.Resource.DefaultTimeToLive.Should().Be(600);
	}
}


public class ContainerManagementGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task DeleteContainer_ClearsAllItems()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await _container.DeleteContainerAsync();

		_container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task ReadThroughput_ReturnsSyntheticValue()
	{
		var throughput = await _container.ReadThroughputAsync();

		throughput.Should().Be(400);
	}

	[Fact]
	public async Task ReplaceThroughput_AcceptsWithoutError()
	{
		var act = () => _container.ReplaceThroughputAsync(1000);

		await act.Should().NotThrowAsync();
	}

	[Fact]
	public async Task ReplaceContainer_AcceptsProperties()
	{
		var properties = new ContainerProperties("test-container", "/partitionKey")
		{
			IndexingPolicy = new IndexingPolicy { Automatic = true }
		};

		var response = await _container.ReplaceContainerAsync(properties);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}


public class ContainerManagementGapTests4
{
	[Fact]
	public async Task DeleteContainer_StreamVariant_Returns204()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await container.DeleteContainerStreamAsync();

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}
}


public class ContainerThroughputTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Container_ReadThroughputAsync_ReturnsValue()
	{
		var result = await _container.ReadThroughputAsync();
		result.Should().NotBeNull();
	}

	[Fact]
	public async Task Container_ReadThroughputAsync_WithRequestOptions_ReturnsResponse()
	{
		var response = await _container.ReadThroughputAsync(new RequestOptions());
		response.Should().NotBeNull();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Container_ReplaceThroughputAsync_Int_Succeeds()
	{
		var response = await _container.ReplaceThroughputAsync(800);
		response.Should().NotBeNull();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Container_ReplaceThroughputAsync_ThroughputProperties_Succeeds()
	{
		var tp = ThroughputProperties.CreateManualThroughput(1000);
		var response = await _container.ReplaceThroughputAsync(tp);
		response.Should().NotBeNull();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}


/// <summary>
/// Validates that the Conflicts property on the Container is accessible and returns a
/// non-null instance. InMemoryContainer returns an NSubstitute mock.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.conflicts
/// </summary>
public class ContainerConflictsPropertyTests5
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public void Conflicts_Property_ReturnsNonNull()
	{
		_container.Conflicts.Should().NotBeNull();
	}
}


public class ContainerManagementGapTests2
{
	[Fact]
	public async Task ReadContainer_ReturnsContainerProperties()
	{
		var container = new InMemoryContainer("my-container", "/partitionKey");

		var response = await container.ReadContainerAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Id.Should().Be("my-container");
	}
}


public class ContainerDatabaseBacklinkTests
{
	[Fact]
	public void Container_Database_Property_ReturnsNonNull()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.Database.Should().NotBeNull();
	}

	[Fact]
	public async Task Container_CreatedViaDatabase_Database_ReturnsParent()
	{
		var client = new InMemoryCosmosClient();
		var dbResponse = await client.CreateDatabaseAsync("test-db");
		var containerResponse = await dbResponse.Database.CreateContainerAsync("test-container", "/partitionKey");

		// The container should have a Database property
		containerResponse.Container.Should().NotBeNull();
	}
}

// ─── Unique Key Policy Enforcement ──────────────────────────────────────

public class UniqueKeyPolicyTests
{
	[Fact]
	public async Task CreateItem_ViolatesUniqueKey_ThrowsConflict()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task CreateItem_SameUniqueKey_DifferentPartition_Succeeds()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		// Same email but different partition — should succeed (unique keys are per-partition)
		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", email = "alice@test.com" }),
			new PartitionKey("b"));

		await act.Should().NotThrowAsync();
	}

	[Fact]
	public async Task CreateItem_CompositeUniqueKey_ViolatesBoth_ThrowsConflict()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/firstName", "/lastName" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", firstName = "Alice", lastName = "Smith" }),
			new PartitionKey("a"));

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", firstName = "Alice", lastName = "Smith" }),
			new PartitionKey("a"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task CreateItem_CompositeUniqueKey_OnlyOneDiffers_Succeeds()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/firstName", "/lastName" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", firstName = "Alice", lastName = "Smith" }),
			new PartitionKey("a"));

		// Different lastName — should succeed
		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", firstName = "Alice", lastName = "Jones" }),
			new PartitionKey("a"));

		await act.Should().NotThrowAsync();
	}

	[Fact]
	public async Task UpsertItem_ViolatesUniqueKey_OfDifferentItem_ThrowsConflict()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		// Upsert a DIFFERENT item with same email — should conflict
		var act = () => container.UpsertItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task UpsertItem_SameItem_UpdatesWithoutConflict()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		// Upsert same item (id=1) with same email — should succeed (updating self)
		var act = () => container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "alice@test.com", name = "updated" }),
			new PartitionKey("a"));

		await act.Should().NotThrowAsync();
	}
}

// ─── ConflictResolutionPolicy Stored But Not Enforced ───────────────────

public class ConflictResolutionPolicyTests
{
	/// <summary>
	/// In real Cosmos DB, the conflict resolution policy with a custom stored procedure
	/// resolves write conflicts in multi-region setups by invoking the specified sproc.
	/// The emulator stores the policy but operates with implicit strong consistency and
	/// single-region semantics, so conflict resolution never actually triggers.
	/// </summary>
	[Fact(Skip = "ConflictResolutionPolicy is stored on ContainerProperties and returned " +
		"on reads, but it is never enforced. The emulator operates in single-region mode " +
		"with implicit strong consistency, so write–write conflicts that would trigger the " +
		"policy in a multi-region setup cannot occur. The stored policy is purely decorative.")]
	public async Task ConflictResolution_CustomSproc_ShouldResolveConflicts()
	{
		var client = new InMemoryCosmosClient();
		var db = (InMemoryDatabase)(await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		await db.CreateContainerAsync(new ContainerProperties("ctr1", "/pk")
		{
			ConflictResolutionPolicy = new ConflictResolutionPolicy
			{
				Mode = ConflictResolutionMode.Custom,
				ResolutionProcedure = "dbs/testdb/colls/ctr1/sprocs/resolveConflict"
			}
		});

		// In a multi-region real Cosmos account, concurrent writes from different regions
		// would trigger the custom stored procedure. This cannot be simulated.
		Assert.Fail("Cannot simulate multi-region write conflicts in single-region emulator.");
	}

	/// <summary>
	/// Sister test: the policy is stored and echoed back, but has no runtime effect.
	/// </summary>
	[Fact]
	public async Task ConflictResolution_EmulatorBehavior_PolicyStoredButNotEnforced()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos DB: ConflictResolutionPolicy determines how write conflicts are
		//   resolved in multi-region write configurations. LastWriterWins uses _ts
		//   comparison. Custom mode invokes a stored procedure.
		// In-Memory Emulator: The policy is accepted by ContainerProperties and returned
		//   on ReadContainerAsync / ReplaceContainerAsync. However, since the emulator is
		//   single-region and strongly consistent, no write–write conflicts can occur,
		//   and the policy is never triggered. It's stored for API compatibility only.
		var client = new InMemoryCosmosClient();
		var db = (InMemoryDatabase)(await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var containerResponse = await db.CreateContainerAsync(new ContainerProperties("ctr1", "/pk")
		{
			ConflictResolutionPolicy = new ConflictResolutionPolicy
			{
				Mode = ConflictResolutionMode.LastWriterWins
			}
		});

		var readBack = await containerResponse.Container.ReadContainerAsync();
		readBack.Resource.ConflictResolutionPolicy.Mode.Should().Be(
			ConflictResolutionMode.LastWriterWins,
			"the policy is stored and returned but never actually enforced");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1 — Bug Fix Tests: Change Feed Clearing on Delete (A-series)
// ═══════════════════════════════════════════════════════════════════════════

public class DeleteContainerChangeFeedTests
{
	[Fact]
	public async Task DeleteContainerAsync_ClearsChangeFeed()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		container.GetChangeFeedCheckpoint().Should().BeGreaterThan(0, "change feed should have entries before delete");

		await container.DeleteContainerAsync();

		container.GetChangeFeedCheckpoint().Should().Be(0, "change feed should be empty after DeleteContainerAsync");
	}

	[Fact]
	public async Task DeleteContainerStreamAsync_ClearsChangeFeed()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		container.GetChangeFeedCheckpoint().Should().BeGreaterThan(0);

		await container.DeleteContainerStreamAsync();

		container.GetChangeFeedCheckpoint().Should().Be(0, "change feed should be empty after DeleteContainerStreamAsync");
	}

	[Fact]
	public async Task DeleteContainer_ThenAddItems_ChangeFeedOnlyHasNewEntries()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "old", pk = "a" }), new PartitionKey("a"));

		await container.DeleteContainerAsync();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "new", pk = "b" }), new PartitionKey("b"));

		container.GetChangeFeedCheckpoint().Should().Be(1, "only the post-delete item should be in the change feed");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1 — Bug Fix Tests: Property Preservation on Create (B-series)
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseContainerCreationPropertyTests
{
	[Fact]
	public async Task CreateContainerAsync_WithContainerProperties_PreservesUniqueKeyPolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var properties = new ContainerProperties("ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};

		var response = await db.CreateContainerAsync(properties);
		var container = (InMemoryContainer)response.Container;

		// Create first item
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "dup@test.com" }),
			new PartitionKey("a"));

		// Attempt duplicate email in same partition — should conflict
		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "dup@test.com" }),
			new PartitionKey("a"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task CreateContainerAsync_WithContainerProperties_PreservesDefaultTtl()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var properties = new ContainerProperties("ctr", "/pk")
		{
			DefaultTimeToLive = 3600
		};

		var response = await db.CreateContainerAsync(properties);
		var container = (InMemoryContainer)response.Container;

		container.DefaultTimeToLive.Should().Be(3600);
	}

	[Fact]
	public async Task CreateContainerIfNotExistsAsync_WithContainerProperties_PreservesUniqueKeyPolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var properties = new ContainerProperties("ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};

		var response = await db.CreateContainerIfNotExistsAsync(properties);
		var container = (InMemoryContainer)response.Container;

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "dup@test.com" }),
			new PartitionKey("a"));

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "dup@test.com" }),
			new PartitionKey("a"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task CreateContainerStreamAsync_WithContainerProperties_PreservesUniqueKeyPolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = (InMemoryDatabase)(await client.CreateDatabaseAsync("test-db")).Database;

		var properties = new ContainerProperties("ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};

		await db.CreateContainerStreamAsync(properties);
		var container = (InMemoryContainer)db.GetContainer("ctr");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "dup@test.com" }),
			new PartitionKey("a"));

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "dup@test.com" }),
			new PartitionKey("a"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2 — Container Lifecycle & Database Integration (D-series)
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerLifecycleTests
{
	[Fact]
	public async Task DeleteContainerAsync_RemovesFromDatabase()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.CreateContainerAsync("ctr1", "/pk");
		await db.CreateContainerAsync("ctr2", "/pk");

		var ctr1 = db.GetContainer("ctr1");
		await ctr1.DeleteContainerAsync();

		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var containers = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			containers.AddRange(page);
		}

		containers.Should().ContainSingle(c => c.Id == "ctr2");
		containers.Should().NotContain(c => c.Id == "ctr1");
	}

	[Fact]
	public async Task DeleteContainerStreamAsync_RemovesFromDatabase()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.CreateContainerAsync("ctr1", "/pk");
		await db.CreateContainerAsync("ctr2", "/pk");

		var ctr1 = db.GetContainer("ctr1");
		await ctr1.DeleteContainerStreamAsync();

		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var containers = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			containers.AddRange(page);
		}

		containers.Should().ContainSingle(c => c.Id == "ctr2");
	}

	[Fact]
	public async Task DeleteContainer_ThenRecreate_SameId_Succeeds()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var resp1 = await db.CreateContainerAsync("ctr", "/pk");
		var ctr1 = (InMemoryContainer)resp1.Container;
		await ctr1.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		await ctr1.DeleteContainerAsync();

		// Recreate with same id
		var resp2 = await db.CreateContainerAsync("ctr", "/pk");
		var ctr2 = (InMemoryContainer)resp2.Container;

		ctr2.ItemCount.Should().Be(0, "recreated container should be empty");
		resp2.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task DeleteContainer_ClearsAllItems_AndChangeFeed()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b" }), new PartitionKey("b"));

		container.ItemCount.Should().Be(2);
		container.GetChangeFeedCheckpoint().Should().Be(2);

		await container.DeleteContainerAsync();

		container.ItemCount.Should().Be(0);
		container.GetChangeFeedCheckpoint().Should().Be(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3 — Container Querying (E-series)
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerQueryingTests
{
	[Fact]
	public async Task GetContainerQueryIterator_ReturnsAllContainers()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.CreateContainerAsync("ctr1", "/pk");
		await db.CreateContainerAsync("ctr2", "/pk");
		await db.CreateContainerAsync("ctr3", "/pk");

		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var containers = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			containers.AddRange(page);
		}

		containers.Select(c => c.Id).Should().BeEquivalentTo(["ctr1", "ctr2", "ctr3"]);
	}

	[Fact]
	public async Task GetContainerQueryIterator_EmptyDatabase_ReturnsEmpty()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var containers = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			containers.AddRange(page);
		}

		containers.Should().BeEmpty();
	}

	[Fact]
	public async Task GetContainerQueryStreamIterator_ReturnsContainers()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.CreateContainerAsync("ctr1", "/pk");
		await db.CreateContainerAsync("ctr2", "/pk");

		var iterator = db.GetContainerQueryStreamIterator();
		var response = await iterator.ReadNextAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().NotBeNull();
	}

	[Fact]
	public async Task GetContainerQueryIterator_AfterDelete_ExcludesDeleted()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.CreateContainerAsync("ctr1", "/pk");
		await db.CreateContainerAsync("ctr2", "/pk");
		await db.CreateContainerAsync("ctr3", "/pk");

		var ctr2 = db.GetContainer("ctr2");
		await ctr2.DeleteContainerAsync();

		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var containers = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			containers.AddRange(page);
		}

		containers.Select(c => c.Id).Should().BeEquivalentTo(["ctr1", "ctr3"]);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 4 — Replace Container Edge Cases (C-series)
// ═══════════════════════════════════════════════════════════════════════════

public class ReplaceContainerPropertyTests
{
	[Fact]
	public async Task ReplaceContainerAsync_UpdatesDefaultTtl_ReadReflectsChange()
	{
		var container = new InMemoryContainer("test", "/pk");

		var props = new ContainerProperties("test", "/pk") { DefaultTimeToLive = 1800 };
		await container.ReplaceContainerAsync(props);

		var readBack = await container.ReadContainerAsync();
		readBack.Resource.DefaultTimeToLive.Should().Be(1800);
	}

	[Fact]
	public async Task ReplaceContainerAsync_UpdatesIndexingPolicy_ReadReflectsChange()
	{
		var container = new InMemoryContainer("test", "/pk");

		var props = new ContainerProperties("test", "/pk")
		{
			IndexingPolicy = new IndexingPolicy { Automatic = false, IndexingMode = IndexingMode.None }
		};
		await container.ReplaceContainerAsync(props);

		var readBack = await container.ReadContainerAsync();
		readBack.Resource.IndexingPolicy.Automatic.Should().BeFalse();
		readBack.Resource.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
	}

	[Fact]
	public async Task ReplaceContainerStreamAsync_PersistsMultiplePropertyChanges()
	{
		var container = new InMemoryContainer("test", "/pk");

		var props = new ContainerProperties("test", "/pk")
		{
			DefaultTimeToLive = 900,
			IndexingPolicy = new IndexingPolicy { Automatic = false }
		};
		await container.ReplaceContainerStreamAsync(props);

		var readBack = await container.ReadContainerAsync();
		readBack.Resource.DefaultTimeToLive.Should().Be(900);
		readBack.Resource.IndexingPolicy.Automatic.Should().BeFalse();
	}

	[Fact]
	public async Task ReplaceContainerAsync_CannotChangePartitionKeyPath()
	{
		// Real Cosmos DB behaviour:
		// Attempting to change the partition key path via ReplaceContainerAsync returns
		// 400 BadRequest with message "Partition key paths for a container cannot be changed."
		// The in-memory emulator should ideally throw a similar error.
		var container = new InMemoryContainer("test", "/pk");

		var props = new ContainerProperties("test", "/differentPk");
		var act = () => container.ReplaceContainerAsync(props);

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 5 — Container Creation Edge Cases (B5-B7)
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerCreationEdgeCaseTests2
{
	[Fact]
	public async Task CreateContainerAsync_DuplicateId_ThrowsConflict()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.CreateContainerAsync("ctr", "/pk");

		var act = () => db.CreateContainerAsync("ctr", "/pk");

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task CreateContainerIfNotExistsAsync_DuplicateId_ReturnsOk()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var resp1 = await db.CreateContainerIfNotExistsAsync("ctr", "/pk");
		resp1.StatusCode.Should().Be(HttpStatusCode.Created);

		var resp2 = await db.CreateContainerIfNotExistsAsync("ctr", "/pk");
		resp2.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task CreateContainerStreamAsync_DuplicateId_ReturnsConflict()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var props = new ContainerProperties("ctr", "/pk");
		var resp1 = await db.CreateContainerStreamAsync(props);
		resp1.StatusCode.Should().Be(HttpStatusCode.Created);

		var resp2 = await db.CreateContainerStreamAsync(props);
		resp2.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 6 — Throughput Edge Cases (F-series)
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerThroughputEdgeCaseTests
{
	[Fact]
	public async Task ReadThroughputAsync_WithRequestOptions_ReturnsThroughputProperties()
	{
		var container = new InMemoryContainer("test", "/pk");

		var response = await container.ReadThroughputAsync(new RequestOptions());

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Should().NotBeNull();
	}

	[Fact]
	public async Task ReplaceThroughputAsync_WithAutoscaleThroughputProperties_Accepted()
	{
		var container = new InMemoryContainer("test", "/pk");

		var tp = ThroughputProperties.CreateAutoscaleThroughput(4000);
		var response = await container.ReplaceThroughputAsync(tp);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Should().NotBeNull();
	}

	[Fact]
	public async Task Database_ReadThroughputAsync_ReturnsSynthetic400()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var throughput = await db.ReadThroughputAsync();

		throughput.Should().Be(400);
	}

	[Fact]
	public async Task Database_ReplaceThroughputAsync_AcceptsWithoutError()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var act = () => db.ReplaceThroughputAsync(1000);

		await act.Should().NotThrowAsync();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 7 — DeleteAllByPartitionKey Edge Cases (G-series)
// ═══════════════════════════════════════════════════════════════════════════

public class DeleteAllByPartitionKeyEdgeCaseTests
{
	[Fact]
	public async Task DeleteAllByPK_RecordsChangeFeedTombstones()
	{
		var container = new InMemoryContainer("test", "/pk");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a"));

		var checkpointBefore = container.GetChangeFeedCheckpoint();
		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("a"));

		// Expect 2 additional tombstone entries
		container.GetChangeFeedCheckpoint().Should().Be(checkpointBefore + 2);
	}

	/// <summary>
	/// Sister test documenting the emulator's actual behavior for DeleteAllByPK change feed.
	/// </summary>
	[Fact]
	public async Task DeleteAllByPK_ChangeFeedTombstones_NowRecorded()
	{
		// FIXED: DeleteAllItemsByPartitionKeyStreamAsync now records change feed tombstones
		// for each deleted item, matching real Cosmos DB behavior.
		var container = new InMemoryContainer("test", "/pk");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a"));

		var checkpointBefore = container.GetChangeFeedCheckpoint();
		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("a"));

		container.GetChangeFeedCheckpoint().Should().Be(checkpointBefore + 2,
			"tombstones are now recorded for each deleted item");
		container.ItemCount.Should().Be(0, "items are still removed");
	}

	[Fact]
	public async Task DeleteAllByPK_WithCompositePartitionKey_RemovesCorrectItems()
	{
		var container = new InMemoryContainer("test", new[] { "/tenant", "/region" });

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenant = "t1", region = "us" }),
			new PartitionKeyBuilder().Add("t1").Add("us").Build());
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", tenant = "t1", region = "eu" }),
			new PartitionKeyBuilder().Add("t1").Add("eu").Build());
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", tenant = "t2", region = "us" }),
			new PartitionKeyBuilder().Add("t2").Add("us").Build());

		await container.DeleteAllItemsByPartitionKeyStreamAsync(
			new PartitionKeyBuilder().Add("t1").Add("us").Build());

		container.ItemCount.Should().Be(2);

		// Item 1 should be gone
		var act = () => container.ReadItemAsync<JObject>("1",
			new PartitionKeyBuilder().Add("t1").Add("us").Build());
		await act.Should().ThrowAsync<CosmosException>();

		// Items 2 and 3 should remain
		var item2 = await container.ReadItemAsync<JObject>("2",
			new PartitionKeyBuilder().Add("t1").Add("eu").Build());
		item2.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteAllByPK_MultipleTimes_Idempotent()
	{
		var container = new InMemoryContainer("test", "/pk");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var resp1 = await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("a"));
		resp1.StatusCode.Should().Be(HttpStatusCode.OK);

		var resp2 = await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("a"));
		resp2.StatusCode.Should().Be(HttpStatusCode.OK, "second delete on empty partition should still return OK");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 8 — Container Properties & Metadata (H, I series)
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerPropertiesMetadataTests
{
	[Fact]
	public async Task ReadContainerAsync_ReturnsPartitionKeyPath_Composite()
	{
		var container = new InMemoryContainer("test", new[] { "/tenant", "/region" });

		var response = await container.ReadContainerAsync();

		response.Resource.PartitionKeyPaths.Should().NotBeNull();
		response.Resource.PartitionKeyPaths.Should().HaveCount(2);
	}

	[Fact]
	public async Task ReadContainerStreamAsync_ReturnsJsonWithContainerProperties()
	{
		var container = new InMemoryContainer("test", "/pk");

		using var response = await container.ReadContainerStreamAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().NotBeNull();

		using var reader = new StreamReader(response.Content);
		var json = await reader.ReadToEndAsync();
		var obj = JObject.Parse(json);
		obj["id"]?.ToString().Should().Be("test");
	}

	[Fact]
	public void Container_Scripts_Property_ReturnsNonNull()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.Scripts.Should().NotBeNull();
	}

	[Fact]
	public void Container_Database_Property_ReturnsNonNull()
	{
		// ── Divergent behavior note ──
		// Real Cosmos DB: The Database property returns the actual parent Database instance
		//   that created or manages this container.
		// In-Memory Emulator: Returns a fresh NSubstitute mock on each access. There is no
		//   back-reference to the parent InMemoryDatabase. This is sufficient for most
		//   test scenarios but code that navigates container.Database.Id will get a null.
		var container = new InMemoryContainer("test", "/pk");
		container.Database.Should().NotBeNull();
	}
}

public class DefineContainerBuilderTests2
{
	[Fact]
	public async Task DefineContainer_ReturnsContainerBuilder()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var builder = db.DefineContainer("ctr", "/pk");

		builder.Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase A — Bug Fix Tests: Throughput Persistence & Id Validation
// ═══════════════════════════════════════════════════════════════════════════

public class ThroughputPersistenceTests
{
	[Fact]
	public async Task Container_ReplaceThroughput_ThenRead_ReturnsPersisted()
	{
		var container = new InMemoryContainer("test", "/pk");

		await container.ReplaceThroughputAsync(800);

		var throughput = await container.ReadThroughputAsync();
		throughput.Should().Be(800);
	}

	[Fact]
	public async Task Container_ReplaceThroughput_ThenReadWithRequestOptions_ReturnsPersisted()
	{
		var container = new InMemoryContainer("test", "/pk");

		await container.ReplaceThroughputAsync(1200);

		var response = await container.ReadThroughputAsync(new RequestOptions());
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Should().NotBeNull();
	}

	[Fact]
	public async Task Database_ReplaceThroughput_ThenRead_ReturnsPersisted()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.ReplaceThroughputAsync(1000);

		var throughput = await db.ReadThroughputAsync();
		throughput.Should().Be(1000);
	}

	[Fact]
	public async Task Container_DefaultThroughput_Is400()
	{
		var container = new InMemoryContainer("test", "/pk");

		var throughput = await container.ReadThroughputAsync();
		throughput.Should().Be(400);
	}

	[Fact]
	public async Task Database_DefaultThroughput_Is400()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var throughput = await db.ReadThroughputAsync();
		throughput.Should().Be(400);
	}
}

public class ReplaceContainerIdValidationTests
{
	[Fact]
	public async Task ReplaceContainerAsync_MismatchedId_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("my-container", "/pk");

		var props = new ContainerProperties("wrong-id", "/pk");
		var act = () => container.ReplaceContainerAsync(props);

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ReplaceContainerStreamAsync_MismatchedId_ReturnsBadRequest()
	{
		var container = new InMemoryContainer("my-container", "/pk");

		var props = new ContainerProperties("wrong-id", "/pk");
		var response = await container.ReplaceContainerStreamAsync(props);

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ReplaceContainerAsync_MatchingId_Succeeds()
	{
		var container = new InMemoryContainer("my-container", "/pk");

		var props = new ContainerProperties("my-container", "/pk") { DefaultTimeToLive = 600 };
		var response = await container.ReplaceContainerAsync(props);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceContainerStreamAsync_MatchingId_Succeeds()
	{
		var container = new InMemoryContainer("my-container", "/pk");

		var props = new ContainerProperties("my-container", "/pk") { DefaultTimeToLive = 600 };
		var response = await container.ReplaceContainerStreamAsync(props);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase B — Constructor & Properties Tests
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerConstructorDefaultsTests
{
	[Fact]
	public void DefaultConstructor_SetsDefaults()
	{
		var container = new InMemoryContainer();

		container.Id.Should().Be("in-memory-container");
	}

	[Fact]
	public async Task DefaultConstructor_PartitionKeyDefaultsToId()
	{
		var container = new InMemoryContainer();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1" }), new PartitionKey("1"));

		var item = await container.ReadItemAsync<JObject>("1", new PartitionKey("1"));
		item.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReadContainerAsync_ResponseContainer_ReturnsSelf()
	{
		var container = new InMemoryContainer("test", "/pk");

		var response = await container.ReadContainerAsync();

		response.Container.Should().BeSameAs(container);
	}

	[Fact]
	public async Task DeleteContainerAsync_ResponseContainer_ReturnsSelf()
	{
		var container = new InMemoryContainer("test", "/pk");

		var response = await container.DeleteContainerAsync();

		response.Container.Should().BeSameAs(container);
	}

	[Fact]
	public async Task ReplaceContainerAsync_ResponseContainer_ReturnsSelf()
	{
		var container = new InMemoryContainer("test", "/pk");

		var props = new ContainerProperties("test", "/pk");
		var response = await container.ReplaceContainerAsync(props);

		response.Container.Should().BeSameAs(container);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase C — Replace Container Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ReplaceContainerPreservationTests
{
	[Fact]
	public async Task ReplaceContainerAsync_OnlyTtlChange_IndexingPolicyResetToDefault()
	{
		var container = new InMemoryContainer("test", "/pk");
		var initialPolicy = new IndexingPolicy { Automatic = false, IndexingMode = IndexingMode.None };
		await container.ReplaceContainerAsync(new ContainerProperties("test", "/pk") { IndexingPolicy = initialPolicy });

		// Replace with new ContainerProperties that has default IndexingPolicy (Automatic=true)
		await container.ReplaceContainerAsync(new ContainerProperties("test", "/pk") { DefaultTimeToLive = 300 });

		var readBack = await container.ReadContainerAsync();
		// ContainerProperties constructor always creates a default IndexingPolicy (Automatic=true)
		// so the Replace overwrites the previous Automatic=false with Automatic=true
		readBack.Resource.IndexingPolicy.Automatic.Should().BeTrue();
	}

	[Fact]
	public async Task ReplaceContainerAsync_MultipleSequentialReplaces_LastWins()
	{
		var container = new InMemoryContainer("test", "/pk");

		await container.ReplaceContainerAsync(new ContainerProperties("test", "/pk") { DefaultTimeToLive = 100 });
		await container.ReplaceContainerAsync(new ContainerProperties("test", "/pk") { DefaultTimeToLive = 200 });
		await container.ReplaceContainerAsync(new ContainerProperties("test", "/pk") { DefaultTimeToLive = 300 });

		var readBack = await container.ReadContainerAsync();
		readBack.Resource.DefaultTimeToLive.Should().Be(300);
	}

	[Fact]
	public async Task ReplaceContainerAsync_ShouldPreserveUniqueKeyPolicy_WhenNotIncludedInReplacement()
	{
		var properties = new ContainerProperties("test", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		// Replace with TTL only — no UniqueKeyPolicy
		await container.ReplaceContainerAsync(new ContainerProperties("test", "/pk") { DefaultTimeToLive = 900 });

		// Should still enforce uniqueness
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "dup@test.com" }), new PartitionKey("a"));

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "dup@test.com" }), new PartitionKey("a"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task ReplaceContainerAsync_InvalidatesComputedPropertyCache()
	{
		var props = new ContainerProperties("test", "/pk");
		props.ComputedProperties = new System.Collections.ObjectModel.Collection<ComputedProperty>
		{
			new() { Name = "cp1", Query = "SELECT VALUE c.name FROM c" }
		};
		var container = new InMemoryContainer(props);

		// Seed an item to populate the cache
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));

		// Replace — should invalidate computed property cache
		await container.ReplaceContainerAsync(new ContainerProperties("test", "/pk"));

		// Verify the container still works
		var readBack = await container.ReadContainerAsync();
		readBack.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase D — Container Creation Extended
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerCreationValidationTests
{
	[Fact]
	public async Task CreateContainerAsync_WithEmptyStringId_Throws()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var act = () => db.CreateContainerAsync("", "/pk");
		await act.Should().ThrowAsync<ArgumentException>();
	}

	[Fact]
	public async Task CreateContainerAsync_WithThroughputProperties_Succeeds()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var props = new ContainerProperties("ctr", "/pk");
		var response = await db.CreateContainerAsync(props, ThroughputProperties.CreateManualThroughput(1000));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateContainerStreamAsync_WithThroughputProperties_Succeeds()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var props = new ContainerProperties("ctr", "/pk");
		var response = await db.CreateContainerStreamAsync(props, ThroughputProperties.CreateManualThroughput(1000));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateContainerIfNotExistsAsync_ExistingContainerWithData_PreservesData()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var resp1 = await db.CreateContainerAsync("ctr", "/pk");
		var container = (InMemoryContainer)resp1.Container;
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		// CreateIfNotExists on same id — data should survive
		var resp2 = await db.CreateContainerIfNotExistsAsync("ctr", "/pk");
		var existing = (InMemoryContainer)resp2.Container;

		existing.ItemCount.Should().Be(1);
	}

	[Fact]
	public async Task CreateContainerStreamAsync_PreservesIndexingPolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var props = new ContainerProperties("ctr", "/pk")
		{
			IndexingPolicy = new IndexingPolicy { Automatic = false, IndexingMode = IndexingMode.None }
		};
		await db.CreateContainerStreamAsync(props);

		var container = db.GetContainer("ctr");
		var readBack = await container.ReadContainerAsync();
		readBack.Resource.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.None);
	}

	[Fact]
	public async Task CreateContainerIfNotExistsAsync_WithThroughputProperties_Succeeds()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var props = new ContainerProperties("ctr", "/pk");
		var response = await db.CreateContainerIfNotExistsAsync(props, ThroughputProperties.CreateManualThroughput(1000));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase E — Delete Operations Extended
// ═══════════════════════════════════════════════════════════════════════════

public class DeleteContainerRegistrationTests
{
	[Fact]
	public async Task DeleteContainerAsync_ShouldClearRegisteredTriggersAndUDFs()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.RegisterUdf("upper", args => ((string)args[0]).ToUpperInvariant());

		await container.DeleteContainerAsync();

		// Re-add an item and verify UDF no longer works (throws because it's unregistered)
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "alice" }), new PartitionKey("a"));

		var act = () =>
		{
			var iterator = container.GetItemQueryIterator<string>("SELECT VALUE udf.upper(c.name) FROM c");
			return iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}
}

public class DeleteAllByPartitionKeyExtendedTests
{
	[Fact]
	public async Task DeleteAllByPK_ThenCreateInSamePartition_Succeeds()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("a"));

		// Create new item in the same partition
		var response = await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		container.ItemCount.Should().Be(1);
	}
}

public class DeleteContainerIdempotencyTests
{
	[Fact]
	public async Task DeleteContainerAsync_CalledTwice_SecondCallStillSucceeds()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var resp1 = await container.DeleteContainerAsync();
		resp1.StatusCode.Should().Be(HttpStatusCode.NoContent);

		var resp2 = await container.DeleteContainerAsync();
		resp2.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task DeleteContainerStreamAsync_CalledTwice_SecondCallStillSucceeds()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var resp1 = await container.DeleteContainerStreamAsync();
		resp1.StatusCode.Should().Be(HttpStatusCode.NoContent);

		var resp2 = await container.DeleteContainerStreamAsync();
		resp2.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase F — FeedRange Configuration Tests
// ═══════════════════════════════════════════════════════════════════════════

public class FeedRangeConfigurationTests
{
	[Fact]
	public async Task GetFeedRangesAsync_FeedRangeCount5_Returns5Ranges()
	{
		var container = new InMemoryContainer("test", "/pk") { FeedRangeCount = 5 };

		var ranges = await container.GetFeedRangesAsync();

		ranges.Should().HaveCount(5);
	}

	[Fact]
	public async Task GetFeedRangesAsync_FeedRangeCount0_Returns1Range()
	{
		var container = new InMemoryContainer("test", "/pk") { FeedRangeCount = 0 };

		var ranges = await container.GetFeedRangesAsync();

		ranges.Should().HaveCount(1);
	}

	[Fact]
	public async Task GetFeedRangesAsync_NegativeFeedRangeCount_Returns1Range()
	{
		var container = new InMemoryContainer("test", "/pk") { FeedRangeCount = -5 };

		var ranges = await container.GetFeedRangesAsync();

		ranges.Should().HaveCount(1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase G — Container Query Iterator Extended
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerQueryIteratorExtendedTests
{
	[Fact]
	public async Task GetContainerQueryIterator_WithQueryDefinition_ReturnsAllContainers()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;
		await db.CreateContainerAsync("ctr1", "/pk");
		await db.CreateContainerAsync("ctr2", "/pk");

		var iterator = db.GetContainerQueryIterator<ContainerProperties>(new QueryDefinition("SELECT * FROM c"));
		var containers = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			containers.AddRange(page);
		}

		containers.Select(c => c.Id).Should().BeEquivalentTo(["ctr1", "ctr2"]);
	}

	[Fact]
	public async Task GetContainerQueryStreamIterator_WithQueryDefinition_ReturnsOk()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;
		await db.CreateContainerAsync("ctr1", "/pk");

		var iterator = db.GetContainerQueryStreamIterator(new QueryDefinition("SELECT * FROM c"));
		var response = await iterator.ReadNextAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase H — Database Lifecycle Integration
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseDeletionContainerTests
{
	[Fact]
	public async Task Database_DeleteAsync_ClearsAllContainersAndData()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		var resp1 = await db.CreateContainerAsync("ctr1", "/pk");
		var ctr1 = (InMemoryContainer)resp1.Container;
		await ctr1.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var resp2 = await db.CreateContainerAsync("ctr2", "/pk");
		var ctr2 = (InMemoryContainer)resp2.Container;
		await ctr2.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b" }), new PartitionKey("b"));

		await db.DeleteAsync();

		// Containers list should be empty
		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var containers = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			containers.AddRange(page);
		}
		containers.Should().BeEmpty();
	}

	[Fact]
	public async Task Database_DeleteAsync_RemovesDatabaseFromClient()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseAsync("test-db");

		var db = client.GetDatabase("test-db");
		await db.DeleteAsync();

		// Re-create should succeed (database no longer exists)
		var response = await client.CreateDatabaseAsync("test-db");
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase I — ClearItems Public Method
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerClearItemsTests
{
	[Fact]
	public async Task ClearItems_RemovesAllItemsAndChangeFeed()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b" }), new PartitionKey("b"));

		container.ClearItems();

		container.ItemCount.Should().Be(0);
		container.GetChangeFeedCheckpoint().Should().Be(0);
	}

	[Fact]
	public void ClearItems_OnEmptyContainer_NoOp()
	{
		var container = new InMemoryContainer("test", "/pk");

		var act = () => container.ClearItems();

		act.Should().NotThrow();
		container.ItemCount.Should().Be(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase J — Container Special Characters
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerSpecialCharacterTests
{
	[Fact]
	public async Task Container_WithSpecialCharactersInId_WorksCorrectly()
	{
		var container = new InMemoryContainer("test-container_v2.1 (prod)", "/pk");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var item = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		item.StatusCode.Should().Be(HttpStatusCode.OK);
		container.Id.Should().Be("test-container_v2.1 (prod)");
	}

	[Fact]
	public async Task Container_WithUnicodeId_WorksCorrectly()
	{
		var container = new InMemoryContainer("コンテナ-テスト", "/pk");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var item = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		item.StatusCode.Should().Be(HttpStatusCode.OK);
		container.Id.Should().Be("コンテナ-テスト");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase K — CancellationToken Support
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerCancellationTokenTests
{
	[Fact]
	public async Task ReadContainerAsync_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/pk");
		using var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => container.ReadContainerAsync(cancellationToken: cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase L — ReadContainerStreamAsync JSON Round-Trip
// ═══════════════════════════════════════════════════════════════════════════

public class ReadContainerStreamRoundTripTests
{
	[Fact]
	public async Task ReadContainerStreamAsync_JsonCanBeDeserializedToContainerProperties()
	{
		var container = new InMemoryContainer("my-container", "/myPk");

		using var response = await container.ReadContainerStreamAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		using var reader = new StreamReader(response.Content);
		var json = await reader.ReadToEndAsync();
		var deserialized = Newtonsoft.Json.JsonConvert.DeserializeObject<ContainerProperties>(json);

		deserialized.Should().NotBeNull();
		deserialized.Id.Should().Be("my-container");
		deserialized.PartitionKeyPath.Should().Be("/myPk");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  SKIP-4 — Container.Database.Id Returns Null
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerDatabaseIdTests
{
	[Fact]
	public void Container_Database_Id_ShouldReturnParentDatabaseId()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.Database.Id.Should().NotBeNull();
	}

	[Fact]
	public void Container_Database_SameInstanceOnEachAccess()
	{
		var container = new InMemoryContainer("test", "/pk");
		var db1 = container.Database;
		var db2 = container.Database;
		db1.Should().BeSameAs(db2, "Database property should return the same instance");
	}
}
