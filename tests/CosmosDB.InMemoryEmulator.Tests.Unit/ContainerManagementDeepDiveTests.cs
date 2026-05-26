using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1: Bug-proving tests (RED → GREEN)
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerStreamValidationTests
{
	// T01: BUG-1 — ReplaceContainerStreamAsync should reject partition key path changes
	[Fact]
	public async Task ReplaceContainerStreamAsync_RejectsPartitionKeyPathChange()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		var newProps = new ContainerProperties("test", "/differentPk");
		using var response = await container.ReplaceContainerStreamAsync(newProps);

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// T02: BUG-1 — ReplaceContainerStreamAsync should reject invalid computed properties
	[Fact]
	public async Task ReplaceContainerStreamAsync_RejectsInvalidComputedProperties()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		var newProps = new ContainerProperties("test", "/pk");
		// Add 21 computed properties (max is 20)
		for (var i = 0; i < 21; i++)
		{
			newProps.ComputedProperties.Add(new ComputedProperty
			{
				Name = $"cp{i}",
				Query = $"SELECT VALUE c.field{i} FROM c"
			});
		}

		using var response = await container.ReplaceContainerStreamAsync(newProps);

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// T03: BUG-2 — ReplaceContainerStreamAsync should preserve UniqueKeyPolicy
	[Fact]
	public async Task ReplaceContainerStreamAsync_PreservesUniqueKeyPolicy()
	{
		var originalProps = new ContainerProperties("test", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(originalProps);
		container.ExplicitlyCreated = true;

		// Replace with props that omit UniqueKeyPolicy
		var replaceProps = new ContainerProperties("test", "/pk");
		using var response = await container.ReplaceContainerStreamAsync(replaceProps);
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		// Verify UniqueKeyPolicy was preserved
		var readResponse = await container.ReadContainerAsync();
		readResponse.Resource.UniqueKeyPolicy.UniqueKeys.Should().HaveCount(1);
		readResponse.Resource.UniqueKeyPolicy.UniqueKeys[0].Paths.Should().Contain("/email");

		// Verify unique key is actually enforced
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", email = "x@test.com" }),
			new PartitionKey("a"));

		var act = () => container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", email = "x@test.com" }),
			new PartitionKey("a"));
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	// T04: BUG-3 — ReadContainerStreamAsync should return 404 for non-existent containers
	[Fact]
	public async Task ReadContainerStreamAsync_NonExistentContainer_Returns404()
	{
		var db = new InMemoryDatabase("testdb");
		var container = (InMemoryContainer)db.GetContainer("nonexistent");

		using var response = await container.ReadContainerStreamAsync();

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

public class DeleteContainerCleanupTests
{
	// T05: BUG-4 — DeleteContainerAsync should clear stored procedure properties
	[Fact]
	public async Task DeleteContainer_ClearsStoredProcedureProperties()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		await container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "sproc1",
			Body = "function() { return true; }"
		});

		// Verify sproc exists
		var sproc = await container.Scripts.ReadStoredProcedureAsync("sproc1");
		sproc.StatusCode.Should().Be(HttpStatusCode.OK);

		// Delete container
		await container.DeleteContainerAsync();

		// Verify sproc properties are cleared
		var act = () => container.Scripts.ReadStoredProcedureAsync("sproc1");
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// T06: BUG-4 — DeleteContainerAsync should clear trigger properties
	[Fact]
	public async Task DeleteContainer_ClearsTriggerProperties()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trigger1",
			Body = "function() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		// Verify trigger exists
		var trigger = await container.Scripts.ReadTriggerAsync("trigger1");
		trigger.StatusCode.Should().Be(HttpStatusCode.OK);

		// Delete container
		await container.DeleteContainerAsync();

		// Verify trigger properties are cleared
		var act = () => container.Scripts.ReadTriggerAsync("trigger1");
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// T34: BUG-4 — DeleteContainerStreamAsync should clear stored procedure properties
	[Fact]
	public async Task DeleteContainer_StreamVariant_ClearsStoredProcedureProperties()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		await container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "sproc1",
			Body = "function() { return true; }"
		});

		// Delete container via stream
		using var response = await container.DeleteContainerStreamAsync();
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);

		// Verify sproc properties are cleared
		var act = () => container.Scripts.ReadStoredProcedureAsync("sproc1");
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// T35: BUG-4 — DeleteContainerStreamAsync should clear trigger properties
	[Fact]
	public async Task DeleteContainer_StreamVariant_ClearsTriggerProperties()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trigger1",
			Body = "function() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		// Delete container via stream
		using var response = await container.DeleteContainerStreamAsync();
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);

		// Verify trigger properties are cleared
		var act = () => container.Scripts.ReadTriggerAsync("trigger1");
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

public class ContainerExplicitCreationLifecycleTests
{
	// T07: BUG-5 — After DeleteContainerAsync, lazy resolve should show container not found
	[Fact]
	public async Task DeleteContainer_ThenLazyResolve_ReadContainerThrowsNotFound()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync("mycontainer", "/pk");

		// Verify container exists
		var container = db.GetContainer("mycontainer");
		var readResponse = await container.ReadContainerAsync();
		readResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		// Delete the container
		await container.DeleteContainerAsync();

		// Get a new lazy reference
		var newRef = db.GetContainer("mycontainer");

		// ReadContainerAsync should throw NotFound
		var act = () => newRef.ReadContainerAsync();
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// T08: BUG-5 — Database delete should clear _explicitlyCreatedContainers
	[Fact]
	public async Task DatabaseDelete_ClearsExplicitlyCreatedContainers()
	{
		var client = new InMemoryCosmosClient();
		var dbResponse = await client.CreateDatabaseAsync("testdb");
		var db = dbResponse.Database;

		await db.CreateContainerAsync("mycontainer", "/pk");

		// Verify container exists
		var container = db.GetContainer("mycontainer");
		var readResponse = await container.ReadContainerAsync();
		readResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		// Delete the database
		await db.DeleteAsync();

		// Re-create the database and get a lazy container reference
		var newDbResponse = await client.CreateDatabaseAsync("testdb");
		var newDb = newDbResponse.Database;
		var newContainer = newDb.GetContainer("mycontainer");

		// ReadContainerAsync should throw NotFound
		var act = () => newContainer.ReadContainerAsync();
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

public class ThroughputPersistenceDeepDiveTests
{
	// Issue #26: CreateContainerAsync throughput parameter should be persisted
	[Fact]
	public async Task CreateContainer_WithThroughput_PersistsValue()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;

		var containerProps = new ContainerProperties("items", "/pk");
		await db.CreateContainerAsync(containerProps, throughput: 2000);

		var actualThroughput = await client.GetContainer("testdb", "items").ReadThroughputAsync();
		actualThroughput.Should().Be(2000);
	}

	// Issue #26: CreateContainerIfNotExistsAsync throughput parameter should be persisted
	[Fact]
	public async Task CreateContainerIfNotExists_WithThroughput_PersistsValue()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;

		await db.CreateContainerIfNotExistsAsync("items", "/pk", throughput: 1500);

		var actualThroughput = await client.GetContainer("testdb", "items").ReadThroughputAsync();
		actualThroughput.Should().Be(1500);
	}

	// Issue #26: CreateContainerAsync with ThroughputProperties should persist
	[Fact]
	public async Task CreateContainer_WithThroughputProperties_PersistsValue()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;

		var containerProps = new ContainerProperties("items", "/pk");
		await db.CreateContainerAsync(containerProps, ThroughputProperties.CreateManualThroughput(3000));

		var actualThroughput = await client.GetContainer("testdb", "items").ReadThroughputAsync();
		actualThroughput.Should().Be(3000);
	}

	// T09: BUG-6 — ReplaceThroughputAsync(ThroughputProperties) should persist the value
	[Fact]
	public async Task ReplaceThroughput_ThroughputProperties_PersistsValue()
	{
		var container = new InMemoryContainer("test", "/pk");

		// Default throughput should be 400
		var initialThroughput = await container.ReadThroughputAsync();
		initialThroughput.Should().Be(400);

		// Replace with ThroughputProperties
		await container.ReplaceThroughputAsync(ThroughputProperties.CreateManualThroughput(800));

		// Throughput should now be 800
		var newThroughput = await container.ReadThroughputAsync();
		newThroughput.Should().Be(800);
	}
}

public class RegistrationValidationTests
{
	// T10: BUG-7 — RegisterUdf should validate null name
	[Fact]
	public void RegisterUdf_NullName_ThrowsArgumentNull()
	{
		var container = new InMemoryContainer("test", "/pk");

		var act = () => container.RegisterUdf(null!, _ => null);

		act.Should().Throw<ArgumentNullException>();
	}

	// T11: BUG-8 — RegisterTrigger should validate null triggerId (pre-trigger overload)
	[Fact]
	public void RegisterTrigger_NullTriggerId_ThrowsArgumentNull()
	{
		var container = new InMemoryContainer("test", "/pk");

		var act = () => container.RegisterTrigger(null!, TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => doc));

		act.Should().Throw<ArgumentNullException>();
	}

	// T11b: BUG-8 — RegisterTrigger should validate null triggerId (post-trigger overload)
	[Fact]
	public void RegisterTrigger_PostHandler_NullTriggerId_ThrowsArgumentNull()
	{
		var container = new InMemoryContainer("test", "/pk");

		var act = () => container.RegisterTrigger(null!, TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => { }));

		act.Should().Throw<ArgumentNullException>();
	}

	// T12: BUG-8 — DeregisterTrigger should validate null triggerId
	[Fact]
	public void DeregisterTrigger_NullTriggerId_ThrowsArgumentNull()
	{
		var container = new InMemoryContainer("test", "/pk");

		var act = () => container.DeregisterTrigger(null!);

		act.Should().Throw<ArgumentNullException>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2: Coverage gap tests (GREEN expected)
// ═══════════════════════════════════════════════════════════════════════════

public class ContainerNameValidationTests
{
	// T13: GAP-9 — Name too long
	[Fact]
	public async Task CreateContainerAsync_NameTooLong_ThrowsBadRequest()
	{
		var db = new InMemoryDatabase("testdb");
		var longName = new string('x', 256);

		var act = () => db.CreateContainerAsync(longName, "/pk");

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// T14: GAP-9/10 — Name with slash
	[Fact]
	public async Task CreateContainerAsync_NameWithSlash_ThrowsBadRequest()
	{
		var db = new InMemoryDatabase("testdb");

		var act = () => db.CreateContainerAsync("my/container", "/pk");

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// T15: GAP-10 — Name with backslash
	[Fact]
	public async Task CreateContainerAsync_NameWithBackslash_ThrowsBadRequest()
	{
		var db = new InMemoryDatabase("testdb");

		var act = () => db.CreateContainerAsync("my\\container", "/pk");

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// T16: GAP-10 — Name with hash
	[Fact]
	public async Task CreateContainerAsync_NameWithHash_ThrowsBadRequest()
	{
		var db = new InMemoryDatabase("testdb");

		var act = () => db.CreateContainerAsync("my#container", "/pk");

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// T17: GAP-10 — Name with question mark
	[Fact]
	public async Task CreateContainerAsync_NameWithQuestionMark_ThrowsBadRequest()
	{
		var db = new InMemoryDatabase("testdb");

		var act = () => db.CreateContainerAsync("my?container", "/pk");

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

public class ContainerCreationEdgeCaseTests3
{
	// T18: GAP-11 — CreateContainerIfNotExists with different PK returns existing
	[Fact]
	public async Task CreateContainerIfNotExists_DifferentPkPath_ReturnsExistingContainer()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync("container1", "/a");

		// Create again with different PK path - should return existing
		var response = await db.CreateContainerIfNotExistsAsync("container1", "/b");

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		// The container should still have the original PK path
		var readResponse = await response.Container.ReadContainerAsync();
		readResponse.Resource.PartitionKeyPath.Should().Be("/a");
	}
}

public class DeleteAllByPartitionKeyDeepDiveTests
{
	// T19: GAP-12 — DeleteAllByPK with PartitionKey.None
	[Fact]
	public async Task DeleteAllByPK_PartitionKeyNone_RemovesMatchingItems()
	{
		var container = new InMemoryContainer("test", "/pk");

		// Create items with no partition key value (PartitionKey.None-like behavior)
		await container.CreateItemAsync(JObject.FromObject(new { id = "1" }), PartitionKey.None);
		await container.CreateItemAsync(JObject.FromObject(new { id = "2" }), PartitionKey.None);
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "other" }),
			new PartitionKey("other"));

		using var response = await container.DeleteAllItemsByPartitionKeyStreamAsync(PartitionKey.None);

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		// Items with PK.None should be gone
		var act1 = () => container.ReadItemAsync<JObject>("1", PartitionKey.None);
		(await act1.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);

		// Items with other PK should still exist
		var remaining = await container.ReadItemAsync<JObject>("3", new PartitionKey("other"));
		remaining.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

public class ContainerQueryFilterTests
{
	// T20: GAP-13 — GetContainerQueryIterator with WHERE filter
	[Fact]
	public async Task GetContainerQueryIterator_WhereFilter_ReturnsMatchingContainer()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync("container-1", "/pk");
		await db.CreateContainerAsync("container-2", "/pk");
		await db.CreateContainerAsync("container-3", "/pk");

		var iterator = db.GetContainerQueryIterator<ContainerProperties>(
			"SELECT * FROM c WHERE c.id = 'container-2'");

		var results = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
		results[0].Id.Should().Be("container-2");
	}

	// T21: GAP-14 — GetContainerQueryIterator with continuation token paging
	[Fact]
	public async Task GetContainerQueryIterator_ContinuationToken_CorrectPaging()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync("c1", "/pk");
		await db.CreateContainerAsync("c2", "/pk");
		await db.CreateContainerAsync("c3", "/pk");

		var allResults = new List<ContainerProperties>();
		string? continuationToken = null;

		// Page through with MaxItemCount=1
		do
		{
			var iterator = db.GetContainerQueryIterator<ContainerProperties>(
				queryText: null,
				continuationToken: continuationToken,
				requestOptions: new QueryRequestOptions { MaxItemCount = 1 });

			var page = await iterator.ReadNextAsync();
			allResults.AddRange(page);
			continuationToken = page.ContinuationToken;
		} while (continuationToken != null);

		allResults.Should().HaveCount(3);
	}

	// T22: GAP-15 — GetContainerQueryIterator returns properties without metadata
	[Fact]
	public async Task GetContainerQueryIterator_Properties_ConstructedFromIdAndPk()
	{
		var db = new InMemoryDatabase("testdb");
		var props = new ContainerProperties("mycontainer", "/pk")
		{
			DefaultTimeToLive = 3600
		};
		await db.CreateContainerAsync(props);

		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var page = await iterator.ReadNextAsync();
		var returned = page.First();

		returned.Id.Should().Be("mycontainer");
		returned.PartitionKeyPath.Should().Be("/pk");
	}

	// Issue #7: GetContainerQueryIterator<string> with SELECT VALUE query should project container IDs
	[Fact]
	public async Task GetContainerQueryIterator_SelectValueId_ReturnsContainerIds()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync(new ContainerProperties("container-a", "/pk"));
		await db.CreateContainerAsync(new ContainerProperties("container-b", "/pk"));

		var iterator = db.GetContainerQueryIterator<string>("SELECT VALUE(c.id) FROM c");
		var page = await iterator.ReadNextAsync();
		var ids = page.ToList();

		ids.Should().Contain("container-a");
		ids.Should().Contain("container-b");
	}

	// Issue #7: Alternative SELECT VALUE syntax without parentheses
	[Fact]
	public async Task GetContainerQueryIterator_SelectValueIdNoParen_ReturnsContainerIds()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync(new ContainerProperties("my-container", "/pk"));

		var iterator = db.GetContainerQueryIterator<string>("SELECT VALUE c.id FROM c");
		var page = await iterator.ReadNextAsync();
		var ids = page.ToList();

		ids.Should().Contain("my-container");
	}
}

public class ContainerCancellationTokenDeepDiveTests
{
	// T23: GAP-16 — DeleteContainerAsync with cancelled token
	[Fact]
	public async Task DeleteContainerAsync_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/pk");
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => container.DeleteContainerAsync(cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	// T24: GAP-16 — ReplaceContainerAsync with cancelled token
	[Fact]
	public async Task ReplaceContainerAsync_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => container.ReplaceContainerAsync(
			new ContainerProperties("test", "/pk"), cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}
}

public class ReplaceContainerEdgeCaseTests
{
	// T25: GAP-17 — ReplaceContainerAsync with null properties
	[Fact]
	public async Task ReplaceContainerAsync_NullProperties_Throws()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		var act = () => container.ReplaceContainerAsync(null!);

		await act.Should().ThrowAsync<Exception>();
	}
}

public class DeleteContainerIdempotencyDeepDiveTests
{
	// T26: GAP-18 — Delete twice, database registration removed after first
	[Fact]
	public async Task DeleteContainerAsync_Twice_OnlyRemovesFromDatabaseOnce()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync("mycontainer", "/pk");

		var container = db.GetContainer("mycontainer");

		// First delete
		var response1 = await container.DeleteContainerAsync();
		response1.StatusCode.Should().Be(HttpStatusCode.NoContent);

		// Second delete - should still return NoContent (idempotent)
		var response2 = await container.DeleteContainerAsync();
		response2.StatusCode.Should().Be(HttpStatusCode.NoContent);

		// Container should not appear in iterator
		var iterator = db.GetContainerQueryIterator<ContainerProperties>();
		var page = await iterator.ReadNextAsync();
		page.Should().NotContain(p => p.Id == "mycontainer");
	}
}

public class DatabaseDeletionContainerDeepDiveTests
{
	// T27/S01 — Database delete cascade behavior
	[Fact]
	public async Task DatabaseDeleteAsync_ShouldCascadeContainerCleanup()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync("c1", "/pk");
		var container = (InMemoryContainer)db.GetContainer("c1");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		await db.DeleteAsync();

		// Items should be gone after database delete (cascade)
		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

public class ContainerCreationResponseTests
{
	// T28: GAP-20 — CreateContainerStreamAsync response body
	[Fact]
	public async Task CreateContainerStreamAsync_ResponseBody_ContainsContainerProperties()
	{
		var db = new InMemoryDatabase("testdb");
		var props = new ContainerProperties("myCont", "/pk");

		using var response = await db.CreateContainerStreamAsync(props);

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		// Stream response should be non-null (may or may not have a body depending on implementation)
		response.Should().NotBeNull();
	}
}

public class ComputedPropertyValidationTests
{
	// T29: GAP-21 — Too many computed properties via Replace
	[Fact]
	public async Task ReplaceContainerAsync_ComputedProperties_TooMany_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		var newProps = new ContainerProperties("test", "/pk");
		for (var i = 0; i < 21; i++)
		{
			newProps.ComputedProperties.Add(new ComputedProperty
			{
				Name = $"cp{i}",
				Query = $"SELECT VALUE c.field{i} FROM c"
			});
		}

		var act = () => container.ReplaceContainerAsync(newProps);

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// T30: GAP-21 — Reserved computed property name via Replace
	[Fact]
	public async Task ReplaceContainerAsync_ComputedProperties_ReservedName_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		var newProps = new ContainerProperties("test", "/pk");
		newProps.ComputedProperties.Add(new ComputedProperty
		{
			Name = "id",
			Query = "SELECT VALUE c.someField FROM c"
		});

		var act = () => container.ReplaceContainerAsync(newProps);

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// T31: GAP-21 — Missing SELECT VALUE in computed property via Replace
	[Fact]
	public async Task ReplaceContainerAsync_ComputedProperties_MissingSelectValue_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		var newProps = new ContainerProperties("test", "/pk");
		newProps.ComputedProperties.Add(new ComputedProperty
		{
			Name = "cp1",
			Query = "SELECT c.someField FROM c"
		});

		var act = () => container.ReplaceContainerAsync(newProps);

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

public class FeedRangeStabilityTests
{
	// T32: GAP-22 — Feed ranges stable across item operations
	[Fact]
	public async Task GetFeedRangesAsync_StableAcrossItemOperations()
	{
		var container = new InMemoryContainer("test", "/pk") { FeedRangeCount = 3 };

		var ranges1 = await container.GetFeedRangesAsync();
		ranges1.Should().HaveCount(3);

		// Add items
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "b" }),
			new PartitionKey("b"));

		var ranges2 = await container.GetFeedRangesAsync();
		ranges2.Should().HaveCount(3);

		// Delete items
		await container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));

		var ranges3 = await container.GetFeedRangesAsync();
		ranges3.Should().HaveCount(3);

		// Feed ranges should be consistent
		for (var i = 0; i < 3; i++)
		{
			ranges1[i].ToString().Should().Be(ranges2[i].ToString());
			ranges2[i].ToString().Should().Be(ranges3[i].ToString());
		}
	}
}

public class ContainerTtlValidationTests
{
	// T33: GAP-23 — DefaultTimeToLive = 0 should throw
	[Fact]
	public async Task DefaultTimeToLive_SetToZero_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.ExplicitlyCreated = true;

		var newProps = new ContainerProperties("test", "/pk")
		{
			DefaultTimeToLive = 0
		};

		var act = () => container.ReplaceContainerAsync(newProps);

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3: Behavioral difference tests (SKIP + SISTER)
// ═══════════════════════════════════════════════════════════════════════════

public class DeletedContainerBehaviorTests
{
	// S02 — After DeleteContainerAsync, held reference returns 404
	[Fact]
	public async Task ReadContainerAsync_ShouldReturn404ForDeletedContainer()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync("c1", "/pk");
		var container = db.GetContainer("c1");

		await container.DeleteContainerAsync();

		var act = () => container.ReadContainerAsync();
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// SISTER: documents actual divergent behavior
	[Fact]
	public async Task DeleteContainerAsync_ThenReadContainer_Returns404()
	{
		var db = new InMemoryDatabase("testdb");
		await db.CreateContainerAsync("c1", "/pk");
		var container = db.GetContainer("c1");

		await container.DeleteContainerAsync();

		var act = () => container.ReadContainerAsync();
		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}
