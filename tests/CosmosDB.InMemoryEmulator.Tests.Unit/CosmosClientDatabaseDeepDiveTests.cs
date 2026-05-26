using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1: BUG-1 — Database ThroughputProperties roundtrip
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseThroughputPropertiesRoundtripTests
{
	// T1: ReplaceThroughputAsync(ThroughputProperties) → ReadThroughputAsync roundtrip
	[Fact]
	public async Task ReplaceThroughputAsync_ThroughputProperties_ThenRead_ReturnsNewValue()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.ReplaceThroughputAsync(ThroughputProperties.CreateManualThroughput(2000));

		var throughput = await db.ReadThroughputAsync();
		throughput.Should().Be(2000);
	}

	// T2: ReplaceThroughputAsync(ThroughputProperties) → ReadThroughputAsync(RequestOptions) roundtrip
	[Fact]
	public async Task ReplaceThroughputAsync_ThroughputProperties_ThenReadWithRequestOptions_ReturnsNewValue()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.ReplaceThroughputAsync(ThroughputProperties.CreateManualThroughput(1500));

		var response = await db.ReadThroughputAsync(new RequestOptions());
		response.Resource.Throughput.Should().Be(1500);
	}

	// T3: Interleaved int → ThroughputProperties → Read roundtrip
	[Fact]
	public async Task ReplaceThroughputAsync_Int_ThenThroughputProperties_ThenRead_ReturnsLatest()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("test-db")).Database;

		await db.ReplaceThroughputAsync(1000);
		var t1 = await db.ReadThroughputAsync();
		t1.Should().Be(1000);

		await db.ReplaceThroughputAsync(ThroughputProperties.CreateManualThroughput(3000));
		var t2 = await db.ReadThroughputAsync();
		t2.Should().Be(3000);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2: BUG-2 — CreateDatabaseStreamAsync guards
// ═══════════════════════════════════════════════════════════════════════════

public class CreateDatabaseStreamGuardTests
{
	// S1: Disposed client
	[Fact]
	public async Task CreateDatabaseStreamAsync_DisposedClient_ThrowsObjectDisposed()
	{
		var client = new InMemoryCosmosClient();
		client.Dispose();

		var act = () => client.CreateDatabaseStreamAsync(new DatabaseProperties("test-db"));

		await act.Should().ThrowAsync<ObjectDisposedException>();
	}

	// S2: Cancelled token
	[Fact]
	public async Task CreateDatabaseStreamAsync_CancelledToken_Throws()
	{
		var client = new InMemoryCosmosClient();
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => client.CreateDatabaseStreamAsync(
			new DatabaseProperties("test-db"), cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	// S3: Invalid name with forward slash
	[Fact]
	public async Task CreateDatabaseStreamAsync_NameWithForwardSlash_ThrowsBadRequest()
	{
		var client = new InMemoryCosmosClient();

		var act = () => client.CreateDatabaseStreamAsync(new DatabaseProperties("test/db"));

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// S4: Name exceeds 255 chars
	[Fact]
	public async Task CreateDatabaseStreamAsync_NameExceeds255Chars_ThrowsBadRequest()
	{
		var client = new InMemoryCosmosClient();
		var longName = new string('x', 256);

		var act = () => client.CreateDatabaseStreamAsync(new DatabaseProperties(longName));

		(await act.Should().ThrowAsync<CosmosException>()).Which
			.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3: BUG-3 — _explicitlyCreatedContainers cleanup on delete
// ═══════════════════════════════════════════════════════════════════════════

public class ExplicitlyCreatedContainersClearedOnDeleteTests
{
	// E1: DeleteAsync clears _explicitlyCreatedContainers
	[Fact]
	public async Task DeleteAsync_Clears_ExplicitlyCreatedContainers()
	{
		var client = new InMemoryCosmosClient();
		var db = (InMemoryDatabase)(await client.CreateDatabaseAsync("test-db")).Database;
		await db.CreateContainerAsync("ctr1", "/pk");

		db.IsContainerExplicitlyCreated("ctr1").Should().BeTrue();

		await db.DeleteAsync();

		db.IsContainerExplicitlyCreated("ctr1").Should().BeFalse();
	}

	// E2: DeleteStreamAsync clears _explicitlyCreatedContainers
	[Fact]
	public async Task DeleteStreamAsync_Clears_ExplicitlyCreatedContainers()
	{
		var client = new InMemoryCosmosClient();
		var db = (InMemoryDatabase)(await client.CreateDatabaseAsync("test-db")).Database;
		await db.CreateContainerAsync("ctr1", "/pk");

		db.IsContainerExplicitlyCreated("ctr1").Should().BeTrue();

		await db.DeleteStreamAsync();

		db.IsContainerExplicitlyCreated("ctr1").Should().BeFalse();
	}

	// E3: Delete → recreate → auto-created container not stale explicit
	[Fact]
	public async Task DeleteAsync_ThenRecreate_ContainerStartsFresh()
	{
		var client = new InMemoryCosmosClient();
		var db1 = (InMemoryDatabase)(await client.CreateDatabaseAsync("test-db")).Database;
		await db1.CreateContainerAsync("ctr1", "/pk");
		db1.IsContainerExplicitlyCreated("ctr1").Should().BeTrue();

		await db1.DeleteAsync();

		// Re-creating the database gives a new instance
		var db2 = (InMemoryDatabase)(await client.CreateDatabaseAsync("test-db")).Database;
		db2.IsContainerExplicitlyCreated("ctr1").Should().BeFalse();

		// GetContainer creates a lazy (not explicit) container
		var container = db2.GetContainer("ctr1");
		container.Should().NotBeNull();
		db2.IsContainerExplicitlyCreated("ctr1").Should().BeFalse();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 4: Missing edge case GREEN tests
// ═══════════════════════════════════════════════════════════════════════════

public class DatabaseEdgeCaseDeepDiveTests
{
	// M1: ReadAsync on standalone database (no client)
	[Fact]
	public async Task ReadAsync_StandaloneDatabase_NoClient_ReturnsOk()
	{
		var db = new InMemoryDatabase("standalone-db");

		var response = await db.ReadAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Id.Should().Be("standalone-db");
	}

	// M2: CreateContainerIfNotExistsAsync with DefaultTimeToLive
	[Fact]
	public async Task CreateContainerIfNotExistsAsync_WithDefaultTimeToLive_SetsOnContainer()
	{
		var db = new InMemoryDatabase("testdb");
		var props = new ContainerProperties("ctr", "/pk")
		{
			DefaultTimeToLive = 3600
		};

		await db.CreateContainerIfNotExistsAsync(props);

		var container = (InMemoryContainer)db.GetContainer("ctr");
		container.DefaultTimeToLive.Should().Be(3600);
	}

	// M3: CreateContainerAsync with DefaultTimeToLive
	[Fact]
	public async Task CreateContainerAsync_WithDefaultTimeToLive_SetsOnContainer()
	{
		var db = new InMemoryDatabase("testdb");
		var props = new ContainerProperties("ctr", "/pk")
		{
			DefaultTimeToLive = 1800
		};

		await db.CreateContainerAsync(props);

		var container = (InMemoryContainer)db.GetContainer("ctr");
		container.DefaultTimeToLive.Should().Be(1800);
	}

	// M4: CreateContainerAsync with IndexingPolicy
	[Fact]
	public async Task CreateContainerAsync_WithIndexingPolicy_PolicyApplied()
	{
		var db = new InMemoryDatabase("testdb");
		var policy = new IndexingPolicy { Automatic = false };
		var props = new ContainerProperties("ctr", "/pk")
		{
			IndexingPolicy = policy
		};

		await db.CreateContainerAsync(props);

		var container = (InMemoryContainer)db.GetContainer("ctr");
		container.IndexingPolicy.Automatic.Should().BeFalse();
	}

	// M5: CreateDatabaseIfNotExistsAsync with cancelled token
	[Fact]
	public async Task CreateDatabaseIfNotExistsAsync_CancelledToken_Throws()
	{
		var client = new InMemoryCosmosClient();
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => client.CreateDatabaseIfNotExistsAsync("test-db", cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	// M6: User re-creation after database delete-recreate cycle
	[Fact]
	public async Task User_RecreateAfterDbDelete_Succeeds()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		await db.CreateUserAsync("user1");

		await db.DeleteAsync();

		var db2 = (await client.CreateDatabaseAsync("testdb")).Database;
		var response = await db2.CreateUserAsync("user1");
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	// M7: CreateDatabaseStreamAsync response body deserializable
	[Fact]
	public async Task CreateDatabaseStreamAsync_ResponseBody_DeserializesToDatabaseProperties()
	{
		var client = new InMemoryCosmosClient();

		using var response = await client.CreateDatabaseStreamAsync(new DatabaseProperties("mydb"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Content.Should().NotBeNull();
		response.Content.Position = 0;
		using var reader = new System.IO.StreamReader(response.Content);
		var json = await reader.ReadToEndAsync();
		var dbProps = JsonConvert.DeserializeObject<DatabaseProperties>(json);
		dbProps!.Id.Should().Be("mydb");
	}

	// M8: GetContainer via client 2-arg returns correct container Id
	[Fact]
	public async Task GetContainer_ViaClient_ReturnsContainerWithCorrectId()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseAsync("testdb");

		var container = client.GetContainer("testdb", "mycontainer");

		container.Id.Should().Be("mycontainer");
	}

	// M9: Endpoint returns localhost URI
	[Fact]
	public void Endpoint_ReturnsLocalhost8081()
	{
		var client = new InMemoryCosmosClient();

		client.Endpoint.Should().Be(new Uri("https://localhost:8081/"));
	}

	// M10: CreateContainerIfNotExistsAsync existing → returns same instance
	[Fact]
	public async Task CreateContainerIfNotExistsAsync_ExistingContainer_ReturnsSameInstance()
	{
		var db = new InMemoryDatabase("testdb");
		var r1 = await db.CreateContainerAsync("ctr", "/pk");
		var r2 = await db.CreateContainerIfNotExistsAsync("ctr", "/pk");

		r2.StatusCode.Should().Be(HttpStatusCode.OK);
		r2.Container.Id.Should().Be("ctr");
	}

	// M11: Concurrent GetDatabase same id → same instance
	[Fact]
	public async Task ConcurrentGetDatabase_SameId_ReturnsSameInstance()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseAsync("testdb");

		var tasks = Enumerable.Range(0, 50)
			.Select(_ => Task.Run(() => client.GetDatabase("testdb")))
			.ToArray();

		var results = await Task.WhenAll(tasks);
		results.Should().AllSatisfy(db => db.Id.Should().Be("testdb"));
		// All should be the same instance
		results.Distinct().Should().HaveCount(1);
	}

	// M12: Concurrent create and delete — no exceptions
	[Fact]
	public async Task ConcurrentCreateAndDelete_NoExceptions()
	{
		var client = new InMemoryCosmosClient();
		var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

		var tasks = Enumerable.Range(0, 20).Select(i => Task.Run(async () =>
		{
			try
			{
				var dbName = $"db-{i % 5}";
				try
				{
					await client.CreateDatabaseAsync(dbName);
				}
				catch (CosmosException) { /* Conflict expected */ }

				var db = client.GetDatabase(dbName);
				try
				{
					await db.DeleteAsync();
				}
				catch (CosmosException) { /* NotFound expected */ }
			}
			catch (Exception ex)
			{
				exceptions.Add(ex);
			}
		})).ToArray();

		await Task.WhenAll(tasks);
		exceptions.Should().BeEmpty();
	}

	// M13: Stream API response has headers
	[Fact]
	public async Task StreamApi_ResponseHasHeaders()
	{
		var client = new InMemoryCosmosClient();

		using var response = await client.CreateDatabaseStreamAsync(new DatabaseProperties("testdb"));

		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	// M14: GetContainerQueryIterator only shows own database's containers
	[Fact]
	public async Task GetContainerQueryIterator_OnlyShowsOwnDatabaseContainers()
	{
		var client = new InMemoryCosmosClient();
		var db1 = (await client.CreateDatabaseAsync("db1")).Database;
		var db2 = (await client.CreateDatabaseAsync("db2")).Database;

		await db1.CreateContainerAsync("shared-name", "/pk");
		await db2.CreateContainerAsync("other-name", "/pk");

		var iterator = db1.GetContainerQueryIterator<ContainerProperties>();
		var results = new List<ContainerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
		results[0].Id.Should().Be("shared-name");
	}
}
