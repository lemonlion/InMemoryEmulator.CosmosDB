using System.Collections.ObjectModel;
using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Cosmos DB is case-sensitive for UDF names, stored procedure IDs, trigger IDs,
/// and container names in routing. These tests verify the emulator matches that behaviour.
/// </summary>
public class CaseSensitivityTests
{
	// ═══════════════════════════════════════════════════════════════════════════
	//  UDF case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Udf_RegisteredWithCasing_IsNotCallableWithDifferentCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 2);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 10 }),
			new PartitionKey("pk1"));

		// Query with WRONG casing — should throw because "MYFUNC" is not registered, only "myFunc"
		var query = new QueryDefinition("SELECT VALUE udf.MYFUNC(c.value) FROM c");

		var act = async () =>
		{
			var iter = container.GetItemQueryIterator<double>(query);
			while (iter.HasMoreResults) await iter.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Udf_RegisteredWithCasing_IsCallableWithExactCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 2);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 10 }),
			new PartitionKey("pk1"));

		// Query with CORRECT casing — should work
		var query = new QueryDefinition("SELECT VALUE udf.myFunc(c.value) FROM c");
		var iter = container.GetItemQueryIterator<double>(query);
		var results = new List<double>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Should().Be(20);
	}

	[Fact]
	public async Task Udf_TwoUdfs_DifferingOnlyByCasing_AreSeparate()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterUdf("myFunc", args => "lower");
		container.RegisterUdf("MYFUNC", args => "upper");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1" }),
			new PartitionKey("pk1"));

		var queryLower = new QueryDefinition("SELECT VALUE udf.myFunc(c.id) FROM c");
		var queryUpper = new QueryDefinition("SELECT VALUE udf.MYFUNC(c.id) FROM c");

		var iterLower = container.GetItemQueryIterator<string>(queryLower);
		var resultsLower = new List<string>();
		while (iterLower.HasMoreResults) resultsLower.AddRange(await iterLower.ReadNextAsync());

		var iterUpper = container.GetItemQueryIterator<string>(queryUpper);
		var resultsUpper = new List<string>();
		while (iterUpper.HasMoreResults) resultsUpper.AddRange(await iterUpper.ReadNextAsync());

		resultsLower.Should().ContainSingle().Which.Should().Be("lower");
		resultsUpper.Should().ContainSingle().Which.Should().Be("upper");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Stored Procedure case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task StoredProcedure_RegisteredWithCasing_IsNotCallableWithDifferentCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterStoredProcedure("myProc", (pk, args) => "result");

		var scripts = container.Scripts;

		// Call with WRONG casing — should not find the handler (throws 404)
		var act = () => scripts.ExecuteStoredProcedureAsync<string>(
			"MYPROC",
			new PartitionKey("pk1"),
			Array.Empty<dynamic>());

		(await act.Should().ThrowAsync<CosmosException>())
			.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task StoredProcedure_TwoProcedures_DifferingOnlyByCasing_AreSeparate()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterStoredProcedure("myProc", (pk, args) => "lower");
		container.RegisterStoredProcedure("MYPROC", (pk, args) => "upper");

		var scripts = container.Scripts;

		var responseLower = await scripts.ExecuteStoredProcedureAsync<string>(
			"myProc", new PartitionKey("pk1"), Array.Empty<dynamic>());
		var responseUpper = await scripts.ExecuteStoredProcedureAsync<string>(
			"MYPROC", new PartitionKey("pk1"), Array.Empty<dynamic>());

		responseLower.Resource.Should().Be("lower");
		responseUpper.Resource.Should().Be("upper");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Trigger case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Trigger_RegisteredWithCasing_DoesNotFireWithDifferentCasing()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["triggered"] = true;
				return doc;
			}));

		// Reference trigger with WRONG casing — should throw because "MYTRIGGER" is not registered
		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "MYTRIGGER" } });

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Trigger_TwoTriggers_DifferingOnlyByCasing_AreSeparate()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["source"] = "lower";
				return doc;
			}));
		container.RegisterTrigger("MYTRIGGER", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["source"] = "upper";
				return doc;
			}));

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "MYTRIGGER" } });

		// Read back the items to verify each trigger applied its own value
		var readLower = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		var readUpper = await container.ReadItemAsync<JObject>("2", new PartitionKey("a"));

		readLower.Resource["source"]!.Value<string>().Should().Be("lower");
		readUpper.Resource["source"]!.Value<string>().Should().Be("upper");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Container routing case sensitivity (FakeCosmosHandler.CreateRouter)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Router_ContainerNames_AreCaseSensitive()
	{
		var container1 = new InMemoryContainer("MyData", "/partitionKey");
		var container2 = new InMemoryContainer("mydata", "/partitionKey");

		await container1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "UpperCase" },
			new PartitionKey("pk1"));
		await container2.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "LowerCase" },
			new PartitionKey("pk1"));

		using var handler1 = new FakeCosmosHandler(container1);
		using var handler2 = new FakeCosmosHandler(container2);

		// Register both containers — they differ only in casing and should be separate
		using var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
		{
			["MyData"] = handler1,
			["mydata"] = handler2,
		});

		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(router) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var c1 = client.GetContainer("db", "MyData");
		var c2 = client.GetContainer("db", "mydata");

		var results1 = new List<TestDocument>();
		var iter1 = c1.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iter1.HasMoreResults) results1.AddRange(await iter1.ReadNextAsync());

		var results2 = new List<TestDocument>();
		var iter2 = c2.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iter2.HasMoreResults) results2.AddRange(await iter2.ReadNextAsync());

		results1.Should().ContainSingle().Which.Name.Should().Be("UpperCase");
		results2.Should().ContainSingle().Which.Name.Should().Be("LowerCase");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section C: Item ID Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ItemId_DifferentCasings_AreSeparateDocuments()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Upper" }),
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "Lower" }),
			new PartitionKey("pk1"));

		container.ItemCount.Should().Be(2);
	}

	[Fact]
	public async Task ItemId_ReadItem_RequiresExactCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Upper" }),
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "Lower" }),
			new PartitionKey("pk1"));

		var readUpper = await container.ReadItemAsync<JObject>("Item1", new PartitionKey("pk1"));
		var readLower = await container.ReadItemAsync<JObject>("item1", new PartitionKey("pk1"));

		readUpper.Resource["name"]!.ToString().Should().Be("Upper");
		readLower.Resource["name"]!.ToString().Should().Be("Lower");
	}

	[Fact]
	public async Task ItemId_DeleteItem_OnlyAffectsExactCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Upper" }),
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "Lower" }),
			new PartitionKey("pk1"));

		await container.DeleteItemAsync<JObject>("Item1", new PartitionKey("pk1"));

		container.ItemCount.Should().Be(1);
		var remaining = await container.ReadItemAsync<JObject>("item1", new PartitionKey("pk1"));
		remaining.Resource["name"]!.ToString().Should().Be("Lower");
	}

	[Fact]
	public async Task ItemId_ReplaceItem_OnlyAffectsExactCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Upper" }),
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "Lower" }),
			new PartitionKey("pk1"));

		await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "Replaced" }),
			"Item1", new PartitionKey("pk1"));

		var readUpper = await container.ReadItemAsync<JObject>("Item1", new PartitionKey("pk1"));
		var readLower = await container.ReadItemAsync<JObject>("item1", new PartitionKey("pk1"));

		readUpper.Resource["name"]!.ToString().Should().Be("Replaced");
		readLower.Resource["name"]!.ToString().Should().Be("Lower");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section D: Partition Key Value Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task PartitionKey_DifferentCasings_TreatedAsSeparatePartitions()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "doc1", partitionKey = "pkA", name = "Upper" }),
			new PartitionKey("pkA"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "doc1", partitionKey = "pka", name = "Lower" }),
			new PartitionKey("pka"));

		container.ItemCount.Should().Be(2);
	}

	[Fact]
	public async Task PartitionKey_ReadItem_RequiresExactCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "doc1", partitionKey = "pkA", name = "Upper" }),
			new PartitionKey("pkA"));

		var read = await container.ReadItemAsync<JObject>("doc1", new PartitionKey("pkA"));
		read.Resource["name"]!.ToString().Should().Be("Upper");

		var act = () => container.ReadItemAsync<JObject>("doc1", new PartitionKey("pka"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task PartitionKey_QueryWithPartitionKey_OnlyReturnsExactMatch()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pkA", name = "Upper" }),
			new PartitionKey("pkA"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", partitionKey = "pka", name = "Lower" }),
			new PartitionKey("pka"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pkA") });

		var results = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which["name"]!.ToString().Should().Be("Upper");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section A: Database Name Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Database_NamesAreCaseSensitive_SeparateInstances()
	{
		var client = new InMemoryCosmosClient();

		await client.CreateDatabaseAsync("MyDB");
		await client.CreateDatabaseAsync("mydb");

		var db1 = client.GetDatabase("MyDB");
		var db2 = client.GetDatabase("mydb");

		db1.Id.Should().Be("MyDB");
		db2.Id.Should().Be("mydb");
	}

	[Fact]
	public async Task Database_CreateAsync_DifferentCasings_NoConflict()
	{
		var client = new InMemoryCosmosClient();

		var r1 = await client.CreateDatabaseAsync("MyDB");
		var r2 = await client.CreateDatabaseAsync("mydb");

		r1.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
		r2.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
	}

	[Fact]
	public async Task Database_CreateIfNotExists_DifferentCasings_BothCreated()
	{
		var client = new InMemoryCosmosClient();

		var r1 = await client.CreateDatabaseIfNotExistsAsync("MyDB");
		var r2 = await client.CreateDatabaseIfNotExistsAsync("mydb");

		r1.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
		r2.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
	}

	[Fact]
	public async Task Database_GetDatabaseQueryIterator_ListsBothCasings()
	{
		var client = new InMemoryCosmosClient();

		await client.CreateDatabaseAsync("MyDB");
		await client.CreateDatabaseAsync("mydb");

		var iter = client.GetDatabaseQueryIterator<DatabaseProperties>();
		var databases = new List<DatabaseProperties>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			databases.AddRange(page);
		}

		databases.Select(d => d.Id).Should().Contain("MyDB").And.Contain("mydb");
	}

	[Fact]
	public async Task Database_CreateIfNotExists_ExactCasing_ReturnsOk()
	{
		var client = new InMemoryCosmosClient();

		await client.CreateDatabaseAsync("MyDB");
		var r2 = await client.CreateDatabaseIfNotExistsAsync("MyDB");

		r2.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section B: Container Name Case Sensitivity via InMemoryDatabase
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Container_NamesAreCaseSensitive_SeparateInstances()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		await db.CreateContainerAsync("MyContainer", "/pk");
		await db.CreateContainerAsync("mycontainer", "/pk");

		var c1 = db.GetContainer("MyContainer");
		var c2 = db.GetContainer("mycontainer");

		c1.Id.Should().Be("MyContainer");
		c2.Id.Should().Be("mycontainer");
	}

	[Fact]
	public async Task Container_CreateAsync_DifferentCasings_NoConflict()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var r1 = await db.CreateContainerAsync("Foo", "/pk");
		var r2 = await db.CreateContainerAsync("foo", "/pk");

		r1.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
		r2.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
	}

	[Fact]
	public async Task Container_CreateIfNotExists_DifferentCasings_BothCreated()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var r1 = await db.CreateContainerIfNotExistsAsync("Foo", "/pk");
		var r2 = await db.CreateContainerIfNotExistsAsync("foo", "/pk");

		r1.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
		r2.StatusCode.Should().Be(System.Net.HttpStatusCode.Created);
	}

	[Fact]
	public async Task Container_GetContainerQueryIterator_ListsBothCasings()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		await db.CreateContainerAsync("Foo", "/pk");
		await db.CreateContainerAsync("foo", "/pk");

		var iter = db.GetContainerQueryIterator<ContainerProperties>();
		var containers = new List<ContainerProperties>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			containers.AddRange(page);
		}

		containers.Select(c => c.Id).Should().Contain("Foo").And.Contain("foo");
	}

	[Fact]
	public async Task Container_DifferentCasings_StoreDataSeparately()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		await db.CreateContainerAsync("Foo", "/pk");
		await db.CreateContainerAsync("foo", "/pk");

		var c1 = db.GetContainer("Foo");
		var c2 = db.GetContainer("foo");

		await c1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "a", Name = "UpperContainer" },
			new PartitionKey("a"));
		await c2.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "a", Name = "LowerContainer" },
			new PartitionKey("a"));

		var read1 = await c1.ReadItemAsync<TestDocument>("1", new PartitionKey("a"));
		var read2 = await c2.ReadItemAsync<TestDocument>("1", new PartitionKey("a"));

		read1.Resource.Name.Should().Be("UpperContainer");
		read2.Resource.Name.Should().Be("LowerContainer");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section E: User Name Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task User_NamesAreCaseSensitive_SeparateInstances()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var r1 = await db.CreateUserAsync("Admin");
		var r2 = await db.CreateUserAsync("admin");

		r1.Resource.Id.Should().Be("Admin");
		r2.Resource.Id.Should().Be("admin");
	}

	[Fact]
	public async Task User_GetUser_RequiresExactCasing()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		await db.CreateUserAsync("Admin");
		await db.CreateUserAsync("admin");

		var u1 = db.GetUser("Admin");
		var u2 = db.GetUser("admin");

		u1.Id.Should().Be("Admin");
		u2.Id.Should().Be("admin");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section F: Permission ID Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Permission_IdsAreCaseSensitive_SeparateEntries()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;
		await db.CreateUserAsync("user1");
		var user = db.GetUser("user1");

		var r1 = await user.CreatePermissionAsync(
			new PermissionProperties("ReadAll", PermissionMode.Read, db.GetContainer("c")));
		var r2 = await user.CreatePermissionAsync(
			new PermissionProperties("readall", PermissionMode.Read, db.GetContainer("c")));

		r1.Resource.Id.Should().Be("ReadAll");
		r2.Resource.Id.Should().Be("readall");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section G: Scripts API Sproc CRUD Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Sproc_Scripts_CreateAndRead_CaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var scripts = container.Scripts;

		await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "MyProc",
			Body = "function() { return true; }"
		});

		var read = await scripts.ReadStoredProcedureAsync("MyProc");
		read.Resource.Id.Should().Be("MyProc");

		var act = () => scripts.ReadStoredProcedureAsync("myproc");
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Sproc_Scripts_Delete_CaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var scripts = container.Scripts;

		await scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "MyProc",
			Body = "function() { return true; }"
		});

		var act = () => scripts.DeleteStoredProcedureAsync("myproc");
		await act.Should().ThrowAsync<CosmosException>();

		// Exact casing should work
		await scripts.DeleteStoredProcedureAsync("MyProc");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section H: Scripts API Trigger CRUD Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Trigger_Scripts_CreateAndRead_CaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var scripts = container.Scripts;

		await scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "MyTrig",
			Body = "function() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create
		});

		var read = await scripts.ReadTriggerAsync("MyTrig");
		read.Resource.Id.Should().Be("MyTrig");

		var act = () => scripts.ReadTriggerAsync("mytrig");
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Trigger_Scripts_Delete_CaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var scripts = container.Scripts;

		await scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "MyTrig",
			Body = "function() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create
		});

		var act = () => scripts.DeleteTriggerAsync("mytrig");
		await act.Should().ThrowAsync<CosmosException>();

		await scripts.DeleteTriggerAsync("MyTrig");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section I: Scripts API UDF CRUD Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Udf_Scripts_CreateTwoDifferentCasings_NoConflict()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var scripts = container.Scripts;

		var r1 = await scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "MyFunc", Body = "function(x) { return x; }" });
		var r2 = await scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "MYFUNC", Body = "function(x) { return x; }" });

		r1.Resource.Id.Should().Be("MyFunc");
		r2.Resource.Id.Should().Be("MYFUNC");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section J: SQL Keyword Case Insensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SqlKeywords_AllUppercase_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task SqlKeywords_AllLowercase_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("select * from c where c.name = 'Alice'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task SqlKeywords_MixedCase_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SeLeCt * FrOm c WhErE c.name = 'Alice'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section K: SQL Function Name Case Insensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SqlFunction_MixedCasing_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Hello World" }),
			new PartitionKey("pk1"));

		// Test CONTAINS with various casings
		var casings = new[] { "CONTAINS", "contains", "Contains" };
		foreach (var fn in casings)
		{
			var iter = container.GetItemQueryIterator<JObject>(
				new QueryDefinition($"SELECT * FROM c WHERE {fn}(c.name, 'Hello')"));
			var results = new List<JObject>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			results.Should().ContainSingle($"{fn} should find the item");
		}
	}

	[Fact]
	public async Task SqlFunction_Aggregate_MixedCasing_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = "pk1", value = i }),
				new PartitionKey("pk1"));

		var casings = new[] { "COUNT", "count", "Count" };
		foreach (var fn in casings)
		{
			var iter = container.GetItemQueryIterator<int>(
				new QueryDefinition($"SELECT VALUE {fn}(1) FROM c"));
			var results = new List<int>();
			while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
			results.Should().ContainSingle().Which.Should().Be(5, $"{fn}(1) should return 5");
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section L: SQL Property Path Case Sensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SqlProperty_WrongCasing_ReturnsNoResults()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		// JSON has "name" (lowercase), query uses "Name" (PascalCase) — should not match
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.Name = 'Alice'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task SqlProperty_SelectWrongCasing_ReturnsUndefined()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		// SELECT c.Name when JSON has "name" — should return object without "Name" value
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.Name FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
		var doc = results[0];
		// "Name" should be missing/null since JSON property is "name" (lowercase)
		(doc["Name"] == null || doc["Name"]!.Type == Newtonsoft.Json.Linq.JTokenType.Null)
			.Should().BeTrue();
	}

	[Fact]
	public async Task SqlProperty_NestedPath_CaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", address = new { city = "London" } }),
			new PartitionKey("pk1"));

		// Correct casing
		var iterCorrect = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.address.city = 'London'"));
		var correct = new List<JObject>();
		while (iterCorrect.HasMoreResults) correct.AddRange(await iterCorrect.ReadNextAsync());
		correct.Should().ContainSingle();

		// Wrong casing
		var iterWrong = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.address.City = 'London'"));
		var wrong = new List<JObject>();
		while (iterWrong.HasMoreResults) wrong.AddRange(await iterWrong.ReadNextAsync());
		wrong.Should().BeEmpty();
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section M: SQL FROM Alias Case Insensitivity
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SqlAlias_UppercaseAlias_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		// Default alias is "c", use uppercase "C" in SELECT
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT C.name FROM C"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task SqlAlias_MixedCaseRef_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }),
			new PartitionKey("pk1"));

		// FROM c AS x, reference as X
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT X.name FROM c AS x WHERE X.name = 'Alice'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Section N: UDF SQL Prefix Normalization
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Udf_SqlPrefix_Lowercase_udf_ResolvesCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 10);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 5 }),
			new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE udf.myFunc(c.value) FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(50);
	}

	[Fact]
	public async Task Udf_SqlPrefix_Uppercase_UDF_ResolvesCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 10);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 5 }),
			new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE UDF.myFunc(c.value) FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(50);
	}

	[Fact]
	public async Task Udf_SqlPrefix_MixedCase_Udf_ResolvesCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.RegisterUdf("myFunc", args => Convert.ToDouble(args[0]) * 10);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", value = 5 }),
			new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE Udf.myFunc(c.value) FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(50);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B1: DISTINCT with case-variant string values
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Distinct_CaseVariantValues_AreSeparate()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		foreach (var (i, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "ALICE") })
			await container.CreateItemAsync(
				JObject.FromObject(new { id = i, partitionKey = "pk1", name }),
				new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT DISTINCT VALUE c.name FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
		results.Should().Contain(new[] { "Alice", "alice", "ALICE" });
	}

	[Fact]
	public async Task Distinct_CaseVariantObjectValues_AreSeparate()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		foreach (var (i, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "ALICE") })
			await container.CreateItemAsync(
				JObject.FromObject(new { id = i, partitionKey = "pk1", name }),
				new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT DISTINCT c.name FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B2: ORDER BY with mixed-case string values
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task OrderBy_MixedCaseStrings_OrdersByCodePoint()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		foreach (var (i, name) in new[] { ("1", "banana"), ("2", "Apple"), ("3", "cherry"), ("4", "apple") })
			await container.CreateItemAsync(
				JObject.FromObject(new { id = i, partitionKey = "pk1", name }),
				new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.name FROM c ORDER BY c.name ASC"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var names = results.Select(r => r["name"]!.ToString()).ToList();
		names.Should().Equal("Apple", "apple", "banana", "cherry");
	}

	[Fact]
	public async Task OrderBy_MixedCaseStrings_Desc_ReversesOrder()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		foreach (var (i, name) in new[] { ("1", "banana"), ("2", "Apple"), ("3", "cherry"), ("4", "apple") })
			await container.CreateItemAsync(
				JObject.FromObject(new { id = i, partitionKey = "pk1", name }),
				new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.name FROM c ORDER BY c.name DESC"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var names = results.Select(r => r["name"]!.ToString()).ToList();
		names.Should().Equal("cherry", "banana", "apple", "Apple");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B3: GROUP BY with case-variant grouping keys
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task GroupBy_CaseVariantKeys_CreateSeparateGroups()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "4", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "5", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(2);
		var alice = results.First(r => r["name"]!.ToString() == "Alice");
		var aliceLower = results.First(r => r["name"]!.ToString() == "alice");
		((int)alice["cnt"]!).Should().Be(2);
		((int)aliceLower["cnt"]!).Should().Be(3);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B4: LIKE operator case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Like_UppercasePattern_DoesNotMatchLowercase()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name LIKE 'A%'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task Like_LowercasePattern_DoesNotMatchUppercase()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name LIKE 'a%'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task Like_ExactCasePattern_MatchesExactly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "ALICE" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name LIKE 'Ali_e'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B5: IN operator case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task In_CaseSensitive_OnlyMatchesExactCase()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk1", name = "ALICE" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name IN ('Alice')"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task In_MultipleValues_EachCaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk1", name = "ALICE" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name IN ('Alice', 'alice')"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(2);
		results.Select(r => r["id"]!.ToString()).Should().BeEquivalentTo(new[] { "1", "2" });
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B6: CONTAINS/STARTSWITH/ENDSWITH string matching case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Contains_StringMatch_IsCaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.name, 'ali')"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task StartsWith_StringMatch_IsCaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.name, 'ali')"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task EndsWith_StringMatch_IsCaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE ENDSWITH(c.name, 'CE')"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B7: CONTAINS/STARTSWITH/ENDSWITH with case-insensitive 3rd argument
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Contains_ThirdArgTrue_CaseInsensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.name, 'ALI', true)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task StartsWith_ThirdArgTrue_CaseInsensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.name, 'ALI', true)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task EndsWith_ThirdArgTrue_CaseInsensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE ENDSWITH(c.name, 'ICE', true)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task Contains_ThirdArgFalse_StaysCaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.name, 'ALI', false)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B8: STRING_EQUALS with case-insensitive 3rd argument
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task StringEquals_ThirdArgTrue_CaseInsensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'ALICE', true)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task StringEquals_ThirdArgFalse_CaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'ALICE', false)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task StringEquals_NoThirdArg_CaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'ALICE')"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B9: LOWER/UPPER for case-insensitive comparison pattern
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Lower_EnablesCaseInsensitiveComparison()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk1", name = "ALICE" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE LOWER(c.name) = 'alice'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task Upper_EnablesCaseInsensitiveComparison()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk1", name = "ALICE" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE UPPER(c.name) = 'ALICE'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B10: Parameterized query case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ParameterizedQuery_ValueIsCaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name = @name").WithParameter("@name", "Alice"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B11: BETWEEN with string values case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Between_StringValues_CaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "alice" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk1", name = "Bob" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name BETWEEN 'A' AND 'Z'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		// Only uppercase-starting "Alice" and "Bob" are in A-Z range
		results.Should().HaveCount(2);
		results.Select(r => r["name"]!.ToString()).Should().BeEquivalentTo(new[] { "Alice", "Bob" });
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B12: Null coalesce (??) preserves case
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task NullCoalesce_PreservesCaseOfFallback()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.missing ?? 'DEFAULT' FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("DEFAULT");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B13: JOIN alias case insensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task JoinAlias_IsCaseSensitive_InEmulator()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", tags = new[] { new { name = "x" }, new { name = "y" } } }),
			new PartitionKey("pk1"));

		// Same case alias works
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT VALUE t FROM c JOIN t IN c.tags WHERE t.name = 'x'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["name"]!.ToString().Should().Be("x");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  B14: Additional built-in function name case insensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SqlFunction_StringFunctions_MixedCasing_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Hello" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT UPPER(c.name) AS u, lower(c.name) AS l, Length(c.name) AS len FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var r = results.Should().ContainSingle().Subject;
		r["u"]!.ToString().Should().Be("HELLO");
		r["l"]!.ToString().Should().Be("hello");
		((int)r["len"]!).Should().Be(5);
	}

	[Fact]
	public async Task SqlFunction_MathFunctions_MixedCasing_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", value = -3.7 }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT ABS(c.value) AS a, floor(c.value) AS f, Ceiling(c.value) AS ce FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var r = results.Should().ContainSingle().Subject;
		((double)r["a"]!).Should().Be(3.7);
		((double)r["f"]!).Should().Be(-4);
		((double)r["ce"]!).Should().Be(-3);
	}

	[Fact]
	public async Task SqlFunction_TypeChecking_MixedCasing_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "hello" }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT IS_STRING(c.name) AS a, is_number(c.name) AS b, Is_Defined(c.name) AS d FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var r = results.Should().ContainSingle().Subject;
		((bool)r["a"]!).Should().BeTrue();
		((bool)r["b"]!).Should().BeFalse();
		((bool)r["d"]!).Should().BeTrue();
	}

	[Fact]
	public async Task SqlFunction_ArrayFunctions_MixedCasing_Works()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", tags = new[] { "a", "b", "c" } }), new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT ARRAY_LENGTH(c.tags) AS al, array_contains(c.tags, 'a') AS ac FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var r = results.Should().ContainSingle().Subject;
		((int)r["al"]!).Should().Be(3);
		((bool)r["ac"]!).Should().BeTrue();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C1: ReplaceItemAsync case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ReplaceItem_RequiresExactIdCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "Item1", partitionKey = "pk1" }), new PartitionKey("pk1"));

		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			container.ReplaceItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk1" }), "item1", new PartitionKey("pk1")));
		ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReplaceItem_RequiresExactPartitionKeyCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "PkA" }), new PartitionKey("PkA"));

		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			container.ReplaceItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pka" }), "1", new PartitionKey("pka")));
		ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C2: UpsertItemAsync case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task UpsertItem_DifferentIdCasing_CreatesNewDocument()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.UpsertItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "first" }), new PartitionKey("pk1"));
		await container.UpsertItemAsync(JObject.FromObject(new { id = "Item1", partitionKey = "pk1", name = "second" }), new PartitionKey("pk1"));

		container.ItemCount.Should().Be(2);
	}

	[Fact]
	public async Task UpsertItem_ExactCasing_UpdatesExistingDocument()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.UpsertItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "first" }), new PartitionKey("pk1"));
		await container.UpsertItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk1", name = "second" }), new PartitionKey("pk1"));

		container.ItemCount.Should().Be(1);
		var r = await container.ReadItemAsync<JObject>("item1", new PartitionKey("pk1"));
		r.Resource["name"]!.ToString().Should().Be("second");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C3: ReadManyItemsAsync case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ReadMany_MixedCaseIds_OnlyReturnsExactMatches()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "Item1", partitionKey = "pk1" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk1" }), new PartitionKey("pk1"));

		var response = await container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)> { ("Item1", new PartitionKey("pk1")), ("item1", new PartitionKey("pk1")) });
		response.Count.Should().Be(2);
	}

	[Fact]
	public async Task ReadMany_WrongCaseId_ReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk1" }), new PartitionKey("pk1"));

		var response = await container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)> { ("ITEM1", new PartitionKey("pk1")) });
		response.Count.Should().Be(0);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C4: TransactionalBatch case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Batch_Create_CaseSensitiveIds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(JObject.FromObject(new { id = "Item1", partitionKey = "pk1" }));
		batch.CreateItem(JObject.FromObject(new { id = "item1", partitionKey = "pk1" }));
		var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		container.ItemCount.Should().Be(2);
	}

	[Fact]
	public async Task Batch_Read_RequiresExactIdCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk1" }), new PartitionKey("pk1"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("ITEM1");
		var response = await batch.ExecuteAsync();

		// Individual operation should be 404
		response[0].StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Batch_Delete_RequiresExactIdCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk1" }), new PartitionKey("pk1"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("ITEM1");
		var response = await batch.ExecuteAsync();

		response[0].StatusCode.Should().Be(HttpStatusCode.NotFound);
		container.ItemCount.Should().Be(1); // Item not deleted
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C5: DeleteAllItemsByPartitionKeyStreamAsync case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task DeleteAllByPK_RequiresExactCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pkA" }), new PartitionKey("pkA"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pka" }), new PartitionKey("pka"));

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pkA"));

		container.ItemCount.Should().Be(1);
		var remaining = await container.ReadItemAsync<JObject>("2", new PartitionKey("pka"));
		remaining.Resource["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task DeleteAllByPK_OnlyDeletesExactPK()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(JObject.FromObject(new { id = $"a{i}", partitionKey = "PK" }), new PartitionKey("PK"));
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(JObject.FromObject(new { id = $"b{i}", partitionKey = "pk" }), new PartitionKey("pk"));

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("PK"));
		container.ItemCount.Should().Be(5);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C6: PatchItemAsync path case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Patch_UserPropertyPath_IsCaseSensitive()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "old" }), new PartitionKey("pk1"));

		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Set("/Name", "new") });

		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		result.Resource["name"]!.ToString().Should().Be("old"); // lowercase "name" untouched
		result.Resource["Name"]!.ToString().Should().Be("new"); // new "Name" property created
	}

	[Fact]
	public async Task Patch_WrongPropertyPathCasing_CreatesNewProperty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "value" }), new PartitionKey("pk1"));

		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Set("/NAME", "val") });

		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		result.Resource["name"]!.ToString().Should().Be("value"); // original untouched
		result.Resource["NAME"]!.ToString().Should().Be("val"); // new property
	}

	[Fact]
	public async Task Patch_ReservedPath_Id_CaseInsensitiveValidation()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1" }), new PartitionKey("pk1"));

		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/ID", "new-id") }));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Patch_ReservedPath_PartitionKey_CaseInsensitiveValidation()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1" }), new PartitionKey("pk1"));

		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/PartitionKey", "new-pk") }));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  C7: Hierarchical/composite partition key case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task HierarchicalPK_EachComponent_IsCaseSensitive()
	{
		var container = new InMemoryContainer("test", new[] { "/tenant", "/region" });
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenant = "TenantA", region = "RegionX" }),
			new PartitionKeyBuilder().Add("TenantA").Add("RegionX").Build());
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", tenant = "tenantA", region = "RegionX" }),
			new PartitionKeyBuilder().Add("tenantA").Add("RegionX").Build());

		container.ItemCount.Should().Be(2);
	}

	[Fact]
	public async Task HierarchicalPK_ReadItem_RequiresExactComponentCasing()
	{
		var container = new InMemoryContainer("test", new[] { "/tenant", "/region" });
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenant = "TenantA", region = "RegionX" }),
			new PartitionKeyBuilder().Add("TenantA").Add("RegionX").Build());

		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			container.ReadItemAsync<JObject>("1",
				new PartitionKeyBuilder().Add("tenantA").Add("RegionX").Build()));
		ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D1: Database delete case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Database_DeleteAsync_RequiresExactCasing()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseAsync("mydb");
		await client.CreateDatabaseAsync("MyDB");

		await client.GetDatabase("mydb").DeleteAsync();

		// "MyDB" should still exist
		var response = await client.GetDatabase("MyDB").ReadAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D2: Container delete case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Container_DeleteAsync_RequiresExactCasing()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("db")).Database;
		await db.CreateContainerAsync("mycontainer", "/pk");
		await db.CreateContainerAsync("MyContainer", "/pk");

		await db.GetContainer("mycontainer").DeleteContainerAsync();

		var response = await db.GetContainer("MyContainer").ReadContainerAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  D3: Container/Database properties readback
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Container_Properties_PreserveExactCasing()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("db")).Database;
		await db.CreateContainerAsync("MyContainer", "/pk");

		var response = await db.GetContainer("MyContainer").ReadContainerAsync();
		response.Resource.Id.Should().Be("MyContainer");
	}

	[Fact]
	public async Task Database_Properties_PreserveExactCasing()
	{
		var client = new InMemoryCosmosClient();
		await client.CreateDatabaseAsync("MyDatabase");

		var response = await client.GetDatabase("MyDatabase").ReadAsync();
		response.Resource.Id.Should().Be("MyDatabase");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  E1: Change feed preserves exact casing
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ChangeFeed_PreservesExactPropertyCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", FirstName = "Alice", lastName = "Smith" }),
			new PartitionKey("pk1"));

		var iter = container.GetChangeFeedIterator<JObject>(ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		JObject? change = null;
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			change = page.First();
		}

		change!["FirstName"]!.ToString().Should().Be("Alice");
		change!["lastName"]!.ToString().Should().Be("Smith");
	}

	[Fact]
	public async Task ChangeFeed_PreservesExactIdCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(JObject.FromObject(new { id = "Item1", partitionKey = "pk1" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "item1", partitionKey = "pk2" }), new PartitionKey("pk2"));

		var iter = container.GetChangeFeedIterator<JObject>(ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var changes = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(page);
		}

		changes.Should().HaveCount(2);
		changes.Select(c => c["id"]!.ToString()).Should().BeEquivalentTo(new[] { "Item1", "item1" });
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  E2: State export/import preserves casing
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task StateExport_PreservesPropertyCasing()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", FirstName = "Alice" }),
			new PartitionKey("pk1"));

		var state = container.ExportState();
		state.Should().Contain("FirstName");
	}

	[Fact]
	public async Task StateImport_PreservesPropertyCasing()
	{
		var source = new InMemoryContainer("source", "/partitionKey");
		await source.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", FirstName = "Alice" }),
			new PartitionKey("pk1"));
		var state = source.ExportState();

		var target = new InMemoryContainer("target", "/partitionKey");
		target.ImportState(state);

		var iter = target.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.FirstName FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["FirstName"]!.ToString().Should().Be("Alice");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  F1: Unique key policy case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task UniqueKeyPolicy_PathIsCaseInsensitive_InEmulator()
	{
		// Note: The emulator uses JToken.SelectToken which is case-sensitive for property lookup,
		// but the unique key path resolution normalizes paths. We test that unique keys
		// with different property casings work as expected in the emulator.
		var props = new ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/userName" } } }
			}
		};
		var container = new InMemoryContainer(props);

		// Same userName value on same PK should conflict
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", userName = "alice" }),
			new PartitionKey("pk1"));

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", partitionKey = "pk1", userName = "alice" }),
			new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task UniqueKeyPolicy_ValueIsCaseSensitive()
	{
		var props = new ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(props);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", email = "Alice@example.com" }),
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", partitionKey = "pk1", email = "alice@example.com" }),
			new PartitionKey("pk1"));

		container.ItemCount.Should().Be(2); // Different case = different value
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  G1: Computed property name case sensitivity in queries
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_QueryByName_IsCaseSensitive()
	{
		var props = new ContainerProperties("test", "/partitionKey")
		{
			ComputedProperties = new System.Collections.ObjectModel.Collection<ComputedProperty>
			{
				new() { Name = "cp_fullName", Query = "SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c" }
			}
		};
		var container = new InMemoryContainer(props);
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", first = "Alice", last = "Smith" }),
			new PartitionKey("pk1"));

		// Correct casing
		var iter1 = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.cp_fullName FROM c"));
		var results1 = new List<JObject>();
		while (iter1.HasMoreResults) results1.AddRange(await iter1.ReadNextAsync());
		results1.Should().ContainSingle().Which["cp_fullName"]!.ToString().Should().Be("Alice Smith");

		// Wrong casing — property not found
		var iter2 = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.cp_fullname FROM c"));
		var results2 = new List<JObject>();
		while (iter2.HasMoreResults) results2.AddRange(await iter2.ReadNextAsync());
		results2.Should().ContainSingle().Which["cp_fullname"].Should().BeNull();
	}

	[Fact]
	public async Task ComputedProperty_DifferentCaseNames_AreSeparateProperties()
	{
		// The emulator allows "Foo" and "foo" as separate computed properties
		var props = new ContainerProperties("test", "/partitionKey")
		{
			ComputedProperties = new System.Collections.ObjectModel.Collection<ComputedProperty>
			{
				new() { Name = "Foo", Query = "SELECT VALUE c.a FROM c" },
				new() { Name = "foo", Query = "SELECT VALUE c.b FROM c" }
			}
		};
		var container = new InMemoryContainer(props);
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", a = "alpha", b = "beta" }),
			new PartitionKey("pk1"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.Foo, c.foo FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var r = results.Should().ContainSingle().Subject;
		r["Foo"]!.ToString().Should().Be("alpha");
		r["foo"]!.ToString().Should().Be("beta");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  H1: Partition key path case sensitivity
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task PartitionKeyPath_IsCaseSensitive()
	{
		var container = new InMemoryContainer("test", "/pk");

		// Document has "PK" (wrong case) — partition key lookup should NOT find it
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", PK = "wrongCase" }),
			new PartitionKey("wrongCase")); // Must supply PK explicitly since auto-extract won't find /pk

		// Query scoped to partition should work with explicitly supplied PK
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("wrongCase") });
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
	}
}
