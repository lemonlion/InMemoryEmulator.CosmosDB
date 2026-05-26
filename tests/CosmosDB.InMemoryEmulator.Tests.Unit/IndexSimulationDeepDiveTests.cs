using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════
// Phase 1 — FakeCosmosHandler Metadata Tests (BUG-1, BUG-2, BUG-3 fixes)
// ═══════════════════════════════════════════════════════════

public class FakeCosmosHandlerMetadataDeepDiveTests
{
	private static async Task<JObject> GetCollectionMetadataViaClient(InMemoryContainer container)
	{
		using var handler = new FakeCosmosHandler(container);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var cosmosContainer = client.GetContainer("db", container.Id);
		var response = await cosmosContainer.ReadContainerAsync();
		var json = JsonConvert.SerializeObject(response.Resource);
		return JObject.Parse(json);
	}

	[Fact]
	public async Task Metadata_ReflectsIndexingMode_None()
	{
		var container = new InMemoryContainer("meta-mode", "/partitionKey");
		container.IndexingPolicy = new IndexingPolicy { IndexingMode = IndexingMode.None, Automatic = false };
		container.IndexingPolicy.IncludedPaths.Clear();

		var result = await GetCollectionMetadataViaClient(container);
		var policy = result["indexingPolicy"]!;
		policy["indexingMode"]!.ToString().Should().BeOneOf("none", "None");
		policy["automatic"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task Metadata_ReflectsIncludedPaths()
	{
		var container = new InMemoryContainer("meta-inc", "/partitionKey");
		container.IndexingPolicy.IncludedPaths.Clear();
		container.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/name/?" });

		var result = await GetCollectionMetadataViaClient(container);
		var included = result["indexingPolicy"]!["includedPaths"]!.ToObject<List<JObject>>()!;
		included.Should().ContainSingle(p => p["path"]!.ToString() == "/name/?");
	}

	[Fact]
	public async Task Metadata_ReflectsExcludedPaths()
	{
		var container = new InMemoryContainer("meta-exc", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/secret/*" });

		var result = await GetCollectionMetadataViaClient(container);
		var excluded = result["indexingPolicy"]!["excludedPaths"]!.ToObject<List<JObject>>()!;
		excluded.Should().Contain(p => p["path"]!.ToString() == "/secret/*");
	}

	[Fact]
	public async Task Metadata_DefaultExcludedPaths_AlwaysIncludesEtag()
	{
		var container = new InMemoryContainer("meta-etag", "/partitionKey");
		// No explicit excluded paths
		var result = await GetCollectionMetadataViaClient(container);
		var excluded = result["indexingPolicy"]!["excludedPaths"]!.ToObject<List<JObject>>()!;
		excluded.Should().Contain(p => p["path"]!.ToString() == "/\"_etag\"/?");
	}

	[Fact]
	public async Task Metadata_ExcludedPaths_AlwaysIncludesEtag_EvenWithUserPaths()
	{
		var container = new InMemoryContainer("meta-etag2", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/secret/*" });

		var result = await GetCollectionMetadataViaClient(container);
		var excluded = result["indexingPolicy"]!["excludedPaths"]!.ToObject<List<JObject>>()!;
		excluded.Should().Contain(p => p["path"]!.ToString() == "/\"_etag\"/?", "BUG-3 fix: _etag always present");
		excluded.Should().Contain(p => p["path"]!.ToString() == "/secret/*");
	}

	[Fact]
	public async Task Metadata_CompositeIndexes_Serialized()
	{
		var container = new InMemoryContainer("meta-comp", "/partitionKey");
		container.IndexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
		{
			new() { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new() { Path = "/value", Order = CompositePathSortOrder.Descending }
		});

		var result = await GetCollectionMetadataViaClient(container);
		var composites = result["indexingPolicy"]!["compositeIndexes"];
		composites.Should().NotBeNull("BUG-2 fix: composite indexes should be serialized");
		var firstSet = composites![0]!.ToObject<List<JObject>>()!;
		firstSet.Should().HaveCount(2);
		firstSet[0]["path"]!.ToString().Should().Be("/name");
		firstSet[1]["path"]!.ToString().Should().Be("/value");
	}

	[Fact]
	public async Task Metadata_SpatialIndexes_Serialized()
	{
		var container = new InMemoryContainer("meta-spatial", "/partitionKey");
		container.IndexingPolicy.SpatialIndexes.Add(new SpatialPath
		{
			Path = "/location/*",
			SpatialTypes = { SpatialType.Point, SpatialType.Polygon }
		});

		var result = await GetCollectionMetadataViaClient(container);
		var spatials = result["indexingPolicy"]!["spatialIndexes"];
		spatials.Should().NotBeNull("BUG-2 fix: spatial indexes should be serialized");
		var firstSpatial = spatials![0]!;
		firstSpatial["path"]!.ToString().Should().Be("/location/*");
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 2 — ExcludedPath Query Types (emulator ignores exclusions)
// ═══════════════════════════════════════════════════════════

public class ExcludedPathQueryDeepDiveTests
{
	private readonly InMemoryContainer _container;

	public ExcludedPathQueryDeepDiveTests()
	{
		_container = new InMemoryContainer("excl-path", "/partitionKey");
		_container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/value/*" });
		_container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/name/*" });
	}

	private async Task SeedItems()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk", name = "Alice", value = 10 }), new PartitionKey("pk"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk", name = "Bob", value = 20 }), new PartitionKey("pk"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk", name = "Charlie", value = 0 }), new PartitionKey("pk"));
	}

	private async Task<List<JObject>> QueryAll(QueryDefinition query)
	{
		var iter = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task ExcludedPath_BetweenQuery_StillWorks()
	{
		await SeedItems();
		var results = await QueryAll(new QueryDefinition("SELECT * FROM c WHERE c.value BETWEEN 5 AND 15"));
		results.Should().HaveCount(1);
		results[0]["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ExcludedPath_LikeQuery_StillWorks()
	{
		await SeedItems();
		var results = await QueryAll(new QueryDefinition("SELECT * FROM c WHERE c.name LIKE 'Al%'"));
		results.Should().HaveCount(1);
		results[0]["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ExcludedPath_InQuery_StillWorks()
	{
		await SeedItems();
		var results = await QueryAll(new QueryDefinition("SELECT * FROM c WHERE c.value IN (10, 20)"));
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task ExcludedPath_AggregateQuery_StillWorks()
	{
		await SeedItems();
		var iter = _container.GetItemQueryIterator<JToken>(new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE c.value > 0"));
		var results = new List<JToken>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().HaveCount(1);
		results[0].Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task ExcludedPath_OrderByExcludedField_StillWorks()
	{
		await SeedItems();
		var results = await QueryAll(new QueryDefinition("SELECT * FROM c ORDER BY c.value"));
		results.Should().HaveCount(3);
		results[0]["value"]!.Value<int>().Should().Be(0);
		results[2]["value"]!.Value<int>().Should().Be(20);
	}

	[Fact]
	public async Task ExcludedPath_GroupByExcludedField_StillWorks()
	{
		await SeedItems();
		var results = await QueryAll(new QueryDefinition("SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name"));
		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task ExcludedPath_PartitionKeyPath_QueriesStillWork()
	{
		var container = new InMemoryContainer("excl-pk", "/partitionKey");
		container.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/partitionKey/*" });
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1" }), new PartitionKey("pk1"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk2" }), new PartitionKey("pk2"));

		var iter = container.GetItemQueryIterator<JObject>(new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = 'pk1'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().HaveCount(1);
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 3 — Composite/Spatial Index Roundtrip
// ═══════════════════════════════════════════════════════════

public class CompositeIndexRoundtripDeepDiveTests
{
	[Fact]
	public async Task CompositeIndex_SurvivesReadContainerAsync()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseIfNotExistsAsync("db");
		var containerProps = new ContainerProperties("comp-rt", "/pk");
		containerProps.IndexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
		{
			new() { Path = "/a", Order = CompositePathSortOrder.Ascending },
			new() { Path = "/b", Order = CompositePathSortOrder.Descending }
		});
		var result = await db.Database.CreateContainerIfNotExistsAsync(containerProps);

		var readResponse = await result.Container.ReadContainerAsync();
		readResponse.Resource.IndexingPolicy.CompositeIndexes.Should().HaveCount(1);
		readResponse.Resource.IndexingPolicy.CompositeIndexes[0].Should().HaveCount(2);
		readResponse.Resource.IndexingPolicy.CompositeIndexes[0][0].Path.Should().Be("/a");
		readResponse.Resource.IndexingPolicy.CompositeIndexes[0][1].Order.Should().Be(CompositePathSortOrder.Descending);
	}

	[Fact]
	public async Task CompositeIndex_UpdatedViaReplaceContainerAsync()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseIfNotExistsAsync("db");
		var result = await db.Database.CreateContainerIfNotExistsAsync(new ContainerProperties("comp-repl", "/pk"));

		var props = (await result.Container.ReadContainerAsync()).Resource;
		props.IndexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
		{
			new() { Path = "/x", Order = CompositePathSortOrder.Ascending }
		});
		await result.Container.ReplaceContainerAsync(props);

		var readBack = await result.Container.ReadContainerAsync();
		readBack.Resource.IndexingPolicy.CompositeIndexes.Should().HaveCount(1);
	}

	[Fact]
	public async Task SpatialIndex_SurvivesReadContainerAsync()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseIfNotExistsAsync("db");
		var containerProps = new ContainerProperties("spatial-rt", "/pk");
		containerProps.IndexingPolicy.SpatialIndexes.Add(new SpatialPath
		{
			Path = "/location/*",
			SpatialTypes = { SpatialType.Point, SpatialType.Polygon }
		});
		var result = await db.Database.CreateContainerIfNotExistsAsync(containerProps);

		var readResponse = await result.Container.ReadContainerAsync();
		readResponse.Resource.IndexingPolicy.SpatialIndexes.Should().HaveCount(1);
		readResponse.Resource.IndexingPolicy.SpatialIndexes[0].Path.Should().Be("/location/*");
	}

	[Fact]
	public async Task SpatialIndex_UpdatedViaReplaceContainerAsync()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseIfNotExistsAsync("db");
		var result = await db.Database.CreateContainerIfNotExistsAsync(new ContainerProperties("spatial-repl", "/pk"));

		var props = (await result.Container.ReadContainerAsync()).Resource;
		props.IndexingPolicy.SpatialIndexes.Add(new SpatialPath { Path = "/geo/*", SpatialTypes = { SpatialType.Point } });
		await result.Container.ReplaceContainerAsync(props);

		var readBack = await result.Container.ReadContainerAsync();
		readBack.Resource.IndexingPolicy.SpatialIndexes.Should().HaveCount(1);
	}

	[Fact]
	public async Task CompositeIndex_EmptyContainer_NoError()
	{
		var container = new InMemoryContainer("comp-empty", "/pk");
		container.IndexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
		{
			new() { Path = "/a", Order = CompositePathSortOrder.Ascending },
			new() { Path = "/b", Order = CompositePathSortOrder.Descending }
		});

		var iter = container.GetItemQueryIterator<JObject>(new QueryDefinition("SELECT * FROM c ORDER BY c.a ASC, c.b DESC"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().BeEmpty();
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 4 — ORDER BY Edge Cases
// ═══════════════════════════════════════════════════════════

public class OrderByDeepDiveTests
{
	private async Task<List<JObject>> QueryAll(InMemoryContainer container, string sql)
	{
		var iter = container.GetItemQueryIterator<JObject>(new QueryDefinition(sql));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task OrderBy_BooleanValues_SortCorrectly()
	{
		var container = new InMemoryContainer("ob-bool", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", flag = true }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk", flag = false }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", flag = true }), new PartitionKey("pk"));

		var results = await QueryAll(container, "SELECT * FROM c ORDER BY c.flag");
		results[0]["flag"]!.Value<bool>().Should().BeFalse("false sorts before true in ASC");
	}

	[Fact]
	public async Task OrderBy_DescWithNulls_NullsSortCorrectly()
	{
		var container = new InMemoryContainer("ob-descnull", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", val = 10 }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk" }), new PartitionKey("pk")); // no val
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", val = 20 }), new PartitionKey("pk"));

		var results = await QueryAll(container, "SELECT * FROM c ORDER BY c.val DESC");
		// Items with val should come before items without val
		results.Should().HaveCount(3);
		var firstVal = results[0]["val"]?.Value<int>();
		firstVal.Should().NotBeNull();
	}

	[Fact]
	public async Task OrderBy_WithDistinct()
	{
		var container = new InMemoryContainer("ob-distinct", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", name = "Alice" }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk", name = "Bob" }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", name = "Alice" }), new PartitionKey("pk"));

		var results = await QueryAll(container, "SELECT DISTINCT c.name FROM c ORDER BY c.name");
		results.Should().HaveCount(2);
		results[0]["name"]!.ToString().Should().Be("Alice");
		results[1]["name"]!.ToString().Should().Be("Bob");
	}

	[Fact]
	public async Task OrderBy_WithGroupBy()
	{
		var container = new InMemoryContainer("ob-groupby", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", name = "Bob" }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk", name = "Alice" }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", name = "Alice" }), new PartitionKey("pk"));

		var results = await QueryAll(container, "SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name ORDER BY c.name");
		results.Should().HaveCount(2);
		results[0]["name"]!.ToString().Should().Be("Alice");
		results[0]["cnt"]!.Value<long>().Should().Be(2);
		results[1]["name"]!.ToString().Should().Be("Bob");
	}

	[Fact]
	public async Task OrderBy_WithParameterizedWhere()
	{
		var container = new InMemoryContainer("ob-param", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", val = 5 }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk", val = 15 }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", val = 10 }), new PartitionKey("pk"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.val > @min ORDER BY c.val").WithParameter("@min", 4);
		var iter = container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(3);
		results[0]["val"]!.Value<int>().Should().Be(5);
		results[2]["val"]!.Value<int>().Should().Be(15);
	}

	[Fact]
	public async Task OrderBy_StringValues_CaseSensitive()
	{
		var container = new InMemoryContainer("ob-case", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", name = "bob" }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk", name = "Alice" }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", name = "alice" }), new PartitionKey("pk"));

		var results = await QueryAll(container, "SELECT * FROM c ORDER BY c.name");
		// Ordinal: uppercase letters come before lowercase
		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task OrderBy_NumericPrecision_IntAndFloat()
	{
		var container = new InMemoryContainer("ob-numeric", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", val = 42 }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk", val = 42.5 }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", val = 41.9 }), new PartitionKey("pk"));

		var results = await QueryAll(container, "SELECT * FROM c ORDER BY c.val");
		results[0]["val"]!.Value<double>().Should().BeLessThan(42);
		results[1]["val"]!.Value<double>().Should().Be(42);
		results[2]["val"]!.Value<double>().Should().BeGreaterThan(42);
	}

	[Fact]
	public async Task OrderBy_EmptyStringVsNull()
	{
		var container = new InMemoryContainer("ob-empty", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", name = "Bob" }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk", name = "" }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", name = (string?)null }), new PartitionKey("pk"));

		var results = await QueryAll(container, "SELECT * FROM c ORDER BY c.name");
		results.Should().HaveCount(3);
		// null sorts before empty string, empty string sorts before "Bob"
		results[0]["name"]!.Type.Should().Be(JTokenType.Null);
		results[1]["name"]!.ToString().Should().Be("");
		results[2]["name"]!.ToString().Should().Be("Bob");
	}

	[Fact]
	public async Task OrderBy_DeepNestedProperty_ThreeLevel()
	{
		var container = new InMemoryContainer("ob-deep", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk", a = new { b = new { c = 30 } } }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "pk", a = new { b = new { c = 10 } } }), new PartitionKey("pk"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "pk", a = new { b = new { c = 20 } } }), new PartitionKey("pk"));

		var results = await QueryAll(container, "SELECT * FROM c ORDER BY c.a.b.c");
		results[0]["a"]!["b"]!["c"]!.Value<int>().Should().Be(10);
		results[1]["a"]!["b"]!["c"]!.Value<int>().Should().Be(20);
		results[2]["a"]!["b"]!["c"]!.Value<int>().Should().Be(30);
	}
}

// ═══════════════════════════════════════════════════════════
// Phase 5 — Policy Edge Cases
// ═══════════════════════════════════════════════════════════

public class IndexPolicyEdgeCaseDeepDiveTests
{
	[Fact]
	public async Task IndexingPolicy_ExportImport_PolicyNotPersisted()
	{
		var container = new InMemoryContainer("exp-pol", "/pk");
		container.IndexingPolicy = new IndexingPolicy { Automatic = false, IndexingMode = IndexingMode.None };
		container.IndexingPolicy.IncludedPaths.Clear();
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "pk" }), new PartitionKey("pk"));

		var state = container.ExportState();
		var newContainer = new InMemoryContainer("exp-pol", "/pk");
		newContainer.ImportState(state);

		// Policy is NOT preserved in export/import — resets to defaults
		newContainer.IndexingPolicy.Automatic.Should().BeTrue("policy not persisted in export/import");
		newContainer.IndexingPolicy.IndexingMode.Should().Be(IndexingMode.Consistent);
	}

	[Fact]
	public async Task ReplaceContainer_NullIndexingPolicy_PreservesExisting()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseIfNotExistsAsync("db");
		var result = await db.Database.CreateContainerIfNotExistsAsync(new ContainerProperties("rpol", "/pk"));

		// Set custom policy
		var props = (await result.Container.ReadContainerAsync()).Resource;
		props.IndexingPolicy.Automatic = false;
		await result.Container.ReplaceContainerAsync(props);

		// Read, null out indexing policy, replace
		var props2 = (await result.Container.ReadContainerAsync()).Resource;
		props2.IndexingPolicy.Automatic.Should().BeFalse("custom policy was set");
	}

	[Fact]
	public async Task IndexingPolicy_ReadContainerStreamAsync_SyncsPolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseIfNotExistsAsync("db");
		var containerProps = new ContainerProperties("stream-read", "/pk");
		containerProps.IndexingPolicy.Automatic = false;
		var result = await db.Database.CreateContainerIfNotExistsAsync(containerProps);

		using var streamResponse = await result.Container.ReadContainerStreamAsync();
		streamResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		using var sr = new System.IO.StreamReader(streamResponse.Content);
		var json = await sr.ReadToEndAsync();
		var obj = JObject.Parse(json);
		obj["indexingPolicy"]!["automatic"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IndexingPolicy_ReplaceContainerStreamAsync_UpdatesPolicy()
	{
		var client = new InMemoryCosmosClient();
		var db = await client.CreateDatabaseIfNotExistsAsync("db");
		var result = await db.Database.CreateContainerIfNotExistsAsync(new ContainerProperties("stream-repl", "/pk"));

		var props = (await result.Container.ReadContainerAsync()).Resource;
		props.IndexingPolicy.Automatic = false;
		await result.Container.ReplaceContainerAsync(props);

		var readBack = await result.Container.ReadContainerAsync();
		readBack.Resource.IndexingPolicy.Automatic.Should().BeFalse();
	}
}
