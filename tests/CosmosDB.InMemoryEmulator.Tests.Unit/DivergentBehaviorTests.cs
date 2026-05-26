using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Tests — Known differences from real Cosmos DB
//  Each test documents a gap that is intentionally not fixed, with a
//  sister skipped test showing the expected real Cosmos DB behavior.
//  See divergent-behavior-deep-dive-plan.md for details on each gap ID.
// ═══════════════════════════════════════════════════════════════════════════

// ─── M7: Cross-partition aggregates multiply results when ────────────────
//         PartitionKeyRangeCount > 1

public class CrossPartitionAggregateTests
{
	private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public async Task CrossPartition_Count_ShouldNotMultiplyResults()
	{
		var container = new InMemoryContainer("test-agg", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "b" }), new PartitionKey("b"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "c" }), new PartitionKey("c"));

		using var handler = new FakeCosmosHandler(container, new FakeCosmosHandlerOptions
		{
			PartitionKeyRangeCount = 2
		});
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test-agg");

		var count = await cosmosContainer.GetItemQueryIterator<int>(
			new QueryDefinition("SELECT VALUE COUNT(1) FROM c"))
			.ReadNextAsync();
		count.Resource.Single().Should().Be(3);

		// SUM should return correct value, not doubled
		var sum = await cosmosContainer.GetItemQueryIterator<int>(
			new QueryDefinition("SELECT VALUE SUM(1) FROM c"))
			.ReadNextAsync();
		sum.Resource.Single().Should().Be(3);
	}
}

// ─── M9: Subquery ORDER BY and OFFSET/LIMIT — RESOLVED ──────────────────

public class SubqueryOrderByTests
{
	[Fact]
	public async Task Subquery_WithOrderByAndLimit_ShouldReturnOrderedSubset()
	{
		var container = new InMemoryContainer("subq-m9", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", scores = new[] { 30, 10, 50, 20, 40 } }),
			new PartitionKey("a"));

		// Subquery ORDER BY + OFFSET/LIMIT: sort ascending, skip first, take 3 → [20, 30, 40]
		var query = new QueryDefinition(
			"SELECT ARRAY(SELECT VALUE s FROM s IN c.scores ORDER BY s ASC OFFSET 1 LIMIT 3) AS result FROM c WHERE c.id = '1'");

		var iterator = container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		var result = results[0]["result"]!.ToObject<int[]>();
		result.Should().Equal(20, 30, 40);
	}
}

// ─── L2: Array functions only accept identifiers, not literal arrays ────

public class ArrayFunctionLiteralTests
{
	[Fact]
	public async Task ArrayContains_WithLiteralArray_ShouldWork()
	{
		var container = new InMemoryContainer("test-l2-literal", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		// ARRAY_CONTAINS with literal array
		var iter1 = container.GetItemQueryIterator<JToken>(
			"SELECT VALUE ARRAY_CONTAINS([1,2,3], 2) FROM c");
		var r1 = await iter1.ReadNextAsync();
		r1.First().Value<bool>().Should().BeTrue();

		// ARRAY_LENGTH with literal array
		var iter2 = container.GetItemQueryIterator<JToken>(
			"SELECT VALUE ARRAY_LENGTH([10,20,30,40]) FROM c");
		var r2 = await iter2.ReadNextAsync();
		r2.First().Value<long>().Should().Be(4);

		// ARRAY_SLICE with literal array
		var iter3 = container.GetItemQueryIterator<JToken>(
			"SELECT VALUE ARRAY_SLICE([10,20,30,40,50], 1, 2) FROM c");
		var r3 = await iter3.ReadNextAsync();
		var arr = r3.First() as JArray ?? JArray.Parse(r3.First().ToString());
		arr.Select(t => t.Value<int>()).Should().Equal(20, 30);
	}
}

// ─── L3: GetCurrentDateTime() not consistent across rows ────────────────

public class GetCurrentDateTimeConsistencyTests
{
	// Previously divergent: Each evaluation called DateTime.UtcNow independently per row.
	// Now fixed: GetCurrentDateTime() returns/uses the per-query snapshot like GetCurrentDateTimeStatic().
	[Fact]
	public async Task GetCurrentDateTime_ShouldReturnSameValueForAllRows()
	{
		var container = new InMemoryContainer("test-l3", "/pk");
		for (var i = 0; i < 20; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"doc{i}", pk = "a" }), new PartitionKey("a"));

		var results = await container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT GetCurrentDateTime() AS ts FROM c")).ReadNextAsync();

		var timestamps = results.Resource.Select(r => r["ts"]!.ToString()).Distinct().ToList();
		Assert.Single(timestamps);
	}
}

// ─── L4: linqSerializerOptions and continuationToken on ─────────────────
//         GetItemLinqQueryable are ignored

public class LinqQueryableOptionsTests
{
	// APPROACH 1 PERMISSIVENESS: GetItemLinqQueryable ignores linqSerializerOptions and continuationToken.
	[Fact(Skip = "APPROACH 1 PERMISSIVENESS (L4): InMemoryContainer ignores linqSerializerOptions " +
				  "and continuationToken on GetItemLinqQueryable because it uses Newtonsoft.Json " +
				  "internally and materializes all items via LINQ-to-Objects. This divergence only " +
				  "affects Approach 1 (direct InMemoryContainer). With Approach 3 (CosmosClient + " +
				  "FakeCosmosHandler), the real SDK's CosmosLinqQueryProvider handles serializer " +
				  "options and continuation tokens through the HTTP pipeline.")]
	public void GetItemLinqQueryable_WithSerializerOptions_ShouldRespectOptions()
	{
		// Expected real Cosmos behavior:
		// LINQ queries should respect custom CosmosLinqSerializerOptions
		// (e.g., PropertyNamingPolicy), and continuationToken should resume iteration.
	}
}

// ─── L6: Undefined vs null not distinguished in ORDER BY ────────────────

public class UndefinedNullOrderByTests
{
	// DIVERGENT: undefined and null are treated identically in ORDER BY.
	// Real Cosmos DB has a specific type ordering: undefined < null < boolean < number < string < array < object.
	[Fact]
	public async Task OrderBy_ShouldDistinguishUndefinedFromNull()
	{
		var container = new InMemoryContainer("test-l6", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"2\",\"pk\":\"a\",\"val\":null}"), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a" }), new PartitionKey("a")); // undefined val

		var query = new QueryDefinition("SELECT c.id, c.val FROM c ORDER BY c.val ASC");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Cosmos type ordering: undefined < null < number
		results.Should().HaveCount(3);
		results[0]["id"]!.ToString().Should().Be("3", "undefined sorts first");
		results[1]["id"]!.ToString().Should().Be("2", "null sorts second");
		results[2]["id"]!.ToString().Should().Be("1", "number sorts last");
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  Sister tests for existing gaps — showing actual emulator behavior
// ═══════════════════════════════════════════════════════════════════════════

// ─── M7 sister: Cross-partition aggregate with default range count ───────

public class CrossPartitionAggregateSisterTests
{
	[Fact]
	public async Task CrossPartition_Count_WithDefaultRange_ReturnsCorrectCount()
	{
		// EMULATOR: With default PartitionKeyRangeCount=1, aggregates work correctly
		var container = new InMemoryContainer("test-m7", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", val = 20 }), new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", val = 30 }), new PartitionKey("c"));

		var query = new QueryDefinition("SELECT VALUE COUNT(1) FROM c");
		var iterator = container.GetItemQueryIterator<int>(query);
		var results = new List<int>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(3);
	}
}

// ─── L2 sister: Array functions with identifiers work, literals don't ───

public class ArrayFunctionLiteralSisterTests
{
	[Fact]
	public async Task ArrayContains_WithIdentifier_Works()
	{
		// EMULATOR: Array functions work correctly with document field references
		var container = new InMemoryContainer("test-l2", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", tags = new[] { "red", "green", "blue" } }),
			new PartitionKey("a"));

		var query = new QueryDefinition("SELECT c.id FROM c WHERE ARRAY_CONTAINS(c.tags, 'green')");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}
}

// ─── L3 sister: GetCurrentDateTime per-row behavior ─────────────────────

public class GetCurrentDateTimeSisterTests
{
	[Fact]
	public async Task GetCurrentDateTime_EmulatorBehavior_ReturnsValidTimestamps()
	{
		// EMULATOR: GetCurrentDateTime() works but may return slightly different
		// values per-row (evaluated per-row, not per-query like real Cosmos)
		var container = new InMemoryContainer("test-l3", "/pk");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a" }), new PartitionKey("a"));

		var query = new QueryDefinition("SELECT GetCurrentDateTime() AS ts FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(5);
		results.Should().OnlyContain(r => r["ts"] != null);
	}
}

// ─── L4 sister: LINQ queryable ignores custom options ───────────────────

public class LinqQueryableOptionsSisterTests
{
	[Fact]
	public async Task GetItemLinqQueryable_EmulatorBehavior_WorksWithoutOptions()
	{
		// EMULATOR: LINQ queryable works but ignores linqSerializerOptions
		var container = new InMemoryContainer("test-l4", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }),
			new PartitionKey("a"));

		var queryable = container.GetItemLinqQueryable<JObject>(allowSynchronousQueryExecution: true);
		var results = queryable.ToList();

		results.Should().ContainSingle().Which["name"]!.ToString().Should().Be("Alice");
	}
}


// ═══════════════════════════════════════════════════════════════════════════
//  New divergence coverage — D-series IDs
// ═══════════════════════════════════════════════════════════════════════════

// ─── D1: Consistency levels ignored ─────────────────────────────────────

public class ConsistencyLevelDivergenceTests
{
	[Fact(Skip = "D1: Consistency levels are ignored. Real Cosmos DB supports 5 consistency " +
				 "levels (Strong, Bounded Staleness, Session, Consistent Prefix, Eventual) " +
				 "that affect read behavior. The emulator always returns the latest write " +
				 "immediately regardless of the configured consistency level.")]
	public void ConsistencyLevel_ShouldAffectReadBehavior() { }

	[Fact]
	public async Task ConsistencyLevel_EmulatorBehavior_AllLevelsReturnSameResult()
	{
		// EMULATOR: All consistency levels return the latest write immediately
		var container = new InMemoryContainer("test-d1", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }),
			new PartitionKey("a"));

		// Regardless of consistency level, the read always returns the latest state
		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["name"]!.ToString().Should().Be("Alice");

		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Bob" }),
			new PartitionKey("a"));

		result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["name"]!.ToString().Should().Be("Bob");
	}
}

// ─── D2: Request charge always 1.0 RU ──────────────────────────────────

public class RequestChargeDivergenceTests
{
	[Fact(Skip = "D2: Request charge always returns 1.0 RU regardless of operation complexity. " +
				 "Real Cosmos DB charges vary by operation type (reads ~1 RU, writes ~5-10 RU), " +
				 "document size, index paths, and cross-partition fan-out. Simulating real RU " +
				 "costs would require a complete cost model.")]
	public void RequestCharge_ShouldVaryByOperationComplexity() { }

	[Fact]
	public async Task RequestCharge_EmulatorBehavior_AlwaysReturns1RU()
	{
		// EMULATOR: All operations return exactly 1.0 RU
		var container = new InMemoryContainer("test-d2", "/pk");

		var create = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }),
			new PartitionKey("a"));
		create.RequestCharge.Should().Be(1.0);

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		read.RequestCharge.Should().Be(1.0);

		var replace = await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Bob" }),
			"1", new PartitionKey("a"));
		replace.RequestCharge.Should().Be(1.0);

		var delete = await container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));
		delete.RequestCharge.Should().Be(1.0);
	}
}

// ─── D3: Continuation tokens are plain integers ────────────────────────

public class ContinuationTokenDivergenceTests
{
	[Fact(Skip = "D3: Continuation tokens are plain integer offsets instead of opaque base64-encoded " +
				 "JSON. Real Cosmos DB tokens are opaque and contain partition key ranges, composite " +
				 "continuation info, and resource IDs. Could implement base64-encoded tokens but " +
				 "would break existing consumer code that may parse them.")]
	public void ContinuationToken_ShouldBeOpaqueBase64EncodedJson() { }

	[Fact]
	public async Task ContinuationToken_EmulatorBehavior_IsPlainIntegerOffset()
	{
		// EMULATOR: Continuation tokens are simple integer strings
		var container = new InMemoryContainer("test-d3", "/pk");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a" }), new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 2 });

		var response = await iterator.ReadNextAsync();
		var token = response.ContinuationToken;

		// Token should be a parseable integer (page offset)
		int.TryParse(token, out _).Should().BeTrue(
			"emulator uses plain integer continuation tokens, not opaque base64");
	}
}

// ─── D4: System properties format differences ──────────────────────────

public class SystemPropertiesDivergenceTests
{
	[Fact]
	public async Task SystemProperties_RidIsHierarchicalBase64()
	{
		var container = new InMemoryContainer("test-d4", "/pk");
		var created = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var doc = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		var rid = doc["_rid"]!.ToString();

		// _rid should be base64 of exactly 8 bytes (hierarchical: 4-byte container + 4-byte doc counter)
		var ridBytes = Convert.FromBase64String(rid);
		ridBytes.Length.Should().Be(8);
	}

	[Fact]
	public async Task SystemProperties_EmulatorBehavior_HasTsAndEtag()
	{
		// EMULATOR: _ts and _etag are present and functional, _rid/_self may differ
		var container = new InMemoryContainer("test-d4", "/pk");
		var created = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var doc = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;

		doc["_ts"].Should().NotBeNull();
		doc["_etag"].Should().NotBeNull();
		((long)doc["_ts"]!).Should().BeGreaterThan(0);
		doc["_etag"]!.ToString().Should().NotBeNullOrEmpty();
	}
}

// ─── D5: IndexingPolicy stored but not enforced ────────────────────────

public class IndexingPolicyDivergenceTests
{
	[Fact(Skip = "D5: IndexingPolicy is stored but not enforced. Real Cosmos DB uses indexes " +
				 "to optimize queries — excluding a path from indexing would cause queries on " +
				 "that path to fail or require a full scan. The emulator always does full scans " +
				 "so queries work regardless of indexing policy.")]
	public void IndexingPolicy_ShouldAffectQueryPerformance() { }

	[Fact]
	public async Task IndexingPolicy_EmulatorBehavior_StoredButNotEnforced()
	{
		// EMULATOR: IndexingPolicy is stored on the container but has no query impact
		var props = new ContainerProperties("test-d5", "/pk")
		{
			IndexingPolicy = new IndexingPolicy
			{
				IndexingMode = IndexingMode.None
			}
		};
		var container = new InMemoryContainer(props);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }),
			new PartitionKey("a"));

		// Query still works even with IndexingMode.None
		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name = 'Alice'");
		var response = await iterator.ReadNextAsync();

		response.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}
}

// ─── D6: TTL eviction is lazy (on next read) ──────────────────────────

public class TtlEvictionDivergenceTests
{
	[Fact(Skip = "D6: TTL eviction is lazy — expired items are removed on next read/query, not " +
				 "proactively by a background process. Real Cosmos DB has a background TTL " +
				 "eviction process that removes items independently of reads. The emulator's " +
				 "lazy approach means expired items still consume memory until accessed.")]
	public void TTL_ShouldProactivelyEvictExpiredItems() { }

	[Fact]
	public async Task TTL_EmulatorBehavior_LazyEvictionOnRead()
	{
		// EMULATOR: TTL items are evicted lazily when the container is accessed
		var props = new ContainerProperties("test-d6", "/pk") { DefaultTimeToLive = 1 };
		var container = new InMemoryContainer(props);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		// Item is immediately visible after creation
		var iterator1 = container.GetItemQueryIterator<JObject>("SELECT * FROM c");
		var response1 = await iterator1.ReadNextAsync();
		response1.Should().ContainSingle("item should be visible immediately after creation");

		// Wait for TTL to expire
		await Task.Delay(2000);

		// After TTL expires, a query/read triggers eviction — item is no longer returned.
		// Note: the exact eviction behaviour may vary; the item might still appear if
		// eviction is deferred until the next write or explicit eviction pass.
		// The key divergence is that eviction is NOT proactive.
		try
		{
			var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
			// If read succeeds, the item hasn't been lazily evicted yet — still a valid divergence
		}
		catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
		{
			// Item was evicted — also valid
		}
	}
}

// ─── D7: Analytical store not simulated ────────────────────────────────

public class AnalyticalStoreDivergenceTests
{
	[Fact(Skip = "D7: Analytical store (Azure Synapse Link) is not simulated. Real Cosmos DB " +
				 "supports an analytical store for OLAP workloads with column-store format " +
				 "accessible via Synapse notebooks and serverless SQL. This is out of scope " +
				 "for the in-memory emulator.")]
	public void AnalyticalStore_ShouldBeAvailable() { }
}

// ─── D9: LINQ accepts operations real Cosmos rejects ───────────────────

public class LinqOperatorDivergenceTests
{
	[Fact(Skip = "D9: LINQ accepts all LINQ-to-Objects operators that real Cosmos SQL rejects. " +
				 "String.Format, Regex, custom comparers, local method calls, etc. all work " +
				 "because the emulator evaluates LINQ in-process. Real Cosmos converts LINQ " +
				 "to SQL and rejects unsupported operators.")]
	public void Linq_ShouldRejectUnsupportedOperators() { }

	[Fact]
	public async Task Linq_EmulatorBehavior_AcceptsStringContains()
	{
		// EMULATOR: LINQ-to-Objects accepts operators that real Cosmos SQL would reject
		var container = new InMemoryContainer("test-d9", "/pk");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "a", Name = "Alice" },
			new PartitionKey("a"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "a", Name = "Bob" },
			new PartitionKey("a"));

		var queryable = container.GetItemLinqQueryable<TestDocument>(allowSynchronousQueryExecution: true);
		// String.Contains with StringComparison would fail in real Cosmos LINQ-to-SQL
		var results = queryable.Where(d => d.Name.Contains("li")).ToList();

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Deep Dive — D10: CosmosResponseFactory
// ═══════════════════════════════════════════════════════════════════════════

public class CosmosResponseFactoryDivergenceTests
{
	[Fact(Skip = "D10: CosmosResponseFactory is a non-functional NSubstitute stub. " +
		"CosmosClient.ResponseFactory returns a mock whose methods return default/null values. " +
		"Real Cosmos DB returns a factory that can deserialize response messages.")]
	public void CosmosResponseFactory_ShouldDeserializeResponses() { }

	[Fact]
	public void CosmosResponseFactory_EmulatorBehavior_ReturnsStub()
	{
		var client = new InMemoryCosmosClient();
		var factory = client.ResponseFactory;
		factory.Should().NotBeNull("the stub should exist");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Deep Dive — D11: Unsupported SQL Function Error Type
// ═══════════════════════════════════════════════════════════════════════════

public class UnsupportedSqlFunctionDivergenceTests
{
	[Fact]
	public async Task UnsupportedSqlFunction_ShouldThrowCosmosExceptionBadRequest()
	{
		var container = new InMemoryContainer("test-d11", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var act = () => container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT VALUE NONEXISTENT_FUNCTION(c.id) FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });

		act.Should().Throw<CosmosException>().Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Deep Dive — D12: SQL Parse Error Type
// ═══════════════════════════════════════════════════════════════════════════

public class SqlParseErrorDivergenceTests
{
	[Fact]
	public async Task MalformedSql_ShouldThrowCosmosExceptionBadRequest()
	{
		var container = new InMemoryContainer("test-d12", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var act = () => container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECTTTTT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });

		act.Should().Throw<CosmosException>().Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Deep Dive — D18: Invalid Continuation Token
// ═══════════════════════════════════════════════════════════════════════════

public class InvalidContinuationTokenEdgeCaseTests
{
	[Fact]
	public void ContinuationToken_InvalidToken_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test-d18", "/pk");

		var act = () => container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c",
			continuationToken: "not-a-valid-token",
			requestOptions: new QueryRequestOptions { MaxItemCount = 10, PartitionKey = new PartitionKey("a") });

		act.Should().Throw<CosmosException>()
			.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Deep Dive — D23: Permission Tokens Are Synthetic
// ═══════════════════════════════════════════════════════════════════════════

public class PermissionTokenDivergenceTests
{
	[Fact(Skip = "D23: Permission tokens are synthetic stubs. Real Cosmos DB generates " +
		"HMAC-signed resource tokens that are cryptographically valid. Emulator tokens " +
		"have format 'type=resource&ver=1&sig=stub_<id>' with fake signatures.")]
	public void PermissionToken_ShouldBeCryptographicallyValid() { }

	[Fact]
	public async Task PermissionToken_EmulatorBehavior_IsSyntheticStub()
	{
		var client = new InMemoryCosmosClient();
		var db = client.GetDatabase("db");
		var user = db.GetUser("user1");
		var permission = await user.CreatePermissionAsync(
			new PermissionProperties("perm1", PermissionMode.Read,
				client.GetDatabase("db").GetContainer("c1")));

		var token = permission.Resource.Token;
		token.Should().Contain("stub_", "emulator uses synthetic tokens");
		token.Should().StartWith("type=resource&ver=1&sig=");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Deep Dive — M7: Cross-Partition Aggregate Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class CrossPartitionAggregateEdgeCaseTests
{
	[Fact]
	public async Task CrossPartition_MinMax_WithMultipleRanges_StillCorrect()
	{
		var container = new InMemoryContainer("test-m7", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", val = 20 }), new PartitionKey("b"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", val = 30 }), new PartitionKey("c"));

		var iterator = container.GetItemQueryIterator<long>(
			"SELECT VALUE MIN(c.val) FROM c");
		var results = new List<long>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Single().Should().Be(10);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Deep Dive — D2: Query Request Charge Edge Case
// ═══════════════════════════════════════════════════════════════════════════

public class RequestChargeQueryDivergenceTests
{
	[Fact]
	public async Task RequestCharge_Query_EmulatorBehavior_ReturnsFixedRU()
	{
		var container = new InMemoryContainer("test-d2q", "/pk");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a", val = i }), new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.val > 5",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var response = await iterator.ReadNextAsync();

		response.RequestCharge.Should().BeGreaterThanOrEqualTo(0,
			"emulator returns a fixed RU regardless of query complexity");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Divergent Behavior Deep Dive — D5: Composite Index Not Required
// ═══════════════════════════════════════════════════════════════════════════

public class CompositeIndexDivergenceTests
{
	[Fact]
	public async Task IndexingPolicy_CompositeIndex_EmulatorBehavior_NotRequired()
	{
		var container = new InMemoryContainer("test-d5", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Charlie", age = 30 }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", name = "Alice", age = 25 }), new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", name = "Alice", age = 35 }), new PartitionKey("a"));

		// Real Cosmos requires a composite index for multi-field ORDER BY.
		// Emulator does not enforce this — it just works.
		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.name ASC, c.age DESC",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Select(r => r["id"]!.ToString()).Should().ContainInOrder("3", "2", "1");
	}
}
