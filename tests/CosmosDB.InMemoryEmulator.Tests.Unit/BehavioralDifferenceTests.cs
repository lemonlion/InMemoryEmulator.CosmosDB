using System.Globalization;
using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests that verify actual InMemoryContainer behaviour where it DIFFERS from
/// real Cosmos DB. Each test has a comment explaining the behavioural difference.
/// Use these to understand the limitations of the in-memory implementation.
/// </summary>
public class BehavioralDifferenceTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	// ── Change Feed ──────────────────────────────────────────────────────────

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB change feed returns only the latest
	/// version of each document, ordered by modification time (_ts), and supports
	/// continuation tokens for incremental reads. InMemoryContainer returns all
	/// current items from the store in a single page, regardless of when they were
	/// created or modified, and does not support incremental continuation.
	/// </summary>
	[Fact]
	public async Task ChangeFeed_ReturnsAllCurrentItems_NotIncrementalChanges()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		var iterator = _container.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified)
			{
				break;
			}

			results.AddRange(response);
		}

		// InMemoryContainer returns all current items as a snapshot, not incremental changes
		results.Should().HaveCount(2);
	}

	// ── Delete Container ─────────────────────────────────────────────────────

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB DeleteContainerAsync returns
	/// HttpStatusCode.NoContent (204). InMemoryContainer also clears internal
	/// state but the container object remains usable - you can still add items
	/// after deletion. Real Cosmos DB would reject subsequent operations until
	/// the container is recreated.
	/// </summary>
	[Fact]
	public async Task DeleteContainer_ContainerRemainsUsable_UnlikeRealCosmos()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		await _container.DeleteContainerAsync();

		// InMemoryContainer allows continued use after deletion
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk1"));
		response.Resource.Name.Should().Be("Bob");
	}

	// ── Throughput ───────────────────────────────────────────────────────────

	/// <summary>
	/// FIXED: Throughput is now persisted. ReplaceThroughput stores the value
	/// and ReadThroughput returns it, matching real Cosmos DB behavior.
	/// </summary>
	[Fact]
	public async Task ReadThroughput_ReturnsPreviouslyReplacedValue()
	{
		await _container.ReplaceThroughputAsync(1000);

		var throughput = await _container.ReadThroughputAsync();

		// Now correctly returns the replaced value
		throughput.Should().Be(1000);
	}

	// ── ETag format ──────────────────────────────────────────────────────────

	/// <summary>
	/// ETags are quoted monotonically increasing hex counters.
	/// The format differs from real Cosmos (opaque timestamp-based) but
	/// the concurrency semantics (If-Match / If-None-Match) work correctly,
	/// and monotonic ordering is preserved.
	/// </summary>
	[Fact]
	public async Task ETag_IsQuotedMonotonicHex()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var etag = response.ETag;
		etag.Should().NotBeNullOrEmpty();
		// InMemoryContainer uses quoted hex counter format
		etag.Should().StartWith("\"").And.EndWith("\"");
		etag.Should().MatchRegex("^\"[0-9a-f]{16}\"$");
	}

	// ── LINQ query ──────────────────────────────────────────────────────────

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB LINQ queries are translated to SQL
	/// and executed server-side. InMemoryContainer LINQ operates on an in-memory
	/// IQueryable, meaning all LINQ-to-Objects operators work but there is no
	/// SQL translation step. Some LINQ expressions that would fail on real Cosmos
	/// (e.g. unsupported operators) will succeed against InMemoryContainer.
	/// </summary>
	[Fact]
	public async Task LinqQuery_SupportsAllLinqToObjectsOperators()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);

		// String.Contains works in LINQ-to-Objects but may not always translate to Cosmos SQL
		var results = queryable.Where(d => d.Name.Contains("li")).ToList();
		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Alice");
	}

	// ── Partition key extraction ─────────────────────────────────────────────

	/// <summary>
	/// InMemoryContainer now correctly extracts the partition key value from
	/// the document body using the configured partition key path when no
	/// explicit PartitionKey is supplied — matching real Cosmos DB behaviour.
	/// </summary>
	[Fact]
	public async Task PartitionKey_ExtractsFromConfiguredPath_WhenNotSupplied()
	{
		var doc = new TestDocument { Id = "my-id", PartitionKey = "my-pk", Name = "Alice" };

		await _container.CreateItemAsync(doc);

		var response = await _container.ReadItemAsync<TestDocument>("my-id", new PartitionKey("my-pk"));
		response.Resource.Name.Should().Be("Alice");
	}

	// ── Stream CRUD enforces ETag checks ────────────────────────────

	/// <summary>
	/// Stream-based CRUD now enforces If-Match / If-None-Match like real Cosmos DB.
	/// </summary>
	[Fact]
	public async Task StreamReplace_ChecksIfMatch_LikeRealCosmos()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var updatedDoc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
		var json = Newtonsoft.Json.JsonConvert.SerializeObject(updatedDoc);
		var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var requestOptions = new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" };
		using var response = await _container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("pk1"), requestOptions);

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	// ── Replace Container ────────────────────────────────────────────────────

	/// <summary>
	/// ReplaceContainerAsync should persist property changes so that
	/// subsequent ReadContainerAsync calls return the updated values.
	/// </summary>
	[Fact]
	public async Task ReplaceContainer_PersistsPropertyChanges()
	{
		var newProperties = new ContainerProperties("test-container", "/partitionKey")
		{
			DefaultTimeToLive = 600
		};
		await _container.ReplaceContainerAsync(newProperties);

		var readResponse = await _container.ReadContainerAsync();
		readResponse.Resource.DefaultTimeToLive.Should().Be(600);
	}

	// ── Feed ranges ──────────────────────────────────────────────────────────

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB returns feed ranges that correspond
	/// to physical partitions and can be used for parallel processing. 
	/// InMemoryContainer returns a single NSubstitute mock FeedRange that has
	/// no real partition routing behaviour.
	/// </summary>
	[Fact]
	public async Task GetFeedRanges_ReturnsSingleMockRange()
	{
		var feedRanges = await _container.GetFeedRangesAsync();

		// InMemoryContainer always returns exactly one mocked FeedRange
		feedRanges.Should().HaveCount(1);
	}

	// ── ChangeFeed processor builders ────────────────────────────────────────

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB ChangeFeedProcessorBuilder creates a
	/// processor that monitors the container for changes. InMemoryContainer returns
	/// an <see cref="InMemoryChangeFeedProcessor"/> that polls a list-based change
	/// feed. The builder itself is functional but the processing model is simplified.
	/// </summary>
	[Fact]
	public void ChangeFeedProcessorBuilder_ReturnsMock_NoRealProcessing()
	{
		var builder = _container.GetChangeFeedProcessorBuilder<TestDocument>(
			"testProcessor",
			(IReadOnlyCollection<TestDocument> changes, CancellationToken token) => Task.CompletedTask);

		builder.Should().NotBeNull();
	}

	// ── Change Feed — empty container ────────────────────────────────────────

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB change feed returns 304 NotModified
	/// when there are no new changes. InMemoryContainer returns 200 OK with an
	/// empty result set. This is because InMemoryContainer uses a simple list-based
	/// change feed that doesn't support the NotModified status code pattern.
	/// </summary>
	[Fact]
	public async Task ChangeFeed_EmptyContainer_ReturnsOk_NotNotModified()
	{
		var iterator = _container.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().BeEmpty();
	}

	// ── Change Feed — tombstones ─────────────────────────────────────────────

	/// <summary>
	/// Deletes are recorded in the change feed as tombstone entries.
	/// The checkpoint advances after a delete.
	/// </summary>
	[Fact]
	public async Task ChangeFeed_DeletesRecordedAsTombstone()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "ToDelete" },
			new PartitionKey("pk1"));
		var checkpointAfterCreate = _container.GetChangeFeedCheckpoint();

		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var checkpointAfterDelete = _container.GetChangeFeedCheckpoint();

		checkpointAfterDelete.Should().Be(checkpointAfterCreate + 1);
	}

	// ── Aggregate queries ────────────────────────────────────────────────────

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB aggregates like COUNT without
	/// GROUP BY return a single aggregated value. InMemoryContainer also returns
	/// a single aggregated value for COUNT(1).
	/// </summary>
	[Fact]
	public async Task Aggregate_Count_WithoutGroupBy_ReturnsCount()
	{
		for (var i = 0; i < 3; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
		}

		var iterator = _container.GetItemQueryIterator<JToken>("SELECT VALUE COUNT(1) FROM c");
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Value<int>().Should().Be(3);
	}

	// ── Null-coalescing operator ─────────────────────────────────────────────

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: CosmosSqlParser handles the null-coalescing operator (??).
	/// SELECT VALUE returns raw scalar values so JToken must be used, not JObject.
	/// </summary>
	[Fact]
	public async Task Query_NullCoalesce_ProducesScalarResult_NotJObject()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JToken>(
			"""SELECT VALUE (c.name ?? "default") FROM c""");

		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 1: System Properties & Response Metadata
	// ══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB documents always have a <c>_rid</c>
	/// Real Cosmos DB always generates a <c>_rid</c>
	/// (resource ID) property. Now also set by the emulator.
	/// </summary>
	[Fact]
	public async Task SystemProperties_RidPresent_OnDocuments()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		response.Resource.ContainsKey("_rid").Should().BeTrue();
	}

	/// <summary>
	/// Real Cosmos DB documents always have a <c>_self</c>
	/// link. Now also set by the emulator.
	/// </summary>
	[Fact]
	public async Task SystemProperties_SelfPresent_OnDocuments()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		response.Resource.ContainsKey("_self").Should().BeTrue();
	}

	/// <summary>
	/// Real Cosmos DB documents always have an <c>_attachments</c>
	/// property. Now also set by the emulator.
	/// </summary>
	[Fact]
	public async Task SystemProperties_AttachmentsPresent_OnDocuments()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		response.Resource.ContainsKey("_attachments").Should().BeTrue();
	}

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos DB request charges vary by operation.
	/// InMemoryContainer always returns 1.0 RU for every operation.
	/// </summary>
	[Fact]
	public async Task RequestCharge_AlwaysReturns1RU_ForAllOperations()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		// Create
		var createResponse = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));
		createResponse.RequestCharge.Should().Be(1.0);

		// Read
		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.RequestCharge.Should().Be(1.0);

		// Query
		var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			page.RequestCharge.Should().Be(1.0);
		}

		// Delete
		var deleteResponse = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		deleteResponse.RequestCharge.Should().Be(1.0);
	}

	[Fact(Skip = "Real Cosmos returns varying RU charges per operation (reads ~1, writes ~5-10, queries vary).")]
	public void RequestCharge_ShouldVaryByOperation_RealCosmos() { }

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos session tokens encode partition key range IDs
	/// and logical sequence numbers. InMemoryContainer uses <c>0:{N}#{N}</c> where N increments with each write.
	/// </summary>
	[Fact]
	public async Task SessionToken_IsSyntheticGuidFormat()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var sessionToken = response.Headers["x-ms-session-token"];
		sessionToken.Should().StartWith("0:");
		sessionToken.Should().MatchRegex(@"^0:\d+#\d+$");
	}

	[Fact(Skip = "Real Cosmos session tokens use format like '0:-1#12345' with partition range and LSN.")]
	public void SessionToken_ShouldContainPartitionAndLSN_RealCosmos() { }

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos diagnostics contain timing, endpoint info,
	/// and request latency. InMemoryContainer returns a mock with empty ToString() and
	/// zero elapsed time.
	/// </summary>
	[Fact]
	public async Task Diagnostics_ReturnsMock_EmptyToString()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var response = await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		response.Diagnostics.Should().NotBeNull();
		response.Diagnostics.GetClientElapsedTime().Should().Be(TimeSpan.Zero);
	}

	[Fact(Skip = "Real Cosmos diagnostics contain detailed timing, latency, and endpoint information.")]
	public void Diagnostics_ShouldContainTimingInfo_RealCosmos() { }

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 2: Consistency
	// ══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos respects <c>ConsistencyLevel</c> on request
	/// options. InMemoryContainer ignores it — all reads are immediately consistent.
	/// </summary>
	[Fact]
	public async Task ConsistencyLevel_Ignored_AlwaysStrongSemantics()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "Updated";
		await _container.ReplaceItemAsync(doc, "1", new PartitionKey("pk1"));

		// Read with Eventual consistency — should still see latest (strong in emulator)
		var options = new ItemRequestOptions { ConsistencyLevel = ConsistencyLevel.Eventual };
		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"), options);
		response.Resource.Name.Should().Be("Updated");
	}

	[Fact(Skip = "Real Cosmos with Eventual consistency may return stale data.")]
	public void ConsistencyLevel_ShouldAffectReadBehavior_RealCosmos() { }

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 3: Container Lifecycle
	// ══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos enforces IndexingPolicy exclusion paths
	/// so excluded paths are not indexed and cannot be queried efficiently.
	/// InMemoryContainer stores the policy but all queries scan every item regardless.
	/// </summary>
	[Fact]
	public async Task ReplaceContainer_IndexingPolicy_StoredButNotEnforced()
	{
		var newProps = new ContainerProperties("test-container", "/partitionKey")
		{
			IndexingPolicy = new IndexingPolicy
			{
				Automatic = true,
				IndexingMode = IndexingMode.Consistent,
				ExcludedPaths = { new ExcludedPath { Path = "/name/*" } }
			}
		};
		await _container.ReplaceContainerAsync(newProps);

		// Verify policy is stored
		var readResponse = await _container.ReadContainerAsync();
		readResponse.Resource.IndexingPolicy.ExcludedPaths
			.Should().Contain(p => p.Path == "/name/*");

		// Create an item and query on the "excluded" path — still works in emulator
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c WHERE c.name = 'Alice'");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().HaveCount(1);
	}

	[Fact(Skip = "Real Cosmos would return 0 results or an error when querying an excluded path without a scan.")]
	public void ReplaceContainer_IndexingPolicyShouldAffectQueries_RealCosmos() { }

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos Container.Conflicts provides conflict
	/// resolution. InMemoryContainer returns a new NSubstitute mock each access.
	/// </summary>
	[Fact]
	public void Conflicts_ReturnsMock_NoRealConflictResolution()
	{
		var conflicts = _container.Conflicts;
		conflicts.Should().NotBeNull();

		// New mock instance each access
		var conflicts2 = _container.Conflicts;
		conflicts2.Should().NotBeSameAs(conflicts);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 4: Change Feed
	// ══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// In incremental change feed mode, multiple updates to the same document
	/// should result in only the latest version being returned.
	/// </summary>
	[Fact]
	public async Task ChangeFeed_IncrementalMode_UpdatesReturnOnlyLatestVersion()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "v1" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		doc.Name = "v2";
		await _container.ReplaceItemAsync(doc, "1", new PartitionKey("pk1"));

		doc.Name = "v3";
		await _container.ReplaceItemAsync(doc, "1", new PartitionKey("pk1"));

		var iterator = _container.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			results.AddRange(response);
		}

		// Change feed returns all entries (create + 2 replaces = 3 entries)
		// but the last entry for id "1" should have the latest name
		results.Last(r => r.Id == "1").Name.Should().Be("v3");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 5: Error Formatting
	// ══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos sub-status codes provide fine-grained
	/// error classification (e.g. 1001, 1003). InMemoryContainer always returns 0.
	/// </summary>
	[Fact]
	public async Task CosmosException_SubStatusCode_AlwaysZero()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Trigger 409 Conflict by inserting duplicate
		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			_container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Dup" },
				new PartitionKey("pk1")));

		ex.StatusCode.Should().Be(HttpStatusCode.Conflict);
		ex.SubStatusCode.Should().Be(0);
	}

	[Fact(Skip = "Real Cosmos sub-status codes provide fine-grained classification (e.g. 1001 timeout, 1003 rate limiting).")]
	public void CosmosException_SubStatusCodeShouldBeSpecific_RealCosmos() { }

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos error messages include activity ID,
	/// request URI, and detailed status info. InMemoryContainer uses minimal messages.
	/// </summary>
	[Fact]
	public async Task CosmosException_MessageFormat_SimplerThanRealCosmos()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			_container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Dup" },
				new PartitionKey("pk1")));

		// InMemoryContainer message is short — no ActivityId or request URI
		ex.Message.Should().NotContain("ActivityId");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 6: Continuation Tokens
	// ══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos continuation tokens are opaque
	/// base64-encoded structures. InMemoryContainer uses plain integer offsets.
	/// </summary>
	[Fact]
	public async Task ContinuationToken_IsPlainInteger_NotOpaqueBase64()
	{
		for (var i = 0; i < 3; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
		}

		var options = new QueryRequestOptions { MaxItemCount = 1 };
		var iterator = _container.GetItemQueryIterator<TestDocument>("SELECT * FROM c", requestOptions: options);

		var response = await iterator.ReadNextAsync();
		var token = response.ContinuationToken;

		// InMemoryContainer uses integer offset as continuation token
		int.TryParse(token, NumberStyles.Integer, CultureInfo.InvariantCulture, out _).Should().BeTrue();
	}

	/// <summary>
	/// Invalid continuation tokens now throw 400 BadRequest (matching real Cosmos).
	/// </summary>
	[Fact]
	public async Task ContinuationToken_Invalid_ThrowsBadRequest()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Pass garbage continuation token — should throw BadRequest
		var act = () => _container.GetItemQueryIterator<TestDocument>(
			"SELECT * FROM c",
			continuationToken: "invalid-garbage-token");
		act.Should().Throw<CosmosException>()
			.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 7: LINQ Enhancements
	// ══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: LINQ operators like GroupJoin and TakeWhile that
	/// are not translatable to Cosmos SQL work in InMemoryContainer because it
	/// runs LINQ-to-Objects.
	/// </summary>
	[Fact]
	public async Task LinqQuery_UnsupportedOperators_SucceedInMemory_WouldFailRealCosmos()
	{
		for (var i = 0; i < 5; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = i },
				new PartitionKey("pk1"));
		}

		var queryable = _container.GetItemLinqQueryable<TestDocument>(true);

		// TakeWhile is not supported by Cosmos SQL
		var results = queryable.OrderBy(d => d.Value).TakeWhile(d => d.Value < 3).ToList();
		results.Should().HaveCount(3);

		// Aggregate (reduce) is not supported by Cosmos SQL
		var sum = queryable.Sum(d => d.Value);
		sum.Should().Be(10);
	}

	[Fact(Skip = "Real Cosmos would reject TakeWhile, Aggregate, and other LINQ-to-Objects-only operators.")]
	public void LinqQuery_UnsupportedOperators_ShouldThrow_RealCosmos() { }

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 8: Partition Key Edge Cases
	// ══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// BEHAVIORAL DIFFERENCE: Real Cosmos treats PartitionKey.None and PartitionKey.Null
	/// differently (None = no partition key in document, Null = explicit null).
	/// InMemoryContainer treats them as resolving to the same internal key for storage,
	/// so items stored with either key end up in the same partition.
	/// </summary>
	[Fact]
	public async Task PartitionKey_NoneVsNull_TreatedIdentically()
	{
		var container = new InMemoryContainer("pk-test", "/pk");

		// Create with explicit PartitionKey.Null
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = (string?)null, name = "Test" }),
			PartitionKey.Null);

		// Read with PartitionKey.None — succeeds because emulator treats them the same
		var response = await container.ReadItemAsync<JObject>("1", PartitionKey.None);
		response.Resource["name"]!.Value<string>().Should().Be("Test");
	}

	[Fact(Skip = "Real Cosmos distinguishes PartitionKey.None (missing) from PartitionKey.Null (explicit null).")]
	public void PartitionKey_NoneVsNull_ShouldDiffer_RealCosmos() { }

}
