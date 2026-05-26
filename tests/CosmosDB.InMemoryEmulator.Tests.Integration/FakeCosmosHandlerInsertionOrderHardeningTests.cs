using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Hardening tests for insertion order behavior in queries without ORDER BY.
/// Covers edge cases: large batches, multi-partition interleaving, repeated replaces,
/// mixed operation sequences, projections, filters, pagination, and more.
/// Ref: Observed behavior on Windows Cosmos DB Emulator — documents return
///   in the order they were created when no ORDER BY is applied.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerInsertionOrderHardeningTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("test-insertion-order-hardening", "/partitionKey");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	private async Task<List<T>> DrainQuery<T>(string sql, string? partitionKey = null)
	{
		var options = partitionKey is not null
			? new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) }
			: null;
		var iterator = _container.GetItemQueryIterator<T>(sql, requestOptions: options);
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}

	private async Task<List<T>> DrainQuery<T>(QueryDefinition queryDef, string? partitionKey = null)
	{
		var options = partitionKey is not null
			? new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey) }
			: null;
		var iterator = _container.GetItemQueryIterator<T>(queryDef, requestOptions: options);
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  1. Large batch insertion order
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Inserting 50+ documents and querying without ORDER BY must return them
	/// in exact insertion order.
	/// </summary>
	[Fact]
	public async Task Query_LargeBatch_ReturnsDocsInInsertionOrder()
	{
		const string pk = "pk-large-batch";
		var insertedIds = new List<string>();

		for (var i = 1; i <= 55; i++)
		{
			var id = $"large-{i:D4}";
			insertedIds.Add(id);
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc {i}", Value = i },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(insertedIds);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  2. Multiple partitions interleaved
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Documents inserted across 3+ partitions in an interleaved pattern must
	/// appear in global insertion order when queried cross-partition.
	/// </summary>
	[Fact]
	public async Task Query_MultiplePKsInterleaved_ReturnsCrossPartitionInsertionOrder()
	{
		var partitions = new[] { "pk-interleave-a", "pk-interleave-b", "pk-interleave-c" };
		var insertedIds = new List<string>();

		for (var i = 1; i <= 15; i++)
		{
			var pk = partitions[(i - 1) % 3];
			var id = $"interleave-{i:D4}";
			insertedIds.Add(id);
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc {i}", Value = i },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<TestDocument>("SELECT * FROM c WHERE STARTSWITH(c.partitionKey, 'pk-interleave-')");

		results.Select(r => r.Id).Should().Equal(insertedIds);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  3. Replace multiple times
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Replacing the same document 5+ times must never change its position in
	/// the insertion order.
	/// </summary>
	[Fact]
	public async Task Query_AfterMultipleReplacesOnSameDoc_PositionNeverChanges()
	{
		const string pk = "pk-multi-replace";
		var insertedIds = new List<string>();

		for (var i = 1; i <= 5; i++)
		{
			var id = $"mr-{i:D4}";
			insertedIds.Add(id);
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc {i}", Value = i },
				new PartitionKey(pk));
		}

		// Replace the 3rd document 6 times with different content each time
		for (var r = 1; r <= 6; r++)
		{
			await _container.ReplaceItemAsync(
				new TestDocument { Id = "mr-0003", PartitionKey = pk, Name = $"Replaced-{r}", Value = 100 + r },
				"mr-0003",
				new PartitionKey(pk));
		}

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(insertedIds);
		// Verify the content was actually updated
		results.Single(r => r.Id == "mr-0003").Name.Should().Be("Replaced-6");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  4. Mixed operations sequence
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Complex mixed operation sequence:
	/// create A, B, C, D, E → delete C → replace A → create F → upsert B (existing) → upsert G (new)
	/// Expected order: A, B, D, E, F, G
	/// </summary>
	[Fact]
	public async Task Query_MixedOperationSequence_ReturnsExpectedInsertionOrder()
	{
		const string pk = "pk-mixed-ops";

		// Create A, B, C, D, E
		await _container.CreateItemAsync(
			new TestDocument { Id = "mixed-A", PartitionKey = pk, Name = "A", Value = 1 },
			new PartitionKey(pk));
		await _container.CreateItemAsync(
			new TestDocument { Id = "mixed-B", PartitionKey = pk, Name = "B", Value = 2 },
			new PartitionKey(pk));
		await _container.CreateItemAsync(
			new TestDocument { Id = "mixed-C", PartitionKey = pk, Name = "C", Value = 3 },
			new PartitionKey(pk));
		await _container.CreateItemAsync(
			new TestDocument { Id = "mixed-D", PartitionKey = pk, Name = "D", Value = 4 },
			new PartitionKey(pk));
		await _container.CreateItemAsync(
			new TestDocument { Id = "mixed-E", PartitionKey = pk, Name = "E", Value = 5 },
			new PartitionKey(pk));

		// Delete C
		await _container.DeleteItemAsync<TestDocument>("mixed-C", new PartitionKey(pk));

		// Replace A (should not change position)
		await _container.ReplaceItemAsync(
			new TestDocument { Id = "mixed-A", PartitionKey = pk, Name = "A-replaced", Value = 100 },
			"mixed-A",
			new PartitionKey(pk));

		// Create F (should appear at end)
		await _container.CreateItemAsync(
			new TestDocument { Id = "mixed-F", PartitionKey = pk, Name = "F", Value = 6 },
			new PartitionKey(pk));

		// Upsert B (existing — should not change position)
		await _container.UpsertItemAsync(
			new TestDocument { Id = "mixed-B", PartitionKey = pk, Name = "B-upserted", Value = 200 },
			new PartitionKey(pk));

		// Upsert G (new — should appear at end)
		await _container.UpsertItemAsync(
			new TestDocument { Id = "mixed-G", PartitionKey = pk, Name = "G", Value = 7 },
			new PartitionKey(pk));

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(
			["mixed-A", "mixed-B", "mixed-D", "mixed-E", "mixed-F", "mixed-G"]);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  5. Query with SELECT specific fields (projection)
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Projecting specific fields (SELECT c.id, c.name) must still maintain
	/// insertion order.
	/// </summary>
	[Fact]
	public async Task Query_WithProjection_MaintainsInsertionOrder()
	{
		const string pk = "pk-projection";
		var insertedIds = new List<string>();

		for (var i = 1; i <= 8; i++)
		{
			var id = $"proj-{i:D4}";
			insertedIds.Add(id);
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Name-{i}", Value = i },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<ProjectedDocument>(
			$"SELECT c.id, c.name FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(insertedIds);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  6. Query with WHERE filter
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Filtering with a WHERE clause must preserve relative insertion order of
	/// the matched documents.
	/// </summary>
	[Fact]
	public async Task Query_WithWhereFilter_MaintainsRelativeInsertionOrder()
	{
		const string pk = "pk-where-filter";

		// Insert with alternating isActive values: true, false, true, false, true...
		for (var i = 1; i <= 10; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument
				{
					Id = $"filter-{i:D4}",
					PartitionKey = pk,
					Name = $"Doc {i}",
					Value = i,
					IsActive = i % 2 != 0
				},
				new PartitionKey(pk));
		}

		// Query only active documents
		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}' AND c.isActive = true", pk);

		// Expect odd-numbered docs only, in original order
		results.Select(r => r.Id).Should().Equal(
			["filter-0001", "filter-0003", "filter-0005", "filter-0007", "filter-0009"]);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  7. Query with TOP/OFFSET pagination
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Using TOP N returns the first N documents in insertion order.
	/// </summary>
	[Fact]
	public async Task Query_WithTop_ReturnsFirstNInInsertionOrder()
	{
		const string pk = "pk-top";
		var insertedIds = new List<string>();

		for (var i = 1; i <= 20; i++)
		{
			var id = $"top-{i:D4}";
			insertedIds.Add(id);
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc {i}", Value = i },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<TestDocument>(
			$"SELECT TOP 5 * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(insertedIds.Take(5));
	}

	/// <summary>
	/// Using OFFSET/LIMIT returns the correct page of documents in insertion order.
	/// </summary>
	[Fact]
	public async Task Query_WithOffsetLimit_ReturnsCorrectPageInInsertionOrder()
	{
		const string pk = "pk-offset";
		var insertedIds = new List<string>();

		for (var i = 1; i <= 20; i++)
		{
			var id = $"offset-{i:D4}";
			insertedIds.Add(id);
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc {i}", Value = i },
				new PartitionKey(pk));
		}

		// Get page 2 (items 6-10)
		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}' OFFSET 5 LIMIT 5", pk);

		results.Select(r => r.Id).Should().Equal(insertedIds.Skip(5).Take(5));
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  8. Empty container query
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Querying an empty container must return an empty result without errors.
	/// </summary>
	[Fact]
	public async Task Query_EmptyContainer_ReturnsEmptyResults()
	{
		var results = await DrainQuery<TestDocument>(
			"SELECT * FROM c WHERE c.partitionKey = 'pk-nonexistent'", "pk-nonexistent");

		results.Should().BeEmpty();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  9. Single document
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// A container with a single document must return just that document.
	/// </summary>
	[Fact]
	public async Task Query_SingleDocument_ReturnsThatDocument()
	{
		const string pk = "pk-single-doc";

		await _container.CreateItemAsync(
			new TestDocument { Id = "only-one", PartitionKey = pk, Name = "Solo", Value = 42 },
			new PartitionKey(pk));

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Should().HaveCount(1);
		results[0].Id.Should().Be("only-one");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  10. Delete all then recreate
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// After deleting all documents and inserting new ones, the new documents
	/// should appear in their own insertion order with no ghost entries.
	/// </summary>
	[Fact]
	public async Task Query_DeleteAllThenRecreate_ShowsOnlyNewDocsInOrder()
	{
		const string pk = "pk-del-all-recreate";

		// Insert A, B, C
		await _container.CreateItemAsync(
			new TestDocument { Id = "orig-A", PartitionKey = pk, Name = "A", Value = 1 },
			new PartitionKey(pk));
		await _container.CreateItemAsync(
			new TestDocument { Id = "orig-B", PartitionKey = pk, Name = "B", Value = 2 },
			new PartitionKey(pk));
		await _container.CreateItemAsync(
			new TestDocument { Id = "orig-C", PartitionKey = pk, Name = "C", Value = 3 },
			new PartitionKey(pk));

		// Delete all
		await _container.DeleteItemAsync<TestDocument>("orig-A", new PartitionKey(pk));
		await _container.DeleteItemAsync<TestDocument>("orig-B", new PartitionKey(pk));
		await _container.DeleteItemAsync<TestDocument>("orig-C", new PartitionKey(pk));

		// Insert D, E
		await _container.CreateItemAsync(
			new TestDocument { Id = "new-D", PartitionKey = pk, Name = "D", Value = 4 },
			new PartitionKey(pk));
		await _container.CreateItemAsync(
			new TestDocument { Id = "new-E", PartitionKey = pk, Name = "E", Value = 5 },
			new PartitionKey(pk));

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(["new-D", "new-E"]);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  11. Upsert-only to create multiple documents
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Using only UpsertItemAsync to create new documents must produce results
	/// in the order the upserts were called.
	/// </summary>
	[Fact]
	public async Task Query_UpsertOnlyCreates_ReturnsInUpsertCallOrder()
	{
		const string pk = "pk-upsert-create";
		var insertedIds = new List<string>();

		for (var i = 1; i <= 10; i++)
		{
			var id = $"upsert-new-{i:D4}";
			insertedIds.Add(id);
			await _container.UpsertItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc {i}", Value = i },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(insertedIds);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  12. Query with DISTINCT
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// DISTINCT on a field with duplicates must preserve the insertion order of
	/// the first occurrence of each distinct value.
	/// </summary>
	[Fact]
	public async Task Query_WithDistinctValue_MaintainsFirstOccurrenceInsertionOrder()
	{
		const string pk = "pk-distinct";

		// Insert documents with duplicate Value fields
		// Values: 10, 20, 10, 30, 20, 40
		var docs = new[]
		{
			("dist-01", 10), ("dist-02", 20), ("dist-03", 10),
			("dist-04", 30), ("dist-05", 20), ("dist-06", 40)
		};
		foreach (var (id, value) in docs)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc-{value}", Value = value },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<int>(
			$"SELECT DISTINCT VALUE c[\"value\"] FROM c WHERE c.partitionKey = '{pk}'", pk);

		// First occurrences in insertion order: 10, 20, 30, 40
		results.Should().Equal([10, 20, 30, 40]);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  13. Aggregate functions don't crash
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// COUNT aggregate should return the correct count without crashing.
	/// </summary>
	[Fact]
	public async Task Query_CountAggregate_ReturnsCorrectCount()
	{
		const string pk = "pk-agg-count";

		for (var i = 1; i <= 7; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"agg-{i:D4}", PartitionKey = pk, Name = $"Doc {i}", Value = i },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<int>(
			$"SELECT VALUE COUNT(1) FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Should().HaveCount(1);
		results[0].Should().Be(7);
	}

	/// <summary>
	/// SUM aggregate should return the correct sum without crashing.
	/// </summary>
	[Fact]
	public async Task Query_SumAggregate_ReturnsCorrectSum()
	{
		const string pk = "pk-agg-sum";

		for (var i = 1; i <= 5; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"sum-{i:D4}", PartitionKey = pk, Name = $"Doc {i}", Value = i * 10 },
				new PartitionKey(pk));
		}

		// SUM of 10+20+30+40+50 = 150
		var results = await DrainQuery<int>(
			$"SELECT VALUE SUM(c[\"value\"]) FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Should().HaveCount(1);
		results[0].Should().Be(150);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  14. Cross-partition: with vs without partition key filter
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// When querying within a single partition, insertion order within that
	/// partition is preserved regardless of documents in other partitions.
	/// </summary>
	[Fact]
	public async Task Query_SinglePartitionFilter_PreservesOrderWithinPartition()
	{
		var pkTarget = "pk-xpart-target";
		var pkOther = "pk-xpart-other";

		// Interleave: target-1, other-1, target-2, other-2, target-3
		await _container.CreateItemAsync(
			new TestDocument { Id = "xpart-t1", PartitionKey = pkTarget, Name = "T1", Value = 1 },
			new PartitionKey(pkTarget));
		await _container.CreateItemAsync(
			new TestDocument { Id = "xpart-o1", PartitionKey = pkOther, Name = "O1", Value = 2 },
			new PartitionKey(pkOther));
		await _container.CreateItemAsync(
			new TestDocument { Id = "xpart-t2", PartitionKey = pkTarget, Name = "T2", Value = 3 },
			new PartitionKey(pkTarget));
		await _container.CreateItemAsync(
			new TestDocument { Id = "xpart-o2", PartitionKey = pkOther, Name = "O2", Value = 4 },
			new PartitionKey(pkOther));
		await _container.CreateItemAsync(
			new TestDocument { Id = "xpart-t3", PartitionKey = pkTarget, Name = "T3", Value = 5 },
			new PartitionKey(pkTarget));

		// Query with partition key filter — should give target docs in their insertion order
		var filteredResults = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pkTarget}'", pkTarget);
		filteredResults.Select(r => r.Id).Should().Equal(["xpart-t1", "xpart-t2", "xpart-t3"]);

		// Cross-partition query — should give all docs in global insertion order
		var allResults = await DrainQuery<TestDocument>(
			"SELECT * FROM c WHERE STARTSWITH(c.partitionKey, 'pk-xpart-')");
		allResults.Select(r => r.Id).Should().Equal(
			["xpart-t1", "xpart-o1", "xpart-t2", "xpart-o2", "xpart-t3"]);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  15. Rapid sequential creates
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Rapidly creating 20 documents sequentially (each awaited) must produce
	/// results in the exact call order.
	/// </summary>
	[Fact]
	public async Task Query_RapidSequentialCreates_MaintainsCallOrder()
	{
		const string pk = "pk-rapid";
		var insertedIds = new List<string>();

		for (var i = 1; i <= 20; i++)
		{
			var id = $"rapid-{i:D4}";
			insertedIds.Add(id);
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Rapid {i}", Value = i },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(insertedIds);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Additional hardening: ORDER BY overrides insertion order
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// A query with ORDER BY must return documents sorted by the specified field,
	/// overriding the natural insertion order.
	/// </summary>
	[Fact]
	public async Task Query_WithOrderBy_OverridesInsertionOrder()
	{
		const string pk = "pk-orderby";

		// Insert in non-sorted order: values 50, 10, 40, 20, 30
		var values = new[] { 50, 10, 40, 20, 30 };
		for (var i = 0; i < values.Length; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument
				{
					Id = $"orderby-{i + 1:D4}",
					PartitionKey = pk,
					Name = $"Doc {values[i]}",
					Value = values[i]
				},
				new PartitionKey(pk));
		}

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}' ORDER BY c[\"value\"] ASC", pk);

		results.Select(r => r.Value).Should().Equal([10, 20, 30, 40, 50]);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Additional hardening: WHERE + VALUE filter preserves relative order
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// A range filter (WHERE c.value > X) must preserve relative insertion order
	/// of the matching documents.
	/// </summary>
	[Fact]
	public async Task Query_WithRangeFilter_MaintainsRelativeInsertionOrder()
	{
		const string pk = "pk-range-filter";

		// Insert values: 5, 15, 3, 25, 8, 30, 2
		var values = new[] { 5, 15, 3, 25, 8, 30, 2 };
		var expectedIdsAbove10 = new List<string>();

		for (var i = 0; i < values.Length; i++)
		{
			var id = $"range-{i + 1:D4}";
			if (values[i] > 10) expectedIdsAbove10.Add(id);
			await _container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = pk, Name = $"Doc-{values[i]}", Value = values[i] },
				new PartitionKey(pk));
		}

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}' AND c[\"value\"] > 10", pk);

		// Expected: range-0002 (15), range-0004 (25), range-0006 (30) — in insertion order
		results.Select(r => r.Id).Should().Equal(expectedIdsAbove10);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Additional hardening: Delete from middle, verify remaining order
	// ═══════════════════════════════════════════════════════════════════════════

	/// <summary>
	/// Deleting multiple non-consecutive documents must not disturb the relative
	/// order of the remaining documents.
	/// </summary>
	[Fact]
	public async Task Query_AfterMultipleNonConsecutiveDeletes_PreservesRemainingOrder()
	{
		const string pk = "pk-multi-del";

		for (var i = 1; i <= 10; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"mdel-{i:D4}", PartitionKey = pk, Name = $"Doc {i}", Value = i },
				new PartitionKey(pk));
		}

		// Delete positions 2, 5, 8 (non-consecutive)
		await _container.DeleteItemAsync<TestDocument>("mdel-0002", new PartitionKey(pk));
		await _container.DeleteItemAsync<TestDocument>("mdel-0005", new PartitionKey(pk));
		await _container.DeleteItemAsync<TestDocument>("mdel-0008", new PartitionKey(pk));

		var results = await DrainQuery<TestDocument>(
			$"SELECT * FROM c WHERE c.partitionKey = '{pk}'", pk);

		results.Select(r => r.Id).Should().Equal(
			["mdel-0001", "mdel-0003", "mdel-0004", "mdel-0006", "mdel-0007", "mdel-0009", "mdel-0010"]);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Projection helper models
	// ═══════════════════════════════════════════════════════════════════════════

	private class ProjectedDocument
	{
		[JsonProperty("id")]
		public string Id { get; set; } = default!;

		[JsonProperty("name")]
		public string Name { get; set; } = default!;
	}

}
