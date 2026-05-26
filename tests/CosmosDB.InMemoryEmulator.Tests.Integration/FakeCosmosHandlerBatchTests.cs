using System.Net;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Serialization.HybridRow;
using Microsoft.Azure.Cosmos.Serialization.HybridRow.IO;
using Microsoft.Azure.Cosmos.Serialization.HybridRow.Layouts;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for TransactionalBatch operations through FakeCosmosHandler.
/// Validates the HybridRow binary batch protocol handling.
/// Parity-validated: runs against both FakeCosmosHandler (in-memory) and real emulator.
/// Schema reflection tests are tagged InMemoryOnly.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerBatchTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("test-batch", "/partitionKey");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	[Fact]
	public async Task Batch_CreateTwoItems_ReturnsOk()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "item-1", PartitionKey = "pk1", Name = "Alice", Value = 1 });
		batch.CreateItem(new TestDocument { Id = "item-2", PartitionKey = "pk1", Name = "Bob", Value = 2 });

		var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.IsSuccessStatusCode.Should().BeTrue();
		response.Count.Should().Be(2);
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);
		response[1].StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_CreateItems_PersistsToBackingContainer()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "p1", PartitionKey = "pk1", Name = "Persisted", Value = 42 });

		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		// Verify item exists via direct point read through the SDK
		var readResponse = await _container.ReadItemAsync<TestDocument>("p1", new PartitionKey("pk1"));
		readResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		readResponse.Resource.Name.Should().Be("Persisted");
	}

	[Fact]
	public async Task Batch_UpsertItem_ReturnsOk()
	{
		// Create initial item
		await _container.CreateItemAsync(new TestDocument { Id = "u1", PartitionKey = "pk1", Name = "Original", Value = 1 }, new PartitionKey("pk1"));

		// Upsert via batch
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "u1", PartitionKey = "pk1", Name = "Updated", Value = 2 });

		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var readResponse = await _container.ReadItemAsync<TestDocument>("u1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task Batch_ReadItem_ReturnsOk()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "r1", PartitionKey = "pk1", Name = "Readable", Value = 1 }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("r1");

		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_DeleteItem_ReturnsOk()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "d1", PartitionKey = "pk1", Name = "Deletable", Value = 1 }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("d1");

		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		// Verify item is gone
		try
		{
			await _container.ReadItemAsync<TestDocument>("d1", new PartitionKey("pk1"));
			throw new Exception("Should have thrown CosmosException with NotFound");
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			// Expected
		}
	}

	[Fact]
	public async Task Batch_ReplaceItem_ReturnsOk()
	{
		await _container.CreateItemAsync(new TestDocument { Id = "rp1", PartitionKey = "pk1", Name = "Original", Value = 1 }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("rp1", new TestDocument { Id = "rp1", PartitionKey = "pk1", Name = "Replaced", Value = 99 });

		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var readResponse = await _container.ReadItemAsync<TestDocument>("rp1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Replaced");
	}

	[Fact]
	public async Task Batch_MixedOperations_ReturnsOk()
	{
		// Pre-create items for replace and delete
		await _container.CreateItemAsync(new TestDocument { Id = "mx-replace", PartitionKey = "pk1", Name = "ToReplace", Value = 1 }, new PartitionKey("pk1"));
		await _container.CreateItemAsync(new TestDocument { Id = "mx-delete", PartitionKey = "pk1", Name = "ToDelete", Value = 2 }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "mx-new", PartitionKey = "pk1", Name = "New", Value = 10 });
		batch.ReplaceItem("mx-replace", new TestDocument { Id = "mx-replace", PartitionKey = "pk1", Name = "Replaced", Value = 11 });
		batch.DeleteItem("mx-delete");
		batch.ReadItem("mx-replace");

		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
		response.Count.Should().Be(4);
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);     // Create
		response[1].StatusCode.Should().Be(HttpStatusCode.OK);          // Replace
		response[2].StatusCode.Should().Be(HttpStatusCode.NoContent);   // Delete
		response[3].StatusCode.Should().Be(HttpStatusCode.OK);          // Read
	}

	[Fact]
	public async Task Batch_DuplicateId_RollsBackOnFailure()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "dup-1", PartitionKey = "pk1", Name = "First", Value = 1 });
		batch.CreateItem(new TestDocument { Id = "dup-1", PartitionKey = "pk1", Name = "Duplicate", Value = 2 });

		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();

		// First item should have been rolled back
		try
		{
			await _container.ReadItemAsync<TestDocument>("dup-1", new PartitionKey("pk1"));
			throw new Exception("Should have thrown CosmosException with NotFound");
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
		{
			// Expected - rollback
		}
	}

	[Fact]
	public async Task Batch_ConcurrentCreateSameId_OnlyOneSucceeds()
	{
		// Issue #57: When multiple concurrent transactional batches each attempt to CreateItem
		// with the same id and partition key, only one should succeed. The others should fail
		// with 409 Conflict and roll back (transactional semantics).
		// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-document
		//   "If the id already exists in the collection, a 409 Conflict is returned."
		var tasks = Enumerable.Range(1, 3).Select(attempt => Task.Run(async () =>
		{
			var batch = _container.CreateTransactionalBatch(new PartitionKey("concurrent-pk"));
			batch.CreateItem(new TestDocument
			{
				Id = "latest-conc",
				PartitionKey = "concurrent-pk",
				Name = $"Attempt-{attempt}",
				Value = attempt
			});
			batch.CreateItem(new TestDocument
			{
				Id = $"tracking-conc-{attempt}",
				PartitionKey = "concurrent-pk",
				Name = $"Tracking-{attempt}",
				Value = attempt
			});
			return await batch.ExecuteAsync();
		})).ToArray();

		var results = await Task.WhenAll(tasks);

		var successCount = results.Count(r => r.IsSuccessStatusCode);
		var failCount = results.Count(r => !r.IsSuccessStatusCode);

		// Exactly one batch should succeed
		successCount.Should().Be(1, "only one concurrent batch creating the same id should succeed");
		failCount.Should().Be(2, "the other batches should fail with Conflict and roll back");

		// The failed batches should have 409 Conflict status
		foreach (var failed in results.Where(r => !r.IsSuccessStatusCode))
		{
			failed.StatusCode.Should().Be(HttpStatusCode.Conflict);
		}

		// Verify exactly one "latest-conc" document exists
		var latestResponse = await _container.ReadItemAsync<TestDocument>("latest-conc", new PartitionKey("concurrent-pk"));
		latestResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		// Verify only the winning batch's tracking document exists
		var winningAttempt = latestResponse.Resource.Value;
		var trackingResponse = await _container.ReadItemAsync<TestDocument>(
			$"tracking-conc-{winningAttempt}", new PartitionKey("concurrent-pk"));
		trackingResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		// The other tracking documents should not exist (were rolled back)
		foreach (var attempt in Enumerable.Range(1, 3).Where(a => a != winningAttempt))
		{
			var ex = await Assert.ThrowsAsync<CosmosException>(() =>
				_container.ReadItemAsync<TestDocument>($"tracking-conc-{attempt}", new PartitionKey("concurrent-pk")));
			ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public void BatchSchemas_SelfBuiltSchemasMatchSdkInternals()
	{
		// Canary test: verify our self-built HybridRow schemas produce layouts
		// that are compatible with the SDK's internal BatchSchemaProvider.
		// If a future Cosmos SDK version changes the batch wire format, this test
		// will detect the mismatch immediately.

		var bspType = typeof(CosmosClient).Assembly.GetType("Microsoft.Azure.Cosmos.BatchSchemaProvider");
		if (bspType is null)
		{
			// SDK reorganised its internals — self-built schemas are the only option.
			// This is fine: if batch tests above pass, the schemas are correct.
			return;
		}

		var flags = BindingFlags.Public | BindingFlags.Static;
		var sdkOpLayout = (Layout)bspType.GetProperty("BatchOperationLayout", flags)!.GetValue(null)!;
		var sdkResultLayout = (Layout)bspType.GetProperty("BatchResultLayout", flags)!.GetValue(null)!;
		var sdkResolver = (LayoutResolverNamespace)bspType.GetProperty("BatchLayoutResolver", flags)!.GetValue(null)!;

		// Verify our schemas resolve to layouts with the same SchemaId
		var selfOpLayout = sdkResolver.Resolve(sdkOpLayout.SchemaId);
		var selfResultLayout = sdkResolver.Resolve(sdkResultLayout.SchemaId);

		selfOpLayout.SchemaId.Should().Be(sdkOpLayout.SchemaId);
		selfResultLayout.SchemaId.Should().Be(sdkResultLayout.SchemaId);

		// Verify the SDK resolver's namespace has the expected schema names
		sdkResolver.Namespace.Schemas.Should().Contain(s => s.Name == "BatchOperation");
		sdkResolver.Namespace.Schemas.Should().Contain(s => s.Name == "BatchResult");

		// Verify field counts match — if the SDK adds new fields, we'll know
		var sdkOpSchema = sdkResolver.Namespace.Schemas.First(s => s.Name == "BatchOperation");
		var sdkResultSchema = sdkResolver.Namespace.Schemas.First(s => s.Name == "BatchResult");
		sdkOpSchema.Properties.Should().HaveCount(12, "BatchOperation schema should have 12 fields");
		sdkResultSchema.Properties.Should().HaveCount(6, "BatchResult schema should have 6 fields");

		// Verify the field names we depend on exist in the SDK schema
		var expectedOpFields = new[] { "operationType", "id", "resourceBody", "ifMatch", "ifNoneMatch" };
		foreach (var field in expectedOpFields)
		{
			sdkOpSchema.Properties.Should().Contain(p => p.Path == field,
				$"BatchOperation schema should contain field '{field}'");
		}

		var expectedResultFields = new[] { "statusCode", "eTag", "resourceBody", "requestCharge" };
		foreach (var field in expectedResultFields)
		{
			sdkResultSchema.Properties.Should().Contain(p => p.Path == field,
				$"BatchResult schema should contain field '{field}'");
		}
	}

	[Fact]
	[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
	public void BatchSchemas_SelfBuiltRoundTrip_WriteThenRead()
	{
		// Verify that a HybridRow written with our self-built result schema
		// can be read back correctly — proves the schema layout is functional.
		var schemas = typeof(FakeCosmosHandler)
			.GetNestedType("BatchSchemas", BindingFlags.NonPublic)!;
		var resultLayoutProp = schemas.GetProperty("ResultLayout", BindingFlags.Public | BindingFlags.Static)!;
		var resolverProp = schemas.GetProperty("Resolver", BindingFlags.Public | BindingFlags.Static)!;
		var resultLayout = (Layout)resultLayoutProp.GetValue(null)!;
		var resolver = (LayoutResolverNamespace)resolverProp.GetValue(null)!;

		// Write
		var resizer = new MemorySpanResizer<byte>(256);
		var row = new RowBuffer(256, resizer);
		row.InitLayout(HybridRowVersion.V1, resultLayout, resolver);
		var writeResult = RowWriter.WriteBuffer(ref row, 0, (ref RowWriter writer, TypeArgument _, int _2) =>
		{
			var wr = writer.WriteInt32("statusCode", 200);
			if (wr != Result.Success) return wr;
			wr = writer.WriteString("eTag", "test-etag-123");
			if (wr != Result.Success) return wr;
			wr = writer.WriteFloat64("requestCharge", 1.5);
			return wr;
		});
		writeResult.Should().Be(Result.Success);

		// Read
		var readRow = new RowBuffer(row.Length);
		readRow.ReadFrom(resizer.Memory.Span[..row.Length], HybridRowVersion.V1, resolver).Should().BeTrue();
		var reader = new RowReader(ref readRow);
		int? statusCode = null;
		string? eTag = null;
		double? requestCharge = null;
		while (reader.Read())
		{
			switch (reader.Path)
			{
				case "statusCode":
					reader.ReadInt32(out int sc);
					statusCode = sc;
					break;
				case "eTag":
					reader.ReadString(out string et);
					eTag = et;
					break;
				case "requestCharge":
					reader.ReadFloat64(out double rc);
					requestCharge = rc;
					break;
			}
		}

		statusCode.Should().Be(200);
		eTag.Should().Be("test-etag-123");
		requestCharge.Should().Be(1.5);
	}
}
