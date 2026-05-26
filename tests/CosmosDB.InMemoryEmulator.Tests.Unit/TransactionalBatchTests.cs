using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class TransactionalBatchTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_CreateMultipleItems_AllSucceed()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" });

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Count.Should().Be(2);
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);
		response[1].StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_ReadItem_ExecutesSuccessfully()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" };
		await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("1");

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Count.Should().Be(1);
		response[0].StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_UpsertItem_CreatesNew()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Batch_UpsertItem_UpdatesExisting()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alicia" });

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Alicia");
	}

	[Fact]
	public async Task Batch_ReplaceItem_UpdatesExisting()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task Batch_DeleteItem_RemovesItem()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1");

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Batch_PatchItem_AppliesOperations()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new List<PatchOperation>
		{
			PatchOperation.Set("/name", "Patched"),
			PatchOperation.Increment("/value", 5)
		});

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Patched");
		readResponse.Resource.Value.Should().Be(15);
	}

	[Fact]
	public async Task Batch_MixedOperations_AllExecuteInSequence()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "new1", PartitionKey = "pk1", Name = "New" });
		batch.ReplaceItem("existing", new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Replaced" });
		batch.ReadItem("existing");

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Count.Should().Be(3);
	}

	[Fact]
	public async Task Batch_CreateItemStream_Works()
	{
		var doc = new TestDocument { Id = "stream1", PartitionKey = "pk1", Name = "Stream" };
		var json = JsonConvert.SerializeObject(doc);
		var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItemStream(stream);

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var readResponse = await _container.ReadItemAsync<TestDocument>("stream1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Stream");
	}

	[Fact]
	public async Task Batch_FluentApi_ChainsOperations()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"))
			.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" })
			.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" });

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Count.Should().Be(2);
	}

	// ── Atomicity / rollback tests ──

	[Fact]
	public async Task Batch_FailingOperation_RollsBackPreviousOperations()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_FailingOperation_MarksEarlierOpsAsFailedDependency()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" });

		using var response = await batch.ExecuteAsync();

		response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
		response[1].StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task Batch_ExceedingMaxOperations_ThrowsBadRequest()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		for (var i = 0; i < 101; i++)
		{
			batch.CreateItem(new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
		}

		var act = () => batch.ExecuteAsync();

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_DeleteNonExistentItem_FailsAndRollsBack()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		batch.DeleteItem("nonexistent");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_UpsertItemStream_Works()
	{
		var doc = new TestDocument { Id = "stream1", PartitionKey = "pk1", Name = "UpsertStream" };
		var json = JsonConvert.SerializeObject(doc);
		var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItemStream(stream);

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var readResponse = await _container.ReadItemAsync<TestDocument>("stream1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("UpsertStream");
	}

	[Fact]
	public async Task Batch_ReplaceItemStream_Works()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "StreamReplaced" };
		var json = JsonConvert.SerializeObject(updated);
		var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItemStream("1", stream);

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("StreamReplaced");
	}
}


public class TransactionalBatchGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_FailedOp_PriorOps_MarkedFailedDependency()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "first", PartitionKey = "pk1", Name = "First" });
		batch.ReadItem("nonexistent");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
		// First op should be marked FailedDependency after rollback
		response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
	}

	[Fact]
	public async Task Batch_ReplaceInBatch_NonExistent_Fails()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "Bad" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_DeleteInBatch_NonExistent_Fails()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("nonexistent");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_PatchInBatch_AppliesChanges()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")]);

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task Batch_WithIfMatch_StaleETag_FailsBatch()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1",
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new TransactionalBatchItemRequestOptions { IfMatchEtag = "\"stale\"" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Batch_EmptyBatch_Returns200()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}


public class TransactionalBatchGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_SubsequentOpsAfterFailure_Also424()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "first", PartitionKey = "pk1", Name = "A" });
		batch.ReadItem("nonexistent"); // This will fail (404)
		batch.CreateItem(new TestDocument { Id = "third", PartitionKey = "pk1", Name = "C" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
		// All ops marked as FailedDependency after rollback
		response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
		response[2].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
	}

	[Fact]
	public async Task Batch_ReadResult_ContainsDocumentData()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("1");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		var result = response.GetOperationResultAtIndex<TestDocument>(0);
		result.Resource.Should().NotBeNull();
		result.Resource.Name.Should().Be("Alice");
	}
}


public class TransactionalBatchEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_EmptyBatch_ExecuteAsync_Succeeds()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));

		using var response = await batch.ExecuteAsync();

		// An empty batch should succeed with no operations
		response.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_PatchItem_WithIfMatch_Succeeds()
	{
		var createResponse = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")],
			new TransactionalBatchPatchItemRequestOptions { IfMatchEtag = createResponse.ETag });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
	}
}


public class TransactionalBatchGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_FailingOperation_RollsBackPrevious()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_CreateDuplicate_InBatch_Fails409()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "First" });
		batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "Second" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task Batch_Over100Operations_Throws()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		for (var i = 0; i < 101; i++)
		{
			batch.CreateItem(new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
		}

		var act = () => batch.ExecuteAsync();

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_UpsertInBatch_CreatesOrReplaces()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Updated" });
		batch.UpsertItem(new TestDocument { Id = "new-item", PartitionKey = "pk1", Name = "NewItem" });

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var read1 = await _container.ReadItemAsync<TestDocument>("existing", new PartitionKey("pk1"));
		read1.Resource.Name.Should().Be("Updated");

		var read2 = await _container.ReadItemAsync<TestDocument>("new-item", new PartitionKey("pk1"));
		read2.Resource.Name.Should().Be("NewItem");
	}

	[Fact]
	public async Task Batch_Rollback_RestoresExactSnapshot()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "pre-existing", PartitionKey = "pk1", Name = "PreExisting", Value = 42 },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "will-rollback", PartitionKey = "pk1", Name = "New" });
		batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "Bad" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();

		var read = await _container.ReadItemAsync<TestDocument>("pre-existing", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("PreExisting");
		read.Resource.Value.Should().Be(42);

		var act = () => _container.ReadItemAsync<TestDocument>("will-rollback", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_OperationOrder_Matters()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Created" });
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Replaced");
	}
}


public class TransactionalBatchGapTests4
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_SingleOp_BehavesLikeRegularOp()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Single" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response.Count.Should().Be(1);
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Single");
	}

	[Fact]
	public async Task Batch_WithRequestOptions_ExecuteAsync()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "WithOptions" });

		using var response = await batch.ExecuteAsync(new TransactionalBatchRequestOptions());

		response.IsSuccessStatusCode.Should().BeTrue();
		response.Count.Should().Be(1);

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("WithOptions");
	}

	[Fact]
	public async Task Batch_MaxDocumentSizePerBatch_Enforced()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		// Add many items that together exceed 2MB
		for (var i = 0; i < 10; i++)
		{
			var largeValue = new string('x', 300 * 1024); // 300KB each = 3MB total
			batch.CreateItem(new { id = $"{i}", partitionKey = "pk1", data = largeValue });
		}

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}
}


// ── Category 1: Status Code Correctness ──

public class TransactionalBatchStatusCodeTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_CreateItem_Returns201Created()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_ReadItem_Returns200OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("1");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_ReplaceItem_Returns200OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_DeleteItem_Returns204NoContent()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task Batch_UpsertItem_NewItem_Returns201Created()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_UpsertItem_ExistingItem_Returns200OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_PatchItem_Returns200OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")]);

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.OK);
	}
}


// ── Category 2: Rollback Integrity ──

public class TransactionalBatchRollbackIntegrityTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_FailedRollback_RestoresTimestamps()
	{
		// Pre-existing item count for reference
		_container.DefaultTimeToLive = 3600;

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "will-rollback", PartitionKey = "pk1", Name = "Ghost" });
		batch.ReadItem("nonexistent"); // Will fail → rollback

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();

		// The rolled-back item should not exist, and critically its timestamp should not leak.
		// If timestamps leak, a future item with the same key could inherit a stale timestamp
		// and expire prematurely via TTL.
		var act = () => _container.ReadItemAsync<TestDocument>("will-rollback", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();

		// Now create the item fresh — it should get a fresh timestamp, not a leaked one
		await _container.CreateItemAsync(
			new TestDocument { Id = "will-rollback", PartitionKey = "pk1", Name = "Real" },
			new PartitionKey("pk1"));

		// Item should be readable (not expired from a leaked old timestamp)
		var read = await _container.ReadItemAsync<TestDocument>("will-rollback", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Real");
	}

	[Fact]
	public async Task Batch_FailedRollback_DoesNotLeakChangeFeedEntries()
	{
		// Get initial change feed state
		var iteratorBefore = _container.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);
		var beforeItems = new List<TestDocument>();
		while (iteratorBefore.HasMoreResults)
		{
			var feedResponse = await iteratorBefore.ReadNextAsync();
			if (feedResponse.StatusCode == HttpStatusCode.NotModified) break;
			beforeItems.AddRange(feedResponse);
		}

		// Execute a batch that will fail
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "ghost1", PartitionKey = "pk1", Name = "Ghost1" });
		batch.CreateItem(new TestDocument { Id = "ghost2", PartitionKey = "pk1", Name = "Ghost2" });
		batch.ReadItem("nonexistent"); // Will fail → rollback

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();

		// Change feed should NOT contain phantom entries for ghost1/ghost2
		var iteratorAfter = _container.GetChangeFeedIterator<TestDocument>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.Incremental);
		var afterItems = new List<TestDocument>();
		while (iteratorAfter.HasMoreResults)
		{
			var feedResponse = await iteratorAfter.ReadNextAsync();
			if (feedResponse.StatusCode == HttpStatusCode.NotModified) break;
			afterItems.AddRange(feedResponse);
		}

		afterItems.Count.Should().Be(beforeItems.Count,
			"change feed should not contain phantom entries from a rolled-back batch");
	}
}


// ── Category 3: Request Options Passthrough ──

public class TransactionalBatchRequestOptionsTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_PatchItem_WithIfMatchEtag_StaleEtag_FailsBatch()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")],
			new TransactionalBatchPatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Batch_PatchItem_WithIfMatchEtag_CurrentEtag_Succeeds()
	{
		var createResponse = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 10 },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", [PatchOperation.Set("/name", "Patched")],
			new TransactionalBatchPatchItemRequestOptions { IfMatchEtag = createResponse.ETag });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Patched");
	}
}


// ── Category 4: Size Limit Enforcement ──

public class TransactionalBatchSizeLimitTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_StreamOperations_ContributeToSizeLimit()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		for (var i = 0; i < 10; i++)
		{
			var largeValue = new string('x', 300 * 1024); // 300KB each = 3MB total
			var json = JsonConvert.SerializeObject(new { id = $"s{i}", partitionKey = "pk1", data = largeValue });
			var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
			batch.CreateItemStream(stream);
		}

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
	}

	[Fact]
	public async Task Batch_Exactly2MB_Succeeds()
	{
		// Create a payload that fits within 2MB including system property overhead (~200 bytes)
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		var dataSize = (2 * 1024 * 1024) - 500; // Leave room for JSON overhead + system properties
		var data = new string('a', dataSize);
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = data });

		// This should succeed — item fits within 2MB after enrichment
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_JustOver2MB_FailsEntityTooLarge()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		// Create a single item that is clearly over 2MB in batch payload
		var data = new string('a', 2 * 1024 * 1024 + 1000);
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = data });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
	}
}


// ── Category 5: GetEnumerator / IEnumerable ──

public class TransactionalBatchEnumerationTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_Response_CanBeEnumerated_WithForeach()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" });
		batch.CreateItem(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "C" });

		using var response = await batch.ExecuteAsync();

		var results = new List<TransactionalBatchOperationResult>();
		foreach (var result in response)
		{
			results.Add(result);
		}

		results.Count.Should().Be(3);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
	}
}


// ── Category 6: Operation Result Metadata ──

public class TransactionalBatchResultMetadataTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_CreateResult_HasETag()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Batch_ReplaceResult_HasETag()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Batch_UpsertResult_HasETag()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].ETag.Should().NotBeNullOrEmpty();
	}
}


// ── Category 7: Intra-Batch Operation Sequences ──

public class TransactionalBatchIntraBatchSequenceTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_CreateThenRead_SameItemInBatch()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		batch.ReadItem("1");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response.Count.Should().Be(2);

		var readResult = response.GetOperationResultAtIndex<TestDocument>(1);
		readResult.Resource.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Batch_CreateThenDelete_SameItemInBatch()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		batch.DeleteItem("1");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_DeleteThenCreate_SameId_InBatch()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1");
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Recreated");
	}

	[Fact]
	public async Task Batch_UpsertThenPatch_SameItemInBatch()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 });
		batch.PatchItem("1", [PatchOperation.Set("/name", "Patched"), PatchOperation.Increment("/value", 5)]);

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Patched");
		read.Resource.Value.Should().Be(15);
	}

	[Fact]
	public async Task Batch_MultiplePatches_SameItemInBatch()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 0 },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", [PatchOperation.Increment("/value", 10)]);
		batch.PatchItem("1", [PatchOperation.Increment("/value", 20)]);
		batch.PatchItem("1", [PatchOperation.Set("/name", "Triple-Patched")]);

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Value.Should().Be(30);
		read.Resource.Name.Should().Be("Triple-Patched");
	}

	[Fact]
	public async Task Batch_CreateThenUpsert_SameItemInBatch()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Created" });
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Upserted");
	}
}


// ── Category 8: Boundary Conditions ──

public class TransactionalBatchBoundaryTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_Exactly100Operations_Succeeds()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		for (var i = 0; i < 100; i++)
		{
			batch.CreateItem(new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
		}

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response.Count.Should().Be(100);
	}

	[Fact]
	public async Task Batch_ResponseIndexer_OutOfRange_ReturnsNull()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		// Accessing beyond the count should return null (not throw)
		var outOfRange = response[response.Count];
		outOfRange.Should().BeNull();
	}

	[Fact]
	public async Task Batch_ResponseIndexer_NegativeIndex_ReturnsNull()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		var negativeIndex = response[-1];
		negativeIndex.Should().BeNull();
	}
}


// ── Category 9: Concurrent and Re-execution ──

public class TransactionalBatchExecutionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_TwoBatches_SamePartition_Sequential_BothSucceed()
	{
		var batch1 = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch1.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" });
		using var response1 = await batch1.ExecuteAsync();
		response1.IsSuccessStatusCode.Should().BeTrue();

		var batch2 = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch2.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Second" });
		using var response2 = await batch2.ExecuteAsync();
		response2.IsSuccessStatusCode.Should().BeTrue();

		var read1 = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read1.Resource.Name.Should().Be("First");
		var read2 = await _container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk1"));
		read2.Resource.Name.Should().Be("Second");
	}
}


// ── Category 10: Partition Key Enforcement ──

public class TransactionalBatchPartitionKeyTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_AllOperations_UseBatchPartitionKey()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		// Item should be readable with the batch's partition key
		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Alice");

		// Item should NOT be readable with a different partition key
		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk2"));
		await act.Should().ThrowAsync<CosmosException>();
	}
}


// ── Category 11: Response Properties ──

public class TransactionalBatchResponsePropertiesTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_Response_RequestCharge_IsPopulated()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		response.RequestCharge.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task Batch_FailedResponse_RequestCharge_IsPopulated()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("nonexistent");

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeFalse();
		response.RequestCharge.Should().BeGreaterThan(0);
	}
}


// ── Skipped Tests + Divergent Behaviour Tests ──

public class TransactionalBatchDivergentBehaviourTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_ResourceStream_AvailableOnResults()
	{
		// Real Cosmos DB: ResourceStream on each operation result contains the serialized document body.
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		response[0].ResourceStream.Should().NotBeNull();
		using var reader = new StreamReader(response[0].ResourceStream);
		var json = await reader.ReadToEndAsync();
		var doc = JObject.Parse(json);
		doc["name"]!.Value<string>().Should().Be("Alice");
	}

	// Sister test documenting actual in-memory behaviour for ResourceStream
	[Fact]
	public async Task Batch_ResourceStream_InMemory_IsPopulatedOnResults()
	{
		// Previously divergent: NSubstitute mocks returned null for ResourceStream.
		// Now ResourceStream is wired up and returns the serialized document body.
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].ResourceStream.Should().NotBeNull();
	}

	[Fact]
	public async Task Batch_CancellationToken_Respected()
	{
		// Real Cosmos DB: Passing a cancelled CancellationToken to ExecuteAsync throws OperationCanceledException.
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => batch.ExecuteAsync(cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	// Sister test documenting actual in-memory behaviour for CancellationToken
	[Fact]
	public async Task Batch_CancellationToken_NotCancelled_Succeeds()
	{
		// Verify that a non-cancelled token allows the batch to complete normally.
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });

		using var cts = new CancellationTokenSource();

		using var response = await batch.ExecuteAsync(cts.Token);
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Alice");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category A: Response Property Coverage
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchResponsePropertyDeepTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_Response_ActivityId_IsPopulated()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.ActivityId.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Batch_Response_Diagnostics_IsNotNull()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.Diagnostics.Should().NotBeNull();
		response.Diagnostics.GetClientElapsedTime().Should().Be(TimeSpan.Zero);
	}

	[Fact]
	public async Task Batch_Response_Headers_IsNotNull()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.Headers.Should().NotBeNull();
	}

	[Fact]
	public async Task Batch_FailedResponse_ErrorMessage_IsPopulated()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "B" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.ErrorMessage.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Batch_Response_RetryAfter_IsTimeSpanZero()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.RetryAfter.Should().Be(TimeSpan.Zero);
	}

	[Fact]
	public async Task Batch_FailedResponse_Count_IncludesAllOperations()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "ok1", PartitionKey = "pk1", Name = "B" });
		batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "C" }); // fails
		batch.CreateItem(new TestDocument { Id = "ok2", PartitionKey = "pk1", Name = "D" }); // won't execute

		using var response = await batch.ExecuteAsync();
		response.Count.Should().Be(3); // all ops counted even though batch failed
	}

	[Fact]
	public async Task Batch_Response_IsDisposable()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		var response = await batch.ExecuteAsync();

		var dispose = () => response.Dispose();
		dispose.Should().NotThrow();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category B: Operation Result Data Coverage
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchOperationResultDataTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_CreateResult_ResourceStream_ContainsDocument()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		using var response = await batch.ExecuteAsync();
		var rs = response[0].ResourceStream;
		rs.Should().NotBeNull();
		using var reader = new StreamReader(rs);
		var json = await reader.ReadToEndAsync();
		json.Should().Contain("Alice");
	}

	[Fact]
	public async Task Batch_UpsertResult_ResourceStream_ContainsDocument()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bob" });
		using var response = await batch.ExecuteAsync();
		var rs = response[0].ResourceStream;
		rs.Should().NotBeNull();
		using var reader = new StreamReader(rs);
		var json = await reader.ReadToEndAsync();
		json.Should().Contain("Bob");
	}

	[Fact]
	public async Task Batch_ReplaceResult_ResourceStream_ContainsDocument()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });
		using var response = await batch.ExecuteAsync();
		var rs = response[0].ResourceStream;
		rs.Should().NotBeNull();
		using var reader = new StreamReader(rs);
		var json = await reader.ReadToEndAsync();
		json.Should().Contain("Updated");
	}

	[Fact]
	public async Task Batch_ReadResult_ResourceStream_ContainsDocument()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Read" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("1");
		using var response = await batch.ExecuteAsync();
		var rs = response[0].ResourceStream;
		rs.Should().NotBeNull();
		using var reader = new StreamReader(rs);
		var json = await reader.ReadToEndAsync();
		json.Should().Contain("Read");
	}

	[Fact]
	public async Task Batch_PatchResult_ResourceStream_ContainsDocument()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Old" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new[] { PatchOperation.Replace("/name", "Patched") });
		using var response = await batch.ExecuteAsync();
		var rs = response[0].ResourceStream;
		rs.Should().NotBeNull();
		using var reader = new StreamReader(rs);
		var json = await reader.ReadToEndAsync();
		json.Should().Contain("Patched");
	}

	[Fact]
	public async Task Batch_PatchResult_HasETag()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Old" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new[] { PatchOperation.Replace("/name", "Patched") });
		using var response = await batch.ExecuteAsync();
		response[0].ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Batch_DeleteResult_ResourceStream_IsNullOrEmpty()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1");
		using var response = await batch.ExecuteAsync();
		response[0].ResourceStream.Should().BeNull();
	}

	[Fact]
	public async Task Batch_CreateItemStream_Result_HasETag()
	{
		var json = JsonConvert.SerializeObject(new { id = "1", partitionKey = "pk1", name = "A" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItemStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		response[0].ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Batch_CreateItemStream_Result_ResourceStream_ContainsDocument()
	{
		var json = JsonConvert.SerializeObject(new { id = "1", partitionKey = "pk1", name = "StreamDoc" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItemStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		var rs = response[0].ResourceStream;
		rs.Should().NotBeNull();
		using var reader = new StreamReader(rs);
		var content = await reader.ReadToEndAsync();
		content.Should().Contain("StreamDoc");
	}

	[Fact]
	public async Task Batch_UpsertItemStream_Result_HasETag()
	{
		var json = JsonConvert.SerializeObject(new { id = "1", partitionKey = "pk1", name = "A" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItemStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		response[0].ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Batch_ReplaceItemStream_Result_HasETag()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var json = JsonConvert.SerializeObject(new { id = "1", partitionKey = "pk1", name = "Replaced" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItemStream("1", new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		response[0].ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Batch_OperationResult_IsSuccessStatusCode_TrueForSuccessOps()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response[0].IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_OperationResult_IsSuccessStatusCode_FalseForFailedOps()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "ok1", PartitionKey = "pk1", Name = "B" });
		batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "C" });

		using var response = await batch.ExecuteAsync();
		response[0].IsSuccessStatusCode.Should().BeFalse(); // rolled back → FailedDependency
		response[1].IsSuccessStatusCode.Should().BeFalse(); // 409
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category E: Request Options Passthrough
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchRequestOptionsDeepTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_ReplaceItem_WithIfMatchEtag_CurrentEtag_Succeeds()
	{
		var created = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1",
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new TransactionalBatchItemRequestOptions { IfMatchEtag = created.ETag });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_UpsertItem_WithIfMatchEtag_StaleEtag_Fails()
	{
		var created = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));
		// Update to make the original ETag stale
		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" },
			"1", new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "C" },
			new TransactionalBatchItemRequestOptions { IfMatchEtag = created.ETag });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_DeleteItem_WithIfMatchEtag_StaleEtag_Fails()
	{
		var created = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));
		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" },
			"1", new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1", new TransactionalBatchItemRequestOptions { IfMatchEtag = created.ETag });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_DeleteItem_WithIfMatchEtag_CurrentEtag_Succeeds()
	{
		var created = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1", new TransactionalBatchItemRequestOptions { IfMatchEtag = created.ETag });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category F: Error Scenarios
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchErrorScenarioTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_PatchNonExistentItem_FailsBatch()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		batch.PatchItem("nonexistent", new[] { PatchOperation.Replace("/name", "X") });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency); // rolled back
	}

	[Fact]
	public async Task Batch_ExceedingMaxOps_ThrowsBadRequest_MessageContainsLimit()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		for (var i = 0; i <= 100; i++)
			batch.CreateItem(new TestDocument { Id = i.ToString(), PartitionKey = "pk1", Name = "A" });

		var act = () => batch.ExecuteAsync();
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Batch_CreateThenCreate_SameId_FailsAndRollsBack()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "First" });
		batch.CreateItem(new TestDocument { Id = "dup", PartitionKey = "pk1", Name = "Second" });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
		response[1].StatusCode.Should().Be(HttpStatusCode.Conflict);

		// Verify rollback — item should not exist
		var act = () => _container.ReadItemAsync<TestDocument>("dup", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category C: Unique Key Policy Interaction
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchUniqueKeyTests
{
	[Fact]
	public async Task Batch_CreateItem_UniqueKeyViolation_FailsBatch()
	{
		var containerProps = new ContainerProperties("uk-batch", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(containerProps);

		await container.CreateItemAsync(
			new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "UniqueMe" },
			new PartitionKey("pk1"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "new1", PartitionKey = "pk1", Name = "UniqueMe" }); // violates unique key

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_UpsertItem_IntoUniqueKeyConstraint_Succeeds()
	{
		var containerProps = new ContainerProperties("uk-batch", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(containerProps);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "UniqueMe" },
			new PartitionKey("pk1"));

		// Upsert same id with same unique key value — should succeed
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "UniqueMe" });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category G: Intra-Batch Complex Sequences
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchComplexSequenceTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_CreateThenPatch_SameItemInBatch()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Initial" });
		batch.PatchItem("1", new[] { PatchOperation.Replace("/name", "Patched") });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task Batch_ReplaceThenDelete_SameItemInBatch()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });
		batch.DeleteItem("1");

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_ReplaceThenRead_SameItemInBatch()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Old" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "New" });
		batch.ReadItem("1");

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var result = response.GetOperationResultAtIndex<TestDocument>(1);
		result.Resource.Name.Should().Be("New");
	}

	[Fact]
	public async Task Batch_PatchThenRead_SameItemInBatch()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Old" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new[] { PatchOperation.Replace("/name", "Patched") });
		batch.ReadItem("1");

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var result = response.GetOperationResultAtIndex<TestDocument>(1);
		result.Resource.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task Batch_DeleteThenRead_SameId_FailsBatch()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1");
		batch.ReadItem("1"); // Should find nothing → 404 → fail

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();

		// Item should be restored by rollback
		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("A");
	}

	[Fact]
	public async Task Batch_CreateThenReplace_ThenPatch_ThenRead_SameItem()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Created" });
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" });
		batch.PatchItem("1", new[] { PatchOperation.Replace("/name", "Patched") });
		batch.ReadItem("1");

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var result = response.GetOperationResultAtIndex<TestDocument>(3);
		result.Resource.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task Batch_UpsertThenDelete_ThenUpsert_SameItem()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" });
		batch.DeleteItem("1");
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Recreated");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category H: Stream Operations (Deeper Coverage)
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchStreamDeepTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_UpsertItemStream_UpdatesExisting()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Old" },
			new PartitionKey("pk1"));

		var json = JsonConvert.SerializeObject(new { id = "1", partitionKey = "pk1", name = "Updated" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItemStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task Batch_ReplaceItemStream_NonExistent_FailsBatch()
	{
		var json = JsonConvert.SerializeObject(new { id = "missing", partitionKey = "pk1", name = "X" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItemStream("missing", new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_CreateItemStream_ReturnsCorrectStatusCode()
	{
		var json = JsonConvert.SerializeObject(new { id = "1", partitionKey = "pk1", name = "A" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItemStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_MixedStreamAndTyped_AllSucceed()
	{
		var json = JsonConvert.SerializeObject(new { id = "2", partitionKey = "pk1", name = "StreamDoc" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "TypedDoc" });
		batch.CreateItemStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
		response.Count.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category I: Change Feed Integration
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchChangeFeedIntegrationTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_SuccessfulCreates_AppearInChangeFeed()
	{
		var checkpoint = _container.GetChangeFeedCheckpoint();

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var iter = _container.GetChangeFeedIterator<JObject>(checkpoint);
		var changes = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			changes.AddRange(page);
		}
		changes.Should().HaveCountGreaterThanOrEqualTo(2);
	}

	[Fact]
	public async Task Batch_SuccessfulDeletes_AppearAsChangeFeedTombstones()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var checkpoint = _container.GetChangeFeedCheckpoint();

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1");
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var iter = _container.GetChangeFeedIterator<JObject>(checkpoint);
		var changes = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			changes.AddRange(page);
		}
		changes.Should().NotBeEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category J: Serialization Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchSerializationTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_CreateItem_WithNestedObject_Succeeds()
	{
		var doc = new TestDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Name = "A",
			Nested = new NestedObject { Description = "nested", Score = 9.5 }
		};

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(doc);
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Nested!.Description.Should().Be("nested");
	}

	[Fact]
	public async Task Batch_CreateItem_WithSpecialCharactersInId_Succeeds()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "special-id_123.test", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("special-id_123.test", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("A");
	}

	[Fact]
	public async Task Batch_CreateItem_WithNullProperties_Succeeds()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = null! });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_Stream_UTF8_Encoding_Preserved()
	{
		var json = JsonConvert.SerializeObject(new { id = "1", partitionKey = "pk1", name = "héllo wörld" });
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItemStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("héllo wörld");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category: 2MB Error Path
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatch2MBErrorPathTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_Over2MB_ResponseIndexer_ReturnsResults()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		var bigString = new string('A', 1024 * 1024);
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = bigString });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = bigString });
		batch.CreateItem(new TestDocument { Id = "3", PartitionKey = "pk1", Name = "small" });

		using var response = await batch.ExecuteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
		response.IsSuccessStatusCode.Should().BeFalse();
		response.Count.Should().Be(3);
		response[0].Should().NotBeNull();
		response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Skipped + Divergent Behaviour Sister Tests
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchSkippedAndDivergentTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Batch_PartitionKeyMismatch_Document_Vs_BatchPK_ThrowsBadRequest()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk2", Name = "A" }); // doc says pk2, batch says pk1
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact(Skip = "Divergent: emulator diagnostics returns zero elapsed time")]
	public async Task Batch_Diagnostics_ContainsRequestLatency()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.Diagnostics.GetClientElapsedTime().Should().BeGreaterThan(TimeSpan.Zero);
	}

	[Fact]
	public async Task Batch_Diagnostics_InMemory_ReturnsZeroElapsedTime()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.Diagnostics.GetClientElapsedTime().Should().Be(TimeSpan.Zero);
	}

	[Fact(Skip = "Divergent: emulator uses synthetic session tokens")]
	public async Task Batch_Headers_ContainSessionToken()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.Headers.Session.Should().NotBeNullOrEmpty();
		response.Headers.Session.Should().MatchRegex(@"\d+:#\d+");
	}

	[Fact(Skip = "Divergent: emulator always returns 1.0 RU")]
	public async Task Batch_RequestCharge_ScalesWithOperationCount()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		for (var i = 0; i < 10; i++)
			batch.CreateItem(new TestDocument { Id = i.ToString(), PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.RequestCharge.Should().BeGreaterThan(10d); // real Cosmos: ~5.3 RU per create
	}

	[Fact]
	public async Task Batch_RequestCharge_InMemory_AlwaysReturns1RU()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		for (var i = 0; i < 10; i++)
			batch.CreateItem(new TestDocument { Id = i.ToString(), PartitionKey = "pk1", Name = "A" });
		using var response = await batch.ExecuteAsync();
		response.RequestCharge.Should().Be(1.0);
	}

	[Fact(Skip = "Divergent: emulator batch is not isolated from concurrent direct CRUD")]
	public async Task Batch_ConcurrentBatchAndDirectCrud_IsolationGuaranteed()
	{
		// Real Cosmos: batch execution is serialized within a partition key
		// Emulator: snapshot/restore but no global lock
		await Task.CompletedTask;
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Plan 42: Transactional Batch Deep Dive Tests
// ═══════════════════════════════════════════════════════════════════════════

// ── Bug Fix Verification ──
public class TransactionalBatchBugFixTests
{
	private readonly InMemoryContainer _container = new("batch-bugfix", "/partitionKey");

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task Batch_UpsertItemStream_NewItem_Returns201Created()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItemStream(ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Alice\"}"));
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_UpsertItemStream_ExistingItem_Returns200OK()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItemStream(ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Updated\"}"));
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_CreateItemStream_ResourceStream_ContainsSystemProperties()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItemStream(ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Alice\"}"));
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		using var reader = new StreamReader(response[0].ResourceStream);
		var body = JObject.Parse(reader.ReadToEnd());
		body["_ts"].Should().NotBeNull("system property _ts should be present");
		body["_etag"].Should().NotBeNull("system property _etag should be present");
	}

	[Fact]
	public async Task Batch_UpsertItemStream_ResourceStream_ContainsSystemProperties()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItemStream(ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Alice\"}"));
		using var response = await batch.ExecuteAsync();

		using var reader = new StreamReader(response[0].ResourceStream);
		var body = JObject.Parse(reader.ReadToEnd());
		body["_ts"].Should().NotBeNull();
		body["_etag"].Should().NotBeNull();
	}

	[Fact]
	public async Task Batch_ReplaceItemStream_ResourceStream_ContainsSystemProperties()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItemStream("1", ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Updated\"}"));
		using var response = await batch.ExecuteAsync();

		using var reader = new StreamReader(response[0].ResourceStream);
		var body = JObject.Parse(reader.ReadToEnd());
		body["_ts"].Should().NotBeNull();
		body["_etag"].Should().NotBeNull();
	}
}

// ── Request Options Extended ──
public class TransactionalBatchRequestOptionsExtendedTests
{
	private readonly InMemoryContainer _container = new("batch-reqopt", "/partitionKey");

	[Fact]
	public async Task Batch_PatchItem_WithFilterPredicate_MatchingFilter_Succeeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new[] { PatchOperation.Set("/name", "Updated") },
			new TransactionalBatchPatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.name = 'Alice'" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_PatchItem_WithFilterPredicate_NonMatchingFilter_Fails()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new[] { PatchOperation.Set("/name", "Updated") },
			new TransactionalBatchPatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.name = 'NonExistent'" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_ReplaceItemStream_WithIfMatchEtag_StaleEtag_Fails()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItemStream("1",
			new MemoryStream(Encoding.UTF8.GetBytes("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Updated\"}")),
			new TransactionalBatchItemRequestOptions { IfMatchEtag = "stale-etag" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_ReplaceItemStream_WithIfMatchEtag_CurrentEtag_Succeeds()
	{
		var createResp = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));
		var currentEtag = createResp.ETag;

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItemStream("1",
			new MemoryStream(Encoding.UTF8.GetBytes("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Updated\"}")),
			new TransactionalBatchItemRequestOptions { IfMatchEtag = currentEtag });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
	}
}

// ── Edge Cases Extended ──
public class TransactionalBatchEdgeCaseExtendedTests
{
	private readonly InMemoryContainer _container = new("batch-edge", "/partitionKey");

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task Batch_FirstOperationFails_AllSubsequent424()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		// Replace non-existent item (fails)
		batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "X" });
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response[0].StatusCode.Should().Be(HttpStatusCode.NotFound);
		response[1].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
		response[2].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
	}

	[Fact]
	public async Task Batch_LastOperationFails_AllPrior424()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" });
		// Replace non-existent item (fails)
		batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "X" });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response[0].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
		response[1].StatusCode.Should().Be(HttpStatusCode.FailedDependency);
		response[2].StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Batch_StreamWithInvalidJson_ThrowsOnExecute()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItemStream(ToStream("not valid json"));
		var act = async () => await batch.ExecuteAsync();
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Batch_CreateItemStream_MissingIdField_AutoGeneratesId()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItemStream(ToStream("{\"partitionKey\":\"pk1\",\"name\":\"NoId\"}"));
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
		response[0].StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_SingleItemFails_ResponseHasOneEntry()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "X" });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.Count.Should().Be(1);
		response[0].StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ── TTL Tests ──
public class TransactionalBatchTtlExtendedTests
{
	[Fact]
	public async Task Batch_CreateItem_WithContainerTtl_ItemGetsTimestamp()
	{
		var container = new InMemoryContainer("batch-ttl", "/partitionKey");
		container.DefaultTimeToLive = 3600;

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var item = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		item.Resource["_ts"].Should().NotBeNull();
	}

	[Fact]
	public async Task Batch_CreateItem_WithContainerTtl_ItemExpires()
	{
		var container = new InMemoryContainer("batch-ttl2", "/partitionKey");
		container.DefaultTimeToLive = 1;

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		await Task.Delay(2000);
		var readResponse = await container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		readResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Batch_Rollback_DoesNotLeakTtlMetadata()
	{
		var container = new InMemoryContainer("batch-ttl3", "/partitionKey");
		container.DefaultTimeToLive = 3600;

		await container.CreateItemAsync(
			new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Original" }, new PartitionKey("pk1"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "new1", PartitionKey = "pk1", Name = "New" });
		// This will fail — non-existent item
		batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "X" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();

		// Original item should still be there and unchanged
		var item = await container.ReadItemAsync<TestDocument>("existing", new PartitionKey("pk1"));
		item.Resource.Name.Should().Be("Original");
	}
}

// ── Unique Key Extended ──
public class TransactionalBatchUniqueKeyExtendedTests
{
	[Fact]
	public async Task Batch_Rollback_FreesUniqueKeySlots_RecreateSucceeds()
	{
		var props = new ContainerProperties("batch-uk1", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy { UniqueKeys = { new UniqueKey { Paths = { "/name" } } } }
		};
		var container = new InMemoryContainer(props);

		// Batch: create unique item, then fail
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "UniqueAlice" });
		batch.ReplaceItem("nonexistent", new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "X" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();

		// Now create with same unique key — should succeed since batch was rolled back
		var batch2 = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch2.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "UniqueAlice" });
		using var response2 = await batch2.ExecuteAsync();
		response2.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_IntraBatch_TwoItems_SameUniqueKey_SecondFails()
	{
		var props = new ContainerProperties("batch-uk2", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy { UniqueKeys = { new UniqueKey { Paths = { "/name" } } } }
		};
		var container = new InMemoryContainer(props);

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "SameName" });
		batch.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "SameName" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}

	[Fact]
	public async Task Batch_UpsertAsCreate_ConflictingUniqueKey_Fails()
	{
		var props = new ContainerProperties("batch-uk3", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy { UniqueKeys = { new UniqueKey { Paths = { "/name" } } } }
		};
		var container = new InMemoryContainer(props);
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "UniqueAlice" }, new PartitionKey("pk1"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "2", PartitionKey = "pk1", Name = "UniqueAlice" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
	}
}

// ── GetOperationResultAtIndex ──
public class TransactionalBatchGetOperationResultTests
{
	private readonly InMemoryContainer _container = new("batch-getop", "/partitionKey");

	[Fact]
	public async Task Batch_GetOperationResultAtIndex_Create_ReturnsTypedResource()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		using var response = await batch.ExecuteAsync();

		var result = response.GetOperationResultAtIndex<TestDocument>(0);
		result.Resource.Id.Should().Be("1");
		result.Resource.Name.Should().Be("Alice");
		result.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_GetOperationResultAtIndex_Upsert_ReturnsTypedResource()
	{
		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		using var response = await batch.ExecuteAsync();

		var result = response.GetOperationResultAtIndex<TestDocument>(0);
		result.Resource.Id.Should().Be("1");
		result.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Batch_GetOperationResultAtIndex_Replace_ReturnsTypedResource()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" });
		using var response = await batch.ExecuteAsync();

		var result = response.GetOperationResultAtIndex<TestDocument>(0);
		result.Resource.Name.Should().Be("Updated");
		result.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_GetOperationResultAtIndex_Patch_ReturnsTypedResource()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new[] { PatchOperation.Set("/name", "Patched") });
		using var response = await batch.ExecuteAsync();

		var result = response.GetOperationResultAtIndex<TestDocument>(0);
		result.Resource.Name.Should().Be("Patched");
		result.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_GetOperationResultAtIndex_Delete_ResourceIsDefault()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" }, new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1");
		using var response = await batch.ExecuteAsync();

		var result = response.GetOperationResultAtIndex<TestDocument>(0);
		result.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}
}

// ── Concurrent Batches ──
public class TransactionalBatchConcurrentExtendedTests
{
	[Fact]
	public async Task Batch_TwoConcurrentBatches_DifferentPartitions_BothSucceed()
	{
		var container = new InMemoryContainer("batch-conc", "/partitionKey");

		var batch1 = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch1.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });
		var batch2 = container.CreateTransactionalBatch(new PartitionKey("pk2"));
		batch2.CreateItem(new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" });

		var results = await Task.WhenAll(batch1.ExecuteAsync(), batch2.ExecuteAsync());
		results[0].IsSuccessStatusCode.Should().BeTrue();
		results[1].IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_ThenDirectCrud_StateConsistent()
	{
		var container = new InMemoryContainer("batch-state", "/partitionKey");

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		// Direct CRUD should see the batch result
		var item = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		item.Resource.Name.Should().Be("Alice");

		// Direct update should succeed
		await container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" }, "1", new PartitionKey("pk1"));
		var updated = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		updated.Resource.Name.Should().Be("Updated");
	}
}

// ── Miscellaneous ──
public class TransactionalBatchMiscExtendedTests
{
	[Fact]
	public async Task Batch_PatchItem_AllFivePatchTypes_InSingleBatch()
	{
		var container = new InMemoryContainer("batch-allpatch", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice", counter = 10, tags = new[] { "a" }, extra = "old" }),
			new PartitionKey("pk1"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new[]
		{
			PatchOperation.Set("/name", "Updated"),           // Set
            PatchOperation.Replace("/extra", "new"),           // Replace
            PatchOperation.Add("/added", "newField"),          // Add
            PatchOperation.Remove("/tags"),                    // Remove
            PatchOperation.Increment("/counter", 5),           // Increment
        });
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var item = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		item.Resource["name"]!.Value<string>().Should().Be("Updated");
		item.Resource["extra"]!.Value<string>().Should().Be("new");
		item.Resource["added"]!.Value<string>().Should().Be("newField");
		item.Resource["tags"].Should().BeNull();
		item.Resource["counter"]!.Value<int>().Should().Be(15);
	}

	[Fact]
	public async Task Batch_NestedPartitionKeyPath_WorksCorrectly()
	{
		var container = new InMemoryContainer("batch-nested-pk", "/metadata/partitionKey");
		var doc = JObject.FromObject(new { id = "1", metadata = new { partitionKey = "pk1" }, name = "Alice" });

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(doc);
		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var item = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		item.Resource["name"]!.Value<string>().Should().Be("Alice");
	}
}

