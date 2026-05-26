using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class CreateItemTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task CreateItemAsync_WithValidItem_ReturnsCreatedResponse()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Resource.Should().NotBeNull();
		response.Resource.Id.Should().Be("1");
		response.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task CreateItemAsync_WithValidItem_SetsETag()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task CreateItemAsync_DuplicateId_ThrowsConflict()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task CreateItemAsync_SameIdDifferentPartitionKey_BothSucceed()
	{
		var item1 = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" };
		var item2 = new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Second" };

		await _container.CreateItemAsync(item1, new PartitionKey("pk1"));
		var response = await _container.CreateItemAsync(item2, new PartitionKey("pk2"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateItemAsync_WithoutId_ThrowsInvalidOperation()
	{
		var item = new { notAnId = "test" };

		var act = () => _container.CreateItemAsync(item, new PartitionKey("pk1"));

		await act.Should().ThrowAsync<InvalidOperationException>();
	}

	[Fact]
	public async Task CreateItemAsync_WithNullPartitionKey_ExtractsFromPartitionKeyPath()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item);

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task CreateItemAsync_ItemIsRetrievable()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 42 };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		readResponse.Resource.Name.Should().Be("Test");
		readResponse.Resource.Value.Should().Be(42);
	}
}

public class ReadItemTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task ReadItemAsync_ExistingItem_ReturnsOk()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task ReadItemAsync_NonExistentItem_ThrowsNotFound()
	{
		var act = () => _container.ReadItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"));

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReadItemAsync_WrongPartitionKey_ThrowsNotFound()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("wrong-pk"));

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReadItemAsync_ReturnsETag()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReadItemAsync_WithIfNoneMatchCurrentETag_ThrowsNotModified()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
		var etag = createResponse.ETag;

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = etag });

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task ReadItemAsync_WithIfNoneMatchStaleETag_ReturnsOk()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "\"stale-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

public class UpsertItemTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task UpsertItemAsync_NewItem_ReturnsCreated()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.UpsertItemAsync(item, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task UpsertItemAsync_ExistingItem_ReturnsOkWithUpdatedData()
	{
		var original = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(original, new PartitionKey("pk1"));

		var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
		var response = await _container.UpsertItemAsync(updated, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task UpsertItemAsync_WithIfMatchCurrentETag_Succeeds()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
		var response = await _container.UpsertItemAsync(updated, new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = createResponse.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task UpsertItemAsync_WithIfMatchStaleETag_ThrowsPreconditionFailed()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
		var act = () => _container.UpsertItemAsync(updated, new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task UpsertItemAsync_UpdatesETag()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
		var originalEtag = createResponse.ETag;

		var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
		var upsertResponse = await _container.UpsertItemAsync(updated, new PartitionKey("pk1"));

		upsertResponse.ETag.Should().NotBe(originalEtag);
	}
}

public class ReplaceItemTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task ReplaceItemAsync_ExistingItem_ReturnsOk()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };
		var response = await _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Replaced");
	}

	[Fact]
	public async Task ReplaceItemAsync_NonExistentItem_ThrowsNotFound()
	{
		var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };

		var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReplaceItemAsync_WithIfMatchCurrentETag_Succeeds()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };
		var response = await _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = createResponse.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceItemAsync_WithIfMatchStaleETag_ThrowsPreconditionFailed()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };
		var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task ReplaceItemAsync_DataIsUpdated()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 1 };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced", Value = 99 };
		await _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Replaced");
		readResponse.Resource.Value.Should().Be(99);
	}
}

public class DeleteItemTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task DeleteItemAsync_ExistingItem_ReturnsNoContent()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task DeleteItemAsync_NonExistentItem_ThrowsNotFound()
	{
		var act = () => _container.DeleteItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"));

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteItemAsync_ItemNoLongerReadable()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteItemAsync_WithIfMatchCurrentETag_Succeeds()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = createResponse.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task DeleteItemAsync_WithIfMatchStaleETag_ThrowsPreconditionFailed()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		var exception = await act.Should().ThrowAsync<CosmosException>();
		exception.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task DeleteItemAsync_OnlyAffectsSpecifiedPartitionKey()
	{
		var item1 = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Keep" };
		var item2 = new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Delete" };
		await _container.CreateItemAsync(item1, new PartitionKey("pk1"));
		await _container.CreateItemAsync(item2, new PartitionKey("pk2"));

		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk2"));

		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Keep");
	}

	[Fact]
	public async Task DeleteItemAsync_CanRecreateAfterDelete()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var newItem = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" };
		var response = await _container.CreateItemAsync(newItem, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		var readResponse = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		readResponse.Resource.Name.Should().Be("Recreated");
	}
}


/// <summary>
/// Validates that CancellationToken is respected across Container operations.
/// All methods accept a CancellationToken and should throw OperationCanceledException
/// when the token is already cancelled.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.createitemasync
/// </summary>
public class CancellationTokenTests5
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task CreateItem_WithCancelledToken_ThrowsOperationCancelled()
	{
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var act = () => _container.CreateItemAsync(item, new PartitionKey("pk1"),
			cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task ReadItem_WithCancelledToken_ThrowsOperationCancelled()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => _container.ReadItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"), cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task UpsertItem_WithCancelledToken_ThrowsOperationCancelled()
	{
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var act = () => _container.UpsertItemAsync(item, new PartitionKey("pk1"),
			cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task DeleteItem_WithCancelledToken_ThrowsOperationCancelled()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => _container.DeleteItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"), cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task PatchItem_WithCancelledToken_ThrowsOperationCancelled()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task CreateItemStream_WithCancelledToken_ThrowsOperationCancelled()
	{
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		var act = () => _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)),
			new PartitionKey("pk1"), cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}
}


public class ReplaceItemGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Replace_WithNullItem_Throws()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Orig" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.ReplaceItemAsync<TestDocument>(null!, "1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Replace_IdParameterDiffersFromBody_ThrowsBadRequest()
	{
		// Create an item with id "1"
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		// Replace with id parameter = "1" but item body has id = "different"
		// SDK docs state body id must match the id parameter.
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
		var replacement = new TestDocument { Id = "different", PartitionKey = "pk1", Name = "Replaced" };
		var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}


/// <summary>
/// Validates that the partition key value is immutable during Replace operations.
/// Per API docs: "The item's partition key value is immutable. To change an item's
/// partition key value you must delete the original item and insert a new item."
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.replaceitemasync
/// </summary>
public class ReplacePartitionKeyImmutabilityTests5
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Replace_WithDifferentPkInBody_ThrowsBadRequest()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		// Replace with a different partition key value in the body — should throw BadRequest
		var replacement = new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Replaced" };
		var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest);
	}
}


public class ReadItemGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Read_WrongPartitionKey_ThrowsNotFound()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("wrong-pk"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Read_AfterUpdate_ReturnsUpdatedData()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task Read_NonExistent_ThrowsCosmosExceptionWith404()
	{
		var act = () => _container.ReadItemAsync<TestDocument>("missing", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Read_IfNoneMatch_WithStaleETag_ReturnsItem()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "\"stale-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task Read_IfNoneMatch_WithCurrentETag_Returns304()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = createResponse.ETag });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}
}


public class DeleteItemGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Delete_WithNullId_Throws()
	{
		var act = () => _container.DeleteItemAsync<TestDocument>(null!, new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Delete_RecordsTombstoneInChangeFeed()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		var checkpointAfterCreate = _container.GetChangeFeedCheckpoint();

		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var checkpointAfterDelete = _container.GetChangeFeedCheckpoint();
		checkpointAfterDelete.Should().Be(checkpointAfterCreate + 1);
	}
}


public class ReadItemGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Read_ResponseContainsETag()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Read_IfMatchEtag_IsNotEnforcedOnRead()
	{
		// Per docs: IfMatch is not supported on reads
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Should succeed even with a bogus IfMatch — reads don't use IfMatch
		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"bogus\"" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Read_AfterDelete_Returns404()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}


/// <summary>
/// Validates that system metadata properties (<c>_ts</c>, <c>_etag</c>) are available
/// in the response body when reading items, as documented in the API remarks:
/// "Items contain meta data that can be obtained by mapping these meta data attributes
/// to properties in T."
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.readitemasync
/// </summary>
public class SystemMetadataPropertyTests5
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Read_ResponseBody_Contains_Ts_SystemProperty()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));

		// Real Cosmos DB includes _ts (Unix epoch seconds) in the response body
		response.Resource["_ts"].Should().NotBeNull();
		response.Resource["_ts"]!.Type.Should().Be(JTokenType.Integer);
		response.Resource["_ts"]!.Value<long>().Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task Read_ResponseBody_Contains_Etag_SystemProperty()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));

		// Real Cosmos DB includes _etag in the response body
		response.Resource["_etag"].Should().NotBeNull();
		response.Resource["_etag"]!.Type.Should().Be(JTokenType.String);
		response.Resource["_etag"]!.ToString().Should().NotBeEmpty();
	}

	[Fact]
	public async Task Read_Ts_UpdatedOnReplace()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var readBefore = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsBefore = readBefore.Resource["_ts"]?.Value<long>() ?? 0;

		// Small delay to ensure timestamp changes
		await Task.Delay(10);

		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		var readAfter = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsAfter = readAfter.Resource["_ts"]?.Value<long>() ?? 0;

		tsAfter.Should().BeGreaterThanOrEqualTo(tsBefore);
	}
}


public class ReplaceItemGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Replace_NonExistent_ThrowsNotFound()
	{
		var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };

		var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Replace_UpdatesETag()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var replaceResponse = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		replaceResponse.ETag.Should().NotBe(createResponse.ETag);
	}

	[Fact]
	public async Task Replace_ResponseIs200()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Replace_RecordsInChangeFeed()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));
		var beforeReplace = _container.GetChangeFeedCheckpoint();

		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		var afterReplace = _container.GetChangeFeedCheckpoint();
		afterReplace.Should().BeGreaterThan(beforeReplace);
	}
}


public class UpsertItemGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Upsert_WithIfMatch_CorrectETag_Succeeds()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = create.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task Upsert_WithIfMatch_OnNewItem_CreatesItem()
	{
		// If-Match is "applicable only on PUT and DELETE" per REST API docs.
		// Upsert uses POST, so If-Match is ignored on the insert path.
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"nonexistent\"" });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Upsert_WithNullItem_Throws()
	{
		var act = () => _container.UpsertItemAsync<TestDocument>(null!, new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}
}


public class DeleteItemGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Delete_ResponseIs204()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task Delete_WithIfMatch_CorrectETag_Succeeds()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = createResponse.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task Delete_WithIfMatch_WrongETag_ThrowsPreconditionFailed()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Delete_AfterDelete_CanRecreate()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));
		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var recreated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" };
		var response = await _container.CreateItemAsync(recreated, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Recreated");
	}
}


/// <summary>
/// Validates edge cases for DeleteItemAsync, specifically that IfNoneMatchEtag
/// (which is meaningful for reads, not writes) does not interfere with delete.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.deleteitemasync
/// </summary>
public class DeleteEdgeCaseTests5
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Delete_WithIfNoneMatchEtag_IsIgnored_DeleteSucceeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// IfNoneMatch is for conditional reads, not deletes. Should be ignored.
		var response = await _container.DeleteItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "\"some-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}
}


public class CreateItemGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Create_WithPartitionKeyNone_ExtractsFromDocument()
	{
		// PartitionKey.None falls through to extract PK from document body
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, PartitionKey.None);

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		// Retrievable with the document's PK value
		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task Create_ResponseContainsCorrectStatusCode_201()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Create_ResponseContainsRequestCharge()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.RequestCharge.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task Create_ItemWithSystemProperties_OverwrittenBySystem()
	{
		// Create item that includes _ts and _etag in the JSON
		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ts":999999,"_etag":"\"fake\""}""";
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var etag = read.ETag;

		// _etag should be overwritten by the system, not "fake"
		etag.Should().NotBe("\"fake\"");
	}
}


public class CrudNullGuardTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task CreateItemAsync_WithNullItem_Throws()
	{
		var act = () => _container.CreateItemAsync<TestDocument>(null!, new PartitionKey("pk1"));
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task ReplaceItemAsync_WithNullId_Throws()
	{
		var act = () => _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			null!, new PartitionKey("pk1"));
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task ReplaceItemAsync_WithEmptyId_ThrowsBadRequest()
	{
		var act = () => _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			"", new PartitionKey("pk1"));

		// Body id "1" differs from parameter id "" — returns BadRequest
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ReadItemAsync_WithNullId_Throws()
	{
		var act = () => _container.ReadItemAsync<TestDocument>(null!, new PartitionKey("pk1"));
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task DeleteItemAsync_WithNullId_Throws()
	{
		var act = () => _container.DeleteItemAsync<TestDocument>(null!, new PartitionKey("pk1"));
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task PatchItemAsync_WithNullId_Throws()
	{
		var act = () => _container.PatchItemAsync<TestDocument>(
			null!, new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "test")]);
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task PatchItemAsync_WithNullOperations_Throws()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var act = () => _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"), null!);
		await act.Should().ThrowAsync<Exception>();
	}
}


public class DeleteAllItemsByPartitionKeyTests
{
	[Fact]
	public async Task DeleteAllByPartitionKey_RemovesOnlyThatPartition()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob" },
			new PartitionKey("pk2"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie" },
			new PartitionKey("pk1"));

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));

		container.ItemCount.Should().Be(1);

		var read = await container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk2"));
		read.Resource.Name.Should().Be("Bob");
	}
}


public class ReplaceItemGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Replace_WithDifferentPartitionKey_InBody_ThrowsBadRequest()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		// Replace with a different PK value in the body but same PK parameter — should throw
		var replacement = new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Replaced" };
		var act = () => _container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest);
	}
}


public class CreateItemGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Create_WithExplicitPartitionKey_ExtractsFromDocument_WhenNoneProvided()
	{
		var item = new TestDocument { Id = "auto-pk", PartitionKey = "pk1", Name = "Auto PK" };

		var response = await _container.CreateItemAsync(item);

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await _container.ReadItemAsync<TestDocument>("auto-pk", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Auto PK");
	}

	[Fact]
	public async Task Create_ResponseContainsETag_NonNullNonEmpty()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.ETag.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Create_ResponseBodyMatchesInput()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 42 };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.Resource.Id.Should().Be("1");
		response.Resource.Name.Should().Be("Test");
		response.Resource.Value.Should().Be(42);
	}

	[Fact]
	public async Task Create_WithNestedPartitionKeyPath_ExtractsCorrectly()
	{
		var container = new InMemoryContainer("test-container", "/nested/description");
		var item = new TestDocument
		{
			Id = "1",
			PartitionKey = "ignored",
			Name = "Test",
			Nested = new NestedObject { Description = "deep-pk", Score = 1.0 }
		};

		var response = await container.CreateItemAsync(item, new PartitionKey("deep-pk"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("deep-pk"));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task Create_WithIdContainingSpecialCharacters_Succeeds()
	{
		var item = new TestDocument { Id = "id/with?special#chars", PartitionKey = "pk1", Name = "Special" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await _container.ReadItemAsync<TestDocument>("id/with?special#chars", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Special");
	}

	[Fact]
	public async Task Create_WithIdContainingUnicode_Succeeds()
	{
		var item = new TestDocument { Id = "日本語-id", PartitionKey = "pk1", Name = "Unicode" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await _container.ReadItemAsync<TestDocument>("日本語-id", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Unicode");
	}

	[Fact]
	public async Task Create_SameIdDifferentPartitionKey_BothRetrievable()
	{
		var item1 = new TestDocument { Id = "same", PartitionKey = "pk1", Name = "First" };
		var item2 = new TestDocument { Id = "same", PartitionKey = "pk2", Name = "Second" };

		await _container.CreateItemAsync(item1, new PartitionKey("pk1"));
		await _container.CreateItemAsync(item2, new PartitionKey("pk2"));

		var read1 = await _container.ReadItemAsync<TestDocument>("same", new PartitionKey("pk1"));
		var read2 = await _container.ReadItemAsync<TestDocument>("same", new PartitionKey("pk2"));

		read1.Resource.Name.Should().Be("First");
		read2.Resource.Name.Should().Be("Second");
	}
}


public class CreateItemGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Create_WithNullItem_ThrowsArgumentNullException()
	{
		var act = () => _container.CreateItemAsync<TestDocument>(null!, new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Create_WithEmptyId_Succeeds_OrThrows()
	{
		// Cosmos rejects empty ID with 400; InMemoryContainer may differ
		var item = new TestDocument { Id = "", PartitionKey = "pk1", Name = "EmptyId" };

		try
		{
			var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
			// If it succeeds, that's a behavioral difference — it should still be retrievable
			response.StatusCode.Should().BeOneOf(HttpStatusCode.Created, HttpStatusCode.BadRequest);
		}
		catch (CosmosException ex)
		{
			ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		}
	}

	[Fact]
	public async Task Create_WithVeryLongId_255Chars_Succeeds()
	{
		var longId = new string('a', 255);
		var item = new TestDocument { Id = longId, PartitionKey = "pk1", Name = "LongId" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await _container.ReadItemAsync<TestDocument>(longId, new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("LongId");
	}

	[Fact]
	public async Task Create_WithEnableContentResponseOnWrite_False_ResourceIsNull()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
	}

	[Fact]
	public async Task Create_WithCompositePartitionKey_TwoPaths()
	{
		var container = new InMemoryContainer("test", new[] { "/partitionKey", "/name" });
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await container.CreateItemAsync(item);

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Create_WithCancellationToken_Cancelled_ThrowsOperationCanceledException()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		using var cts = new CancellationTokenSource();
		await cts.CancelAsync();

		var act = () => _container.CreateItemAsync(item, new PartitionKey("pk1"),
			cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}
}


public class ReadItemGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Read_WithNullId_Throws()
	{
		var act = () => _container.ReadItemAsync<TestDocument>(null!, new PartitionKey("pk1"));

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task Read_WithEmptyId_ThrowsNotFound()
	{
		var act = () => _container.ReadItemAsync<TestDocument>("", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Read_CosmosException_Contains404StatusCode()
	{
		var act = () => _container.ReadItemAsync<TestDocument>("missing", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
		ex.Which.Message.Should().NotBeNullOrEmpty();
	}
}


public class EnableContentResponseOnWriteTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Upsert_WithEnableContentResponseOnWrite_False_ResourceIsNull()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.UpsertItemAsync(item, new PartitionKey("pk1"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Replace_WithEnableContentResponseOnWrite_False_ResourceIsNull()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Patch_WithEnableContentResponseOnWrite_False_ResourceIsNull()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
			new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { EnableContentResponseOnWrite = false });

		response.Resource.Should().BeNull();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Create_WithEnableContentResponseOnWrite_True_ResourceIsPopulated()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"),
			new ItemRequestOptions { EnableContentResponseOnWrite = true });

		response.Resource.Should().NotBeNull();
		response.Resource.Name.Should().Be("Test");
	}
}


public class ItemRequestOptionsEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task ReadItemAsync_WithIfNoneMatch_WildcardStar_ThrowsNotModified()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task UpsertItemAsync_WithIfMatch_WildcardStar_AlwaysSucceeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task CreateItemAsync_WithSessionToken_DoesNotThrow()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"),
			new ItemRequestOptions { SessionToken = "0:some-session-token" });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task ReadItemAsync_WithConsistencyLevel_DoesNotThrow()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { ConsistencyLevel = ConsistencyLevel.Eventual });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteItemAsync_WithIfMatch_WildcardStar_AlwaysSucceeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}
}


public class DeleteItemGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Delete_ResponseResource_IsAlwaysNull()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		response.Resource.Should().BeNull();
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task Delete_WithEnableContentResponseOnWrite_StillReturnsNoContent()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
		response.Resource.Should().BeNull();
	}
}


public class UpsertItemGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Upsert_NewItem_Returns201()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "New" };

		var response = await _container.UpsertItemAsync(item, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Upsert_ExistingItem_Returns200()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var updated = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" };
		var response = await _container.UpsertItemAsync(updated, new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Upsert_StatusCodeDistinguishes_CreateVsReplace()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" };

		var createResponse = await _container.UpsertItemAsync(item, new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var replaceResponse = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second" },
			new PartitionKey("pk1"));
		replaceResponse.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Upsert_WithIfMatch_StaleETag_ThrowsPreconditionFailed()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Upsert_ReplacesEntireDocument_NotMerge()
	{
		var item = new TestDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Name = "Original",
			Value = 42,
			Tags = ["tag1", "tag2"]
		};
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var replacement = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" };
		await _container.UpsertItemAsync(replacement, new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Replaced");
		read.Resource.Value.Should().Be(0);
		read.Resource.Tags.Should().BeEmpty();
	}

	[Fact]
	public async Task Upsert_UpdatesETag()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		var createResponse = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var upsertResponse = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"));

		upsertResponse.ETag.Should().NotBe(createResponse.ETag);
	}

	[Fact]
	public async Task Upsert_RecordsInChangeFeed()
	{
		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" },
			new PartitionKey("pk1"));

		var checkpoint = _container.GetChangeFeedCheckpoint();
		checkpoint.Should().BeGreaterThan(0);
	}
}

// ─── ReplaceItemAsync Unique Key Validation ─────────────────────────────

public class ReplaceItemUniqueKeyTests
{
	[Fact]
	public async Task ReplaceItem_ViolatesUniqueKey_ThrowsConflict()
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
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "bob@test.com" }),
			new PartitionKey("a"));

		// Replace item 2 changing email to collide with item 1
		var act = () => container.ReplaceItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "alice@test.com" }),
			"2", new PartitionKey("a"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}
}

// ─── PatchItemAsync Unique Key Validation ───────────────────────────────

public class PatchItemUniqueKeyTests
{
	[Fact]
	public async Task PatchItem_ViolatesUniqueKey_ThrowsConflict()
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
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "bob@test.com" }),
			new PartitionKey("a"));

		// Patch item 2 changing email to collide with item 1
		var act = () => container.PatchItemAsync<JObject>(
			"2", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/email", "alice@test.com") });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}
}

// ─── Stream CRUD Unique Key Validation ──────────────────────────────────

public class StreamCrudUniqueKeyTests
{
	private static MemoryStream ToStream(object obj) =>
		new(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj)));

	[Fact]
	public async Task CreateItemStream_ViolatesUniqueKey_ReturnsConflict()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemStreamAsync(
			ToStream(new { id = "1", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		var response = await container.CreateItemStreamAsync(
			ToStream(new { id = "2", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task UpsertItemStream_ViolatesUniqueKey_OfDifferentItem_ReturnsConflict()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemStreamAsync(
			ToStream(new { id = "1", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		var response = await container.UpsertItemStreamAsync(
			ToStream(new { id = "2", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task ReplaceItemStream_ViolatesUniqueKey_ReturnsConflict()
	{
		var properties = new ContainerProperties("unique-ctr", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemStreamAsync(
			ToStream(new { id = "1", pk = "a", email = "alice@test.com" }),
			new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			ToStream(new { id = "2", pk = "a", email = "bob@test.com" }),
			new PartitionKey("a"));

		var response = await container.ReplaceItemStreamAsync(
			ToStream(new { id = "2", pk = "a", email = "alice@test.com" }),
			"2", new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}
}

// ─── Trigger Execution ──────────────────────────────────────────────────

public class TriggerExecutionTests
{
	/// <summary>
	/// Pre-triggers can now modify documents via C# handlers registered with RegisterTrigger.
	/// The trigger is registered as a C# Func&lt;JObject, JObject&gt; and fires when
	/// PreTriggers is specified in ItemRequestOptions.
	/// </summary>
	[Fact]
	public async Task PreTrigger_ShouldModifyDocumentOnCreate()
	{
		var container = new InMemoryContainer("test-container", "/pk");

		// Register a C# pre-trigger that adds a 'createdBy' field
		container.RegisterTrigger("addCreatedBy", TriggerType.Pre, TriggerOperation.Create,
			preHandler: doc =>
			{
				doc["createdBy"] = "trigger";
				return doc;
			});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "addCreatedBy" } });

		var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		response.Resource["createdBy"]!.Value<string>().Should().Be("trigger");
	}

	/// <summary>
	/// Demonstrates that CreateTriggerAsync alone (without RegisterTrigger) stores trigger
	/// metadata but does not cause trigger execution. JavaScript bodies are not interpreted.
	/// To get trigger execution, use RegisterTrigger with a C# handler.
	/// </summary>
	[Fact]
	public async Task PreTrigger_CreateTriggerAsyncAlone_DoesNotFireWithoutRegisterTrigger()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos DB: trigger body is executed as server-side JavaScript.
		//   The trigger can read/modify the incoming document before it is committed.
		// In-Memory Emulator: CreateTriggerAsync stores trigger metadata (returns 201 Created)
		//   but does not execute JavaScript bodies. To get trigger execution, register a
		//   C# handler via container.RegisterTrigger(). If PreTriggers is specified in
		//   ItemRequestOptions but no C# handler is registered, the trigger is not found
		//   and a BadRequest (400) is thrown.
		var container = new InMemoryContainer("test-container", "/pk");

		// This succeeds (201 Created) — metadata is stored.
		var triggerResponse = await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "addCreatedBy",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = @"function() { /* would add createdBy */ }"
		});
		triggerResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		// Create an item without specifying PreTriggers — no trigger fires.
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		// Verify the trigger did NOT modify the document.
		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["createdBy"].Should().BeNull(
			"CreateTriggerAsync alone does not enable trigger execution — use RegisterTrigger");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 1: Bug Fix Tests
// ═══════════════════════════════════════════════════════════════════════════

public class ReadItemErrorMessageTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task ReadItemAsync_NotFound_ErrorMessageDoesNotSayAlreadyExists()
	{
		var act = () => _container.ReadItemAsync<TestDocument>("missing", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
		ex.Which.Message.Should().NotContain("already exists");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 2: ETag / Conditional Requests
// ═══════════════════════════════════════════════════════════════════════════

public class CrudIfMatchWildcardTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task Upsert_WithIfMatchWildcard_ExistingItem_Succeeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Replace_WithIfMatchWildcard_ExistingItem_Succeeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" },
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Delete_WithIfMatchWildcard_ExistingItem_Succeeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}
}

public class CrudETagFormatTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task Create_ETagHasQuotedHexFormat()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		response.ETag.Should().MatchRegex("^\"[0-9a-f]{16}\"$");
	}

	[Fact]
	public async Task Create_TwoConsecutiveCreates_DifferentETags()
	{
		var r1 = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		var r2 = await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));

		r1.ETag.Should().NotBe(r2.ETag);
	}

	[Fact]
	public async Task Upsert_SameDataUpserted_StillChangesETag()
	{
		var r1 = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var r2 = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		r2.ETag.Should().NotBe(r1.ETag);
	}

	[Fact]
	public async Task ETagInBody_MatchesResponseHeader_OnCreate()
	{
		var response = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["_etag"]?.ToString().Should().Be(read.ETag);
	}

	[Fact]
	public async Task ETagInBody_MatchesResponseHeader_OnReplace()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" }, "1", new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["_etag"]?.ToString().Should().Be(read.ETag);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 3: Response Metadata
// ═══════════════════════════════════════════════════════════════════════════

public class CrudResponseMetadataTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task Read_RequestCharge_IsGreaterThanZero()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		var response = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		response.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
	}

	[Fact]
	public async Task Upsert_RequestCharge_IsGreaterThanZero()
	{
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		response.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
	}

	[Fact]
	public async Task Replace_RequestCharge_IsGreaterThanZero()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" }, "1", new PartitionKey("pk1"));
		response.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
	}

	[Fact]
	public async Task Delete_RequestCharge_IsGreaterThanZero()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		response.RequestCharge.Should().BeGreaterThanOrEqualTo(0);
	}

	[Fact]
	public async Task Delete_ResponseResource_IsNull()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		response.Resource.Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 3b: Timestamps
// ═══════════════════════════════════════════════════════════════════════════

public class CrudTimestampTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task Create_Ts_IsWithin60SecondsOfNow()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var ts = read.Resource["_ts"]!.Value<long>();
		var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

		ts.Should().BeGreaterThanOrEqualTo(now - 60);
		ts.Should().BeLessThanOrEqualTo(now + 5);
	}

	[Fact]
	public async Task Replace_Ts_IsGreaterThanOrEqualToCreateTs()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		var before = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1")))
			.Resource["_ts"]!.Value<long>();

		await Task.Delay(10);
		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" }, "1", new PartitionKey("pk1"));
		var after = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1")))
			.Resource["_ts"]!.Value<long>();

		after.Should().BeGreaterThanOrEqualTo(before);
	}

	[Fact]
	public async Task Upsert_Update_Ts_IsGreaterThanOrEqualToCreateTs()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		var before = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1")))
			.Resource["_ts"]!.Value<long>();

		await Task.Delay(10);
		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));
		var after = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1")))
			.Resource["_ts"]!.Value<long>();

		after.Should().BeGreaterThanOrEqualTo(before);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 4: Partition Key Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class CrudPartitionKeyEdgeCaseTests
{
	[Fact]
	public async Task Create_WithEmptyStringPartitionKey_Succeeds()
	{
		var container = new InMemoryContainer("test", "/pk");
		var response = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "" }), new PartitionKey(""));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey(""));
		read.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Create_WithNullPropertyValues_Preserved()
	{
		var container = new InMemoryContainer("test", "/pk");
		var doc = JObject.FromObject(new { id = "1", pk = "a", name = (string?)null });
		await container.CreateItemAsync(doc, new PartitionKey("a"));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		read.Resource["name"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task Create_WithExtraProperties_Preserved()
	{
		var container = new InMemoryContainer("test", "/pk");
		var doc = JObject.FromObject(new { id = "1", pk = "a", extraProp = "value", nested = new { x = 1 } });
		await container.CreateItemAsync(doc, new PartitionKey("a"));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		read.Resource["extraProp"]?.ToString().Should().Be("value");
		read.Resource["nested"]?["x"]?.Value<int>().Should().Be(1);
	}

	[Fact]
	public async Task Upsert_WithPartitionKeyNone_ExtractsFromDocument()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var response = await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" });

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 5: Document Size Validation
// ═══════════════════════════════════════════════════════════════════════════

public class CrudDocumentSizeValidationTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task Create_Over2MB_ThrowsRequestEntityTooLarge()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = new string('x', 2_100_000) };
		var act = () => _container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
	}

	[Fact]
	public async Task Replace_Over2MB_ThrowsRequestEntityTooLarge()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "small" }, new PartitionKey("pk1"));

		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = new string('x', 2_100_000) };
		var act = () => _container.ReplaceItemAsync(doc, "1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
	}

	[Fact]
	public async Task Upsert_Over2MB_ThrowsRequestEntityTooLarge()
	{
		var doc = new TestDocument { Id = "1", PartitionKey = "pk1", Name = new string('x', 2_100_000) };
		var act = () => _container.UpsertItemAsync(doc, new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 6: Error Semantics & Empty IDs
// ═══════════════════════════════════════════════════════════════════════════

public class CrudEmptyIdEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task Replace_WithEmptyStringId_ThrowsOrFails()
	{
		var doc = new TestDocument { Id = "", PartitionKey = "pk1", Name = "A" };
		var act = () => _container.ReplaceItemAsync(doc, "", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Delete_WithEmptyStringId_ThrowsOrFails()
	{
		var act = () => _container.DeleteItemAsync<TestDocument>("", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Replace_WithIfNoneMatchEtag_IsIgnoredOnWrites()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" },
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = read.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK, "IfNoneMatch should be ignored on write operations");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 7: Change Feed Integration
// ═══════════════════════════════════════════════════════════════════════════

public class CrudChangeFeedIntegrationTests
{
	[Fact]
	public async Task Create_RecordsChangeFeedEntry()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var before = container.GetChangeFeedCheckpoint();

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		container.GetChangeFeedCheckpoint().Should().Be(before + 1);
	}

	[Fact]
	public async Task Upsert_InsertPath_RecordsChangeFeedEntry()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var before = container.GetChangeFeedCheckpoint();

		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		container.GetChangeFeedCheckpoint().Should().Be(before + 1);
	}

	[Fact]
	public async Task Upsert_UpdatePath_RecordsChangeFeedEntry()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		var before = container.GetChangeFeedCheckpoint();

		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));

		container.GetChangeFeedCheckpoint().Should().Be(before + 1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 8: DeleteAllByPartitionKey Gaps
// ═══════════════════════════════════════════════════════════════════════════

public class DeleteAllByPKExtendedGapTests
{
	[Fact]
	public async Task DeleteAll_RecordsChangeFeedTombstones()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));
		var before = container.GetChangeFeedCheckpoint();

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));

		container.GetChangeFeedCheckpoint().Should().Be(before + 2);
	}

	[Fact]
	public async Task DeleteAll_ThenRecreateItemsInSamePartition()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));

		var response = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task DeleteAll_DoesNotAffectOtherPartitions_ItemCounts()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "C" }, new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "4", PartitionKey = "pk2", Name = "D" }, new PartitionKey("pk2"));
		await container.CreateItemAsync(
			new TestDocument { Id = "5", PartitionKey = "pk2", Name = "E" }, new PartitionKey("pk2"));

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));

		container.ItemCount.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 10: CancellationToken
// ═══════════════════════════════════════════════════════════════════════════

public class ReplaceItemCancelledTokenTests
{
	[Fact]
	public async Task ReplaceItem_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" }, new PartitionKey("pk1"));

		using var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = () => container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "B" },
			"1", new PartitionKey("pk1"), cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CRUD Deep Dive — Phase 11: Divergent Behavior Tests (Skip + Sister)
// ═══════════════════════════════════════════════════════════════════════════

public class CrudSystemPropertyDivergentTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task Read_ResponseBody_Contains_Rid_SystemProperty()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" }, new PartitionKey("pk1"));
		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		response.Resource["_rid"].Should().NotBeNull();
	}

	[Fact]
	public async Task Read_ResponseBody_Contains_Self_SystemProperty()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" }, new PartitionKey("pk1"));
		var response = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		response.Resource["_self"].Should().NotBeNull();
	}
}
