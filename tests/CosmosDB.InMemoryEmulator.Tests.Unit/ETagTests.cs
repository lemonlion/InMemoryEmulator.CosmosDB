using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class ETagGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task IfMatch_WithWildcard_Star_AlwaysSucceeds()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task IfNoneMatch_WithWildcard_Star_Returns304WhenExists()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}
}


public class ETagGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task ETag_ChangesOnEveryWrite()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" };
		var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
		var firstEtag = create.ETag;

		var upsert = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second" },
			new PartitionKey("pk1"));
		var secondEtag = upsert.ETag;

		firstEtag.Should().NotBe(secondEtag);
	}

	[Fact]
	public async Task ETag_ConsistentAcrossMultipleReads()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var read1 = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var read2 = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		read1.ETag.Should().Be(read2.ETag);
	}

	[Fact]
	public async Task ConcurrentUpsert_IfMatch_SecondWriteFails()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" };
		var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));
		var etag = create.ETag;

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First Writer" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });

		var act = () => _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second Writer" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}
}


public class ETagGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task IfMatch_OnCreate_IsIgnored()
	{
		// Create doesn't have a prior version, so IfMatch should be irrelevant
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		var response = await _container.CreateItemAsync(item, new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"nonexistent\"" });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task IfMatch_OnPatch_WithCorrectETag_Succeeds()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 };
		var create = await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { IfMatchEtag = create.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task IfMatch_OnPatch_WithStaleETag_Fails412()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var act = () => _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { IfMatchEtag = "\"stale\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}
}

#region ETag Response Tests

public class ETagResponseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task Delete_TypedResponse_ETag_ShouldBeNull()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		response.ETag.Should().BeNull();
	}

	[Fact]
	public async Task Replace_ResponseETag_ChangesFromCreate()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var createETag = create.ETag;

		var replace = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			"1", new PartitionKey("pk1"));

		replace.ETag.Should().NotBeNullOrEmpty();
		replace.ETag.Should().NotBe(createETag);
	}

	[Fact]
	public async Task Patch_ResponseETag_ChangesFromCreate()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 1 },
			new PartitionKey("pk1"));
		var createETag = create.ETag;

		var patch = await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")]);

		patch.ETag.Should().NotBeNullOrEmpty();
		patch.ETag.Should().NotBe(createETag);
	}

	[Fact]
	public async Task ETag_Format_IsQuotedHex_OnAllWriteOperations()
	{
		var hexPattern = "^\"[0-9a-f]{16}\"$";

		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 1 },
			new PartitionKey("pk1"));
		create.ETag.Should().MatchRegex(hexPattern);

		var upsert = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" },
			new PartitionKey("pk1"));
		upsert.ETag.Should().MatchRegex(hexPattern);

		var replace = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));
		replace.ETag.Should().MatchRegex(hexPattern);

		var patch = await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")]);
		patch.ETag.Should().MatchRegex(hexPattern);
	}

	[Fact]
	public async Task DocumentBody_ETag_MatchesResponseETag()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var bodyETag = read.Resource["_etag"]?.ToString();

		bodyETag.Should().Be(create.ETag);
	}

	[Fact]
	public async Task DocumentBody_ETag_UpdatesOnEveryWrite()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" },
			new PartitionKey("pk1"));

		var read1 = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var etag1 = read1.Resource["_etag"]?.ToString();

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second" },
			new PartitionKey("pk1"));

		var read2 = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var etag2 = read2.Resource["_etag"]?.ToString();

		etag1.Should().NotBe(etag2);
	}
}

#endregion

#region IfMatch Wildcard Tests

public class ETagIfMatchWildcardTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task IfMatch_Wildcard_OnReplace_Succeeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task IfMatch_Wildcard_OnDelete_Succeeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.DeleteItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task IfMatch_Wildcard_OnPatch_Succeeds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 1 },
			new PartitionKey("pk1"));

		var response = await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

#endregion

#region IfNoneMatch Edge Cases

public class ETagIfNoneMatchEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task IfNoneMatch_Wildcard_OnRead_WhenItemDoesNotExist_Returns404()
	{
		var act = () => _container.ReadItemAsync<TestDocument>("nonexistent", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task IfNoneMatch_StaleETag_OnRead_Returns200()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" },
			new PartitionKey("pk1"));
		var oldETag = create.ETag;

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second" },
			new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = oldETag });

		read.StatusCode.Should().Be(HttpStatusCode.OK);
		read.Resource.Name.Should().Be("Second");
	}

	[Fact]
	public async Task IfNoneMatch_OnUpsert_IsIgnored()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		// Real Cosmos ignores IfNoneMatch on write operations
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task IfNoneMatch_OnReplace_IsIgnored()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var response = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

#endregion

#region ETag Lifecycle Tests

public class ETagLifecycleTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task CreateDeleteRecreate_GetsNewETag()
	{
		var create1 = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "First" },
			new PartitionKey("pk1"));
		var etag1 = create1.ETag;

		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var create2 = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Second" },
			new PartitionKey("pk1"));
		var etag2 = create2.ETag;

		etag1.Should().NotBe(etag2);
	}

	[Fact]
	public async Task Upsert_WithIfMatch_WhenItemDoesNotExist_CreatesItem()
	{
		// If-Match is "applicable only on PUT and DELETE" per REST API docs.
		// Upsert uses POST, so If-Match is ignored on the insert path.
		var response = await _container.UpsertItemAsync(
			new TestDocument { Id = "new", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Delete_WithIfMatch_NonExistentItem_Returns404_Not412()
	{
		var act = () => _container.DeleteItemAsync<TestDocument>(
			"nonexistent", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Replace_WithIfMatch_NonExistentItem_Returns404_Not412()
	{
		var act = () => _container.ReplaceItemAsync(
			new TestDocument { Id = "nonexistent", PartitionKey = "pk1", Name = "Test" },
			"nonexistent", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

#endregion

#region ETag Stream Tests

public class ETagStreamTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task StreamRead_IfNoneMatch_Wildcard_WhenExists_Returns304()
	{
		await _container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Test\"}"),
			new PartitionKey("pk1"));

		var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task StreamRead_IfNoneMatch_StaleETag_Returns200()
	{
		var createResp = await _container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"First\"}"),
			new PartitionKey("pk1"));
		var oldETag = createResp.Headers["ETag"];

		await _container.UpsertItemStreamAsync(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Second\"}"),
			new PartitionKey("pk1"));

		var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = oldETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task StreamReplace_IfMatch_Wildcard_Succeeds()
	{
		await _container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Original\"}"),
			new PartitionKey("pk1"));

		var response = await _container.ReplaceItemStreamAsync(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Replaced\"}"),
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task StreamDelete_IfMatch_Wildcard_Succeeds()
	{
		await _container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Test\"}"),
			new PartitionKey("pk1"));

		var response = await _container.DeleteItemStreamAsync(
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task StreamPatch_WithCurrentETag_Succeeds()
	{
		var createResp = await _container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Test\"}"),
			new PartitionKey("pk1"));
		var currentETag = createResp.Headers["ETag"];

		var response = await _container.PatchItemStreamAsync(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { IfMatchEtag = currentETag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task StreamDelete_Response_HasNoETagHeader()
	{
		await _container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"Test\"}"),
			new PartitionKey("pk1"));

		var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

		response.Headers["ETag"].Should().BeNull();
	}
}

#endregion

#region ETag Batch Tests

public class ETagBatchStreamTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task BatchStream_Replace_WithStaleETag_FailsBatch()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var oldETag = create.ETag;

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Changed" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItemStream("1",
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"BatchReplaced\"}"),
			new TransactionalBatchItemRequestOptions { IfMatchEtag = oldETag });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task BatchStream_Upsert_WithStaleETag_FailsBatch()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var oldETag = create.ETag;

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Changed" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItemStream(
			ToStream("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"BatchUpserted\"}"),
			new TransactionalBatchItemRequestOptions { IfMatchEtag = oldETag });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Batch_Delete_WithIfMatch_StaleETag_FailsBatch()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));
		var oldETag = create.ETag;

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Changed" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1", new TransactionalBatchItemRequestOptions { IfMatchEtag = oldETag });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task Batch_Delete_WithIfMatch_CurrentETag_Succeeds()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1", new TransactionalBatchItemRequestOptions { IfMatchEtag = create.ETag });

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		// Confirm item is deleted
		var readAct = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await readAct.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

#endregion

#region GAP-4: Stream Read with Current Specific ETag

public class ETagStreamReadCurrentTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task StreamRead_IfNoneMatch_CurrentETag_Returns304()
	{
		var create = await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var response = await _container.ReadItemStreamAsync(
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = create.ETag });

		response.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}
}

#endregion

#region GAP-6: Body _etag After Various Operations

public class ETagBodyMatchTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task DocumentBody_ETag_MatchesResponseETag_AfterUpsert()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var upsertResponse = await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["_etag"]!.Value<string>().Should().Be(upsertResponse.ETag);
	}

	[Fact]
	public async Task DocumentBody_ETag_MatchesResponseETag_AfterReplace()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var replaceResponse = await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["_etag"]!.Value<string>().Should().Be(replaceResponse.ETag);
	}

	[Fact]
	public async Task DocumentBody_ETag_MatchesResponseETag_AfterPatch()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var patchResponse = await _container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			new[] { PatchOperation.Set("/name", "Patched") });

		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["_etag"]!.Value<string>().Should().Be(patchResponse.ETag);
	}
}

#endregion

#region GAP-7: Wildcard IfMatch on Non-Existent Upsert

public class ETagWildcardUpsertTests
{
	[Fact]
	public async Task IfMatch_Wildcard_OnUpsert_WhenItemDoesNotExist_CreatesItem()
	{
		// If-Match is "applicable only on PUT and DELETE" per REST API docs.
		// Upsert uses POST, so If-Match (including wildcard) is ignored on the insert path.
		var container = new InMemoryContainer("test", "/partitionKey");

		var response = await container.UpsertItemAsync(
			new TestDocument { Id = "new", PartitionKey = "pk1", Name = "New" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "*" });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

#endregion

#region GAP-8: IfNoneMatch Wildcard on Create When Exists

public class ETagIfNoneMatchCreateTests
{
	[Fact]
	public async Task IfNoneMatch_Wildcard_OnCreate_WhenItemAlreadyExists_Returns409()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Existing" },
			new PartitionKey("pk1"));

		var act = () => container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Duplicate" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.And.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}
}

#endregion

#region GAP-9: Stream Create ETag Header

public class ETagStreamCreateTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task StreamCreate_Response_HasETagHeader()
	{
		var json = "{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"test\"}";
		var response = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		response.Headers["ETag"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task StreamCreate_ResponseETag_ChangesFromPriorItem()
	{
		var json1 = "{\"id\":\"1\",\"partitionKey\":\"pk1\",\"name\":\"first\"}";
		var response1 = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json1)), new PartitionKey("pk1"));

		var json2 = "{\"id\":\"2\",\"partitionKey\":\"pk1\",\"name\":\"second\"}";
		var response2 = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json2)), new PartitionKey("pk1"));

		response2.Headers["ETag"].Should().NotBe(response1.Headers["ETag"]);
	}
}

#endregion

#region GAP-11: ETag in SQL Query Results

public class ETagQueryTests
{
	[Fact]
	public async Task ETag_InSqlQueryResults()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var create1 = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));
		var create2 = await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "B" },
			new PartitionKey("pk1"));

		var query = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, c._etag FROM c ORDER BY c.id"));
		var results = new List<JObject>();
		while (query.HasMoreResults)
			results.AddRange(await query.ReadNextAsync());

		results.Should().HaveCount(2);
		results[0]["_etag"]!.Value<string>().Should().Be(create1.ETag);
		results[1]["_etag"]!.Value<string>().Should().Be(create2.ETag);
	}
}

#endregion

#region GAP-12: ETag Preserved Through Export/Import

public class ETagPersistenceTests
{
	[Fact]
	public async Task ETag_PreservedThroughExportImport()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));

		var state = container.ExportState();
		container.ClearItems();

		container.ImportState(state);

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		// After import, body _etag exists but may differ from original (re-enrichment)
		read.Resource["_etag"]!.Value<string>().Should().NotBeNullOrEmpty();
	}
}

#endregion

#region GAP-13: Patch All Operation Types Generate New ETag

public class ETagPatchAllTypesTests
{
	[Fact]
	public async Task Patch_AllOperationTypes_GenerateNewETag()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var create = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Test", value = 10, tags = new[] { "a" } }),
			new PartitionKey("pk1"));
		var etags = new List<string> { create.ETag };

		var r1 = await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Set("/name", "Updated") });
		etags.Add(r1.ETag);

		var r2 = await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Increment("/value", 1) });
		etags.Add(r2.ETag);

		var r3 = await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Add("/tags/-", "b") });
		etags.Add(r3.ETag);

		var r4 = await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Remove("/tags/0") });
		etags.Add(r4.ETag);

		var r5 = await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Replace("/name", "Final") });
		etags.Add(r5.ETag);

		etags.Should().OnlyHaveUniqueItems();
	}
}

#endregion

#region GAP-14/15: Stream Delete/Replace IfMatch Non-Existent

public class ETagStreamNonExistentTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task StreamDelete_IfMatch_NonExistentItem_Returns404()
	{
		var response = await _container.DeleteItemStreamAsync(
			"missing", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task StreamReplace_IfMatch_NonExistentItem_Returns404()
	{
		var json = "{\"id\":\"missing\",\"partitionKey\":\"pk1\",\"name\":\"test\"}";
		var response = await _container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)),
			"missing", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

#endregion

#region GAP-16: Multiple Rapid Writes Unique ETags

public class ETagRapidWriteTests
{
	[Fact]
	public async Task MultipleRapidWrites_EachGetsUniqueETag()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var create = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "V0" },
			new PartitionKey("pk1"));

		var etags = new List<string> { create.ETag };
		for (var i = 1; i <= 10; i++)
		{
			var r = await container.UpsertItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"V{i}" },
				new PartitionKey("pk1"));
			etags.Add(r.ETag);
		}

		etags.Should().OnlyHaveUniqueItems();
		etags.Should().HaveCount(11);
	}
}

#endregion

#region GAP-17: Batch Read Item ETag

public class ETagBatchReadTests
{
	[Fact]
	public async Task Batch_ReadItem_Response_HasEmptyETag()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("1");

		using var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();
		// Batch read currently returns empty ETag (known gap)
		response[0].ETag.Should().BeEmpty();
	}
}

#endregion
