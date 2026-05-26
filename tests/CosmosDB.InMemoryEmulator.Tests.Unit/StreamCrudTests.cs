using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class StreamCrudTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static string ReadStream(Stream stream)
	{
		using var reader = new StreamReader(stream);
		return reader.ReadToEnd();
	}

	[Fact]
	public async Task CreateItemStreamAsync_ValidItem_ReturnsCreated()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Content.Should().NotBeNull();
	}

	[Fact]
	public async Task CreateItemStreamAsync_Duplicate_ReturnsConflict()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task ReadItemStreamAsync_ExistingItem_ReturnsOkWithContent()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().NotBeNull();
		var body = ReadStream(response.Content);
		var jObj = JObject.Parse(body);
		jObj["name"]!.ToString().Should().Be("Test");
	}

	[Fact]
	public async Task ReadItemStreamAsync_NonExistent_ReturnsNotFound()
	{
		var response = await _container.ReadItemStreamAsync("nonexistent", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task UpsertItemStreamAsync_NewItem_ReturnsCreated()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		var response = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task UpsertItemStreamAsync_ExistingItem_ReturnsOk()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
		var response = await _container.UpsertItemStreamAsync(ToStream(updatedJson), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceItemStreamAsync_ExistingItem_ReturnsOk()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var replacementJson = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
		var response = await _container.ReplaceItemStreamAsync(ToStream(replacementJson), "1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceItemStreamAsync_NonExistent_ReturnsNotFound()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		var response = await _container.ReplaceItemStreamAsync(ToStream(json), "1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteItemStreamAsync_ExistingItem_ReturnsNoContent()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task DeleteItemStreamAsync_NonExistent_ReturnsNotFound()
	{
		var response = await _container.DeleteItemStreamAsync("nonexistent", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PatchItemStreamAsync_ExistingItem_ReturnsOk()
	{
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original", Value = 1 };
		await _container.CreateItemAsync(item, new PartitionKey("pk1"));

		var patchOperations = new[] { PatchOperation.Replace("/name", "Patched") };
		var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"), patchOperations);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task PatchItemStreamAsync_NonExistent_ReturnsNotFound()
	{
		var patchOperations = new[] { PatchOperation.Replace("/name", "Patched") };
		var response = await _container.PatchItemStreamAsync("nonexistent", new PartitionKey("pk1"), patchOperations);

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task UpsertItemStreamAsync_MissingLowercaseId_Returns400()
	{
		var json = """{"Id":"1","partitionKey":"pk1","name":"PascalCase"}""";

		var response = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}


public class StreamETagHandlingTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task UpsertItemStreamAsync_WithIfMatch_CurrentETag_ReturnsOk()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
		var createResponse = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
		var etag = createResponse.Headers.ETag;

		var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
		var response = await _container.UpsertItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(updatedJson)),
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task UpsertItemStreamAsync_WithIfMatch_StaleETag_ReturnsPreconditionFailed()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
		var response = await _container.UpsertItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(updatedJson)),
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task PatchItemStreamAsync_WithIfMatch_StaleETag_ReturnsPreconditionFailed()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test", Value = 10 },
			new PartitionKey("pk1"));

		var response = await _container.PatchItemStreamAsync(
			"1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task CreateItemStreamAsync_WithNullStream_Throws()
	{
		var act = () => _container.CreateItemStreamAsync(null!, new PartitionKey("pk1"));
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task ReplaceItemStreamAsync_WithIfMatch_CurrentETag_ReturnsOk()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
		var createResponse = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
		var etag = createResponse.Headers.ETag;

		var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
		var response = await _container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(updatedJson)),
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceItemStreamAsync_WithIfMatch_StaleETag_ReturnsPreconditionFailed()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var updatedJson = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
		var response = await _container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(updatedJson)),
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}
}


/// <summary>
/// Validates the stream API contract specifically for DeleteItemStreamAsync with stale ETag.
/// The Upsert/Patch/Replace variants are already tested in CosmosClientApiGapTests.cs
/// (StreamETagHandlingTests). This covers the missing Delete variant.
/// See: https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.container.deleteitemstreamasync
/// </summary>
public class StreamDeleteETagTests5
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task DeleteItemStream_WithIfMatch_StaleETag_Returns412_DoesNotThrow()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var response = await _container.DeleteItemStreamAsync(
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}
}


public class StreamETagGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task ReadStream_WithIfNoneMatch_CurrentETag_Returns304()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		var createResponse = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
		var etag = createResponse.Headers.ETag;

		var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = etag });

		response.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task UpsertStream_WithIfMatch_StaleETag_Returns412()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var response = await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Updated"}"""),
			new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task ReplaceStream_WithIfMatch_StaleETag_Returns412()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var response = await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Updated"}"""),
			"1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task DeleteStream_WithIfMatch_StaleETag_Returns412()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"stale\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task PatchStream_WithIfMatch_StaleETag_Returns412()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test","value":10}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var response = await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")],
			new PatchItemRequestOptions { IfMatchEtag = "\"stale\"" });

		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task CreateStream_ResponseContainsETagHeader()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		response.Headers.ETag.Should().NotBeNullOrEmpty();
	}
}


public class ResponseMetadataGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task StreamResponse_StatusCode_InResponseMessage()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		var response = await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task StreamResponse_Content_ContainsDocumentJson()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var readResponse = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		readResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		readResponse.Content.Should().NotBeNull();

		using var reader = new StreamReader(readResponse.Content);
		var body = await reader.ReadToEndAsync();
		body.Should().Contain("\"name\"");
	}
}


public class StreamOperationGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task CreateStream_WithoutId_AutoGeneratesId()
	{
		var json = """{"partitionKey":"pk1","name":"NoId"}""";
		var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task CreateStream_Duplicate_Returns409_NotThrow()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		var response = await _container.CreateItemStreamAsync(ToStream(json), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task ReadStream_NotFound_Returns404StatusCode()
	{
		var response = await _container.ReadItemStreamAsync("missing", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReplaceStream_NotFound_Returns404StatusCode()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";
		var response = await _container.ReplaceItemStreamAsync(ToStream(json), "1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteStream_NotFound_Returns404StatusCode()
	{
		var response = await _container.DeleteItemStreamAsync("missing", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task UpsertStream_NewItem_Returns201_Existing_Returns200()
	{
		var json = """{"id":"1","partitionKey":"pk1","name":"Test"}""";

		var createResponse = await _container.UpsertItemStreamAsync(ToStream(json), new PartitionKey("pk1"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var updateJson = """{"id":"1","partitionKey":"pk1","name":"Updated"}""";
		var updateResponse = await _container.UpsertItemStreamAsync(ToStream(updateJson), new PartitionKey("pk1"));
		updateResponse.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}


public class StreamOperationGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task AllStreamMethods_ReturnStatusCode_NotThrow_OnError()
	{
		// Stream methods return errors via StatusCode, not exceptions
		var readResponse = await _container.ReadItemStreamAsync("missing", new PartitionKey("pk1"));
		readResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);

		var replaceResponse = await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"missing","partitionKey":"pk1"}"""),
			"missing", new PartitionKey("pk1"));
		replaceResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);

		var deleteResponse = await _container.DeleteItemStreamAsync("missing", new PartitionKey("pk1"));
		deleteResponse.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  B: Response Body Validation
// ═══════════════════════════════════════════════════════════════════════════

public class StreamResponseBodyTests
{
	private readonly InMemoryContainer _container = new("body-test", "/partitionKey");
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

	[Fact]
	public async Task CreateStream_ResponseContent_ContainsCreatedDocument()
	{
		using var response = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice"}"""),
			new PartitionKey("pk1"));

		response.Content.Should().NotBeNull();
		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["id"]!.ToString().Should().Be("1");
		body["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task UpsertStream_NewItem_ResponseContent_ContainsDocument()
	{
		using var response = await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Bob"}"""),
			new PartitionKey("pk1"));

		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["name"]!.ToString().Should().Be("Bob");
	}

	[Fact]
	public async Task UpsertStream_ExistingItem_ResponseContent_ContainsUpdatedDocument()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Old"}"""),
			new PartitionKey("pk1"));

		using var response = await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"New"}"""),
			new PartitionKey("pk1"));

		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["name"]!.ToString().Should().Be("New");
	}

	[Fact]
	public async Task ReplaceStream_ResponseContent_ContainsReplacedDocument()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Original"}"""),
			new PartitionKey("pk1"));

		using var response = await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Replaced"}"""),
			"1", new PartitionKey("pk1"));

		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["name"]!.ToString().Should().Be("Replaced");
	}

	[Fact]
	public async Task ReadStream_AfterCreateStream_ReturnsCompleteDocument()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice","value":42}"""),
			new PartitionKey("pk1"));

		using var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["name"]!.ToString().Should().Be("Alice");
		body["value"]!.Value<int>().Should().Be(42);
	}

	[Fact]
	public async Task DeleteStream_Success_ContentIsNull()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));

		using var response = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
		response.Content.Should().BeNull();
	}

	[Fact]
	public async Task PatchStream_ResponseContent_ContainsPatchedDocument()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Before"}"""), new PartitionKey("pk1"));

		using var response = await _container.PatchItemStreamAsync(
			"1", new PartitionKey("pk1"), new[] { PatchOperation.Replace("/name", "After") });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["name"]!.ToString().Should().Be("After");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  C: System Properties in Stream Responses
// ═══════════════════════════════════════════════════════════════════════════

public class StreamSystemPropertyTests
{
	private readonly InMemoryContainer _container = new("sysprop-test", "/partitionKey");
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

	[Fact]
	public async Task CreateStream_ResponseBody_ContainsEtagSystemProperty()
	{
		using var response = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""),
			new PartitionKey("pk1"));

		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["_etag"].Should().NotBeNull();
		body["_etag"]!.ToString().Should().NotBeEmpty();
	}

	[Fact]
	public async Task CreateStream_ResponseBody_ContainsTsSystemProperty()
	{
		using var response = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""),
			new PartitionKey("pk1"));

		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["_ts"].Should().NotBeNull();
		body["_ts"]!.Value<long>().Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task UpsertStream_ExistingItem_UpdatesEtagAndTs()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Old"}"""),
			new PartitionKey("pk1"));

		using var read1 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var body1 = JObject.Parse(await ReadStreamAsync(read1.Content));
		var etag1 = body1["_etag"]!.ToString();

		await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"New"}"""),
			new PartitionKey("pk1"));

		using var read2 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var body2 = JObject.Parse(await ReadStreamAsync(read2.Content));
		body2["_etag"]!.ToString().Should().NotBe(etag1);
	}

	[Fact]
	public async Task ReplaceStream_UpdatesEtag()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""),
			new PartitionKey("pk1"));

		using var read1 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var etag1 = JObject.Parse(await ReadStreamAsync(read1.Content))["_etag"]!.ToString();

		await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"replaced"}"""),
			"1", new PartitionKey("pk1"));

		using var read2 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var etag2 = JObject.Parse(await ReadStreamAsync(read2.Content))["_etag"]!.ToString();
		etag2.Should().NotBe(etag1);
	}

	[Fact]
	public async Task PatchStream_UpdatesEtagAndTs()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"orig"}"""), new PartitionKey("pk1"));

		using var read1 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var body1 = JObject.Parse(await ReadStreamAsync(read1.Content));
		var etag1 = body1["_etag"]!.ToString();
		var ts1 = body1["_ts"]!.Value<long>();

		await _container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Replace("/name", "patched") });

		using var read2 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var body2 = JObject.Parse(await ReadStreamAsync(read2.Content));
		body2["_etag"]!.ToString().Should().NotBe(etag1);
		body2["_ts"]!.Value<long>().Should().BeGreaterThanOrEqualTo(ts1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  D: IsSuccessStatusCode
// ═══════════════════════════════════════════════════════════════════════════

public class StreamIsSuccessStatusCodeTests
{
	private readonly InMemoryContainer _container = new("success-test", "/partitionKey");
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task Stream_SuccessResponses_HaveIsSuccessStatusCode_True()
	{
		using var create = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
		create.IsSuccessStatusCode.Should().BeTrue();

		using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		read.IsSuccessStatusCode.Should().BeTrue();

		using var upsert = await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"updated"}"""), new PartitionKey("pk1"));
		upsert.IsSuccessStatusCode.Should().BeTrue();

		using var replace = await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"replaced"}"""),
			"1", new PartitionKey("pk1"));
		replace.IsSuccessStatusCode.Should().BeTrue();

		using var delete = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
		delete.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Stream_ErrorResponses_HaveIsSuccessStatusCode_False()
	{
		using var readMiss = await _container.ReadItemStreamAsync("miss", new PartitionKey("pk1"));
		readMiss.IsSuccessStatusCode.Should().BeFalse();

		using var deleteMiss = await _container.DeleteItemStreamAsync("miss", new PartitionKey("pk1"));
		deleteMiss.IsSuccessStatusCode.Should().BeFalse();

		using var replaceMiss = await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"miss","partitionKey":"pk1"}"""), "miss", new PartitionKey("pk1"));
		replaceMiss.IsSuccessStatusCode.Should().BeFalse();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  E: Response Headers
// ═══════════════════════════════════════════════════════════════════════════

public class StreamCrudResponseHeaderTests
{
	private readonly InMemoryContainer _container = new("hdr-test", "/partitionKey");
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task Stream_AllCrudResponses_ContainRequestChargeHeader()
	{
		using var create = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
		create.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();

		using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		read.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();

		using var upsert = await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","x":"y"}"""), new PartitionKey("pk1"));
		upsert.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();

		using var replace = await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","z":"w"}"""), "1", new PartitionKey("pk1"));
		replace.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();

		using var delete = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
		delete.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Stream_AllCrudResponses_ContainActivityIdHeader()
	{
		using var create = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
		create.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();

		using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		read.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();

		using var delete = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
		delete.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task Stream_WriteResponses_ContainETagHeader()
	{
		using var create = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
		create.Headers.ETag.Should().NotBeNullOrEmpty();

		using var upsert = await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"u"}"""), new PartitionKey("pk1"));
		upsert.Headers.ETag.Should().NotBeNullOrEmpty();

		using var replace = await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"r"}"""), "1", new PartitionKey("pk1"));
		replace.Headers.ETag.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  F: ETag Lifecycle in Stream API
// ═══════════════════════════════════════════════════════════════════════════

public class StreamETagLifecycleTests
{
	private readonly InMemoryContainer _container = new("etag-life", "/partitionKey");
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task UpsertStream_ChangesETag()
	{
		using var create = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
		var etag1 = create.Headers.ETag;

		using var upsert = await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"u"}"""), new PartitionKey("pk1"));
		upsert.Headers.ETag.Should().NotBe(etag1);
	}

	[Fact]
	public async Task ReplaceStream_ChangesETag()
	{
		using var create = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
		var etag1 = create.Headers.ETag;

		using var replace = await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"r"}"""), "1", new PartitionKey("pk1"));
		replace.Headers.ETag.Should().NotBe(etag1);
	}

	[Fact]
	public async Task ReadStream_ConsecutiveReads_ETagConsistent()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));

		using var read1 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		using var read2 = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		read1.Headers.ETag.Should().Be(read2.Headers.ETag);
	}

	[Fact]
	public async Task DeleteStream_WithIfMatch_CurrentETag_Succeeds()
	{
		using var create = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));
		var etag = create.Headers.ETag;

		using var delete = await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });
		delete.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  G: Data Integrity
// ═══════════════════════════════════════════════════════════════════════════

public class StreamDataIntegrityTests
{
	private readonly InMemoryContainer _container = new("integrity-test", "/partitionKey");
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

	[Fact]
	public async Task CreateStream_ThenTypedRead_DataRoundTrips()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice","value":42}"""),
			new PartitionKey("pk1"));

		var result = await _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		result.Resource.Name.Should().Be("Alice");
		result.Resource.Value.Should().Be(42);
	}

	[Fact]
	public async Task TypedCreate_ThenStreamRead_DataRoundTrips()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Bob", Value = 99 },
			new PartitionKey("pk1"));

		using var response = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["name"]!.ToString().Should().Be("Bob");
		body["value"]!.Value<int>().Should().Be(99);
	}

	[Fact]
	public async Task UpsertStream_ReplacesEntireDocument_NotMerge()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice","extra":"field"}"""),
			new PartitionKey("pk1"));

		await _container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"Alice"}"""),
			new PartitionKey("pk1"));

		using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var body = JObject.Parse(await ReadStreamAsync(read.Content));
		body["extra"].Should().BeNull("upsert replaces entire document, it does not merge");
	}

	[Fact]
	public async Task DeleteStream_ThenRead_Returns404()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));

		await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

		using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		read.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteStream_ThenCreate_SameId_Succeeds()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"first"}"""),
			new PartitionKey("pk1"));

		await _container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));

		using var create = await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"second"}"""),
			new PartitionKey("pk1"));
		create.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task ReplaceStream_FullyReplacesDocument()
	{
		await _container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","fieldA":"a","fieldB":"b"}"""),
			new PartitionKey("pk1"));

		await _container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","fieldA":"updated"}"""),
			"1", new PartitionKey("pk1"));

		using var read = await _container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		var body = JObject.Parse(await ReadStreamAsync(read.Content));
		body["fieldA"]!.ToString().Should().Be("updated");
		body["fieldB"].Should().BeNull("replace should fully replace, not merge");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  H: Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class StreamEdgeCaseTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

	[Fact]
	public async Task CreateStream_CompositePartitionKey_ExtractsCorrectly()
	{
		var container = new InMemoryContainer("composite-pk", new[] { "/tenantId", "/userId" });

		using var response = await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","tenantId":"t1","userId":"u1","name":"Alice"}"""),
			new PartitionKeyBuilder().Add("t1").Add("u1").Build());

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var result = await container.ReadItemAsync<JObject>("1",
			new PartitionKeyBuilder().Add("t1").Add("u1").Build());
		result.Resource["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task CreateStream_UnicodeContent_PreservedInResponse()
	{
		var container = new InMemoryContainer("unicode-stream", "/pk");

		using var response = await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","name":"日本語テスト🎉"}"""),
			new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		using var read = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
		var body = JObject.Parse(await ReadStreamAsync(read.Content));
		body["name"]!.ToString().Should().Be("日本語テスト🎉");
	}

	[Fact]
	public async Task CreateStream_RecordsInChangeFeed()
	{
		var container = new InMemoryContainer("cf-stream", "/pk");

		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","name":"feedme"}"""),
			new PartitionKey("a"));

		var iter = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var page = await iter.ReadNextAsync();

		page.Should().Contain(j => j["id"]!.ToString() == "1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  I: Divergent Behaviors (Skip + Sister Tests)
// ═══════════════════════════════════════════════════════════════════════════

public class StreamDivergentBehaviorTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

	[Fact]
	public async Task ReplaceStream_IdMismatch_Returns400()
	{
		// SDK docs state body id must match the id parameter.
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
		var container = new InMemoryContainer("mismatch", "/partitionKey");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1"}"""), new PartitionKey("pk1"));

		using var response = await container.ReplaceItemStreamAsync(
			ToStream("""{"id":"wrong","partitionKey":"pk1"}"""),
			"1", new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ReplaceStream_IdMismatch_AlsoReturns400()
	{
		// SDK docs state body id must match the id parameter.
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
		var container = new InMemoryContainer("mismatch-div", "/partitionKey");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"orig"}"""), new PartitionKey("pk1"));

		using var response = await container.ReplaceItemStreamAsync(
			ToStream("""{"id":"wrong","partitionKey":"pk1","name":"replaced"}"""),
			"1", new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Stream_ResponseBody_ContainsAllSystemProperties()
	{
		var container = new InMemoryContainer("sysprop-all", "/pk");
		using var response = await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));

		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["_rid"].Should().NotBeNull();
		body["_self"].Should().NotBeNull();
		body["_attachments"].Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  BUG-1: Invalid JSON → 400 BadRequest (stream contract)
// ═══════════════════════════════════════════════════════════════════════════

public class StreamBugFix_InvalidJsonTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task CreateStream_InvalidJson_Returns400BadRequest()
	{
		var container = new InMemoryContainer("json-test", "/pk");
		var response = await container.CreateItemStreamAsync(
			ToStream("{{not json}}"), new PartitionKey("a"));
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task UpsertStream_InvalidJson_Returns400BadRequest()
	{
		var container = new InMemoryContainer("json-test", "/pk");
		var response = await container.UpsertItemStreamAsync(
			ToStream("{{not json}}"), new PartitionKey("a"));
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ReplaceStream_InvalidJson_Returns400BadRequest()
	{
		var container = new InMemoryContainer("json-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.ReplaceItemStreamAsync(
			ToStream("{{not json}}"), "1", new PartitionKey("a"));
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task CreateStream_EmptyStream_Returns400()
	{
		var container = new InMemoryContainer("json-test", "/pk");
		var response = await container.CreateItemStreamAsync(
			new MemoryStream(), new PartitionKey("a"));
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  BUG-3: EnableContentResponseOnWrite in stream methods
// ═══════════════════════════════════════════════════════════════════════════

public class StreamBugFix_EnableContentResponseOnWriteTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task CreateStream_EnableContentResponseOnWrite_False_ContentIsNull()
	{
		var container = new InMemoryContainer("ecrw-test", "/pk");
		var response = await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });
		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Content.Should().BeNull();
	}

	[Fact]
	public async Task UpsertStream_EnableContentResponseOnWrite_False_ContentIsNull()
	{
		var container = new InMemoryContainer("ecrw-test", "/pk");
		var response = await container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });
		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Content.Should().BeNull();
	}

	[Fact]
	public async Task ReplaceStream_EnableContentResponseOnWrite_False_ContentIsNull()
	{
		var container = new InMemoryContainer("ecrw-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","name":"new"}"""), "1", new PartitionKey("a"),
			new ItemRequestOptions { EnableContentResponseOnWrite = false });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().BeNull();
	}

	[Fact]
	public async Task PatchStream_EnableContentResponseOnWrite_False_ContentIsNull()
	{
		var container = new InMemoryContainer("ecrw-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","name":"before"}"""), new PartitionKey("a"));
		var response = await container.PatchItemStreamAsync("1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/name", "after") },
			new PatchItemRequestOptions { EnableContentResponseOnWrite = false });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().BeNull();
	}

	[Fact]
	public async Task CreateStream_EnableContentResponseOnWrite_True_ContentPopulated()
	{
		var container = new InMemoryContainer("ecrw-test", "/pk");
		var response = await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"),
			new ItemRequestOptions { EnableContentResponseOnWrite = true });
		response.StatusCode.Should().Be(HttpStatusCode.Created);
		response.Content.Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  BUG-5: UpsertStream missing id → 400 (not exception)
// ═══════════════════════════════════════════════════════════════════════════

public class StreamBugFix_UpsertMissingIdTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task UpsertStream_MissingId_Returns400BadRequest_DoesNotThrow()
	{
		var container = new InMemoryContainer("upsert-noid", "/pk");
		var response = await container.UpsertItemStreamAsync(
			ToStream("""{"pk":"a","name":"no-id"}"""), new PartitionKey("a"));
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  EnsureSuccessStatusCode tests
// ═══════════════════════════════════════════════════════════════════════════

public class StreamEnsureSuccessStatusCodeTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task Stream_EnsureSuccessStatusCode_OnSuccess_ReturnsSelf()
	{
		var container = new InMemoryContainer("ensure-test", "/pk");
		using var response = await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var result = response.EnsureSuccessStatusCode();
		result.Should().BeSameAs(response);
	}

	[Fact]
	public async Task Stream_EnsureSuccessStatusCode_OnFailure_ThrowsCosmosException()
	{
		var container = new InMemoryContainer("ensure-test", "/pk");
		using var response = await container.ReadItemStreamAsync("missing", new PartitionKey("a"));
		var act = () => response.EnsureSuccessStatusCode();
		act.Should().Throw<CosmosException>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Error response ETag + Read ETag header tests
// ═══════════════════════════════════════════════════════════════════════════

public class StreamErrorAndReadHeaderTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task Stream_ErrorResponses_DoNotContainETagHeader()
	{
		var container = new InMemoryContainer("etag-err", "/pk");
		using var readMiss = await container.ReadItemStreamAsync("missing", new PartitionKey("a"));
		readMiss.Headers.ETag.Should().BeNullOrEmpty();
	}

	[Fact]
	public async Task ReadStream_ResponseContainsETagHeader()
	{
		var container = new InMemoryContainer("etag-read", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));

		using var response = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
		response.Headers.ETag.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  ETag wildcard and IfNoneMatch edge cases
// ═══════════════════════════════════════════════════════════════════════════

public class StreamETagWildcardTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task UpsertStream_WithIfMatch_Wildcard_AlwaysSucceeds()
	{
		var container = new InMemoryContainer("wild-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","name":"new"}"""), new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = "*" });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceStream_WithIfMatch_Wildcard_AlwaysSucceeds()
	{
		var container = new InMemoryContainer("wild-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","name":"new"}"""), "1", new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = "*" });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteStream_WithIfMatch_Wildcard_AlwaysSucceeds()
	{
		var container = new InMemoryContainer("wild-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.DeleteItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = "*" });
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task ReadStream_IfNoneMatch_Wildcard_Returns304()
	{
		var container = new InMemoryContainer("wild-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.ReadItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { IfNoneMatchEtag = "*" });
		response.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task ReadStream_IfNoneMatch_StaleEtag_Returns200WithContent()
	{
		var container = new InMemoryContainer("wild-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.ReadItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { IfNoneMatchEtag = "\"stale-etag\"" });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Content.Should().NotBeNull();
	}

	[Fact]
	public async Task UpsertStream_IfMatch_OnNonExistentItem_CreatesItem()
	{
		// If-Match is "applicable only on PUT and DELETE" per REST API docs.
		// Upsert uses POST, so If-Match is ignored on the insert path.
		var container = new InMemoryContainer("wild-test", "/pk");
		var response = await container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Stream edge cases (PartitionKey.None, special chars, delete tombstone)
// ═══════════════════════════════════════════════════════════════════════════

public class StreamEdgeCaseAdditionalTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

	[Fact]
	public async Task CreateStream_WithPartitionKeyNone_ExtractsFromDocument()
	{
		var container = new InMemoryContainer("pknone-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"extracted"}"""), PartitionKey.None);

		using var response = await container.ReadItemStreamAsync("1", new PartitionKey("extracted"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Theory]
	[InlineData("item/1")]
	[InlineData("item#1")]
	[InlineData("item 1")]
	[InlineData("item?1")]
	public async Task CreateStream_SpecialCharactersInId_RoundTrips(string itemId)
	{
		var container = new InMemoryContainer("special-test", "/pk");
		var json = $$"""{"id":"{{itemId}}","pk":"a"}""";
		await container.CreateItemStreamAsync(ToStream(json), new PartitionKey("a"));

		using var response = await container.ReadItemStreamAsync(itemId, new PartitionKey("a"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["id"]!.ToString().Should().Be(itemId);
	}

	[Fact]
	public async Task DeleteStream_RecordsTombstoneInChangeFeed()
	{
		var container = new InMemoryContainer("tombstone-test", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));

		var checkpointBeforeDelete = container.GetChangeFeedCheckpoint();
		await container.DeleteItemStreamAsync("1", new PartitionKey("a"));

		var checkpointAfterDelete = container.GetChangeFeedCheckpoint();
		checkpointAfterDelete.Should().Be(checkpointBeforeDelete + 1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  CancellationToken tests for stream methods
// ═══════════════════════════════════════════════════════════════════════════

public class StreamCancellationTokenTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task ReadItemStream_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("cancel-test", "/pk");
		var cts = new CancellationTokenSource();
		cts.Cancel();
		var act = () => container.ReadItemStreamAsync("1", new PartitionKey("a"), cancellationToken: cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task UpsertItemStream_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("cancel-test", "/pk");
		var cts = new CancellationTokenSource();
		cts.Cancel();
		var act = () => container.UpsertItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"), cancellationToken: cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task ReplaceItemStream_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("cancel-test", "/pk");
		var cts = new CancellationTokenSource();
		cts.Cancel();
		var act = () => container.ReplaceItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), "1", new PartitionKey("a"), cancellationToken: cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task DeleteItemStream_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("cancel-test", "/pk");
		var cts = new CancellationTokenSource();
		cts.Cancel();
		var act = () => container.DeleteItemStreamAsync("1", new PartitionKey("a"), cancellationToken: cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task PatchItemStream_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("cancel-test", "/pk");
		var cts = new CancellationTokenSource();
		cts.Cancel();
		var act = () => container.PatchItemStreamAsync("1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/name", "x") }, cancellationToken: cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Patch validation tests (null/empty/max operations, filter predicate)
// ═══════════════════════════════════════════════════════════════════════════

public class StreamPatchValidationTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

	[Fact]
	public async Task PatchStream_NullOperations_Returns400BadRequest()
	{
		var container = new InMemoryContainer("patch-val", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.PatchItemStreamAsync("1", new PartitionKey("a"), null!);
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task PatchStream_EmptyOperations_Returns400BadRequest()
	{
		var container = new InMemoryContainer("patch-val", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"));
		var response = await container.PatchItemStreamAsync("1", new PartitionKey("a"),
			Array.Empty<PatchOperation>());
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task PatchStream_MoreThan10Operations_Returns400BadRequest()
	{
		var container = new InMemoryContainer("patch-val", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","f0":0,"f1":0,"f2":0,"f3":0,"f4":0,"f5":0,"f6":0,"f7":0,"f8":0,"f9":0,"f10":0}"""),
			new PartitionKey("a"));
		var ops = Enumerable.Range(0, 11)
			.Select(i => PatchOperation.Replace($"/f{i}", i)).ToArray();
		var response = await container.PatchItemStreamAsync("1", new PartitionKey("a"), ops);
		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task PatchStream_WithFilterPredicate_Match_ReturnsOk()
	{
		var container = new InMemoryContainer("patch-filter", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","status":"active"}"""), new PartitionKey("a"));
		var response = await container.PatchItemStreamAsync("1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/status", "inactive") },
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.status = 'active'" });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task PatchStream_WithFilterPredicate_NoMatch_Returns412()
	{
		var container = new InMemoryContainer("patch-filter", "/pk");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a","status":"active"}"""), new PartitionKey("a"));
		var response = await container.PatchItemStreamAsync("1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/status", "inactive") },
			new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.status = 'archived'" });
		response.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Mixed stream/typed API tests
// ═══════════════════════════════════════════════════════════════════════════

public class StreamMixedApiTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));
	private static async Task<string> ReadStreamAsync(Stream s) { using var r = new StreamReader(s); return await r.ReadToEndAsync(); }

	[Fact]
	public async Task StreamCreate_TypedPatch_StreamRead_DataConsistent()
	{
		var container = new InMemoryContainer("mixed-test", "/partitionKey");
		await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","partitionKey":"pk1","name":"orig","value":10}"""),
			new PartitionKey("pk1"));

		await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new[] { PatchOperation.Replace("/name", "patched") });

		using var response = await container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = JObject.Parse(await ReadStreamAsync(response.Content));
		body["name"]!.ToString().Should().Be("patched");
		body["value"]!.Value<int>().Should().Be(10);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  IfMatch on Create (should be ignored)
// ═══════════════════════════════════════════════════════════════════════════

public class StreamCreateEdgeCaseTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task CreateStream_WithIfMatchEtag_IgnoredOnCreate()
	{
		var container = new InMemoryContainer("create-ifmatch", "/pk");
		var response = await container.CreateItemStreamAsync(
			ToStream("""{"id":"1","pk":"a"}"""), new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = "\"some-fake-etag\"" });
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  DIV-3: ErrorMessage on failure ResponseMessages
// ═══════════════════════════════════════════════════════════════════════════

public class StreamErrorMessageDivergentTests
{
	[Fact]
	public async Task Stream_ErrorResponse_ContainsErrorMessage()
	{
		// Expected real Cosmos behavior:
		// response.ErrorMessage contains a descriptive error string for error responses.
		var container = new InMemoryContainer("errmsg-test", "/pk");
		using var response = await container.ReadItemStreamAsync("missing", new PartitionKey("a"));
		response.ErrorMessage.Should().NotBeNullOrEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Plan 40: Stream CRUD Deep Dive Tests
// ═══════════════════════════════════════════════════════════════════════════

// ── Bug Verification ──
public class StreamBugVerificationTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task ReadManyStreamAsync_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/pk");
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = async () => await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) },
			cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task ReadManyStreamAsync_ReturnsETagHeader()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var response = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });

		response.Headers["etag"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReadManyStreamAsync_WithIfNoneMatch_MatchingETag_Returns304()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var first = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		var etag = first.Headers["etag"];

		var second = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) },
			new ReadManyRequestOptions { IfNoneMatchEtag = etag });

		second.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task ReadManyStreamAsync_WithIfNoneMatch_StaleETag_Returns200()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var first = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		var etag = first.Headers["etag"];

		// Modify item to change ETag
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "changed" }), new PartitionKey("a"));

		var second = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) },
			new ReadManyRequestOptions { IfNoneMatchEtag = etag });

		second.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task CreateItemStream_WithCancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/pk");
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var act = async () => await container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"),
			cancellationToken: cts.Token);

		await act.Should().ThrowAsync<OperationCanceledException>();
	}
}

// ── ReadMany Coverage ──
public class StreamReadManyCoverageTests
{
	[Fact]
	public async Task ReadManyStreamAsync_EmptyList_Returns200WithEmptyDocuments()
	{
		var container = new InMemoryContainer("test", "/pk");
		var response = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)>());

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		using var reader = new StreamReader(response.Content);
		var body = JObject.Parse(reader.ReadToEnd());
		((JArray)body["Documents"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ReadManyStreamAsync_MixedFoundAndNotFound_ReturnsOnlyFound()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var response = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")), ("missing", new PartitionKey("a")) });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		using var reader = new StreamReader(response.Content);
		var body = JObject.Parse(reader.ReadToEnd());
		var docs = (JArray)body["Documents"]!;
		docs.Should().HaveCount(1);
		docs[0]!["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task ReadManyStreamAsync_ResponseContainsEnvelopeFormat()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var response = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });

		using var reader = new StreamReader(response.Content);
		var body = JObject.Parse(reader.ReadToEnd());
		body["_rid"].Should().NotBeNull();
		body["Documents"].Should().NotBeNull();
		body["_count"]!.Value<int>().Should().Be(1);
	}

	[Fact]
	public async Task ReadManyStreamAsync_TtlExpiredItems_ExcludedFromResults()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.DefaultTimeToLive = 1;
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await Task.Delay(1500);

		var response = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });

		using var reader = new StreamReader(response.Content);
		var body = JObject.Parse(reader.ReadToEnd());
		((JArray)body["Documents"]!).Should().BeEmpty();
	}
}

// ── Unique Key Violations ──
public class StreamUniqueKeyViolationTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task CreateStream_UniqueKeyViolation_Returns409Conflict()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy { UniqueKeys = { new UniqueKey { Paths = { "/name" } } } }
		};
		var container = new InMemoryContainer(props);
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\",\"name\":\"unique\"}"), new PartitionKey("a"));

		var response = await container.CreateItemStreamAsync(ToStream("{\"id\":\"2\",\"pk\":\"a\",\"name\":\"unique\"}"), new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task ReplaceStream_UniqueKeyViolation_Returns409Conflict()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy { UniqueKeys = { new UniqueKey { Paths = { "/name" } } } }
		};
		var container = new InMemoryContainer(props);
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\",\"name\":\"alice\"}"), new PartitionKey("a"));
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"2\",\"pk\":\"a\",\"name\":\"bob\"}"), new PartitionKey("a"));

		var response = await container.ReplaceItemStreamAsync(
			ToStream("{\"id\":\"2\",\"pk\":\"a\",\"name\":\"alice\"}"), "2", new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}
}

// ── TTL in Stream Reads ──
public class StreamTtlTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task ReadStream_TtlExpiredItem_Returns404()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.DefaultTimeToLive = 1;
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"));
		await Task.Delay(1500);

		var response = await container.ReadItemStreamAsync("1", new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task UpsertStream_TtlExpiredItem_Returns201Created()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.DefaultTimeToLive = 1;
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"));
		await Task.Delay(1500);

		var response = await container.UpsertItemStreamAsync(
			ToStream("{\"id\":\"1\",\"pk\":\"a\",\"name\":\"renewed\"}"), new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ── C# Triggers on Stream API ──
public class StreamCSharpTriggerTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task CreateStream_PreTriggerCSharp_ModifiesDocument()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["injected"] = "yes"; return doc; }));

		var response = await container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResp = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		readResp.Resource["injected"]!.Value<string>().Should().Be("yes");
	}

	[Fact]
	public async Task CreateStream_PostTriggerCSharp_Fires()
	{
		var container = new InMemoryContainer("test", "/pk");
		var triggered = false;
		container.RegisterTrigger("postAudit", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => triggered = true));

		var response = await container.CreateItemStreamAsync(
			ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "postAudit" } });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
		triggered.Should().BeTrue();
	}

	[Fact]
	public async Task DeleteStream_PostTriggerCSharp_Fires()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"));

		var triggered = false;
		container.RegisterTrigger("postDel", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(_ => triggered = true));

		var response = await container.DeleteItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "postDel" } });

		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
		triggered.Should().BeTrue();
	}
}

// ── ETag Header Lifecycle ──
public class StreamETagHeaderLifecycleTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task PatchStream_ResponseContainsETagHeader()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\",\"name\":\"old\"}"), new PartitionKey("a"));

		var patchBody = new { operations = new[] { new { op = "replace", path = "/name", value = "new" } } };
		var response = await container.PatchItemStreamAsync(
			"1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/name", "new") });

		response.Headers["etag"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReadStream_ETagHeader_MatchesBodyEtag()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"));

		var response = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
		var headerEtag = response.Headers["etag"];
		using var reader = new StreamReader(response.Content);
		var body = JObject.Parse(reader.ReadToEnd());
		var bodyEtag = body["_etag"]!.Value<string>();

		headerEtag.Should().Be(bodyEtag);
	}
}

// ── Replace Body Validation ──
public class StreamReplaceValidationTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task ReplaceStream_BodyIdMatchesParameterId_Succeeds()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"));

		var response = await container.ReplaceItemStreamAsync(
			ToStream("{\"id\":\"1\",\"pk\":\"a\",\"name\":\"updated\"}"), "1", new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReplaceStream_EmptyBody_Returns400BadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"));

		var response = await container.ReplaceItemStreamAsync(
			ToStream(""), "1", new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ── Session Token Header ──
public class StreamSessionTokenTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task Stream_CrudResponses_ContainSessionTokenHeader()
	{
		var container = new InMemoryContainer("test", "/pk");

		var create = await container.CreateItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"));
		create.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();

		var read = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
		read.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();

		var upsert = await container.UpsertItemStreamAsync(ToStream("{\"id\":\"1\",\"pk\":\"a\",\"name\":\"u\"}"), new PartitionKey("a"));
		upsert.Headers["x-ms-session-token"].Should().NotBeNullOrEmpty();
	}
}

// ── Post-Trigger Rollback + Change Feed (divergent behavior) ──
public class StreamPostTriggerChangeFeedTests
{
	private static MemoryStream ToStream(string json) => new(Encoding.UTF8.GetBytes(json));

	[Fact]
	public async Task CreateStream_PostTriggerException_DoesNotAppearInChangeFeed()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.RegisterTrigger("failPost", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => throw new Exception("trigger failure")));

		try
		{
			await container.CreateItemStreamAsync(
				ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "failPost" } });
		}
		catch { /* expected */ }

		var iter = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		iter.HasMoreResults.Should().BeFalse("failed write should not appear in change feed");
	}

	[Fact]
	public async Task CreateStream_PostTriggerException_RollsBackItem_ActualBehavior()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.RegisterTrigger("failPost", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => throw new Exception("trigger failure")));

		try
		{
			await container.CreateItemStreamAsync(
				ToStream("{\"id\":\"1\",\"pk\":\"a\"}"), new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "failPost" } });
		}
		catch (CosmosException)
		{
			// Post-trigger failure throws CosmosException in stream API
		}

		// Item should be rolled back
		var readResp = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
		readResp.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}
