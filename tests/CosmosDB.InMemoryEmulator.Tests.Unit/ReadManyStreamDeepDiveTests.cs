using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Phase 1: ReadMany Stream Variant Parity (Tests #1-17)
// ══════════════════════════════════════════════════════════════════════════════

public class ReadManyStreamDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	private async Task SeedAsync()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie" },
			new PartitionKey("pk2"));
	}

	[Fact]
	public async Task ReadManyStream_CancelledToken_ThrowsOperationCanceledException()
	{
		await SeedAsync();
		var cts = new CancellationTokenSource();
		cts.Cancel();
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var act = () => _container.ReadManyItemsStreamAsync(items, cancellationToken: cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task ReadManyStream_WithIfNoneMatchEtag_Returns304WhenUnchanged()
	{
		await SeedAsync();
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("2", new PartitionKey("pk1"))
		};
		// First read to get ETag
		var first = await _container.ReadManyItemsStreamAsync(items);
		var etag = first.Headers["ETag"];
		etag.Should().NotBeNullOrEmpty();
		// Second read with IfNoneMatchEtag
		var second = await _container.ReadManyItemsStreamAsync(items,
			new ReadManyRequestOptions { IfNoneMatchEtag = etag });
		second.StatusCode.Should().Be(HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task ReadManyStream_ResponseContainsCompositeETagHeader()
	{
		await SeedAsync();
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("2", new PartitionKey("pk1"))
		};
		var response = await _container.ReadManyItemsStreamAsync(items);
		response.Headers["ETag"].Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReadManyStream_SomeNotExist_ReturnsOnlyExisting()
	{
		await SeedAsync();
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("999", new PartitionKey("pk1")) // doesn't exist
        };
		var response = await _container.ReadManyItemsStreamAsync(items);
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(1);
	}

	[Fact]
	public async Task ReadManyStream_WrongPartitionKey_ReturnsEmptyDocuments()
	{
		await SeedAsync();
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("wrong-pk"))
		};
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ReadManyStream_MixedPartitionKeys_ReturnsAll()
	{
		await SeedAsync();
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("3", new PartitionKey("pk2"))
		};
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(2);
	}

	[Fact]
	public async Task ReadManyStream_AfterItemUpdate_ReturnsUpdatedVersion()
	{
		await SeedAsync();
		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice Updated" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		body["Documents"]![0]!["name"]!.Value<string>().Should().Be("Alice Updated");
	}

	[Fact]
	public async Task ReadManyStream_AfterItemDelete_ExcludesDeletedItem()
	{
		await SeedAsync();
		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("2", new PartitionKey("pk1"))
		};
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(1);
		body["Documents"]![0]!["id"]!.Value<string>().Should().Be("2");
	}

	[Fact]
	public async Task ReadManyStream_AfterItemReplace_ReturnsReplacedVersion()
	{
		await SeedAsync();
		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		body["Documents"]![0]!["name"]!.Value<string>().Should().Be("Replaced");
	}

	[Fact]
	public async Task ReadManyStream_AfterPatchOperation_ReturnsPatchedVersion()
	{
		await SeedAsync();
		await _container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			[PatchOperation.Set("/name", "Patched")]);
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		body["Documents"]![0]!["name"]!.Value<string>().Should().Be("Patched");
	}

	[Fact]
	public async Task ReadManyStream_LargeList_100Items_ReturnsAll()
	{
		for (int i = 0; i < 100; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
		}
		var items = Enumerable.Range(0, 100)
			.Select(i => ($"{i}", new PartitionKey("pk1"))).ToList();
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(100);
	}

	[Fact]
	public async Task ReadManyStream_HierarchicalPartitionKey_ReturnsItems()
	{
		var container = new InMemoryContainer(new ContainerProperties("test",
			["/tenantId", "/userId"]));
		var pk = new PartitionKeyBuilder().Add("t1").Add("u1").Build();
		await container.CreateItemAsync(JObject.FromObject(new
		{
			id = "1",
			tenantId = "t1",
			userId = "u1",
			name = "Doc1"
		}), pk);
		var items = new List<(string, PartitionKey)> { ("1", pk) };
		var response = await container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(1);
	}

	[Fact]
	public async Task ReadManyStream_NumericPartitionKey_ReturnsItems()
	{
		var container = new InMemoryContainer("test", "/num");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", num = 42, name = "Doc" }),
			new PartitionKey(42));
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey(42)) };
		var response = await container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(1);
	}

	[Fact]
	public async Task ReadManyStream_SpecialCharactersInId_ReturnsItem()
	{
		var specialId = "item/with+special&chars";
		await _container.CreateItemAsync(
			new TestDocument { Id = specialId, PartitionKey = "pk1", Name = "Special" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)> { (specialId, new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(1);
	}

	[Fact]
	public async Task ReadManyStream_UnicodeId_ReturnsItem()
	{
		var unicodeId = "ドキュメント_1";
		await _container.CreateItemAsync(
			new TestDocument { Id = unicodeId, PartitionKey = "pk1", Name = "Unicode" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)> { (unicodeId, new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(1);
	}

	[Fact]
	public async Task ReadManyStream_CaseSensitiveIds_TreatedDistinct()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "ABC", PartitionKey = "pk1", Name = "Upper" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "abc", PartitionKey = "pk1", Name = "Lower" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)>
		{
			("ABC", new PartitionKey("pk1")),
			("abc", new PartitionKey("pk1"))
		};
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((JArray)body["Documents"]!).Should().HaveCount(2);
	}

	[Fact]
	public async Task ReadManyStream_ResponseHeaders_ContainRequestChargeAndActivityId()
	{
		await SeedAsync();
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsStreamAsync(items);
		response.Headers["x-ms-request-charge"].Should().NotBeNullOrEmpty();
		response.Headers["x-ms-activity-id"].Should().NotBeNullOrEmpty();
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Phase 2: Edge Cases & Gaps (Tests #18-30)
// ══════════════════════════════════════════════════════════════════════════════

public class ReadManyPartitionKeyDeepDiveV2Tests
{
	[Fact]
	public async Task ReadMany_PartitionKeyNone_ReturnsItems()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", name = "NoPartition" }),
			PartitionKey.Null);
		var items = new List<(string, PartitionKey)> { ("1", PartitionKey.Null) };
		var response = await container.ReadManyItemsAsync<JObject>(items);
		response.Should().HaveCount(1);
		response.First()["name"]!.Value<string>().Should().Be("NoPartition");
	}

	[Fact]
	public async Task ReadMany_ThreeLevelHierarchicalPartitionKey_ReturnsItems()
	{
		var container = new InMemoryContainer(new ContainerProperties("test",
			["/tenantId", "/region", "/userId"]));
		var pk = new PartitionKeyBuilder().Add("t1").Add("us-east").Add("u1").Build();
		await container.CreateItemAsync(JObject.FromObject(new
		{
			id = "1",
			tenantId = "t1",
			region = "us-east",
			userId = "u1"
		}), pk);
		var items = new List<(string, PartitionKey)> { ("1", pk) };
		var response = await container.ReadManyItemsAsync<JObject>(items);
		response.Should().HaveCount(1);
	}
}

public class ReadManyOptionsDeepDiveV2Tests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task ReadMany_CompositeEtag_ChangesAfterMutation()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var first = await _container.ReadManyItemsAsync<TestDocument>(items);
		var etag1 = first.ETag;
		// Mutate
		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"));
		var second = await _container.ReadManyItemsAsync<TestDocument>(items);
		var etag2 = second.ETag;
		etag2.Should().NotBe(etag1);
	}

	[Fact]
	public async Task ReadMany_CompositeEtag_IsNullWhenNoItemsFound()
	{
		var items = new List<(string, PartitionKey)> { ("nonexistent", new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsAsync<TestDocument>(items);
		response.Count.Should().Be(0);
		response.Headers.ETag.Should().BeNullOrEmpty();
	}

	[Fact]
	public async Task ReadMany_WithStaleIfNoneMatchEtag_Returns200WithItems()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var first = await _container.ReadManyItemsAsync<TestDocument>(items);
		var etag = first.Headers.ETag;
		// Mutate to make ETag stale
		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"));
		var second = await _container.ReadManyItemsAsync<TestDocument>(items,
			new ReadManyRequestOptions { IfNoneMatchEtag = etag });
		second.StatusCode.Should().Be(HttpStatusCode.OK);
		second.Count.Should().Be(1);
	}
}

public class ReadManyMutationDeepDiveV2Tests
{
	[Fact]
	public async Task ReadMany_AfterClearItems_ReturnsEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		container.ClearItems();
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var response = await container.ReadManyItemsAsync<TestDocument>(items);
		response.Should().BeEmpty();
	}
}

public class ReadManyDeserializationDeepDiveV2Tests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task ReadManyStream_SystemProperties_IncludeRidSelfAttachments()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		var doc = body["Documents"]![0]!;
		doc["_rid"].Should().NotBeNull();
		doc["_self"].Should().NotBeNull();
		doc["_attachments"].Should().NotBeNull();
		doc["_ts"].Should().NotBeNull();
		doc["_etag"].Should().NotBeNull();
	}

	[Fact]
	public async Task ReadManyStream_CountFieldMatchesFoundItemsWhenSomeMissing()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("999", new PartitionKey("pk1")) // doesn't exist
        };
		var response = await _container.ReadManyItemsStreamAsync(items);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		body["_count"]!.Value<int>().Should().Be(1);
		((JArray)body["Documents"]!).Should().HaveCount(1);
	}
}

public class ReadManyResponseDeepDiveV2Tests
{
	private readonly InMemoryContainer _container = new("test", "/partitionKey");

	[Fact]
	public async Task ReadMany_Headers_ContainItemCount()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("2", new PartitionKey("pk1"))
		};
		var response = await _container.ReadManyItemsAsync<TestDocument>(items);
		response.Count.Should().Be(2);
	}

	[Fact]
	public async Task ReadMany_Headers_ContainActivityId()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) };
		var response = await _container.ReadManyItemsAsync<TestDocument>(items);
		response.ActivityId.Should().NotBeNullOrEmpty();
	}

	[Fact]
	public async Task ReadMany_StatusCode_IsOkWhenNoneExist()
	{
		var items = new List<(string, PartitionKey)>
		{
			("nonexistent1", new PartitionKey("pk1")),
			("nonexistent2", new PartitionKey("pk2"))
		};
		var response = await _container.ReadManyItemsAsync<TestDocument>(items);
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Should().BeEmpty();
	}
}

public class ReadManyConcurrencyDeepDiveTests
{
	[Fact]
	public async Task ReadManyStream_ConcurrentCalls_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (int i = 0; i < 10; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
		}
		var items = Enumerable.Range(0, 10)
			.Select(i => ($"{i}", new PartitionKey("pk1"))).ToList();

		var tasks = Enumerable.Range(0, 20).Select(_ =>
			container.ReadManyItemsStreamAsync(items));
		var results = await Task.WhenAll(tasks);
		results.Should().AllSatisfy(r => r.StatusCode.Should().Be(HttpStatusCode.OK));
	}
}

public class ReadManyEmptyIdDeepDiveTests
{
	[Fact]
	public async Task CreateItem_EmptyStringId_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var act = () => container.CreateItemAsync(
			new TestDocument { Id = "", PartitionKey = "pk1", Name = "EmptyId" },
			new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}
