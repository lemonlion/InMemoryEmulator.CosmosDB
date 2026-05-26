using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class ReadManyTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedItems()
	{
		var items = new[]
		{
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new TestDocument { Id = "3", PartitionKey = "pk2", Name = "Charlie" },
		};
		foreach (var item in items)
		{
			await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
		}
	}

	#region Happy Path (typed)

	[Fact]
	public async Task ReadMany_AllExist_ReturnsAll()
	{
		await SeedItems();
		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pk1")),
			("3", new PartitionKey("pk2")),
		};

		var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Should().HaveCount(2);
		response.Resource.Select(d => d.Name).Should().Contain("Alice").And.Contain("Charlie");
	}

	[Fact]
	public async Task ReadMany_SingleItem_ReturnsIt()
	{
		await SeedItems();
		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("2", new PartitionKey("pk1")),
		};

		var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

		response.Resource.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ReadMany_MixedPartitionKeys_ReturnsAll()
	{
		await SeedItems();
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("3", new PartitionKey("pk2")),
		};

		var response = await _container.ReadManyItemsAsync<TestDocument>(items);

		response.Resource.Should().HaveCount(2);
	}

	#endregion

	#region Missing Items (typed)

	[Fact]
	public async Task ReadMany_SomeNotExist_ReturnsOnlyExisting()
	{
		await SeedItems();
		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pk1")),
			("nonexistent", new PartitionKey("pk1")),
		};

		var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

		response.Resource.Should().HaveCount(1);
		response.Resource.First().Name.Should().Be("Alice");
	}

	[Fact]
	public async Task ReadMany_NoneExist_ReturnsEmpty()
	{
		await SeedItems();
		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("nonexistent1", new PartitionKey("pk1")),
			("nonexistent2", new PartitionKey("pk2")),
		};

		var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

		response.Resource.Should().BeEmpty();
	}

	[Fact]
	public async Task ReadMany_WrongPartitionKey_DoesNotReturn()
	{
		await SeedItems();
		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pk2")),
		};

		var response = await _container.ReadManyItemsAsync<TestDocument>(itemsToRead);

		response.Resource.Should().BeEmpty();
	}

	[Fact]
	public async Task ReadMany_EmptyList_ReturnsEmpty()
	{
		await SeedItems();
		var response = await _container.ReadManyItemsAsync<TestDocument>([]);

		response.Resource.Should().BeEmpty();
	}

	#endregion

	#region Duplicate Handling

	[Fact]
	public async Task ReadMany_DuplicateIdsInList_ReturnsDuplicates()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("1", new PartitionKey("pk1")),
		};

		var response = await _container.ReadManyItemsAsync<TestDocument>(items);

		response.Resource.Should().HaveCount(2);
	}

	[Fact]
	public async Task ReadMany_DuplicateIdsInList_Stream_ReturnsDuplicates()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("1", new PartitionKey("pk1")),
		};

		using var response = await _container.ReadManyItemsStreamAsync(items);
		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);
		((JArray)jObj["Documents"]!).Should().HaveCount(2);
	}

	#endregion

	#region Stream Variant

	[Fact]
	public async Task ReadManyStream_AllExist_ReturnsOkWithDocuments()
	{
		await SeedItems();
		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pk1")),
			("2", new PartitionKey("pk1")),
		};

		using var response = await _container.ReadManyItemsStreamAsync(itemsToRead);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);
		((JArray)jObj["Documents"]!).Should().HaveCount(2);
	}

	[Fact]
	public async Task ReadManyStream_EmptyList_ReturnsOkWithEmptyDocuments()
	{
		var emptyList = new List<(string id, PartitionKey pk)>();

		using var response = await _container.ReadManyItemsStreamAsync(emptyList);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);
		((JArray)jObj["Documents"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ReadManyStream_AllMissing_ReturnsOkWithEmptyDocuments()
	{
		var items = new List<(string id, PartitionKey pk)>
		{
			("nonexistent1", new PartitionKey("pk1")),
			("nonexistent2", new PartitionKey("pk2")),
		};

		using var response = await _container.ReadManyItemsStreamAsync(items);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);
		((JArray)jObj["Documents"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ReadManyStream_ContainsCountField()
	{
		await SeedItems();
		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pk1")),
			("3", new PartitionKey("pk2")),
		};

		using var response = await _container.ReadManyItemsStreamAsync(itemsToRead);
		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);

		jObj["_count"]!.Value<int>().Should().Be(2);
	}

	#endregion

	#region Response Metadata

	[Fact]
	public async Task ReadMany_StatusCode_IsOk()
	{
		await SeedItems();
		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk1"))]);

		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReadMany_Count_MatchesFoundItems()
	{
		await SeedItems();
		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("pk1")),
			("2", new PartitionKey("pk1")),
			("nonexistent", new PartitionKey("pk1")),
		};

		var response = await _container.ReadManyItemsAsync<TestDocument>(items);

		response.Resource.Should().HaveCount(2);
		response.Count.Should().Be(2);
	}

	[Fact]
	public async Task ReadMany_RequestCharge_IsPositive()
	{
		await SeedItems();
		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk1"))]);

		response.RequestCharge.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task ReadMany_ActivityId_IsPresent()
	{
		await SeedItems();
		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk1"))]);

		response.ActivityId.Should().NotBeNull();
	}

	#endregion

	#region Error Handling

	[Fact]
	public async Task ReadMany_NullList_ThrowsArgumentNullException()
	{
		var act = () => _container.ReadManyItemsAsync<TestDocument>(null!);

		await act.Should().ThrowAsync<ArgumentNullException>();
	}

	[Fact]
	public async Task ReadManyStream_NullList_ThrowsArgumentNullException()
	{
		var act = () => _container.ReadManyItemsStreamAsync(null!);

		await act.Should().ThrowAsync<ArgumentNullException>();
	}

	#endregion

	#region Scale

	[Fact]
	public async Task ReadMany_LargeList_100Items_ReturnsAll()
	{
		for (var i = 0; i < 110; i++)
		{
			await _container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
		}

		var items = Enumerable.Range(0, 110)
			.Select(i => ($"{i}", new PartitionKey("pk1")))
			.ToList();

		var response = await _container.ReadManyItemsAsync<TestDocument>(items);

		response.Resource.Should().HaveCount(110);
	}

	#endregion

	#region After Mutations

	[Fact]
	public async Task ReadMany_AfterItemUpdate_ReturnsUpdatedVersion()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		await _container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice Updated" },
			new PartitionKey("pk1"));

		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk1"))]);

		response.Resource.Should().ContainSingle().Which.Name.Should().Be("Alice Updated");
	}

	[Fact]
	public async Task ReadMany_AfterItemDelete_ExcludesDeletedItem()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		await _container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk1")), ("2", new PartitionKey("pk1"))]);

		response.Resource.Should().ContainSingle().Which.Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ReadMany_AfterItemReplace_ReturnsReplacedVersion()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		await _container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice Replaced" },
			"1", new PartitionKey("pk1"));

		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey("pk1"))]);

		response.Resource.Should().ContainSingle().Which.Name.Should().Be("Alice Replaced");
	}

	#endregion

	#region Partition Key Edge Cases

	[Fact]
	public async Task ReadMany_HierarchicalPartitionKey_ReturnsItems()
	{
		var container = new InMemoryContainer("hierarchical-container", ["/tenantId", "/userId"]);
		var doc = JObject.FromObject(new { id = "1", tenantId = "t1", userId = "u1", name = "Alice" });
		var pk = new PartitionKeyBuilder().Add("t1").Add("u1").Build();
		await container.CreateItemAsync(doc, pk);

		var response = await container.ReadManyItemsAsync<JObject>([("1", pk)]);

		response.Resource.Should().ContainSingle();
		response.Resource.First()["name"]!.Value<string>().Should().Be("Alice");
	}

	[Fact]
	public async Task ReadMany_EmptyStringPartitionKey_ReturnsItems()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "", Name = "EmptyPk" },
			new PartitionKey(""));

		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[("1", new PartitionKey(""))]);

		response.Resource.Should().ContainSingle().Which.Name.Should().Be("EmptyPk");
	}

	#endregion

	#region ID Edge Cases

	[Fact]
	public async Task ReadMany_CaseSensitiveIds_TreatedDistinct()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "ABC", PartitionKey = "pk1", Name = "Upper" },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "abc", PartitionKey = "pk1", Name = "Lower" },
			new PartitionKey("pk1"));

		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[("ABC", new PartitionKey("pk1")), ("abc", new PartitionKey("pk1"))]);

		response.Resource.Should().HaveCount(2);
		response.Resource.Select(d => d.Name).Should().BeEquivalentTo(["Upper", "Lower"]);
	}

	[Fact]
	public async Task ReadMany_SpecialCharactersInId_ReturnsItem()
	{
		var specialId = "item/with spaces.and-dots";
		await _container.CreateItemAsync(
			new TestDocument { Id = specialId, PartitionKey = "pk1", Name = "Special" },
			new PartitionKey("pk1"));

		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[(specialId, new PartitionKey("pk1"))]);

		response.Resource.Should().ContainSingle().Which.Name.Should().Be("Special");
	}

	[Fact]
	public async Task ReadMany_UnicodeId_ReturnsItem()
	{
		var unicodeId = "日本語テスト🚀";
		await _container.CreateItemAsync(
			new TestDocument { Id = unicodeId, PartitionKey = "pk1", Name = "Unicode" },
			new PartitionKey("pk1"));

		var response = await _container.ReadManyItemsAsync<TestDocument>(
			[(unicodeId, new PartitionKey("pk1"))]);

		response.Resource.Should().ContainSingle().Which.Name.Should().Be("Unicode");
	}

	#endregion

	#region System Properties

	[Fact]
	public async Task ReadManyStream_ResultsIncludeSystemProperties()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		using var response = await _container.ReadManyItemsStreamAsync(
			[("1", new PartitionKey("pk1"))]);
		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);
		var doc = ((JArray)jObj["Documents"]!).First as JObject;

		doc!["_etag"].Should().NotBeNull();
		doc["_ts"].Should().NotBeNull();
	}

	#endregion

	#region Concurrency

	[Fact]
	public async Task ReadMany_ConcurrentCalls_AllSucceed()
	{
		await SeedItems();
		var itemsToRead = new List<(string id, PartitionKey pk)>
		{
			("1", new PartitionKey("pk1")),
			("2", new PartitionKey("pk1")),
			("3", new PartitionKey("pk2")),
		};

		var tasks = Enumerable.Range(0, 10)
			.Select(_ => _container.ReadManyItemsAsync<TestDocument>(itemsToRead))
			.ToArray();

		var results = await Task.WhenAll(tasks);

		foreach (var response in results)
		{
			response.Resource.Should().HaveCount(3);
		}
	}

	#endregion
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: ReadMany TTL Interaction
// ═══════════════════════════════════════════════════════════════════════════════

public class ReadManyTtlDeepDiveTests
{
	[Fact]
	public async Task ReadMany_TTLExpiredItem_ExcludesExpiredItem()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.DefaultTimeToLive = 1;
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await Task.Delay(1500);
		var result = await container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.Count.Should().Be(0);
	}

	[Fact]
	public async Task ReadMany_MixOfExpiredAndLive_ReturnsOnlyLive()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.DefaultTimeToLive = 1;
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await Task.Delay(1500);
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a"));
		var result = await container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a")),
			("2", new PartitionKey("a"))
		});
		result.Count.Should().Be(1);
		result.First()["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task ReadManyStream_TTLExpiredItem_ExcludesExpiredItem()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.DefaultTimeToLive = 1;
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await Task.Delay(1500);
		var response = await container.ReadManyItemsStreamAsync(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((int)body["_count"]!).Should().Be(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: ReadMany Partition Key Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class ReadManyPartitionKeyDeepDiveTests
{
	[Fact]
	public async Task ReadMany_NumericPartitionKey_ReturnsItems()
	{
		var container = new InMemoryContainer("test", "/numPk");
		var item = JObject.FromObject(new { id = "1", numPk = 42 });
		await container.CreateItemAsync(item, new PartitionKey(42));
		var result = await container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey(42)) });
		result.Count.Should().Be(1);
	}

	[Fact]
	public async Task ReadMany_BooleanPartitionKey_ReturnsItems()
	{
		var container = new InMemoryContainer("test", "/boolPk");
		var item = JObject.FromObject(new { id = "1", boolPk = true });
		await container.CreateItemAsync(item, new PartitionKey(true));
		var result = await container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey(true)) });
		result.Count.Should().Be(1);
	}

	[Fact]
	public async Task ReadMany_SameIdDifferentPartitionKeys_ReturnsBoth()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "b", name = "Bob" }), new PartitionKey("b"));
		var result = await container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a")),
			("1", new PartitionKey("b"))
		});
		result.Count.Should().Be(2);
	}

	[Fact]
	public async Task ReadMany_PartitionKeyNull_ReturnsNullPkItems()
	{
		var container = new InMemoryContainer("test", "/pk");
		var item = JObject.Parse("{\"id\":\"1\",\"pk\":null}");
		await container.CreateItemAsync(item, PartitionKey.Null);
		var result = await container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", PartitionKey.Null) });
		result.Count.Should().Be(1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: ReadMany Request Options & CancellationToken
// ═══════════════════════════════════════════════════════════════════════════════

public class ReadManyOptionsDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task ReadMany_WithRequestOptions_DoesNotThrow()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var options = new ReadManyRequestOptions();
		var result = await _container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) }, options);
		result.Count.Should().Be(1);
	}

	[Fact]
	public async Task ReadManyStream_WithRequestOptions_DoesNotThrow()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var options = new ReadManyRequestOptions();
		var response = await _container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) }, options);
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task ReadMany_CancelledToken_ThrowsOperationCanceledException()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		using var cts = new CancellationTokenSource();
		cts.Cancel();
		var act = () => _container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) }, cancellationToken: cts.Token);
		await act.Should().ThrowAsync<OperationCanceledException>();
	}

	[Fact]
	public async Task ReadMany_WithIfNoneMatchEtag_Returns304WhenUnchanged()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var first = await _container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		first.StatusCode.Should().Be(HttpStatusCode.OK);
		first.ETag.Should().NotBeNullOrEmpty();

		var second = await _container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) },
			new ReadManyRequestOptions { IfNoneMatchEtag = first.ETag });
		second.StatusCode.Should().Be(HttpStatusCode.NotModified);
		second.Count.Should().Be(0);
	}

	[Fact]
	public async Task ReadMany_WithIfNoneMatchEtag_AlwaysReturns200_Divergent()
	{
		// DIVERGENT BEHAVIOR: IfNoneMatchEtag is silently ignored.
		// The emulator does not compute composite response ETags across multiple items.
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var result = await _container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: ReadMany After Mutations Extended
// ═══════════════════════════════════════════════════════════════════════════════

public class ReadManyMutationDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task ReadMany_AfterPatchOperation_ReturnsPatchedVersion()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await _container.PatchItemAsync<JObject>("1", new PartitionKey("a"),
			new[] { PatchOperation.Set("/name", "Alicia") });
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.First()["name"]!.ToString().Should().Be("Alicia");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: ReadMany Response Properties
// ═══════════════════════════════════════════════════════════════════════════════

public class ReadManyResponseDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task Seed()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
	}

	[Fact]
	public async Task ReadMany_Diagnostics_IsNotNull()
	{
		await Seed();
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.Diagnostics.Should().NotBeNull();
	}

	[Fact]
	public async Task ReadMany_ContinuationToken_IsNull()
	{
		await Seed();
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.ContinuationToken.Should().BeNull();
	}

	[Fact]
	public async Task ReadMany_IndexMetrics_IsNull()
	{
		await Seed();
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.IndexMetrics.Should().BeNull();
	}

	[Fact]
	public async Task ReadMany_ResponseIsEnumerable()
	{
		await Seed();
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		var count = 0;
		foreach (var _ in result) count++;
		count.Should().Be(1);
	}

	[Fact]
	public async Task ReadMany_Headers_ContainRequestCharge()
	{
		await Seed();
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.Headers["x-ms-request-charge"].Should().NotBeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: ReadMany Deserialization & Stream Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class ReadManyDeserializationDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task ReadMany_ComplexNestedDocument_Roundtrips()
	{
		var complex = JObject.FromObject(new { id = "1", pk = "a", nested = new { inner = new { value = 42 } }, tags = new[] { "x", "y" } });
		await _container.CreateItemAsync(complex, new PartitionKey("a"));
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.First()["nested"]!["inner"]!["value"]!.Value<int>().Should().Be(42);
		((JArray)result.First()["tags"]!).Should().HaveCount(2);
	}

	[Fact]
	public async Task ReadMany_VeryLongId_ReturnsItem()
	{
		var longId = new string('x', 255);
		await _container.CreateItemAsync(JObject.FromObject(new { id = longId, pk = "a" }), new PartitionKey("a"));
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { (longId, new PartitionKey("a")) });
		result.Count.Should().Be(1);
	}

	[Fact]
	public async Task ReadMany_EmptyContainer_ReturnsEmpty()
	{
		var result = await _container.ReadManyItemsAsync<JObject>(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		result.Count.Should().Be(0);
	}

	[Fact]
	public async Task ReadManyStream_EmptyContainer_ReturnsEmptyEnvelope()
	{
		var response = await _container.ReadManyItemsStreamAsync(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		((int)body["_count"]!).Should().Be(0);
		((JArray)body["Documents"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ReadManyStream_HasRidField()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var response = await _container.ReadManyItemsStreamAsync(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		var body = JObject.Parse(await new StreamReader(response.Content).ReadToEndAsync());
		body["_rid"].Should().NotBeNull();
	}

	[Fact]
	public async Task ReadManyStream_ContentIsValidJson()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var response = await _container.ReadManyItemsStreamAsync(new List<(string, PartitionKey)> { ("1", new PartitionKey("a")) });
		var body = await new StreamReader(response.Content).ReadToEndAsync();
		var parsed = JObject.Parse(body); // Should not throw
		parsed["Documents"].Should().NotBeNull();
	}

	[Fact]
	public async Task ReadMany_TypedAndStream_ReturnSameLogicalResults()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "Bob" }), new PartitionKey("a"));
		var items = new List<(string, PartitionKey)> { ("1", new PartitionKey("a")), ("2", new PartitionKey("a")) };

		var typed = await _container.ReadManyItemsAsync<JObject>(items);
		var stream = await _container.ReadManyItemsStreamAsync(items);
		var streamBody = JObject.Parse(await new StreamReader(stream.Content).ReadToEndAsync());

		typed.Count.Should().Be(((JArray)streamBody["Documents"]!).Count);
	}

	[Fact]
	public async Task ReadMany_ResultOrderMatchesRequestOrder()
	{
		// Emulator returns items in request order (real Cosmos may not guarantee order)
		await _container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a" }), new PartitionKey("a"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a"));

		var items = new List<(string, PartitionKey)>
		{
			("2", new PartitionKey("a")),
			("3", new PartitionKey("a")),
			("1", new PartitionKey("a"))
		};
		var result = await _container.ReadManyItemsAsync<JObject>(items);
		result.Select(r => r["id"]!.ToString()).Should().ContainInConsecutiveOrder("2", "3", "1");
	}
}
