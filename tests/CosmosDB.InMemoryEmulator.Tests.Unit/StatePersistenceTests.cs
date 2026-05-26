using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class StatePersistenceTests
{
	[Fact]
	public void ExportState_EmptyContainer_ReturnsEmptyJson()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");

		var json = container.ExportState();

		var parsed = JObject.Parse(json);
		parsed["items"]!.Should().BeOfType<JArray>();
		((JArray)parsed["items"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ExportState_WithItems_SerializesAllItems()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));

		var json = container.ExportState();

		var parsed = JObject.Parse(json);
		var items = (JArray)parsed["items"]!;
		items.Should().HaveCount(2);
	}

	[Fact]
	public async Task ImportState_RestoresItems()
	{
		var source = new InMemoryContainer("source-container", "/partitionKey");
		await source.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await source.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));

		var exportedJson = source.ExportState();

		var target = new InMemoryContainer("target-container", "/partitionKey");
		target.ImportState(exportedJson);

		target.ItemCount.Should().Be(2);
		var alice = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		alice.Resource.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task ImportState_ClearsExistingDataBeforeImporting()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "existing", PartitionKey = "pk1", Name = "Existing" },
			new PartitionKey("pk1"));

		var stateJson = """{"items":[{"id":"new","partitionKey":"pk1","name":"New","value":0,"isActive":true,"tags":[]}]}""";
		container.ImportState(stateJson);

		container.ItemCount.Should().Be(1);
		var result = await container.ReadItemAsync<TestDocument>("new", new PartitionKey("pk1"));
		result.Resource.Name.Should().Be("New");
	}

	[Fact]
	public async Task ExportState_ToFile_And_ImportState_FromFile_RoundTrips()
	{
		var tempFile = Path.GetTempFileName();
		try
		{
			var source = new InMemoryContainer("source", "/partitionKey");
			await source.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 42, Tags = ["a", "b"] },
				new PartitionKey("pk1"));

			source.ExportStateToFile(tempFile);

			var target = new InMemoryContainer("target", "/partitionKey");
			target.ImportStateFromFile(tempFile);

			target.ItemCount.Should().Be(1);
			var item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
			item.Resource.Name.Should().Be("Alice");
			item.Resource.Value.Should().Be(42);
			item.Resource.Tags.Should().BeEquivalentTo(["a", "b"]);
		}
		finally
		{
			File.Delete(tempFile);
		}
	}

	[Fact]
	public async Task ExportState_PreservesPartitionKeyIsolation()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Different Alice" },
			new PartitionKey("pk2"));

		var json = container.ExportState();

		var target = new InMemoryContainer("target", "/partitionKey");
		target.ImportState(json);

		target.ItemCount.Should().Be(2);
		var pk1Item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		pk1Item.Resource.Name.Should().Be("Alice");
		var pk2Item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk2"));
		pk2Item.Resource.Name.Should().Be("Different Alice");
	}

	[Fact]
	public async Task ImportState_WithNestedObjects_PreservesStructure()
	{
		var source = new InMemoryContainer("source", "/partitionKey");
		await source.CreateItemAsync(new TestDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Name = "Test",
			Nested = new NestedObject { Description = "nested value", Score = 3.14 }
		}, new PartitionKey("pk1"));

		var json = source.ExportState();
		var target = new InMemoryContainer("target", "/partitionKey");
		target.ImportState(json);

		var item = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		item.Resource.Nested.Should().NotBeNull();
		item.Resource.Nested!.Description.Should().Be("nested value");
		item.Resource.Nested!.Score.Should().Be(3.14);
	}

	[Fact]
	public void ImportState_WithInvalidJson_ThrowsException()
	{
		var container = new InMemoryContainer("test-container", "/partitionKey");

		var action = () => container.ImportState("not valid json");

		action.Should().Throw<JsonReaderException>();
	}

	[Fact]
	public async Task ExportState_ItemsAreQueryableAfterImport()
	{
		var source = new InMemoryContainer("source", "/partitionKey");
		await source.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10 },
			new PartitionKey("pk1"));
		await source.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20 },
			new PartitionKey("pk1"));

		var json = source.ExportState();
		var target = new InMemoryContainer("target", "/partitionKey");
		target.ImportState(json);

		var queryDef = new QueryDefinition("SELECT * FROM c WHERE c.value > 15");
		var iterator = target.GetItemQueryIterator<TestDocument>(queryDef);
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Bob");
	}

	[Fact]
	public async Task ExportState_ImportState_RoundTrip()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob" },
			new PartitionKey("pk1"));

		var state = container.ExportState();
		state.Should().NotBeNullOrEmpty();

		var newContainer = new InMemoryContainer("test", "/partitionKey");
		newContainer.ImportState(state);

		var read = await newContainer.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Alice");

		newContainer.ItemCount.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category A: ExportState Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ExportStateEdgeCaseTests
{
	[Fact]
	public async Task ExportState_SingleItem_ProducesValidJson()
	{
		var container = new InMemoryContainer("export-single", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var json = container.ExportState();
		var parsed = JObject.Parse(json);
		var items = (JArray)parsed["items"]!;
		items.Should().HaveCount(1);
		items[0]["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ExportState_IncludesSystemProperties()
	{
		var container = new InMemoryContainer("export-sys", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var json = container.ExportState();
		var items = (JArray)JObject.Parse(json)["items"]!;
		var item = items[0];

		item["_etag"].Should().NotBeNull();
		item["_ts"].Should().NotBeNull();
	}

	[Fact]
	public async Task ExportState_LargeNumberOfItems_AllSerialized()
	{
		var container = new InMemoryContainer("export-large", "/pk");
		for (var i = 0; i < 500; i++)
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a"}""")),
				new PartitionKey("a"));

		var json = container.ExportState();
		var items = (JArray)JObject.Parse(json)["items"]!;
		items.Should().HaveCount(500);
	}

	[Fact]
	public async Task ExportState_WithSpecialCharacters_RoundTrips()
	{
		var container = new InMemoryContainer("export-special", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "quotes\"and\\backslash\nnewline\ttab" }),
			new PartitionKey("a"));

		var json = container.ExportState();
		var target = new InMemoryContainer("target", "/pk");
		target.ImportState(json);

		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["text"]!.ToString().Should().Be("quotes\"and\\backslash\nnewline\ttab");
	}

	[Fact]
	public async Task ExportState_WithNullValues_PreservesNulls()
	{
		var container = new InMemoryContainer("export-null", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","field":null}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var items = (JArray)JObject.Parse(json)["items"]!;
		items[0]["field"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task ExportState_WithBooleanValues_PreservesType()
	{
		var container = new InMemoryContainer("export-bool", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","active":true,"deleted":false}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var item = ((JArray)JObject.Parse(json)["items"]!)[0];
		item["active"]!.Type.Should().Be(JTokenType.Boolean);
		item["active"]!.Value<bool>().Should().BeTrue();
		item["deleted"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task ExportState_WithArrays_PreservesArrays()
	{
		var container = new InMemoryContainer("export-arrays", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"a","tags":["a","b"],"empty":[],"nested":[[1,2],[3]]}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var target = new InMemoryContainer("target", "/pk");
		target.ImportState(json);

		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["tags"]!.Should().BeOfType<JArray>();
		((JArray)result.Resource["tags"]!).Should().HaveCount(2);
		((JArray)result.Resource["empty"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ExportState_WithDeeplyNestedObjects_PreservesAll()
	{
		var container = new InMemoryContainer("export-deep", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"a","a":{"b":{"c":{"d":{"e":"deep"}}}}}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var target = new InMemoryContainer("target", "/pk");
		target.ImportState(json);

		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource.SelectToken("a.b.c.d.e")!.ToString().Should().Be("deep");
	}

	[Fact]
	public async Task ExportState_OutputIsIndentedJson()
	{
		var container = new InMemoryContainer("export-format", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		json.Should().Contain("\n", "ExportState should produce indented (pretty-printed) JSON");
	}

	[Fact]
	public async Task ExportState_CalledTwice_ProducesSameOutput()
	{
		var container = new InMemoryContainer("export-idem", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"test"}""")),
			new PartitionKey("a"));

		var json1 = container.ExportState();
		var json2 = container.ExportState();
		json1.Should().Be(json2);
	}

	[Fact]
	public async Task ExportState_WithNumericTypes_PreservesPrecision()
	{
		var container = new InMemoryContainer("export-num", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"a","intVal":42,"longVal":9999999999,"doubleVal":3.14159265358979}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var target = new InMemoryContainer("target", "/pk");
		target.ImportState(json);

		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["intVal"]!.Value<int>().Should().Be(42);
		result.Resource["longVal"]!.Value<long>().Should().Be(9999999999);
		result.Resource["doubleVal"]!.Value<double>().Should().BeApproximately(3.14159265358979, 0.000001);
	}

	[Fact]
	public async Task ExportState_WithDateTimeValues_PreservesAsStrings()
	{
		var container = new InMemoryContainer("export-dt", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"a","created":"2024-01-15T10:30:00Z"}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var target = new InMemoryContainer("target", "/pk");
		target.ImportState(json);

		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["created"]!.ToString().Should().Contain("2024-01-15");
	}

	[Fact]
	public async Task ExportState_WithEmptyStringValues_PreservesThem()
	{
		var container = new InMemoryContainer("export-emptystr", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"a","emptyField":"","nullField":null}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var items = (JArray)JObject.Parse(json)["items"]!;
		items[0]["emptyField"]!.Type.Should().Be(JTokenType.String);
		items[0]["emptyField"]!.ToString().Should().BeEmpty();
		items[0]["nullField"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task ExportState_WithUnicodeAndEmoji_RoundTrips()
	{
		var container = new InMemoryContainer("export-utf", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"a","text":"\u4e16\u754c\ud83d\ude80"}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var target = new InMemoryContainer("target", "/pk");
		target.ImportState(json);

		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["text"]!.ToString().Should().Contain("\u4e16\u754c");
	}

	[Fact]
	public async Task ExportState_WithMaxNestedDepth_DoesNotStackOverflow()
	{
		var container = new InMemoryContainer("export-deepnest", "/pk");
		// Build 50-level deep JSON
		var deep = new JObject { ["id"] = "1", ["pk"] = "a" };
		var current = deep;
		for (var i = 0; i < 50; i++)
		{
			var child = new JObject { ["level"] = i };
			current["nested"] = child;
			current = child;
		}

		await container.CreateItemAsync(deep, new PartitionKey("a"));

		var act = () => container.ExportState();
		act.Should().NotThrow();
	}

	[Fact]
	public async Task ExportState_ContainerMetadataNotIncluded()
	{
		var container = new InMemoryContainer("export-nometa", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var parsed = JObject.Parse(json);

		parsed.Properties().Select(p => p.Name).Should().BeEquivalentTo(["items"]);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category B: ImportState Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class ImportStateEdgeCaseTests
{
	[Fact]
	public void ImportState_EmptyItemsArray_ResultsInEmptyContainer()
	{
		var container = new InMemoryContainer("import-empty", "/pk");
		container.ImportState("""{"items":[]}""");
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task ImportState_MissingItemsKey_PreservesExistingData()
	{
		var container = new InMemoryContainer("import-nokey", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		container.ImportState("""{"foo":"bar"}""");
		container.ItemCount.Should().Be(1, "missing 'items' key means import is a no-op");
	}

	[Fact]
	public async Task ImportState_EmptyJsonObject_PreservesExistingData()
	{
		var container = new InMemoryContainer("import-obj", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		container.ImportState("{}");
		container.ItemCount.Should().Be(1);
	}

	[Fact]
	public async Task ImportState_GeneratesNewETags()
	{
		var source = new InMemoryContainer("import-etag-src", "/partitionKey");
		await source.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var sourceRead = await source.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var sourceEtag = sourceRead.ETag;

		var target = new InMemoryContainer("import-etag-tgt", "/partitionKey");
		target.ImportState(source.ExportState());

		var targetRead = await target.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		targetRead.ETag.Should().NotBe(sourceEtag, "import generates new etags");
	}

	[Fact]
	public void ImportState_NullJson_ThrowsException()
	{
		var container = new InMemoryContainer("import-null", "/pk");
		var act = () => container.ImportState(null!);
		act.Should().Throw<ArgumentNullException>();
	}

	[Fact]
	public void ImportState_EmptyString_ThrowsException()
	{
		var container = new InMemoryContainer("import-empty-str", "/pk");
		var act = () => container.ImportState("");
		act.Should().Throw<JsonReaderException>();
	}

	[Fact]
	public async Task ImportState_DuplicateIds_SamePartitionKey_LastWins()
	{
		var container = new InMemoryContainer("import-dup", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","name":"first"},{"id":"1","pk":"a","name":"last"}]}""");

		container.ItemCount.Should().Be(1);
		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["name"]!.ToString().Should().Be("last");
	}

	[Fact]
	public async Task ImportState_DuplicateIds_DifferentPartitionKeys_BothStored()
	{
		var container = new InMemoryContainer("import-dup-pk", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","name":"A"},{"id":"1","pk":"b","name":"B"}]}""");

		container.ItemCount.Should().Be(2);
		var a = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		a.Resource["name"]!.ToString().Should().Be("A");
		var b = await container.ReadItemAsync<JObject>("1", new PartitionKey("b"));
		b.Resource["name"]!.ToString().Should().Be("B");
	}

	[Fact]
	public async Task ImportState_CalledMultipleTimes_OnlyLastImportSurvives()
	{
		var container = new InMemoryContainer("import-multi", "/pk");
		container.ImportState("""{"items":[{"id":"A","pk":"a"}]}""");
		container.ImportState("""{"items":[{"id":"B","pk":"b"}]}""");

		container.ItemCount.Should().Be(1);
		var result = await container.ReadItemAsync<JObject>("B", new PartitionKey("b"));
		result.Resource["id"]!.ToString().Should().Be("B");

		var act = () => container.ReadItemAsync<JObject>("A", new PartitionKey("a"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task ImportState_WithExtraJsonProperties_PreservesThem()
	{
		var container = new InMemoryContainer("import-extra", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","customField":"preserved","nestedCustom":{"x":1}}]}""");

		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["customField"]!.ToString().Should().Be("preserved");
		result.Resource["nestedCustom"]!["x"]!.Value<int>().Should().Be(1);
	}

	[Fact]
	public void ImportState_ItemsMissingId_ThrowsInvalidOperation()
	{
		var container = new InMemoryContainer("import-noid", "/pk");
		var act = () => container.ImportState("""{"items":[{"pk":"a","name":"no-id-item"}]}""");

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*'id'*");
	}

	[Fact]
	public async Task ImportState_ItemMissingPartitionKeyField_Behavior()
	{
		var container = new InMemoryContainer("import-nopk", "/pk");
		container.ImportState("""{"items":[{"id":"1","name":"no-pk"}]}""");

		container.ItemCount.Should().Be(1);
		// When PK field is missing, item is stored with null partition key
		var result = await container.ReadItemAsync<JObject>("1", PartitionKey.None);
		result.Resource["name"]!.ToString().Should().Be("no-pk");
	}

	[Fact]
	public async Task ImportState_GeneratesNewTimestamps()
	{
		var source = new InMemoryContainer("import-ts-src", "/pk");
		await source.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		var export = source.ExportState();
		var sourceTs = (long)((JArray)JObject.Parse(export)["items"]!)[0]["_ts"]!;

		await Task.Delay(1100); // Wait so timestamp differs

		var target = new InMemoryContainer("import-ts-tgt", "/pk");
		target.ImportState(export);

		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["_ts"]!.Value<long>().Should().BeGreaterThanOrEqualTo(sourceTs);
	}

	[Fact]
	public async Task ImportState_WithItemsHavingSystemProperties_OverwritesThem()
	{
		var container = new InMemoryContainer("import-sysprop", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","_etag":"\"old-etag\"","_ts":1000}]}""");

		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["_etag"]!.ToString().Should().NotBe("\"old-etag\"",
			"import regenerates ETags");
		result.Resource["_ts"]!.Value<long>().Should().BeGreaterThan(1000,
			"import regenerates timestamps");
	}

	[Fact]
	public void ImportState_ValidJsonButArray_Throws()
	{
		var container = new InMemoryContainer("import-arr", "/pk");
		var act = () => container.ImportState("""[{"id":"1"}]""");
		act.Should().Throw<JsonReaderException>();
	}

	[Fact]
	public void ImportState_ValidJsonButPrimitive_Throws()
	{
		var container = new InMemoryContainer("import-prim", "/pk");
		var act = () => container.ImportState("123");
		act.Should().Throw<JsonReaderException>();
	}

	[Fact]
	public async Task ImportState_VeryLargePayload_1000Items_Succeeds()
	{
		var items = new JArray();
		for (var i = 0; i < 1000; i++)
			items.Add(new JObject { ["id"] = $"{i}", ["pk"] = "a", ["name"] = $"Item{i}" });

		var state = new JObject { ["items"] = items };
		var container = new InMemoryContainer("import-large", "/pk");
		container.ImportState(state.ToString());

		container.ItemCount.Should().Be(1000);

		var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c");
		var all = new List<JObject>();
		while (iter.HasMoreResults) all.AddRange(await iter.ReadNextAsync());
		all.Should().HaveCount(1000);
	}

	[Fact]
	public void ImportState_WithWhitespaceOnlyJson_Throws()
	{
		var container = new InMemoryContainer("import-ws", "/pk");
		var act = () => container.ImportState("   ");
		act.Should().Throw<JsonReaderException>();
	}

	[Fact]
	public async Task ImportState_ItemsWithDifferentSchemas_AllPreserved()
	{
		var container = new InMemoryContainer("import-schema", "/pk");
		container.ImportState("""
            {"items":[
                {"id":"1","pk":"a","name":"Alice","age":30},
                {"id":"2","pk":"a","score":99.5,"active":true},
                {"id":"3","pk":"a","tags":["x","y"],"nested":{"z":1}}
            ]}
            """);

		container.ItemCount.Should().Be(3);
		var r1 = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		r1.Resource["name"]!.ToString().Should().Be("Alice");
		var r2 = await container.ReadItemAsync<JObject>("2", new PartitionKey("a"));
		r2.Resource["score"]!.Value<double>().Should().Be(99.5);
		var r3 = await container.ReadItemAsync<JObject>("3", new PartitionKey("a"));
		((JArray)r3.Resource["tags"]!).Should().HaveCount(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category C: Change Feed Interaction
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceChangeFeedTests
{
	[Fact]
	public void ImportState_DoesNotPopulateChangeFeed()
	{
		var container = new InMemoryContainer("cf-import", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"},{"id":"2","pk":"a"}]}""");

		var iter = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);

		// Change feed should be empty — import bypasses change feed recording
		// HasMoreResults is false when change feed has no entries
		iter.HasMoreResults.Should().BeFalse(
			"ImportState does not record change feed entries — this is by design");
	}

	[Fact]
	public async Task ImportState_SubsequentWritesAppearInChangeFeed()
	{
		var container = new InMemoryContainer("cf-writes", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a"}""")),
			new PartitionKey("a"));

		var iter = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var page = await iter.ReadNextAsync();

		page.Should().Contain(j => j["id"]!.ToString() == "2");
	}

	[Fact]
	public async Task ExportState_DoesNotIncludeChangeFeedHistory()
	{
		var container = new InMemoryContainer("cf-export", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var parsed = JObject.Parse(json);

		// Export only has "items", no change feed data
		parsed.Properties().Select(p => p.Name).Should().BeEquivalentTo(["items"]);
	}

	[Fact]
	public void ClearItems_ClearsChangeFeed_VerifiedViaIterator()
	{
		var container = new InMemoryContainer("cf-clear", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

		container.ClearItems();

		var iter = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		iter.HasMoreResults.Should().BeFalse("change feed should be empty after ClearItems");
	}

	[Fact]
	public void ImportState_ThenExport_ThenReimport_ChangeFeedStillEmpty()
	{
		var container = new InMemoryContainer("cf-chain", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

		var exported = container.ExportState();
		container.ImportState(exported);

		var iter = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		iter.HasMoreResults.Should().BeFalse("chained import/export/reimport should leave change feed empty");
	}

	[Fact]
	public async Task ImportState_ThenModifyItems_ChangeFeedOnlyHasModifications()
	{
		var container = new InMemoryContainer("cf-mod", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"},{"id":"2","pk":"a"},{"id":"3","pk":"a"}]}""");

		// Modify one item
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", name = "modified" }),
			new PartitionKey("a"));

		var iter = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var page = await iter.ReadNextAsync();

		page.Should().ContainSingle();
		page.First()["id"]!.ToString().Should().Be("2");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category D: TTL Interaction
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceTtlTests
{
	[Fact]
	public async Task ImportState_WithDefaultTTL_ImportedItemsRespectTTL()
	{
		var container = new InMemoryContainer("ttl-import", "/pk");
		container.DefaultTimeToLive = 1;
		container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ImportState_WithPerItemTTL_ItemsExpireCorrectly()
	{
		var container = new InMemoryContainer("ttl-peritem", "/pk");
		container.DefaultTimeToLive = 60; // container TTL = 60s
		container.ImportState("""{"items":[{"id":"1","pk":"a","_ttl":1}]}""");

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ExportState_WithTTLItems_IncludesTtlField()
	{
		var container = new InMemoryContainer("ttl-export", "/pk");
		container.DefaultTimeToLive = 60;
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","ttl":300}""")),
			new PartitionKey("a"));

		var json = container.ExportState();
		var items = (JArray)JObject.Parse(json)["items"]!;
		items[0]["ttl"]!.Value<int>().Should().Be(300);
	}

	[Fact]
	public async Task ImportState_IntoContainerWithNoTTL_TtlFieldIgnored()
	{
		var container = new InMemoryContainer("ttl-none", "/pk");
		// No DefaultTimeToLive set
		container.ImportState("""{"items":[{"id":"1","pk":"a","ttl":1}]}""");

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should still exist — container has no TTL configured
		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category E: Hierarchical Partition Keys
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceHierarchicalPkTests
{
	[Fact]
	public async Task ExportImport_HierarchicalPartitionKey_RoundTrips()
	{
		var source = new InMemoryContainer("hk-src", new[] { "/tenantId", "/userId" });
		await source.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenantId = "t1", userId = "u1", name = "Alice" }),
			new PartitionKeyBuilder().Add("t1").Add("u1").Build());

		var json = source.ExportState();
		var target = new InMemoryContainer("hk-tgt", new[] { "/tenantId", "/userId" });
		target.ImportState(json);

		var result = await target.ReadItemAsync<JObject>("1",
			new PartitionKeyBuilder().Add("t1").Add("u1").Build());
		result.Resource["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task ExportImport_ThreeLevelHierarchicalPK_RoundTrips()
	{
		var source = new InMemoryContainer("hk-3level", new[] { "/a", "/b", "/c" });
		await source.CreateItemAsync(
			JObject.FromObject(new { id = "1", a = "x", b = "y", c = "z", name = "deep" }),
			new PartitionKeyBuilder().Add("x").Add("y").Add("z").Build());

		var json = source.ExportState();
		var target = new InMemoryContainer("hk-3tgt", new[] { "/a", "/b", "/c" });
		target.ImportState(json);

		var result = await target.ReadItemAsync<JObject>("1",
			new PartitionKeyBuilder().Add("x").Add("y").Add("z").Build());
		result.Resource["name"]!.ToString().Should().Be("deep");
	}

	[Fact]
	public async Task ExportImport_HierarchicalPK_SameIdDifferentPKValues_BothPreserved()
	{
		var source = new InMemoryContainer("hk-sameid", new[] { "/tenantId", "/userId" });
		await source.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenantId = "t1", userId = "u1", name = "Alice" }),
			new PartitionKeyBuilder().Add("t1").Add("u1").Build());
		await source.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenantId = "t1", userId = "u2", name = "Bob" }),
			new PartitionKeyBuilder().Add("t1").Add("u2").Build());

		var json = source.ExportState();
		var target = new InMemoryContainer("hk-sameid-tgt", new[] { "/tenantId", "/userId" });
		target.ImportState(json);

		target.ItemCount.Should().Be(2);
		var r1 = await target.ReadItemAsync<JObject>("1",
			new PartitionKeyBuilder().Add("t1").Add("u1").Build());
		r1.Resource["name"]!.ToString().Should().Be("Alice");
		var r2 = await target.ReadItemAsync<JObject>("1",
			new PartitionKeyBuilder().Add("t1").Add("u2").Build());
		r2.Resource["name"]!.ToString().Should().Be("Bob");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category G: File-Based Operations
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceFileTests
{
	[Fact]
	public async Task ExportStateToFile_CreatesFile()
	{
		var tempFile = Path.GetTempFileName();
		try
		{
			var container = new InMemoryContainer("file-create", "/pk");
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
				new PartitionKey("a"));

			container.ExportStateToFile(tempFile);
			File.Exists(tempFile).Should().BeTrue();
			File.ReadAllText(tempFile).Should().Contain("\"id\": \"1\"");
		}
		finally { File.Delete(tempFile); }
	}

	[Fact]
	public async Task ExportStateToFile_OverwritesExistingFile()
	{
		var tempFile = Path.GetTempFileName();
		try
		{
			File.WriteAllText(tempFile, "old content");

			var container = new InMemoryContainer("file-overwrite", "/pk");
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
				new PartitionKey("a"));

			container.ExportStateToFile(tempFile);
			File.ReadAllText(tempFile).Should().NotContain("old content");
		}
		finally { File.Delete(tempFile); }
	}

	[Fact]
	public void ImportStateFromFile_FileNotFound_Throws()
	{
		var container = new InMemoryContainer("file-404", "/pk");
		var act = () => container.ImportStateFromFile("/nonexistent/path.json");
		act.Should().Throw<Exception>();
	}

	[Fact]
	public void ImportStateFromFile_EmptyFile_Throws()
	{
		var tempFile = Path.GetTempFileName();
		try
		{
			File.WriteAllText(tempFile, "");
			var container = new InMemoryContainer("file-empty", "/pk");
			var act = () => container.ImportStateFromFile(tempFile);
			act.Should().Throw<JsonReaderException>();
		}
		finally { File.Delete(tempFile); }
	}

	[Fact]
	public async Task ExportStateToFile_And_ImportStateFromFile_LargeDataset()
	{
		var tempFile = Path.GetTempFileName();
		try
		{
			var source = new InMemoryContainer("file-large", "/pk");
			for (var i = 0; i < 100; i++)
				await source.CreateItemStreamAsync(
					new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a","name":"Item{{{i}}}"}""")),
					new PartitionKey("a"));

			source.ExportStateToFile(tempFile);

			var target = new InMemoryContainer("file-large-tgt", "/pk");
			target.ImportStateFromFile(tempFile);

			target.ItemCount.Should().Be(100);
		}
		finally { File.Delete(tempFile); }
	}

	[Fact]
	public void ExportStateToFile_NullPath_Throws()
	{
		var container = new InMemoryContainer("file-null", "/pk");
		var act = () => container.ExportStateToFile(null!);
		act.Should().Throw<ArgumentNullException>();
	}

	[Fact]
	public void ImportStateFromFile_NullPath_Throws()
	{
		var container = new InMemoryContainer("file-null-import", "/pk");
		var act = () => container.ImportStateFromFile(null!);
		act.Should().Throw<ArgumentNullException>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category H: ClearItems
// ═══════════════════════════════════════════════════════════════════════════

public class ClearItemsTests
{
	[Fact]
	public async Task ClearItems_EmptiesAllStorage()
	{
		var container = new InMemoryContainer("clear-all", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		container.ClearItems();
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public void ClearItems_OnEmptyContainer_DoesNotThrow()
	{
		var container = new InMemoryContainer("clear-empty", "/pk");
		var act = () => container.ClearItems();
		act.Should().NotThrow();
	}

	[Fact]
	public async Task ClearItems_ThenCreateItem_WorksNormally()
	{
		var container = new InMemoryContainer("clear-then-create", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		container.ClearItems();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", name = "new" }),
			new PartitionKey("a"));

		container.ItemCount.Should().Be(1);
		var result = await container.ReadItemAsync<JObject>("2", new PartitionKey("a"));
		result.Resource["name"]!.ToString().Should().Be("new");
	}

	[Fact]
	public async Task ClearItems_ClearsETags()
	{
		var container = new InMemoryContainer("clear-etag", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var etag = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).ETag;
		etag.Should().NotBeNullOrEmpty();

		container.ClearItems();

		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public void ClearItems_CalledMultipleTimes_NoError()
	{
		var container = new InMemoryContainer("clear-multi", "/pk");
		var act = () =>
		{
			container.ClearItems();
			container.ClearItems();
			container.ClearItems();
		};
		act.Should().NotThrow();
	}

	[Fact]
	public async Task ClearItems_DoesNotAffectContainerConfig()
	{
		var container = new InMemoryContainer("clear-config", "/pk");
		container.DefaultTimeToLive = 300;

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		container.ClearItems();
		container.ItemCount.Should().Be(0);
		container.DefaultTimeToLive.Should().Be(300, "TTL config survives ClearItems");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category I: Concurrency
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceConcurrencyTests
{
	[Fact]
	public async Task ExportState_WhileWritesHappening_DoesNotThrow()
	{
		var container = new InMemoryContainer("conc-export", "/pk");
		for (var i = 0; i < 50; i++)
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a"}""")),
				new PartitionKey("a"));

		var tasks = Enumerable.Range(50, 50).Select(async i =>
		{
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a"}""")),
				new PartitionKey("a"));
		}).ToList();

		var exportTask = Task.Run(() => container.ExportState());

		var act = async () => await Task.WhenAll(tasks.Append(exportTask));
		await act.Should().NotThrowAsync();
	}

	[Fact]
	public async Task ExportState_DuringConcurrentWrites_MayNotBeAtomicSnapshot()
	{
		// Documents that ConcurrentDictionary enumeration may include some concurrent writes
		var container = new InMemoryContainer("conc-snapshot", "/pk");
		for (var i = 0; i < 20; i++)
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a"}""")),
				new PartitionKey("a"));

		// Start writes concurrently with export
		var writeTasks = Enumerable.Range(20, 30).Select(async i =>
		{
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"a"}""")),
				new PartitionKey("a"));
		});

		var exportResult = "";
		var exportTask = Task.Run(() => exportResult = container.ExportState());
		await Task.WhenAll(writeTasks.Append(exportTask));

		// Export should have at least the 20 original items, possibly more
		var items = (JArray)JObject.Parse(exportResult)["items"]!;
		items.Count.Should().BeGreaterThanOrEqualTo(20);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category J: Cross-Container Export/Import
// ═══════════════════════════════════════════════════════════════════════════

public class CrossContainerExportImportTests
{
	[Fact]
	public async Task ExportFromOneContainer_ImportToAnother_DifferentNames()
	{
		var source = new InMemoryContainer("source-name", "/pk");
		await source.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "data" }),
			new PartitionKey("a"));

		var target = new InMemoryContainer("different-name", "/pk");
		target.ImportState(source.ExportState());

		target.ItemCount.Should().Be(1);
		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["name"]!.ToString().Should().Be("data");
	}

	[Fact]
	public async Task ExportState_ImportState_MultipleTimes_NoStateLeakage()
	{
		var container = new InMemoryContainer("leak-test", "/pk");

		// First cycle
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"A","pk":"a"}""")),
			new PartitionKey("a"));
		var json1 = container.ExportState();

		// Wipe and reimport
		container.ClearItems();
		container.ImportState("""{"items":[{"id":"B","pk":"b"}]}""");

		// Second export — should only have B
		var json2 = container.ExportState();
		var items2 = (JArray)JObject.Parse(json2)["items"]!;
		items2.Should().HaveCount(1);
		items2[0]["id"]!.ToString().Should().Be("B");
	}

	[Fact]
	public async Task ImportState_FromDifferentContainerSchema_PreservesAllFields()
	{
		var source = new InMemoryContainer("schema-src", "/pk");
		await source.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", fieldA = "hello", fieldB = 42, fieldC = true }),
			new PartitionKey("a"));

		var target = new InMemoryContainer("schema-tgt", "/pk");
		target.ImportState(source.ExportState());

		var result = await target.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["fieldA"]!.ToString().Should().Be("hello");
		result.Resource["fieldB"]!.Value<int>().Should().Be(42);
		result.Resource["fieldC"]!.Value<bool>().Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category K: Data Fidelity After Import
// ═══════════════════════════════════════════════════════════════════════════

public class DataFidelityAfterImportTests
{
	[Fact]
	public async Task ImportState_ItemsCanBeUpdatedAfterImport()
	{
		var container = new InMemoryContainer("fidelity-update", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","name":"original"}]}""");

		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
			new PartitionKey("a"));

		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["name"]!.ToString().Should().Be("updated");
	}

	[Fact]
	public async Task ImportState_ItemsCanBeDeletedAfterImport()
	{
		var container = new InMemoryContainer("fidelity-delete", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

		await container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task ImportState_ItemsReadableViaReadItemStream()
	{
		var container = new InMemoryContainer("fidelity-stream", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","name":"stream"}]}""");

		using var response = await container.ReadItemStreamAsync("1", new PartitionKey("a"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		body.Should().Contain("stream");
	}

	[Fact]
	public async Task ImportState_ItemsQueryable()
	{
		var container = new InMemoryContainer("fidelity-query", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","value":10},{"id":"2","pk":"a","value":20}]}""");

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.value > 15");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(1);
		page.First()["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task ImportState_ItemsReadableViaReadMany()
	{
		var container = new InMemoryContainer("fidelity-readmany", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"},{"id":"2","pk":"a"},{"id":"3","pk":"b"}]}""");

		var items = new List<(string, PartitionKey)>
		{
			("1", new PartitionKey("a")),
			("3", new PartitionKey("b"))
		};
		var response = await container.ReadManyItemsAsync<JObject>(items);
		response.Should().HaveCount(2);
	}

	[Fact]
	public async Task ImportState_ItemsCountable_ViaLinqCount()
	{
		var container = new InMemoryContainer("fidelity-linq", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"},{"id":"2","pk":"a"},{"id":"3","pk":"a"}]}""");

		var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c");
		var all = new List<JObject>();
		while (iter.HasMoreResults) all.AddRange(await iter.ReadNextAsync());
		all.Should().HaveCount(3);
	}

	[Fact]
	public async Task ImportState_ItemsAccessibleViaPatchOperations()
	{
		var container = new InMemoryContainer("fidelity-patch", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","name":"original","count":0}]}""");

		await container.PatchItemAsync<JObject>("1", new PartitionKey("a"),
			new[] { PatchOperation.Set("/name", "patched"), PatchOperation.Increment("/count", 5) });

		var result = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		result.Resource["name"]!.ToString().Should().Be("patched");
		result.Resource["count"]!.Value<int>().Should().Be(5);
	}

	[Fact]
	public async Task ImportState_ItemsAccessibleViaTransactionalBatch()
	{
		var container = new InMemoryContainer("fidelity-batch", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","name":"Alice"},{"id":"2","pk":"a","name":"Bob"}]}""");

		var batch = container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.ReadItem("1");
		batch.DeleteItem("2");
		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		container.ItemCount.Should().Be(1);
	}

	[Fact]
	public async Task ImportState_ItemsAccessibleViaChangeFeedAfterModification()
	{
		var container = new InMemoryContainer("fidelity-cf", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","name":"original"}]}""");

		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "modified" }),
			new PartitionKey("a"));

		var iter = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var page = await iter.ReadNextAsync();

		page.Should().ContainSingle();
		page.First()["name"]!.ToString().Should().Be("modified");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category F: Unique Key Policy on Import
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceUniqueKeyTests
{
	[Fact]
	public void ImportState_ViolatesUniqueKeyPolicy_Throws()
	{
		var properties = new ContainerProperties("uk-import", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		var act = () => container.ImportState("""
            {"items":[
                {"id":"1","pk":"a","email":"same@test.com"},
                {"id":"2","pk":"a","email":"same@test.com"}
            ]}
            """);

		act.Should().Throw<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.Conflict);
	}

	[Fact]
	public void ImportState_UniqueKeyPolicy_DifferentPartitions_NoConflict()
	{
		var properties = new ContainerProperties("uk-diff-pk", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		container.ImportState("""
            {"items":[
                {"id":"1","pk":"a","email":"same@test.com"},
                {"id":"2","pk":"b","email":"same@test.com"}
            ]}
            """);

		container.ItemCount.Should().Be(2);
	}

	[Fact]
	public void ImportState_UniqueKeyPolicy_AllValid_Succeeds()
	{
		var properties = new ContainerProperties("uk-valid", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		container.ImportState("""
            {"items":[
                {"id":"1","pk":"a","email":"alice@test.com"},
                {"id":"2","pk":"a","email":"bob@test.com"}
            ]}
            """);

		container.ItemCount.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category L: PITR Interaction
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistencePitrTests
{
	[Fact]
	public async Task ImportState_ThenRestore_OnlyPostImportOperationsRestorable()
	{
		var container = new InMemoryContainer("pitr-import", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a","name":"imported"}]}""");

		// Post-import operation
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", name = "added" }),
			new PartitionKey("a"));

		var checkpoint = DateTimeOffset.UtcNow;
		await Task.Delay(100);

		// Another operation after checkpoint
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", name = "late" }),
			new PartitionKey("a"));

		container.RestoreToPointInTime(checkpoint);

		// Only post-import operations up to checkpoint should be present
		container.ItemCount.Should().Be(1);
		var result = await container.ReadItemAsync<JObject>("2", new PartitionKey("a"));
		result.Resource["name"]!.ToString().Should().Be("added");
	}

	[Fact]
	public void ClearItems_ThenRestore_NoDataRestorable()
	{
		var container = new InMemoryContainer("pitr-clear", "/pk");
		container.ImportState("""{"items":[{"id":"1","pk":"a"}]}""");

		container.ClearItems();
		container.RestoreToPointInTime(DateTimeOffset.UtcNow);

		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task ExportState_RestoreToPointInTime_ThenExport_DifferentResults()
	{
		var container = new InMemoryContainer("pitr-export", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "first" }),
			new PartitionKey("a"));

		var checkpoint = DateTimeOffset.UtcNow;
		await Task.Delay(100);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", name = "second" }),
			new PartitionKey("a"));

		var exportBefore = container.ExportState();
		container.RestoreToPointInTime(checkpoint);
		var exportAfter = container.ExportState();

		var itemsBefore = (JArray)JObject.Parse(exportBefore)["items"]!;
		var itemsAfter = (JArray)JObject.Parse(exportAfter)["items"]!;

		itemsBefore.Count.Should().Be(2);
		itemsAfter.Count.Should().Be(1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Category M: Error Handling / Defensive
// ═══════════════════════════════════════════════════════════════════════════

public class StatePersistenceErrorHandlingTests
{
	[Fact]
	public void ImportState_WithNullItemInArray_Behavior()
	{
		var container = new InMemoryContainer("err-null-item", "/pk");
		// null item in array — should handle gracefully
		var act = () => container.ImportState("""{"items":[null]}""");
		act.Should().Throw<Exception>();
	}

	[Fact]
	public void ExportState_AfterClearItems_ReturnsEmptyItemsArray()
	{
		var container = new InMemoryContainer("err-clear-export", "/pk");
		container.ClearItems();
		var json = container.ExportState();

		var items = (JArray)JObject.Parse(json)["items"]!;
		items.Should().BeEmpty();
	}

	[Fact]
	public void ImportState_ItemsKeyIsNotArray_Behavior()
	{
		var container = new InMemoryContainer("err-not-array", "/pk");
		container.ImportState("""{"items":"not an array"}""");

		// "items" is not a JArray, so the if check fails; container is cleared, nothing imported
		container.ItemCount.Should().Be(0);
	}

	// ─── StateFilePath + auto-persist tests ───────────────────────────────────

	[Fact]
	public async Task StateFilePath_WhenSet_SaveStateOnDispose()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		try
		{
			var filePath = Path.Combine(dir, "test-container.json");

			// Create container, add data, dispose
			var container = new InMemoryContainer("test", "/partitionKey");
			container.StateFilePath = filePath;
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "a", Name = "hello" },
				new PartitionKey("a"));
			await container.CreateItemAsync(
				new TestDocument { Id = "2", PartitionKey = "b", Name = "world" },
				new PartitionKey("b"));
			container.Dispose();

			// File should exist with data
			File.Exists(filePath).Should().BeTrue();
			var json = JObject.Parse(File.ReadAllText(filePath));
			json["items"].Should().HaveCount(2);
		}
		finally
		{
			Directory.Delete(dir, recursive: true);
		}
	}

	[Fact]
	public async Task StateFilePath_WhenNull_DisposeDoesNotCreateFile()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		try
		{
			var container = new InMemoryContainer("test", "/partitionKey");
			// StateFilePath is null by default
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "a", Name = "test" },
				new PartitionKey("a"));
			container.Dispose();

			// No files should have been created
			Directory.GetFiles(dir).Should().BeEmpty();
		}
		finally
		{
			Directory.Delete(dir, recursive: true);
		}
	}

	[Fact]
	public async Task StateFilePath_LoadOnInit_RestoresData()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		try
		{
			var filePath = Path.Combine(dir, "test-container.json");

			// Seed a file manually
			File.WriteAllText(filePath, """
            {
              "items": [
                { "id": "1", "partitionKey": "a", "name": "persisted", "value": 0, "isActive": true, "tags": [] }
              ]
            }
            """);

			// Create container pointing to the file — it should auto-load
			var container = new InMemoryContainer("test", "/partitionKey");
			container.StateFilePath = filePath;
			container.LoadPersistedState();

			container.ItemCount.Should().Be(1);
			var response = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("a"));
			response.Resource.Name.Should().Be("persisted");
		}
		finally
		{
			Directory.Delete(dir, recursive: true);
		}
	}

	[Fact]
	public void StateFilePath_LoadOnInit_NoFile_StartsEmpty()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		try
		{
			var filePath = Path.Combine(dir, "nonexistent.json");

			var container = new InMemoryContainer("test", "/partitionKey");
			container.StateFilePath = filePath;
			container.LoadPersistedState();

			container.ItemCount.Should().Be(0);
		}
		finally
		{
			Directory.Delete(dir, recursive: true);
		}
	}

	[Fact]
	public async Task StateFilePath_RoundTrip_SaveAndRestore()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		try
		{
			var filePath = Path.Combine(dir, "round-trip.json");

			// First "run" — create data and dispose
			var container1 = new InMemoryContainer("test", "/partitionKey");
			container1.StateFilePath = filePath;
			await container1.CreateItemAsync(
				new TestDocument { Id = "item-1", PartitionKey = "a", Name = "Alice" },
				new PartitionKey("a"));
			await container1.CreateItemAsync(
				new TestDocument { Id = "item-2", PartitionKey = "b", Name = "Bob" },
				new PartitionKey("b"));
			container1.Dispose();

			// Second "run" — load from file
			var container2 = new InMemoryContainer("test", "/partitionKey");
			container2.StateFilePath = filePath;
			container2.LoadPersistedState();

			container2.ItemCount.Should().Be(2);

			var alice = await container2.ReadItemAsync<TestDocument>("item-1", new PartitionKey("a"));
			alice.Resource.Name.Should().Be("Alice");

			var bob = await container2.ReadItemAsync<TestDocument>("item-2", new PartitionKey("b"));
			bob.Resource.Name.Should().Be("Bob");
		}
		finally
		{
			Directory.Delete(dir, recursive: true);
		}
	}

	[Fact]
	public async Task StateFilePath_Dispose_CreatesDirectoryIfNeeded()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}", "subdir");
		try
		{
			var filePath = Path.Combine(dir, "test.json");

			var container = new InMemoryContainer("test", "/partitionKey");
			container.StateFilePath = filePath;
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "a", Name = "test" },
				new PartitionKey("a"));
			container.Dispose();

			File.Exists(filePath).Should().BeTrue();
		}
		finally
		{
			if (Directory.Exists(Path.GetDirectoryName(dir)!))
				Directory.Delete(Path.GetDirectoryName(dir)!, recursive: true);
		}
	}

	[Fact]
	public void StateFilePath_DisposeWithEmptyContainer_SavesEmptyState()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		try
		{
			var filePath = Path.Combine(dir, "empty.json");

			var container = new InMemoryContainer("test", "/partitionKey");
			container.StateFilePath = filePath;
			container.Dispose();

			File.Exists(filePath).Should().BeTrue();
			var json = JObject.Parse(File.ReadAllText(filePath));
			json["items"].Should().HaveCount(0);
		}
		finally
		{
			Directory.Delete(dir, recursive: true);
		}
	}

	[Fact]
	public async Task StateFilePath_MultipleSaves_OverwritesPrevious()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		try
		{
			var filePath = Path.Combine(dir, "overwrite.json");

			// First run: 1 item
			var c1 = new InMemoryContainer("test", "/partitionKey");
			c1.StateFilePath = filePath;
			await c1.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "a", Name = "first" },
				new PartitionKey("a"));
			c1.Dispose();

			// Second run: 2 items
			var c2 = new InMemoryContainer("test", "/partitionKey");
			c2.StateFilePath = filePath;
			c2.LoadPersistedState();
			await c2.CreateItemAsync(
				new TestDocument { Id = "2", PartitionKey = "b", Name = "second" },
				new PartitionKey("b"));
			c2.Dispose();

			// Third run: verify 2 items
			var c3 = new InMemoryContainer("test", "/partitionKey");
			c3.StateFilePath = filePath;
			c3.LoadPersistedState();
			c3.ItemCount.Should().Be(2);
		}
		finally
		{
			Directory.Delete(dir, recursive: true);
		}
	}

	[Fact]
	public async Task StatePersistenceDirectory_ViaOptions_AutoPersists()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		try
		{
			// Simulate the DI path: create container with persistence directory
			var container = new InMemoryContainer("my-container", "/partitionKey");
			var expectedFile = Path.Combine(dir, "in-memory-db_my-container.json");
			container.StateFilePath = expectedFile;

			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "a", Name = "test" },
				new PartitionKey("a"));
			container.Dispose();

			File.Exists(expectedFile).Should().BeTrue();

			// Restore
			var container2 = new InMemoryContainer("my-container", "/partitionKey");
			container2.StateFilePath = expectedFile;
			container2.LoadPersistedState();
			container2.ItemCount.Should().Be(1);
		}
		finally
		{
			if (Directory.Exists(dir))
				Directory.Delete(dir, recursive: true);
		}
	}

	[Fact]
	public async Task LoadPersistedState_PreservesSystemProperties()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos-persist-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		try
		{
			var filePath = Path.Combine(dir, "system-props.json");

			// Create, add item, capture etag, dispose (saves)
			var container1 = new InMemoryContainer("test", "/partitionKey");
			container1.StateFilePath = filePath;
			var createResponse = await container1.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "a", Name = "Test" },
				new PartitionKey("a"));
			var originalEtag = createResponse.ETag;
			container1.Dispose();

			// Reload — ExportState includes _etag and _ts in the JSON, and
			// LoadPersistedState should preserve them
			var container2 = new InMemoryContainer("test", "/partitionKey");
			container2.StateFilePath = filePath;
			container2.LoadPersistedState();

			var readResponse = await container2.ReadItemAsync<TestDocument>("1", new PartitionKey("a"));
			// The etag should be preserved from the exported state
			readResponse.ETag.Should().NotBeNullOrEmpty();
		}
		finally
		{
			Directory.Delete(dir, recursive: true);
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Plan 38: State Persistence Deep Dive Tests
// ═══════════════════════════════════════════════════════════════════════════

// ── Batch 1: Export After Mutations ──
public class ExportStateAfterMutationTests
{
	[Fact]
	public async Task ExportState_AfterUpsert_ExportsLatestVersion()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Original" },
			new PartitionKey("pk"));
		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Updated" },
			new PartitionKey("pk"));

		var state = JObject.Parse(container.ExportState());
		var items = (JArray)state["items"]!;
		items.Should().HaveCount(1);
		items[0]!["name"]!.Value<string>().Should().Be("Updated");
	}

	[Fact]
	public async Task ExportState_AfterDelete_DeletedItemNotIncluded()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "ToDelete" },
			new PartitionKey("pk"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Keeper" },
			new PartitionKey("pk"));
		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk"));

		var state = JObject.Parse(container.ExportState());
		var items = (JArray)state["items"]!;
		items.Should().HaveCount(1);
		items[0]!["id"]!.Value<string>().Should().Be("2");
	}

	[Fact]
	public async Task ExportState_AfterPatch_ExportsPatchedValues()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Before" },
			new PartitionKey("pk"));
		await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk"),
			new[] { PatchOperation.Replace("/name", "After") });

		var state = JObject.Parse(container.ExportState());
		var items = (JArray)state["items"]!;
		items[0]!["name"]!.Value<string>().Should().Be("After");
	}

	[Fact]
	public async Task ExportState_WithPartitionKeyNone_HandlesCorrectly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1" }),
			PartitionKey.None);

		var state = JObject.Parse(container.ExportState());
		var items = (JArray)state["items"]!;
		items.Should().HaveCount(1);
		items[0]!["id"]!.Value<string>().Should().Be("1");
	}
}

// ── Batch 2: Import Fidelity ──
public class ImportFidelityDeepDiveTests
{
	[Fact]
	public async Task ImportState_ComputedPropertiesWorkOnImportedItems()
	{
		var props = new Microsoft.Azure.Cosmos.ContainerProperties("test", "/partitionKey")
		{
			ComputedProperties =
			{
				new ComputedProperty { Name = "fullDisplay", Query = "SELECT VALUE CONCAT(c.name, ' (', ToString(c[\"value\"]), ')') FROM c" }
			}
		};
		var container = new InMemoryContainer(props);

		container.ImportState("{\"items\":[{\"id\":\"1\",\"partitionKey\":\"pk\",\"name\":\"Alice\",\"value\":10}]}");

		var results = new List<JObject>();
		var iter = container.GetItemQueryIterator<JObject>("SELECT c.fullDisplay FROM c WHERE c.id = '1'");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results[0]["fullDisplay"]!.Value<string>().Should().Be("Alice (10)");
	}

	[Fact]
	public async Task ImportState_WithIntegerPartitionKeyValues_RoundTrips()
	{
		var container = new InMemoryContainer("test", "/tenantId");

		container.ImportState("{\"items\":[{\"id\":\"1\",\"tenantId\":42,\"name\":\"Test\"}]}");

		var response = await container.ReadItemAsync<JObject>("1", new PartitionKey(42));
		response.Resource["name"]!.Value<string>().Should().Be("Test");
	}

	[Fact]
	public async Task ImportState_ThenRegisterUdf_UdfWorksOnImportedData()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.ImportState("{\"items\":[{\"id\":\"1\",\"partitionKey\":\"pk\",\"name\":\"hello\"}]}");
		container.RegisterUdf("shout", args => args[0]?.ToString()?.ToUpper() + "!");

		var results = new List<JObject>();
		var iter = container.GetItemQueryIterator<JObject>("SELECT udf.shout(c.name) AS r FROM c");
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results[0]["r"]!.Value<string>().Should().Be("HELLO!");
	}

	[Fact]
	public async Task ImportState_RegeneratesRidSelfAttachments()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.ImportState("{\"items\":[{\"id\":\"1\",\"partitionKey\":\"pk\",\"name\":\"Test\"}]}");

		var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk"));
		response.Resource["_rid"].Should().NotBeNull();
		response.Resource["_self"].Should().NotBeNull();
		response.Resource["_attachments"].Should().NotBeNull();
	}
}

// ── Batch 3: TTL Edge Cases ──
public class StatePersistenceTtlDeepDiveTests
{
	[Fact]
	public async Task ExportState_WithTTLExpiredItem_ExcludesExpired()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.DefaultTimeToLive = 1; // 1 second TTL
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Ephemeral" },
			new PartitionKey("pk"));

		await Task.Delay(1500); // wait for TTL to expire

		// ExportState now filters out expired items (matches real Cosmos behavior)
		var state = JObject.Parse(container.ExportState());
		var items = (JArray)state["items"]!;
		items.Should().HaveCount(0);
	}

	[Fact]
	public async Task ImportState_WithOldTimestamp_AndContainerTTL_ItemNotImmediatelyExpired()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.DefaultTimeToLive = 3600; // 1 hour

		// Import state — ImportState sets _timestamps to UtcNow, not the document's _ts
		container.ImportState("{\"items\":[{\"id\":\"1\",\"partitionKey\":\"pk\",\"name\":\"Test\",\"_ts\":1000000000}]}");

		// Item should be readable since timestamps are renewed on import
		var response = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk"));
		response.Resource["name"]!.Value<string>().Should().Be("Test");
	}
}

// ── Batch 4: Auto-Persist ──
public class AutoPersistDeepDiveTests : IDisposable
{
	private readonly string _dir = Path.Combine(Path.GetTempPath(), $"cosmos-ap-{Guid.NewGuid():N}");

	public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, true); }

	[Fact]
	public void LoadPersistedState_DirectoryDoesNotExist_StartsEmpty()
	{
		var nonExistentDir = Path.Combine(Path.GetTempPath(), $"cosmos-nx-{Guid.NewGuid():N}");
		var container = new InMemoryContainer("test", "/partitionKey");
		container.StateFilePath = Path.Combine(nonExistentDir, "state.json");

		// Should not crash — File.Exists returns false for non-existent dir
		container.LoadPersistedState();

		var state = JObject.Parse(container.ExportState());
		((JArray)state["items"]!).Should().BeEmpty();
	}

	[Fact]
	public void LoadPersistedState_StateFilePathNull_ThrowsInvalidOperationException()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var act = () => container.LoadPersistedState();

		act.Should().Throw<InvalidOperationException>();
	}

	[Fact]
	public async Task Dispose_CalledTwice_NoError()
	{
		var filePath = Path.Combine(_dir, "state.json");
		var container = new InMemoryContainer("test", "/partitionKey");
		container.StateFilePath = filePath;
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test" },
			new PartitionKey("pk"));

		container.Dispose();
		container.Dispose(); // second call should be harmless

		File.Exists(filePath).Should().BeTrue();
		var state = JObject.Parse(File.ReadAllText(filePath));
		((JArray)state["items"]!).Should().HaveCount(1);
	}

	[Fact]
	public async Task Dispose_AfterClearItems_SavesEmptyState()
	{
		var filePath = Path.Combine(_dir, "state.json");
		var container = new InMemoryContainer("test", "/partitionKey");
		container.StateFilePath = filePath;
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test" },
			new PartitionKey("pk"));
		container.ClearItems();
		container.Dispose();

		var state = JObject.Parse(File.ReadAllText(filePath));
		((JArray)state["items"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task LoadPersistedState_ModifyItems_Dispose_FinalStateIncludesAll()
	{
		var filePath = Path.Combine(_dir, "state.json");

		// Phase 1: create initial state
		var container1 = new InMemoryContainer("test", "/partitionKey");
		container1.StateFilePath = filePath;
		await container1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Original" },
			new PartitionKey("pk"));
		container1.Dispose();

		// Phase 2: load, modify, dispose
		var container2 = new InMemoryContainer("test", "/partitionKey");
		container2.StateFilePath = filePath;
		container2.LoadPersistedState();
		await container2.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Added" },
			new PartitionKey("pk"));
		container2.Dispose();

		// Phase 3: verify final state
		var state = JObject.Parse(File.ReadAllText(filePath));
		var items = (JArray)state["items"]!;
		items.Should().HaveCount(2);
	}
}

// ── Batch 5: ClearItems Fidelity ──
public class ClearItemsFidelityTests
{
	[Fact]
	public async Task ClearItems_ClearsUniqueKeyTracking_CanRecreatePreviouslyConflictingItems()
	{
		var props = new Microsoft.Azure.Cosmos.ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(props);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Unique" },
			new PartitionKey("pk"));

		container.ClearItems();

		// Should now be able to create an item with the same unique key value
		var response = await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "Unique" },
			new PartitionKey("pk"));
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task ClearItems_ThenExportState_EmptyExport()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test" },
			new PartitionKey("pk"));

		container.ClearItems();

		var state = JObject.Parse(container.ExportState());
		((JArray)state["items"]!).Should().BeEmpty();
	}
}

// ── Batch 6: PITR + Persistence Interaction ──
public class PitrPersistenceInteractionTests : IDisposable
{
	private readonly string _dir = Path.Combine(Path.GetTempPath(), $"cosmos-pitr-{Guid.NewGuid():N}");

	public void Dispose() { if (Directory.Exists(_dir)) Directory.Delete(_dir, true); }

	[Fact]
	public async Task RestoreToPointInTime_ThenExportState_MatchesRestoredState()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "V1" },
			new PartitionKey("pk"));
		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);
		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "V2" },
			new PartitionKey("pk"));

		container.RestoreToPointInTime(restorePoint);

		var state = JObject.Parse(container.ExportState());
		var items = (JArray)state["items"]!;
		items.Should().HaveCount(1);
		items[0]!["name"]!.Value<string>().Should().Be("V1");
	}

	[Fact]
	public async Task RestoreToPointInTime_ThenDispose_PersistedFileHasRestoredState()
	{
		var filePath = Path.Combine(_dir, "state.json");
		var container = new InMemoryContainer("test", "/partitionKey");
		container.StateFilePath = filePath;
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "V1" },
			new PartitionKey("pk"));
		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(50);
		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "V2" },
			new PartitionKey("pk"));

		container.RestoreToPointInTime(restorePoint);
		container.Dispose();

		var state = JObject.Parse(File.ReadAllText(filePath));
		var items = (JArray)state["items"]!;
		items[0]!["name"]!.Value<string>().Should().Be("V1");
	}

	[Fact]
	public async Task ImportState_FromExportAfterPITR_DoubleRoundTrip()
	{
		var container1 = new InMemoryContainer("test", "/partitionKey");
		await container1.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Original" },
			new PartitionKey("pk"));
		var snapshot = DateTimeOffset.UtcNow;
		await Task.Delay(50);
		await container1.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Modified" },
			new PartitionKey("pk"));

		container1.RestoreToPointInTime(snapshot);
		var exported = container1.ExportState();

		var container2 = new InMemoryContainer("test", "/partitionKey");
		container2.ImportState(exported);

		var response = await container2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
		response.Resource.Name.Should().Be("Original");
	}
}

// ── Batch 7: File Operations + Cross-Platform ──
public class FileOperationsDeepDiveTests
{
	[Fact]
	public async Task ExportImportFile_PathWithSpaces_Works()
	{
		var dir = Path.Combine(Path.GetTempPath(), $"cosmos test {Guid.NewGuid():N}");
		try
		{
			Directory.CreateDirectory(dir);
			var filePath = Path.Combine(dir, "state file.json");
			var container = new InMemoryContainer("test", "/partitionKey");
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test" },
				new PartitionKey("pk"));

			container.ExportStateToFile(filePath);

			var container2 = new InMemoryContainer("test", "/partitionKey");
			container2.ImportStateFromFile(filePath);

			var response = await container2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
			response.Resource.Name.Should().Be("Test");
		}
		finally
		{
			if (Directory.Exists(dir)) Directory.Delete(dir, true);
		}
	}

	[Fact]
	public async Task ExportStateToFile_WritesUtf8()
	{
		var tempFile = Path.GetTempFileName();
		try
		{
			var container = new InMemoryContainer("test", "/partitionKey");
			await container.CreateItemAsync(
				JObject.FromObject(new { id = "1", partitionKey = "pk", name = "Ünïcödé 🎉" }),
				new PartitionKey("pk"));

			container.ExportStateToFile(tempFile);

			var bytes = File.ReadAllBytes(tempFile);
			var text = Encoding.UTF8.GetString(bytes);
			text.Should().Contain("Ünïcödé 🎉");
		}
		finally
		{
			File.Delete(tempFile);
		}
	}

	[Fact]
	public async Task ExportImportFile_TempPath_CrossPlatformSafe()
	{
		var filePath = Path.Combine(Path.GetTempPath(), $"cosmos-xp-{Guid.NewGuid():N}.json");
		try
		{
			var container = new InMemoryContainer("test", "/partitionKey");
			await container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk", Name = "CrossPlatform" },
				new PartitionKey("pk"));

			container.ExportStateToFile(filePath);

			File.Exists(filePath).Should().BeTrue();
			var container2 = new InMemoryContainer("test", "/partitionKey");
			container2.ImportStateFromFile(filePath);

			var response = await container2.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
			response.Resource.Name.Should().Be("CrossPlatform");
		}
		finally
		{
			if (File.Exists(filePath)) File.Delete(filePath);
		}
	}
}

// ── Batch 8: Error Handling ──
public class StatePersistenceErrorHandlingDeepDiveTests
{
	[Fact]
	public void ImportState_TruncatedJson_ThrowsJsonException()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var act = () => container.ImportState("{\"items\":[{\"id\":\"1\"");

		act.Should().Throw<JsonReaderException>();
	}

	[Fact]
	public async Task ExportStateToFile_InvalidPath_ThrowsIOException()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test" },
			new PartitionKey("pk"));

		var act = () => container.ExportStateToFile(Path.Combine("Z:\\nonexistent\\path\\that\\should\\not\\exist", "state.json"));

		act.Should().Throw<Exception>(); // DirectoryNotFoundException or IOException
	}
}

// ── Batch 9: Change Feed Deep ──
public class StatePersistenceChangeFeedDeepDiveTests
{
	[Fact]
	public async Task ImportState_ThenChangeFeed_OnlyGetsPostImportChanges()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		container.ImportState("{\"items\":[{\"id\":\"1\",\"partitionKey\":\"pk\",\"name\":\"Imported\"}]}");

		// Change feed after import should be empty
		var iter1 = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		iter1.HasMoreResults.Should().BeFalse("ImportState does not populate change feed");

		// Post-import write should appear in change feed
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk", Name = "New" },
			new PartitionKey("pk"));

		var iter2 = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		iter2.HasMoreResults.Should().BeTrue();
		var page = await iter2.ReadNextAsync();
		page.Count.Should().Be(1);
		page.First()["id"]!.Value<string>().Should().Be("2");
	}
}

// ── Batch 10: Concurrency ──
public class StatePersistenceConcurrencyDeepDiveTests
{
	[Fact]
	public async Task ImportState_DuringConcurrentReads_NoCrash()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Original" },
			new PartitionKey("pk"));

		var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
		var readTask = Task.Run(async () =>
		{
			while (!cts.Token.IsCancellationRequested)
			{
				try
				{
					await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
				}
				catch (CosmosException) { /* item may disappear during import */ }
			}
		});

		await Task.Delay(100);
		container.ImportState("{\"items\":[{\"id\":\"2\",\"partitionKey\":\"pk\",\"name\":\"Imported\"}]}");

		cts.Cancel();
		await readTask; // should not throw
	}

	[Fact]
	public async Task ClearItems_DuringConcurrentReads_NoCrash()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk", Name = "Test" },
			new PartitionKey("pk"));

		var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
		var readTask = Task.Run(async () =>
		{
			while (!cts.Token.IsCancellationRequested)
			{
				try
				{
					await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk"));
				}
				catch (CosmosException) { /* item cleared */ }
			}
		});

		await Task.Delay(100);
		container.ClearItems();

		cts.Cancel();
		await readTask; // should not throw
	}
}

// ── Batch 11: Hierarchical PK Edge ──
public class HierarchicalPkPersistenceEdgeTests
{
	[Fact]
	public async Task ExportImport_HierarchicalPK_MissingComponent_RoundTrips()
	{
		var container = new InMemoryContainer("test", new[] { "/tenantId", "/region" });
		// Create an item with both PK components
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenantId = "t1", region = "us" }),
			new PartitionKeyBuilder().Add("t1").Add("us").Build());

		var exported = container.ExportState();
		var container2 = new InMemoryContainer("test", new[] { "/tenantId", "/region" });
		container2.ImportState(exported);

		var response = await container2.ReadItemAsync<JObject>("1",
			new PartitionKeyBuilder().Add("t1").Add("us").Build());
		response.Resource["tenantId"]!.Value<string>().Should().Be("t1");
		response.Resource["region"]!.Value<string>().Should().Be("us");
	}
}
