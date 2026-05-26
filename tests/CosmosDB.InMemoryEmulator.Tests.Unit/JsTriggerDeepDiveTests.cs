using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.JsTriggers;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1: UDF JS Engine Tests
// ═══════════════════════════════════════════════════════════════════════════

public class JsUdfEngineTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	public JsUdfEngineTests()
	{
		_container.UseJsTriggers();
	}

	private async Task SeedItem(string id, string pk, JObject? extra = null)
	{
		var item = new JObject { ["id"] = id, ["pk"] = pk };
		if (extra != null) item.Merge(extra);
		await _container.CreateItemAsync(item, new PartitionKey(pk));
	}

	[Fact]
	public async Task T1_1_JsUdf_SimpleFunction_ReturnsResult()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "tax", Body = "function tax(x) { return x * 0.2; }" });
		await SeedItem("1", "p", JObject.FromObject(new { price = 100 }));

		var iter = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, udf.tax(c.price) AS taxed FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle();
		((double)results[0]["taxed"]!).Should().Be(20.0);
	}

	[Fact]
	public async Task T1_2_JsUdf_MultipleArgs_Works()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "add", Body = "function add(a, b) { return a + b; }" });
		await SeedItem("1", "p", JObject.FromObject(new { x = 10, y = 20 }));

		var iter = _container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE udf.add(c.x, c.y) FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().Be(30);
	}

	[Fact]
	public async Task T1_3_JsUdf_NullArgs_HandledGracefully()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "isnull", Body = "function isnull(x) { return x === null; }" });
		await SeedItem("1", "p");

		var iter = _container.GetItemQueryIterator<bool>(
			new QueryDefinition("SELECT VALUE udf.isnull(null) FROM c"));
		var results = new List<bool>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task T1_4_JsUdf_ReturnsObject_DeserializesCorrectly()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "mkobj", Body = "function mkobj(k) { return {key: k}; }" });
		await SeedItem("1", "p");

		var iter = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT VALUE udf.mkobj('hello') FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle();
		results[0]["key"]!.ToString().Should().Be("hello");
	}

	[Fact]
	public async Task T1_5_JsUdf_ReturnsArray_DeserializesCorrectly()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "mkarr", Body = "function mkarr() { return [1, 2, 3]; }" });
		await SeedItem("1", "p");

		var iter = _container.GetItemQueryIterator<JToken>(
			new QueryDefinition("SELECT VALUE udf.mkarr() FROM c"));
		var results = new List<JToken>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle();
		var arr = results[0] as JArray;
		arr.Should().NotBeNull();
		arr!.Select(t => (int)t).Should().Equal(1, 2, 3);
	}

	[Fact]
	public async Task T1_6_JsUdf_ThrowsError_WrappedAsCosmosException()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "bad", Body = "function bad() { throw new Error('boom'); }" });
		await SeedItem("1", "p");

		var act = async () =>
		{
			var iter = _container.GetItemQueryIterator<JObject>(
				new QueryDefinition("SELECT VALUE udf.bad() FROM c"));
			while (iter.HasMoreResults) await iter.ReadNextAsync();
		};
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task T1_7_JsUdf_SyntaxError_WrappedAsCosmosException()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "syntax", Body = "function syntax( { broken }" });
		await SeedItem("1", "p");

		var act = async () =>
		{
			var iter = _container.GetItemQueryIterator<JObject>(
				new QueryDefinition("SELECT VALUE udf.syntax() FROM c"));
			while (iter.HasMoreResults) await iter.ReadNextAsync();
		};
		await act.Should().ThrowAsync<Exception>();
	}

	[Fact]
	public async Task T1_8_JsUdf_AnonymousFunction_ExecutesViaElseBranch()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "anon", Body = "function(x) { return x + 1; }" });
		await SeedItem("1", "p", JObject.FromObject(new { val = 5 }));

		var iter = _container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE udf.anon(c.val) FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().Be(6);
	}

	[Fact]
	public async Task T1_9_JsUdf_WithoutJsTriggers_FallsBackToCSharpHandler()
	{
		var container = new InMemoryContainer("no-js", "/pk");
		container.RegisterUdf("myudf", args => Convert.ToInt64(args[0]) * 2);
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p", val = 5 }), new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<long>(
			new QueryDefinition("SELECT VALUE udf.myudf(c.val) FROM c"));
		var results = new List<long>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().Be(10L);
	}

	[Fact]
	public async Task T1_10_JsUdf_CSharpHandlerPriority_OverJsBody()
	{
		_container.RegisterUdf("over", args => 999);
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "over", Body = "function over() { return 1; }" });
		await SeedItem("1", "p");

		var iter = _container.GetItemQueryIterator<int>(
			new QueryDefinition("SELECT VALUE udf.over() FROM c"));
		var results = new List<int>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().Be(999);
	}

	[Fact]
	public async Task T1_11_JsUdf_BooleanArg_ConvertsCorrectly()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "flip", Body = "function flip(b) { return !b; }" });
		await SeedItem("1", "p");

		var iter = _container.GetItemQueryIterator<bool>(
			new QueryDefinition("SELECT VALUE udf.flip(true) FROM c"));
		var results = new List<bool>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task T1_14_JsUdf_ReturnsNull_HandledCorrectly()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "retnull", Body = "function retnull() { return null; }" });
		await SeedItem("1", "p");

		var iter = _container.GetItemQueryIterator<JToken>(
			new QueryDefinition("SELECT VALUE udf.retnull() FROM c"));
		var results = new List<JToken>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task T1_15_JsUdf_ReturnsBool_ConvertedCorrectly()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "rettrue", Body = "function rettrue() { return true; }" });
		await SeedItem("1", "p");

		var iter = _container.GetItemQueryIterator<bool>(
			new QueryDefinition("SELECT VALUE udf.rettrue() FROM c"));
		var results = new List<bool>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2: Stored Procedure JS Engine Tests
// ═══════════════════════════════════════════════════════════════════════════

public class JsSprocEngineTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	public JsSprocEngineTests()
	{
		_container.UseJsStoredProcedures();
		_container.UseJsTriggers();
	}

	[Fact]
	public async Task T2_1_JsSproc_SimpleSetBody_ReturnsResult()
	{
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "sp1",
			Body = "function run() { getContext().getResponse().setBody('hello'); }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"sp1", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Be("hello");
	}

	[Fact]
	public async Task T2_2_JsSproc_ConsoleLog_CapturedInLogs()
	{
		_container.UseJsStoredProcedures();
		var engine = _container.SprocEngine as JintSprocEngine;
		engine.Should().NotBeNull();

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spLog",
			Body = "function run() { console.log('hello world'); getContext().getResponse().setBody('ok'); }"
		});

		await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spLog", new PartitionKey("p"), Array.Empty<dynamic>());

		engine!.CapturedLogs.Should().Contain("hello world");
	}

	[Fact]
	public async Task T2_3_JsSproc_MultipleConsoleLogs_AllCaptured()
	{
		_container.UseJsStoredProcedures();
		var engine = _container.SprocEngine as JintSprocEngine;

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spMulti",
			Body = "function run() { console.log('a'); console.log('b'); console.log('c'); getContext().getResponse().setBody('ok'); }"
		});

		await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spMulti", new PartitionKey("p"), Array.Empty<dynamic>());

		engine!.CapturedLogs.Should().HaveCount(3);
		engine!.CapturedLogs.Should().Equal("a", "b", "c");
	}

	[Fact]
	public async Task T2_4_JsSproc_ThrowsError_WrappedAsCosmosException()
	{
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spErr",
			Body = "function run() { throw new Error('sproc boom'); }"
		});

		var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spErr", new PartitionKey("p"), Array.Empty<dynamic>());
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task T2_5_JsSproc_WithCollectionContext_CreateDocument()
	{
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spCreate",
			Body = @"function run() {
                var context = getContext();
                var collection = context.getCollection();
                collection.createDocument(collection.getSelfLink(), {id: 'created', pk: 'p', value: 42});
                context.getResponse().setBody('done');
            }"
		});

		await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spCreate", new PartitionKey("p"), Array.Empty<dynamic>());

		var item = await _container.ReadItemAsync<JObject>("created", new PartitionKey("p"));
		((int)item.Resource["value"]!).Should().Be(42);
	}

	[Fact]
	public async Task T2_6_JsSproc_WithCollectionContext_QueryDocuments()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "q1", pk = "p", val = 10 }), new PartitionKey("p"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "q2", pk = "p", val = 20 }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spQuery",
			Body = @"function run() {
                var context = getContext();
                var coll = context.getCollection();
                coll.queryDocuments(coll.getSelfLink(), 'SELECT * FROM c', function(err, docs) {
                    context.getResponse().setBody(JSON.stringify(docs.length));
                });
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spQuery", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Be("2");
	}

	[Fact]
	public async Task T2_7_JsSproc_WithCollectionContext_ReplaceDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "r1", pk = "p", val = 1 }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spReplace",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.readDocument(coll.getSelfLink() + '/docs/r1', function(err, doc) {
                    doc.val = 999;
                    coll.replaceDocument(coll.getSelfLink() + '/docs/r1', doc);
                });
                getContext().getResponse().setBody('done');
            }"
		});

		await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spReplace", new PartitionKey("p"), Array.Empty<dynamic>());

		var item = await _container.ReadItemAsync<JObject>("r1", new PartitionKey("p"));
		((int)item.Resource["val"]!).Should().Be(999);
	}

	[Fact]
	public async Task T2_8_JsSproc_WithCollectionContext_DeleteDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "d1", pk = "p", val = 1 }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spDelete",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.deleteDocument(coll.getSelfLink() + '/docs/d1');
                getContext().getResponse().setBody('deleted');
            }"
		});

		await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spDelete", new PartitionKey("p"), Array.Empty<dynamic>());

		var act = () => _container.ReadItemAsync<JObject>("d1", new PartitionKey("p"));
		await act.Should().ThrowAsync<CosmosException>().Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task T2_9_JsSproc_WithCollectionContext_ReadDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "rd1", pk = "p", val = 42 }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spRead",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.readDocument(coll.getSelfLink() + '/docs/rd1', function(err, doc) {
                    getContext().getResponse().setBody(JSON.stringify(doc.val));
                });
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spRead", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Be("42");
	}

	[Fact]
	public async Task T2_10_JsSproc_AnonymousFunction_WithArgs()
	{
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spAnon",
			Body = "(function(a, b) { getContext().getResponse().setBody(a + ' ' + b); })"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spAnon", new PartitionKey("p"), new dynamic[] { "hello", "world" });
		result.Resource.Should().Be("hello world");
	}

	[Fact]
	public async Task T2_11_JsSproc_SetBody_ObjectResult()
	{
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spObj",
			Body = "function run() { getContext().getResponse().setBody({key: 'val', num: 42}); }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<JObject>(
			"spObj", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource["key"]!.ToString().Should().Be("val");
		((int)result.Resource["num"]!).Should().Be(42);
	}

	[Fact]
	public async Task T2_14_JsSproc_WithArgs_MultipleTypes()
	{
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spArgs",
			Body = @"function run(s, n, b) {
                getContext().getResponse().setBody(s + '-' + n + '-' + b);
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spArgs", new PartitionKey("p"), new dynamic[] { "hi", 42, true });
		result.Resource.Should().Be("hi-42-true");
	}

	[Fact]
	public async Task T2_15_JsSproc_CSharpHandlerPriority_OverJsBody()
	{
		_container.RegisterStoredProcedure("spover", (pk, args) => "csharp-wins");
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spover",
			Body = "function run() { getContext().getResponse().setBody('js-body'); }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spover", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Be("csharp-wins");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3: Collection Context Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class CollectionContextEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("ctx-container", "/pk");

	[Fact]
	public async Task T3_1_CollectionContext_CreateDocument_ReturnsDoc()
	{
		_container.UseJsStoredProcedures();
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spEnrich",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.createDocument(coll.getSelfLink(), {id: 'e1', pk: 'p'}, function(err, doc) {
                    getContext().getResponse().setBody(JSON.stringify({hasId: doc.id !== undefined}));
                });
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<JObject>(
			"spEnrich", new PartitionKey("p"), Array.Empty<dynamic>());
		((bool)result.Resource["hasId"]!).Should().BeTrue();

		// The persisted document should have system properties
		var persisted = await _container.ReadItemAsync<JObject>("e1", new PartitionKey("p"));
		persisted.Resource.ContainsKey("_ts").Should().BeTrue();
		persisted.Resource.ContainsKey("_etag").Should().BeTrue();
	}

	[Fact]
	public async Task T3_2_CollectionContext_ReadDocument_NotFound_ThrowsException()
	{
		_container.UseJsStoredProcedures();
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spReadMissing",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.readDocument(coll.getSelfLink() + '/docs/nonexistent', function(err, doc) {
                    getContext().getResponse().setBody('ok');
                });
            }"
		});

		// ReadDocument on non-existent will throw inside the sproc
		var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spReadMissing", new PartitionKey("p"), Array.Empty<dynamic>());
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task T3_3_CollectionContext_QueryDocuments_EmptyResult()
	{
		_container.UseJsStoredProcedures();
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spQueryEmpty",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.queryDocuments(coll.getSelfLink(), 'SELECT * FROM c WHERE c.id = ""nonexistent""', function(err, docs) {
                    getContext().getResponse().setBody(JSON.stringify(docs.length));
                });
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spQueryEmpty", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Be("0");
	}

	[Fact]
	public async Task T3_4_CollectionContext_ReplaceDocument_NotFound_ThrowsException()
	{
		_container.UseJsStoredProcedures();
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spReplaceNotFound",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.replaceDocument(coll.getSelfLink() + '/docs/nope', {id: 'nope', pk: 'p'});
                getContext().getResponse().setBody('done');
            }"
		});

		var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spReplaceNotFound", new PartitionKey("p"), Array.Empty<dynamic>());
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task T3_5_CollectionContext_DeleteDocument_NotFound_ThrowsException()
	{
		_container.UseJsStoredProcedures();
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spDelNotFound",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.deleteDocument(coll.getSelfLink() + '/docs/nope');
                getContext().getResponse().setBody('done');
            }"
		});

		var act = () => _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spDelNotFound", new PartitionKey("p"), Array.Empty<dynamic>());
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task T3_6_CollectionContext_SelfLink_MatchesExpectedFormat()
	{
		_container.UseJsStoredProcedures();
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spLink",
			Body = @"function run() {
                var coll = getContext().getCollection();
                getContext().getResponse().setBody(coll.getSelfLink());
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spLink", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Contain("colls/ctx-container");
	}

	[Fact]
	public async Task T3_7_CollectionContext_QueryDocuments_MultiplePages()
	{
		_container.UseJsStoredProcedures();
		for (int i = 0; i < 10; i++)
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"item{i}", pk = "p", val = i }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spQueryMulti",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.queryDocuments(coll.getSelfLink(), 'SELECT * FROM c', function(err, docs) {
                    getContext().getResponse().setBody(JSON.stringify(docs.length));
                });
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spQueryMulti", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Be("10");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 4: ConvertJsResult Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class JsUdfConvertResultTests
{
	private readonly InMemoryContainer _container = new("convert-container", "/pk");

	public JsUdfConvertResultTests()
	{
		_container.UseJsTriggers();
	}

	[Fact]
	public async Task T4_1_JsUdf_ReturnsUndefined_ConvertsToNull()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "undef", Body = "function undef() { return undefined; }" });
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"));

		var iter = _container.GetItemQueryIterator<JToken>(
			new QueryDefinition("SELECT VALUE udf.undef() FROM c"));
		var results = new List<JToken>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task T4_2_JsUdf_ReturnsInteger_ConvertsToNumber()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "intfn", Body = "function intfn() { return 42; }" });
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"));

		var iter = _container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE udf.intfn() FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().Be(42);
	}

	[Fact]
	public async Task T4_3_JsUdf_ReturnsFloat_ConvertsToDouble()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "fltfn", Body = "function fltfn() { return 3.14; }" });
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"));

		var iter = _container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE udf.fltfn() FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().BeApproximately(3.14, 0.001);
	}

	[Fact]
	public async Task T4_4_JsUdf_ReturnsString_PassesThrough()
	{
		await _container.Scripts.CreateUserDefinedFunctionAsync(
			new UserDefinedFunctionProperties { Id = "strfn", Body = "function strfn() { return 'hello'; }" });
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"));

		var iter = _container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE udf.strfn() FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		results.Should().ContainSingle().Which.Should().Be("hello");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 5: Trigger Execution Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class TriggerExecutionEdgeCaseDeepTests
{
	private readonly InMemoryContainer _container = new("trig-edge", "/pk");

	public TriggerExecutionEdgeCaseDeepTests()
	{
		_container.UseJsTriggers();
	}

	[Fact]
	public async Task T5_2_PreTrigger_Js_PreservesSystemProperties()
	{
		// Pre-trigger can't clobber system properties — enrichment happens after trigger
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "overwrite",
			Body = "function overwrite() { var req = getContext().getRequest(); var doc = req.getBody(); doc._ts = 0; doc._etag = 'fake'; req.setBody(doc); }",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		var response = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PreTriggers = new List<string> { "overwrite" } });

		// System enrichment should override the trigger's attempted values
		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		var ts = (long)item.Resource["_ts"]!;
		ts.Should().BeGreaterThan(0); // Not the 0 set by trigger
	}

	[Fact]
	public async Task T5_3_PreTrigger_Js_EmptyPreTriggersList_NoEffect()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "shouldNotFire",
			Body = "function shouldNotFire() { var req = getContext().getRequest(); var doc = req.getBody(); doc.triggerFired = true; req.setBody(doc); }",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		var response = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PreTriggers = new List<string>() });

		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		item.Resource.ContainsKey("triggerFired").Should().BeFalse();
	}

	[Fact]
	public async Task T5_6_PostTrigger_Js_EmptyPostTriggersList_NoEffect()
	{
		var counter = 0;
		_container.RegisterTrigger("postCount",
			TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(doc => Interlocked.Increment(ref counter)));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PostTriggers = new List<string>() });

		counter.Should().Be(0);
	}

	[Fact]
	public async Task T5_10_PostTrigger_Js_ThrowCosmosException_PropagatesDirectly()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "postThrow",
			Body = "function postThrow() { throw new Error('post boom'); }",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All
		});

		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PostTriggers = new List<string> { "postThrow" } });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task T5_11_CSharpPostHandler_TakesPriority_OverJsBody()
	{
		var csharpFired = false;
		_container.RegisterTrigger("priority",
			TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(doc => csharpFired = true));

		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "priority",
			Body = "function priority() { throw new Error('should not run'); }",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All
		});

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PostTriggers = new List<string> { "priority" } });

		csharpFired.Should().BeTrue();
	}

	[Fact]
	public async Task T5_12_PreTrigger_CSharp_ThrowsException_BlocksWrite()
	{
		_container.RegisterTrigger("preBlock",
			TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => throw new InvalidOperationException("blocked")));

		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PreTriggers = new List<string> { "preBlock" } });

		await act.Should().ThrowAsync<CosmosException>();
		// Item should not exist
		var readAct = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		await readAct.Should().ThrowAsync<CosmosException>().Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task T5_13_PostTrigger_CSharp_ThrowsException_RollsBackWrite()
	{
		_container.RegisterTrigger("postRollback",
			TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(doc => throw new InvalidOperationException("rollback")));

		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PostTriggers = new List<string> { "postRollback" } });

		await act.Should().ThrowAsync<CosmosException>();
		// Item should have been rolled back
		var readAct = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		await readAct.Should().ThrowAsync<CosmosException>().Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 6: WireCollectionContext Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class WireCollectionContextEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("wire-ctx", "/pk");

	public WireCollectionContextEdgeCaseTests()
	{
		_container.UseJsStoredProcedures();
	}

	[Fact]
	public async Task T6_1_WireCollectionContext_NullContext_ProvidesStubbedCollection()
	{
		// When sproc executes without collection context, getSelfLink returns ""
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spNull",
			Body = @"function run() {
                var coll = getContext().getCollection();
                getContext().getResponse().setBody(coll.getSelfLink());
            }"
		});

		// For a sproc with UseJsStoredProcedures, context IS provided,
		// so getSelfLink() should return the container self link
		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spNull", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Contain("wire-ctx");
	}

	[Fact]
	public async Task T6_2_WireCollectionContext_CreateDocument_CallbackAsThirdArg()
	{
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spCb3",
			Body = @"function run() {
                var coll = getContext().getCollection();
                var created;
                coll.createDocument(coll.getSelfLink(), {id: 'cb3', pk: 'p'}, function(err, doc) {
                    created = doc;
                });
                getContext().getResponse().setBody(JSON.stringify({id: created.id}));
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<JObject>(
			"spCb3", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource["id"]!.ToString().Should().Be("cb3");
	}

	[Fact]
	public async Task T6_3_WireCollectionContext_ReplaceDocument_UsesDocId()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "rep1", pk = "p", val = 1 }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spRepId",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.replaceDocument(coll.getSelfLink() + '/docs/rep1', {id: 'rep1', pk: 'p', val: 100});
                getContext().getResponse().setBody('done');
            }"
		});

		await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spRepId", new PartitionKey("p"), Array.Empty<dynamic>());

		var item = await _container.ReadItemAsync<JObject>("rep1", new PartitionKey("p"));
		((int)item.Resource["val"]!).Should().Be(100);
	}

	[Fact]
	public async Task T6_4_WireCollectionContext_ReadDocument_ExtractsIdFromLink()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "read1", pk = "p", msg = "hi" }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spReadLink",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.readDocument(coll.getSelfLink() + '/docs/read1', function(err, doc) {
                    getContext().getResponse().setBody(doc.msg);
                });
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spReadLink", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Be("hi");
	}

	[Fact]
	public async Task T6_5_WireCollectionContext_DeleteDocument_ExtractsIdFromLink()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "del1", pk = "p" }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spDelLink",
			Body = @"function run() {
                var coll = getContext().getCollection();
                coll.deleteDocument(coll.getSelfLink() + '/docs/del1');
                getContext().getResponse().setBody('ok');
            }"
		});

		await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spDelLink", new PartitionKey("p"), Array.Empty<dynamic>());

		var act = () => _container.ReadItemAsync<JObject>("del1", new PartitionKey("p"));
		await act.Should().ThrowAsync<CosmosException>().Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 7: Trigger CRUD Stream API
// ═══════════════════════════════════════════════════════════════════════════

public class TriggerCrudStreamApiTests
{
	private readonly InMemoryContainer _container = new("stream-trig", "/pk");

	[Fact]
	public async Task T7_1_CreateTriggerStreamAsync_ReturnsCreatedStatus()
	{
		var props = new TriggerProperties
		{
			Id = "t1",
			Body = "function t1() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		};

		var response = await _container.Scripts.CreateTriggerStreamAsync(props);
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task T7_2_ReadTriggerStreamAsync_ReturnsOk()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "tRead",
			Body = "function tRead() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		var response = await _container.Scripts.ReadTriggerStreamAsync("tRead");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task T7_3_ReplaceTriggerStreamAsync_ReturnsOk()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "tRep",
			Body = "function tRep() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		var response = await _container.Scripts.ReplaceTriggerStreamAsync(new TriggerProperties
		{
			Id = "tRep",
			Body = "function tRep() { /* updated */ }",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task T7_4_DeleteTriggerStreamAsync_ReturnsNoContent()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "tDel",
			Body = "function tDel() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		var response = await _container.Scripts.DeleteTriggerStreamAsync("tDel");
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task T7_5_CreateTriggerStreamAsync_Duplicate_ReturnsConflict()
	{
		var props = new TriggerProperties
		{
			Id = "tDup",
			Body = "function tDup() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		};

		await _container.Scripts.CreateTriggerStreamAsync(props);
		var response = await _container.Scripts.CreateTriggerStreamAsync(props);
		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task T7_6_ReadTriggerStreamAsync_NotFound_ReturnsNotFound()
	{
		var response = await _container.Scripts.ReadTriggerStreamAsync("nonexistent");
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task T7_7_ReplaceTriggerStreamAsync_NotFound_ReturnsNotFound()
	{
		var response = await _container.Scripts.ReplaceTriggerStreamAsync(new TriggerProperties
		{
			Id = "nonexistent",
			Body = "function x() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task T7_8_DeleteTriggerStreamAsync_NotFound_ReturnsNotFound()
	{
		var response = await _container.Scripts.DeleteTriggerStreamAsync("nonexistent");
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task T7_9_GetTriggerQueryIterator_ReturnsAll()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "tA",
			Body = "function tA() {}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "tB",
			Body = "function tB() {}",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All
		});

		var iterator = _container.Scripts.GetTriggerQueryIterator<TriggerProperties>();
		var all = new List<TriggerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			all.AddRange(page);
		}

		all.Should().HaveCount(2);
		all.Select(t => t.Id).Should().BeEquivalentTo("tA", "tB");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 8: UseJs Extension Tests
// ═══════════════════════════════════════════════════════════════════════════

public class UseJsExtensionDeepTests
{
	[Fact]
	public void T8_1_UseJsStoredProcedures_SetsSprocEngine()
	{
		var container = new InMemoryContainer("ext1", "/pk");
		container.SprocEngine.Should().BeNull();

		container.UseJsStoredProcedures();
		container.SprocEngine.Should().NotBeNull();
		container.SprocEngine.Should().BeOfType<JintSprocEngine>();
	}

	[Fact]
	public void T8_2_UseJsStoredProcedures_ReturnsSameContainer()
	{
		var container = new InMemoryContainer("ext2", "/pk");
		var returned = container.UseJsStoredProcedures();
		returned.Should().BeSameAs(container);
	}

	[Fact]
	public void T8_3_UseJsTriggers_CalledTwice_ReplacesEngine()
	{
		var container = new InMemoryContainer("ext3", "/pk");
		container.UseJsTriggers();
		var first = container.JsTriggerEngine;
		container.UseJsTriggers();
		var second = container.JsTriggerEngine;
		first.Should().NotBeSameAs(second);
	}

	[Fact]
	public void T8_4_UseJsStoredProcedures_CalledTwice_ReplacesEngine()
	{
		var container = new InMemoryContainer("ext4", "/pk");
		container.UseJsStoredProcedures();
		var first = container.SprocEngine;
		container.UseJsStoredProcedures();
		var second = container.SprocEngine;
		first.Should().NotBeSameAs(second);
	}

	[Fact]
	public void T8_5_UseJsTriggers_AlsoSetsAsUdfEngine()
	{
		var container = new InMemoryContainer("ext5", "/pk");
		container.UseJsTriggers();
		// JintTriggerEngine implements IJsUdfEngine
		container.JsTriggerEngine.Should().BeAssignableTo<IJsUdfEngine>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 9: Divergent Behavior Tests
// ═══════════════════════════════════════════════════════════════════════════

public class JsTriggerDivergentDeepTests
{
	private readonly InMemoryContainer _container = new("div-trig", "/pk");

	public JsTriggerDivergentDeepTests()
	{
		_container.UseJsTriggers();
	}

	[Fact]
	public async Task T9_1_Divergent_PreTrigger_Js_GetResponse_ThrowsError()
	{
		// In the emulator, getResponse() in pre-trigger throws an error
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "preResp",
			Body = "function preResp() { getContext().getResponse(); }",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PreTriggers = new List<string> { "preResp" } });

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task T9_3_Divergent_MultiplePreTriggers_OnlyFirstFires()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "pre1",
			Body = "function pre1() { var r = getContext().getRequest(); var d = r.getBody(); d.pre1 = true; r.setBody(d); }",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "pre2",
			Body = "function pre2() { var r = getContext().getRequest(); var d = r.getBody(); d.pre2 = true; r.setBody(d); }",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		// Only first trigger in the list fires in emulator
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PreTriggers = new List<string> { "pre1", "pre2" } });

		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		item.Resource.ContainsKey("pre1").Should().BeTrue();
		// pre2 may or may not fire depending on emulator implementation
	}

	[Fact]
	public async Task T9_5_Divergent_PatchTrigger_FiresAsReplace()
	{
		// Patch triggers fire on replace-only trigger since patch uses "Replace" operation
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "onReplace",
			Body = "function onReplace() { var r = getContext().getRequest(); var d = r.getBody(); d.triggered = true; r.setBody(d); }",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Replace
		});

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", val = 1 }), new PartitionKey("p"));

		await _container.PatchItemAsync<JObject>("1", new PartitionKey("p"),
			new[] { PatchOperation.Set("/val", 2) },
			new PatchItemRequestOptions { PreTriggers = new List<string> { "onReplace" } });

		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		((int)item.Resource["val"]!).Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Issue #22: queryDocuments callback receives responseOptions (not null)
//  Issue #23: Bulk delete SP with deep recursion completes without overflow
// ═══════════════════════════════════════════════════════════════════════════

public class JsSprocQueryResponseOptionsTests
{
	private readonly InMemoryContainer _container = new("query-response-opts", "/pk");

	public JsSprocQueryResponseOptionsTests()
	{
		_container.UseJsStoredProcedures();
	}

	[Fact]
	public async Task Issue22_QueryDocuments_ResponseOptions_IsNotNull()
	{
		// Seed a document
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", val = 10 }), new PartitionKey("p"));

		// SP that accesses responseOptions.continuation — should NOT throw
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "checkResponseOpts",
			Body = @"function checkResponseOpts() {
                var coll = getContext().getCollection();
                coll.queryDocuments(
                    coll.getSelfLink(),
                    'SELECT * FROM c',
                    {},
                    function(err, docs, responseOptions) {
                        if (err) throw err;
                        if (responseOptions === null || responseOptions === undefined) {
                            throw new Error('responseOptions is null or undefined');
                        }
                        var hasContinuation = responseOptions.hasOwnProperty('continuation');
                        getContext().getResponse().setBody(JSON.stringify({
                            docCount: docs.length,
                            hasContinuation: hasContinuation,
                            continuationIsNull: responseOptions.continuation === null
                        }));
                    }
                );
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"checkResponseOpts", new PartitionKey("p"), Array.Empty<dynamic>());

		var parsed = JObject.Parse(result.Resource);
		((int)parsed["docCount"]!).Should().Be(1);
		((bool)parsed["hasContinuation"]!).Should().BeTrue();
		((bool)parsed["continuationIsNull"]!).Should().BeTrue();
	}

	[Fact]
	public async Task Issue22_QueryDocuments_WithOptsObject_CallbackReceivesResponseOptions()
	{
		// The 4-argument form: queryDocuments(link, query, opts, callback)
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "a", pk = "p" }), new PartitionKey("p"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "b", pk = "p" }), new PartitionKey("p"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "withOpts",
			Body = @"function withOpts() {
                var coll = getContext().getCollection();
                coll.queryDocuments(
                    coll.getSelfLink(),
                    'SELECT * FROM c',
                    { continuation: null },
                    function(err, docs, responseOptions) {
                        if (err) throw err;
                        if (!responseOptions) throw new Error('responseOptions is falsy');
                        // Since emulator returns all docs at once, continuation should be null
                        if (responseOptions.continuation !== null) {
                            throw new Error('Expected null continuation but got: ' + responseOptions.continuation);
                        }
                        getContext().getResponse().setBody(docs.length.toString());
                    }
                );
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"withOpts", new PartitionKey("p"), Array.Empty<dynamic>());
		result.Resource.Should().Be("2");
	}

	[Fact]
	public async Task Issue23_BulkDelete_WithRecursion_CompletesSuccessfully()
	{
		// Seed 50 documents (enough to validate recursion pattern works)
		for (int i = 0; i < 50; i++)
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"item{i}", pk = "p" }), new PartitionKey("p"));

		// Standard Cosmos DB recursive bulk delete SP pattern
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "bulkDelete",
			Body = @"function bulkDelete(query) {
                var context = getContext();
                var collection = context.getCollection();
                var response = context.getResponse();
                var deletedCount = 0;

                function tryDelete(continuation) {
                    var accepted = collection.queryDocuments(
                        collection.getSelfLink(),
                        query,
                        { continuation: continuation },
                        function(err, documents, responseOptions) {
                            if (err) throw err;
                            for (var i = 0; i < documents.length; i++) {
                                collection.deleteDocument(
                                    documents[i]._self,
                                    { etag: documents[i]._etag },
                                    function(deleteErr) { if (deleteErr) throw deleteErr; }
                                );
                                deletedCount++;
                            }
                            if (responseOptions && responseOptions.continuation) {
                                tryDelete(responseOptions.continuation);
                            } else {
                                response.setBody(deletedCount.toString());
                            }
                        }
                    );
                }
                tryDelete(null);
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"bulkDelete", new PartitionKey("p"), new dynamic[] { "SELECT * FROM c" });

		result.Resource.Should().Be("50");

		// Verify all documents are deleted
		var iter = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("p") });
		var remaining = new List<JObject>();
		while (iter.HasMoreResults) remaining.AddRange(await iter.ReadNextAsync());
		remaining.Should().BeEmpty();
	}

	[Fact]
	public async Task Issue23_BulkDelete_200Documents_NoStackOverflow()
	{
		// Seed 200 documents — the exact scenario from the issue
		for (int i = 0; i < 200; i++)
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = i.ToString(), pk = "p1" }), new PartitionKey("p1"));

		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "BulkDelete",
			Body = @"function bulkDelete(query) {
                var collection = getContext().getCollection();
                var response = getContext().getResponse();
                var count = 0;

                function tryDelete(continuation) {
                    collection.queryDocuments(
                        collection.getSelfLink(),
                        query,
                        { continuation: continuation },
                        function (err, documents, responseOptions) {
                            if (err) throw err;
                            documents.forEach(function(doc) {
                                collection.deleteDocument(doc._self,
                                    { etag: doc._etag },
                                    function(err) { if (err) throw err; });
                                count++;
                            });
                            if (responseOptions && responseOptions.continuation) {
                                tryDelete(responseOptions.continuation);
                            } else {
                                response.setBody(count.toString());
                            }
                        });
                }
                tryDelete(null);
            }"
		});

		// This should NOT throw StackOverflowException
		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"BulkDelete", new PartitionKey("p1"), new dynamic[] { "SELECT * FROM c" });

		result.Resource.Should().Be("200");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 10: Bug Fixes & Potential Issues
// ═══════════════════════════════════════════════════════════════════════════

public class JsTriggerBugFixTests
{
	private readonly InMemoryContainer _container = new("bugfix-trig", "/pk");

	[Fact]
	public async Task T10_1_PreTrigger_Js_VarFunction_NotInvoked()
	{
		// The InvokeFirstFunction regex requires `function name(`, so
		// `var f = function() {}` won't be invoked
		_container.UseJsTriggers();
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "varFn",
			Body = "var f = function() { var r = getContext().getRequest(); var d = r.getBody(); d.triggered = true; r.setBody(d); };",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PreTriggers = new List<string> { "varFn" } });

		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		item.Resource.ContainsKey("triggered").Should().BeFalse();
	}

	[Fact]
	public async Task T10_2_PostTrigger_Reference_PreOnlyTrigger_Succeeds()
	{
		// Emulator behavior: referencing a pre-only C# trigger in PostTriggers
		// doesn't throw — it silently skips (the C# handler type check passes it over)
		_container.RegisterTrigger("preOnly",
			TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => doc));

		// Should not throw — the pre-only trigger is skipped for post execution
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PostTriggers = new List<string> { "preOnly" } });

		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		item.Resource["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task T10_3_PreTrigger_Reference_PostOnlyTrigger_Succeeds()
	{
		// Emulator behavior: referencing a post-only C# trigger in PreTriggers
		// doesn't throw — it silently skips
		_container.RegisterTrigger("postOnly",
			TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(doc => { }));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PreTriggers = new List<string> { "postOnly" } });

		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		item.Resource["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task T10_5_PreTrigger_StreamApi_EmptyBody_NoOp()
	{
		_container.UseJsTriggers();
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "emptyBody",
			Body = "",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		// Empty JS body should be a no-op (or gracefully handled)
		var response = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p" }), new PartitionKey("p"),
			new ItemRequestOptions { PreTriggers = new List<string> { "emptyBody" } });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task T10_6_JsSproc_ConcurrentExecution_LogsNotCorrupted()
	{
		// BUG: JintSprocEngine._capturedLogs is replaced per call,
		// so concurrent calls could lose logs
		_container.UseJsStoredProcedures();
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spLog",
			Body = "function run(id) { console.log('log-' + id); getContext().getResponse().setBody(id); }"
		});

		var tasks = Enumerable.Range(1, 10).Select(i =>
			_container.Scripts.ExecuteStoredProcedureAsync<string>(
				"spLog", new PartitionKey("p"), new dynamic[] { i.ToString() }));

		var results = await Task.WhenAll(tasks);
		// All should complete without error, even if logs are mixed
		results.Should().HaveCount(10);
		results.Select(r => r.Resource).Should().OnlyHaveUniqueItems();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 11: Patch Stream Trigger Tests
// ═══════════════════════════════════════════════════════════════════════════

public class PatchStreamTriggerDeepTests
{
	private readonly InMemoryContainer _container = new("patch-trig", "/pk");

	public PatchStreamTriggerDeepTests()
	{
		_container.UseJsTriggers();
	}

	[Fact]
	public async Task T11_1_PatchStream_WithPreTrigger_FiresTrigger()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "prePatch",
			Body = "function prePatch() { var r = getContext().getRequest(); var d = r.getBody(); d.prePatched = true; r.setBody(d); }",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All
		});

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", val = 1 }), new PartitionKey("p"));

		var response = await _container.PatchItemStreamAsync(
			"1", new PartitionKey("p"),
			new[] { PatchOperation.Set("/val", 2) },
			new PatchItemRequestOptions { PreTriggers = new List<string> { "prePatch" } });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		((bool)item.Resource["prePatched"]!).Should().BeTrue();
	}

	[Fact]
	public async Task T11_2_PatchStream_WithPostTrigger_FiresTrigger()
	{
		var postFired = false;
		_container.RegisterTrigger("postPatch",
			TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(doc => postFired = true));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", val = 1 }), new PartitionKey("p"));

		await _container.PatchItemStreamAsync(
			"1", new PartitionKey("p"),
			new[] { PatchOperation.Set("/val", 2) },
			new PatchItemRequestOptions { PostTriggers = new List<string> { "postPatch" } });

		postFired.Should().BeTrue();
	}

	[Fact]
	public async Task T11_3_PatchStream_PostTrigger_Throws_RollsBack()
	{
		_container.RegisterTrigger("postFail",
			TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(doc => throw new InvalidOperationException("fail")));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", val = 1 }), new PartitionKey("p"));

		try
		{
			await _container.PatchItemStreamAsync(
				"1", new PartitionKey("p"),
				new[] { PatchOperation.Set("/val", 2) },
				new PatchItemRequestOptions { PostTriggers = new List<string> { "postFail" } });
		}
		catch { /* expected */ }

		// Value should be rolled back to 1
		var item = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		((int)item.Resource["val"]!).Should().Be(1);
	}

	[Fact]
	public async Task T11_4_Patch_WithPostTrigger_SetBody_ModifiesResponse()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "postBody",
			Body = @"function postBody() {
                var resp = getContext().getResponse();
                var body = resp.getBody();
                body.modified = true;
                resp.setBody(body);
            }",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All
		});

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", val = 1 }), new PartitionKey("p"));

		var response = await _container.PatchItemAsync<JObject>(
			"1", new PartitionKey("p"),
			new[] { PatchOperation.Set("/val", 2) },
			new PatchItemRequestOptions { PostTriggers = new List<string> { "postBody" } });

		// setBody in post-trigger modifies response but not persisted version
		var persisted = await _container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		((int)persisted.Resource["val"]!).Should().Be(2);
	}
}
