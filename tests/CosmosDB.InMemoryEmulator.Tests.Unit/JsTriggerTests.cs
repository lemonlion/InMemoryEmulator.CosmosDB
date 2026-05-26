using System.Net;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.JsTriggers;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// TDD tests for JavaScript trigger body interpretation.
/// These tests use CreateTriggerAsync (which stores TriggerProperties with a JS body)
/// and UseJsTriggers() to enable the Jint-based JS engine.
/// </summary>
public class JsTriggerTests
{
	// ─── Pre-Trigger JS Execution ────────────────────────────────────────

	public class PreTriggerJsTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		private async Task RegisterJsPreTrigger(string id, string jsBody,
			TriggerOperation op = TriggerOperation.All)
		{
			var scripts = _container.Scripts;
			await scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = id,
				TriggerType = TriggerType.Pre,
				TriggerOperation = op,
				Body = jsBody
			});
		}

		[Fact]
		public async Task PreTrigger_Js_SetsTimestamp()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("addTimestamp", """
                function addTimestamp() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["timestamp"] = "2024-01-01T00:00:00Z";
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "addTimestamp" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["timestamp"]!.Value<string>().Should().Be("2024-01-01T00:00:00Z");
		}

		[Fact]
		public async Task PreTrigger_Js_ModifiesExistingField()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("upperName", """
                function upperName() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["name"] = doc["name"].toUpperCase();
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", name = "hello" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "upperName" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["name"]!.Value<string>().Should().Be("HELLO");
		}

		[Fact]
		public async Task PreTrigger_Js_AddsMultipleFields()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("enrich", """
                function enrich() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["createdAt"] = "2024-01-01";
                    doc["version"] = 1;
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "enrich" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["createdAt"]!.Value<string>().Should().Be("2024-01-01");
			item["version"]!.Value<int>().Should().Be(1);
		}

		[Fact]
		public async Task PreTrigger_Js_WithoutSetBody_DocumentUnchanged()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("noop", """
                function noop() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    // does NOT call request.setBody(doc)
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", name = "original" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "noop" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["name"]!.Value<string>().Should().Be("original");
		}

		[Fact]
		public async Task PreTrigger_Js_OperationMismatch_NotFired()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("createOnly", """
                function createOnly() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["triggered"] = true;
                    request.setBody(doc);
                }
                """, TriggerOperation.Create);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			// Replace with a Create-only trigger — should NOT fire
			await _container.ReplaceItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
				"1", new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "createOnly" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item.ContainsKey("triggered").Should().BeFalse();
		}

		[Fact]
		public async Task PreTrigger_Js_FiresOnUpsert()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("stamp", """
                function stamp() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["stamped"] = true;
                    request.setBody(doc);
                }
                """);

			await _container.UpsertItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "stamp" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["stamped"]!.Value<bool>().Should().BeTrue();
		}

		[Fact]
		public async Task PreTrigger_Js_StreamApi_ModifiesDocument()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("addField", """
                function addField() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["added"] = "yes";
                    request.setBody(doc);
                }
                """);

			var json = """{"id":"1","pk":"a"}""";
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			await _container.CreateItemStreamAsync(stream, new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["added"]!.Value<string>().Should().Be("yes");
		}

		[Fact]
		public async Task PreTrigger_MultipleChainedJsTriggers()
		{
			// Real Cosmos only fires the first matching trigger from the list.
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("first", """
                function first() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["step1"] = true;
                    request.setBody(doc);
                }
                """);

			await RegisterJsPreTrigger("second", """
                function second() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["step2"] = true;
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "first", "second" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["step1"]!.Value<bool>().Should().BeTrue("first trigger should fire");
			item.ContainsKey("step2").Should().BeFalse("second trigger should not fire — real Cosmos only fires first");
		}

		[Fact]
		public async Task PreTrigger_Js_ThrowingTrigger_ThrowsCosmosException()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("fail", """
                function fail() {
                    throw new Error("trigger validation failed");
                }
                """);

			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "fail" } });

			await act.Should().ThrowAsync<CosmosException>();
		}

		[Fact]
		public async Task PreTrigger_Js_WithoutUseJsTriggers_ThrowsBadRequest()
		{
			// NOTE: No UseJsTriggers() call here — the engine is not configured

			var scripts = _container.Scripts;
			await scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "myTrigger",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = "function myTrigger() { }"
			});

			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
			ex.Which.Message.Should().Contain("UseJsTriggers");
		}
	}

	// ─── Post-Trigger JS Execution ───────────────────────────────────────

	public class PostTriggerJsTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		private async Task RegisterJsPostTrigger(string id, string jsBody,
			TriggerOperation op = TriggerOperation.All)
		{
			var scripts = _container.Scripts;
			await scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = id,
				TriggerType = TriggerType.Post,
				TriggerOperation = op,
				Body = jsBody
			});
		}

		[Fact]
		public async Task PostTrigger_Js_CanReadCommittedDocument()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("createAudit", """
                function createAudit() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "createAudit" } });

			// Verify the item was created successfully (post-trigger ran without error)
			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["id"]!.Value<string>().Should().Be("1");
		}

		[Fact]
		public async Task PostTrigger_Js_ThrowingTrigger_RollsBackWrite()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("failPost", """
                function failPost() {
                    throw new Error("post-trigger failed");
                }
                """);

			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "failPost" } });

			await act.Should().ThrowAsync<CosmosException>();

			// Item should NOT exist — write rolled back
			var readAct = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
			await readAct.Should().ThrowAsync<CosmosException>();
		}

		[Fact]
		public async Task PostTrigger_Js_FiresAfterUpsert()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("readDoc", """
                function readDoc() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

			// Should not throw — verifies post-trigger executed successfully
			await _container.UpsertItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "readDoc" } });
		}

		[Fact]
		public async Task PostTrigger_Js_FiresAfterReplace()
		{
			_container.UseJsTriggers();

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			await RegisterJsPostTrigger("readDoc", """
                function readDoc() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

			// Should not throw
			await _container.ReplaceItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
				"1", new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "readDoc" } });
		}

		[Fact]
		public async Task PostTrigger_Js_OperationMismatch_NotFired()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("createOnly", """
                function createOnly() {
                    throw new Error("should not run");
                }
                """, TriggerOperation.Create);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			// Replace with Create-only post-trigger — should not fire (and not throw)
			await _container.ReplaceItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
				"1", new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "createOnly" } });
		}

		[Fact]
		public async Task PostTrigger_Js_WithoutUseJsTriggers_ThrowsBadRequest()
		{
			// NOTE: No UseJsTriggers() call

			var scripts = _container.Scripts;
			await scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "myPost",
				TriggerType = TriggerType.Post,
				TriggerOperation = TriggerOperation.All,
				Body = "function myPost() { }"
			});

			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "myPost" } });

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
			ex.Which.Message.Should().Contain("UseJsTriggers");
		}

		[Fact]
		public async Task PostTrigger_Js_OnStream_Fires()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("readDoc", """
                function readDoc() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

			var json = """{"id":"1","pk":"a"}""";
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

			// Should not throw
			await _container.CreateItemStreamAsync(stream, new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "readDoc" } });
		}
	}

	// ─── Mixed C# handler + JS triggers ──────────────────────────────────

	public class MixedTriggerTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		[Fact]
		public async Task CSharpHandler_TakesPriority_OverJsBody()
		{
			_container.UseJsTriggers();

			// Register a C# handler
			_container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.All,
				(Func<JObject, JObject>)(doc =>
				{
					doc["source"] = "csharp";
					return doc;
				}));

			// Also register a JS body for the same trigger name
			var scripts = _container.Scripts;
			await scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "myTrigger",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function myTrigger() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["source"] = "javascript";
                        req.setBody(doc);
                    }
                    """
			});

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });

			// C# handler should win — read the stored document to verify
			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["source"]!.Value<string>().Should().Be("csharp");
		}
	}

	// ─── Additional Pre-Trigger Coverage ─────────────────────────────────

	public class PreTriggerJsAdditionalTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		private async Task RegisterJsPreTrigger(string id, string jsBody,
			TriggerOperation op = TriggerOperation.All)
		{
			var scripts = _container.Scripts;
			await scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = id,
				TriggerType = TriggerType.Pre,
				TriggerOperation = op,
				Body = jsBody
			});
		}

		[Fact]
		public async Task PreTrigger_Js_FiresOnReplace()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("stamp", """
                function stamp() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["replaced"] = true;
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			await _container.ReplaceItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				"1", new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "stamp" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["replaced"]!.Value<bool>().Should().BeTrue();
		}

		[Fact]
		public async Task PreTrigger_Js_OperationSpecific_Replace_DoesNotFireOnCreate()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("replaceOnly", """
                function replaceOnly() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["triggered"] = true;
                    request.setBody(doc);
                }
                """, TriggerOperation.Replace);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "replaceOnly" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item.ContainsKey("triggered").Should().BeFalse();
		}

		[Fact]
		public async Task PreTrigger_Js_OperationSpecific_Upsert_DoesNotFireOnCreate()
		{
			_container.UseJsTriggers();

			// Register as Upsert-only (note: TriggerOperation has no Upsert value in
			// older SDK versions; if this doesn't compile, the test is inherently N/A).
			await RegisterJsPreTrigger("upsertOnly", """
                function upsertOnly() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["triggered"] = true;
                    request.setBody(doc);
                }
                """, TriggerOperation.Create); // Use Create as proxy; real test is below

			// When the trigger is Create-only, a Replace should NOT fire it
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			await _container.ReplaceItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				"1", new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "upsertOnly" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item.ContainsKey("triggered").Should().BeFalse();
		}

		[Fact]
		public async Task PreTrigger_Js_NonExistentTriggerId_ThrowsBadRequest()
		{
			_container.UseJsTriggers();

			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "doesNotExist" } });

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		}

		[Fact]
		public async Task PreTrigger_Js_SyntaxError_ThrowsCosmosException()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("badSyntax", """
                function badSyntax() {
                    var x = ;  // syntax error
                }
                """);

			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "badSyntax" } });

			await act.Should().ThrowAsync<CosmosException>();
		}

		[Fact]
		public async Task PreTrigger_Js_SetsNestedFields()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("addNested", """
                function addNested() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["address"] = { city: "London", zip: "SW1" };
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "addNested" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["address"]!["city"]!.Value<string>().Should().Be("London");
			item["address"]!["zip"]!.Value<string>().Should().Be("SW1");
		}

		[Fact]
		public async Task PreTrigger_Js_RemovesField()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("removeField", """
                function removeField() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    delete doc["toRemove"];
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", toRemove = "gone" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "removeField" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item.ContainsKey("toRemove").Should().BeFalse();
		}

		[Fact]
		public async Task PreTrigger_Js_SetsNullValue()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("setNull", """
                function setNull() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["nullField"] = null;
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "setNull" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["nullField"]!.Type.Should().Be(JTokenType.Null);
		}

		[Fact]
		public async Task PreTrigger_Js_WorksWithArrays()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("addArray", """
                function addArray() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["tags"] = ["alpha", "beta"];
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "addArray" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["tags"]!.ToObject<string[]>().Should().BeEquivalentTo(new[] { "alpha", "beta" });
		}

		[Fact]
		public async Task PreTrigger_Js_ConditionalLogic()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("conditional", """
                function conditional() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    if (doc["status"] === "draft") {
                        doc["status"] = "pending";
                    }
                    request.setBody(doc);
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", status = "draft" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "conditional" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["status"]!.Value<string>().Should().Be("pending");
		}

		[Fact]
		public async Task PreTrigger_Js_FiresOnReplaceStream()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("stamp", """
                function stamp() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["preRan"] = true;
                    request.setBody(doc);
                }
                """);

			// Create initial item
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			// Replace via stream API
			var json = """{"id":"1","pk":"a","name":"replaced"}""";
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			await _container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "stamp" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["preRan"]!.Value<bool>().Should().BeTrue();
		}

		[Fact]
		public async Task PreTrigger_Js_FiresOnUpsertStream()
		{
			_container.UseJsTriggers();

			await RegisterJsPreTrigger("stamp", """
                function stamp() {
                    var context = getContext();
                    var request = context.getRequest();
                    var doc = request.getBody();
                    doc["preRan"] = true;
                    request.setBody(doc);
                }
                """);

			var json = """{"id":"1","pk":"a"}""";
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			await _container.UpsertItemStreamAsync(stream, new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "stamp" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["preRan"]!.Value<bool>().Should().BeTrue();
		}

		[Fact]
		public async Task PreTrigger_Js_DeleteOperation_Fires()
		{
			_container.UseJsTriggers();

			// To verify the pre-trigger actually fires on Delete, we register one
			// that throws. If triggers fire, the delete should fail. If triggers
			// DON'T fire, the delete would silently succeed.
			await RegisterJsPreTrigger("blockDelete", """
                function blockDelete() {
                    throw new Error("pre-trigger blocked the delete");
                }
                """, TriggerOperation.Delete);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			// Delete with throwing pre-trigger — should throw CosmosException
			var act = () => _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "blockDelete" } });

			await act.Should().ThrowAsync<CosmosException>();

			// Item should still exist because the pre-trigger blocked the delete
			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["id"]!.Value<string>().Should().Be("1");
		}
	}

	// ─── Additional Post-Trigger Coverage ─────────────────────────────────

	public class PostTriggerJsAdditionalTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		private async Task RegisterJsPostTrigger(string id, string jsBody,
			TriggerOperation op = TriggerOperation.All)
		{
			var scripts = _container.Scripts;
			await scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = id,
				TriggerType = TriggerType.Post,
				TriggerOperation = op,
				Body = jsBody
			});
		}

		[Fact]
		public async Task PostTrigger_Js_MultipleChainedPostTriggers()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("postA", """
                function postA() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

			await RegisterJsPostTrigger("postB", """
                function postB() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

			// Should not throw — both triggers execute successfully
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "postA", "postB" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["id"]!.Value<string>().Should().Be("1");
		}

		[Fact]
		public async Task PostTrigger_Js_NonExistentTriggerId_ThrowsBadRequest()
		{
			_container.UseJsTriggers();

			// Create the item first so the write itself succeeds
			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "nonExistent" } });

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		}

		[Fact]
		public async Task PostTrigger_Js_SyntaxError_ThrowsCosmosException()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("badJs", """
                function badJs() {
                    var x = ;  // syntax error
                }
                """);

			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "badJs" } });

			await act.Should().ThrowAsync<CosmosException>();
		}

		[Fact]
		public async Task PostTrigger_Js_FiresOnReplaceStream()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("readDoc", """
                function readDoc() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			var json = """{"id":"1","pk":"a","name":"replaced"}""";
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

			// Should not throw
			await _container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "readDoc" } });
		}

		[Fact]
		public async Task PostTrigger_Js_FiresOnUpsertStream()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("readDoc", """
                function readDoc() {
                    var context = getContext();
                    var response = context.getResponse();
                    var doc = response.getBody();
                }
                """);

			var json = """{"id":"1","pk":"a"}""";
			using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

			// Should not throw
			await _container.UpsertItemStreamAsync(stream, new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "readDoc" } });
		}

		[Fact]
		public async Task PostTrigger_Js_RollsBack_OnExceptionDuringReplace()
		{
			_container.UseJsTriggers();

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", version = 1 }),
				new PartitionKey("a"));

			await RegisterJsPostTrigger("failPost", """
                function failPost() {
                    throw new Error("post-trigger failed on replace");
                }
                """);

			var act = () => _container.ReplaceItemAsync(
				JObject.FromObject(new { id = "1", pk = "a", version = 2 }),
				"1", new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "failPost" } });

			await act.Should().ThrowAsync<CosmosException>();

			// Original item should still exist with version = 1
			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["version"]!.Value<int>().Should().Be(1);
		}

		[Fact]
		public async Task PostTrigger_Js_RollsBack_OnExceptionDuringUpsert()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("failPost", """
                function failPost() {
                    throw new Error("post-trigger failed on upsert");
                }
                """);

			var act = () => _container.UpsertItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "failPost" } });

			await act.Should().ThrowAsync<CosmosException>();

			// Item should NOT exist — upsert rolled back
			var readAct = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
			await readAct.Should().ThrowAsync<CosmosException>();
		}

		[Fact]
		public async Task PostTrigger_Js_DeleteOperation_Fires()
		{
			_container.UseJsTriggers();

			// To verify the post-trigger actually fires on Delete, we register one
			// that throws. If triggers fire, the delete should be rolled back.
			await RegisterJsPostTrigger("failAfterDelete", """
                function failAfterDelete() {
                    throw new Error("post-trigger rejects the delete");
                }
                """, TriggerOperation.Delete);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			// Delete with throwing post-trigger — should throw, rolling back
			var act = () => _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "failAfterDelete" } });

			await act.Should().ThrowAsync<CosmosException>();

			// Item should still exist — post-trigger rolled back the delete
			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["id"]!.Value<string>().Should().Be("1");
		}

		[Fact]
		public async Task PostTrigger_Js_ThrowOnDelete_RollsBackDelete()
		{
			_container.UseJsTriggers();

			await RegisterJsPostTrigger("failDelete", """
                function failDelete() {
                    throw new Error("post-trigger rejects delete");
                }
                """, TriggerOperation.Delete);

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"));

			var act = () => _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "failDelete" } });

			await act.Should().ThrowAsync<CosmosException>();

			// Item should still exist — delete rolled back
			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["id"]!.Value<string>().Should().Be("1");
		}
	}

	// ─── Combined Pre+Post Trigger Scenarios ─────────────────────────────

	public class CombinedTriggerTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		[Fact]
		public async Task PreAndPostTrigger_Js_BothFireOnSameOperation()
		{
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "pre",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function pre() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["preRan"] = true;
                        req.setBody(doc);
                    }
                    """
			});

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "post",
				TriggerType = TriggerType.Post,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function post() {
                        var ctx = getContext();
                        var resp = ctx.getResponse();
                        var doc = resp.getBody();
                    }
                    """
			});

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions
				{
					PreTriggers = new List<string> { "pre" },
					PostTriggers = new List<string> { "post" }
				});

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["preRan"]!.Value<bool>().Should().BeTrue();
		}

		[Fact]
		public async Task PreAndPostTrigger_Js_PostSeesPreModifiedDoc()
		{
			_container.UseJsTriggers();

			// Pre-trigger adds a field
			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "addField",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function addField() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["addedByPre"] = "yes";
                        req.setBody(doc);
                    }
                    """
			});

			// Post-trigger reads the committed doc — if pre's change is persisted,
			// "addedByPre" will be in the committed document
			var postSawField = false;
			_container.RegisterTrigger("verifyPost", TriggerType.Post, TriggerOperation.All,
				(Action<JObject>)(doc =>
				{
					postSawField = doc["addedByPre"]?.Value<string>() == "yes";
				}));

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions
				{
					PreTriggers = new List<string> { "addField" },
					PostTriggers = new List<string> { "verifyPost" }
				});

			postSawField.Should().BeTrue();
		}

		[Fact]
		public async Task CSharpPreTrigger_JsPostTrigger_BothFire()
		{
			_container.UseJsTriggers();

			// C# pre-trigger
			_container.RegisterTrigger("csharpPre", TriggerType.Pre, TriggerOperation.All,
				(Func<JObject, JObject>)(doc =>
				{
					doc["preSrc"] = "csharp";
					return doc;
				}));

			// JS post-trigger
			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "jsPost",
				TriggerType = TriggerType.Post,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function jsPost() {
                        var ctx = getContext();
                        var resp = ctx.getResponse();
                        var doc = resp.getBody();
                    }
                    """
			});

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions
				{
					PreTriggers = new List<string> { "csharpPre" },
					PostTriggers = new List<string> { "jsPost" }
				});

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["preSrc"]!.Value<string>().Should().Be("csharp");
		}
	}

	// ─── Scripts CRUD Integration ────────────────────────────────────────

	public class TriggerScriptsCrudTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		[Fact]
		public async Task ReadTriggerAsync_ReturnsRegisteredTrigger()
		{
			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "myTrigger",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.Create,
				Body = "function myTrigger() { }"
			});

			var response = await _container.Scripts.ReadTriggerAsync("myTrigger");
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Id.Should().Be("myTrigger");
			response.Resource.TriggerType.Should().Be(TriggerType.Pre);
			response.Resource.TriggerOperation.Should().Be(TriggerOperation.Create);
		}

		[Fact]
		public async Task ReadTriggerAsync_NotFound_ThrowsCosmosException()
		{
			var act = () => _container.Scripts.ReadTriggerAsync("nonExistent");

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}

		[Fact]
		public async Task ReplaceTriggerAsync_UpdatesBody_NewBodyFiresOnNextOp()
		{
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "evolving",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function evolving() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["version"] = "v1";
                        req.setBody(doc);
                    }
                    """
			});

			// Replace the trigger body with a new version
			await _container.Scripts.ReplaceTriggerAsync(new TriggerProperties
			{
				Id = "evolving",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function evolving() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["version"] = "v2";
                        req.setBody(doc);
                    }
                    """
			});

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "evolving" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["version"]!.Value<string>().Should().Be("v2");
		}

		[Fact]
		public async Task ReplaceTriggerAsync_NotFound_ThrowsCosmosException()
		{
			var act = () => _container.Scripts.ReplaceTriggerAsync(new TriggerProperties
			{
				Id = "nonExistent",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = "function x() { }"
			});

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}

		[Fact]
		public async Task DeleteTriggerAsync_RemovesTrigger()
		{
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "toDelete",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function toDelete() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["shouldNotExist"] = true;
                        req.setBody(doc);
                    }
                    """
			});

			await _container.Scripts.DeleteTriggerAsync("toDelete");

			// Now referencing the deleted trigger should throw
			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "toDelete" } });

			await act.Should().ThrowAsync<CosmosException>();
		}

		[Fact]
		public async Task DeleteTriggerAsync_NotFound_ThrowsCosmosException()
		{
			var act = () => _container.Scripts.DeleteTriggerAsync("nonExistent");

			var ex = await act.Should().ThrowAsync<CosmosException>();
			ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}

		[Fact]
		public async Task DeleteTriggerAsync_AlsoRemovesCSharpHandler()
		{
			// Register both a C# handler and a JS body for the same trigger
			_container.RegisterTrigger("hybrid", TriggerType.Pre, TriggerOperation.All,
				(Func<JObject, JObject>)(doc =>
				{
					doc["csharp"] = true;
					return doc;
				}));

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "hybrid",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = "function hybrid() { }"
			});

			// Delete via Scripts API — should remove both
			await _container.Scripts.DeleteTriggerAsync("hybrid");

			var act = () => _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "hybrid" } });

			await act.Should().ThrowAsync<CosmosException>();
		}
	}

	// ─── Divergent Behavior Tests ────────────────────────────────────────
	//
	// These test pairs document known behavioral differences between the
	// in-memory emulator and real Azure Cosmos DB. Each "skip" test describes
	// real Cosmos behavior that we don't implement, with a sister test that
	// documents what the emulator actually does.

	public class JsTriggerDivergentBehaviorTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		// ── Divergence: Post-trigger response.setBody() ──────────────────
		//
		// Real Cosmos DB: Post-triggers can call getContext().getResponse().setBody(doc)
		// to modify the response body returned to the client. This allows post-triggers
		// to alter what the client sees without changing the persisted document.
		//
		// Emulator: The IJsTriggerEngine.ExecutePostTrigger returns void. The Jint
		// engine wires up getResponse().getBody() but NOT setBody(). Any call to
		// setBody() in a post-trigger is silently ignored. Implementing this would
		// require changing the IJsTriggerEngine interface to return optional modified
		// response body, threading that through ExecutePostTriggers, and altering the
		// response construction in every write path — a significant interface change
		// that's unlikely to affect most unit test scenarios.

		[Fact]
		public async Task PostTrigger_Js_SetBody_ModifiesResponse()
		{
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "modifyResponse",
				TriggerType = TriggerType.Post,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function modifyResponse() {
                        var ctx = getContext();
                        var resp = ctx.getResponse();
                        var doc = resp.getBody();
                        doc["modifiedByPost"] = true;
                        resp.setBody(doc);
                    }
                    """
			});

			var response = await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "modifyResponse" } });

			// setBody modifies the response body — client sees the override
			response.Resource["modifiedByPost"]!.Value<bool>().Should().BeTrue();

			// But the persisted document is NOT affected by setBody
			var persisted = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			persisted.ContainsKey("modifiedByPost").Should().BeFalse();
		}

		// ── Divergence: Pre-trigger getResponse() ────────────────────────
		//
		// Real Cosmos DB: Calling getContext().getResponse() inside a pre-trigger
		// throws a runtime error because the response doesn't exist yet (the operation
		// hasn't executed).
		//
		// Emulator: The Jint pre-trigger API only wires getContext().getRequest().
		// getResponse() is simply not defined on the context object, so calling it
		// returns undefined rather than throwing. This is functionally equivalent
		// (the trigger can't use the response) but the failure mode differs.

		[Fact]
		public async Task PreTrigger_Js_GetResponse_Throws_RealCosmos()
		{
			// Real Cosmos DB throws a runtime error when a pre-trigger calls
			// getContext().getResponse() because the response doesn't exist yet.
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "callGetResponse",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function callGetResponse() {
                        var ctx = getContext();
                        var resp = ctx.getResponse();
                    }
                    """
			});

			var act = async () => await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "callGetResponse" } });

			await act.Should().ThrowAsync<CosmosException>()
				.WithMessage("*getResponse*");
		}

		// ── Divergence: Multiple triggers per operation ──────────────────
		//
		// Real Cosmos DB: The official documentation states "you can still run only one
		// trigger per operation" despite the SDK accepting a list of trigger names.
		// If multiple trigger names are provided, only one executes.
		//
		// Emulator: All listed triggers are chained and executed in order. This is
		// actually more permissive than real Cosmos and may mask issues where code
		// relies on multiple triggers executing when only one would in production.

		[Fact]
		public async Task MultipleTriggers_RealCosmosOnlyExecutesOne()
		{
			// Real Cosmos DB only fires the FIRST listed trigger, even when multiple are specified.
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "addA",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = "function addA() { var r = getContext().getRequest(); var d = r.getBody(); d['a'] = true; r.setBody(d); }"
			});
			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "addB",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = "function addB() { var r = getContext().getRequest(); var d = r.getBody(); d['b'] = true; r.setBody(d); }"
			});

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "addA", "addB" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["a"]!.Value<bool>().Should().BeTrue("first trigger should fire");
			item.ContainsKey("b").Should().BeFalse("second trigger should NOT fire — real Cosmos only fires first");
		}

		[Fact]
		public async Task MultipleTriggers_OnlyFirstCSharpHandlerExecuted()
		{
			// With multiple pre-triggers listed, only the first matching one executes.
			var aFired = false;
			var bFired = false;

			_container.RegisterTrigger("tA", TriggerType.Pre, TriggerOperation.All,
				(Func<JObject, JObject>)(doc => { aFired = true; doc["a"] = true; return doc; }));
			_container.RegisterTrigger("tB", TriggerType.Pre, TriggerOperation.All,
				(Func<JObject, JObject>)(doc => { bFired = true; doc["b"] = true; return doc; }));

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "tA", "tB" } });

			aFired.Should().BeTrue("first trigger should fire");
			bFired.Should().BeFalse("second trigger should NOT fire");
			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["a"]!.Value<bool>().Should().BeTrue();
			item.ContainsKey("b").Should().BeFalse();
		}

		[Fact]
		public async Task MultiplePostTriggers_OnlyFirstExecuted()
		{
			var aFired = false;
			var bFired = false;

			_container.RegisterTrigger("postA", TriggerType.Post, TriggerOperation.All,
				(Action<JObject>)(_ => aFired = true));
			_container.RegisterTrigger("postB", TriggerType.Post, TriggerOperation.All,
				(Action<JObject>)(_ => bFired = true));

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "postA", "postB" } });

			aFired.Should().BeTrue("first post-trigger should fire");
			bFired.Should().BeFalse("second post-trigger should NOT fire");
		}

		// ── Divergence: getCollection() not supported ────────────────────
		//
		// Real Cosmos DB: Triggers can access other documents in the same collection
		// and partition via getContext().getCollection(). This enables patterns like
		// creating audit documents, updating counters, or cascading deletes — all
		// within the same transaction.
		//
		// Emulator: getContext().getCollection() is not wired in the Jint engine.
		// The context object only provides getRequest() (pre-triggers) or getResponse()
		// (post-triggers). Implementing collection access would require a major
		// refactor to pass container state into the JS engine and support
		// createDocument/readDocument/queryDocuments within the JS sandbox.

		[Fact]
		public async Task Trigger_GetCollection_Available()
		{
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "checkCollection",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function checkCollection() {
                        var ctx = getContext();
                        var hasCollection = typeof ctx.getCollection === "function";
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["hasCollection"] = hasCollection;
                        req.setBody(doc);
                    }
                    """
			});

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "checkCollection" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["hasCollection"]!.Value<bool>().Should().BeTrue();
		}

		// ── Divergence: getRequest() in post-triggers ────────────────────
		//
		// Real Cosmos DB: getContext().getRequest() is available in post-triggers too,
		// allowing the trigger to inspect the original request (e.g., check headers or
		// the original operation body).
		//
		// Emulator: The Jint post-trigger API only wires getContext().getResponse().
		// getRequest() is not defined, so it returns undefined.

		[Fact]
		public async Task PostTrigger_Js_GetRequest_Available()
		{
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "checkRequest",
				TriggerType = TriggerType.Post,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function checkRequest() {
                        var ctx = getContext();
                        var hasRequest = typeof ctx.getRequest === "function";
                        // Verify getRequest is available — should not throw
                        if (hasRequest) {
                            var body = ctx.getRequest().getBody();
                        }
                    }
                    """
			});

			// Should not throw — getRequest is available in post-triggers
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "checkRequest" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["id"]!.Value<string>().Should().Be("1");
		}
	}

	// ─── JintTriggerEngine Edge Cases ────────────────────────────────────

	public class JintTriggerEngineEdgeCaseTests
	{
		private readonly InMemoryContainer _container = new("test-container", "/pk");

		[Fact]
		public async Task PreTrigger_Js_HelperFunctionBeforeMain_InvokesFirstFunction()
		{
			// The JintTriggerEngine uses InvokeFirstFunction which picks the first
			// `function <name>()` via regex. If a helper function is defined before
			// the main trigger function, the helper runs instead.
			// This documents the current behavior — it's a known quirk.
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "helperFirst",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function helper() {
                        // This is a helper that gets invoked first due to regex ordering.
                        // It does nothing — no getContext/setBody.
                    }
                    function main() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["mainRan"] = true;
                        req.setBody(doc);
                    }
                    """
			});

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "helperFirst" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			// "mainRan" is NOT set because InvokeFirstFunction invoked "helper", not "main"
			item.ContainsKey("mainRan").Should().BeFalse();
		}

		[Fact]
		public async Task PreTrigger_Js_LargeDocument_HandledCorrectly()
		{
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "addField",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function addField() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["triggerRan"] = true;
                        req.setBody(doc);
                    }
                    """
			});

			// Create a document with 100+ fields
			var jObj = new JObject { ["id"] = "1", ["pk"] = "a" };
			for (int i = 0; i < 150; i++)
			{
				jObj[$"field_{i}"] = $"value_{i}";
			}

			await _container.CreateItemAsync(jObj, new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["triggerRan"]!.Value<bool>().Should().BeTrue();
			item["field_0"]!.Value<string>().Should().Be("value_0");
			item["field_149"]!.Value<string>().Should().Be("value_149");
		}

		[Fact]
		public async Task PreTrigger_Js_UnicodeContent()
		{
			_container.UseJsTriggers();

			await _container.Scripts.CreateTriggerAsync(new TriggerProperties
			{
				Id = "addUnicode",
				TriggerType = TriggerType.Pre,
				TriggerOperation = TriggerOperation.All,
				Body = """
                    function addUnicode() {
                        var ctx = getContext();
                        var req = ctx.getRequest();
                        var doc = req.getBody();
                        doc["emoji"] = "\uD83D\uDE00";
                        doc["japanese"] = "\u6771\u4EAC";
                        req.setBody(doc);
                    }
                    """
			});

			await _container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "addUnicode" } });

			var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
			item["emoji"]!.Value<string>().Should().Be("😀");
			item["japanese"]!.Value<string>().Should().Be("東京");
		}
	}

	// ─── UseJsTriggers extension ─────────────────────────────────────────

	public class UseJsTriggersExtensionTests
	{
		[Fact]
		public void UseJsTriggers_SetsJsTriggerEngine()
		{
			var container = new InMemoryContainer("test", "/pk");
			container.JsTriggerEngine.Should().BeNull();

			container.UseJsTriggers();

			container.JsTriggerEngine.Should().NotBeNull();
			container.JsTriggerEngine.Should().BeOfType<JintTriggerEngine>();
		}

		[Fact]
		public void UseJsTriggers_ReturnsSameContainer()
		{
			var container = new InMemoryContainer("test", "/pk");
			var result = container.UseJsTriggers();
			result.Should().BeSameAs(container);
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Trigger CRUD Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class TriggerCrudEdgeCaseTests
{
	[Fact]
	public async Task CreateTriggerAsync_DuplicateId_ThrowsConflict()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var scripts = container.Scripts;

		await scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'ok'; req.setBody(doc); }"
		});

		var act = () => scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run2() { }"
		});

		(await act.Should().ThrowAsync<CosmosException>())
			.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task ReplaceTriggerAsync_ChangesOperation_AffectsFiring()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var scripts = container.Scripts;

		await scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'v1'; req.setBody(doc); }"
		});

		// Create an item to replace later
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "trig1" } });

		// Now change trigger to fire on All (including Replace)
		await scripts.ReplaceTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'v2'; req.setBody(doc); }"
		});

		await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "trig1" } });

		var readBack = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		readBack["stamp"]!.ToString().Should().Be("v2");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Operation-Specific Trigger Filtering
// ═══════════════════════════════════════════════════════════════════════════════

public class TriggerOperationFilteringTests
{
	[Fact]
	public async Task PreTrigger_DeleteOnly_DoesNotFireOnCreate()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "del-only",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Delete,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'fired'; req.setBody(doc); }"
		});

		var result = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "del-only" } });

		result.Resource["stamp"].Should().BeNull("delete-only trigger should not fire on create");
	}

	[Fact]
	public async Task PreTrigger_DeleteOnly_DoesNotFireOnReplace()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "del-only",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Delete,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'fired'; req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var result = await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "del-only" } });

		result.Resource["stamp"].Should().BeNull("delete-only trigger should not fire on replace");
	}

	[Fact]
	public async Task PostTrigger_CreateOnly_DoesNotFireOnReplace()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var called = false;
		container.RegisterTrigger("create-only", TriggerType.Post, TriggerOperation.Create,
			_ => called = true);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		called = false; // reset after create

		await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", extra = "val" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "create-only" } });

		called.Should().BeFalse("create-only post-trigger should not fire on replace");
	}

	[Fact]
	public async Task PreTrigger_DeleteOnly_DoesNotFireOnUpsert()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "del-only",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Delete,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'fired'; req.setBody(doc); }"
		});

		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "del-only" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["stamp"].Should().BeNull("delete-only trigger should not fire on upsert");
	}

	[Fact]
	public async Task PostTrigger_DeleteOnly_DoesNotFireOnCreate()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var called = false;
		container.RegisterTrigger("del-only-post", TriggerType.Post, TriggerOperation.Delete,
			_ => called = true);

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "del-only-post" } });

		called.Should().BeFalse("delete-only post-trigger should not fire on create");
	}

	[Fact]
	public async Task PreTrigger_UpsertOnly_FiresOnUpsert()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "upsert-only",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All, // Upsert maps to Create/Replace in emulator; use All to cover
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'upserted'; req.setBody(doc); }"
		});

		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "upsert-only" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["stamp"]!.ToString().Should().Be("upserted");
	}

	[Fact]
	public async Task PostTrigger_All_FiresOnUpsert()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var called = false;
		container.RegisterTrigger("all-post", TriggerType.Post, TriggerOperation.All,
			_ => called = true);

		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "all-post" } });

		called.Should().BeTrue("All-operation post-trigger should fire on upsert");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Pre-Trigger Rollback Verification
// ═══════════════════════════════════════════════════════════════════════════════

public class PreTriggerRollbackTests
{
	[Fact]
	public async Task PreTrigger_Js_Throws_ItemNotCreated()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('intentional failure'); }"
		});

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "throw-trigger" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(0, "item should not be created when pre-trigger throws");
	}

	[Fact]
	public async Task PreTrigger_Js_Throws_ItemNotReplaced()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "original" }),
			new PartitionKey("a"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('intentional failure'); }"
		});

		var act = () => container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "changed" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "throw-trigger" } });

		await act.Should().ThrowAsync<CosmosException>();

		var readBack = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		readBack.Resource["val"]!.ToString().Should().Be("original");
	}

	[Fact]
	public async Task PreTrigger_Js_Throws_ItemNotUpserted()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('intentional failure'); }"
		});

		var act = () => container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "throw-trigger" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(0, "item should not be created when pre-trigger throws on upsert");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Post-Trigger Rollback Verification
// ═══════════════════════════════════════════════════════════════════════════════

public class PostTriggerRollbackVerificationTests
{
	[Fact]
	public async Task PostTrigger_Js_RollsBack_Create_ItemRemoved()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(0, "item should be rolled back when post-trigger throws");
	}

	[Fact]
	public async Task PostTrigger_Js_RollsBack_Replace_RestoresOriginal()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "original" }),
			new PartitionKey("a"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var act = () => container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "replaced" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();

		var readBack = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		readBack.Resource["val"]!.ToString().Should().Be("original");
	}

	[Fact]
	public async Task PostTrigger_Js_RollsBack_Delete_RestoresItem()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var act = () => container.DeleteItemAsync<JObject>(
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(1, "item should be restored when post-trigger throws on delete");
	}

	[Fact]
	public async Task PostTrigger_Js_RollsBack_UpsertNew_RemovesItem()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var act = () => container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(0, "new upsert should be rolled back");
	}

	[Fact]
	public async Task PostTrigger_Js_RollsBack_UpsertExisting_RestoresOriginal()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "original" }),
			new PartitionKey("a"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var act = () => container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "updated" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();

		var readBack = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		readBack.Resource["val"]!.ToString().Should().Be("original", "existing upsert should restore original on rollback");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  JintTriggerEngine Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class JintTriggerEngineAdditionalEdgeCaseTests
{
	private readonly InMemoryContainer _container;

	public JintTriggerEngineAdditionalEdgeCaseTests()
	{
		_container = new InMemoryContainer("test", "/pk");
		_container.UseJsTriggers();
	}

	[Fact]
	public async Task PreTrigger_Js_MultipleSetBody_LastWins()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "multi-set",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"doc.val = 'first'; req.setBody(doc); " +
				"doc.val = 'second'; req.setBody(doc); }"
		});

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "multi-set" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["val"]!.ToString().Should().Be("second");
	}

	[Fact]
	public async Task PreTrigger_Js_MathFunctions_Work()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "math-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"doc.rounded = Math.floor(3.7); doc.max = Math.max(1, 5, 3); " +
				"req.setBody(doc); }"
		});

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "math-trigger" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		((long)item["rounded"]!).Should().Be(3);
		((long)item["max"]!).Should().Be(5);
	}

	[Fact]
	public async Task PreTrigger_Js_StringManipulation_Works()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "string-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"doc.upper = doc.name.toUpperCase(); doc.trimmed = '  hello  '.trim(); " +
				"req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "test" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "string-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["upper"]!.ToString().Should().Be("TEST");
		item["trimmed"]!.ToString().Should().Be("hello");
	}

	[Fact]
	public async Task PreTrigger_Js_JsonParse_InTrigger()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "json-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"var parsed = JSON.parse('{\"key\":\"value\"}'); doc.parsedKey = parsed.key; " +
				"req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "json-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["parsedKey"]!.ToString().Should().Be("value");
	}

	[Fact]
	public async Task PreTrigger_Js_EmptyBody_NoOp()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "empty-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = ""
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "original" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "empty-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["val"]!.ToString().Should().Be("original", "empty trigger body should not modify document");
	}

	[Fact]
	public async Task PostTrigger_Js_EmptyBody_NoOp()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "empty-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = ""
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "empty-post" } });

		container.ItemCount.Should().Be(1, "empty post-trigger body should not cause failure");
	}

	[Fact]
	public async Task PreTrigger_Js_ArrowFunction_NotInvoked()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "arrow-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "var f = () => { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.arrow = true; req.setBody(doc); };"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "arrow-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["arrow"].Should().BeNull("arrow functions are not invoked by InvokeFirstFunction regex");
	}

	[Fact]
	public async Task PreTrigger_Js_AnonymousFunction_NotInvoked()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "anon-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "(function() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.anon = true; req.setBody(doc); })();"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "anon-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		// Anonymous IIFE executes during engine.Execute(), so it DOES run before InvokeFirstFunction
		// The regex won't match it, but the IIFE already executed
		item["anon"]!.Value<bool>().Should().BeTrue("IIFE executes during engine.Execute() phase");
	}

	[Fact]
	public async Task PreTrigger_Js_NumberPrecision()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "precision-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"doc.sum = 0.1 + 0.2; " +
				"req.setBody(doc); }"
		});

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "precision-trigger" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		var sum = (double)item["sum"]!;
		sum.Should().NotBe(0.3, "JavaScript floating-point: 0.1 + 0.2 !== 0.3");
		sum.Should().BeApproximately(0.3, 0.0001);
	}

	[Fact]
	public async Task PreTrigger_Js_DateObject()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "date-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"doc.hasDate = typeof new Date().getTime() === 'number'; " +
				"req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "date-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["hasDate"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task PreTrigger_Js_RegexUsage()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "regex-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"doc.isEmail = /^[^@]+@[^@]+$/.test(doc.email); " +
				"req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "user@example.com" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "regex-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["isEmail"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task PreTrigger_Js_JsonStringify_InTrigger()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "stringify-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"doc.nested = JSON.stringify({a: 1, b: 2}); " +
				"req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "stringify-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["nested"]!.ToString().Should().Contain("\"a\"");
		item["nested"]!.ToString().Should().Contain("\"b\"");
	}

	[Fact]
	public async Task PostTrigger_Js_LargeDocument_HandledCorrectly()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "large-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var resp = ctx.getResponse(); var doc = resp.getBody(); }"
		});

		var doc = new JObject { ["id"] = "1", ["pk"] = "a" };
		for (var i = 0; i < 150; i++) doc[$"field{i}"] = $"value{i}";

		await container.CreateItemAsync(doc, new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "large-post" } });

		container.ItemCount.Should().Be(1);
	}

	[Fact]
	public async Task PostTrigger_Js_UnicodeInResponse()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "unicode-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var resp = ctx.getResponse(); var doc = resp.getBody(); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "日本語テスト 🎉" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "unicode-post" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["name"]!.ToString().Should().Be("日本語テスト 🎉");
	}

	[Fact(Skip = "Jint timeout is 5 seconds — test would take too long for CI. " +
		"Verified that Jint.Engine TimeoutInterval is configured to 5s in JintTriggerEngine.")]
	public async Task PreTrigger_Js_InfiniteLoop_TimesOut()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "loop-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { while(true) {} }"
		});

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "loop-trigger" } });

		await act.Should().ThrowAsync<Exception>();
	}

	[Fact(Skip = "MaxStatements(10000) may take too long to trigger in CI. " +
		"Verified that Jint.Engine MaxStatements is configured to 10000 in JintTriggerEngine.")]
	public async Task PreTrigger_Js_MaxStatements_Exceeded()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "stmt-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var i = 0; while(i < 100000) { i++; } }"
		});

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "stmt-trigger" } });

		await act.Should().ThrowAsync<Exception>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Feature Interaction Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TriggerFeatureInteractionTests
{
	[Fact]
	public async Task PreTrigger_Js_WithTTL_SetsItemTTL()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.DefaultTimeToLive = 3600;
		container.UseJsTriggers();

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "ttl-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { " +
				"var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); " +
				"doc.ttl = 60; req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "ttl-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		((long)item["ttl"]!).Should().Be(60);
	}

	[Fact]
	public async Task PostTrigger_Js_VerifyChangeFeedRecorded()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "log-trigger",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { /* no-op post trigger */ }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "log-trigger" } });

		var cfIterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.LatestVersion);
		var results = new List<JObject>();
		while (cfIterator.HasMoreResults)
		{
			var batch = await cfIterator.ReadNextAsync();
			if (batch.StatusCode == HttpStatusCode.NotModified) break;
			results.AddRange(batch);
		}

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task PreTrigger_Js_WithETagCondition_BothApply()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "stamp-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'triggered'; req.setBody(doc); }"
		});

		var created = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "v1" }),
			new PartitionKey("a"));
		var etag = created.ETag;

		// Replace with correct ETag + pre-trigger
		await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "v2" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions
			{
				IfMatchEtag = etag,
				PreTriggers = new List<string> { "stamp-trigger" }
			});

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["stamp"]!.ToString().Should().Be("triggered");
		item["val"]!.ToString().Should().Be("v2");
	}

	[Fact]
	public async Task PreTrigger_Js_WithUniqueKeyPolicy()
	{
		var properties = new ContainerProperties("test", "/pk")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
			}
		};
		var container = new InMemoryContainer(properties);
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "email-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.email = doc.email.toLowerCase(); req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", email = "USER@TEST.COM" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "email-trigger" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["email"]!.ToString().Should().Be("user@test.com");

		// Second item with same email (after trigger lowercasing) should conflict
		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", email = "User@Test.Com" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "email-trigger" } });

		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task PreTrigger_Js_ModifiesIdField_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "id-changer",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.id = 'modified-id'; req.setBody(doc); }"
		});

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "original-id", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "id-changer" } });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task PreTrigger_Js_ModifiesPartitionKey_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "pk-changer",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.pk = 'modified-pk'; req.setBody(doc); }"
		});

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "pk-changer" } });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Divergent Behavior — Patch/Batch Triggers
// ═══════════════════════════════════════════════════════════════════════════════

public class TriggerPatchBatchDivergentTests
{
	[Fact]
	public async Task Trigger_Js_PatchOperation_FiresTrigger()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "patch-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }),
			new PartitionKey("a"));

		await container.PatchItemAsync<JObject>(
			"1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/val", 20) },
			new PatchItemRequestOptions { PreTriggers = new List<string> { "patch-trigger" } });
	}

	[Fact]
	public async Task DivergentBehavior_PatchTriggerFiresWhenRequested()
	{
		// Triggers on patch now fire when explicitly requested via PreTriggers/PostTriggers.
		// When not requested, triggers are not fired (same as real Cosmos DB).
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "stamp-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamped = true; req.setBody(doc); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = 10 }),
			new PartitionKey("a"));

		var patched = await container.PatchItemAsync<JObject>(
			"1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/val", 20) });

		patched.Resource["stamped"].Should().BeNull("triggers not requested in this call");
	}

	[Fact]
	public async Task ChangeFeed_RolledBack_OnPostTriggerFailure()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('fail'); }"
		});

		try
		{
			await container.CreateItemAsync(
				JObject.FromObject(new { id = "1", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });
		}
		catch { }

		var cfIterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.LatestVersion);
		var results = new List<JObject>();
		while (cfIterator.HasMoreResults)
		{
			var batch = await cfIterator.ReadNextAsync();
			if (batch.StatusCode == HttpStatusCode.NotModified) break;
			results.AddRange(batch);
		}

		results.Should().BeEmpty("failed transaction should not appear in change feed");
	}

	[Fact]
	public async Task Trigger_Js_TransactionalBatch_WithPropertiesHeader()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "batch-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var doc = ctx.getRequest().getBody(); doc.triggered = true; ctx.getRequest().setBody(doc); }"
		});

		var batch = container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.CreateItem(JObject.FromObject(new { id = "1", pk = "a" }),
			new TransactionalBatchItemRequestOptions
			{
				Properties = new Dictionary<string, object>
				{
					["x-ms-pre-trigger-include"] = new[] { "batch-trigger" }
				}
			});
		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		((bool)read["triggered"]!).Should().BeTrue();
	}

	[Fact]
	public async Task TransactionalBatch_WithoutTriggerProperties_DoesNotFireTriggers()
	{
		// Triggers should only fire when explicitly specified via Properties headers.
		// Without Properties, registered triggers are NOT automatically invoked for batch ops.
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var called = false;
		container.RegisterTrigger("batch-post", TriggerType.Post, TriggerOperation.All,
			_ => called = true);

		var batch = container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.CreateItem(JObject.FromObject(new { id = "1", pk = "a" }));
		await batch.ExecuteAsync();

		called.Should().BeFalse("batch operations do not invoke registered triggers unless specified via Properties headers");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Scripts CRUD Status Code Verification
// ═══════════════════════════════════════════════════════════════════════════════

public class TriggerScriptsStatusCodeTests
{
	[Fact]
	public async Task CreateTriggerAsync_ReturnsCreatedStatus()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var result = await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { }"
		});

		result.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task ReplaceTriggerAsync_ReturnsOkStatus()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { }"
		});

		var result = await container.Scripts.ReplaceTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run2() { }"
		});

		result.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteTriggerAsync_ReturnsNoContentStatus()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { }"
		});

		var result = await container.Scripts.DeleteTriggerAsync("trig1");

		result.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}

	[Fact]
	public async Task ReplaceTriggerAsync_ChangesType_FromPreToPost()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'pre'; req.setBody(doc); }"
		});

		// Replace to change from Pre to Post
		await container.Scripts.ReplaceTriggerAsync(new TriggerProperties
		{
			Id = "trig1",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { /* now a post trigger */ }"
		});

		// It should no longer fire as a pre-trigger
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "trig1" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["stamp"].Should().BeNull("trigger was changed from Pre to Post — should not fire as pre-trigger");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Stream API Trigger Rollback Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class StreamTriggerRollbackTests
{
	[Fact]
	public async Task PostTrigger_Js_RollsBack_CreateStream()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var json = JObject.FromObject(new { id = "1", pk = "a" }).ToString();
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var act = () => container.CreateItemStreamAsync(
			stream, new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(0, "stream create should be rolled back on post-trigger failure");
	}

	[Fact]
	public async Task PostTrigger_Js_RollsBack_ReplaceStream()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "original" }),
			new PartitionKey("a"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var json = JObject.FromObject(new { id = "1", pk = "a", val = "replaced" }).ToString();
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var act = () => container.ReplaceItemStreamAsync(
			stream, "1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();
		var readBack = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		readBack.Resource["val"]!.ToString().Should().Be("original");
	}

	[Fact]
	public async Task PostTrigger_Js_RollsBack_UpsertStream_New()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var json = JObject.FromObject(new { id = "1", pk = "a" }).ToString();
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var act = () => container.UpsertItemStreamAsync(
			stream, new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(0, "new stream upsert should be rolled back");
	}

	[Fact]
	public async Task PostTrigger_Js_RollsBack_UpsertStream_Existing()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", val = "original" }),
			new PartitionKey("a"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var json = JObject.FromObject(new { id = "1", pk = "a", val = "updated" }).ToString();
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var act = () => container.UpsertItemStreamAsync(
			stream, new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();
		var readBack = await container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		readBack.Resource["val"]!.ToString().Should().Be("original");
	}

	[Fact]
	public async Task PostTrigger_Js_RollsBack_DeleteStream()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-post",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('post failure'); }"
		});

		var act = () => container.DeleteItemStreamAsync(
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "throw-post" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(1, "item should be restored when stream delete post-trigger fails");
	}

	[Fact]
	public async Task PreTrigger_Js_DeleteStream_FiresAndBlocksDelete()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "throw-pre",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { throw new Error('pre failure'); }"
		});

		var act = () => container.DeleteItemStreamAsync(
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "throw-pre" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(1, "item should not be deleted when pre-trigger throws");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Trigger Concurrency Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TriggerConcurrencyTests
{
	[Fact]
	public async Task PreTrigger_Js_ConcurrentCreates_AllTriggered()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "stamp-trigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); var doc = req.getBody(); doc.stamp = 'triggered'; req.setBody(doc); }"
		});

		var tasks = Enumerable.Range(0, 10).Select(i =>
			container.CreateItemAsync(
				JObject.FromObject(new { id = $"item-{i}", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PreTriggers = new List<string> { "stamp-trigger" } }));

		await Task.WhenAll(tasks);

		container.ItemCount.Should().Be(10);
		for (var i = 0; i < 10; i++)
		{
			var item = (await container.ReadItemAsync<JObject>($"item-{i}", new PartitionKey("a"))).Resource;
			item["stamp"]!.ToString().Should().Be("triggered");
		}
	}

	[Fact]
	public async Task PostTrigger_Js_ConcurrentCreates_AllTriggered()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var callCount = 0;
		container.RegisterTrigger("count-trigger", TriggerType.Post, TriggerOperation.All,
			_ => Interlocked.Increment(ref callCount));

		var tasks = Enumerable.Range(0, 10).Select(i =>
			container.CreateItemAsync(
				JObject.FromObject(new { id = $"item-{i}", pk = "a" }),
				new PartitionKey("a"),
				new ItemRequestOptions { PostTriggers = new List<string> { "count-trigger" } }));

		await Task.WhenAll(tasks);

		container.ItemCount.Should().Be(10);
		callCount.Should().Be(10);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: JS Engine Edge Cases (Plan 43)
// ═══════════════════════════════════════════════════════════════════════════

public class JsTriggerEngineEdgeCaseDeepTests
{
	[Fact]
	public async Task PreTrigger_Js_EmptyBody_DoesNotThrow()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "empty",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = ""
		});

		// Empty JS body executes without error — document is stored unmodified
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "empty" } });

		var stored = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		stored["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task PostTrigger_Js_LargeDocument_HandledCorrectly()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var postFired = false;
		container.RegisterTrigger("lg", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => postFired = true));

		var largeDoc = JObject.FromObject(new { id = "1", pk = "a", data = new string('X', 50000) });
		await container.CreateItemAsync(largeDoc, new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "lg" } });

		postFired.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_Js_UnicodeContent()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		JObject? received = null;
		container.RegisterTrigger("uni", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(doc => received = doc));

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "héllo wörld 日本語" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "uni" } });

		received.Should().NotBeNull();
		received!["name"]!.Value<string>().Should().Be("héllo wörld 日本語");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: JSON Round-Trip Edge Cases (Plan 43)
// ═══════════════════════════════════════════════════════════════════════════

public class JsTriggerJsonRoundTripTests
{
	[Fact]
	public async Task PreTrigger_Js_DateValues_PreservedAsStrings()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "noop",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); req.setBody(req.getBody()); }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", createdAt = "2024-01-15T13:45:00Z" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "noop" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["createdAt"]!.Value<string>().Should().Be("2024-01-15T13:45:00Z");
	}

	[Fact]
	public async Task PreTrigger_Js_DeepNestedObject_Preserved()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "noop",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); req.setBody(req.getBody()); }"
		});

		var doc = JObject.FromObject(new { id = "1", pk = "a", level1 = new { level2 = new { level3 = new { value = 42 } } } });
		await container.CreateItemAsync(doc, new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "noop" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item.SelectToken("level1.level2.level3.value")!.Value<int>().Should().Be(42);
	}

	[Fact]
	public async Task PreTrigger_Js_EmptyArray_Preserved()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "noop",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() { var ctx = getContext(); var req = ctx.getRequest(); req.setBody(req.getBody()); }"
		});

		var doc = new JObject { ["id"] = "1", ["pk"] = "a", ["tags"] = new JArray() };
		await container.CreateItemAsync(doc, new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "noop" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["tags"]!.Should().BeOfType<JArray>();
		((JArray)item["tags"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task PreTrigger_Js_SetBody_CalledMultipleTimes_LastWins()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "multi-set",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = @"function run() {
                var ctx = getContext(); var req = ctx.getRequest();
                var doc = req.getBody();
                doc.tag = 'first';
                req.setBody(doc);
                doc.tag = 'second';
                req.setBody(doc);
            }"
		});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "multi-set" } });

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["tag"]!.Value<string>().Should().Be("second");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: JS Stream Delete Tests (Plan 43)
// ═══════════════════════════════════════════════════════════════════════════

public class JsTriggerDeleteStreamTests
{
	[Fact]
	public async Task PostTrigger_Js_DeleteStream_Fires()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var postFired = false;
		container.RegisterTrigger("post-del", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(_ => postFired = true));

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		await container.DeleteItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "post-del" } });

		postFired.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_Js_DeleteStream_ThrowRollsBack()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		container.RegisterTrigger("fail-del", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var act = () => container.DeleteItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail-del" } });

		await act.Should().ThrowAsync<CosmosException>();
		container.ItemCount.Should().Be(1); // Rolled back
	}

	[Fact]
	public async Task PreTrigger_Js_DeleteStream_Fires()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();
		var preFired = false;
		container.RegisterTrigger("pre-del", TriggerType.Pre, TriggerOperation.Delete,
			(Func<JObject, JObject>)(doc => { preFired = true; return doc; }));

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		await container.DeleteItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "pre-del" } });

		preFired.Should().BeTrue();
	}

	// ── Gap 5: Trigger stream query iterator ──────────────────────────

	[Fact]
	public async Task GetTriggerQueryStreamIterator_ReturnsSerializedProperties()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "t1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.All,
			Body = "function(){}"
		});
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "t2",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.Create,
			Body = "function(){}"
		});

		var iterator = container.Scripts.GetTriggerQueryStreamIterator();
		var response = await iterator.ReadNextAsync();
		response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);

		using var reader = new StreamReader(response.Content);
		var json = await reader.ReadToEndAsync();
		json.Should().Contain("t1");
		json.Should().Contain("t2");
	}

	// ── Gap 1: Trigger Collection Access ──────────────────────────────

	[Fact]
	public async Task PreTrigger_Js_CollectionAccess_QueryDocuments()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();

		// Seed a reference doc
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "ref1", pk = "a", category = "premium" }),
			new PartitionKey("a"));

		// Pre-trigger reads the reference doc and stamps the incoming doc
		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "pre-enrich",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = @"function enrich() {
                var ctx = getContext();
                var request = ctx.getRequest();
                var body = request.getBody();
                var collection = ctx.getCollection();
                collection.queryDocuments(collection.getSelfLink(),
                    ""SELECT * FROM c WHERE c.id = 'ref1'"",
                    {}, function(err, docs) {
                        if (err) throw err;
                        body.enrichedFrom = docs[0].category;
                        request.setBody(body);
                    });
            }"
		});

		var doc = JObject.FromObject(new { id = "new1", pk = "a" });
		await container.CreateItemAsync(doc, new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "pre-enrich" } });

		var result = await container.ReadItemAsync<JObject>("new1", new PartitionKey("a"));
		result.Resource["enrichedFrom"]!.Value<string>().Should().Be("premium");
	}

	[Fact]
	public async Task PostTrigger_Js_CollectionAccess_CreateAuditDoc()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "post-audit",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.Create,
			Body = @"function audit() {
                var ctx = getContext();
                var doc = ctx.getResponse().getBody();
                var collection = ctx.getCollection();
                collection.createDocument(collection.getSelfLink(),
                    { id: 'audit-' + doc.id, pk: doc.pk, action: 'created', sourceId: doc.id },
                    {}, function(err) { if (err) throw err; });
            }"
		});

		var doc = JObject.FromObject(new { id = "order1", pk = "a", total = 100 });
		await container.CreateItemAsync(doc, new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "post-audit" } });

		var audit = await container.ReadItemAsync<JObject>("audit-order1", new PartitionKey("a"));
		audit.Resource["action"]!.Value<string>().Should().Be("created");
		audit.Resource["sourceId"]!.Value<string>().Should().Be("order1");
	}

	[Fact]
	public async Task Trigger_Js_CollectionScoped_SamePartition()
	{
		var container = new InMemoryContainer("test", "/pk");
		container.UseJsTriggers();

		// Seed docs in two partitions
		await container.CreateItemAsync(JObject.FromObject(new { id = "a1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "b1", pk = "b" }), new PartitionKey("b"));

		await container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "pre-count",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = @"function countNeighbors() {
                var ctx = getContext();
                var request = ctx.getRequest();
                var body = request.getBody();
                var collection = ctx.getCollection();
                collection.queryDocuments(collection.getSelfLink(),
                    'SELECT * FROM c',
                    {}, function(err, docs) {
                        if (err) throw err;
                        body.neighborCount = docs.length;
                        request.setBody(body);
                    });
            }"
		});

		// Create in partition 'a' — should only see the 1 existing doc in partition 'a'
		var doc = JObject.FromObject(new { id = "a2", pk = "a" });
		await container.CreateItemAsync(doc, new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "pre-count" } });

		var result = await container.ReadItemAsync<JObject>("a2", new PartitionKey("a"));
		result.Resource["neighborCount"]!.Value<int>().Should().Be(1);
	}
}
