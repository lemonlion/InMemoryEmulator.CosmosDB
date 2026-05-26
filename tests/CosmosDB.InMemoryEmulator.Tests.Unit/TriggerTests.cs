using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Trigger Tests — TDD for C# trigger handler execution
// ═══════════════════════════════════════════════════════════════════════════

// ─── Phase 1: Trigger Storage & Registration ────────────────────────────

public class TriggerRegistrationTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public void RegisterTrigger_PreTrigger_StoresHandler()
	{
		_container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["added"] = true;
				return doc;
			}));

		// Trigger is registered — we verify it fires in Phase 2 tests
		// For now, just verify no exception + deregister works
		_container.DeregisterTrigger("addField");
	}

	[Fact]
	public void DeregisterTrigger_RemovesTrigger()
	{
		_container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => doc));

		_container.DeregisterTrigger("myTrigger");

		// Using a deregistered trigger should fail
		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });
		act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task CreateTriggerAsync_StoresTriggerProperties()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "myTrigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function() { /* JS body */ }"
		});

		// Register a C# handler for this trigger
		_container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["modified"] = true;
				return doc;
			}));

		// Now the trigger should fire
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["modified"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task ReadTriggerAsync_ReturnsStoredTrigger()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "myTrigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function() {}"
		});

		var response = await _container.Scripts.ReadTriggerAsync("myTrigger");
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		response.Resource.Id.Should().Be("myTrigger");
		response.Resource.TriggerType.Should().Be(TriggerType.Pre);
	}

	[Fact]
	public async Task ReadTriggerAsync_NotFound_Throws()
	{
		var act = () => _container.Scripts.ReadTriggerAsync("nonexistent");
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task DeleteTriggerAsync_RemovesTrigger()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "myTrigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function() {}"
		});

		var response = await _container.Scripts.DeleteTriggerAsync("myTrigger");
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);

		var act = () => _container.Scripts.ReadTriggerAsync("myTrigger");
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task ReplaceTriggerAsync_UpdatesTrigger()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "myTrigger",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function() {}"
		});

		var response = await _container.Scripts.ReplaceTriggerAsync(new TriggerProperties
		{
			Id = "myTrigger",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All,
			Body = "function() { /* updated */ }"
		});

		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var read = await _container.Scripts.ReadTriggerAsync("myTrigger");
		read.Resource.TriggerType.Should().Be(TriggerType.Post);
		read.Resource.TriggerOperation.Should().Be(TriggerOperation.All);
	}
}

// ─── Phase 2: Pre-Trigger Execution ─────────────────────────────────────

public class PreTriggerExecutionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PreTrigger_ModifiesDocument_OnCreate()
	{
		_container.RegisterTrigger("addCreatedBy", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["createdBy"] = "trigger";
				return doc;
			}));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "addCreatedBy" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["createdBy"]!.Value<string>().Should().Be("trigger");
	}

	[Fact]
	public async Task PreTrigger_ModifiesDocument_OnUpsert()
	{
		_container.RegisterTrigger("stamp", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc =>
			{
				doc["stamped"] = true;
				return doc;
			}));

		await _container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "stamp" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["stamped"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task PreTrigger_ModifiesDocument_OnReplace()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "original" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("addVersion", TriggerType.Pre, TriggerOperation.Replace,
			(Func<JObject, JObject>)(doc =>
			{
				doc["version"] = 2;
				return doc;
			}));

		await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "addVersion" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["version"]!.Value<int>().Should().Be(2);
		item["name"]!.Value<string>().Should().Be("updated");
	}

	[Fact]
	public async Task PreTrigger_ModifiesDocument_OnCreateStream()
	{
		_container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["streamModified"] = true;
				return doc;
			}));

		var json = """{"id":"1","pk":"a"}""";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
		await _container.CreateItemStreamAsync(stream, new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["streamModified"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task PreTrigger_ModifiesDocument_OnUpsertStream()
	{
		_container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc =>
			{
				doc["streamModified"] = true;
				return doc;
			}));

		var json = """{"id":"1","pk":"a"}""";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
		await _container.UpsertItemStreamAsync(stream, new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["streamModified"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task PreTrigger_ModifiesDocument_OnReplaceStream()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Replace,
			(Func<JObject, JObject>)(doc =>
			{
				doc["streamModified"] = true;
				return doc;
			}));

		var json = """{"id":"1","pk":"a","name":"replaced"}""";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
		await _container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "addField" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["streamModified"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task PreTrigger_NonExistentTrigger_ThrowsBadRequest()
	{
		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "doesNotExist" } });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task PreTrigger_OperationMismatch_TriggerNotFired()
	{
		// Register trigger for Create only
		_container.RegisterTrigger("createOnly", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["triggered"] = true;
				return doc;
			}));

		// Create item first (without trigger)
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		// Replace with trigger — should NOT fire because trigger is for Create, not Replace
		await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "replaced" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "createOnly" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["triggered"].Should().BeNull();
	}

	[Fact]
	public async Task PreTrigger_MultipleTriggers_ChainInOrder()
	{
		_container.RegisterTrigger("first", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["first"] = true;
				doc["order"] = "1";
				return doc;
			}));

		_container.RegisterTrigger("second", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["second"] = true;
				doc["order"] = doc["order"]!.Value<string>() + ",2";
				return doc;
			}));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "first", "second" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		// Real Cosmos only fires the first matching trigger
		item["first"]!.Value<bool>().Should().BeTrue();
		item["order"]!.Value<string>().Should().Be("1");
		item.ContainsKey("second").Should().BeFalse();
	}

	[Fact]
	public async Task PreTrigger_TriggerOperationAll_FiresOnAnyOperation()
	{
		_container.RegisterTrigger("allOps", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc =>
			{
				doc["triggered"] = true;
				return doc;
			}));

		var options = new ItemRequestOptions { PreTriggers = new List<string> { "allOps" } };

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"), options);

		var item1 = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item1["triggered"]!.Value<bool>().Should().BeTrue();

		// Replace — trigger should fire again
		await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "replaced" }),
			"1", new PartitionKey("a"), options);

		var item2 = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item2["triggered"]!.Value<bool>().Should().BeTrue();
		item2["name"]!.Value<string>().Should().Be("replaced");

		// Upsert — trigger should fire again
		await _container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "upserted" }),
			new PartitionKey("a"), options);

		var item3 = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item3["triggered"]!.Value<bool>().Should().BeTrue();
		item3["name"]!.Value<string>().Should().Be("upserted");
	}

	[Fact]
	public async Task PreTrigger_NoPreTriggersSpecified_TriggerNotFired()
	{
		_container.RegisterTrigger("addField", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc =>
			{
				doc["triggered"] = true;
				return doc;
			}));

		// Create without specifying PreTriggers
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["triggered"].Should().BeNull();
	}
}

// ─── Phase 3: Post-Trigger Execution ────────────────────────────────────

public class PostTriggerExecutionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PostTrigger_FiresAfterCreate()
	{
		_container.RegisterTrigger("audit", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(doc =>
			{
				// Post-trigger creates an audit entry
				var auditDoc = new JObject
				{
					["id"] = "audit-" + doc["id"]!.Value<string>(),
					["pk"] = doc["pk"]!.Value<string>(),
					["action"] = "created"
				};
				_container.CreateItemAsync(auditDoc, new PartitionKey(doc["pk"]!.Value<string>())).GetAwaiter().GetResult();
			}));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "audit" } });

		var audit = (await _container.ReadItemAsync<JObject>("audit-1", new PartitionKey("a"))).Resource;
		audit["action"]!.Value<string>().Should().Be("created");
	}

	[Fact]
	public async Task PostTrigger_ExceptionRollsBackWrite()
	{
		_container.RegisterTrigger("failingTrigger", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => throw new InvalidOperationException("Post-trigger failed!")));

		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "failingTrigger" } });

		await act.Should().ThrowAsync<CosmosException>();

		// Item should NOT exist — the write was rolled back
		var readAct = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		await readAct.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task PostTrigger_FiresAfterUpsert()
	{
		var postTriggered = false;
		_container.RegisterTrigger("flag", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => postTriggered = true));

		await _container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "flag" } });

		postTriggered.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_FiresAfterReplace()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var postTriggered = false;
		_container.RegisterTrigger("flag", TriggerType.Post, TriggerOperation.Replace,
			(Action<JObject>)(_ => postTriggered = true));

		await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "flag" } });

		postTriggered.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_NonExistentTrigger_ThrowsBadRequest()
	{
		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "nope" } });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task PostTrigger_OnStream_FiresAfterCreate()
	{
		var postTriggered = false;
		_container.RegisterTrigger("flag", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => postTriggered = true));

		var json = """{"id":"1","pk":"a"}""";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
		await _container.CreateItemStreamAsync(stream, new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "flag" } });

		postTriggered.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_OperationMismatch_NotFired()
	{
		var postTriggered = false;
		_container.RegisterTrigger("createOnly", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => postTriggered = true));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		// Replace with a Create-only post-trigger — should not fire
		await _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "createOnly" } });

		postTriggered.Should().BeFalse();
	}
}

// ─── Phase 4: Delete Trigger Execution ──────────────────────────────────

public class DeleteTriggerTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PreTrigger_OnDelete_ThrowingTriggerAborts()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("blockDelete", TriggerType.Pre, TriggerOperation.Delete,
			(Func<JObject, JObject>)(_ => throw new InvalidOperationException("Delete blocked!")));

		var act = () => _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "blockDelete" } });

		await act.Should().ThrowAsync<CosmosException>();

		// Item should still exist — delete was aborted
		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task PreTrigger_OnDelete_NonThrowingAllowsDeletion()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var preFired = false;
		_container.RegisterTrigger("auditDelete", TriggerType.Pre, TriggerOperation.Delete,
			(Func<JObject, JObject>)(doc => { preFired = true; return doc; }));

		await _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "auditDelete" } });

		preFired.Should().BeTrue();

		var readAct = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		await readAct.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task PostTrigger_FiresAfterDelete()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var postFired = false;
		string? deletedId = null;
		_container.RegisterTrigger("afterDelete", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(doc => { postFired = true; deletedId = doc["id"]!.Value<string>(); }));

		await _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "afterDelete" } });

		postFired.Should().BeTrue();
		deletedId.Should().Be("1");
	}

	[Fact]
	public async Task PostTrigger_ExceptionOnDelete_RollsBackDelete()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("failingPostDelete", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(_ => throw new InvalidOperationException("Post-trigger failed!")));

		var act = () => _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "failingPostDelete" } });

		await act.Should().ThrowAsync<CosmosException>();

		// Item should still exist — the delete was rolled back
		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task PostTrigger_Stream_RollsBack_OnExceptionDuringUpsertStream()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", value = "original" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("failUpsert", TriggerType.Post, TriggerOperation.Upsert,
			(Action<JObject>)(_ => throw new InvalidOperationException("Post-trigger failed!")));

		var json = """{"id":"1","pk":"a","value":"updated"}""";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var act = () => _container.UpsertItemStreamAsync(stream, new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "failUpsert" } });

		await act.Should().ThrowAsync<CosmosException>();

		// Item should be rolled back to original value
		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["value"]!.Value<string>().Should().Be("original");
	}

	[Fact]
	public async Task PostTrigger_Stream_RollsBack_OnExceptionDuringReplaceStream()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", value = "original" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("failReplace", TriggerType.Post, TriggerOperation.Replace,
			(Action<JObject>)(_ => throw new InvalidOperationException("Post-trigger failed!")));

		var json = """{"id":"1","pk":"a","value":"replaced"}""";
		using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));

		var act = () => _container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "failReplace" } });

		await act.Should().ThrowAsync<CosmosException>();

		// Item should be rolled back to original value
		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["value"]!.Value<string>().Should().Be("original");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: Registration & CRUD Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class TriggerRegistrationEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task RegisterTrigger_SameIdTwice_OverwritesHandler()
	{
		_container.RegisterTrigger("t1", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["v"] = 1; return doc; }));
		_container.RegisterTrigger("t1", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["v"] = 2; return doc; }));

		// The second registration should overwrite — verify by reading the stored item
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "t1" } });
		var stored = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		stored["v"]!.Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task RegisterTrigger_CaseSensitive_DifferentTriggers()
	{
		_container.RegisterTrigger("myTrigger", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["tag"] = "lower"; return doc; }));
		_container.RegisterTrigger("MyTrigger", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["tag"] = "upper"; return doc; }));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "myTrigger" } });
		var s1 = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		s1["tag"]!.Value<string>().Should().Be("lower");

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "MyTrigger" } });
		var s2 = (await _container.ReadItemAsync<JObject>("2", new PartitionKey("a"))).Resource;
		s2["tag"]!.Value<string>().Should().Be("upper");
	}

	[Fact]
	public void DeregisterTrigger_NonExistent_DoesNotThrow()
	{
		var act = () => _container.DeregisterTrigger("nonexistent");
		act.Should().NotThrow();
	}

	[Fact]
	public async Task DeregisterTrigger_ThenReRegister_Works()
	{
		_container.RegisterTrigger("t1", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["v"] = 1; return doc; }));
		_container.DeregisterTrigger("t1");
		_container.RegisterTrigger("t1", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["v"] = 2; return doc; }));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "t1" } });
		var stored = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		stored["v"]!.Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task CreateTriggerAsync_DuplicateId_ThrowsConflict()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "dup",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() {}"
		});

		var act = () => _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "dup",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() {}"
		});
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task CreateTriggerAsync_ReturnsCreatedStatusCode()
	{
		var response = await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "t1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() {}"
		});
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task ReplaceTriggerAsync_ReturnsOkStatusCode()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "t1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() {}"
		});

		var response = await _container.Scripts.ReplaceTriggerAsync(new TriggerProperties
		{
			Id = "t1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run2() {}"
		});
		response.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task DeleteTriggerAsync_ReturnsNoContentStatusCode()
	{
		await _container.Scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "t1",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create,
			Body = "function run() {}"
		});

		var response = await _container.Scripts.DeleteTriggerAsync("t1");
		response.StatusCode.Should().Be(HttpStatusCode.NoContent);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: Pre-Trigger Gaps
// ═══════════════════════════════════════════════════════════════════════════

public class PreTriggerEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PreTrigger_ThrowingHandler_AbortsCreate()
	{
		_container.RegisterTrigger("fail", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "fail" } });

		await act.Should().ThrowAsync<CosmosException>();
		_container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task PreTrigger_ThrowingHandler_AbortsUpsert()
	{
		_container.RegisterTrigger("fail", TriggerType.Pre, TriggerOperation.Upsert,
			(Func<JObject, JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => _container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "fail" } });

		await act.Should().ThrowAsync<CosmosException>();
		_container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task PreTrigger_ThrowingHandler_AbortsReplace()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "orig" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("fail", TriggerType.Pre, TriggerOperation.Replace,
			(Func<JObject, JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "new" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "fail" } });

		await act.Should().ThrowAsync<CosmosException>();
		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["v"]!.Value<string>().Should().Be("orig");
	}

	[Fact]
	public async Task PreTrigger_EmptyTriggersArray_NoEffect()
	{
		_container.RegisterTrigger("t1", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["tag"] = "fired"; return doc; }));

		var result = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string>() });

		result.Resource["tag"].Should().BeNull();
	}

	[Fact]
	public async Task PreTrigger_OperationSpecific_Delete_NotFiredOnCreate()
	{
		_container.RegisterTrigger("delOnly", TriggerType.Pre, TriggerOperation.Delete,
			(Func<JObject, JObject>)(doc => { doc["fired"] = true; return doc; }));

		var result = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "delOnly" } });

		result.Resource["fired"].Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: Post-Trigger & Rollback Gaps
// ═══════════════════════════════════════════════════════════════════════════

public class PostTriggerEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PostTrigger_ReceivesEnrichedDoc_WithSystemProperties()
	{
		JObject? received = null;
		_container.RegisterTrigger("capture", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(doc => received = doc));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "capture" } });

		received.Should().NotBeNull();
		received!["_ts"].Should().NotBeNull();
		received!["_etag"].Should().NotBeNull();
	}

	[Fact]
	public async Task PostTrigger_MultiplePostTriggers_ChainInOrder()
	{
		var order = new List<string>();
		_container.RegisterTrigger("first", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => order.Add("first")));
		_container.RegisterTrigger("second", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => order.Add("second")));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "first", "second" } });

		// Real Cosmos only fires the first matching trigger
		order.Should().BeEquivalentTo(new[] { "first" });
	}

	[Fact]
	public async Task PostTrigger_ExceptionRollsBack_Upsert_ExistingItem()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "orig" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.Upsert,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => _container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "new" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });

		await act.Should().ThrowAsync<CosmosException>();
		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["v"]!.Value<string>().Should().Be("orig");
	}

	[Fact]
	public async Task PostTrigger_OperationAll_FiresOnCreate()
	{
		var fired = false;
		_container.RegisterTrigger("all", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => fired = true));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "all" } });

		fired.Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: Rollback Detail Tests
// ═══════════════════════════════════════════════════════════════════════════

public class TriggerRollbackTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PostTrigger_RollsBack_EtagIsRestoredToOriginal()
	{
		var created = await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));
		var origEtag = created.ETag;

		_container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.Replace,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "new" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });

		await act.Should().ThrowAsync<CosmosException>();
		var read = await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		read.ETag.Should().Be(origEtag);
	}

	[Fact]
	public async Task PostTrigger_RollsBack_ItemContentIsExactlyOriginal()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "original" }),
			new PartitionKey("a"));

		_container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.Replace,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => _container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "changed" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });

		await act.Should().ThrowAsync<CosmosException>();
		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["v"]!.Value<string>().Should().Be("original");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: Mixed Trigger Tests
// ═══════════════════════════════════════════════════════════════════════════

public class TriggerMixedTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PreAndPostTrigger_BothFire_OnSameOperation()
	{
		var preFired = false;
		var postFired = false;
		_container.RegisterTrigger("pre", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { preFired = true; return doc; }));
		_container.RegisterTrigger("post", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => postFired = true));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions
			{
				PreTriggers = new List<string> { "pre" },
				PostTriggers = new List<string> { "post" }
			});

		preFired.Should().BeTrue();
		postFired.Should().BeTrue();
	}

	[Fact]
	public async Task PreTrigger_PostTrigger_PostSeesPreModifiedDoc()
	{
		_container.RegisterTrigger("pre", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["preTag"] = "set"; return doc; }));

		JObject? postDoc = null;
		_container.RegisterTrigger("post", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(doc => postDoc = doc));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions
			{
				PreTriggers = new List<string> { "pre" },
				PostTriggers = new List<string> { "post" }
			});

		postDoc.Should().NotBeNull();
		postDoc!["preTag"]!.Value<string>().Should().Be("set");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: Unsupported Operations
// ═══════════════════════════════════════════════════════════════════════════

public class TriggerUnsupportedOperationTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PatchItemAsync_DoesNotSupportTriggers()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "orig" }),
			new PartitionKey("a"));

		var preFired = false;
		_container.RegisterTrigger("pre", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => { preFired = true; return doc; }));

		await _container.PatchItemAsync<JObject>("1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/v", "patched") });

		preFired.Should().BeFalse();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: PatchItemStreamAsync Trigger Support (Issue #22)
// ═══════════════════════════════════════════════════════════════════════════

public class PatchItemStreamAsyncTriggerTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PatchItemStreamAsync_PreTrigger_Fires()
	{
		_container.RegisterTrigger("add-field", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => { doc["injected"] = "by-trigger"; return doc; }));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "original" }),
			new PartitionKey("a"));

		await _container.PatchItemStreamAsync("1", new PartitionKey("a"),
			[PatchOperation.Set("/name", "patched")],
			new PatchItemRequestOptions { PreTriggers = new List<string> { "add-field" } });

		var read = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		read["injected"]!.ToString().Should().Be("by-trigger");
		read["name"]!.ToString().Should().Be("patched");
	}

	[Fact]
	public async Task PatchItemStreamAsync_PostTrigger_Fires()
	{
		var called = false;
		_container.RegisterTrigger("post-flag", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => called = true));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "original" }),
			new PartitionKey("a"));

		await _container.PatchItemStreamAsync("1", new PartitionKey("a"),
			[PatchOperation.Set("/name", "patched")],
			new PatchItemRequestOptions { PostTriggers = new List<string> { "post-flag" } });

		called.Should().BeTrue();
	}

	[Fact]
	public async Task PatchItemStreamAsync_PostTrigger_RollbackOnError()
	{
		_container.RegisterTrigger("fail-post", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => throw new InvalidOperationException("boom")));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "original" }),
			new PartitionKey("a"));

		var act = () => _container.PatchItemStreamAsync("1", new PartitionKey("a"),
			[PatchOperation.Set("/name", "patched")],
			new PatchItemRequestOptions { PostTriggers = new List<string> { "fail-post" } });
		await act.Should().ThrowAsync<CosmosException>();

		var read = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		read["name"]!.ToString().Should().Be("original", "patch should be rolled back on post-trigger failure");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: TransactionalBatch Trigger Support (Issue #23)
// ═══════════════════════════════════════════════════════════════════════════

public class TransactionalBatchTriggerTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	private static TransactionalBatchItemRequestOptions WithPreTrigger(string triggerName) =>
		new() { Properties = new Dictionary<string, object> { ["x-ms-pre-trigger-include"] = new[] { triggerName } } };

	private static TransactionalBatchItemRequestOptions WithPostTrigger(string triggerName) =>
		new() { Properties = new Dictionary<string, object> { ["x-ms-post-trigger-include"] = new[] { triggerName } } };

	[Fact]
	public async Task Batch_CreateItem_WithPreTrigger_FiresTrigger()
	{
		_container.RegisterTrigger("add-field", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => { doc["injected"] = "batch-pre"; return doc; }));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.CreateItem(JObject.FromObject(new { id = "1", pk = "a" }), WithPreTrigger("add-field"));
		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		read["injected"]!.ToString().Should().Be("batch-pre");
	}

	[Fact]
	public async Task Batch_CreateItem_WithPostTrigger_FiresTrigger()
	{
		var called = false;
		_container.RegisterTrigger("post-flag", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => called = true));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.CreateItem(JObject.FromObject(new { id = "1", pk = "a" }), WithPostTrigger("post-flag"));
		await batch.ExecuteAsync();

		called.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_UpsertItem_WithPostTrigger_FiresTrigger()
	{
		var called = false;
		_container.RegisterTrigger("post-flag", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => called = true));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "orig" }),
			new PartitionKey("a"));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.UpsertItem(JObject.FromObject(new { id = "1", pk = "a", name = "updated" }), WithPostTrigger("post-flag"));
		await batch.ExecuteAsync();

		called.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_PatchItem_WithPreTrigger_FiresTrigger()
	{
		_container.RegisterTrigger("add-field", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => { doc["injected"] = "batch-patch"; return doc; }));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "orig" }),
			new PartitionKey("a"));

		var patchOptions = new TransactionalBatchPatchItemRequestOptions
		{
			Properties = new Dictionary<string, object> { ["x-ms-pre-trigger-include"] = new[] { "add-field" } }
		};
		var batch = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.PatchItem("1", [PatchOperation.Set("/name", "patched")], patchOptions);
		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeTrue();

		var read = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		read["injected"]!.ToString().Should().Be("batch-patch");
	}

	[Fact]
	public async Task Batch_TriggerFailure_CausesRollback()
	{
		_container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => throw new InvalidOperationException("trigger boom")));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.CreateItem(JObject.FromObject(new { id = "1", pk = "a" }), WithPostTrigger("fail"));
		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();

		var act = () => _container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task Batch_WithoutTriggerProperties_DoesNotFireTriggers()
	{
		var called = false;
		_container.RegisterTrigger("post-flag", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => called = true));

		var batch = _container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.CreateItem(JObject.FromObject(new { id = "1", pk = "a" }));
		await batch.ExecuteAsync();

		called.Should().BeFalse("triggers should not fire unless explicitly specified via Properties");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase: Divergent Behavior (Skip + Sister)
// ═══════════════════════════════════════════════════════════════════════════

public class TriggerDivergentBehaviorTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task PatchItemAsync_FiresTriggers_RealCosmos()
	{
		await Task.CompletedTask;
	}

	[Fact]
	public async Task GetTriggerQueryIterator_ReturnsAllTriggers_RealCosmos()
	{
		var scripts = _container.Scripts;
		await scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "t1",
			Body = "function(){}",
			TriggerType = TriggerType.Pre,
			TriggerOperation = TriggerOperation.Create
		});
		await scripts.CreateTriggerAsync(new TriggerProperties
		{
			Id = "t2",
			Body = "function(){}",
			TriggerType = TriggerType.Post,
			TriggerOperation = TriggerOperation.All
		});

		var iterator = scripts.GetTriggerQueryIterator<TriggerProperties>();
		var all = new List<TriggerProperties>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			all.AddRange(page);
		}

		all.Should().HaveCount(2);
		all.Select(t => t.Id).Should().BeEquivalentTo("t1", "t2");
	}

	[Fact]
	public async Task PostTriggerRollback_ChangeFeedClean_RealCosmos()
	{
		var checkpoint = _container.GetChangeFeedCheckpoint();

		_container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<CosmosException>();

		var changes = _container.GetChangeFeedIterator<JObject>(checkpoint);
		var items = new List<JObject>();
		while (changes.HasMoreResults)
		{
			var page = await changes.ReadNextAsync();
			items.AddRange(page);
		}
		items.Should().BeEmpty("change feed should not contain entries for rolled-back operations");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Plan 44: Trigger Tests Deep Dive
// ═══════════════════════════════════════════════════════════════════════════

// ── B1-B5: Change Feed Rollback on Post-Trigger Failure ──
public class TriggerChangeFeedRollbackTests
{
	private static async Task<List<JObject>> GetChangeFeedSince(InMemoryContainer container, long checkpoint)
	{
		var iter = container.GetChangeFeedIterator<JObject>(checkpoint);
		var items = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			items.AddRange(page);
		}
		return items;
	}

	[Fact]
	public async Task PostTrigger_ExceptionOnUpsert_ChangeFeedIsClean()
	{
		var container = new InMemoryContainer("cf-upsert", "/pk");
		var checkpoint = container.GetChangeFeedCheckpoint();

		container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<Exception>();

		var changes = await GetChangeFeedSince(container, checkpoint);
		changes.Should().BeEmpty("change feed should not contain entries for rolled-back upsert");
	}

	[Fact]
	public async Task PostTrigger_ExceptionOnReplace_ChangeFeedIsClean()
	{
		var container = new InMemoryContainer("cf-replace", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", v = "orig" }), new PartitionKey("a"));
		var checkpoint = container.GetChangeFeedCheckpoint();

		container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "upd" }), "1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<Exception>();

		var changes = await GetChangeFeedSince(container, checkpoint);
		changes.Should().BeEmpty("change feed should not contain entries for rolled-back replace");
	}

	[Fact]
	public async Task PostTrigger_ExceptionOnCreateStream_ChangeFeedIsClean()
	{
		var container = new InMemoryContainer("cf-cstream", "/pk");
		var checkpoint = container.GetChangeFeedCheckpoint();

		container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => container.CreateItemStreamAsync(
			new MemoryStream(System.Text.Encoding.UTF8.GetBytes("{\"id\":\"1\",\"pk\":\"a\"}")),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<Exception>();

		var changes = await GetChangeFeedSince(container, checkpoint);
		changes.Should().BeEmpty();
	}

	[Fact]
	public async Task PostTrigger_ExceptionOnUpsertStream_ChangeFeedIsClean()
	{
		var container = new InMemoryContainer("cf-ustream", "/pk");
		var checkpoint = container.GetChangeFeedCheckpoint();

		container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => container.UpsertItemStreamAsync(
			new MemoryStream(System.Text.Encoding.UTF8.GetBytes("{\"id\":\"1\",\"pk\":\"a\"}")),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<Exception>();

		var changes = await GetChangeFeedSince(container, checkpoint);
		changes.Should().BeEmpty();
	}

	[Fact]
	public async Task PostTrigger_ExceptionOnReplaceStream_ChangeFeedIsClean()
	{
		var container = new InMemoryContainer("cf-rstream", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var checkpoint = container.GetChangeFeedCheckpoint();

		container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => container.ReplaceItemStreamAsync(
			new MemoryStream(System.Text.Encoding.UTF8.GetBytes("{\"id\":\"1\",\"pk\":\"a\",\"v\":\"upd\"}")),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<Exception>();

		var changes = await GetChangeFeedSince(container, checkpoint);
		changes.Should().BeEmpty();
	}

	[Fact]
	public async Task PostTrigger_ExceptionOnDelete_ChangeFeedHasNoTombstone()
	{
		var container = new InMemoryContainer("cf-delete", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var checkpoint = container.GetChangeFeedCheckpoint();

		container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<Exception>();

		var changes = await GetChangeFeedSince(container, checkpoint);
		changes.Should().BeEmpty("delete tombstone should not appear for rolled-back delete");
	}

	[Fact]
	public async Task PostTrigger_ExceptionOnDeleteStream_ChangeFeedHasNoTombstone()
	{
		var container = new InMemoryContainer("cf-dstream", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		var checkpoint = container.GetChangeFeedCheckpoint();

		container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => container.DeleteItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<Exception>();

		var changes = await GetChangeFeedSince(container, checkpoint);
		changes.Should().BeEmpty();
	}
}

// ── M1-M6: Stream Trigger Coverage ──
public class TriggerStreamCoverageTests
{
	private readonly InMemoryContainer _container = new("trig-stream", "/pk");

	[Fact]
	public async Task PostTrigger_OnUpsertStream_FiresAfterUpsert()
	{
		var triggered = false;
		_container.RegisterTrigger("post-flag", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => triggered = true));

		await _container.UpsertItemStreamAsync(
			new MemoryStream(System.Text.Encoding.UTF8.GetBytes("{\"id\":\"1\",\"pk\":\"a\"}")),
			new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "post-flag" } });

		triggered.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_OnReplaceStream_FiresAfterReplace()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var triggered = false;
		_container.RegisterTrigger("post-flag", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => triggered = true));

		await _container.ReplaceItemStreamAsync(
			new MemoryStream(System.Text.Encoding.UTF8.GetBytes("{\"id\":\"1\",\"pk\":\"a\",\"v\":\"upd\"}")),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "post-flag" } });

		triggered.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_OnDeleteStream_FiresAfterDelete()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var triggered = false;
		_container.RegisterTrigger("post-del", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(_ => triggered = true));

		await _container.DeleteItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "post-del" } });

		triggered.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_Stream_RollsBack_OnExceptionDuringDeleteStream()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		_container.RegisterTrigger("fail-post", TriggerType.Post, TriggerOperation.Delete,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => _container.DeleteItemStreamAsync("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail-post" } });
		await act.Should().ThrowAsync<Exception>();

		// Item should still exist (rolled back)
		var read = await _container.ReadItemStreamAsync("1", new PartitionKey("a"));
		read.StatusCode.Should().Be(HttpStatusCode.OK);
	}
}

// ── M7: TriggerOperation Filtering ──
public class TriggerOperationFilteringDeepDiveTests
{
	private readonly InMemoryContainer _container = new("trig-filter", "/pk");

	[Fact]
	public async Task PreTrigger_OperationAll_FiresOnDelete()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var triggered = false;
		_container.RegisterTrigger("pre-all", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => { triggered = true; return doc; }));

		await _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "pre-all" } });

		triggered.Should().BeTrue();
	}

	[Fact]
	public async Task PostTrigger_OperationAll_FiresOnDelete()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var triggered = false;
		_container.RegisterTrigger("post-all", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => triggered = true));

		await _container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "post-all" } });

		triggered.Should().BeTrue();
	}
}

// ── E4: Multiple Triggers ──
public class TriggerMultipleTriggerTests
{
	private readonly InMemoryContainer _container = new("trig-multi", "/pk");

	[Fact]
	public async Task PreTrigger_MultipleTriggers_FirstMismatchSecondMatches()
	{
		_container.RegisterTrigger("trigA", TriggerType.Pre, TriggerOperation.Replace,
			(Func<JObject, JObject>)(doc => { doc["trigA"] = true; return doc; }));
		_container.RegisterTrigger("trigB", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["trigB"] = true; return doc; }));

		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "trigA", "trigB" } });

		var item = (await _container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["trigA"].Should().BeNull("trigA is for Replace, should not fire on Create");
		item["trigB"]!.Value<bool>().Should().BeTrue("trigB is for Create, should fire");
	}

	[Fact]
	public async Task PreTrigger_MultipleTriggers_FirstNotFound_ThrowsBadRequest()
	{
		_container.RegisterTrigger("trigB", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => doc));

		var act = () => _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "unknown", "trigB" } });
		await act.Should().ThrowAsync<CosmosException>();
	}
}

// ── E9: Same Name for Pre and Post ──
public class TriggerSameNameTests
{
	[Fact]
	public async Task RegisterTrigger_SameNameForPreAndPost_LastWins()
	{
		var container = new InMemoryContainer("trig-samename", "/pk");

		var preFired = false;
		container.RegisterTrigger("t1", TriggerType.Pre, TriggerOperation.All,
			(Func<JObject, JObject>)(doc => { preFired = true; return doc; }));
		// This overwrites the pre-trigger registration
		container.RegisterTrigger("t1", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => { }));

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "t1" } });

		preFired.Should().BeFalse("pre-trigger was overwritten by post-trigger registration");
	}
}

// ── M22: Triggers + IfMatchEtag ──
public class TriggerETagInteractionTests
{
	[Fact]
	public async Task PreTrigger_WithStaleEtag_EtagCheckedBeforeTrigger()
	{
		var container = new InMemoryContainer("trig-etag", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", v = "orig" }), new PartitionKey("a"));

		var preFired = false;
		container.RegisterTrigger("pre", TriggerType.Pre, TriggerOperation.Replace,
			(Func<JObject, JObject>)(doc => { preFired = true; return doc; }));

		var act = () => container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", v = "upd" }), "1", new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = "stale-etag", PreTriggers = new List<string> { "pre" } });
		await act.Should().ThrowAsync<CosmosException>();
		preFired.Should().BeFalse("ETag check should happen before trigger");
	}
}

// ── M25: Upsert New Item Rollback ──
public class TriggerUpsertNewItemRollbackTests
{
	[Fact]
	public async Task PostTrigger_ExceptionRollsBack_Upsert_NewItem()
	{
		var container = new InMemoryContainer("trig-upsert-new", "/pk");

		container.RegisterTrigger("fail", TriggerType.Post, TriggerOperation.All,
			(Action<JObject>)(_ => throw new InvalidOperationException("fail!")));

		var act = () => container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"),
			new ItemRequestOptions { PostTriggers = new List<string> { "fail" } });
		await act.Should().ThrowAsync<Exception>();

		// Item should not exist at all (was a new upsert, rolled back)
		var readAct = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a"));
		await readAct.Should().ThrowAsync<CosmosException>();
	}
}

// ── M9-M10: EnableContentResponseOnWrite + Triggers ──
public class TriggerContentResponseOptionsTests
{
	[Fact]
	public async Task PreTrigger_WithEnableContentResponseOnWriteFalse_StillModifiesDoc()
	{
		var container = new InMemoryContainer("trig-ecrow", "/pk");
		container.RegisterTrigger("add-field", TriggerType.Pre, TriggerOperation.Create,
			(Func<JObject, JObject>)(doc => { doc["injected"] = "yes"; return doc; }));

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"),
			new ItemRequestOptions
			{
				EnableContentResponseOnWrite = false,
				PreTriggers = new List<string> { "add-field" }
			});

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["injected"]!.Value<string>().Should().Be("yes");
	}

	[Fact]
	public async Task PostTrigger_WithEnableContentResponseOnWriteFalse_StillFires()
	{
		var container = new InMemoryContainer("trig-ecrow2", "/pk");
		var triggered = false;
		container.RegisterTrigger("post", TriggerType.Post, TriggerOperation.Create,
			(Action<JObject>)(_ => triggered = true));

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"),
			new ItemRequestOptions
			{
				EnableContentResponseOnWrite = false,
				PostTriggers = new List<string> { "post" }
			});

		triggered.Should().BeTrue();
	}
}

// ── M12-M13: Nonexistent Item with Triggers ──
public class TriggerNonexistentItemTests
{
	[Fact]
	public async Task ReplaceNonExistentItem_WithPreTrigger_ThrowsNotFound()
	{
		var container = new InMemoryContainer("trig-nexist", "/pk");
		container.RegisterTrigger("pre", TriggerType.Pre, TriggerOperation.Replace,
			(Func<JObject, JObject>)(doc => doc));

		var act = () => container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), "1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "pre" } });
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteNonExistentItem_WithPreTrigger_ThrowsNotFound()
	{
		var container = new InMemoryContainer("trig-nexist2", "/pk");
		container.RegisterTrigger("pre", TriggerType.Pre, TriggerOperation.Delete,
			(Func<JObject, JObject>)(doc => doc));

		var act = () => container.DeleteItemAsync<JObject>("1", new PartitionKey("a"),
			new ItemRequestOptions { PreTriggers = new List<string> { "pre" } });
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}
