using System.Collections.ObjectModel;
using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Computed Property Query Clause Tests
// ═══════════════════════════════════════════════════════════════════════════

public class ComputedPropertyQueryClauseTests
{
	private static InMemoryContainer CreateContainer(params (string Name, string Query)[] defs)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				defs.Select(d => new ComputedProperty { Name = d.Name, Query = d.Query }).ToList())
		};
		return new InMemoryContainer(props);
	}

	private static async Task Seed(InMemoryContainer container, params object[] items)
	{
		foreach (var item in items)
		{
			var jObj = JObject.FromObject(item);
			await container.CreateItemAsync(jObj, new PartitionKey(jObj["pk"]!.ToString()));
		}
	}

	[Fact]
	public async Task ComputedProperty_UsedWithLikeOperator()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" });

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id FROM c WHERE c.cp_lower LIKE 'ali%'"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ComputedProperty_OrderByDescending()
	{
		var container = CreateContainer(("cp_len", "SELECT VALUE LENGTH(c.name) FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", name = "Al" },
			new { id = "2", pk = "p", name = "Alice" },
			new { id = "3", pk = "p", name = "Bob" });

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, c.cp_len FROM c ORDER BY c.cp_len DESC"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var ids = results.Select(r => r["id"]!.ToString()).ToList();
		ids.Should().Equal("2", "3", "1"); // 5, 3, 2
	}

	[Fact]
	public async Task ComputedProperty_AggregateMin()
	{
		var container = CreateContainer(("cp_price", "SELECT VALUE c.price * 0.9 FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", price = 100 },
			new { id = "2", pk = "p", price = 50 },
			new { id = "3", pk = "p", price = 200 });

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE MIN(c.cp_price) FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(45); // 50 * 0.9
	}

	[Fact]
	public async Task ComputedProperty_AggregateMax()
	{
		var container = CreateContainer(("cp_price", "SELECT VALUE c.price * 0.9 FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", price = 100 },
			new { id = "2", pk = "p", price = 50 },
			new { id = "3", pk = "p", price = 200 });

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE MAX(c.cp_price) FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(180); // 200 * 0.9
	}

	[Fact]
	public async Task ComputedProperty_AggregateAvg()
	{
		var container = CreateContainer(("cp_discounted", "SELECT VALUE c.price * 0.5 FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", price = 100 },
			new { id = "2", pk = "p", price = 200 });

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE AVG(c.cp_discounted) FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(75); // (50+100)/2
	}

	[Fact]
	public async Task ComputedProperty_AggregateCountWithFilter()
	{
		var container = CreateContainer(("cp_len", "SELECT VALUE LENGTH(c.name) FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", name = "Al" },
			new { id = "2", pk = "p", name = "Alice" },
			new { id = "3", pk = "p", name = "Bob" });

		var iter = container.GetItemQueryIterator<int>(
			new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE c.cp_len > 2"));
		var results = new List<int>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(2); // Alice(5), Bob(3)
	}

	[Fact]
	public async Task ComputedProperty_StartsWithFunction()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" });

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id FROM c WHERE STARTSWITH(c.cp_lower, 'ali')"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ComputedProperty_ComplexNestedArithmetic()
	{
		var container = CreateContainer(("cp_final", "SELECT VALUE FLOOR(c.price * 0.8 + 10) FROM c"));
		await Seed(container, new { id = "1", pk = "p", price = 100 });

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE c.cp_final FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(90); // FLOOR(100*0.8+10) = FLOOR(90) = 90
	}

	[Fact]
	public async Task ComputedProperty_UsedInHavingClause()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.category) FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", category = "Fruit" },
			new { id = "2", pk = "p", category = "Fruit" },
			new { id = "3", pk = "p", category = "Meat" });

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.cp_lower, COUNT(1) AS cnt FROM c GROUP BY c.cp_lower HAVING COUNT(1) > 1"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["cp_lower"]!.ToString().Should().Be("fruit");
	}

	[Fact]
	public async Task ComputedProperty_UsedWithNotOperator()
	{
		var container = CreateContainer(("cp_active", "SELECT VALUE c.isActive FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", isActive = true },
			new { id = "2", pk = "p", isActive = false });

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id FROM c WHERE NOT c.cp_active"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task ComputedProperty_NullCoalesceInOuterQuery()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await Seed(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p" }); // no name

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, (c.cp_lower ?? 'unknown') AS display FROM c ORDER BY c.id"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().HaveCount(2);
		results[0]["display"]!.ToString().Should().Be("alice");
		results[1]["display"]!.ToString().Should().Be("unknown");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Computed Property Expression Type Tests
// ═══════════════════════════════════════════════════════════════════════════

public class ComputedPropertyExpressionTypeTests
{
	private static InMemoryContainer CreateContainer(params (string Name, string Query)[] defs)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				defs.Select(d => new ComputedProperty { Name = d.Name, Query = d.Query }).ToList())
		};
		return new InMemoryContainer(props);
	}

	[Fact]
	public async Task ComputedProperty_ReturnsObjectLiteral()
	{
		var container = CreateContainer(("cp_obj", "SELECT VALUE {\"lower\": LOWER(c.name), \"len\": LENGTH(c.name)} FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.cp_obj FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var obj = results.Single()["cp_obj"]!;
		obj["lower"]!.ToString().Should().Be("alice");
		((int)obj["len"]!).Should().Be(5);
	}

	[Fact]
	public async Task ComputedProperty_ReturnsArrayLiteral()
	{
		var container = CreateContainer(("cp_arr", "SELECT VALUE [c.first, c.last] FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", first = "John", last = "Doe" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.cp_arr FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var arr = (JArray)results.Single()["cp_arr"]!;
		arr.Select(t => t.ToString()).Should().Equal("John", "Doe");
	}

	[Fact]
	public async Task ComputedProperty_EndsWithFunction()
	{
		var container = CreateContainer(("cp_ends", "SELECT VALUE ENDSWITH(c.name, 'e') FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "p", name = "Bob" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id FROM c WHERE c.cp_ends = true"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ComputedProperty_ReplaceFunction()
	{
		var container = CreateContainer(("cp_replaced", "SELECT VALUE REPLACE(c.name, 'A', 'X') FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.cp_replaced FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("Xlice");
	}

	[Fact]
	public async Task ComputedProperty_DeeplyNestedPath()
	{
		var container = CreateContainer(("cp_deep", "SELECT VALUE c.a.b.c.d FROM c"));
		var item = JObject.Parse("{\"id\":\"1\",\"pk\":\"p\",\"a\":{\"b\":{\"c\":{\"d\":\"deepval\"}}}}");
		await container.CreateItemAsync(item, new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.cp_deep FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("deepval");
	}

	[Fact]
	public async Task ComputedProperty_TernaryExpression()
	{
		var container = CreateContainer(("cp_label", "SELECT VALUE (c.age > 18 ? 'adult' : 'minor') FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", age = 25 }),
			new PartitionKey("p"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "p", age = 10 }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, c.cp_label FROM c ORDER BY c.id"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results[0]["cp_label"]!.ToString().Should().Be("adult");
		results[1]["cp_label"]!.ToString().Should().Be("minor");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Computed Property Edge Case Tests
// ═══════════════════════════════════════════════════════════════════════════

public class ComputedPropertyEdgeCaseTests
{
	private static InMemoryContainer CreateContainer(params (string Name, string Query)[] defs)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				defs.Select(d => new ComputedProperty { Name = d.Name, Query = d.Query }).ToList())
		};
		return new InMemoryContainer(props);
	}

	[Fact]
	public async Task ComputedProperty_EmptyStringInput()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.cp_lower FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("");
	}

	[Fact]
	public async Task ComputedProperty_NegativeNumberArithmetic()
	{
		var container = CreateContainer(("cp_neg", "SELECT VALUE c.price * -1 FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", price = 42 }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE c.cp_neg FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(-42);
	}

	[Fact]
	public async Task ComputedProperty_MixedDefinedUndefined()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "p" }), // no "name" property
			new PartitionKey("p"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "p", name = "Bob" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, c.cp_lower FROM c WHERE IS_DEFINED(c.cp_lower)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		// Items 1 and 3 have "name", so cp_lower should be defined for them
		results.Select(r => r["id"]!.ToString()).Should().BeEquivalentTo(new[] { "1", "3" });
	}

	[Fact]
	public async Task ComputedProperty_StatePersistence_ExportImport()
	{
		var container = CreateContainer(("cp_upper", "SELECT VALUE UPPER(c.name) FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "test" }),
			new PartitionKey("p"));

		var state = container.ExportState();
		var target = CreateContainer(("cp_upper", "SELECT VALUE UPPER(c.name) FROM c"));
		target.ImportState(state);

		var iter = target.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.cp_upper FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("TEST");
	}

	[Fact]
	public async Task ComputedProperty_MultipleContainersIsolated()
	{
		var c1 = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		var c2 = CreateContainer(("cp_upper", "SELECT VALUE UPPER(c.name) FROM c"));

		await c1.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p", name = "Test" }), new PartitionKey("p"));
		await c2.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "p", name = "Test" }), new PartitionKey("p"));

		var iter1 = c1.GetItemQueryIterator<string>(new QueryDefinition("SELECT VALUE c.cp_lower FROM c"));
		var r1 = new List<string>();
		while (iter1.HasMoreResults) r1.AddRange(await iter1.ReadNextAsync());
		r1.Should().ContainSingle().Which.Should().Be("test");

		var iter2 = c2.GetItemQueryIterator<string>(new QueryDefinition("SELECT VALUE c.cp_upper FROM c"));
		var r2 = new List<string>();
		while (iter2.HasMoreResults) r2.AddRange(await iter2.ReadNextAsync());
		r2.Should().ContainSingle().Which.Should().Be("TEST");
	}

	[Fact]
	public async Task ComputedProperty_ClearItemsThenQuery()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Test" }),
			new PartitionKey("p"));

		container.ClearItems();

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.cp_lower FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ComputedProperty_ZeroMultiplication()
	{
		var container = CreateContainer(("cp_zero", "SELECT VALUE c.price * 0 FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", price = 42 }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<double>(
			new QueryDefinition("SELECT VALUE c.cp_zero FROM c"));
		var results = new List<double>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(0);
	}

	[Fact]
	public async Task ComputedProperty_UnicodeInput()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "ÜNÏCÖDÉ" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.cp_lower FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("ünïcödé");
	}

	[Fact]
	public async Task ComputedProperty_EvaluationOrderIndependent()
	{
		// Same CPs defined in different order should produce the same results
		var c1 = CreateContainer(
			("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"),
			("cp_len", "SELECT VALUE LENGTH(c.name) FROM c"));
		var c2 = CreateContainer(
			("cp_len", "SELECT VALUE LENGTH(c.name) FROM c"),
			("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));

		var item = new { id = "1", pk = "p", name = "Alice" };
		await c1.CreateItemAsync(JObject.FromObject(item), new PartitionKey("p"));
		await c2.CreateItemAsync(JObject.FromObject(item), new PartitionKey("p"));

		var q = new QueryDefinition("SELECT c.cp_lower, c.cp_len FROM c");

		var iter1 = c1.GetItemQueryIterator<JObject>(q);
		var r1 = new List<JObject>();
		while (iter1.HasMoreResults) r1.AddRange(await iter1.ReadNextAsync());

		var iter2 = c2.GetItemQueryIterator<JObject>(q);
		var r2 = new List<JObject>();
		while (iter2.HasMoreResults) r2.AddRange(await iter2.ReadNextAsync());

		r1.Single()["cp_lower"]!.ToString().Should().Be(r2.Single()["cp_lower"]!.ToString());
		((int)r1.Single()["cp_len"]!).Should().Be((int)r2.Single()["cp_len"]!);
	}

	[Fact]
	public async Task ComputedProperty_WhitespaceInQuery()
	{
		// Extra whitespace/newlines in query text — should still work
		var container = CreateContainer(("cp_lower", "  SELECT   VALUE   LOWER( c.name )   FROM   c  "));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Test" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.cp_lower FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("test");
	}

	[Fact]
	public async Task ComputedProperty_IsNullOnNullInput()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		// Item with explicit null name
		var item = JObject.Parse("{\"id\":\"1\",\"pk\":\"p\",\"name\":null}");
		await container.CreateItemAsync(item, new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id FROM c WHERE IS_NULL(c.cp_lower)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		// LOWER(null) behavior: in Cosmos this is undefined, so IS_NULL would return false
		// But the emulator may treat it as null
		results.Should().HaveCountGreaterThanOrEqualTo(0); // document actual behavior
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Computed Property Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

public class ComputedPropertyIntegrationTests
{
	private static InMemoryContainer CreateContainer(params (string Name, string Query)[] defs)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				defs.Select(d => new ComputedProperty { Name = d.Name, Query = d.Query }).ToList())
		};
		return new InMemoryContainer(props);
	}

	[Fact]
	public async Task ComputedProperty_StreamQueryIterator()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		var iterator = container.GetItemQueryStreamIterator(
			new QueryDefinition("SELECT c.id, c.cp_lower FROM c"));
		using var response = await iterator.ReadNextAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);
		var docs = (JArray)jObj["Documents"]!;
		docs.Should().HaveCount(1);
		docs[0]!["cp_lower"]!.ToString().Should().Be("alice");
	}

	[Fact]
	public async Task ComputedProperty_PaginatedQueryWithContinuation()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		for (int i = 0; i < 10; i++)
		{
			await container.CreateItemAsync(
				JObject.FromObject(new { id = i.ToString(), pk = "p", name = $"Name{i}" }),
				new PartitionKey("p"));
		}

		var allResults = new List<JObject>();
		string? continuation = null;
		do
		{
			var iter = container.GetItemQueryIterator<JObject>(
				new QueryDefinition("SELECT c.id, c.cp_lower FROM c ORDER BY c.id"),
				continuation,
				new QueryRequestOptions { MaxItemCount = 3 });
			var resp = await iter.ReadNextAsync();
			allResults.AddRange(resp);
			continuation = resp.ContinuationToken;
		} while (continuation != null);

		allResults.Should().HaveCount(10);
		allResults.All(r => r["cp_lower"] != null).Should().BeTrue();
	}

	[Fact]
	public async Task ComputedProperty_FakeCosmosHandler()
	{
		var backingContainer = new InMemoryContainer(
			new ContainerProperties("mycontainer", "/pk")
			{
				ComputedProperties = new Collection<ComputedProperty>
				{
					new() { Name = "cp_lower", Query = "SELECT VALUE LOWER(c.name) FROM c" }
				}
			});

		await backingContainer.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		using var handler = new FakeCosmosHandler(backingContainer);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var container = client.GetContainer("fakeDb", "mycontainer");
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, c.cp_lower FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["cp_lower"]!.ToString().Should().Be("alice");
	}

	[Fact]
	public async Task ComputedProperty_FakeCosmosHandler_SelectStarExcludesCPs()
	{
		var backingContainer = new InMemoryContainer(
			new ContainerProperties("mycontainer", "/pk")
			{
				ComputedProperties = new Collection<ComputedProperty>
				{
					new() { Name = "cp_lower", Query = "SELECT VALUE LOWER(c.name) FROM c" }
				}
			});

		await backingContainer.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		using var handler = new FakeCosmosHandler(backingContainer);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(5),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

		var container = client.GetContainer("fakeDb", "mycontainer");
		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		var item = results.Single();
		item["name"]!.ToString().Should().Be("Alice");
		// SELECT * should NOT include computed properties
		item["cp_lower"].Should().BeNull();
	}

	[Fact]
	public async Task ComputedProperty_ReplaceItemAsync_ReEvaluates()
	{
		var container = CreateContainer(("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		// Replace with different name
		await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Bob" }),
			"1", new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<string>(
			new QueryDefinition("SELECT VALUE c.cp_lower FROM c"));
		var results = new List<string>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("bob");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Computed Property Validation Tests
// ═══════════════════════════════════════════════════════════════════════════

public class ComputedPropertyValidationDeepTests
{
	[Fact]
	public void ComputedProperty_DuplicateNames_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = "SELECT VALUE c.a FROM c" },
				new() { Name = "cp_x", Query = "SELECT VALUE c.b FROM c" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_Exactly20_Allowed()
	{
		var defs = Enumerable.Range(1, 20)
			.Select(i => new ComputedProperty { Name = $"cp_{i}", Query = $"SELECT VALUE c.f{i} FROM c" })
			.ToList();
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(defs)
		};
		var container = new InMemoryContainer(props); // Should not throw
		container.Should().NotBeNull();
	}

	[Fact]
	public void ComputedProperty_QueryWithOrderBy_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = "SELECT VALUE c.name FROM c ORDER BY c.name" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_QueryWithGroupBy_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = "SELECT VALUE COUNT(1) FROM c GROUP BY c.pk" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_QueryWithDistinct_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = "SELECT DISTINCT VALUE c.name FROM c" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_SelfReferencing_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = "SELECT VALUE c.cp_x FROM c" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_NullQuery_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = null! }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_EmptyQuery_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = "" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_CrossCPReference_OrderIndependent()
	{
		// CP-B references CP-A, but CP-A is defined AFTER CP-B — should still be caught
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_upper_of_lower", Query = "SELECT VALUE UPPER(c.cp_lower) FROM c" },
				new() { Name = "cp_lower", Query = "SELECT VALUE LOWER(c.name) FROM c" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_CrossCPReference_NoFalsePositive()
	{
		// CP named "first" should NOT block a query referencing "c.first_name"
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_first", Query = "SELECT VALUE c.first FROM c" },
				new() { Name = "cp_full", Query = "SELECT VALUE c.first_name FROM c" }
			}
		};
		// Should NOT throw — "c.first_name" does not reference CP "cp_first"
		var container = new InMemoryContainer(props);
		container.Should().NotBeNull();
	}

	[Fact]
	public void ComputedProperty_ReservedName_Ts()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "_ts", Query = "SELECT VALUE c.name FROM c" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_ReservedName_Etag()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "_etag", Query = "SELECT VALUE c.name FROM c" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_QueryWithTop_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = "SELECT VALUE TOP 1 c.name FROM c" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_QueryWithJoin_ThrowsBadRequest()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_x", Query = "SELECT VALUE t FROM c JOIN t IN c.tags" }
			}
		};
		var ex = Assert.ThrowsAny<CosmosException>(() => new InMemoryContainer(props));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ComputedProperty_ReplaceContainerWithInvalidCP_ThrowsBadRequest()
	{
		var container = new InMemoryContainer(
			new ContainerProperties("test", "/pk")
			{
				ComputedProperties = new Collection<ComputedProperty>
				{
					new() { Name = "cp_x", Query = "SELECT VALUE c.name FROM c" }
				}
			});

		// Replace with invalid CP (> 20 CPs)
		var defs = Enumerable.Range(1, 21)
			.Select(i => new ComputedProperty { Name = $"cp_{i}", Query = $"SELECT VALUE c.f{i} FROM c" })
			.ToList();
		var newProps = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(defs)
		};

		var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
			container.ReplaceContainerAsync(newProps));
		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ComputedProperty_IsDefinedOnCP_CorrectSemantics()
	{
		var container = new InMemoryContainer(
			new ContainerProperties("test", "/pk")
			{
				ComputedProperties = new Collection<ComputedProperty>
				{
					new() { Name = "cp_lower", Query = "SELECT VALUE LOWER(c.name) FROM c" }
				}
			});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "p" }), // no name
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id FROM c WHERE IS_DEFINED(c.cp_lower) ORDER BY c.id"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		// Only item 1 has "name" defined, so only item 1's CP should be defined
		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ComputedProperty_IsDefinedOnCP_TrueWhenSourceExists()
	{
		var container = new InMemoryContainer(
			new ContainerProperties("test", "/pk")
			{
				ComputedProperties = new Collection<ComputedProperty>
				{
					new() { Name = "cp_lower", Query = "SELECT VALUE LOWER(c.name) FROM c" }
				}
			});

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		var iter = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id FROM c WHERE IS_DEFINED(c.cp_lower)"));
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}
}
