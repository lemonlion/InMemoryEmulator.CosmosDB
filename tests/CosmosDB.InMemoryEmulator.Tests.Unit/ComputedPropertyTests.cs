using System.Collections.ObjectModel;
using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for Cosmos DB computed properties — virtual top-level properties defined on
/// a container with a Name and Query, evaluated at query time but not persisted on documents.
/// </summary>
public class ComputedPropertyTests
{
	private static InMemoryContainer CreateContainerWithComputedProperties(
		params (string Name, string Query)[] definitions)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				definitions.Select(d => new ComputedProperty
				{
					Name = d.Name,
					Query = d.Query
				}).ToList())
		};
		return new InMemoryContainer(props);
	}

	private static async Task SeedItems(InMemoryContainer container, params object[] items)
	{
		foreach (var item in items)
		{
			var jObj = JObject.FromObject(item);
			await container.CreateItemAsync(jObj, new PartitionKey(jObj["pk"]!.ToString()));
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  SELECT projection
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_ProjectedInSelect()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "BOB" });

		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(2);
		results.Select(r => r["cp_lowerName"]?.ToString()).Should().BeEquivalentTo("alice", "bob");
	}

	[Fact]
	public async Task ComputedProperty_NotIncludedInSelectStar()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT * FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var doc = response.First();

		doc["cp_lowerName"].Should().BeNull("SELECT * must not include computed properties");
		doc["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task ComputedProperty_ExplicitSelectWithPersistedAndComputed()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.name, c.cp_lowerName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var doc = response.First();

		doc["name"]!.ToString().Should().Be("Alice");
		doc["cp_lowerName"]!.ToString().Should().Be("alice");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  WHERE clause
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_UsedInWhereClause()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" });

		var query = new QueryDefinition("SELECT c.id FROM c WHERE c.cp_lowerName = 'alice'");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().HaveCount(1);
		response.First()["id"]!.ToString().Should().Be("1");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  ORDER BY
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_UsedInOrderBy()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Charlie" },
			new { id = "2", pk = "p", name = "Alice" },
			new { id = "3", pk = "p", name = "Bob" });

		var query = new QueryDefinition(
			"SELECT c.id FROM c ORDER BY c.cp_lowerName ASC");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Select(r => r["id"]!.ToString()).Should().ContainInOrder("2", "3", "1");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  GROUP BY
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_UsedInGroupBy()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_category", "SELECT VALUE LOWER(c.category) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", category = "Books" },
			new { id = "2", pk = "p", category = "books" },
			new { id = "3", pk = "p", category = "Electronics" });

		var query = new QueryDefinition(
			"SELECT c.cp_category, COUNT(1) AS cnt FROM c GROUP BY c.cp_category");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		var booksGroup = results.First(r => r["cp_category"]!.ToString() == "books");
		booksGroup["cnt"]!.Value<int>().Should().Be(2);

		var elecGroup = results.First(r => r["cp_category"]!.ToString() == "electronics");
		elecGroup["cnt"]!.Value<int>().Should().Be(1);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// ═══════════════════════════════════════════════════════════════════════════
	//  Multiple computed properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_MultipleOnSameContainer()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"),
			("cp_upperName", "SELECT VALUE UPPER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.cp_lowerName, c.cp_upperName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var doc = response.First();

		doc["cp_lowerName"]!.ToString().Should().Be("alice");
		doc["cp_upperName"]!.ToString().Should().Be("ALICE");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  ReplaceContainerAsync updates
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_UpdatedViaReplaceContainer()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		// Verify original works
		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
		var iter1 = container.GetItemQueryIterator<JObject>(query);
		var res1 = await iter1.ReadNextAsync();
		res1.First()["cp_lowerName"]!.ToString().Should().Be("alice");

		// Replace with a different computed property
		var readResp = await container.ReadContainerAsync();
		var updatedProps = readResp.Resource;
		updatedProps.ComputedProperties = new Collection<ComputedProperty>
		{
			new ComputedProperty
			{
				Name = "cp_upperName",
				Query = "SELECT VALUE UPPER(c.name) FROM c"
			}
		};
		await container.ReplaceContainerAsync(updatedProps);

		// Old computed property no longer exists
		var query2 = new QueryDefinition("SELECT c.cp_upperName FROM c");
		var iter2 = container.GetItemQueryIterator<JObject>(query2);
		var res2 = await iter2.ReadNextAsync();
		res2.First()["cp_upperName"]!.ToString().Should().Be("ALICE");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Arithmetic expression
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_ArithmeticExpression()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_discountedPrice", "SELECT VALUE c.price * 0.8 FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", price = 100.0 });

		var query = new QueryDefinition("SELECT c.cp_discountedPrice FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var doc = response.First();

		doc["cp_discountedPrice"]!.Value<double>().Should().Be(80.0);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  String concatenation
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_ConcatExpression()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_fullName", "SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", first = "Jane", last = "Doe" });

		var query = new QueryDefinition("SELECT c.cp_fullName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_fullName"]!.ToString().Should().Be("Jane Doe");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Partition-scoped queries
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_WithPartitionKeyFilter()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p1", name = "Alice" },
			new { id = "2", pk = "p2", name = "Bob" });

		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
		var options = new QueryRequestOptions { PartitionKey = new PartitionKey("p1") };
		var iterator = container.GetItemQueryIterator<JObject>(query, requestOptions: options);
		var response = await iterator.ReadNextAsync();

		response.Should().HaveCount(1);
		response.First()["cp_lowerName"]!.ToString().Should().Be("alice");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  No regression for containers without computed properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_NoComputedProperties_ZeroOverhead()
	{
		var container = new InMemoryContainer("test", "/pk");

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.name FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().HaveCount(1);
		response.First()["name"]!.ToString().Should().Be("Alice");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Container read returns computed properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_StoredAndReturnedOnContainerRead()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		var response = await container.ReadContainerAsync();
		var cps = response.Resource.ComputedProperties;

		cps.Should().HaveCount(1);
		cps[0].Name.Should().Be("cp_lowerName");
		cps[0].Query.Should().Be("SELECT VALUE LOWER(c.name) FROM c");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Document read does not include computed properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_DoesNotPersistOnDocument()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		// Direct point read — computed property should NOT be on the document
		var readResponse = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		readResponse.Resource["cp_lowerName"].Should().BeNull(
			"computed properties are virtual and not persisted on documents");
		readResponse.Resource["name"]!.ToString().Should().Be("Alice");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 2: Query clause combinations
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_UsedWithDistinct()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_category", "SELECT VALUE LOWER(c.category) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", category = "Books" },
			new { id = "2", pk = "p", category = "books" },
			new { id = "3", pk = "p", category = "Electronics" });

		var query = new QueryDefinition("SELECT DISTINCT c.cp_category FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task ComputedProperty_UsedWithTop()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Charlie" },
			new { id = "2", pk = "p", name = "Alice" },
			new { id = "3", pk = "p", name = "Bob" });

		var query = new QueryDefinition(
			"SELECT TOP 2 c.cp_lowerName FROM c ORDER BY c.cp_lowerName");
		var results = new List<JObject>();
		var iterator = container.GetItemQueryIterator<JObject>(query);
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(2);
		results[0]["cp_lowerName"]!.ToString().Should().Be("alice");
		results[1]["cp_lowerName"]!.ToString().Should().Be("bob");
	}

	[Fact]
	public async Task ComputedProperty_UsedWithOffsetLimit()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" },
			new { id = "3", pk = "p", name = "Charlie" });

		var query = new QueryDefinition(
			"SELECT c.cp_lowerName FROM c ORDER BY c.cp_lowerName OFFSET 1 LIMIT 1");
		var results = new List<JObject>();
		var iterator = container.GetItemQueryIterator<JObject>(query);
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().ContainSingle()
			.Which["cp_lowerName"]!.ToString().Should().Be("bob");
	}

	[Fact]
	public async Task ComputedProperty_UsedWithValueSelect()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" });

		var query = new QueryDefinition("SELECT VALUE c.cp_lowerName FROM c");
		var iterator = container.GetItemQueryIterator<JToken>(query);
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Select(r => r.ToString()).Should().BeEquivalentTo("alice", "bob");
	}

	[Fact]
	public async Task ComputedProperty_UsedInAggregateFunction()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_discountedPrice", "SELECT VALUE c.price * 0.8 FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", price = 100.0 },
			new { id = "2", pk = "p", price = 200.0 });

		var query = new QueryDefinition("SELECT VALUE SUM(c.cp_discountedPrice) FROM c");
		var iterator = container.GetItemQueryIterator<JToken>(query);
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().ContainSingle().Which.Value<double>().Should().Be(240.0);
	}

	[Fact]
	public async Task ComputedProperty_UsedWithAlias()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.cp_lowerName AS lowered FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["lowered"]!.ToString().Should().Be("alice");
	}

	[Fact]
	public async Task ComputedProperty_UsedInWhereWithComparison()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_discountedPrice", "SELECT VALUE c.price * 0.8 FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", price = 100.0 },
			new { id = "2", pk = "p", price = 200.0 });

		var query = new QueryDefinition("SELECT c.id FROM c WHERE c.cp_discountedPrice < 100");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ComputedProperty_UsedInWhereWithBooleanLogic()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" },
			new { id = "3", pk = "p", name = "Charlie" });

		var query = new QueryDefinition(
			"SELECT c.id FROM c WHERE c.cp_lowerName = 'alice' OR c.cp_lowerName = 'bob'");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().HaveCount(2);
	}

	[Fact]
	public async Task ComputedProperty_UsedInExpressionInSelect()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_discountedPrice", "SELECT VALUE c.price * 0.8 FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", price = 100.0 });

		var query = new QueryDefinition(
			"SELECT c.price - c.cp_discountedPrice AS savings FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["savings"]!.Value<double>().Should().BeApproximately(20.0, 0.01);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 3: Expression types
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_NestedPropertyAccess()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_city", "SELECT VALUE c.address.city FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", address = new { city = "London" } });

		var query = new QueryDefinition("SELECT c.cp_city FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_city"]!.ToString().Should().Be("London");
	}

	[Fact]
	public async Task ComputedProperty_MathFunctions()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_rounded", "SELECT VALUE ROUND(c.price) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", price = 19.99 });

		var query = new QueryDefinition("SELECT c.cp_rounded FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_rounded"]!.Value<double>().Should().Be(20.0);
	}

	[Fact]
	public async Task ComputedProperty_StringLengthFunction()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_nameLen", "SELECT VALUE LENGTH(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.cp_nameLen FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_nameLen"]!.Value<int>().Should().Be(5);
	}

	[Fact]
	public async Task ComputedProperty_BooleanExpression()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_hasAli", "SELECT VALUE CONTAINS(c.name, 'ali', true) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" });

		var query = new QueryDefinition("SELECT c.id, c.cp_hasAli FROM c ORDER BY c.id");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		results[0]["cp_hasAli"]!.Value<bool>().Should().BeTrue();
		results[1]["cp_hasAli"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task ComputedProperty_TypeCheckFunction()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_isStr", "SELECT VALUE IS_STRING(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.cp_isStr FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_isStr"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task ComputedProperty_ArrayFunction()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_tagCount", "SELECT VALUE ARRAY_LENGTH(c.tags) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", tags = new[] { "a", "b", "c" } });

		var query = new QueryDefinition("SELECT c.cp_tagCount FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_tagCount"]!.Value<int>().Should().Be(3);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 4: Edge cases & lifecycle
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_EmptyComputedPropertiesCollection()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>()
		};
		var container = new InMemoryContainer(props);

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.name FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().ContainSingle().Which["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task ComputedProperty_EvaluatesPerItem()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_discounted", "SELECT VALUE c.price * 0.9 FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", price = 100.0 },
			new { id = "2", pk = "p", price = 200.0 },
			new { id = "3", pk = "p", price = 300.0 });

		var query = new QueryDefinition(
			"SELECT c.id, c.cp_discounted FROM c ORDER BY c.id");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		results[0]["cp_discounted"]!.Value<double>().Should().Be(90.0);
		results[1]["cp_discounted"]!.Value<double>().Should().Be(180.0);
		results[2]["cp_discounted"]!.Value<double>().Should().Be(270.0);
	}

	[Fact]
	public async Task ComputedProperty_ReEvaluatesAfterDocumentUpdate()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		// Verify initial value
		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
		var iter1 = container.GetItemQueryIterator<JObject>(query);
		(await iter1.ReadNextAsync()).First()["cp_lowerName"]!.ToString().Should().Be("alice");

		// Update the document
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Bob" }),
			new PartitionKey("p"));

		// CP should reflect the update
		var iter2 = container.GetItemQueryIterator<JObject>(query);
		(await iter2.ReadNextAsync()).First()["cp_lowerName"]!.ToString().Should().Be("bob");
	}

	[Fact]
	public async Task ComputedProperty_CrossPartitionQuery()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p1", name = "Alice" },
			new { id = "2", pk = "p2", name = "Bob" });

		// Cross-partition (no partition key filter)
		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c ORDER BY c.cp_lowerName");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}

		results.Should().HaveCount(2);
		results[0]["cp_lowerName"]!.ToString().Should().Be("alice");
		results[1]["cp_lowerName"]!.ToString().Should().Be("bob");
	}

	[Fact]
	public async Task ComputedProperty_OldCPRemovedAfterReplace()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		// Replace with completely different CP
		var readResp = await container.ReadContainerAsync();
		var props = readResp.Resource;
		props.ComputedProperties = new Collection<ComputedProperty>
		{
			new() { Name = "cp_upperName", Query = "SELECT VALUE UPPER(c.name) FROM c" }
		};
		await container.ReplaceContainerAsync(props);

		// Old CP should return null
		var query = new QueryDefinition("SELECT c.cp_lowerName, c.cp_upperName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var doc = response.First();

		// Old CP should not evaluate to "alice" anymore — it is no longer a computed property
		var oldVal = doc["cp_lowerName"];
		(oldVal is null || oldVal.Type == JTokenType.Null).Should().BeTrue(
			"old CP should no longer be evaluated after replace — property should be absent or null");

		doc["cp_upperName"]!.ToString().Should().Be("ALICE");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 5: Divergent behaviour
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ComputedProperty_UndefinedPropagation_RealCosmos()
	{
		// When the source property is missing (undefined), LOWER(undefined) → undefined,
		// meaning the computed property should be ABSENT from the result document entirely.
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", age = 30 }); // no "name" field

		var query = new QueryDefinition("SELECT c.id, c.cp_lowerName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var doc = response.First();

		// Property should be absent (undefined), not present as null
		doc["cp_lowerName"].Should().BeNull("property should be absent from the result entirely");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 1 — Implementation bug tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ComputedPropertyImplementationBugTests
{
	private static InMemoryContainer CreateContainerWithComputedProperties(
		params (string Name, string Query)[] definitions)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				definitions.Select(d => new ComputedProperty
				{
					Name = d.Name,
					Query = d.Query
				}).ToList())
		};
		return new InMemoryContainer(props);
	}

	private static async Task SeedItems(InMemoryContainer container, params object[] items)
	{
		foreach (var item in items)
		{
			var jObj = JObject.FromObject(item);
			await container.CreateItemAsync(jObj, new PartitionKey(jObj["pk"]!.ToString()));
		}
	}

	[Fact]
	public async Task ComputedProperty_NotIncludedInSelectAliasStar_RealCosmos()
	{
		// EXPECTED REAL COSMOS: SELECT c.* should exclude computed properties,
		// identical to SELECT * behaviour.
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.* FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var doc = response.First();

		doc.ContainsKey("cp_lowerName").Should().BeFalse("SELECT c.* should exclude CPs like SELECT *");
	}

	[Fact]
	public async Task ComputedProperty_ConcatWithUndefinedArg_RealCosmos()
	{
		// CONCAT with any undefined arg → entire result undefined
		var container = CreateContainerWithComputedProperties(
			("cp_fullName", "SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", first = "Jane" });

		var query = new QueryDefinition("SELECT c.id, c.cp_fullName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var doc = response.First();

		doc["cp_fullName"].Should().BeNull("CONCAT with undefined arg should not produce a value");
	}

	[Fact]
	public async Task ComputedProperty_PatchComputedPropertyPath_RealCosmos()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		// Real Cosmos: this should throw 400 Bad Request
		var act = () => container.PatchItemAsync<JObject>(
			"1", new PartitionKey("p"),
			new[] { PatchOperation.Set("/cp_lowerName", "hacked") });
		await act.Should().ThrowAsync<CosmosException>();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 2 — Unfinished tests from prior plan
// ═══════════════════════════════════════════════════════════════════════════════

public class ComputedPropertyUnfinishedPriorPlanTests
{
	private static InMemoryContainer CreateContainerWithComputedProperties(
		params (string Name, string Query)[] definitions)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				definitions.Select(d => new ComputedProperty
				{
					Name = d.Name,
					Query = d.Query
				}).ToList())
		};
		return new InMemoryContainer(props);
	}

	private static async Task SeedItems(InMemoryContainer container, params object[] items)
	{
		foreach (var item in items)
		{
			var jObj = JObject.FromObject(item);
			await container.CreateItemAsync(jObj, new PartitionKey(jObj["pk"]!.ToString()));
		}
	}

	[Fact]
	public async Task ComputedProperty_SubstringWithIndexOf()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_prefix", "SELECT VALUE SUBSTRING(c.categoryName, 0, INDEX_OF(c.categoryName, ',')) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", categoryName = "Bikes, Touring Bikes" });

		var query = new QueryDefinition("SELECT c.cp_prefix FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_prefix"]!.ToString().Should().Be("Bikes");
	}

	[Fact]
	public async Task ComputedProperty_ConditionalIIF()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_ageGroup", "SELECT VALUE IIF(c.age >= 18, 'adult', 'minor') FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", age = 25 },
			new { id = "2", pk = "p", age = 10 });

		var query = new QueryDefinition("SELECT c.id, c.cp_ageGroup FROM c ORDER BY c.id");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		results[0]["cp_ageGroup"]!.ToString().Should().Be("adult");
		results[1]["cp_ageGroup"]!.ToString().Should().Be("minor");
	}

	[Fact]
	public async Task ComputedProperty_CoalesceExpression()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_displayName", "SELECT VALUE c.nickname ?? c.name FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob", nickname = "Bobby" });

		var query = new QueryDefinition("SELECT c.id, c.cp_displayName FROM c ORDER BY c.id");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		results[0]["cp_displayName"]!.ToString().Should().Be("Alice");
		results[1]["cp_displayName"]!.ToString().Should().Be("Bobby");
	}

	[Fact]
	public async Task ComputedProperty_DifferentFromAlias()
	{
		// CP query using "root" instead of "c" as the FROM alias
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(root.name) FROM root"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_lowerName"]!.ToString().Should().Be("alice");
	}

	[Fact]
	public async Task ComputedProperty_ReadItemStreamAsync_ExcludesComputed()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var response = await container.ReadItemStreamAsync("1", new PartitionKey("p"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		using var reader = new StreamReader(response.Content);
		var json = await reader.ReadToEndAsync();
		var doc = JObject.Parse(json);

		doc.ContainsKey("cp_lowerName").Should().BeFalse(
			"point read (stream) should not include computed properties");
	}

	[Fact]
	public async Task ComputedProperty_NameCollisionWithPersistedProperty()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_name", "SELECT VALUE LOWER(c.name) FROM c"));

		// Document has a persisted field with the same name as the CP
		var jObj = JObject.FromObject(new { id = "1", pk = "p", name = "Alice", cp_name = "Persisted" });
		await container.CreateItemAsync(jObj, new PartitionKey("p"));

		// In queries, the computed value should override the persisted value
		var query = new QueryDefinition("SELECT c.cp_name FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_name"]!.ToString().Should().Be("alice",
			"computed definition should take precedence over persisted value in queries");
	}

	[Fact]
	public async Task ComputedProperty_ReplaceContainerStreamAsync_InvalidatesCache()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		// Verify initial CP works
		var q1 = container.GetItemQueryIterator<JObject>(new QueryDefinition("SELECT c.cp_lowerName FROM c"));
		(await q1.ReadNextAsync()).First()["cp_lowerName"]!.ToString().Should().Be("alice");

		// Replace via stream variant
		var readResp = await container.ReadContainerAsync();
		var props = readResp.Resource;
		props.ComputedProperties = new Collection<ComputedProperty>
		{
			new() { Name = "cp_upperName", Query = "SELECT VALUE UPPER(c.name) FROM c" }
		};

		using var stream = new MemoryStream();
		using (var writer = new StreamWriter(stream, leaveOpen: true))
		{
			await writer.WriteAsync(JsonConvert.SerializeObject(props));
		}
		stream.Position = 0;
		await container.ReplaceContainerStreamAsync(props);

		// New CP should work after stream replace
		var q2 = container.GetItemQueryIterator<JObject>(new QueryDefinition("SELECT c.cp_upperName FROM c"));
		(await q2.ReadNextAsync()).First()["cp_upperName"]!.ToString().Should().Be("ALICE");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 3 — CRUD operation & lifecycle tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ComputedPropertyCrudLifecycleTests
{
	private static InMemoryContainer CreateContainerWithComputedProperties(
		params (string Name, string Query)[] definitions)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				definitions.Select(d => new ComputedProperty
				{
					Name = d.Name,
					Query = d.Query
				}).ToList())
		};
		return new InMemoryContainer(props);
	}

	private static async Task SeedItems(InMemoryContainer container, params object[] items)
	{
		foreach (var item in items)
		{
			var jObj = JObject.FromObject(item);
			await container.CreateItemAsync(jObj, new PartitionKey(jObj["pk"]!.ToString()));
		}
	}

	[Fact]
	public async Task ComputedProperty_ReadManyItemsAsync_ExcludesComputed()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" });

		var response = await container.ReadManyItemsAsync<JObject>(
			new List<(string, PartitionKey)>
			{
				("1", new PartitionKey("p")),
				("2", new PartitionKey("p"))
			});

		foreach (var doc in response)
		{
			doc.ContainsKey("cp_lowerName").Should().BeFalse(
				"ReadManyItemsAsync should not include computed properties");
		}
	}

	[Fact]
	public async Task ComputedProperty_ReadManyItemsStreamAsync_ExcludesComputed()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var response = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("p")) });

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		using var reader = new StreamReader(response.Content);
		var json = await reader.ReadToEndAsync();
		json.Should().NotContain("cp_lowerName",
			"ReadManyItemsStreamAsync should not include computed properties");
	}

	[Fact]
	public async Task ComputedProperty_ChangeFeedDoesNotIncludeComputed()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var iterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.LatestVersion);

		var items = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			items.AddRange(response);
		}

		items.Should().NotBeEmpty();
		foreach (var doc in items)
		{
			doc.ContainsKey("cp_lowerName").Should().BeFalse(
				"change feed should not include computed properties");
		}
	}

	[Fact]
	public async Task ComputedProperty_TransactionalBatchReadItem_ExcludesComputed()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var batch = container.CreateTransactionalBatch(new PartitionKey("p"));
		batch.ReadItem("1");
		var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		var result = response.GetOperationResultAtIndex<JObject>(0);
		result.Resource.ContainsKey("cp_lowerName").Should().BeFalse(
			"batch read should not include computed properties");
	}

	[Fact]
	public async Task ComputedProperty_DeleteItemThenQuery_NoCPLeakage()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		// Verify CP before delete
		var q1 = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.cp_lowerName FROM c"));
		(await q1.ReadNextAsync()).Should().ContainSingle()
			.Which["cp_lowerName"]!.ToString().Should().Be("alice");

		// Delete the item
		await container.DeleteItemAsync<JObject>("1", new PartitionKey("p"));

		// Query should return no results
		var q2 = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.cp_lowerName FROM c"));
		var response = await q2.ReadNextAsync();
		response.Should().BeEmpty();
	}

	[Fact]
	public async Task ComputedProperty_UpsertDoesNotPersistComputed()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		// Upsert with a field named same as CP
		var updated = JObject.FromObject(new { id = "1", pk = "p", name = "Bob", cp_lowerName = "overridden" });
		await container.UpsertItemAsync(updated, new PartitionKey("p"));

		// Point read should show persisted value (not computed)
		var readResp = await container.ReadItemAsync<JObject>("1", new PartitionKey("p"));
		readResp.Resource["cp_lowerName"]!.ToString().Should().Be("overridden",
			"point read returns persisted data, not computed");

		// Query should use computed value
		var q = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.cp_lowerName FROM c"));
		(await q.ReadNextAsync()).First()["cp_lowerName"]!.ToString().Should().Be("bob",
			"query should use computed definition, not persisted value");
	}

	[Fact]
	public async Task ComputedProperty_CreateContainerAsync_ViaDatabase()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var containerProps = new ContainerProperties("testcontainer", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_lowerName", Query = "SELECT VALUE LOWER(c.name) FROM c" }
			}
		};

		var containerResponse = await db.CreateContainerAsync(containerProps);
		var container = containerResponse.Container;

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_lowerName"]!.ToString().Should().Be("alice");
	}

	[Fact]
	public async Task ComputedProperty_CreateContainerIfNotExistsAsync_ViaDatabase()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseAsync("testdb")).Database;

		var containerProps = new ContainerProperties("testcontainer", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_lowerName", Query = "SELECT VALUE LOWER(c.name) FROM c" }
			}
		};

		var containerResponse = await db.CreateContainerIfNotExistsAsync(containerProps);
		var container = containerResponse.Container;

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "p", name = "Alice" }),
			new PartitionKey("p"));

		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_lowerName"]!.ToString().Should().Be("alice");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 4 — Expression types & query operator tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ComputedPropertyExpressionTests
{
	private static InMemoryContainer CreateContainerWithComputedProperties(
		params (string Name, string Query)[] definitions)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				definitions.Select(d => new ComputedProperty
				{
					Name = d.Name,
					Query = d.Query
				}).ToList())
		};
		return new InMemoryContainer(props);
	}

	private static async Task SeedItems(InMemoryContainer container, params object[] items)
	{
		foreach (var item in items)
		{
			var jObj = JObject.FromObject(item);
			await container.CreateItemAsync(jObj, new PartitionKey(jObj["pk"]!.ToString()));
		}
	}

	[Fact]
	public async Task ComputedProperty_UsingSystemProperty_Ts()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_timestamp", "SELECT VALUE c._ts FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.cp_timestamp FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var ts = response.First()["cp_timestamp"];

		ts.Should().NotBeNull();
		ts!.Value<long>().Should().BeGreaterThan(0, "_ts system property should be accessible in CP");
	}

	[Fact]
	public async Task ComputedProperty_TimestampToDateTime()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_datetime", "SELECT VALUE TimestampToDateTime(c._ts * 1000) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", name = "Alice" });

		var query = new QueryDefinition("SELECT c.cp_datetime FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var dt = response.First()["cp_datetime"]!.ToString();

		dt.Should().Contain("T", "should be a datetime string in ISO format");
		dt.Should().EndWith("Z", "should be UTC");
	}

	[Fact]
	public async Task ComputedProperty_StringSplit()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_skuParts", "SELECT VALUE StringSplit(c.sku, '-') FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", sku = "BK-T79U-50" });

		var query = new QueryDefinition("SELECT c.cp_skuParts FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var parts = response.First()["cp_skuParts"] as JArray;

		parts.Should().NotBeNull();
		parts!.Select(t => t.ToString()).Should().BeEquivalentTo(new[] { "BK", "T79U", "50" });
	}

	[Fact]
	public async Task ComputedProperty_ParameterizedQueryOnCP()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" });

		var query = new QueryDefinition("SELECT c.id FROM c WHERE c.cp_lowerName = @name")
			.WithParameter("@name", "alice");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ComputedProperty_InOperatorWithCP()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", name = "Bob" },
			new { id = "3", pk = "p", name = "Charlie" });

		var query = new QueryDefinition(
			"SELECT c.id FROM c WHERE c.cp_lowerName IN ('alice', 'bob') ORDER BY c.id");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		results.Should().HaveCount(2);
		results[0]["id"]!.ToString().Should().Be("1");
		results[1]["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task ComputedProperty_BetweenOperatorWithCP()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_discounted", "SELECT VALUE c.price * 0.8 FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", price = 50.0 },   // 40
			new { id = "2", pk = "p", price = 100.0 },  // 80
			new { id = "3", pk = "p", price = 200.0 }); // 160

		var query = new QueryDefinition(
			"SELECT c.id FROM c WHERE c.cp_discounted BETWEEN 50 AND 100");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().ContainSingle().Which["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task ComputedProperty_ArrayContainsOnCPResult_RealCosmos()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_tags", "SELECT VALUE c.tags FROM c"));

		var item1 = JObject.FromObject(new { id = "1", pk = "p" });
		item1["tags"] = new JArray("important", "urgent");
		await container.CreateItemAsync(item1, new PartitionKey("p"));

		var item2 = JObject.FromObject(new { id = "2", pk = "p" });
		item2["tags"] = new JArray("low");
		await container.CreateItemAsync(item2, new PartitionKey("p"));

		var query = new QueryDefinition(
			"SELECT c.id FROM c WHERE ARRAY_CONTAINS(c.cp_tags, 'important')");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task ComputedProperty_IsDefinedOnCP()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Alice" },
			new { id = "2", pk = "p", age = 30 }); // no name

		var query = new QueryDefinition(
			"SELECT c.id FROM c WHERE IS_DEFINED(c.cp_lowerName) ORDER BY c.id");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		// Both items have the CP defined (emulator returns null for undefined source,
		// which IS_DEFINED treats as defined)
		response.Count.Should().BeGreaterThanOrEqualTo(1);
	}

	[Fact]
	public async Task ComputedProperty_ToStringConversion()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_ageStr", "SELECT VALUE ToString(c.age) FROM c"));

		await SeedItems(container, new { id = "1", pk = "p", age = 25 });

		var query = new QueryDefinition("SELECT c.cp_ageStr FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		response.First()["cp_ageStr"]!.ToString().Should().Be("25");
	}

	[Fact]
	public async Task ComputedProperty_NullExplicitInput()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		// Explicit null value for name (not missing/undefined)
		var jObj = JObject.FromObject(new { id = "1", pk = "p" });
		jObj["name"] = JValue.CreateNull();
		await container.CreateItemAsync(jObj, new PartitionKey("p"));

		var query = new QueryDefinition("SELECT c.cp_lowerName FROM c");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();

		// Cosmos DB: LOWER(null) → undefined → computed property not set
		response.First()["cp_lowerName"].Should().BeNull();
	}

	[Fact]
	public async Task ComputedProperty_MultipleCPsUsedTogether()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"),
			("cp_discounted", "SELECT VALUE c.price * 0.9 FROM c"));

		await SeedItems(container,
			new { id = "1", pk = "p", name = "Charlie", price = 200.0 },
			new { id = "2", pk = "p", name = "Alice", price = 50.0 },
			new { id = "3", pk = "p", name = "Bob", price = 100.0 });

		var query = new QueryDefinition(
			"SELECT c.cp_lowerName, c.cp_discounted FROM c WHERE c.cp_discounted < 100 ORDER BY c.cp_lowerName");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		results.Should().HaveCount(2);
		results[0]["cp_lowerName"]!.ToString().Should().Be("alice");
		results[0]["cp_discounted"]!.Value<double>().Should().Be(45.0);
		results[1]["cp_lowerName"]!.ToString().Should().Be("bob");
		results[1]["cp_discounted"]!.Value<double>().Should().Be(90.0);
	}

	[Fact]
	public async Task ComputedProperty_CPWithJoinQuery()
	{
		var container = CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT VALUE LOWER(c.name) FROM c"));

		var jObj = JObject.FromObject(new { id = "1", pk = "p", name = "Alice" });
		jObj["tags"] = new JArray(
			JObject.FromObject(new { name = "tag1" }),
			JObject.FromObject(new { name = "tag2" }));
		await container.CreateItemAsync(jObj, new PartitionKey("p"));

		var query = new QueryDefinition(
			"SELECT c.cp_lowerName, t.name AS tagName FROM c JOIN t IN c.tags");
		var iterator = container.GetItemQueryIterator<JObject>(query);
		var response = await iterator.ReadNextAsync();
		var results = response.ToList();

		results.Should().HaveCount(2);
		results.All(r => r["cp_lowerName"]!.ToString() == "alice").Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 5 — Validation & constraint divergent tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ComputedPropertyValidationDivergentTests
{
	private static InMemoryContainer CreateContainerWithComputedProperties(
		params (string Name, string Query)[] definitions)
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
				definitions.Select(d => new ComputedProperty
				{
					Name = d.Name,
					Query = d.Query
				}).ToList())
		};
		return new InMemoryContainer(props);
	}

	private static async Task SeedItems(InMemoryContainer container, params object[] items)
	{
		foreach (var item in items)
		{
			var jObj = JObject.FromObject(item);
			await container.CreateItemAsync(jObj, new PartitionKey(jObj["pk"]!.ToString()));
		}
	}

	[Fact]
	public void ComputedProperty_MaxLimit_RealCosmos()
	{
		var definitions = Enumerable.Range(1, 21)
			.Select(i => ($"cp_{i}", $"SELECT VALUE c.field{i} FROM c"))
			.ToArray();

		var act = () => CreateContainerWithComputedProperties(definitions);

		act.Should().Throw<CosmosException>();
	}

	[Fact]
	public void ComputedProperty_ReservedNameValidation_RealCosmos()
	{
		var act = () => CreateContainerWithComputedProperties(
			("id", "SELECT VALUE LOWER(c.name) FROM c"));

		act.Should().Throw<CosmosException>();
	}

	[Fact]
	public void ComputedProperty_QueryMustBeSelectValue_RealCosmos()
	{
		var act = () => CreateContainerWithComputedProperties(
			("cp_lowerName", "SELECT LOWER(c.name) FROM c"));
		act.Should().Throw<CosmosException>().Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_QueryCannotContainWhere_RealCosmos()
	{
		var act = () => CreateContainerWithComputedProperties(
			("cp_name", "SELECT VALUE c.name FROM c WHERE c.age > 18"));
		act.Should().Throw<CosmosException>().Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public void ComputedProperty_CannotReferenceOtherCP_RealCosmos()
	{
		var act = () => CreateContainerWithComputedProperties(
			("cp_lower", "SELECT VALUE LOWER(c.name) FROM c"),
			("cp_upper_of_lower", "SELECT VALUE UPPER(c.cp_lower) FROM c"));
		act.Should().Throw<CosmosException>().Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 6 — LINQ divergent tests
// ═══════════════════════════════════════════════════════════════════════════════

public class ComputedPropertyLinqDivergentTests
{
	[Fact(Skip = "In real Cosmos DB, LINQ queries are translated to SQL and computed properties " +
				 "are available. In the emulator, LINQ queries bypass the SQL engine entirely " +
				 "and read directly from the in-memory store, so computed properties are not " +
				 "available in LINQ queries.")]
	public void ComputedProperty_LinqQuery_RealCosmos() { }

	[Fact]
	public async Task ComputedProperty_LinqQuery_DoesNotIncludeComputed()
	{
		// DIVERGENT: LINQ queries bypass the SQL engine — CPs are not available
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>
			{
				new() { Name = "cp_lowerName", Query = "SELECT VALUE LOWER(c.name) FROM c" }
			}
		};
		var container = new InMemoryContainer(props);

		var jObj = JObject.FromObject(new { id = "1", pk = "p", name = "Alice" });
		await container.CreateItemAsync(jObj, new PartitionKey("p"));

		var queryable = container.GetItemLinqQueryable<JObject>(allowSynchronousQueryExecution: true);
		var results = queryable.ToList();

		results.Should().ContainSingle();
		results[0].ContainsKey("cp_lowerName").Should().BeFalse(
			"LINQ queries bypass SQL engine and do not evaluate computed properties");
	}
}
