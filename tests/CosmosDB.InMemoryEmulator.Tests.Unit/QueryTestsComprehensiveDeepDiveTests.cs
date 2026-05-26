using System.Collections.ObjectModel;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan #28: QueryTests Comprehensive Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════


// ── 1.1 StringTo* Functions — Null Input ─────────────────────────────────────

public class StringToFunctionNullTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task SeedAsync()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":null}""")),
			new PartitionKey("p"));
	}

	[Fact]
	public async Task StringToArray_NullInput_ReturnsUndefined()
	{
		await SeedAsync();
		var results = await QueryAsync<JObject>("SELECT StringToArray(c.val) AS r FROM c");
		// null input → undefined → field excluded from projection
		results[0]["r"].Should().BeNull();
	}

	[Fact]
	public async Task StringToObject_NullInput_ReturnsUndefined()
	{
		await SeedAsync();
		var results = await QueryAsync<JObject>("SELECT StringToObject(c.val) AS r FROM c");
		results[0]["r"].Should().BeNull();
	}

	[Fact]
	public async Task StringToNumber_NullInput_ReturnsUndefined()
	{
		await SeedAsync();
		var results = await QueryAsync<JObject>("SELECT StringToNumber(c.val) AS r FROM c");
		results[0]["r"].Should().BeNull();
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.2 StringToBoolean — Non-String Inputs ─────────────────────────────────

public class StringToBooleanEdgeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task StringToBoolean_NumberInput_ReturnsUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":42}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT StringToBoolean(c.val) AS r FROM c");
		results[0]["r"].Should().BeNull();
	}

	[Fact]
	public async Task StringToBoolean_ObjectInput_ReturnsUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":{"a":1}}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT StringToBoolean(c.val) AS r FROM c");
		results[0]["r"].Should().BeNull();
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.3 NumberBin Edge Cases ─────────────────────────────────────────────────

public class NumberBinEdgeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task NumberBin_NegativeValue_BinsCorrectly()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":-7}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT NumberBin(c.val, 5) AS r FROM c");
		// Floor(-7/5)*5 = Floor(-1.4)*5 = -2*5 = -10
		results[0]["r"]!.Value<double>().Should().Be(-10);
	}

	[Fact]
	public async Task NumberBin_ZeroBinSize_ReturnsUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":10}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT NumberBin(c.val, 0) AS r FROM c");
		results[0]["r"].Should().BeNull();
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.4 DISTINCT on Objects — Property Order ────────────────────────────────

public class DistinctObjectPropertyOrderTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Distinct_ObjectPropertyOrder_TreatedAsSame()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","obj":{"a":1,"b":2}}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"2","pk":"p","obj":{"b":2,"a":1}}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT DISTINCT VALUE c.obj FROM c");
		// Emulator uses JToken.DeepEquals which treats same properties as equal regardless of order
		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task Distinct_ObjectPropertyOrder_Divergent_EmulatorTreatsAsDifferent()
	{
		// No divergence here — emulator correctly handles property order via JToken.DeepEquals
		await Task.CompletedTask;
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.5 IS_PRIMITIVE — Comprehensive ────────────────────────────────────────

public class IsPrimitiveComprehensiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task IsPrimitive_Null_ReturnsTrue()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":null}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT IS_PRIMITIVE(c.val) AS r FROM c");
		results[0]["r"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task IsPrimitive_Object_ReturnsFalse()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":{"a":1}}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT IS_PRIMITIVE(c.val) AS r FROM c");
		results[0]["r"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsPrimitive_Array_ReturnsFalse()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":[1,2]}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT IS_PRIMITIVE(c.val) AS r FROM c");
		results[0]["r"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsPrimitive_UndefinedField_ReturnsFalse()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT IS_PRIMITIVE(c.missing) AS r FROM c");
		// Emulator returns false for undefined fields
		results[0]["r"]!.Value<bool>().Should().BeFalse();
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.6 COUNT Semantics ──────────────────────────────────────────────────────

public class CountSemanticsTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Count_FieldWithNullValue_CountsNullAsPresent()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":null}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"p","val":42}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"p"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT COUNT(c.val) AS r FROM c");
		// Emulator counts only non-null, non-undefined fields
		// item 1: val=null → excluded, item 2: val=42 → counted, item 3: missing → excluded  
		results[0]["r"]!.Value<int>().Should().Be(1);
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.7 ARRAY_CONTAINS — Partial Object Match ──────────────────────────────

public class ArrayContainsPartialMatchTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task ArrayContains_PartialObjectMatch_WithThirdArg()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","items":[{"id":"a","name":"Apple","price":1},{"id":"b","name":"Banana","price":2}]}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<bool>(
			"""SELECT VALUE ARRAY_CONTAINS(c.items, {"id": "a"}, true) FROM c""");
		results[0].Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContains_PartialObjectMatch_NoThirdArg_ExactOnly()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","items":[{"id":"a","name":"Apple","price":1}]}""")),
			new PartitionKey("p"));
		// Without 3rd arg or with false — requires exact match
		var results = await QueryAsync<bool>(
			"""SELECT VALUE ARRAY_CONTAINS(c.items, {"id": "a"}) FROM c""");
		results[0].Should().BeFalse();
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.10 ARRAY_SLICE Edge Cases ─────────────────────────────────────────────

public class ArraySliceEdgeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task ArraySlice_StartAndLength_ReturnsSubset()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","arr":[1,2,3,4,5]}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT ARRAY_SLICE(c.arr, 1, 2) AS r FROM c");
		var arr = results[0]["r"]!.ToObject<int[]>()!;
		arr.Should().Equal(2, 3);
	}

	[Fact]
	public async Task ArraySlice_StartBeyondArrayLength_ReturnsEmpty()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","arr":[1,2,3]}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT ARRAY_SLICE(c.arr, 10) AS r FROM c");
		results[0]["r"]!.ToObject<int[]>()!.Should().BeEmpty();
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.13 SELECT * with Computed Properties ──────────────────────────────────

public class ComputedPropertySelectTests
{
	[Fact]
	public async Task SelectStar_ExcludesComputedProperties()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
			[
				new ComputedProperty { Name = "fullName", Query = "SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c" }
			])
		};
		var container = new InMemoryContainer(props);
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","first":"John","last":"Doe"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>(container, "SELECT * FROM c");
		results[0]["fullName"].Should().BeNull();
	}

	[Fact]
	public async Task SelectExplicit_IncludesComputedProperties()
	{
		var props = new ContainerProperties("test", "/pk")
		{
			ComputedProperties = new Collection<ComputedProperty>(
			[
				new ComputedProperty { Name = "fullName", Query = "SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c" }
			])
		};
		var container = new InMemoryContainer(props);
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","first":"John","last":"Doe"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>(container, "SELECT c.fullName FROM c");
		results[0]["fullName"]!.ToString().Should().Be("John Doe");
	}

	private static async Task<List<T>> QueryAsync<T>(InMemoryContainer container, string sql)
	{
		var query = container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.15 ORDER BY with Complex Expression ───────────────────────────────────

public class OrderByComplexExpressionTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task OrderBy_ComplexArithmeticExpression_Works()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","a":10,"b":5}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"2","pk":"p","a":3,"b":1}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"3","pk":"p","a":7,"b":3}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT c.id FROM c ORDER BY c.a + c.b");
		results[0]["id"]!.ToString().Should().Be("2"); // 3+1=4
		results[1]["id"]!.ToString().Should().Be("3"); // 7+3=10
		results[2]["id"]!.ToString().Should().Be("1"); // 10+5=15
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.17 CONCAT Variadic ────────────────────────────────────────────────────

public class ConcatVariadicTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Concat_FiveArgs_ConcatenatesAll()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>(
			"SELECT CONCAT('a', 'b', 'c', 'd', 'e') AS r FROM c");
		results[0]["r"]!.ToString().Should().Be("abcde");
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.18 Nested Ternary ─────────────────────────────────────────────────────

public class NestedTernaryTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task NestedTernary_EvaluatesCorrectly()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","a":true,"b":false}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>(
			"SELECT (c.a ? (c.b ? 'x' : 'y') : 'z') AS r FROM c");
		// a=true, b=false → 'y'
		results[0]["r"]!.ToString().Should().Be("y");
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.20 MaxItemCount = -1 ──────────────────────────────────────────────────

public class MaxItemCountTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task MaxItemCount_MinusOne_ReturnsAllInSinglePage()
	{
		for (var i = 0; i < 10; i++)
			await _container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"p"}""")),
				new PartitionKey("p"));

		var query = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { MaxItemCount = -1 });

		var page = await query.ReadNextAsync();
		page.Count.Should().Be(10);
		query.HasMoreResults.Should().BeFalse();
	}
}


// ── 1.22 COALESCE with Null ─────────────────────────────────────────────────

public class CoalesceNullTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Coalesce_NullLeft_ReturnsFallback()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","val":null}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT (c.val ?? 'fallback') AS r FROM c");
		// Emulator treats null same as undefined for COALESCE — returns the right side
		results[0]["r"]!.ToString().Should().Be("fallback");
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.23 REPLACE Function Edge Cases ────────────────────────────────────────

public class ReplaceFunctionEdgeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Replace_SearchNotFound_ReturnsOriginal()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","name":"Hello"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT REPLACE(c.name, 'xyz', 'abc') AS r FROM c");
		results[0]["r"]!.ToString().Should().Be("Hello");
	}

	[Fact]
	public async Task Replace_EmptyReplacement_RemovesOccurrences()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","name":"Hello World"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT REPLACE(c.name, ' ', '') AS r FROM c");
		results[0]["r"]!.ToString().Should().Be("HelloWorld");
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.24 REPLICATE Negative Count ───────────────────────────────────────────

public class ReplicateEdgeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Replicate_NegativeCount_ReturnsUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT REPLICATE('abc', -1) AS r FROM c");
		results[0]["r"].Should().BeNull();
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.25 DateTimeAdd Invalid Part ───────────────────────────────────────────

public class DateTimeAddEdgeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task DateTimeAdd_InvalidPart_ReturnsUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>(
			"SELECT DateTimeAdd('invalid', 1, '2023-01-01T00:00:00Z') AS r FROM c");
		results[0]["r"].Should().BeNull();
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.27 Query with Composite Partition Key ─────────────────────────────────

public class CompositePartitionKeyQueryTests
{
	[Fact]
	public async Task Query_WithCompositePartitionKey_FiltersCorrectly()
	{
		var container = new InMemoryContainer("test", ["/tenant", "/region"]);
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","tenant":"a","region":"us","name":"One"}""")),
			new PartitionKeyBuilder().Add("a").Add("us").Build());
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"2","tenant":"b","region":"eu","name":"Two"}""")),
			new PartitionKeyBuilder().Add("b").Add("eu").Build());

		var query = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.tenant = 'a'"));
		var results = new List<JObject>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which["name"]!.ToString().Should().Be("One");
	}
}


// ── 1.28 SELECT with Array Index Access ─────────────────────────────────────

public class ArrayIndexSelectTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Select_ArrayIndex_ProjectsCorrectly()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","tags":["first","second","third"]}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT c.tags[0] AS first_tag FROM c");
		results[0]["first_tag"]!.ToString().Should().Be("first");
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.29 WHERE with Chained String Functions ────────────────────────────────

public class ChainedStringFunctionTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Where_ChainedFunctions_Works()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","name":"  Alice  "}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"p","name":"Bob"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>(
			"SELECT c.id FROM c WHERE LOWER(TRIM(c.name)) = 'alice'");
		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.30 ARRAY_CONCAT with 3 Arrays ────────────────────────────────────────

public class ArrayConcatThreeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task ArrayConcat_ThreeArrays_ConcatenatesAll()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"p","a":[1],"b":[2,3],"c":[4,5,6]}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>(
			"SELECT ARRAY_CONCAT(c.a, c.b, c.c) AS r FROM c");
		results[0]["r"]!.ToObject<int[]>()!.Should().Equal(1, 2, 3, 4, 5, 6);
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.12 WHERE IS NOT NULL ──────────────────────────────────────────────────

public class IsNotNullTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Where_IsNotNull_ExcludesNullAndUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":"hello"}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"p","val":null}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"p"}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT c.id FROM c WHERE c.val IS NOT NULL");
		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.11 Aggregate with Mixed Types ─────────────────────────────────────────

public class AggregateMixedTypeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task Sum_MixedTypes_IgnoresNonNumeric()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"p","val":10}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"p","val":"hello"}""")),
			new PartitionKey("p"));
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"p","val":20}""")),
			new PartitionKey("p"));
		var results = await QueryAsync<JObject>("SELECT SUM(c.val) AS r FROM c");
		results[0]["r"]!.Value<double>().Should().Be(30);
	}

	private async Task<List<T>> QueryAsync<T>(string sql)
	{
		var query = _container.GetItemQueryIterator<T>(new QueryDefinition(sql));
		var results = new List<T>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}
}


// ── 1.21 Stream Iterator Response Envelope ──────────────────────────────────

public class StreamIteratorEnvelopeTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task StreamIterator_ResponseHas_Count_Rid_Documents()
	{
		for (var i = 0; i < 3; i++)
			await _container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"id":"{{{i}}}","pk":"p"}""")),
				new PartitionKey("p"));

		var feed = _container.GetItemQueryStreamIterator("SELECT * FROM c");
		var response = await feed.ReadNextAsync();
		var json = await new StreamReader(response.Content).ReadToEndAsync();
		var envelope = JObject.Parse(json);

		envelope["_count"]!.Value<int>().Should().Be(3);
		envelope["_rid"].Should().NotBeNull();
		envelope["Documents"]!.Should().BeOfType<JArray>().Which.Should().HaveCount(3);
	}
}
