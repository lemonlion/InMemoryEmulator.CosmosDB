using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class ExtendedArrayFunctionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedItems()
	{
		var items = new[]
		{
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, Tags = ["a", "b", "c"] },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, Tags = ["b", "c", "d"] },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, Tags = ["x", "y", "z"] },
			new TestDocument { Id = "4", PartitionKey = "pk1", Name = "Diana", Value = 40, Tags = [] },
		};
		foreach (var item in items)
		{
			await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
		}
	}

	private async Task SeedTypeDiverseItems()
	{
		await SeedItems();

		// id=5: numeric tags [1, 2, 3]
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "5", partitionKey = "pk1", tags = new[] { 1, 2, 3 } }),
			new PartitionKey("pk1"));

		// id=6: mixed types [1, "a", true, null]
		var mixed = new JObject { ["id"] = "6", ["partitionKey"] = "pk1", ["tags"] = new JArray(1, "a", true, JValue.CreateNull()) };
		await _container.CreateItemAsync(mixed, new PartitionKey("pk1"));

		// id=7: nested array property
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "7", partitionKey = "pk1", nested = new { items = new[] { "p", "q" } } }),
			new PartitionKey("pk1"));

		// id=8: no tags property at all
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "8", partitionKey = "pk1", name = "NoTags" }),
			new PartitionKey("pk1"));

		// id=9: tags is a string scalar, not an array
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "9", partitionKey = "pk1", tags = "not-an-array" }),
			new PartitionKey("pk1"));

		// id=10: tags with duplicates ["a", "a", "b"]
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "10", partitionKey = "pk1", tags = new[] { "a", "a", "b" } }),
			new PartitionKey("pk1"));

		// id=11: two array properties for both-from-document tests
		var twoArrays = new JObject { ["id"] = "11", ["partitionKey"] = "pk1", ["tags"] = new JArray("a", "b"), ["otherTags"] = new JArray("b", "c") };
		await _container.CreateItemAsync(twoArrays, new PartitionKey("pk1"));

		// id=12: object array for OBJ tests
		var objArray = new JObject
		{
			["id"] = "12",
			["partitionKey"] = "pk1",
			["tags"] = new JArray(
				new JObject { ["name"] = "x" },
				new JObject { ["name"] = "y" })
		};
		await _container.CreateItemAsync(objArray, new PartitionKey("pk1"));

		// id=13: float array for FL tests
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "13", partitionKey = "pk1", tags = new[] { 1.5, 2.5, 3.5 } }),
			new PartitionKey("pk1"));

		// id=14: array-of-array for NA tests
		var nestedArrays = new JObject
		{
			["id"] = "14",
			["partitionKey"] = "pk1",
			["tags"] = new JArray(new JArray(1, 2), new JArray(3, 4))
		};
		await _container.CreateItemAsync(nestedArrays, new PartitionKey("pk1"));

		// id=15: bool+string mix for TS tests
		var boolStringMix = new JObject
		{
			["id"] = "15",
			["partitionKey"] = "pk1",
			["tags"] = new JArray(true, "true", false, "false")
		};
		await _container.CreateItemAsync(boolStringMix, new PartitionKey("pk1"));
	}

	// ── ARRAY_CONTAINS_ANY ──────────────────────────────────────────────────

	[Fact]
	public async Task ArrayContainsAny_WithMatchingElement_ReturnsTrue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a', 'z'])");

		var results = await QueryAll<TestDocument>(query);

		// Item 1 has "a", Item 3 has "z"
		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(["1", "3"]);
	}

	[Fact]
	public async Task ArrayContainsAny_WithNoMatchingElement_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['q', 'r'])");

		var results = await QueryAll<TestDocument>(query);

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ArrayContainsAny_WithEmptySearchArray_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, [])");

		var results = await QueryAll<TestDocument>(query);

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ArrayContainsAny_WithEmptySourceArray_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a']) AND c.id = '4'");

		var results = await QueryAll<TestDocument>(query);

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ArrayContainsAny_WithNumericElements_ReturnsTrue()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, [2, 4]) FROM c WHERE c.id = '5'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_WithMixedTypes_MatchesSameTypeOnly()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1, "a", true, null] — searching for ["a"] should match the string "a"
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '6'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_NumberDoesNotMatchString_TypeSensitive()
	{
		await SeedTypeDiverseItems();
		// Item 5 has [1, 2, 3] — searching for string "1" should NOT match number 1
		// Real Cosmos DB is type-sensitive: number ≠ string
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['1']) FROM c WHERE c.id = '5'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAny_BoolDoesNotMatchString_TypeSensitive()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1, "a", true, null] — searching for string "true" should NOT match boolean true
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['true']) FROM c WHERE c.id = '6'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAny_WithNullElement_MatchesNull()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1, "a", true, null] — searching for [null] should match
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, [null]) FROM c WHERE c.id = '6'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_NonExistentProperty_ReturnsFalse()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.nonExistent, ['a']) FROM c WHERE c.id = '8'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAny_NonArrayProperty_ReturnsFalse()
	{
		await SeedTypeDiverseItems();
		// Item 9 has tags = "not-an-array" (string scalar)
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '9'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAny_InProjection_ReturnsBoolean()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ARRAY_CONTAINS_ANY(c.tags, ['a']) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["result"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_WithDuplicatesInSource_StillMatches()
	{
		await SeedTypeDiverseItems();
		// Item 10 has ["a", "a", "b"]
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '10'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_WithDuplicatesInSearch_StillMatches()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a', 'a']) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_BothArraysFromDocument_Works()
	{
		await SeedTypeDiverseItems();
		// Item 11 has tags=["a","b"] and otherTags=["b","c"] — overlap is "b"
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, c.otherTags) FROM c WHERE c.id = '11'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_NestedPropertyPath_Works()
	{
		await SeedTypeDiverseItems();
		// Item 7 has nested.items = ["p", "q"]
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.nested.items, ['p']) FROM c WHERE c.id = '7'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_SingleElementMatch_ReturnsTrue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_VariadicForm_Works()
	{
		await SeedItems();
		// Real Cosmos DB syntax: ARRAY_CONTAINS_ANY(array, val1, val2, ...)
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, 'a', 'z') FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_VariadicForm_SingleArg_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, 'a') FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_VariadicForm_NoMatch_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, 'q', 'r') FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	// ── ARRAY_CONTAINS_ALL ──────────────────────────────────────────────────

	[Fact]
	public async Task ArrayContainsAll_WhenAllPresent_ReturnsTrue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, ['a', 'b'])");

		var results = await QueryAll<TestDocument>(query);

		// Only Item 1 has both "a" and "b"
		results.Should().HaveCount(1);
		results[0].Id.Should().Be("1");
	}

	[Fact]
	public async Task ArrayContainsAll_WhenSomeMissing_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, ['a', 'd'])");

		var results = await QueryAll<TestDocument>(query);

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ArrayContainsAll_WithEmptySearchArray_ReturnsAll()
	{
		await SeedItems();
		// In Cosmos DB, ARRAY_CONTAINS_ALL with an empty search array returns true for all items
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, [])");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(4);
	}

	[Fact]
	public async Task ArrayContainsAll_SourceEqualsSearch_ReturnsTrue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a', 'b', 'c']) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_SourceIsSupersetOfSearch_ReturnsTrue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a', 'b']) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_EmptySourceWithNonEmptySearch_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a']) FROM c WHERE c.id = '4'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAll_WithNumericElements_ReturnsTrue()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, [1, 3]) FROM c WHERE c.id = '5'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_NumberDoesNotMatchString_TypeSensitive()
	{
		await SeedTypeDiverseItems();
		// Item 5 has [1, 2, 3] — searching for strings "1","2" should NOT match
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['1', '2']) FROM c WHERE c.id = '5'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAll_WithNullElement_MatchesNull()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1, "a", true, null] — searching for [null] should match
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, [null]) FROM c WHERE c.id = '6'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_NonExistentProperty_ReturnsFalse()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.nonExistent, ['a']) FROM c WHERE c.id = '8'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAll_NonArrayProperty_ReturnsFalse()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a']) FROM c WHERE c.id = '9'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAll_DuplicatesInSearch_StillWorks()
	{
		await SeedItems();
		// Source has "a", search has "a" twice — should still be true
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a', 'a']) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_InProjection_ReturnsBoolean()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ARRAY_CONTAINS_ALL(c.tags, ['a']) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["result"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_BothEmptyArrays_ReturnsTrue()
	{
		await SeedItems();
		// Empty search against empty source → vacuously true
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, []) FROM c WHERE c.id = '4'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_VariadicForm_Works()
	{
		await SeedItems();
		// Real Cosmos DB syntax: ARRAY_CONTAINS_ALL(array, val1, val2, ...)
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, 'a', 'b', 'c') FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_VariadicForm_SingleMissing_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, 'a', 'q') FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAll_NestedPropertyPath_Works()
	{
		await SeedTypeDiverseItems();
		// Item 7 has nested.items = ["p", "q"]
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.nested.items, ['p', 'q']) FROM c WHERE c.id = '7'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── SetIntersect ────────────────────────────────────────────────────────

	[Fact]
	public async Task SetIntersect_ReturnsCommonElements()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['b', 'c', 'z']) AS common FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var common = (JArray)results[0]["common"]!;
		common.Select(t => t.ToString()).Should().BeEquivalentTo(["b", "c"]);
	}

	[Fact]
	public async Task SetIntersect_WithNoOverlap_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['q', 'r']) AS common FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var common = (JArray)results[0]["common"]!;
		common.Should().BeEmpty();
	}

	[Fact]
	public async Task SetIntersect_WithEmptySourceArray_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['a']) AS common FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var common = (JArray)results[0]["common"]!;
		common.Should().BeEmpty();
	}

	[Fact]
	public async Task SetIntersect_BothEmpty_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, []) AS common FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().BeEmpty();
	}

	[Fact]
	public async Task SetIntersect_EmptySecondArray_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, []) AS common FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().BeEmpty();
	}

	[Fact]
	public async Task SetIntersect_CompleteOverlap_ReturnsSameElements()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, c.tags) AS common FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c"]);
	}

	[Fact]
	public async Task SetIntersect_DuplicatesInInput_DedupedInResult()
	{
		await SeedTypeDiverseItems();
		// Item 10 has ["a", "a", "b"] — intersect with ["a","b"] should give ["a","b"] (deduped)
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['a', 'b']) AS common FROM c WHERE c.id = '10'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Select(t => t.ToString()).Should().Equal("a", "b");
	}

	[Fact]
	public async Task SetIntersect_WithNumericElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 5 has [1, 2, 3] — intersect with [2, 4] → [2]
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, [2, 4]) AS common FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().ContainSingle();
		common[0].Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task SetIntersect_TypeSensitive_NumberDoesNotMatchString()
	{
		await SeedTypeDiverseItems();
		// Item 5 has [1, 2, 3] — intersect with ["1","2"] → [] (number ≠ string)
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['1', '2']) AS common FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().BeEmpty();
	}

	[Fact]
	public async Task SetIntersect_WithNullElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1, "a", true, null] — intersect with [null, "a"] → [null is tricky, "a" should match]
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, [null, 'a']) AS common FROM c WHERE c.id = '6'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().HaveCount(2);
	}

	[Fact]
	public async Task SetIntersect_NestedInArrayLength_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ARRAY_LENGTH(SetIntersect(c.tags, ['a', 'b', 'x'])) AS count FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["count"]!.Value<long>().Should().Be(2);
	}

	[Fact]
	public async Task SetIntersect_BothFromDocument_Works()
	{
		await SeedTypeDiverseItems();
		// Item 11 has tags=["a","b"] and otherTags=["b","c"]
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, c.otherTags) AS common FROM c WHERE c.id = '11'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Select(t => t.ToString()).Should().Equal("b");
	}

	[Fact]
	public async Task SetIntersect_WithMixedTypes_OnlyMatchesSameType()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1, "a", true, null] — intersect with [1, "b", true] → [1, true]
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, [1, 'b', true]) AS common FROM c WHERE c.id = '6'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().HaveCount(2);
	}

	// ── SetUnion ────────────────────────────────────────────────────────────

	[Fact]
	public async Task SetUnion_ReturnsCombinedDistinctElements()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetUnion(c.tags, ['b', 'd', 'e']) AS combined FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var combined = (JArray)results[0]["combined"]!;
		combined.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c", "d", "e"]);
	}

	[Fact]
	public async Task SetUnion_WithEmptySourceArray_ReturnsSecondArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetUnion(c.tags, ['a', 'b']) AS combined FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var combined = (JArray)results[0]["combined"]!;
		combined.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b"]);
	}

	[Fact]
	public async Task SetUnion_WithIdenticalArrays_ReturnsOriginal()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetUnion(c.tags, c.tags) AS combined FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var combined = (JArray)results[0]["combined"]!;
		combined.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c"]);
	}

	[Fact]
	public async Task SetUnion_BothEmpty_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetUnion(c.tags, []) AS combined FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Should().BeEmpty();
	}

	[Fact]
	public async Task SetUnion_EmptySecondArray_ReturnsFirstArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetUnion(c.tags, []) AS combined FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c"]);
	}

	[Fact]
	public async Task SetUnion_DuplicatesWithinSingleArray_Deduped()
	{
		await SeedTypeDiverseItems();
		// Item 10 has ["a", "a", "b"] — union with ["c"] → ["a", "b", "c"]
		var query = new QueryDefinition("SELECT SetUnion(c.tags, ['c']) AS combined FROM c WHERE c.id = '10'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Select(t => t.ToString()).Should().Equal("a", "b", "c");
	}

	[Fact]
	public async Task SetUnion_WithNumericElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 5 has [1, 2, 3] — union with [3, 4] → [1, 2, 3, 4]
		var query = new QueryDefinition("SELECT SetUnion(c.tags, [3, 4]) AS combined FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Select(t => t.Value<int>()).Should().Equal(1, 2, 3, 4);
	}

	[Fact]
	public async Task SetUnion_TypeSensitive_NumberAndStringBothKept()
	{
		await SeedTypeDiverseItems();
		// Item 5 has [1, 2, 3] — union with ["1","2"] → [1, 2, 3, "1", "2"] (type-sensitive, no dedup)
		var query = new QueryDefinition("SELECT SetUnion(c.tags, ['1', '2']) AS combined FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Should().HaveCount(5);
	}

	[Fact]
	public async Task SetUnion_OrderPreserved_FirstThenSecond()
	{
		await SeedItems();
		// Item 1 has ["a","b","c"] — union with ["d","e"] → order should be a,b,c,d,e
		var query = new QueryDefinition("SELECT SetUnion(c.tags, ['d', 'e']) AS combined FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Select(t => t.ToString()).Should().Equal("a", "b", "c", "d", "e");
	}

	[Fact]
	public async Task SetUnion_WithNullElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1, "a", true, null] — union with ["b", null] → null deduped
		var query = new QueryDefinition("SELECT SetUnion(c.tags, ['b', null]) AS combined FROM c WHERE c.id = '6'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		// Should have [1, "a", true, null, "b"] — 5 elements (null deduped)
		combined.Should().HaveCount(5);
	}

	[Fact]
	public async Task SetUnion_NestedInArrayLength_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ARRAY_LENGTH(SetUnion(c.tags, ['x', 'y'])) AS count FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["count"]!.Value<long>().Should().Be(5);
	}

	[Fact]
	public async Task SetUnion_BothFromDocument_Works()
	{
		await SeedTypeDiverseItems();
		// Item 11 has tags=["a","b"] and otherTags=["b","c"]
		var query = new QueryDefinition("SELECT SetUnion(c.tags, c.otherTags) AS combined FROM c WHERE c.id = '11'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Select(t => t.ToString()).Should().Equal("a", "b", "c");
	}

	[Fact]
	public async Task SetUnion_WithMixedTypes_AllKept()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1, "a", true, null] — union with [2, "b", false] → all 7 kept
		var query = new QueryDefinition("SELECT SetUnion(c.tags, [2, 'b', false]) AS combined FROM c WHERE c.id = '6'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Should().HaveCount(7);
	}

	// ── VALUE expressions with array functions ──────────────────────────────

	[Fact]
	public async Task ArrayContainsAny_AsValueExpression_ReturnsBoolean()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_AsValueExpression_ReturnsBoolean()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a', 'b', 'c']) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── Cross-cutting / Integration ─────────────────────────────────────────

	[Fact]
	public async Task ExtendedArrayFunctions_CombinedInWhereClause_Works()
	{
		await SeedItems();
		// Item 1 has ["a","b","c"] — matches both conditions
		var query = new QueryDefinition(
			"SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a']) AND ARRAY_CONTAINS_ALL(c.tags, ['a', 'b'])");

		var results = await QueryAll<TestDocument>(query);

		results.Should().ContainSingle();
		results[0].Id.Should().Be("1");
	}

	[Fact]
	public async Task SetIntersect_ResultUsedInWhere_WithArrayLength()
	{
		await SeedItems();
		// Items with at least 1 overlap with ["a","b"] — item 1 has a,b; item 2 has b
		var query = new QueryDefinition(
			"SELECT * FROM c WHERE ARRAY_LENGTH(SetIntersect(c.tags, ['a', 'b'])) > 0");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(["1", "2"]);
	}

	// ── Divergent behavior tests ────────────────────────────────────────────

	// DIVERGENT: SetIntersect returns empty array [] instead of undefined when
	// the source property doesn't exist. Real Cosmos DB would return undefined
	// (omit the property from the result).
	[Fact]
	public async Task SetIntersect_NonExistentProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		// Expected real Cosmos behavior: property "common" would not appear in result
		var query = new QueryDefinition("SELECT SetIntersect(c.nonExistent, ['a']) AS common FROM c WHERE c.id = '8'");
		var results = await QueryAll<JObject>(query);
		results.Should().ContainSingle();
		results[0]["common"].Should().BeNull(); // property should be absent
	}

	// Sister test: emulator now matches real Cosmos behavior
	[Fact]
	public async Task SetIntersect_NonExistentProperty_EmulatorAlsoReturnsUndefined()
	{
		// The emulator now correctly returns undefined (property absent) when input
		// array property doesn't exist, matching real Cosmos DB behavior.
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT SetIntersect(c.nonExistent, ['a']) AS common FROM c WHERE c.id = '8'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["common"].Should().BeNull(); // property absent
	}

	[Fact]
	public async Task SetUnion_NonExistentProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT SetUnion(c.nonExistent, ['a']) AS combined FROM c WHERE c.id = '8'");
		var results = await QueryAll<JObject>(query);
		results.Should().ContainSingle();
		results[0]["combined"].Should().BeNull();
	}

	// Sister test: emulator now matches real Cosmos behavior
	[Fact]
	public async Task SetUnion_NonExistentProperty_EmulatorAlsoReturnsUndefined()
	{
		// The emulator now correctly returns undefined (property absent) when input
		// array property doesn't exist, matching real Cosmos DB behavior.
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT SetUnion(c.nonExistent, ['a']) AS combined FROM c WHERE c.id = '8'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["combined"].Should().BeNull(); // property absent
	}

	// ── SetDifference ───────────────────────────────────────────────────────

	[Fact]
	public async Task SetDifference_ReturnsElementsInFirstNotInSecond()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['b', 'c', 'z']) AS diff FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.ToString()).Should().Equal("a");
	}

	[Fact]
	public async Task SetDifference_WithNoOverlap_ReturnsFirstArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['q', 'r']) AS diff FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.ToString()).Should().Equal("a", "b", "c");
	}

	[Fact]
	public async Task SetDifference_WithCompleteOverlap_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['a', 'b', 'c']) AS diff FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Should().BeEmpty();
	}

	[Fact]
	public async Task SetDifference_WithEmptyFirstArray_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['a']) AS diff FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Should().BeEmpty();
	}

	[Fact]
	public async Task SetDifference_WithEmptySecondArray_ReturnsFirstArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, []) AS diff FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.ToString()).Should().Equal("a", "b", "c");
	}

	[Fact]
	public async Task SetDifference_BothEmpty_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, []) AS diff FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Should().BeEmpty();
	}

	[Fact]
	public async Task SetDifference_DuplicatesInSource_DedupedInResult()
	{
		await SeedTypeDiverseItems();
		// Item 10 has ["a","a","b"] — minus ["b"] → ["a"] (deduped)
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['b']) AS diff FROM c WHERE c.id = '10'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.ToString()).Should().Equal("a");
	}

	[Fact]
	public async Task SetDifference_WithNumericElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 5 has [1,2,3] — minus [2,4] → [1,3]
		var query = new QueryDefinition("SELECT SetDifference(c.tags, [2, 4]) AS diff FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.Value<int>()).Should().Equal(1, 3);
	}

	[Fact]
	public async Task SetDifference_TypeSensitive_NumberDoesNotMatchString()
	{
		await SeedTypeDiverseItems();
		// Item 5 has [1,2,3] — minus ["1","2"] → [1,2,3] (number ≠ string)
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['1', '2']) AS diff FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.Value<int>()).Should().Equal(1, 2, 3);
	}

	[Fact]
	public async Task SetDifference_WithNullElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1,"a",true,null] — minus [null,"a"] → [1, true]
		var query = new QueryDefinition("SELECT SetDifference(c.tags, [null, 'a']) AS diff FROM c WHERE c.id = '6'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Should().HaveCount(2);
		diff[0].Value<int>().Should().Be(1);
		diff[1].Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task SetDifference_WithMixedTypes_OnlyRemovesSameType()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1,"a",true,null] — minus [1,"b",false] → ["a",true,null]
		var query = new QueryDefinition("SELECT SetDifference(c.tags, [1, 'b', false]) AS diff FROM c WHERE c.id = '6'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Should().HaveCount(3);
	}

	[Fact]
	public async Task SetDifference_IsNotCommutative()
	{
		await SeedTypeDiverseItems();
		// Item 11 has tags=["a","b"], otherTags=["b","c"]
		var query1 = new QueryDefinition("SELECT SetDifference(c.tags, c.otherTags) AS diff FROM c WHERE c.id = '11'");
		var query2 = new QueryDefinition("SELECT SetDifference(c.otherTags, c.tags) AS diff FROM c WHERE c.id = '11'");

		var results1 = await QueryAll<JObject>(query1);
		var results2 = await QueryAll<JObject>(query2);

		((JArray)results1[0]["diff"]!).Select(t => t.ToString()).Should().Equal("a");
		((JArray)results2[0]["diff"]!).Select(t => t.ToString()).Should().Equal("c");
	}

	[Fact]
	public async Task SetDifference_IdenticalArrays_ReturnsEmptyArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, c.tags) AS diff FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Should().BeEmpty();
	}

	[Fact]
	public async Task SetDifference_NestedInArrayLength_Works()
	{
		await SeedItems();
		// a,b,c minus b,c = a → length 1
		var query = new QueryDefinition("SELECT ARRAY_LENGTH(SetDifference(c.tags, ['b', 'c'])) AS count FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["count"]!.Value<long>().Should().Be(1);
	}

	[Fact]
	public async Task SetDifference_BothFromDocument_Works()
	{
		await SeedTypeDiverseItems();
		// Item 11: tags=["a","b"] minus otherTags=["b","c"] = ["a"]
		var query = new QueryDefinition("SELECT SetDifference(c.tags, c.otherTags) AS diff FROM c WHERE c.id = '11'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.ToString()).Should().Equal("a");
	}

	[Fact]
	public async Task SetDifference_OrderPreserved_FromFirstArray()
	{
		await SeedItems();
		// Item 3: tags=["x","y","z"] minus ["y"] → ["x","z"] (order preserved)
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['y']) AS diff FROM c WHERE c.id = '3'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.ToString()).Should().Equal("x", "z");
	}

	[Fact]
	public async Task SetDifference_NestedPropertyPath_Works()
	{
		await SeedTypeDiverseItems();
		// Item 7: nested.items=["p","q"] minus ["q"] = ["p"]
		var query = new QueryDefinition("SELECT SetDifference(c.nested.items, ['q']) AS diff FROM c WHERE c.id = '7'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.ToString()).Should().Equal("p");
	}

	[Fact]
	public async Task SetDifference_NonExistentProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT SetDifference(c.nonExistent, ['a']) AS diff FROM c WHERE c.id = '8'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["diff"].Should().BeNull(); // property absent = undefined
	}

	[Fact]
	public async Task SetDifference_InWhereClause_WithArrayLength()
	{
		await SeedItems();
		// Items with diff length > 0 after removing ["b","c","d"]
		// Item 1: ["a","b","c"] minus ["b","c","d"] = ["a"] (length 1 > 0) ✓
		// Item 2: ["b","c","d"] minus ["b","c","d"] = [] (length 0) ✗
		// Item 3: ["x","y","z"] minus ["b","c","d"] = ["x","y","z"] (length 3 > 0) ✓
		// Item 4: [] minus ["b","c","d"] = [] (length 0) ✗
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_LENGTH(SetDifference(c.tags, ['b', 'c', 'd'])) > 0");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(["1", "3"]);
	}

	// ── Parameterized Queries ───────────────────────────────────────────────

	[Fact]
	public async Task ArrayContainsAny_WithParameterizedSearchValues_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, @searchValues)")
			.WithParameter("@searchValues", new JArray("a", "z"));

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(["1", "3"]);
	}

	[Fact]
	public async Task ArrayContainsAll_WithParameterizedSearchValues_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, @searchValues)")
			.WithParameter("@searchValues", new JArray("a", "b"));

		var results = await QueryAll<TestDocument>(query);

		results.Should().ContainSingle();
		results[0].Id.Should().Be("1");
	}

	[Fact]
	public async Task SetIntersect_WithParameterizedArray_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, @otherArray) AS common FROM c WHERE c.id = '1'")
			.WithParameter("@otherArray", new JArray("b", "c", "z"));

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Select(t => t.ToString()).Should().BeEquivalentTo(["b", "c"]);
	}

	[Fact]
	public async Task SetDifference_WithParameterizedArray_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, @otherArray) AS diff FROM c WHERE c.id = '1'")
			.WithParameter("@otherArray", new JArray("b", "c", "z"));

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.ToString()).Should().Equal("a");
	}

	// ── Object Elements in Arrays ───────────────────────────────────────────

	[Fact]
	public async Task SetIntersect_WithObjectElements_MatchesDeepEqual()
	{
		await SeedTypeDiverseItems();
		// Item 12: tags=[{name:"x"},{name:"y"}] — intersect with [{name:"x"}] → [{name:"x"}]
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, [{\"name\":\"x\"}]) AS common FROM c WHERE c.id = '12'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().ContainSingle();
		common[0]["name"]!.Value<string>().Should().Be("x");
	}

	[Fact]
	public async Task SetUnion_WithObjectElements_DeduplicatesDeepEqual()
	{
		await SeedTypeDiverseItems();
		// Item 12: tags=[{name:"x"},{name:"y"}] — union with [{name:"x"},{name:"z"}]
		// → [{name:"x"},{name:"y"},{name:"z"}] (dedup {name:"x"})
		var query = new QueryDefinition("SELECT SetUnion(c.tags, [{\"name\":\"x\"},{\"name\":\"z\"}]) AS combined FROM c WHERE c.id = '12'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var combined = (JArray)results[0]["combined"]!;
		combined.Should().HaveCount(3);
	}

	[Fact]
	public async Task ArrayContainsAny_WithObjectElementInVariadicForm_Works()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, {\"name\":\"x\"}) FROM c WHERE c.id = '12'");
		var results = await QueryAll<bool>(query);
		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAny_WithObjectElement_EmulatorArrayForm()
	{
		await SeedTypeDiverseItems();
		// Using array form with object element
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, [{\"name\":\"x\"}]) FROM c WHERE c.id = '12'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task SetDifference_WithObjectElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 12: tags=[{name:"x"},{name:"y"}] — minus [{name:"x"}] → [{name:"y"}]
		var query = new QueryDefinition("SELECT SetDifference(c.tags, [{\"name\":\"x\"}]) AS diff FROM c WHERE c.id = '12'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Should().ContainSingle();
		diff[0]["name"]!.Value<string>().Should().Be("y");
	}

	// ── NOT Operator Negation ───────────────────────────────────────────────

	[Fact]
	public async Task ArrayContainsAny_WithNotOperator_ReturnsInverse()
	{
		await SeedItems();
		// Items 1 has "a" → excluded. Items 2,3,4 → returned.
		var query = new QueryDefinition("SELECT * FROM c WHERE NOT ARRAY_CONTAINS_ANY(c.tags, ['a'])");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(3);
		results.Select(r => r.Id).Should().BeEquivalentTo(["2", "3", "4"]);
	}

	[Fact]
	public async Task ArrayContainsAll_WithNotOperator_ReturnsInverse()
	{
		await SeedItems();
		// Only item 1 has all of ["a","b","c"] → excluded. Items 2,3,4 → returned.
		var query = new QueryDefinition("SELECT * FROM c WHERE NOT ARRAY_CONTAINS_ALL(c.tags, ['a', 'b', 'c'])");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(3);
		results.Select(r => r.Id).Should().BeEquivalentTo(["2", "3", "4"]);
	}

	// ── Literal Arrays in SQL ───────────────────────────────────────────────

	[Fact]
	public async Task SetIntersect_WithLiteralArrays_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE SetIntersect([1, 2, 3, 4], [3, 4, 5, 6]) FROM c WHERE c.id = '1'");

		var results = await QueryAll<JArray>(query);

		results.Should().ContainSingle();
		results[0].Select(t => t.Value<int>()).Should().Equal(3, 4);
	}

	[Fact]
	public async Task SetUnion_WithLiteralArrays_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE SetUnion([1, 2, 3, 4], [3, 4, 5, 6]) FROM c WHERE c.id = '1'");

		var results = await QueryAll<JArray>(query);

		results.Should().ContainSingle();
		results[0].Select(t => t.Value<int>()).Should().Equal(1, 2, 3, 4, 5, 6);
	}

	[Fact]
	public async Task SetDifference_WithLiteralArrays_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE SetDifference([1, 2, 3], [1, 2, 6, 7]) FROM c WHERE c.id = '1'");

		var results = await QueryAll<JArray>(query);

		results.Should().ContainSingle();
		results[0].Select(t => t.Value<int>()).Should().Equal(3);
	}

	[Fact]
	public async Task ArrayContainsAny_WithLiteralFirstArray_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY([1, true, '3', [1,2,3]], 1, true) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── Chained Set Operations ──────────────────────────────────────────────

	[Fact]
	public async Task SetUnion_OfSetIntersect_ChainedOperations()
	{
		await SeedItems();
		// tags=["a","b","c"], intersect(tags, ["a","b"]) = ["a","b"], union(["a","b"], ["x","y"]) = ["a","b","x","y"]
		var query = new QueryDefinition("SELECT SetUnion(SetIntersect(c.tags, ['a', 'b']), ['x', 'y']) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var result = (JArray)results[0]["result"]!;
		result.Select(t => t.ToString()).Should().Equal("a", "b", "x", "y");
	}

	[Fact]
	public async Task SetDifference_OfSetUnion_ChainedOperations()
	{
		await SeedItems();
		// tags=["a","b","c"], union(tags, ["d"]) = ["a","b","c","d"], diff(["a","b","c","d"], ["a","b"]) = ["c","d"]
		var query = new QueryDefinition("SELECT SetDifference(SetUnion(c.tags, ['d']), ['a', 'b']) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var result = (JArray)results[0]["result"]!;
		result.Select(t => t.ToString()).Should().Equal("c", "d");
	}

	// ── Float/Decimal Elements ──────────────────────────────────────────────

	[Fact]
	public async Task SetIntersect_WithFloatElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 13: tags=[1.5, 2.5, 3.5] — intersect with [2.5, 4.5] → [2.5]
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, [2.5, 4.5]) AS common FROM c WHERE c.id = '13'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().ContainSingle();
		common[0].Value<double>().Should().Be(2.5);
	}

	[Fact]
	public async Task SetDifference_WithFloatElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 13: tags=[1.5, 2.5, 3.5] — minus [2.5] → [1.5, 3.5]
		var query = new QueryDefinition("SELECT SetDifference(c.tags, [2.5]) AS diff FROM c WHERE c.id = '13'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Select(t => t.Value<double>()).Should().Equal(1.5, 3.5);
	}

	[Fact]
	public async Task SetUnion_FloatAndIntegerDistinct()
	{
		await SeedTypeDiverseItems();
		// Union of [1] and [1.0] — in JSON.NET, 1 is Integer, 1.0 is Float → both kept
		var query = new QueryDefinition("SELECT VALUE SetUnion([1], [1.0]) FROM c WHERE c.id = '1'");

		var results = await QueryAll<JArray>(query);

		results.Should().ContainSingle();
		results[0].Should().HaveCount(2);
	}

	[Fact]
	public async Task ArrayContainsAny_WithFloatElement_Works()
	{
		await SeedTypeDiverseItems();
		// Item 13: tags=[1.5, 2.5, 3.5] — contains 2.5?
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, [2.5]) FROM c WHERE c.id = '13'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── Additional Type-Sensitivity ─────────────────────────────────────────

	[Fact]
	public async Task ArrayContainsAll_BoolDoesNotMatchString_TypeSensitive()
	{
		await SeedTypeDiverseItems();
		// Item 6 has [1,"a",true,null] — search for ["true"] should NOT match boolean true
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['true']) FROM c WHERE c.id = '6'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task SetIntersect_BoolDoesNotMatchString_TypeSensitive()
	{
		await SeedTypeDiverseItems();
		// Item 15: tags=[true,"true",false,"false"] — intersect with ["true"] → ["true"] only
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['true']) AS common FROM c WHERE c.id = '15'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().ContainSingle();
		common[0].Value<string>().Should().Be("true");
	}

	[Fact]
	public async Task SetDifference_TypeSensitive_BoolVsString()
	{
		await SeedTypeDiverseItems();
		// Item 15: tags=[true,"true",false,"false"] — minus ["true"] → [true, false, "false"]
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['true']) AS diff FROM c WHERE c.id = '15'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		var diff = (JArray)results[0]["diff"]!;
		diff.Should().HaveCount(3);
	}

	// ── Combined with OR ────────────────────────────────────────────────────

	[Fact]
	public async Task ArrayContainsAny_CombinedWithOr_Works()
	{
		await SeedItems();
		// Item 1 has "a" (matched by ARRAY_CONTAINS_ANY), Item 3 matched by id = '3'
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a']) OR c.id = '3'");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(2);
		results.Select(r => r.Id).Should().BeEquivalentTo(["1", "3"]);
	}

	// ── Nested Array Elements ───────────────────────────────────────────────

	[Fact]
	public async Task SetIntersect_WithNestedArrayElements_Works()
	{
		await SeedTypeDiverseItems();
		// Item 14: tags=[[1,2],[3,4]] — intersect with [[1,2]] → [[1,2]]
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, [[1,2]]) AS common FROM c WHERE c.id = '14'");
		var results = await QueryAll<JObject>(query);
		results.Should().ContainSingle();
		var common = (JArray)results[0]["common"]!;
		common.Should().ContainSingle();
	}

	[Fact]
	public async Task ArrayContainsAny_WithNestedArrayElement_VariadicForm()
	{
		await SeedItems();
		// Docs example: ARRAY_CONTAINS_ANY([1, [1,2,3]], [1,2,3]) — variadic form, [1,2,3] is the value to search
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY([1, [1,2,3]], [1,2,3]) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── Cross-Cutting Operations ────────────────────────────────────────────

	[Fact]
	public async Task SetOperations_CombinedInSingleProjection()
	{
		await SeedItems();
		// Item 1: tags=["a","b","c"]
		var query = new QueryDefinition(
			"SELECT SetIntersect(c.tags, ['a','b']) AS common, SetUnion(c.tags, ['x']) AS combined, SetDifference(c.tags, ['a']) AS diff FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		((JArray)results[0]["common"]!).Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b"]);
		((JArray)results[0]["combined"]!).Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c", "x"]);
		((JArray)results[0]["diff"]!).Select(t => t.ToString()).Should().Equal("b", "c");
	}

	[Fact]
	public async Task ArrayContainsAny_InOrderBy_WithCount()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT c.id, ARRAY_LENGTH(SetIntersect(c.tags, ['a','b','c'])) AS overlap FROM c ORDER BY ARRAY_LENGTH(SetIntersect(c.tags, ['a','b','c'])) DESC");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCountGreaterThanOrEqualTo(4);
		// Item 1: overlap 3, Item 2: overlap 2, Items 3,4: overlap 0
		results[0]["id"]!.Value<string>().Should().Be("1");
		results[0]["overlap"]!.Value<long>().Should().Be(3);
	}

	[Fact]
	public async Task SetDifference_ResultUsedInArrayContainsAny()
	{
		await SeedItems();
		// tags=["a","b","c"], diff(tags, ["a"]) = ["b","c"], contains_any(["b","c"], "b") = true
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(SetDifference(c.tags, ['a']), 'b') FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── Divergent Behavior: Document Array Arg (BUG-3) ──────────────────────

	[Fact(Skip = "In real Cosmos DB, ARRAY_CONTAINS_ANY's second argument is a single scalar expression. If c.otherTags is an array, real Cosmos checks if c.tags contains the value as a nested array ELEMENT (false). The emulator instead iterates otherTags and checks if ANY element matches (true). This is an intentional emulator convenience divergence.")]
	public async Task ArrayContainsAny_DocumentArrayArg_RealCosmosTreatsSingleValue()
	{
		await SeedTypeDiverseItems();
		// Item 11: tags=["a","b"], otherTags=["b","c"]
		// Real Cosmos: does tags contain the VALUE ["b","c"]? No → false
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, c.otherTags) FROM c WHERE c.id = '11'");
		var results = await QueryAll<bool>(query);
		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAny_DocumentArrayArg_EmulatorIteratesElements()
	{
		await SeedTypeDiverseItems();
		// Item 11: tags=["a","b"], otherTags=["b","c"]
		// Emulator: iterates otherTags elements, finds "b" in tags → true
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags, c.otherTags) FROM c WHERE c.id = '11'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── Divergent Behavior: Non-Array Scalar Property (BUG-5) ───────────────

	[Fact]
	public async Task SetIntersect_NonArrayScalarProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		// Item 9: tags="not-an-array"
		var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['a']) AS common FROM c WHERE c.id = '9'");
		var results = await QueryAll<JObject>(query);
		results.Should().ContainSingle();
		results[0]["common"].Should().BeNull(); // undefined in real Cosmos
	}

	[Fact]
	public async Task SetUnion_NonArrayScalarProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT SetUnion(c.tags, ['a']) AS combined FROM c WHERE c.id = '9'");
		var results = await QueryAll<JObject>(query);
		results.Should().ContainSingle();
		results[0]["combined"].Should().BeNull();
	}

	[Fact]
	public async Task SetDifference_NonArrayScalarProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['a']) AS diff FROM c WHERE c.id = '9'");
		var results = await QueryAll<JObject>(query);
		results.Should().ContainSingle();
		results[0]["diff"].Should().BeNull();
	}

	// ════════════════════════════════════════════
	// Deep Dive: Additional Edge Cases
	// ════════════════════════════════════════════

	#region ARRAY_CONTAINS_ANY — Additional Edge Cases

	[Fact]
	public async Task ArrayContainsAny_WhereSourceIsNull_ReturnsFalse()
	{
		await SeedTypeDiverseItems();
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "null1", partitionKey = "pk1", tags = (object?)null }),
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a'])");
		var results = await QueryAll<string>(query);
		results.Should().NotContain("null1");
	}

	[Fact]
	public async Task ArrayContainsAny_CaseSensitive_NoMatch()
	{
		await SeedItems();
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "cs1", partitionKey = "pk1", tags = new[] { "ABC" } }),
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['abc'])");
		var results = await QueryAll<string>(query);
		results.Should().NotContain("cs1");
	}

	[Fact]
	public async Task ArrayContainsAny_WithNestedObjectInArray_MatchesDeepEqual()
	{
		await SeedTypeDiverseItems();
		// Item 12 has tags = [{name:"x"},{name:"y"}]
		var query = new QueryDefinition(
			"SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, [{\"name\":\"x\"}])");
		var results = await QueryAll<string>(query);
		results.Should().Contain("12");
	}

	[Fact]
	public async Task ArrayContainsAny_InSubquery_Works()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT VALUE c.id FROM c WHERE EXISTS(SELECT VALUE 1 FROM t IN c.tags WHERE t = 'a') AND ARRAY_CONTAINS_ANY(c.tags, ['b'])");
		var results = await QueryAll<string>(query);
		results.Should().Contain("1"); // tags=["a","b","c"]
	}

	[Fact]
	public async Task ArrayContainsAny_WithGroupBy_Works()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT c.partitionKey AS pk, COUNT(1) AS cnt FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['a']) GROUP BY c.partitionKey");
		var results = await QueryAll<JObject>(query);
		results.Should().ContainSingle();
		results[0]["cnt"]!.Value<long>().Should().Be(1); // Only id=1 has tag "a"
	}

	[Fact]
	public async Task ArrayContainsAny_WithSingleArgument_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ANY(c.tags) FROM c WHERE c.id = '1'");
		var results = await QueryAll<bool>(query);
		results.Single().Should().BeFalse();
	}

	#endregion

	#region ARRAY_CONTAINS_ALL — Additional Edge Cases

	[Fact]
	public async Task ArrayContainsAll_SingleElementSearch_Present_ReturnsTrue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['a']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<bool>(query);
		results.Single().Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_SingleElementSearch_Missing_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['z']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<bool>(query);
		results.Single().Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAll_WhereSourceIsNull_ReturnsFalse()
	{
		await SeedTypeDiverseItems();
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "null2", partitionKey = "pk1", tags = (object?)null }),
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, ['a'])");
		var results = await QueryAll<string>(query);
		results.Should().NotContain("null2");
	}

	[Fact]
	public async Task ArrayContainsAll_CaseSensitive_NoMatch()
	{
		await SeedItems();
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "cs2", partitionKey = "pk1", tags = new[] { "ABC" } }),
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, ['abc']) FROM c WHERE c.id = 'cs2'");
		var results = await QueryAll<bool>(query);
		results.Single().Should().BeFalse();
	}

	[Fact]
	public async Task ArrayContainsAll_WithNestedObjectInArray_MatchesDeepEqual()
	{
		await SeedTypeDiverseItems();
		// Item 12 has tags = [{name:"x"},{name:"y"}]
		var query = new QueryDefinition(
			"SELECT VALUE ARRAY_CONTAINS_ALL(c.tags, [{\"name\":\"x\"}, {\"name\":\"y\"}]) FROM c WHERE c.id = '12'");
		var results = await QueryAll<bool>(query);
		results.Single().Should().BeTrue();
	}

	[Fact]
	public async Task ArrayContainsAll_CombinedWithArrayContainsAny_InWhere()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS_ALL(c.tags, ['b','c']) AND ARRAY_CONTAINS_ANY(c.tags, ['a'])");
		var results = await QueryAll<string>(query);
		results.Should().ContainSingle().Which.Should().Be("1");
	}

	[Fact]
	public async Task ArrayContainsAll_WithSingleArgument_ReturnsFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ARRAY_CONTAINS_ALL(c.tags) FROM c WHERE c.id = '1'");
		var results = await QueryAll<bool>(query);
		results.Single().Should().BeFalse();
	}

	#endregion

	#region SetIntersect — Additional Edge Cases

	[Fact]
	public async Task SetIntersect_DuplicatesInBothArrays_DedupedCorrectly()
	{
		await SeedTypeDiverseItems();
		// Item 10 has tags=["a","a","b"]
		var query = new QueryDefinition(
			"SELECT VALUE SetIntersect(c.tags, ['a','a','b','b']) FROM c WHERE c.id = '10'");
		var results = await QueryAll<JArray>(query);
		var arr = results.Single();
		arr.Select(t => t.Value<string>()).Should().BeEquivalentTo(new[] { "a", "b" });
	}

	[Fact]
	public async Task SetIntersect_SingleElementArrays_Match()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE SetIntersect(['a'], ['a']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<JArray>(query);
		results.Single().Should().HaveCount(1);
		results.Single()[0]!.Value<string>().Should().Be("a");
	}

	[Fact]
	public async Task SetIntersect_SingleElementArrays_NoMatch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE SetIntersect(['a'], ['b']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<JArray>(query);
		results.Single().Should().BeEmpty();
	}

	[Fact]
	public async Task SetIntersect_WithNullProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "nullsi", partitionKey = "pk1", tags = (object?)null }),
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT SetIntersect(c.tags, ['a']) AS result FROM c WHERE c.id = 'nullsi'");
		var results = await QueryAll<JObject>(query);
		results.Single()["result"].Should().BeNull();
	}

	[Fact]
	public async Task SetIntersect_ChainedThreeSets()
	{
		await SeedItems();
		// Item 1: ["a","b","c"], Item 2: ["b","c","d"]
		// SetIntersect(["a","b","c"], ["b","c","d"]) = ["b","c"]
		// SetIntersect(["b","c"], ["c"]) = ["c"]
		var query = new QueryDefinition(
			"SELECT VALUE SetIntersect(SetIntersect(['a','b','c'], ['b','c','d']), ['c']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<JArray>(query);
		results.Single().Should().HaveCount(1);
		results.Single()[0]!.Value<string>().Should().Be("c");
	}

	[Fact]
	public async Task SetIntersect_WithParameterBothArgs_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE SetIntersect(@a, @b) FROM c WHERE c.id = '1'")
			.WithParameter("@a", new JArray("a", "b", "c"))
			.WithParameter("@b", new JArray("b", "c", "d"));
		var results = await QueryAll<JArray>(query);
		results.Single().Select(t => t.Value<string>()).Should().BeEquivalentTo(new[] { "b", "c" });
	}

	#endregion

	#region SetUnion — Additional Edge Cases

	[Fact]
	public async Task SetUnion_WithNullProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "nullsu", partitionKey = "pk1", tags = (object?)null }),
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT SetUnion(c.tags, ['a']) AS result FROM c WHERE c.id = 'nullsu'");
		var results = await QueryAll<JObject>(query);
		results.Single()["result"].Should().BeNull();
	}

	[Fact]
	public async Task SetUnion_ChainedThreeSets()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT VALUE SetUnion(SetUnion(['a'], ['b']), ['c']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<JArray>(query);
		results.Single().Select(t => t.Value<string>()).Should().BeEquivalentTo(new[] { "a", "b", "c" });
	}

	[Fact]
	public async Task SetUnion_CaseSensitiveStrings()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT VALUE SetUnion(['ABC'], ['abc']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<JArray>(query);
		results.Single().Should().HaveCount(2);
	}

	[Fact]
	public async Task SetUnion_ResultInWhere_FiltersByLength()
	{
		await SeedTypeDiverseItems();
		// Item 11: tags=["a","b"], otherTags=["b","c"] → union=["a","b","c"] (3 elements)
		var query = new QueryDefinition(
			"SELECT VALUE c.id FROM c WHERE ARRAY_LENGTH(SetUnion(c.tags, c.otherTags)) >= 3");
		var results = await QueryAll<string>(query);
		results.Should().Contain("11");
	}

	[Fact]
	public async Task SetUnion_WithParameterBothArgs_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE SetUnion(@a, @b) FROM c WHERE c.id = '1'")
			.WithParameter("@a", new JArray("a"))
			.WithParameter("@b", new JArray("b"));
		var results = await QueryAll<JArray>(query);
		results.Single().Select(t => t.Value<string>()).Should().BeEquivalentTo(new[] { "a", "b" });
	}

	#endregion

	#region SetDifference — Additional Edge Cases

	[Fact]
	public async Task SetDifference_WithNullProperty_ReturnsUndefined()
	{
		await SeedTypeDiverseItems();
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "nullsd", partitionKey = "pk1", tags = (object?)null }),
			new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT SetDifference(c.tags, ['a']) AS result FROM c WHERE c.id = 'nullsd'");
		var results = await QueryAll<JObject>(query);
		results.Single()["result"].Should().BeNull();
	}

	[Fact]
	public async Task SetDifference_ChainedThreeSets()
	{
		await SeedItems();
		// ["a","b","c"] \ ["b"] = ["a","c"], then ["a","c"] \ ["c"] = ["a"]
		var query = new QueryDefinition(
			"SELECT VALUE SetDifference(SetDifference(['a','b','c'], ['b']), ['c']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<JArray>(query);
		results.Single().Should().HaveCount(1);
		results.Single()[0]!.Value<string>().Should().Be("a");
	}

	[Fact]
	public async Task SetDifference_CaseSensitiveStrings()
	{
		await SeedItems();
		// ["ABC","abc"] \ ["abc"] → ["ABC"]
		var query = new QueryDefinition(
			"SELECT VALUE SetDifference(['ABC','abc'], ['abc']) FROM c WHERE c.id = '1'");
		var results = await QueryAll<JArray>(query);
		results.Single().Should().HaveCount(1);
		results.Single()[0]!.Value<string>().Should().Be("ABC");
	}

	[Fact]
	public async Task SetDifference_WithParameterBothArgs_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE SetDifference(@a, @b) FROM c WHERE c.id = '1'")
			.WithParameter("@a", new JArray("a", "b", "c"))
			.WithParameter("@b", new JArray("b"));
		var results = await QueryAll<JArray>(query);
		results.Single().Select(t => t.Value<string>()).Should().BeEquivalentTo(new[] { "a", "c" });
	}

	[Fact]
	public async Task SetDifference_ResultInSelectProjection()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT SetDifference(c.tags, ['a']) AS diff FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Single()["diff"]!.ToObject<string[]>().Should().BeEquivalentTo(new[] { "b", "c" });
	}

	#endregion

	#region Cross-Cutting Composition

	[Fact]
	public async Task ArrayContainsAny_OnSetIntersectResult_Works()
	{
		await SeedTypeDiverseItems();
		// Item 11: tags=["a","b"], otherTags=["b","c"] → intersect=["b"]
		var query = new QueryDefinition(
			"SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS_ANY(SetIntersect(c.tags, c.otherTags), ['b'])");
		var results = await QueryAll<string>(query);
		results.Should().Contain("11");
	}

	[Fact]
	public async Task ArrayContainsAll_OnSetUnionResult_Works()
	{
		await SeedTypeDiverseItems();
		// Item 11: tags=["a","b"], otherTags=["b","c"] → union=["a","b","c"]
		var query = new QueryDefinition(
			"SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS_ALL(SetUnion(c.tags, c.otherTags), ['a','b','c'])");
		var results = await QueryAll<string>(query);
		results.Should().Contain("11");
	}

	[Fact]
	public async Task SetDifference_InArrayLength_InOrderBy()
	{
		await SeedItems();
		// Item 1: ["a","b","c"]\["a"] = ["b","c"] (len=2), Item 2: ["b","c","d"]\["a"] = ["b","c","d"] (len=3)
		var query = new QueryDefinition(
			"SELECT c.id FROM c WHERE ARRAY_LENGTH(c.tags) > 0 ORDER BY ARRAY_LENGTH(SetDifference(c.tags, ['a']))");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCountGreaterThanOrEqualTo(2);
	}

	[Fact]
	public async Task AllFiveFunctions_InSingleQuery_Works()
	{
		await SeedTypeDiverseItems();
		var query = new QueryDefinition(@"
            SELECT
                ARRAY_CONTAINS_ANY(c.tags, ['a']) AS hasAny,
                ARRAY_CONTAINS_ALL(c.tags, ['a','b']) AS hasAll,
                SetIntersect(c.tags, ['a','b']) AS inter,
                SetUnion(c.tags, ['z']) AS unioned,
                SetDifference(c.tags, ['a']) AS diff
            FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		var r = results.Single();
		r["hasAny"]!.Value<bool>().Should().BeTrue();
		r["hasAll"]!.Value<bool>().Should().BeTrue();
		r["inter"]!.ToObject<string[]>().Should().BeEquivalentTo(new[] { "a", "b" });
		r["diff"]!.ToObject<string[]>().Should().BeEquivalentTo(new[] { "b", "c" });
	}

	[Fact]
	public async Task ExtendedArrayFunctions_WithDistinct_Works()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT DISTINCT VALUE ARRAY_CONTAINS_ANY(c.tags, ['a']) FROM c");
		var results = await QueryAll<bool>(query);
		results.Should().Contain(true);
		results.Should().Contain(false);
	}

	[Fact]
	public async Task ExtendedArrayFunctions_WithTop_Works()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT TOP 2 VALUE c.id FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['b'])");
		var results = await QueryAll<string>(query);
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task ExtendedArrayFunctions_WithOffsetLimit_Works()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS_ANY(c.tags, ['b']) ORDER BY c.id OFFSET 0 LIMIT 1");
		var results = await QueryAll<string>(query);
		results.Should().HaveCount(1);
	}

	#endregion

	#region Null vs Undefined Semantics

	[Fact]
	public async Task SetIntersect_ExplicitNullVsMissing_BothUndefined()
	{
		await SeedTypeDiverseItems();
		// Item 8 has no tags property (missing), add item with explicit null
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "nullcheck", partitionKey = "pk1", tags = (object?)null }),
			new PartitionKey("pk1"));

		var queryMissing = new QueryDefinition("SELECT SetIntersect(c.tags, ['a']) AS r FROM c WHERE c.id = '8'");
		var queryNull = new QueryDefinition("SELECT SetIntersect(c.tags, ['a']) AS r FROM c WHERE c.id = 'nullcheck'");

		var resMissing = await QueryAll<JObject>(queryMissing);
		var resNull = await QueryAll<JObject>(queryNull);

		resMissing.Single()["r"].Should().BeNull(); // undefined → excluded from JSON
		resNull.Single()["r"].Should().BeNull();
	}

	[Fact]
	public async Task SetUnion_ExplicitNullVsMissing_BothUndefined()
	{
		await SeedTypeDiverseItems();
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "nullcheck2", partitionKey = "pk1", tags = (object?)null }),
			new PartitionKey("pk1"));

		var queryMissing = new QueryDefinition("SELECT SetUnion(c.tags, ['a']) AS r FROM c WHERE c.id = '8'");
		var queryNull = new QueryDefinition("SELECT SetUnion(c.tags, ['a']) AS r FROM c WHERE c.id = 'nullcheck2'");

		var resMissing = await QueryAll<JObject>(queryMissing);
		var resNull = await QueryAll<JObject>(queryNull);

		resMissing.Single()["r"].Should().BeNull();
		resNull.Single()["r"].Should().BeNull();
	}

	#endregion

	private async Task<List<T>> QueryAll<T>(QueryDefinition query)
	{
		var iterator = _container.GetItemQueryIterator<T>(query);
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}
		return results;
	}
}
