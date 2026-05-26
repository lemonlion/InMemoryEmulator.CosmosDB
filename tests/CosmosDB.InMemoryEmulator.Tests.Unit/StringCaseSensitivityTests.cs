using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests verifying that SQL string comparisons are case-sensitive,
/// matching real Azure Cosmos DB behaviour.
/// Real Cosmos DB uses ordinal (case-sensitive) string comparison
/// for =, !=, &lt;, &gt;, &lt;=, &gt;=, IN, NOT IN, and LIKE.
/// </summary>
public class StringCaseSensitivityTests
{
	private readonly InMemoryContainer _container = new("case-test", "/pk");

	private async Task SeedAsync()
	{
		await CreateItem("""{"id":"1","pk":"a","name":"Alice"}""");
		await CreateItem("""{"id":"2","pk":"a","name":"alice"}""");
		await CreateItem("""{"id":"3","pk":"a","name":"BOB"}""");
		await CreateItem("""{"id":"4","pk":"a","name":"bob"}""");
	}

	private async Task CreateItem(string json)
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("a"));
	}

	// ═══════════════════════════════════════════════════════════════════
	//  C1: Equality (=, !=, IN) must be case-sensitive
	// ═══════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Query_WhereEquals_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name = 'Alice'");
		var page = await iter.ReadNextAsync();

		// Only "Alice" (id=1), NOT "alice" (id=2)
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Query_WhereNotEquals_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name != 'Alice'");
		var page = await iter.ReadNextAsync();

		// "alice", "BOB", "bob" — but NOT "Alice"
		page.Should().HaveCount(3);
		page.Select(d => d["id"]!.Value<string>()).Should().NotContain("1");
	}

	[Fact]
	public async Task Query_WhereIn_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name IN ('alice', 'bob')");
		var page = await iter.ReadNextAsync();

		// Only lowercase matches: id=2 and id=4
		page.Should().HaveCount(2);
		page.Select(d => d["id"]!.Value<string>()).Should().BeEquivalentTo(["2", "4"]);
	}

	// ═══════════════════════════════════════════════════════════════════
	//  C2: Comparison (<, >, <=, >=) and ORDER BY must be case-sensitive
	// ═══════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Query_OrderBy_String_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT c.name FROM c ORDER BY c.name ASC");
		var page = await iter.ReadNextAsync();

		var names = page.Select(d => d["name"]!.Value<string>()).ToList();

		// Ordinal sort: uppercase letters (A=65, B=66) sort before lowercase (a=97, b=98)
		// Expected: "Alice", "BOB", "alice", "bob"
		names.Should().BeEquivalentTo(
			new[] { "Alice", "BOB", "alice", "bob" },
			opts => opts.WithStrictOrdering());
	}

	[Fact]
	public async Task Query_WhereGreaterThan_String_IsCaseSensitive()
	{
		await SeedAsync();

		// In ordinal comparison, 'a' (97) > 'Z' (90), so "alice" > "BOB"
		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name > 'Z'");
		var page = await iter.ReadNextAsync();

		// "alice" and "bob" are > "Z" in ordinal; "Alice" and "BOB" are < "Z"
		page.Should().HaveCount(2);
		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["alice", "bob"]);
	}

	// ═══════════════════════════════════════════════════════════════════
	//  C3: LIKE must be case-sensitive
	// ═══════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Query_Like_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name LIKE 'A%'");
		var page = await iter.ReadNextAsync();

		// Only "Alice" starts with uppercase A, not "alice"
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Query_Like_LowercasePattern_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name LIKE 'b%'");
		var page = await iter.ReadNextAsync();

		// Only "bob" starts with lowercase b, not "BOB"
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("4");
	}

	// ═══════════════════════════════════════════════════════════════════
	//  C4: LIKE without ESCAPE must treat regex metacharacters as literals
	// ═══════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Query_Like_WithSquareBrackets_TreatedAsLiteral()
	{
		var container = new InMemoryContainer("like-test", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","code":"test[1]"}""")), new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","code":"testX"}""")), new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.code LIKE 'test[1]'");
		var page = await iter.ReadNextAsync();

		// Should match only "test[1]" literally, not treat [1] as regex char class
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Query_Like_WithDot_TreatedAsLiteral()
	{
		var container = new InMemoryContainer("like-test2", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","code":"foo.bar"}""")), new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","code":"fooXbar"}""")), new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.code LIKE 'foo.bar'");
		var page = await iter.ReadNextAsync();

		// Should match only "foo.bar" literally, not treat . as regex any-char
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  C5: Remaining Comparison Operators
// ═══════════════════════════════════════════════════════════════════════════

public class StringComparisonOperatorTests
{
	private readonly InMemoryContainer _container = new("case-cmp", "/pk");

	private async Task SeedAsync()
	{
		foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
		{
			await _container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes(
					$$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
				new PartitionKey("a"));
		}
	}

	[Fact]
	public async Task Query_WhereLessThan_String_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name < 'a'");
		var page = await iter.ReadNextAsync();

		// Ordinal: uppercase chars (A=65, B=66) < 'a' (97)
		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "BOB"]);
	}

	[Fact]
	public async Task Query_WhereLessThanOrEqual_String_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name <= 'BOB'");
		var page = await iter.ReadNextAsync();

		// Ordinal: "Alice" <= "BOB" (A<B), "BOB" <= "BOB" (equal)
		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "BOB"]);
	}

	[Fact]
	public async Task Query_WhereGreaterThanOrEqual_String_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name >= 'alice'");
		var page = await iter.ReadNextAsync();

		// Ordinal: "alice" >= "alice" (equal), "bob" >= "alice"
		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["alice", "bob"]);
	}

	[Fact]
	public async Task Query_NotIn_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name NOT IN ('Alice', 'BOB')");
		var page = await iter.ReadNextAsync();

		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["alice", "bob"]);
	}

	[Fact]
	public async Task Query_Between_String_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name BETWEEN 'A' AND 'Z'");
		var page = await iter.ReadNextAsync();

		// Ordinal range [A, Z]: "Alice" and "BOB" are in range; "alice" > "Z", "bob" > "Z"
		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "BOB"]);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  C6: ORDER BY DESC
// ═══════════════════════════════════════════════════════════════════════════

public class StringOrderByDescTests
{
	[Fact]
	public async Task Query_OrderByDesc_String_IsCaseSensitive()
	{
		var container = new InMemoryContainer("case-ord", "/pk");
		foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
		{
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes(
					$$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
				new PartitionKey("a"));
		}

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT c.name FROM c ORDER BY c.name DESC");
		var page = await iter.ReadNextAsync();

		var names = page.Select(d => d["name"]!.Value<string>()).ToList();
		// Reverse ordinal: "bob", "alice", "BOB", "Alice"
		names.Should().BeEquivalentTo(
			new[] { "bob", "alice", "BOB", "Alice" },
			opts => opts.WithStrictOrdering());
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  C7: LIKE Extended Coverage
// ═══════════════════════════════════════════════════════════════════════════

public class StringLikeExtendedTests
{
	private readonly InMemoryContainer _container = new("case-like", "/pk");

	private async Task SeedAsync()
	{
		foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
		{
			await _container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes(
					$$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
				new PartitionKey("a"));
		}
	}

	[Fact]
	public async Task Query_Like_ExactMatch_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name LIKE 'Alice'");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(1);
		page.First()["name"]!.Value<string>().Should().Be("Alice");
	}

	[Fact]
	public async Task Query_Like_UnderscoreWildcard_IsCaseSensitive()
	{
		await SeedAsync();

		// _lice matches any single char + "lice" — both "Alice" and "alice" match
		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name LIKE '_lice'");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(2);
		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
	}

	[Fact]
	public async Task Query_NotLike_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name NOT LIKE 'A%'");
		var page = await iter.ReadNextAsync();

		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["alice", "BOB", "bob"]);
	}

	[Fact]
	public async Task Query_Like_MiddlePercent_IsCaseSensitive()
	{
		await SeedAsync();

		// A%e matches strings starting with 'A' and ending with 'e'
		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name LIKE 'A%e'");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(1);
		page.First()["name"]!.Value<string>().Should().Be("Alice");
	}

	[Fact]
	public async Task Query_Like_SubstringPercent_IsCaseSensitive()
	{
		await SeedAsync();

		// %li% matches any string containing "li"
		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name LIKE '%li%'");
		var page = await iter.ReadNextAsync();

		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
	}

	[Fact]
	public async Task Query_Like_EmptyPattern_MatchesOnlyEmpty()
	{
		var container = new InMemoryContainer("empty-like", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":""}""")),
			new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"notempty"}""")),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name LIKE ''");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  C8: String Functions Case Sensitivity
// ═══════════════════════════════════════════════════════════════════════════

public class StringFunctionCaseSensitivityTests
{
	private readonly InMemoryContainer _container = new("case-func", "/pk");

	private async Task SeedAsync()
	{
		foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
		{
			await _container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes(
					$$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
				new PartitionKey("a"));
		}
	}

	[Fact]
	public async Task Query_Contains_Default_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE CONTAINS(c.name, 'ali')");
		var page = await iter.ReadNextAsync();

		// "ali" is in "alice" (id=2) but not "Alice" (starts with 'A')
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("2");
	}

	[Fact]
	public async Task Query_Contains_ThirdArgTrue_IsCaseInsensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE CONTAINS(c.name, 'ALI', true)");
		var page = await iter.ReadNextAsync();

		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
	}

	[Fact]
	public async Task Query_StartsWith_Default_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE STARTSWITH(c.name, 'a')");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(1);
		page.First()["name"]!.Value<string>().Should().Be("alice");
	}

	[Fact]
	public async Task Query_StartsWith_ThirdArgTrue_IsCaseInsensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE STARTSWITH(c.name, 'a', true)");
		var page = await iter.ReadNextAsync();

		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
	}

	[Fact]
	public async Task Query_EndsWith_Default_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE ENDSWITH(c.name, 'CE')");
		var page = await iter.ReadNextAsync();

		// No name ends with uppercase "CE" — "Alice" ends with "ce", "alice" ends with "ce"
		page.Should().BeEmpty();
	}

	[Fact]
	public async Task Query_EndsWith_ThirdArgTrue_IsCaseInsensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE ENDSWITH(c.name, 'CE', true)");
		var page = await iter.ReadNextAsync();

		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
	}

	[Fact]
	public async Task Query_StringEquals_Default_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE StringEquals(c.name, 'alice')");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(1);
		page.First()["name"]!.Value<string>().Should().Be("alice");
	}

	[Fact]
	public async Task Query_StringEquals_ThirdArgTrue_IsCaseInsensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE StringEquals(c.name, 'alice', true)");
		var page = await iter.ReadNextAsync();

		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
	}

	[Fact]
	public async Task Query_IndexOf_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT c.name, INDEX_OF(c.name, 'a') AS idx FROM c ORDER BY c.id");
		var page = await iter.ReadNextAsync();

		var results = page.ToList();
		// "Alice" → INDEX_OF('a') = -1 (capital A, not lowercase a)
		results[0]["idx"]!.Value<int>().Should().Be(-1);
		// "alice" → INDEX_OF('a') = 0
		results[1]["idx"]!.Value<int>().Should().Be(0);
	}

	[Fact]
	public async Task Query_Replace_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT REPLACE(c.name, 'a', 'X') AS replaced FROM c ORDER BY c.id");
		var page = await iter.ReadNextAsync();

		var results = page.ToList();
		// "Alice" → no 'a' (has 'A') → "Alice"
		results[0]["replaced"]!.Value<string>().Should().Be("Alice");
		// "alice" → has 'a' at 0 → "Xlice"
		results[1]["replaced"]!.Value<string>().Should().Be("Xlice");
	}

	[Fact]
	public async Task Query_RegexMatch_Default_IsCaseSensitive()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE RegexMatch(c.name, '^a')");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(1);
		page.First()["name"]!.Value<string>().Should().Be("alice");
	}

	[Fact]
	public async Task Query_RegexMatch_WithIgnoreCaseModifier()
	{
		await SeedAsync();

		var iter = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE RegexMatch(c.name, '^a', 'i')");
		var page = await iter.ReadNextAsync();

		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  C9: Case Transformations in Comparisons
// ═══════════════════════════════════════════════════════════════════════════

public class CaseTransformComparisonTests
{
	[Fact]
	public async Task Query_Lower_EnablesCaseInsensitiveEquality()
	{
		var container = new InMemoryContainer("lower-test", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Alice"}""")),
			new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"alice"}""")),
			new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"a","name":"BOB"}""")),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE LOWER(c.name) = 'alice'");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(2);
		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["Alice", "alice"]);
	}

	[Fact]
	public async Task Query_Upper_EnablesCaseInsensitiveEquality()
	{
		var container = new InMemoryContainer("upper-test", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"BOB"}""")),
			new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"bob"}""")),
			new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"3","pk":"a","name":"Alice"}""")),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE UPPER(c.name) = 'BOB'");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(2);
		page.Select(d => d["name"]!.Value<string>()).Should().BeEquivalentTo(["BOB", "bob"]);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  C10: DISTINCT, GROUP BY, MIN/MAX
// ═══════════════════════════════════════════════════════════════════════════

public class StringDistinctGroupByTests
{
	private async Task<InMemoryContainer> CreateSeededContainerAsync()
	{
		var container = new InMemoryContainer("case-dg", "/pk");
		foreach (var (id, name) in new[] { ("1", "Alice"), ("2", "alice"), ("3", "BOB"), ("4", "bob") })
		{
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes(
					$$$"""{"id":"{{{id}}}","pk":"a","name":"{{{name}}}"}""")),
				new PartitionKey("a"));
		}
		return container;
	}

	[Fact]
	public async Task Query_Distinct_PreservesCaseDifferences()
	{
		var container = await CreateSeededContainerAsync();

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT DISTINCT c.name FROM c");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(4, "Alice, alice, BOB, bob are all distinct values");
	}

	[Fact]
	public async Task Query_GroupBy_TreatsCaseDifferencesAsSeparateGroups()
	{
		var container = await CreateSeededContainerAsync();

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name");
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(4, "Alice, alice, BOB, bob should be 4 separate groups");
	}

	[Fact]
	public async Task Query_MinMax_String_UsesOrdinalComparison()
	{
		var container = await CreateSeededContainerAsync();

		var iterMin = container.GetItemQueryIterator<JValue>(
			"SELECT VALUE MIN(c.name) FROM c");
		var min = (await iterMin.ReadNextAsync()).First();

		var iterMax = container.GetItemQueryIterator<JValue>(
			"SELECT VALUE MAX(c.name) FROM c");
		var max = (await iterMax.ReadNextAsync()).First();

		// Ordinal: 'A' (65) < 'B' (66) < 'a' (97) < 'b' (98)
		min.Value<string>().Should().Be("Alice");
		max.Value<string>().Should().Be("bob");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  C11: Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class StringCaseEdgeCaseTests
{
	[Fact]
	public async Task Query_ParameterizedQuery_StringIsCaseSensitive()
	{
		var container = new InMemoryContainer("param-test", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Alice"}""")),
			new PartitionKey("a"));
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"2","pk":"a","name":"alice"}""")),
			new PartitionKey("a"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.name = @n")
			.WithParameter("@n", "Alice");
		var iter = container.GetItemQueryIterator<JObject>(query);
		var page = await iter.ReadNextAsync();

		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Query_PropertyNameLookup_IsCaseSensitive()
	{
		var container = new InMemoryContainer("propcase", "/pk");
		// JSON has property "name" (lowercase)
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","name":"Alice"}""")),
			new PartitionKey("a"));

		// Query with "Name" (capital N) — should not match the "name" property
		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.Name = 'Alice'");
		var page = await iter.ReadNextAsync();

		page.Should().BeEmpty("JSON property lookup is case-sensitive: 'Name' != 'name'");
	}

	[Fact]
	public async Task Query_UnicodeCase_IsOrdinal()
	{
		var container = new InMemoryContainer("unicode-case", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Über" }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", name = "über" }),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name = 'Über'");
		var page = await iter.ReadNextAsync();

		// Ordinal: 'Ü' (220) != 'ü' (252) → only exact match
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1: StringConcat (||) null handling bug fix
// ═══════════════════════════════════════════════════════════════════════════

public class StringConcatNullTests
{
	[Fact]
	public async Task Query_StringConcat_WithNull_ReturnsUndefined()
	{
		var container = new InMemoryContainer("concat-null", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "hello" }),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JToken>(
			"SELECT VALUE c.name || null FROM c");
		var page = await iter.ReadNextAsync();
		// null || string → undefined → omitted by SELECT VALUE
		page.Should().BeEmpty();
	}

	[Fact]
	public async Task Query_StringConcat_NullOnLeft_ReturnsUndefined()
	{
		var container = new InMemoryContainer("concat-null", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "world" }),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JToken>(
			"SELECT VALUE null || c.name FROM c");
		var page = await iter.ReadNextAsync();
		// null || string → undefined → omitted by SELECT VALUE
		page.Should().BeEmpty();
	}

	[Fact]
	public async Task Query_StringConcat_PreservesCase()
	{
		var container = new InMemoryContainer("concat-case", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JToken>(
			"SELECT VALUE c.name || '-suffix' FROM c");
		var page = await iter.ReadNextAsync();
		page.First().Value<string>().Should().Be("Alice-suffix");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2: LIKE extended coverage
// ═══════════════════════════════════════════════════════════════════════════

public class StringLikeExtendedDeepDiveTests
{
	[Fact]
	public async Task Query_Like_WithEscape_IsCaseSensitive()
	{
		var container = new InMemoryContainer("like-esc", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "A%" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "a%" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "ABC" }), new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name LIKE 'A!%' ESCAPE '!'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First()["name"]!.Value<string>().Should().Be("A%");
	}

	[Fact]
	public async Task Query_NotLike_WithEscape_IsCaseSensitive()
	{
		var container = new InMemoryContainer("like-esc", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "A%" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "a%" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "ABC" }), new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.name NOT LIKE 'A!%' ESCAPE '!'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(2);
	}

	[Theory]
	[InlineData("+")]
	[InlineData("*")]
	[InlineData("^")]
	[InlineData("$")]
	[InlineData("(")]
	[InlineData(")")]
	[InlineData("{")]
	[InlineData("}")]
	[InlineData("?")]
	public async Task Query_Like_AdditionalRegexMetachars_TreatedAsLiteral(string metaChar)
	{
		var container = new InMemoryContainer("like-meta", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = $"a{metaChar}b" }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", name = "axb" }),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>(
			$"SELECT * FROM c WHERE c.name LIKE 'a{metaChar}b'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Query_Like_OnNullField_DoesNotMatch()
	{
		var container = new InMemoryContainer("like-null", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'A%'");
		var page = await iter.ReadNextAsync();
		page.Should().BeEmpty();
	}

	[Fact]
	public async Task Query_Like_PercentOnly_MatchesAllNonNull()
	{
		var container = new InMemoryContainer("like-pct", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a" }), new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE '%'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(2); // id 1 and 2 (not 3 — null)
	}

	[Fact]
	public async Task Query_Like_UnderscoreOnly_MatchesSingleCharStrings()
	{
		var container = new InMemoryContainer("like-under", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", code = "A" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", code = "AB" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", code = "" }), new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.code LIKE '_'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First()["code"]!.Value<string>().Should().Be("A");
	}

	[Fact]
	public async Task Query_Like_UnderscoreWithNewline_MatchesNewline()
	{
		var container = new InMemoryContainer("like-newline", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "a\nb" }),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'a_b'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
	}

	[Fact]
	public async Task Query_Like_BackslashInData_TreatedAsLiteral()
	{
		var container = new InMemoryContainer("like-bs", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = @"a\b" }),
			new PartitionKey("a"));

		var iter = container.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'a\\b'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3: String functions — explicit false, null, empty
// ═══════════════════════════════════════════════════════════════════════════

public class StringFunctionEdgeCaseDeepDiveTests
{
	private async Task<InMemoryContainer> SeedAsync()
	{
		var c = new InMemoryContainer("func-edge", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Bob" }), new PartitionKey("a"));
		return c;
	}

	[Fact]
	public async Task Query_Contains_ExplicitFalse_SameAsDefault()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE CONTAINS(c.name, 'ali', false)");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First()["name"]!.Value<string>().Should().Be("alice");
	}

	[Fact]
	public async Task Query_StartsWith_ExplicitFalse_SameAsDefault()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE STARTSWITH(c.name, 'a', false)");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First()["name"]!.Value<string>().Should().Be("alice");
	}

	[Fact]
	public async Task Query_EndsWith_ExplicitFalse_SameAsDefault()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE ENDSWITH(c.name, 'ce', false)");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(2); // Both "Alice" and "alice" end with "ce" case-sensitively
	}

	[Fact]
	public async Task Query_StringEquals_ExplicitFalse_SameAsDefault()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'alice', false)");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
	}

	[Fact]
	public async Task Query_Contains_NullInput_ReturnsNoMatch()
	{
		var c = new InMemoryContainer("null-fn", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE CONTAINS(c.name, 'x')");
		var page = await iter.ReadNextAsync();
		page.Should().BeEmpty();
	}

	[Fact]
	public async Task Query_StartsWith_NullInput_ReturnsNoMatch()
	{
		var c = new InMemoryContainer("null-fn", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE STARTSWITH(c.name, 'x')");
		var page = await iter.ReadNextAsync();
		page.Should().BeEmpty();
	}

	[Fact]
	public async Task Query_EndsWith_NullInput_ReturnsNoMatch()
	{
		var c = new InMemoryContainer("null-fn", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE ENDSWITH(c.name, 'x')");
		var page = await iter.ReadNextAsync();
		page.Should().BeEmpty();
	}

	[Fact]
	public async Task Query_Contains_EmptySearchString_MatchesAll()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE CONTAINS(c.name, '')");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(3);
	}

	[Fact]
	public async Task Query_StartsWith_EmptyPrefix_MatchesAll()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE STARTSWITH(c.name, '')");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(3);
	}

	[Fact]
	public async Task Query_EndsWith_EmptySuffix_MatchesAll()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE ENDSWITH(c.name, '')");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(3);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 4: ARRAY_CONTAINS and CONCAT
// ═══════════════════════════════════════════════════════════════════════════

public class StringArrayConcatTests
{
	[Fact]
	public async Task Query_ArrayContains_StringElement_IsCaseSensitive()
	{
		var c = new InMemoryContainer("arr-case", "/pk");
		await c.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", tags = new[] { "Alice" } }),
			new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'alice')");
		var page = await iter.ReadNextAsync();
		page.Should().BeEmpty(); // case-sensitive: "Alice" ≠ "alice"
	}

	[Fact]
	public async Task Query_ConcatFunction_NullArg_ReturnsUndefined()
	{
		var c = new InMemoryContainer("concat-fn", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JToken>(
			"SELECT VALUE CONCAT('a', null, 'b') FROM c");
		var page = await iter.ReadNextAsync();
		// Cosmos DB: CONCAT with null arg returns undefined
		page.Should().BeEmpty();
	}

	[Fact]
	public async Task Query_ConcatFunction_PreservesCase()
	{
		var c = new InMemoryContainer("concat-fn", "/pk");
		await c.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", first = "Alice", last = "BOB" }),
			new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JToken>(
			"SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c");
		var page = await iter.ReadNextAsync();
		page.First().Value<string>().Should().Be("Alice BOB");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 5: Null and empty value edge cases
// ═══════════════════════════════════════════════════════════════════════════

public class StringNullEmptyEdgeCaseTests
{
	[Fact]
	public async Task Query_EmptyStringEquality_Works()
	{
		var c = new InMemoryContainer("empty-eq", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "Bob" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name = ''");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Query_Between_WithNullValue_ExcludesNull()
	{
		var c = new InMemoryContainer("between-null", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Bob" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a")); // name is null/undefined

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name BETWEEN 'A' AND 'Z'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First()["id"]!.Value<string>().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 6: Composed operations
// ═══════════════════════════════════════════════════════════════════════════

public class StringComposedOperationTests
{
	private async Task<InMemoryContainer> SeedAsync()
	{
		var c = new InMemoryContainer("composed-test", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Bob" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "4", pk = "a", name = "bob" }), new PartitionKey("a"));
		return c;
	}

	[Fact]
	public async Task Query_DistinctLower_CollapsesCaseDifferences()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JToken>(
			"SELECT DISTINCT VALUE LOWER(c.name) FROM c");
		var page = await iter.ReadNextAsync();
		page.Select(t => t.Value<string>()).Should().BeEquivalentTo("alice", "bob");
	}

	[Fact]
	public async Task Query_GroupByLower_MergesCaseVariants()
	{
		var c = await SeedAsync();
		var iter = c.GetItemQueryIterator<JObject>(
			"SELECT LOWER(c.name) AS lname, COUNT(1) AS cnt FROM c GROUP BY LOWER(c.name)");
		var page = await iter.ReadNextAsync();
		var results = page.ToList();
		results.Should().HaveCount(2);
		results.Should().Contain(r => r["lname"]!.Value<string>() == "alice" && r["cnt"]!.Value<int>() == 2);
		results.Should().Contain(r => r["lname"]!.Value<string>() == "bob" && r["cnt"]!.Value<int>() == 2);
	}

	[Fact]
	public async Task Query_RegexMatch_CombinedIgnoreCaseMultiline()
	{
		var c = new InMemoryContainer("regex-combined", "/pk");
		await c.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "hello\nAlice" }),
			new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE RegexMatch(c.text, '^alice', 'im')");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1); // "Alice" at start of second line, case-insensitive
	}

	[Fact]
	public async Task Query_MultipleOrderBy_StringTiebreaking()
	{
		var c = new InMemoryContainer("multi-order", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", last = "Smith", first = "alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", last = "Smith", first = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", last = "Brown", first = "Bob" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c ORDER BY c.last ASC, c.first ASC");
		var page = await iter.ReadNextAsync();
		var ids = page.Select(d => d["id"]!.Value<string>()).ToList();
		ids[0].Should().Be("3"); // Brown
								 // Smith, Alice vs Smith, alice — ordinal: 'A'(65) < 'a'(97)
		ids[1].Should().Be("2"); // Alice
		ids[2].Should().Be("1"); // alice
	}

	[Fact]
	public async Task Query_CrossPartition_CaseSensitivity_Consistent()
	{
		var c = new InMemoryContainer("cross-pk", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "b", name = "alice" }), new PartitionKey("b"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name = 'Alice'");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(1);
		page.First()["pk"]!.Value<string>().Should().Be("a");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 7: INDEX_OF null input
// ═══════════════════════════════════════════════════════════════════════════

public class StringIndexOfEdgeCaseTests
{
	[Fact]
	public async Task Query_IndexOf_NullInput_ReturnsUndefined()
	{
		var c = new InMemoryContainer("indexof-null", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>(
			"SELECT INDEX_OF(c.name, 'a') AS idx FROM c");
		var page = await iter.ReadNextAsync();
		page.First()["idx"].Should().BeNull("undefined field property is absent from JSON");
	}

	[Fact]
	public async Task Query_IndexOf_WithStartPosition_IsCaseSensitive()
	{
		var c = new InMemoryContainer("indexof-start", "/pk");
		await c.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "hello hello" }),
			new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>(
			"SELECT INDEX_OF(c.name, 'hello', 1) AS idx FROM c");
		var page = await iter.ReadNextAsync();
		page.First()["idx"]!.Value<int>().Should().Be(6);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Plan 41: String Case Sensitivity Deep Dive Tests
// ═══════════════════════════════════════════════════════════════════════════

// ── A1: REGEX_MATCH null → undefined ──
public class RegexMatchNullUndefinedTests
{
	private readonly InMemoryContainer _container = new("regexnull", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "hello" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task RegexMatch_NullFirstArg_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE RegexMatch(null, 'abc') FROM c");
		results.Should().BeEmpty("null input returns undefined, omitted from SELECT VALUE");
	}

	[Fact]
	public async Task RegexMatch_NullSecondArg_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE RegexMatch('abc', null) FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task RegexMatch_UndefinedField_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE RegexMatch(c.missing, 'abc') FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task RegexMatch_NullInput_NotInWhere()
	{
		var results = await Query<JObject>("SELECT * FROM c WHERE RegexMatch(null, 'x')");
		results.Should().BeEmpty("undefined is falsy in WHERE");
	}
}

// ── A2: INDEX_OF null → undefined ──
public class IndexOfNullUndefinedTests
{
	private readonly InMemoryContainer _container = new("idxnull", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "hello" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task IndexOf_NullFirstArg_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE INDEX_OF(null, 'a') FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task IndexOf_NullSecondArg_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE INDEX_OF('abc', null) FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task IndexOf_UndefinedField_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE INDEX_OF(c.missing, 'a') FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task IndexOf_NullInSelect_ShowsAsNoProperty()
	{
		var results = await Query<JObject>("SELECT INDEX_OF(null, 'a') AS idx FROM c");
		results.Should().ContainSingle();
		results[0]["idx"].Should().BeNull("undefined property is absent from JSON");
	}
}

// ── A3: INDEX_OF bounds checking ──
public class IndexOfBoundsTests
{
	private readonly InMemoryContainer _container = new("idxbounds", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task IndexOf_NegativeStartPos_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE INDEX_OF('hello', 'e', -1) FROM c");
		results.Should().BeEmpty("negative start position returns undefined");
	}

	[Fact]
	public async Task IndexOf_StartPosBeyondLength_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE INDEX_OF('hello', 'e', 100) FROM c");
		results.Should().BeEmpty("start position beyond string length returns undefined");
	}
}

// ── A4: LIKE on non-string types (divergent — skip + sister) ──
public class LikeNonStringTypeTests
{
	private readonly InMemoryContainer _container = new("likenonstr", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", age = 42 }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task Like_NumberLeftOperand_ReturnsUndefined()
	{
		var results = await Query<JObject>("SELECT * FROM c WHERE c.age LIKE '42'");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task Like_BooleanLeftOperand_ReturnsUndefined()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", flag = true }), new PartitionKey("a"));
		var results = await Query<JObject>("SELECT * FROM c WHERE c.flag LIKE 'True'");
		results.Should().BeEmpty();
	}
}

// ── B1: LIKE with pipe character ──
public class LikePipeCharTests
{
	[Fact]
	public async Task Like_WithPipeChar_TreatedAsLiteral()
	{
		var c = new InMemoryContainer("likepipe", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "a|b" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "a" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "b" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'a|b'");
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle().Which["id"]!.Value<string>().Should().Be("1");
	}
}

// ── B2: LIKE consecutive wildcards ──
public class LikeConsecutiveWildcardTests
{
	[Fact]
	public async Task Like_ConsecutivePercents_SameAsOnePercent()
	{
		var c = new InMemoryContainer("likecwild", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "anything" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE '%%'");
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle();
	}

	[Fact]
	public async Task Like_ConsecutiveUnderscores_MatchesExactLength()
	{
		var c = new InMemoryContainer("likeunder", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "ab" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "abc" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE '__'");
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle().Which["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Like_MixedWildcards_PercentThenUnderscore()
	{
		var c = new InMemoryContainer("likemixed", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "x" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "ab" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE '%_'");
		var page = await iter.ReadNextAsync();
		page.Count().Should().Be(2, "matches strings of length >= 1");
	}
}

// ── B3: LIKE ESCAPE case sensitivity ──
public class LikeEscapeCaseSensitivityTests
{
	[Fact]
	public async Task Like_EscapedPercent_CaseSensitive()
	{
		var c = new InMemoryContainer("likeesc", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "A%b" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "a%b" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'A!%b' ESCAPE '!'");
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle().Which["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Like_EscapedUnderscore_CaseSensitive()
	{
		var c = new InMemoryContainer("likeescus", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "A_b" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "a_b" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'A!_b' ESCAPE '!'");
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle().Which["id"]!.Value<string>().Should().Be("1");
	}
}

// ── B4: LIKE with Unicode ──
public class LikeUnicodeTests
{
	[Fact]
	public async Task Like_UnicodeInPattern_CaseSensitive()
	{
		var c = new InMemoryContainer("likeuni", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Über" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "über" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE 'Über%'");
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle().Which["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task Like_UnicodeWildcard_MatchesUnicode()
	{
		var c = new InMemoryContainer("likeuniwild", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Über" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name LIKE '_ber'");
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle();
	}
}

// ── B5: CONTAINS/STARTSWITH/ENDSWITH third arg ──
public class StringFunctionThirdArgTests
{
	private readonly InMemoryContainer _container = new("thirdarg", "/pk");

	private async Task<List<JObject>> Query(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<JObject>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task Contains_ThirdArgFalse_Boolean_StaysCaseSensitive()
	{
		var results = await Query("SELECT * FROM c WHERE CONTAINS(c.name, 'ALI', false)");
		results.Should().BeEmpty("false means case-sensitive, no match");
	}

	[Fact]
	public async Task Contains_ThirdArgNonBooleanString_StaysCaseSensitive()
	{
		var results = await Query("SELECT * FROM c WHERE CONTAINS(c.name, 'ALI', 'yes')");
		results.Should().BeEmpty("only 'true' activates case-insensitive");
	}
}

// ── B6: STRING_EQUALS with undefined/null ──
public class StringEqualsEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("streq", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task StringEquals_UndefinedField_ReturnsUndefined()
	{
		var results = await Query<JObject>("SELECT * FROM c WHERE StringEquals(c.missing, 'alice')");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task StringEquals_NullArg_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE StringEquals(null, 'alice') FROM c");
		results.Should().BeEmpty("null input returns undefined");
	}
}

// ── B7: REPLACE edge cases ──
public class ReplaceCaseSensitivityEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("replcase", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task Replace_CaseSensitiveSearch_NoMatch()
	{
		var results = await Query<string>("SELECT VALUE REPLACE('Alice', 'ALICE', 'Bob') FROM c");
		results.Should().ContainSingle().Which.Should().Be("Alice");
	}

	[Fact]
	public async Task Replace_UndefinedInput_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE REPLACE(c.missing, 'a', 'b') FROM c");
		results.Should().BeEmpty();
	}
}

// ── B8: ORDER BY mixed types ──
public class OrderByMixedTypeTests
{
	[Fact]
	public async Task OrderBy_StringWithNull_NullSortsFirst()
	{
		var c = new InMemoryContainer("ordmixed", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Bob" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = (string?)null }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Alice" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT c.name FROM c ORDER BY c.name ASC");
		var page = await iter.ReadNextAsync();
		var names = page.Select(x => x["name"]?.Value<string>()).ToList();
		names[0].Should().BeNull("null sorts before strings in ASC order");
	}
}

// ── B9: BETWEEN inclusive boundaries ──
public class BetweenInclusiveTests
{
	[Fact]
	public async Task Between_ExactLowerBound_IsInclusive()
	{
		var c = new InMemoryContainer("between", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "bob" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Charlie" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name BETWEEN 'Alice' AND 'bob'");
		var page = await iter.ReadNextAsync();
		page.Any(x => x["name"]!.Value<string>() == "Alice").Should().BeTrue("lower bound is inclusive");
	}

	[Fact]
	public async Task Between_ExactUpperBound_IsInclusive()
	{
		var c = new InMemoryContainer("betweenupp", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "bob" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name BETWEEN 'Alice' AND 'bob'");
		var page = await iter.ReadNextAsync();
		page.Any(x => x["name"]!.Value<string>() == "bob").Should().BeTrue("upper bound is inclusive");
	}
}

// ── B10: IN operator large list ──
public class InOperatorLargeListTests
{
	[Fact]
	public async Task In_LargeList_AllCaseSensitive()
	{
		var c = new InMemoryContainer("inlarge", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Bob" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>("SELECT * FROM c WHERE c.name IN ('Alice', 'Bob')");
		var page = await iter.ReadNextAsync();
		page.Count().Should().Be(2);
		page.Select(x => x["name"]!.Value<string>()).Should().BeEquivalentTo("Alice", "Bob");
	}
}

// ── B11: Empty string edge cases ──
public class EmptyStringFunctionTests
{
	private readonly InMemoryContainer _container = new("emptystr", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task StartsWith_EmptyStringInput_EmptyPrefix_ReturnsTrue()
	{
		var results = await Query<bool>("SELECT VALUE STARTSWITH('', '') FROM c");
		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task EndsWith_EmptyStringInput_EmptyPrefix_ReturnsTrue()
	{
		var results = await Query<bool>("SELECT VALUE ENDSWITH('', '') FROM c");
		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task Contains_EmptyStringInput_EmptySub_ReturnsTrue()
	{
		var results = await Query<bool>("SELECT VALUE CONTAINS('', '') FROM c");
		results.Should().ContainSingle().Which.Should().BeTrue();
	}
}

// ── B12: REGEX_MATCH invalid regex / empty pattern ──
public class RegexMatchEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("regexedge", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task RegexMatch_InvalidPattern_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE RegexMatch('test', '[invalid') FROM c");
		results.Should().BeEmpty("invalid regex returns undefined");
	}

	[Fact]
	public async Task RegexMatch_EmptyPattern_MatchesAll()
	{
		var results = await Query<bool>("SELECT VALUE RegexMatch('test', '') FROM c");
		results.Should().ContainSingle().Which.Should().BeTrue();
	}
}

// ── B13: String concat with undefined ──
public class StringConcatUndefinedTests
{
	private readonly InMemoryContainer _container = new("concatundef", "/pk");

	private async Task<List<T>> Query<T>(string sql)
	{
		await EnsureSeeded();
		var iter = _container.GetItemQueryIterator<T>(sql);
		var page = await iter.ReadNextAsync();
		return page.ToList();
	}

	private bool _seeded;
	private async Task EnsureSeeded()
	{
		if (_seeded) return;
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		_seeded = true;
	}

	[Fact]
	public async Task StringConcat_UndefinedField_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE c.missing || 'suffix' FROM c");
		results.Should().BeEmpty("concat with undefined field returns undefined");
	}

	[Fact]
	public async Task StringConcat_BothUndefined_ReturnsUndefined()
	{
		var results = await Query<JToken>("SELECT VALUE c.x || c.y FROM c");
		results.Should().BeEmpty();
	}
}

// ── B14: DISTINCT in subquery ──
public class DistinctSubqueryCaseSensitiveTests
{
	[Fact]
	public async Task Distinct_InSubquery_CaseSensitive()
	{
		var c = new InMemoryContainer("distsub", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Alice" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<string>("SELECT DISTINCT VALUE c.name FROM c");
		var page = await iter.ReadNextAsync();
		page.Should().HaveCount(2).And.Contain("Alice").And.Contain("alice");
	}
}

// ── B15: GROUP BY with HAVING case-sensitive ──
public class GroupByHavingCaseSensitiveTests
{
	[Fact]
	public async Task GroupBy_Having_CaseSensitiveFilter()
	{
		var c = new InMemoryContainer("grphaving", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "a", name = "Alice" }), new PartitionKey("a"));

		var iter = c.GetItemQueryIterator<JObject>(
			"SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name HAVING c.name = 'Alice'");
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle().Which["cnt"]!.Value<int>().Should().Be(2);
	}
}

// ── B16: LIKE with parameterized pattern ──
public class LikeParameterizedTests
{
	[Fact]
	public async Task Like_ParameterizedPattern_CaseSensitive()
	{
		var c = new InMemoryContainer("likeparam", "/pk");
		await c.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await c.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "alice" }), new PartitionKey("a"));

		var qd = new QueryDefinition("SELECT * FROM c WHERE c.name LIKE @pat").WithParameter("@pat", "A%");
		var iter = c.GetItemQueryIterator<JObject>(qd);
		var page = await iter.ReadNextAsync();
		page.Should().ContainSingle().Which["id"]!.Value<string>().Should().Be("1");
	}
}
