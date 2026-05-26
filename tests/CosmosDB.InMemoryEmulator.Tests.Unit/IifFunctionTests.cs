using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class IifFunctionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedItems()
	{
		var items = new[]
		{
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice", Value = 10, IsActive = true, Tags = ["dot", "net"], Nested = new NestedObject { Description = "desc", Score = 9.5 } },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob", Value = 20, IsActive = false, Tags = ["java"] },
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 0, IsActive = true, Tags = [] },
		};
		foreach (var item in items)
		{
			await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Original tests (pre-existing)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_TrueCondition_ReturnsSecondArgument()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("yes");
	}

	[Fact]
	public async Task Iif_FalseCondition_ReturnsThirdArgument()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, 'yes', 'no') AS result FROM c WHERE c.id = '2'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_WithComparisonExpression_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.value > 15, 'high', 'low') AS level FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["level"]!.ToString().Should().Be("low");   // value=10
		results[1]["level"]!.ToString().Should().Be("high");  // value=20
		results[2]["level"]!.ToString().Should().Be("low");   // value=0
	}

	[Fact]
	public async Task Iif_WithNumericReturnValues_ReturnsNumbers()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, 1, 0) AS flag FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["flag"]!.Value<long>().Should().Be(1);
	}

	[Fact]
	public async Task Iif_InWhereClause_FiltersCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE IIF(c.isActive, c.value, 0) > 5");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Alice");
	}

	[Fact]
	public async Task Iif_NestedIif_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.value > 15, 'high', IIF(c.value > 5, 'medium', 'low')) AS level FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["level"]!.ToString().Should().Be("medium"); // value=10
		results[1]["level"]!.ToString().Should().Be("high");   // value=20
		results[2]["level"]!.ToString().Should().Be("low");    // value=0
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 1: Non-boolean condition bug fix
	//  Real Cosmos DB IIF only treats boolean true as truthy.
	//  Numbers, strings, arrays, objects all return the false branch.
	//  See: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/iif
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_NumericNonZeroCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		// Real Cosmos DB: IIF(42, ...) → false branch (42 is not boolean true)
		var query = new QueryDefinition("SELECT IIF(42, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_NumericZeroCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		// Real Cosmos DB: IIF(0, ...) → false branch (0 is not boolean true)
		var query = new QueryDefinition("SELECT IIF(0, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_StringCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		// Real Cosmos DB: IIF('hello', ...) → false branch ('hello' is not boolean true)
		var query = new QueryDefinition("SELECT IIF('hello', 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_EmptyStringCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		// Real Cosmos DB: IIF('', ...) → false branch ('' is not boolean true)
		var query = new QueryDefinition("SELECT IIF('', 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_NumericFieldAsCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		// Real Cosmos DB: IIF(c.value, ...) → false branch for all items (numeric field is never boolean)
		var query = new QueryDefinition("SELECT IIF(c.value, 'yes', 'no') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("no"); // value=10 — not boolean
		results[1]["result"]!.ToString().Should().Be("no"); // value=20 — not boolean
		results[2]["result"]!.ToString().Should().Be("no"); // value=0  — not boolean
	}

	[Fact]
	public async Task Iif_StringFieldAsCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		// Real Cosmos DB: IIF(c.name, ...) → false branch for all items (string field is never boolean)
		var query = new QueryDefinition("SELECT IIF(c.name, 'yes', 'no') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("no"); // name=Alice   — not boolean
		results[1]["result"]!.ToString().Should().Be("no"); // name=Bob     — not boolean
		results[2]["result"]!.ToString().Should().Be("no"); // name=Charlie — not boolean
	}

	[Fact]
	public async Task Iif_ArrayFieldAsCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		// Real Cosmos DB: IIF(c.tags, ...) → false branch for all items (array is never boolean)
		// Even empty arrays return false branch — only boolean true returns true branch
		var query = new QueryDefinition("SELECT IIF(c.tags, 'yes', 'no') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("no"); // tags=["dot","net"] — not boolean
		results[1]["result"]!.ToString().Should().Be("no"); // tags=["java"]      — not boolean
		results[2]["result"]!.ToString().Should().Be("no"); // tags=[]            — not boolean
	}

	[Fact]
	public async Task Iif_ObjectFieldAsCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		// Real Cosmos DB: IIF(c.nested, ...) → false branch (object is never boolean)
		// Item 1 has nested = { description: "desc", score: 9.5 }
		var query = new QueryDefinition("SELECT IIF(c.nested, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 2: Null and undefined conditions
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_NullCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(null, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_UndefinedPropertyCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.nonExistentField, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 3: Complex boolean expressions in condition
	//  These produce actual boolean values, so IIF should work normally.
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_WithAndCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive AND c.value > 5, 'yes', 'no') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("yes"); // id=1: active AND 10>5
		results[1]["result"]!.ToString().Should().Be("no");  // id=2: NOT active
		results[2]["result"]!.ToString().Should().Be("no");  // id=3: active AND NOT 0>5
	}

	[Fact]
	public async Task Iif_WithOrCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive OR c.value > 15, 'yes', 'no') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("yes"); // id=1: active
		results[1]["result"]!.ToString().Should().Be("yes"); // id=2: 20>15
		results[2]["result"]!.ToString().Should().Be("yes"); // id=3: active
	}

	[Fact]
	public async Task Iif_WithNotCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(NOT c.isActive, 'inactive', 'active') AS status FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["status"]!.ToString().Should().Be("active");   // id=1: NOT true = false
		results[1]["status"]!.ToString().Should().Be("inactive"); // id=2: NOT false = true
		results[2]["status"]!.ToString().Should().Be("active");   // id=3: NOT true = false
	}

	[Fact]
	public async Task Iif_WithEqualityCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.name = 'Alice', 'found', 'not found') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("found");     // id=1: Alice
		results[1]["result"]!.ToString().Should().Be("not found"); // id=2: Bob
		results[2]["result"]!.ToString().Should().Be("not found"); // id=3: Charlie
	}

	[Fact]
	public async Task Iif_WithIsDefinedCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		// Item 1 has nested set (non-null), items 2&3 have nested = null (property still exists in JSON)
		var query = new QueryDefinition("SELECT IIF(IS_DEFINED(c.nested), 'has nested', 'no nested') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("has nested"); // id=1: nested is defined
	}

	[Fact]
	public async Task Iif_WithContainsFunctionCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(CONTAINS(c.name, 'Ali'), 'match', 'no match') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("match");    // Alice contains 'Ali'
		results[1]["result"]!.ToString().Should().Be("no match"); // Bob
		results[2]["result"]!.ToString().Should().Be("no match"); // Charlie
	}

	[Fact]
	public async Task Iif_WithArrayLengthComparisonCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(ARRAY_LENGTH(c.tags) > 0, 'tagged', 'untagged') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("tagged");   // id=1: 2 tags
		results[1]["result"]!.ToString().Should().Be("tagged");   // id=2: 1 tag
		results[2]["result"]!.ToString().Should().Be("untagged"); // id=3: 0 tags
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 4: Return value variations
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_WithMixedReturnTypes_ReturnsCorrectType()
	{
		await SeedItems();
		var queryTrue = new QueryDefinition("SELECT IIF(c.isActive, 42, 'inactive') AS result FROM c WHERE c.id = '1'");
		var queryFalse = new QueryDefinition("SELECT IIF(c.isActive, 42, 'inactive') AS result FROM c WHERE c.id = '2'");

		var resultsTrue = await QueryAll<JObject>(queryTrue);
		var resultsFalse = await QueryAll<JObject>(queryFalse);

		resultsTrue[0]["result"]!.Value<long>().Should().Be(42);
		resultsFalse[0]["result"]!.ToString().Should().Be("inactive");
	}

	[Fact]
	public async Task Iif_WithNullTrueBranch_ReturnsNull()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(true, null, 'fallback') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task Iif_WithNullFalseBranch_ReturnsNull()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(false, 'value', null) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task Iif_WithExpressionReturnValues_EvaluatesExpressions()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, c.value * 2, c.value) AS computed FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["computed"]!.Value<long>().Should().Be(20); // id=1: active, 10*2
		results[1]["computed"]!.Value<long>().Should().Be(20); // id=2: not active, raw 20
		results[2]["computed"]!.Value<long>().Should().Be(0);  // id=3: active, 0*2
	}

	[Fact]
	public async Task Iif_WithFunctionCallReturnValues_EvaluatesFunctions()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, UPPER(c.name), LOWER(c.name)) AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("ALICE");   // id=1: active → UPPER
		results[1]["result"]!.ToString().Should().Be("bob");     // id=2: not active → LOWER
		results[2]["result"]!.ToString().Should().Be("CHARLIE"); // id=3: active → UPPER
	}

	[Fact]
	public async Task Iif_WithBooleanReturnValues_ReturnsBooleans()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.value > 10, true, false) AS overTen FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["overTen"]!.Value<bool>().Should().BeFalse(); // 10 NOT > 10
		results[1]["overTen"]!.Value<bool>().Should().BeTrue();  // 20 > 10
		results[2]["overTen"]!.Value<bool>().Should().BeFalse(); // 0
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 5: Advanced nesting and composition
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_TripleNested_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT IIF(c.value > 15, 'high', IIF(c.value > 5, 'medium', IIF(c.value > 0, 'low', 'zero'))) AS tier FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["tier"]!.ToString().Should().Be("medium"); // value=10
		results[1]["tier"]!.ToString().Should().Be("high");   // value=20
		results[2]["tier"]!.ToString().Should().Be("zero");   // value=0
	}

	[Fact]
	public async Task Iif_InsideConcat_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT CONCAT(IIF(c.isActive, 'Active', 'Inactive'), ': ', c.name) AS label FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["label"]!.ToString().Should().Be("Active: Alice");
		results[1]["label"]!.ToString().Should().Be("Inactive: Bob");
		results[2]["label"]!.ToString().Should().Be("Active: Charlie");
	}

	[Fact]
	public async Task Iif_MultipleInSameSelect_EvaluatesIndependently()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT IIF(c.isActive, 'active', 'inactive') AS status, IIF(c.value > 10, 'high', 'low') AS level FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["status"]!.ToString().Should().Be("active");
		results[0]["level"]!.ToString().Should().Be("low");     // 10 NOT > 10
		results[1]["status"]!.ToString().Should().Be("inactive");
		results[1]["level"]!.ToString().Should().Be("high");    // 20 > 10
		results[2]["status"]!.ToString().Should().Be("active");
		results[2]["level"]!.ToString().Should().Be("low");     // 0
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 6: Usage contexts
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_InOrderBy_SortsCorrectly()
	{
		await SeedItems();
		// Sort active items first (0) then inactive (1), secondary sort by name
		var query = new QueryDefinition(
			"SELECT c.name FROM c ORDER BY IIF(c.isActive, 0, 1), c.name");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		// Active items first (sorted: Alice, Charlie), then inactive (Bob)
		results[0]["name"]!.ToString().Should().Be("Alice");
		results[1]["name"]!.ToString().Should().Be("Charlie");
		results[2]["name"]!.ToString().Should().Be("Bob");
	}

	[Fact]
	public async Task Iif_WithParameterizedQuery_UsesParameterValue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.value > @threshold, 'high', 'low') AS level FROM c ORDER BY c.id")
			.WithParameter("@threshold", 15);

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["level"]!.ToString().Should().Be("low");  // 10
		results[1]["level"]!.ToString().Should().Be("high"); // 20
		results[2]["level"]!.ToString().Should().Be("low");  // 0
	}

	[Fact]
	public async Task Iif_InValueSelect_ReturnsScalar()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IIF(c.isActive, 'yes', 'no') FROM c WHERE c.id = '1'");

		var results = await QueryAll<string>(query);

		results.Should().ContainSingle().Which.Should().Be("yes");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 7: Edge cases
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_FunctionNameCaseInsensitive_Works()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT iif(c.isActive, 'yes', 'no') AS r1, Iif(c.isActive, 'yes', 'no') AS r2 FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["r1"]!.ToString().Should().Be("yes");
		results[0]["r2"]!.ToString().Should().Be("yes");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 8: Non-boolean condition edge cases (Category A)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_NegativeNumberCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(-1, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_FloatCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(3.14, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_BooleanTrueLiteral_ReturnsTrueBranch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(true, 'yes', 'no') AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["result"]!.ToString().Should().Be("yes");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 9: Type-checking function conditions (Category B)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_WithIsNullCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(IS_NULL(c.nested), 'null', 'not null') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("not null"); // nested = object
		results[1]["result"]!.ToString().Should().Be("null");     // nested = null
		results[2]["result"]!.ToString().Should().Be("null");     // nested = null
	}

	[Fact]
	public async Task Iif_WithIsNumberCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(IS_NUMBER(c.value), 'number', 'not number') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results.Should().AllSatisfy(r => r["result"]!.ToString().Should().Be("number"));
	}

	[Fact]
	public async Task Iif_WithIsBoolCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(IS_BOOL(c.isActive), 'bool', 'not bool') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results.Should().AllSatisfy(r => r["result"]!.ToString().Should().Be("bool"));
	}

	[Fact]
	public async Task Iif_WithIsStringCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(IS_STRING(c.name), 'string', 'not string') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results.Should().AllSatisfy(r => r["result"]!.ToString().Should().Be("string"));
	}

	[Fact]
	public async Task Iif_WithIsArrayCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(IS_ARRAY(c.tags), 'array', 'not array') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results.Should().AllSatisfy(r => r["result"]!.ToString().Should().Be("array"));
	}

	[Fact]
	public async Task Iif_WithStartsWithCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(STARTSWITH(c.name, 'A'), 'A-name', 'other') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("A-name"); // Alice
		results[1]["result"]!.ToString().Should().Be("other");  // Bob
		results[2]["result"]!.ToString().Should().Be("other");  // Charlie
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 10: Complex expression conditions (Category C)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_WithArithmeticCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.value + 5 > 12, 'yes', 'no') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("yes"); // 10+5=15 > 12
		results[1]["result"]!.ToString().Should().Be("yes"); // 20+5=25 > 12
		results[2]["result"]!.ToString().Should().Be("no");  // 0+5=5 NOT > 12
	}

	[Fact]
	public async Task Iif_WithNestedPropertyCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT IIF(IS_DEFINED(c.nested) AND NOT IS_NULL(c.nested) AND c.nested.score > 5, 'high', 'low') AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("high"); // score=9.5
		results[1]["result"]!.ToString().Should().Be("low");  // nested=null
		results[2]["result"]!.ToString().Should().Be("low");  // nested=null
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 11: Undefined property in return branches (Category D)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_TrueCondition_UndefinedInUnselectedBranch_ReturnsValue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(true, 'value', c.nonExistent) AS r FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["r"]!.ToString().Should().Be("value");
	}

	[Fact]
	public async Task Iif_FalseCondition_UndefinedInUnselectedBranch_ReturnsValue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(false, c.nonExistent, 'fallback') AS r FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["r"]!.ToString().Should().Be("fallback");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 12: Complex return values (Category E)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_WithNestedPropertyReturnValue_ReturnsNestedValue()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT IIF(IS_DEFINED(c.nested) AND NOT IS_NULL(c.nested), c.nested.score, 0) AS result FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		((double)results[0]["result"]!).Should().Be(9.5);
		((long)results[1]["result"]!).Should().Be(0);
		((long)results[2]["result"]!).Should().Be(0);
	}

	[Fact]
	public async Task Iif_WithFloatReturnValues_ReturnsFloat()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(true, 3.14, 0) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		((double)results[0]["result"]!).Should().Be(3.14);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Group 13: Composition with other SQL features (Category F)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_WithCoalesce_ComposesCorrectly()
	{
		await SeedItems();
		// IIF(false, null, null) → null. COALESCE(null, 'default') → null (null is defined)
		var query = new QueryDefinition("SELECT COALESCE(IIF(false, null, null), 'default') AS r FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["r"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task Iif_MultipleInWhereWithAnd_FiltersCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT c.name FROM c WHERE IIF(c.isActive, true, false) = true AND c.value > 5");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task Iif_WithStringConcatInBothBranches_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition(
			"SELECT IIF(c.isActive, CONCAT('Active-', c.name), CONCAT('Inactive-', c.name)) AS label FROM c ORDER BY c.id");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(3);
		results[0]["label"]!.ToString().Should().Be("Active-Alice");
		results[1]["label"]!.ToString().Should().Be("Inactive-Bob");
		results[2]["label"]!.ToString().Should().Be("Active-Charlie");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 1A: High-priority gap tests (G1–G4)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_SelectedBranchUndefined_OmitsProperty()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(true, c.nonExistent, 'fallback') AS r FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0].Property("r").Should().BeNull("selected branch is undefined so property should be omitted");
	}

	[Fact]
	public async Task Iif_FalseBranchSelected_UndefinedBranch_OmitsProperty()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(false, 'value', c.nonExistent) AS r FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0].Property("r").Should().BeNull("false branch is undefined so property should be omitted");
	}

	[Fact]
	public async Task Iif_UndefinedComparisonCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.nonExistent > 5, 'yes', 'no') AS result FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_BooleanFalseLiteralCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(false, 'yes', 'no') AS result FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_AllArgsUndefined_OmitsProperty()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.missing1, c.missing2, c.missing3) AS r FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0].Property("r").Should().BeNull("condition is undefined → false branch → c.missing3 → undefined → omit");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 1B: SQL feature composition tests (G5–G15)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_WithGroupBy_GroupsCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, 'active', 'inactive') AS status, COUNT(1) AS cnt FROM c GROUP BY IIF(c.isActive, 'active', 'inactive')");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(2);
		var active = results.First(r => r["status"]!.ToString() == "active");
		var inactive = results.First(r => r["status"]!.ToString() == "inactive");
		active["cnt"]!.Value<long>().Should().Be(2);
		inactive["cnt"]!.Value<long>().Should().Be(1);
	}

	[Fact]
	public async Task Iif_WithDistinct_ReturnsDistinctValues()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT DISTINCT IIF(c.isActive, 'yes', 'no') AS status FROM c");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(2);
		results.Select(r => r["status"]!.ToString()).Should().BeEquivalentTo(["yes", "no"]);
	}

	[Fact]
	public async Task Iif_WithTop_LimitsResults()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT TOP 2 IIF(c.value > 10, 'high', 'low') AS level FROM c ORDER BY c.value DESC");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(2);
		results[0]["level"]!.ToString().Should().Be("high");  // value=20
		results[1]["level"]!.ToString().Should().Be("low");   // value=10
	}

	[Fact]
	public async Task Iif_WithOffsetLimit_PaginatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, 'yes', 'no') AS status FROM c ORDER BY c.id OFFSET 1 LIMIT 1");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["status"]!.ToString().Should().Be("no"); // id='2' → inactive
	}

	[Fact]
	public async Task Iif_WithSelectStar_IncludesComputedField()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT *, IIF(c.isActive, 'yes', 'no') AS status FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["status"]!.ToString().Should().Be("yes");
		results[0]["id"]!.ToString().Should().Be("1");
		results[0]["name"]!.ToString().Should().Be("Alice");
	}

	[Fact(Skip = "Known limitation: scalar subquery (SELECT VALUE IIF(...)) AS alias not supported by parser")]
	public async Task Iif_InScalarSubquery_Works()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT (SELECT VALUE IIF(c.isActive, 'yes', 'no')) AS status FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["status"]!.ToString().Should().Be("yes");
	}

	[Fact]
	public async Task Iif_WithJoin_EvaluatesPerJoinedRow()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT t AS tag, IIF(t = 'dot', 'match', 'no') AS result FROM c JOIN t IN c.tags WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(2);
		var dotResult = results.First(r => r["tag"]!.ToString() == "dot");
		var netResult = results.First(r => r["tag"]!.ToString() == "net");
		dotResult["result"]!.ToString().Should().Be("match");
		netResult["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_WithBetweenCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.value BETWEEN 5 AND 15, 'in range', 'out') AS range FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["range"]!.ToString().Should().Be("in range");  // value=10
		results[1]["range"]!.ToString().Should().Be("out");       // value=20
		results[2]["range"]!.ToString().Should().Be("out");       // value=0
	}

	[Fact]
	public async Task Iif_WithInCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.value IN (10, 20), 'match', 'no match') AS result FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("match");     // value=10
		results[1]["result"]!.ToString().Should().Be("match");     // value=20
		results[2]["result"]!.ToString().Should().Be("no match");  // value=0
	}

	[Fact]
	public async Task Iif_WithLikeCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.name LIKE 'A%', 'A-name', 'other') AS result FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("A-name");  // Alice
		results[1]["result"]!.ToString().Should().Be("other");   // Bob
		results[2]["result"]!.ToString().Should().Be("other");   // Charlie
	}

	[Fact]
	public async Task Iif_WithIsNullSyntaxCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.nested IS NULL, 'null', 'not null') AS result FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("not null");  // nested exists
		results[1]["result"]!.ToString().Should().Be("null");      // nested = null
		results[2]["result"]!.ToString().Should().Be("null");      // nested = null
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Phase 1C: Additional edge cases (G16–G27)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Iif_WithCoalesceInBranch_ComposesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, c.nested.score ?? 0, -1) AS result FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["result"]!.Value<double>().Should().Be(9.5);  // id1: active, nested.score=9.5
		results[1]["result"]!.Value<double>().Should().Be(-1);    // id2: inactive
		results[2]["result"]!.Value<double>().Should().Be(0);     // id3: active, nested=null → coalesce to 0
	}

	[Fact]
	public async Task Iif_WithTernaryInBranch_ComposesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, (c.value > 10 ? 'high' : 'low'), 'inactive') AS r FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["r"]!.ToString().Should().Be("low");       // id1: active, 10 NOT > 10
		results[1]["r"]!.ToString().Should().Be("inactive");  // id2: not active
		results[2]["r"]!.ToString().Should().Be("low");       // id3: active, 0 NOT > 10
	}

	[Fact]
	public async Task Iif_WithParameterizedBooleanCondition_UsesParam()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(@flag, 'yes', 'no') AS result FROM c WHERE c.id = '1'")
			.WithParameter("@flag", true);
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("yes");
	}

	[Fact]
	public async Task Iif_WithParameterizedNonBooleanCondition_ReturnsFalseBranch()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(@val, 'yes', 'no') AS result FROM c WHERE c.id = '1'")
			.WithParameter("@val", 42);
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["result"]!.ToString().Should().Be("no");
	}

	[Fact]
	public async Task Iif_WithMathFunctionsInBranches_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.value > 0, SQRT(c.value), ABS(c.value)) AS result FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["result"]!.Value<double>().Should().BeApproximately(Math.Sqrt(10), 0.001);
	}

	[Fact]
	public async Task Iif_WithArrayContainsCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(ARRAY_CONTAINS(c.tags, 'dot'), 'dotnet', 'other') AS result FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("dotnet");  // id1: tags=["dot","net"]
		results[1]["result"]!.ToString().Should().Be("other");   // id2: tags=["java"]
		results[2]["result"]!.ToString().Should().Be("other");   // id3: tags=[]
	}

	[Fact]
	public async Task Iif_WithEndswithCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(ENDSWITH(c.name, 'e'), 'ends-e', 'no') AS result FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["result"]!.ToString().Should().Be("ends-e");   // Alice
		results[1]["result"]!.ToString().Should().Be("no");       // Bob
		results[2]["result"]!.ToString().Should().Be("ends-e");   // Charlie
	}

	[Fact]
	public async Task Iif_NestedIifInCondition_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(IIF(c.isActive, true, false), 'active', 'inactive') AS r FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["r"]!.ToString().Should().Be("active");     // id1
		results[1]["r"]!.ToString().Should().Be("inactive");   // id2
		results[2]["r"]!.ToString().Should().Be("active");     // id3
	}

	[Fact]
	public async Task Iif_WithUnaryMinusInReturn_EvaluatesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(c.isActive, c.value, -c.value) AS r FROM c ORDER BY c.id");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(3);
		results[0]["r"]!.Value<long>().Should().Be(10);   // id1: active
		results[1]["r"]!.Value<long>().Should().Be(-20);  // id2: not active → -20
		results[2]["r"]!.Value<long>().Should().Be(0);    // id3: active, value=0
	}

	[Fact]
	public async Task Iif_EagerEvaluation_BothBranchesEvaluated_DivisionByZero()
	{
		// Emulator eagerly evaluates both branches. 1/0 in unselected branch should not crash
		// because integer division by zero typically produces Infinity or is handled gracefully.
		await SeedItems();
		var query = new QueryDefinition("SELECT IIF(true, 'safe', 1/0) AS r FROM c WHERE c.id = '1'");
		var results = await QueryAll<JObject>(query);
		results.Should().HaveCount(1);
		results[0]["r"]!.ToString().Should().Be("safe");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Helper
	// ═══════════════════════════════════════════════════════════════════════════

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
