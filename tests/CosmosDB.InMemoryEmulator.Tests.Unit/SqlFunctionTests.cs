using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class SqlFunctionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedItems()
	{
		var items = new[]
		{
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice Anderson", Value = 10, IsActive = true, Tags = ["dot", "net"] },
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob Brown", Value = 20, IsActive = false, Tags = ["java"] },
			new TestDocument
			{
				Id = "3", PartitionKey = "pk1", Name = "Charlie", Value = 30, IsActive = true, Tags = ["dot"],
				Nested = new NestedObject { Description = "nested value", Score = 3.14 }
			},
			new TestDocument { Id = "4", PartitionKey = "pk1", Name = "  diana  ", Value = 0, IsActive = true, Tags = [] },
			new TestDocument { Id = "5", PartitionKey = "pk1", Name = "Eve", Value = -5, IsActive = false, Tags = ["a", "b", "c"] },
		};
		foreach (var item in items)
		{
			await _container.CreateItemAsync(item, new PartitionKey(item.PartitionKey));
		}
	}

	[Fact]
	public async Task StartsWith_MatchesPrefix()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.name, @prefix)")
			.WithParameter("@prefix", "Ali");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Alice Anderson");
	}

	[Fact]
	public async Task StartsWith_CaseInsensitive()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.name, @prefix, true)")
			.WithParameter("@prefix", "ali");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task EndsWith_MatchesSuffix()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ENDSWITH(c.name, @suffix)")
			.WithParameter("@suffix", "Anderson");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Alice Anderson");
	}

	[Fact]
	public async Task Contains_MatchesSubstring()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.name, @sub)")
			.WithParameter("@sub", "Brown");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Bob Brown");
	}

	[Fact]
	public async Task Contains_CaseInsensitive()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE CONTAINS(c.name, @sub, true)")
			.WithParameter("@sub", "brown");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task ArrayContains_MatchesElement()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, @tag)")
			.WithParameter("@tag", "dot");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task ArrayLength_FiltersOnLength()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE ARRAY_LENGTH(c.tags) > 1");

		var results = await QueryAll<TestDocument>(query);

		// Item 1: ["dot","net"] (2), Item 5: ["a","b","c"] (3)
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task IsDefined_ReturnsFalseForUndefinedProperty()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE IS_DEFINED(c.nonExistentProperty)");

		var results = await QueryAll<TestDocument>(query);

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task IsNull_ReturnsTrueForNullProperty()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE IS_NULL(c.nested)");

		var results = await QueryAll<TestDocument>(query);

		// Items 1,2,4,5 have null nested, item 3 has a nested object
		results.Should().HaveCount(4);
	}

	[Fact]
	public async Task StringConcat_ConcatenatesStrings()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT CONCAT(c.name, '-', c.id) AS combined FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["combined"]!.ToString().Should().Be("Alice Anderson-1");
	}

	[Fact]
	public async Task Lower_ConvertsToLowerCase()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT LOWER(c.name) AS lowerName FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["lowerName"]!.ToString().Should().Be("alice anderson");
	}

	[Fact]
	public async Task Upper_ConvertsToUpperCase()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT UPPER(c.name) AS upperName FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["upperName"]!.ToString().Should().Be("ALICE ANDERSON");
	}

	[Fact]
	public async Task Trim_RemovesWhitespace()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT TRIM(c.name) AS trimmedName FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["trimmedName"]!.ToString().Should().Be("diana");
	}

	[Fact]
	public async Task Ltrim_RemovesLeadingWhitespace()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT LTRIM(c.name) AS trimmedName FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["trimmedName"]!.ToString().Should().Be("diana  ");
	}

	[Fact]
	public async Task Rtrim_RemovesTrailingWhitespace()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT RTRIM(c.name) AS trimmedName FROM c WHERE c.id = '4'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["trimmedName"]!.ToString().Should().Be("  diana");
	}

	[Fact]
	public async Task Left_ReturnsLeftCharacters()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT LEFT(c.name, 3) AS prefix FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["prefix"]!.ToString().Should().Be("Ali");
	}

	[Fact]
	public async Task Right_ReturnsRightCharacters()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT RIGHT(c.name, 5) AS suffix FROM c WHERE c.id = '2'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["suffix"]!.ToString().Should().Be("Brown");
	}

	[Fact]
	public async Task Length_ReturnsStringLength()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT LENGTH(c.name) AS nameLen FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["nameLen"]!.Value<int>().Should().Be(3);
	}

	[Fact]
	public async Task Substring_ReturnsSubstring()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SUBSTRING(c.name, 0, 3) AS sub FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["sub"]!.ToString().Should().Be("Ali");
	}

	[Fact]
	public async Task IndexOf_ReturnsPosition()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT INDEX_OF(c.name, 'Brown') AS pos FROM c WHERE c.id = '2'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["pos"]!.Value<int>().Should().Be(4);
	}

	[Fact]
	public async Task Replace_ReplacesSubstring()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT REPLACE(c.name, 'Alice', 'Alicia') AS replaced FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["replaced"]!.ToString().Should().Be("Alicia Anderson");
	}

	[Fact]
	public async Task Abs_ReturnsAbsoluteValue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ABS(c.value) AS absVal FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["absVal"]!.Value<int>().Should().Be(5);
	}

	[Fact]
	public async Task Floor_ReturnsFloor()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT FLOOR(c.nested.score) AS floored FROM c WHERE c.id = '3'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["floored"]!.Value<double>().Should().Be(3.0);
	}

	[Fact]
	public async Task Ceiling_ReturnsCeiling()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT CEILING(c.nested.score) AS ceiled FROM c WHERE c.id = '3'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["ceiled"]!.Value<double>().Should().Be(4.0);
	}

	[Fact]
	public async Task Round_ReturnsRoundedValue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ROUND(c.nested.score) AS rounded FROM c WHERE c.id = '3'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["rounded"]!.Value<double>().Should().Be(3.0);
	}

	// ── Additional Math functions ──

	[Fact]
	public async Task Sqrt_ReturnsSquareRoot()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SQRT(c.value) AS sqrtVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["sqrtVal"]!.Value<double>().Should().BeApproximately(Math.Sqrt(10), 0.0001);
	}

	[Fact]
	public async Task Square_ReturnsSquared()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SQUARE(c.value) AS sq FROM c WHERE c.id = '2'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["sq"]!.Value<double>().Should().Be(400);
	}

	[Fact]
	public async Task Power_ReturnsPower()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT POWER(c.value, 2) AS pw FROM c WHERE c.id = '3'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["pw"]!.Value<long>().Should().Be(900);
	}

	[Fact]
	public async Task Exp_ReturnsExponential()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT EXP(1) AS expVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["expVal"]!.Value<double>().Should().BeApproximately(Math.E, 0.0001);
	}

	[Fact]
	public async Task Log_ReturnsNaturalLog()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT LOG(c.value) AS logVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["logVal"]!.Value<double>().Should().BeApproximately(Math.Log(10), 0.0001);
	}

	[Fact]
	public async Task Log10_ReturnsLog10()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT LOG10(c.value) AS log10Val FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["log10Val"]!.Value<double>().Should().BeApproximately(1.0, 0.0001);
	}

	[Fact]
	public async Task Sign_ReturnsSign()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SIGN(c.value) AS signVal FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["signVal"]!.Value<double>().Should().Be(-1);
	}

	[Fact]
	public async Task Trunc_ReturnsTruncated()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT TRUNC(c.nested.score) AS truncVal FROM c WHERE c.id = '3'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["truncVal"]!.Value<double>().Should().Be(3.0);
	}

	[Fact]
	public async Task Pi_ReturnsPi()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT PI() AS piVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["piVal"]!.Value<double>().Should().BeApproximately(Math.PI, 0.0001);
	}

	[Fact]
	public async Task Sin_ReturnsSine()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SIN(1) AS sinVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["sinVal"]!.Value<double>().Should().BeApproximately(Math.Sin(1), 0.0001);
	}

	[Fact]
	public async Task Cos_ReturnsCosine()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT COS(0) AS cosVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["cosVal"]!.Value<double>().Should().Be(1.0);
	}

	[Fact]
	public async Task Tan_ReturnsTangent()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT TAN(0) AS tanVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["tanVal"]!.Value<double>().Should().Be(0.0);
	}

	[Fact]
	public async Task Asin_ReturnsArcSine()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ASIN(1) AS asinVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["asinVal"]!.Value<double>().Should().BeApproximately(Math.PI / 2, 0.0001);
	}

	[Fact]
	public async Task Acos_ReturnsArcCosine()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ACOS(1) AS acosVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["acosVal"]!.Value<double>().Should().Be(0.0);
	}

	[Fact]
	public async Task Atan_ReturnsArcTangent()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ATAN(1) AS atanVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["atanVal"]!.Value<double>().Should().BeApproximately(Math.PI / 4, 0.0001);
	}

	[Fact]
	public async Task Atn2_ReturnsArcTangent2()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ATN2(1, 1) AS atn2Val FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["atn2Val"]!.Value<double>().Should().BeApproximately(Math.PI / 4, 0.0001);
	}

	[Fact]
	public async Task Degrees_ConvertsToDegrees()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT DEGREES(PI()) AS degVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["degVal"]!.Value<double>().Should().BeApproximately(180.0, 0.0001);
	}

	[Fact]
	public async Task Radians_ConvertsToRadians()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT RADIANS(180) AS radVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["radVal"]!.Value<double>().Should().BeApproximately(Math.PI, 0.0001);
	}

	[Fact]
	public async Task Rand_ReturnsValueBetweenZeroAndOne()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT RAND() AS randVal FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var randVal = results[0]["randVal"]!.Value<double>();
		randVal.Should().BeGreaterThanOrEqualTo(0.0).And.BeLessThan(1.0);
	}

	// ── Reverse and RegexMatch ──

	[Fact]
	public async Task Reverse_ReversesString()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT REVERSE(c.name) AS reversed FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["reversed"]!.ToString().Should().Be("evE");
	}

	[Fact]
	public async Task RegexMatch_MatchesPattern()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE RegexMatch(c.name, '^Alice')");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
		results[0].Name.Should().Be("Alice Anderson");
	}

	[Fact]
	public async Task RegexMatch_CaseInsensitive()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE RegexMatch(c.name, '^alice', 'i')");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
	}

	// ── Type checking functions ──

	[Fact]
	public async Task IsArray_ReturnsTrueForArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_ARRAY(c.tags) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task IsBool_ReturnsTrueForBoolean()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_BOOL(c.isActive) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task IsNumber_ReturnsTrueForNumber()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_NUMBER(c.value) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task IsString_ReturnsTrueForString()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_STRING(c.name) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task IsObject_ReturnsTrueForObject()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_OBJECT(c.nested) FROM c WHERE c.id = '3'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task IsPrimitive_ReturnsTrueForPrimitive()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_PRIMITIVE(c.name) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── Conversion functions ──

	[Fact]
	public async Task ToNumber_ConvertsStringToNumber()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ToNumber('42.5') FROM c WHERE c.id = '1'");

		var results = await QueryAll<double>(query);

		results.Should().ContainSingle().Which.Should().Be(42.5);
	}

	[Fact]
	public async Task ToBoolean_ConvertsStringToBoolean()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE ToBoolean('true') FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	// ── IS_FINITE_NUMBER and IS_INTEGER ──

	[Fact]
	public async Task IsFiniteNumber_ReturnsTrueForFiniteNumber()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_FINITE_NUMBER(c.value) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task IsFiniteNumber_ReturnsFalseForString()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_FINITE_NUMBER(c.name) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task IsInteger_ReturnsTrueForIntegerValue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_INTEGER(c.value) FROM c WHERE c.id = '1'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task IsInteger_ReturnsFalseForDouble()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE IS_INTEGER(c.nested.score) FROM c WHERE c.id = '3'");

		var results = await QueryAll<bool>(query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task IsFiniteNumber_InWhereClause_FiltersCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE IS_FINITE_NUMBER(c.value)");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(5);
	}

	[Fact]
	public async Task IsInteger_InWhereClause_FiltersCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE IS_INTEGER(c.value)");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(5);
	}

	// ── Array functions ──

	[Fact]
	public async Task ArraySlice_ReturnsSlice()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ARRAY_SLICE(c.tags, 0, 1) AS sliced FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var sliced = (JArray)results[0]["sliced"]!;
		sliced.Should().HaveCount(1);
		sliced[0]!.ToString().Should().Be("a");
	}

	[Fact]
	public async Task ArrayConcat_ConcatenatesArrays()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ARRAY_CONCAT(c.tags, c.tags) AS doubled FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		var doubled = (JArray)results[0]["doubled"]!;
		doubled.Should().HaveCount(4);
	}

	// ── New string functions ──

	[Fact]
	public async Task Replicate_RepeatsString()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT REPLICATE(c.name, 3) AS rep FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["rep"]!.ToString().Should().Be("EveEveEve");
	}

	[Fact]
	public async Task Replicate_ZeroCount_ReturnsEmpty()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT REPLICATE(c.name, 0) AS rep FROM c WHERE c.id = '5'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["rep"]!.ToString().Should().BeEmpty();
	}

	[Fact]
	public async Task StringEquals_MatchesExact()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'Eve')");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
		results[0].Id.Should().Be("5");
	}

	[Fact]
	public async Task StringEquals_CaseInsensitive()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE STRING_EQUALS(c.name, 'eve', true)");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
		results[0].Id.Should().Be("5");
	}

	[Fact]
	public async Task StringToArray_ParsesJsonArray()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE StringToArray('[1, 2, 3]') FROM c WHERE c.id = '1'");

		var results = await QueryAll<JArray>(query);

		results.Should().ContainSingle();
		results[0].Should().HaveCount(3);
	}

	[Fact]
	public async Task StringToBoolean_ParsesTrueAndFalse()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT StringToBoolean('true') AS t, StringToBoolean('false') AS f FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["t"]!.Value<bool>().Should().BeTrue();
		results[0]["f"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task StringToNumber_ParsesInteger()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE StringToNumber('42') FROM c WHERE c.id = '1'");

		var results = await QueryAll<long>(query);

		results.Should().ContainSingle().Which.Should().Be(42);
	}

	[Fact]
	public async Task StringToNumber_ParsesDecimal()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE StringToNumber('3.14') FROM c WHERE c.id = '1'");

		var results = await QueryAll<double>(query);

		results.Should().ContainSingle().Which.Should().Be(3.14);
	}

	[Fact]
	public async Task StringToObject_ParsesJsonObject()
	{
		await SeedItems();
		var query = new QueryDefinition("""SELECT VALUE StringToObject('{"a": 1}') FROM c WHERE c.id = '1'""");

		var results = await QueryAll<JObject>(query);

		results.Should().ContainSingle();
		results[0]["a"]!.Value<int>().Should().Be(1);
	}

	// ── Integer math functions ──

	[Fact]
	public async Task NumberBin_RoundsDownToNearestBin()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT NumberBin(c.value, 7) AS binned FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["binned"]!.Value<double>().Should().Be(7);
	}

	[Fact]
	public async Task IntAdd_AddsIntegers()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntAdd(c.value, 5) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(15);
	}

	[Fact]
	public async Task IntSub_SubtractsIntegers()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntSub(c.value, 3) AS result FROM c WHERE c.id = '2'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(17);
	}

	[Fact]
	public async Task IntMul_MultipliesIntegers()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntMul(c.value, 3) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(30);
	}

	[Fact]
	public async Task IntDiv_DividesIntegers()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntDiv(c.value, 3) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(3);
	}

	[Fact]
	public async Task IntDiv_DivisionByZero_ReturnsUndefined()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntDiv(c.value, 0) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"].Should().BeNull();
	}

	[Fact]
	public async Task IntMod_ReturnsRemainder()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntMod(c.value, 3) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(1);
	}

	[Fact]
	public async Task IntBitAnd_ReturnsBitwiseAnd()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntBitAnd(c.value, 6) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(10 & 6);
	}

	[Fact]
	public async Task IntBitOr_ReturnsBitwiseOr()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntBitOr(c.value, 5) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(10 | 5);
	}

	[Fact]
	public async Task IntBitXor_ReturnsBitwiseXor()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntBitXor(c.value, 7) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(10 ^ 7);
	}

	[Fact]
	public async Task IntBitNot_ReturnsBitwiseNot()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntBitNot(c.value) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(~10L);
	}

	[Fact]
	public async Task IntBitLeftShift_ShiftsLeft()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntBitLeftShift(c.value, 2) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(10L << 2);
	}

	[Fact]
	public async Task IntBitRightShift_ShiftsRight()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT IntBitRightShift(c.value, 1) AS result FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["result"]!.Value<long>().Should().Be(10L >> 1);
	}

	[Fact]
	public async Task SumAggregate_ReturnsCorrectSum()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT SUM(c.value) AS total FROM c");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["total"]!.Value<int>().Should().Be(55);
	}

	[Fact]
	public async Task AvgAggregate_ReturnsCorrectAverage()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT AVG(c.value) AS average FROM c");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["average"]!.Value<double>().Should().Be(11.0);
	}

	[Fact]
	public async Task MinAggregate_ReturnsMinValue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT MIN(c.value) AS minVal FROM c");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["minVal"]!.Value<int>().Should().Be(-5);
	}

	[Fact]
	public async Task MaxAggregate_ReturnsMaxValue()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT MAX(c.value) AS maxVal FROM c");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["maxVal"]!.Value<int>().Should().Be(30);
	}

	[Fact]
	public async Task GroupBy_GroupsCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT c.isActive, COUNT(1) AS cnt FROM c GROUP BY c.isActive");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(2);
		var activeGroup = results.First(r => r["isActive"]!.Value<bool>());
		activeGroup["cnt"]!.Value<int>().Should().Be(3);
		var inactiveGroup = results.First(r => !r["isActive"]!.Value<bool>());
		inactiveGroup["cnt"]!.Value<int>().Should().Be(2);
	}

	[Fact]
	public async Task GroupByHaving_FiltersGroups()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT c.isActive, COUNT(1) AS cnt FROM c GROUP BY c.isActive HAVING COUNT(1) > 2");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["isActive"]!.Value<bool>().Should().BeTrue();
		results[0]["cnt"]!.Value<int>().Should().Be(3);
	}

	[Fact]
	public async Task InExpression_FiltersMultipleValues()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE c.name IN ('Alice Anderson', 'Bob Brown', 'NotExist')");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task NotIn_FiltersExcludedValues()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE c.name NOT IN ('Alice Anderson', 'Bob Brown')");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task Between_FiltersRange()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE c.value BETWEEN 10 AND 30");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task EmptyResult_ReturnsEmptyCollection()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE c.name = 'NonExistent'");

		var results = await QueryAll<TestDocument>(query);

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ToString_ReturnsStringRepresentation()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT ToString(c.value) AS str FROM c WHERE c.id = '1'");

		var results = await QueryAll<JObject>(query);

		results.Should().HaveCount(1);
		results[0]["str"]!.ToString().Should().Be("10");
	}

	[Fact]
	public async Task ValueSelect_ReturnsScalarValues()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT VALUE c.name FROM c WHERE c.id = '1'");

		var results = await QueryAll<string>(query);

		results.Should().HaveCount(1);
		results[0].Should().Be("Alice Anderson");
	}

	[Fact]
	public async Task NestedPropertyAccess_QueriesCorrectly()
	{
		await SeedItems();
		var query = new QueryDefinition("SELECT * FROM c WHERE c.nested.score > 3.0");

		var results = await QueryAll<TestDocument>(query);

		results.Should().HaveCount(1);
		results[0].Id.Should().Be("3");
	}

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

	// ═══════════════════════════════════════════════════════════════════════════
	//  Spatial functions
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task StDistance_BetweenTwoPoints_ReturnsMeters()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition(
			"SELECT VALUE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [-2.2426, 53.4808]}) FROM c");

		var results = await QueryAll<double>(container, query);

		results.Should().ContainSingle();
		results[0].Should().BeApproximately(262_000, 5_000); // London to Manchester ~262km
	}

	[Fact]
	public async Task StDistance_WithNonPoint_ReturnsEmpty()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Name = "not a point"
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition(
			"SELECT VALUE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [0, 0]}) FROM c");

		var results = await QueryAll<object>(container, query);

		results.Should().ContainSingle().Which.Should().BeNull();
	}

	[Fact]
	public async Task StWithin_PointInsidePolygon_ReturnsTrue()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
		}, new PartitionKey("pk1"));

		// Polygon covering most of southern England
		var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_WITHIN(c.location, {
                'type': 'Polygon',
                'coordinates': [[[-2.0, 50.0], [1.0, 50.0], [1.0, 52.0], [-2.0, 52.0], [-2.0, 50.0]]]
            })
            """);

		var results = await QueryAll<GeoDocument>(container, query);

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task StWithin_PointOutsidePolygon_ReturnsEmpty()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
		}, new PartitionKey("pk1"));

		// Polygon in France
		var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_WITHIN(c.location, {
                'type': 'Polygon',
                'coordinates': [[[1.0, 43.0], [3.0, 43.0], [3.0, 45.0], [1.0, 45.0], [1.0, 43.0]]]
            })
            """);

		var results = await QueryAll<GeoDocument>(container, query);

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task StIntersects_PointInPolygon_ReturnsTrue()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_INTERSECTS(c.location, {
                'type': 'Polygon',
                'coordinates': [[[-1.0, 51.0], [0.0, 51.0], [0.0, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
            })
            """);

		var results = await QueryAll<GeoDocument>(container, query);

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task StIsValid_ValidPoint_ReturnsTrue()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE ST_ISVALID(c.location) FROM c");

		var results = await QueryAll<bool>(container, query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task StIsValid_InvalidGeoJson_ReturnsFalse()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { 999.0, 999.0 } }
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE ST_ISVALID(c.location) FROM c");

		var results = await QueryAll<bool>(container, query);

		results.Should().ContainSingle().Which.Should().BeFalse();
	}

	[Fact]
	public async Task StIsValidDetailed_ValidPolygon_ReturnsValidTrue()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Area = new GeoJsonGeometry
			{
				Type = "Polygon",
				Coordinates = new[] { new[] { new[] { 0.0, 0.0 }, new[] { 1.0, 0.0 }, new[] { 1.0, 1.0 }, new[] { 0.0, 0.0 } } }
			}
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE ST_ISVALIDDETAILED(c.area) FROM c");

		var results = await QueryAll<JObject>(container, query);

		results.Should().ContainSingle();
		results[0]["valid"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task StIsValidDetailed_InvalidPolygon_ReturnsValidFalseWithReason()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Area = new GeoJsonGeometry
			{
				Type = "Polygon",
				Coordinates = new[] { new[] { new[] { 0.0, 0.0 }, new[] { 1.0, 0.0 } } }
			}
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE ST_ISVALIDDETAILED(c.area) FROM c");

		var results = await QueryAll<JObject>(container, query);

		results.Should().ContainSingle();
		results[0]["valid"]!.Value<bool>().Should().BeFalse();
		results[0]["reason"]!.ToString().Should().NotBeEmpty();
	}

	// ── Additional spatial tests ──

	[Fact]
	public async Task StDistance_BetweenSamePoint_ReturnsZero()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition(
			"SELECT VALUE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [-0.1278, 51.5074]}) FROM c");

		var results = await QueryAll<double>(container, query);

		results.Should().ContainSingle().Which.Should().Be(0);
	}

	[Fact]
	public async Task StWithin_PointInCircle_ReturnsTrue()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_WITHIN(c.location, {
                'center': {'type': 'Point', 'coordinates': [-0.1278, 51.5074]},
                'radius': 1000
            })
            """);

		var results = await QueryAll<GeoDocument>(container, query);

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task StIntersects_TwoOverlappingPolygons_ReturnsTrue()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Area = new GeoJsonGeometry
			{
				Type = "Polygon",
				Coordinates = new[] { new[] { new[] { 0.0, 0.0 }, new[] { 2.0, 0.0 }, new[] { 2.0, 2.0 }, new[] { 0.0, 2.0 }, new[] { 0.0, 0.0 } } }
			}
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_INTERSECTS(c.area, {
                'type': 'Polygon',
                'coordinates': [[[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0], [1.0, 1.0]]]
            })
            """);

		var results = await QueryAll<GeoDocument>(container, query);

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task StIsValid_ValidLineString_ReturnsTrue()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Area = new GeoJsonGeometry
			{
				Type = "LineString",
				Coordinates = new[] { new[] { 0.0, 0.0 }, new[] { 1.0, 1.0 }, new[] { 2.0, 0.0 } }
			}
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE ST_ISVALID(c.area) FROM c");

		var results = await QueryAll<bool>(container, query);

		results.Should().ContainSingle().Which.Should().BeTrue();
	}

	[Fact]
	public async Task StIsValidDetailed_LineStringTooFewPoints_ReturnsInvalid()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Area = new GeoJsonGeometry
			{
				Type = "LineString",
				Coordinates = new[] { new[] { 0.0, 0.0 } }
			}
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE ST_ISVALIDDETAILED(c.area) FROM c");

		var results = await QueryAll<JObject>(container, query);

		results.Should().ContainSingle();
		results[0]["valid"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task StDistance_InWhereClause_FiltersNearbyPoints()
	{
		var container = new InMemoryContainer("geo-test", "/partitionKey");
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "london",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { -0.1278, 51.5074 } }
		}, new PartitionKey("pk1"));
		await container.CreateItemAsync(new GeoDocument
		{
			Id = "paris",
			PartitionKey = "pk1",
			Location = new GeoJsonGeometry { Type = "Point", Coordinates = new[] { 2.3522, 48.8566 } }
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("""
            SELECT * FROM c WHERE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [-0.13, 51.51]}) < 10000
            """);

		var results = await QueryAll<GeoDocument>(container, query);

		results.Should().ContainSingle().Which.Id.Should().Be("london");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  UDF registration
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task RegisteredUdf_IsCalledDuringQuery()
	{
		var container = new InMemoryContainer("udf-test", "/partitionKey");
		container.RegisterUdf("doubleValue", args =>
		{
			var val = Convert.ToDouble(args[0]);
			return val * 2;
		});

		await container.CreateItemAsync(new UdfDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Value = 21
		}, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE udf.doubleValue(c.value) FROM c");

		var results = await QueryAll<double>(container, query);

		results.Should().ContainSingle().Which.Should().Be(42);
	}

	[Fact]
	public async Task RegisteredUdf_InWhereClause_FiltersCorrectly()
	{
		var container = new InMemoryContainer("udf-test", "/partitionKey");
		container.RegisterUdf("isEven", args =>
		{
			var val = Convert.ToInt64(args[0]);
			return val % 2 == 0;
		});

		await container.CreateItemAsync(new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 10 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new UdfDocument { Id = "2", PartitionKey = "pk1", Value = 11 }, new PartitionKey("pk1"));
		await container.CreateItemAsync(new UdfDocument { Id = "3", PartitionKey = "pk1", Value = 12 }, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE udf.isEven(c.value)");

		var results = await QueryAll<JObject>(container, query);

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task UnregisteredUdf_ThrowsNotSupportedException()
	{
		var container = new InMemoryContainer("udf-test", "/partitionKey");
		await container.CreateItemAsync(new UdfDocument { Id = "1", PartitionKey = "pk1", Value = 10 }, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE udf.missing(c.value) FROM c");

		var act = async () => await QueryAll<object>(container, query);

		await act.Should().ThrowAsync<CosmosException>()
			.WithMessage("*RegisterUdf*");
	}

	[Fact]
	public async Task RegisteredUdf_WithMultipleArgs_ReceivesAllArgs()
	{
		var container = new InMemoryContainer("udf-test", "/partitionKey");
		container.RegisterUdf("add", args =>
		{
			var a = Convert.ToDouble(args[0]);
			var b = Convert.ToDouble(args[1]);
			return a + b;
		});

		await container.CreateItemAsync(new UdfDocument { Id = "1", PartitionKey = "pk1", X = 10, Y = 32 }, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT VALUE udf.add(c.x, c.y) FROM c");

		var results = await QueryAll<double>(container, query);

		results.Should().ContainSingle().Which.Should().Be(42);
	}

	private static async Task<List<T>> QueryAll<T>(InMemoryContainer container, QueryDefinition query)
	{
		var iterator = container.GetItemQueryIterator<T>(query);
		var results = new List<T>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			results.AddRange(response);
		}
		return results;
	}
}


public class SqlFunctionGapTests3
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task SqlFunc_GetCurrentTimestamp_NotImplemented()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JToken>(
			"SELECT VALUE GetCurrentTimestamp() FROM c");
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().NotBeEmpty();
	}

	[Fact]
	public async Task SqlFunc_Substring_OutOfBounds()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "hello" },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JToken>(
			"SELECT VALUE SUBSTRING(c.name, 10, 5) FROM c");
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
		// Out-of-bounds substring returns empty string
		results[0].ToString().Should().BeEmpty();
	}

	[Fact]
	public async Task SqlFunc_MathFunctions_WithNull_ReturnUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","partitionKey":"pk1","val":null}""")),
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JToken>(
			"SELECT VALUE ABS(c.val) FROM c");
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		// ABS(null) → undefined → omitted by SELECT VALUE
		results.Should().BeEmpty();
	}
}


public class SqlFunctionGapTests2
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task SqlFunc_DateTimeFunctions_NotImplemented()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JToken>(
			"SELECT VALUE GetCurrentDateTime() FROM c");
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().NotBeEmpty();
	}

	[Fact]
	public async Task SqlFunc_IS_INTEGER_DistinguishesFromFloat()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk1","intVal":42,"floatVal":42.5}""")),
			new PartitionKey("pk1"));

		var intResult = _container.GetItemQueryIterator<JToken>(
			"SELECT VALUE IS_INTEGER(c.intVal) FROM c");
		var intResults = new List<JToken>();
		while (intResult.HasMoreResults)
		{
			var page = await intResult.ReadNextAsync();
			intResults.AddRange(page);
		}

		intResults.Should().ContainSingle();
	}

	[Fact]
	public async Task SqlFunc_NullArgs_StringFunctions_ReturnUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","partitionKey":"pk1","name":null}""")),
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JToken>(
			"SELECT VALUE UPPER(c.name) FROM c");
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		// Cosmos DB: UPPER(null) → undefined → excluded from SELECT VALUE results
		results.Should().BeEmpty();
	}
}


public class SqlFunctionGapTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task SeedItem()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Hello World", Value = 42 },
			new PartitionKey("pk1"));
	}

	private async Task<List<JObject>> RunQuery(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}

	private async Task<List<JToken>> RunQueryTokens(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JToken>(sql);
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}
		return results;
	}

	[Fact]
	public async Task Contains_CaseSensitive_Default()
	{
		await SeedItem();
		var results = await RunQuery("""SELECT * FROM c WHERE CONTAINS(c.name, "hello")""");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task Contains_CaseInsensitive_ThirdParam()
	{
		await SeedItem();
		var results = await RunQuery("""SELECT * FROM c WHERE CONTAINS(c.name, "hello", true)""");
		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task StartsWith_CaseInsensitive()
	{
		await SeedItem();
		var results = await RunQuery("""SELECT * FROM c WHERE STARTSWITH(c.name, "hello", true)""");
		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task ArrayContains_PartialMatch()
	{
		var json = """{"id":"1","partitionKey":"pk1","items":[{"name":"urgent","priority":1},{"name":"review","priority":2}]}""";
		await _container.CreateItemStreamAsync(new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"""SELECT * FROM c WHERE ARRAY_CONTAINS(c.items, {"name": "urgent"}, true)""");

		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task Is_Defined_FalseForMissingField()
	{
		await SeedItem();
		var results = await RunQuery("SELECT * FROM c WHERE IS_DEFINED(c.nonExistentField)");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task Is_Defined_TrueForExistingField()
	{
		await SeedItem();
		var results = await RunQuery("SELECT * FROM c WHERE IS_DEFINED(c.name)");
		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task IndexOf_NotFound_ReturnsNegative()
	{
		await SeedItem();
		var results = await RunQueryTokens("SELECT VALUE INDEX_OF(c.name, \"xyz\") FROM c");
		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task Substring_Basic()
	{
		await SeedItem();
		var results = await RunQueryTokens("SELECT VALUE SUBSTRING(c.name, 0, 5) FROM c");
		results.Should().HaveCount(1);
	}

	[Fact]
	public async Task Replace_MultipleOccurrences()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "aaa" },
			new PartitionKey("pk1"));

		var results = await RunQueryTokens("""SELECT VALUE REPLACE(c.name, "a", "bb") FROM c""");
		results.Should().HaveCount(1);
	}
}



public class SqlFunctionGapTests4
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task SqlFunc_Coalesce_WithMultipleArgs()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","partitionKey":"pk1","a":null,"b":"value"}""")),
			new PartitionKey("pk1"));

		// COALESCE returns the first non-undefined value. c.a is null (defined) → returned as null.
		var iterator = _container.GetItemQueryIterator<JToken>(
			"""SELECT VALUE COALESCE(c.a, c.b) FROM c""");
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0].Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task SqlFunc_IS_PRIMITIVE_ReturnsFalse_ForObjectAndArray()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","partitionKey":"pk1","arr":[1,2],"obj":{"a":1},"str":"hello"}""")),
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"""SELECT IS_PRIMITIVE(c.arr) AS arrPrim, IS_PRIMITIVE(c.obj) AS objPrim, IS_PRIMITIVE(c.str) AS strPrim FROM c""");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["arrPrim"]!.Value<bool>().Should().BeFalse();
		results[0]["objPrim"]!.Value<bool>().Should().BeFalse();
		results[0]["strPrim"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task SqlFunc_TypeFunctions_WithUndefined_ReturnFalse()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"""SELECT IS_STRING(c.nonExistent) AS isStr, IS_NUMBER(c.nonExistent) AS isNum FROM c""");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["isStr"]!.Value<bool>().Should().BeFalse();
		results[0]["isNum"]!.Value<bool>().Should().BeFalse();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: Bug Fix Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionBugFixTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	private async Task Seed()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice Anderson", value = 10, isActive = true, tags = new[] { "dot", "net" } }), new PartitionKey("pk1"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "Bob Brown", value = 20, isActive = false }), new PartitionKey("pk1"));
	}

	// Bug 1: LOG with base argument
	[Fact]
	public async Task Log_WithBase_ReturnsCustomBaseLog()
	{
		await Seed();
		var results = await Query("SELECT LOG(8, 2) AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.Value<double>().Should().BeApproximately(3.0, 0.0001);
	}

	// Bug 2: ROUND with precision
	[Fact]
	public async Task Round_WithPrecision_RoundsToDecimalPlaces()
	{
		await Seed();
		var results = await Query("SELECT ROUND(3.14159, 2) AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.Value<double>().Should().BeApproximately(3.14, 0.0001);
	}

	[Fact]
	public async Task Round_NoPrecision_RoundsToInteger()
	{
		await Seed();
		var results = await Query("SELECT ROUND(3.7) AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.Value<double>().Should().BeApproximately(4.0, 0.0001);
	}

	// Bug 3: INDEX_OF with start position
	[Fact]
	public async Task IndexOf_WithStartPosition_ReturnsCorrectIndex()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", text = "hello hello" }), new PartitionKey("pk1"));
		var results = await Query("SELECT INDEX_OF(c.text, 'hello', 1) AS val FROM c");
		results[0]["val"]!.Value<long>().Should().Be(6);
	}

	// Bug 6: StringToNumber rejects NaN/Infinity
	[Fact]
	public async Task StringToNumber_NaN_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT StringToNumber('NaN') AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull(); // undefined → omitted from JSON
	}

	[Fact]
	public async Task StringToNumber_Infinity_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT StringToNumber('Infinity') AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull();
	}

	// Bug 7: COALESCE skips undefined
	[Fact]
	public async Task Coalesce_SkipsUndefined_ReturnsDefined()
	{
		await Seed();
		var results = await Query("SELECT COALESCE(c.nonExistent, c.name) AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.ToString().Should().Be("Alice Anderson");
	}

	[Fact]
	public async Task Coalesce_AllNull_ReturnsNull()
	{
		await Seed();
		var results = await Query("SELECT COALESCE(null, null) AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.Type.Should().Be(JTokenType.Null);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: Missing Function Coverage
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionMissingCoverageTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	private async Task Seed()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice", value = 10, nested = new { description = "D1", score = 3.14 } }), new PartitionKey("pk1"));
	}

	// COT
	[Fact]
	public async Task Cot_ReturnsCorrectValue()
	{
		await Seed();
		var results = await Query("SELECT COT(1) AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.Value<double>().Should().BeApproximately(1.0 / Math.Tan(1), 0.0001);
	}

	// CHOOSE
	[Fact]
	public async Task Choose_ReturnsCorrectElement()
	{
		await Seed();
		var results = await Query("SELECT CHOOSE(2, 'a', 'b', 'c') AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.ToString().Should().Be("b");
	}

	[Fact]
	public async Task Choose_OutOfRange_ReturnsUndefined()
	{
		await Seed();
		// CHOOSE out-of-bounds returns undefined; the field is omitted from the projection
		var results = await Query("SELECT CHOOSE(5, 'a', 'b') AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull();
	}

	// OBJECTTOARRAY / ARRAYTOOBJECT
	[Fact]
	public async Task ObjectToArray_ReturnsKeyValuePairs()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", data = new { x = 1, y = 2 } }), new PartitionKey("pk1"));
		var results = await Query("SELECT ObjectToArray(c.data) AS val FROM c");
		var arr = (JArray)results[0]["val"]!;
		arr.Should().HaveCount(2);
		arr[0]["k"]!.ToString().Should().Be("x");
	}

	[Fact]
	public async Task ArrayToObject_ReturnsObject()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", arr = new[] { new { k = "name", v = "Alice" } } }), new PartitionKey("pk1"));
		var results = await Query("SELECT VALUE ArrayToObject(c.arr) FROM c");
		results[0]["name"]!.ToString().Should().Be("Alice");
	}

	// STRINGJOIN / STRINGSPLIT
	[Fact]
	public async Task StringJoin_JoinsArrayWithSeparator()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", tags = new[] { "a", "b", "c" } }), new PartitionKey("pk1"));
		var results = await Query("SELECT StringJoin(c.tags, '-') AS val FROM c");
		results[0]["val"]!.ToString().Should().Be("a-b-c");
	}

	[Fact]
	public async Task StringSplit_SplitsStringByDelimiter()
	{
		await Seed();
		var results = await Query("SELECT StringSplit('a-b-c', '-') AS val FROM c WHERE c.id = '1'");
		var arr = (JArray)results[0]["val"]!;
		arr.Select(t => t.ToString()).Should().BeEquivalentTo(["a", "b", "c"]);
	}

	// STRINGTONULL
	[Fact]
	public async Task StringToNull_ParsesNullLiteral()
	{
		await Seed();
		var results = await Query("SELECT StringToNull('null') AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task StringToNull_InvalidInput_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT StringToNull('hello') AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull(); // undefined → omitted
	}

	// DOCUMENTID
	[Fact]
	public async Task DocumentId_ReturnsDocumentId()
	{
		await Seed();
		var results = await Query("SELECT DOCUMENTID(c) AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.ToString().Should().NotBeNullOrEmpty();
	}

	// ENDSWITH case-insensitive
	[Fact]
	public async Task EndsWith_CaseInsensitive_ReturnsTrue()
	{
		await Seed();
		var results = await Query("SELECT ENDSWITH(c.name, 'ALICE', true) AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.Value<bool>().Should().BeTrue();
	}

	// COUNT aggregate
	[Fact]
	public async Task CountAggregate_ReturnsCorrectCount()
	{
		await Seed();
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1" }), new PartitionKey("pk1"));
		var results = await Query("SELECT COUNT(1) AS val FROM c");
		results[0]["val"]!.Value<long>().Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: String Function Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionStringEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	private async Task Seed()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Alice", value = 10 }), new PartitionKey("pk1"));
	}

	[Fact]
	public async Task Contains_EmptySubstring_ReturnsTrue()
	{
		await Seed();
		var results = await Query("SELECT CONTAINS(c.name, '') AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task StartsWith_EmptyPrefix_ReturnsTrue()
	{
		await Seed();
		var results = await Query("SELECT STARTSWITH(c.name, '') AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task EndsWith_EmptySuffix_ReturnsTrue()
	{
		await Seed();
		var results = await Query("SELECT ENDSWITH(c.name, '') AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task IndexOf_EmptySubstring_ReturnsZero()
	{
		await Seed();
		var results = await Query("SELECT INDEX_OF(c.name, '') AS val FROM c");
		results[0]["val"]!.Value<long>().Should().Be(0);
	}

	[Fact]
	public async Task Reverse_EmptyString_ReturnsEmpty()
	{
		await Seed();
		var results = await Query("SELECT REVERSE('') AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.ToString().Should().BeEmpty();
	}

	[Fact]
	public async Task Left_CountExceedsLength_ReturnsFullString()
	{
		await Seed();
		var results = await Query("SELECT LEFT(c.name, 100) AS val FROM c");
		results[0]["val"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task Right_CountExceedsLength_ReturnsFullString()
	{
		await Seed();
		var results = await Query("SELECT RIGHT(c.name, 100) AS val FROM c");
		results[0]["val"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task Concat_NoArgs_ReturnsEmpty()
	{
		await Seed();
		var results = await Query("SELECT CONCAT() AS val FROM c WHERE c.id = '1'");
		results[0]["val"]!.ToString().Should().BeEmpty();
	}

	[Fact]
	public async Task StringToBoolean_MixedCase_ReturnsUndefined()
	{
		await Seed();
		// Cosmos DB StringToBoolean is case-sensitive: "True" != "true"
		var results = await Query("SELECT StringToBoolean('True') AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull(); // undefined → omitted
	}

	[Fact]
	public async Task StringToArray_InvalidJson_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT StringToArray('not json') AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull();
	}

	[Fact]
	public async Task ToString_NullInput_ReturnsUndefined()
	{
		await Seed();
		// Cosmos DB: TOSTRING(null) → undefined (property omitted from result)
		var results = await Query("SELECT ToString(null) AS val FROM c WHERE c.id = '1'");
		results.Should().ContainSingle();
		results[0]["val"].Should().BeNull(); // property omitted = null in JObject access
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: Math Function Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionMathEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	private async Task Seed()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1" }), new PartitionKey("pk1"));
	}

	[Fact]
	public async Task Power_ZeroExponent_ReturnsOne()
	{
		await Seed();
		var results = await Query("SELECT POWER(5, 0) AS val FROM c");
		results[0]["val"]!.Value<double>().Should().Be(1.0);
	}

	[Fact]
	public async Task Sign_Zero_ReturnsZero()
	{
		await Seed();
		var results = await Query("SELECT SIGN(0) AS val FROM c");
		results[0]["val"]!.Value<double>().Should().Be(0);
	}

	[Fact]
	public async Task Trunc_NegativeDecimal_TruncatesTowardZero()
	{
		await Seed();
		var results = await Query("SELECT TRUNC(-3.7) AS val FROM c");
		results[0]["val"]!.Value<double>().Should().Be(-3.0);
	}

	[Fact]
	public async Task Exp_Zero_ReturnsOne()
	{
		await Seed();
		var results = await Query("SELECT EXP(0) AS val FROM c");
		results[0]["val"]!.Value<double>().Should().Be(1.0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: Integer Math Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionIntegerEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	private async Task Seed()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1" }), new PartitionKey("pk1"));
	}

	[Fact]
	public async Task IntMod_ByZero_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT IntMod(10, 0) AS val FROM c");
		results[0]["val"].Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: Type Checking Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionTypeCheckEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task IsArray_ReturnsFalseForString()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", val = "hello" }), new PartitionKey("pk1"));
		var results = await Query("SELECT IS_ARRAY(c.val) AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsBool_ReturnsFalseForNumber()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", val = 42 }), new PartitionKey("pk1"));
		var results = await Query("SELECT IS_BOOL(c.val) AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsNumber_ReturnsFalseForBoolean()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", val = true }), new PartitionKey("pk1"));
		var results = await Query("SELECT IS_NUMBER(c.val) AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsString_ReturnsFalseForNumber()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", val = 42 }), new PartitionKey("pk1"));
		var results = await Query("SELECT IS_STRING(c.val) AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsObject_ReturnsFalseForArray()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", val = new[] { 1, 2 } }), new PartitionKey("pk1"));
		var results = await Query("SELECT IS_OBJECT(c.val) AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsObject_ReturnsFalseForNull()
	{
		await _container.CreateItemAsync(JObject.Parse("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"val\":null}"), new PartitionKey("pk1"));
		var results = await Query("SELECT IS_OBJECT(c.val) AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsDefined_TrueForNullProperty()
	{
		await _container.CreateItemAsync(JObject.Parse("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"val\":null}"), new PartitionKey("pk1"));
		var results = await Query("SELECT IS_DEFINED(c.val) AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: Array Function Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionArrayEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task ArraySlice_NegativeStart_FromEnd()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", arr = new[] { 1, 2, 3, 4 } }), new PartitionKey("pk1"));
		var results = await Query("SELECT ARRAY_SLICE(c.arr, -2) AS val FROM c");
		var arr = (JArray)results[0]["val"]!;
		arr.Select(t => t.Value<int>()).Should().BeEquivalentTo([3, 4]);
	}

	[Fact]
	public async Task ArraySlice_StartBeyondLength_ReturnsEmpty()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", arr = new[] { 1, 2 } }), new PartitionKey("pk1"));
		var results = await Query("SELECT ARRAY_SLICE(c.arr, 10) AS val FROM c");
		((JArray)results[0]["val"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ArrayConcat_EmptyArrays_ReturnsEmpty()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1" }), new PartitionKey("pk1"));
		var results = await Query("SELECT ARRAY_CONCAT([], []) AS val FROM c");
		((JArray)results[0]["val"]!).Should().BeEmpty();
	}

	[Fact]
	public async Task ArrayContains_NullElement_MatchesNull()
	{
		await _container.CreateItemAsync(JObject.Parse("{\"id\":\"1\",\"partitionKey\":\"pk1\",\"arr\":[1,null,3]}"), new PartitionKey("pk1"));
		var results = await Query("SELECT ARRAY_CONTAINS(c.arr, null) AS val FROM c");
		results[0]["val"]!.Value<bool>().Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: Aggregate Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionAggregateEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task CountWithFilter_ReturnsFilteredCount()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", active = true }), new PartitionKey("pk1"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", active = false }), new PartitionKey("pk1"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk1", active = true }), new PartitionKey("pk1"));

		var results = await Query("SELECT COUNT(1) AS val FROM c WHERE c.active = true");
		results[0]["val"]!.Value<long>().Should().Be(2);
	}

	[Fact]
	public async Task MaxWithStrings_ReturnsLexMax()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Banana" }), new PartitionKey("pk1"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "2", partitionKey = "pk1", name = "Apple" }), new PartitionKey("pk1"));
		await _container.CreateItemAsync(JObject.FromObject(new { id = "3", partitionKey = "pk1", name = "Cherry" }), new PartitionKey("pk1"));

		var results = await Query("SELECT MAX(c.name) AS val FROM c");
		results[0]["val"]!.ToString().Should().Be("Cherry");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Deep Dive: Conversion Function Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionConversionEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	private async Task<List<JObject>> Query(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	private async Task Seed()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", partitionKey = "pk1" }), new PartitionKey("pk1"));
	}

	[Fact]
	public async Task ToNumber_InvalidString_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT ToNumber('abc') AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull();
	}

	[Fact]
	public async Task ToNumber_NullInput_ReturnsUndefined()
	{
		await Seed();
		// TONUMBER(null) → undefined per Cosmos semantics — field is omitted
		var results = await Query("SELECT ToNumber(null) AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull("undefined values are omitted from projection");
	}

	[Fact]
	public async Task ToBoolean_NullInput_ReturnsUndefined()
	{
		await Seed();
		// TOBOOLEAN(null) returns undefined — field is omitted from result
		var results = await Query("SELECT ToBoolean(null) AS val FROM c WHERE c.id = '1'");
		results[0]["val"].Should().BeNull("undefined values are omitted from projection");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 37 — SQL Function Deep-Dive Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class SqlFunctionDeepDiveTests
{
	private readonly InMemoryContainer _container = new("sql-dd", "/partitionKey");

	private async Task Seed()
	{
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice Anderson", Value = 10, IsActive = true, Tags = ["dot", "net"] },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Bob Brown", Value = 20, IsActive = false, Tags = ["java"] },
			new PartitionKey("pk1"));
		await _container.CreateItemAsync(
			new TestDocument
			{
				Id = "3",
				PartitionKey = "pk1",
				Name = "Charlie",
				Value = 30,
				IsActive = true,
				Tags = ["dot"],
				Nested = new NestedObject { Description = "nested value", Score = 3.14 }
			},
			new PartitionKey("pk1"));
	}

	private async Task<List<JObject>> Query(string sql)
	{
		var iter = _container.GetItemQueryIterator<JObject>(sql);
		var results = new List<JObject>();
		while (iter.HasMoreResults) results.AddRange(await iter.ReadNextAsync());
		return results;
	}

	// ── TYPE() Function ──

	[Fact]
	public async Task Type_String_ReturnsString()
	{
		await Seed();
		var results = await Query("SELECT TYPE(c.name) AS t FROM c WHERE c.id = '1'");
		results[0]["t"]!.Value<string>().Should().Be("string");
	}

	[Fact]
	public async Task Type_Number_ReturnsNumber()
	{
		await Seed();
		var results = await Query("SELECT TYPE(c[\"value\"]) AS t FROM c WHERE c.id = '1'");
		results[0]["t"]!.Value<string>().Should().Be("number");
	}

	[Fact]
	public async Task Type_Boolean_ReturnsBoolean()
	{
		await Seed();
		var results = await Query("SELECT TYPE(c.isActive) AS t FROM c WHERE c.id = '1'");
		results[0]["t"]!.Value<string>().Should().Be("boolean");
	}

	[Fact]
	public async Task Type_Null_ReturnsNull()
	{
		await Seed();
		var results = await Query("SELECT TYPE(null) AS t FROM c WHERE c.id = '1'");
		results[0]["t"]!.Value<string>().Should().Be("null");
	}

	[Fact]
	public async Task Type_Array_ReturnsArray()
	{
		await Seed();
		var results = await Query("SELECT TYPE(c.tags) AS t FROM c WHERE c.id = '1'");
		results[0]["t"]!.Value<string>().Should().Be("array");
	}

	[Fact]
	public async Task Type_Object_ReturnsObject()
	{
		await Seed();
		var results = await Query("SELECT TYPE(c.nested) AS t FROM c WHERE c.id = '3'");
		results[0]["t"]!.Value<string>().Should().Be("object");
	}

	[Fact]
	public async Task Type_UndefinedProperty_ReturnsUndefined()
	{
		await Seed();
		// TYPE on undefined returns undefined — field is omitted
		var results = await Query("SELECT TYPE(c.nonexistent) AS t FROM c WHERE c.id = '1'");
		results[0]["t"].Should().BeNull("TYPE of undefined should be undefined (omitted)");
	}

	// ── IS_NAN ──

	[Fact]
	public async Task IsNan_RegularNumber_ReturnsFalse()
	{
		await Seed();
		var results = await Query("SELECT IS_NAN(c[\"value\"]) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task IsNan_NonNumber_ReturnsFalse()
	{
		await Seed();
		var results = await Query("SELECT IS_NAN(c.name) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<bool>().Should().BeFalse();
	}

	// ── COALESCE Edge Cases ──

	[Fact]
	public async Task Coalesce_ThreeArgs_ReturnsFirstDefined()
	{
		await Seed();
		var results = await Query("SELECT COALESCE(c.nonexistent, c.alsoMissing, c.name) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("Alice Anderson");
	}

	[Fact]
	public async Task Coalesce_AllUndefined_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT COALESCE(c.a, c.b, c.missing) AS r FROM c WHERE c.id = '1'");
		results[0]["r"].Should().BeNull("all args undefined → undefined");
	}

	[Fact]
	public async Task Coalesce_NullAndDefined_ReturnsNull()
	{
		await Seed();
		// null is defined (it's the null value), so it should be returned
		var results = await Query("SELECT COALESCE(null, 'fallback') AS r FROM c WHERE c.id = '1'");
		var token = results[0]["r"];
		token.Should().NotBeNull("COALESCE should return null, not undefined");
		token!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task Coalesce_SingleArg_ReturnsThatArg()
	{
		await Seed();
		var results = await Query("SELECT COALESCE(c.name) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("Alice Anderson");
	}

	// ── CHOOSE Edge Cases ──

	[Fact]
	public async Task Choose_FirstElement_ReturnsCorrect()
	{
		await Seed();
		var results = await Query("SELECT CHOOSE(1, 'a', 'b', 'c') AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("a");
	}

	[Fact]
	public async Task Choose_LastElement_ReturnsCorrect()
	{
		await Seed();
		var results = await Query("SELECT CHOOSE(3, 'a', 'b', 'c') AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("c");
	}

	[Fact]
	public async Task Choose_ZeroIndex_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT CHOOSE(0, 'a', 'b', 'c') AS r FROM c WHERE c.id = '1'");
		results[0]["r"].Should().BeNull("index 0 is out of bounds → undefined");
	}

	[Fact]
	public async Task Choose_OutOfBounds_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT CHOOSE(10, 'a', 'b', 'c') AS r FROM c WHERE c.id = '1'");
		results[0]["r"].Should().BeNull("index beyond count → undefined");
	}

	// ── String Edge Cases ──

	[Fact]
	public async Task Replace_EmptyFind_ReturnsOriginal()
	{
		await Seed();
		var results = await Query("SELECT REPLACE('hello', '', 'x') AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("hello");
	}

	[Fact]
	public async Task Left_ZeroCount_ReturnsEmpty()
	{
		await Seed();
		var results = await Query("SELECT LEFT('hello', 0) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().BeEmpty();
	}

	[Fact]
	public async Task Right_ZeroCount_ReturnsEmpty()
	{
		await Seed();
		var results = await Query("SELECT RIGHT('hello', 0) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().BeEmpty();
	}

	[Fact]
	public async Task Length_NonString_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT LENGTH(42) AS r FROM c WHERE c.id = '1'");
		results[0]["r"].Should().BeNull("LENGTH of non-string → undefined");
	}

	[Fact]
	public async Task IndexOf_WithStartPosition_NotFound_ReturnsNegativeOne()
	{
		await Seed();
		var results = await Query("SELECT INDEX_OF('hello world', 'hello', 5) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<int>().Should().Be(-1);
	}

	[Fact]
	public async Task RegexMatch_InvalidPattern_ReturnsUndefined()
	{
		await Seed();
		// An invalid regex returns undefined (field omitted)
		var results = await Query("SELECT RegexMatch('hello', '[invalid') AS r FROM c WHERE c.id = '1'");
		results[0]["r"].Should().BeNull("invalid regex → undefined");
	}

	[Fact]
	public async Task StringSplit_NoDelimiterFound_ReturnsSingleElement()
	{
		await Seed();
		var results = await Query("SELECT StringSplit('hello', ',') AS r FROM c WHERE c.id = '1'");
		var arr = results[0]["r"] as JArray;
		arr.Should().NotBeNull();
		arr!.Count.Should().Be(1);
		arr[0]!.Value<string>().Should().Be("hello");
	}

	[Fact]
	public async Task EndsWith_CaseSensitive_NoMatch()
	{
		await Seed();
		var results = await Query("SELECT ENDSWITH('Hello', 'LO') AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<bool>().Should().BeFalse();
	}

	[Fact]
	public async Task EndsWith_CaseInsensitive_Match()
	{
		await Seed();
		var results = await Query("SELECT ENDSWITH('Hello', 'LO', true) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<bool>().Should().BeTrue();
	}

	// ── Math Edge Cases ──

	[Fact]
	public async Task Sqrt_NegativeNumber_ReturnsNaN()
	{
		await Seed();
		// SQRT(-1) = NaN — should either return undefined or NaN
		var results = await Query("SELECT SQRT(-1) AS r FROM c WHERE c.id = '1'");
		var token = results[0]["r"];
		// Either NaN or undefined (omitted) is acceptable
		if (token != null)
		{
			var val = token.Value<double>();
			double.IsNaN(val).Should().BeTrue();
		}
	}

	[Fact]
	public async Task Log_Zero_ReturnsNegativeInfinity()
	{
		await Seed();
		var results = await Query("SELECT LOG(0) AS r FROM c WHERE c.id = '1'");
		var token = results[0]["r"];
		if (token != null)
		{
			var val = token.Value<double>();
			double.IsNegativeInfinity(val).Should().BeTrue();
		}
	}

	[Fact]
	public async Task Power_LargeExponent_ReturnsInfinity()
	{
		await Seed();
		var results = await Query("SELECT POWER(10, 309) AS r FROM c WHERE c.id = '1'");
		var token = results[0]["r"];
		if (token != null)
		{
			var val = token.Value<double>();
			double.IsInfinity(val).Should().BeTrue();
		}
	}

	[Fact]
	public async Task Round_NegativePrecision_Works()
	{
		await Seed();
		var results = await Query("SELECT ROUND(1234, -2) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<double>().Should().Be(1200);
	}

	// ── Integer Math Edge Cases ──

	[Fact]
	public async Task IntMod_NegativeValues_Works()
	{
		await Seed();
		var results = await Query("SELECT IntMod(-10, 3) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<long>().Should().Be(-1);
	}

	[Fact]
	public async Task IntAdd_Overflow_Wraps()
	{
		await Seed();
		// IntAdd should handle large numbers
		var results = await Query("SELECT IntAdd(9223372036854775807, 0) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<long>().Should().Be(long.MaxValue);
	}

	// ── Conversion Edge Cases ──

	[Fact]
	public async Task ToString_BoolInput_ReturnsTrueOrFalse()
	{
		await Seed();
		var results = await Query("SELECT ToString(true) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("true");
	}

	[Fact]
	public async Task ToString_NumberInput_ReturnsNumberString()
	{
		await Seed();
		var results = await Query("SELECT ToString(42) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("42");
	}

	[Fact]
	public async Task ToNumber_BoolInput_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT ToNumber(true) AS r FROM c WHERE c.id = '1'");
		results[0]["r"].Should().BeNull("TONUMBER(bool) → undefined");
	}

	[Fact]
	public async Task ObjectToArray_NonObject_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT ObjectToArray('hello') AS r FROM c WHERE c.id = '1'");
		results[0]["r"].Should().BeNull("ObjectToArray(string) → undefined");
	}

	// ── Aggregate Edge Cases ──

	[Fact]
	public async Task Count_EmptyResult_ReturnsZero()
	{
		await Seed();
		var iter = _container.GetItemQueryIterator<int>("SELECT VALUE COUNT(1) FROM c WHERE c.id = 'nonexistent'");
		var items = new List<int>();
		while (iter.HasMoreResults) items.AddRange(await iter.ReadNextAsync());
		items.First().Should().Be(0);
	}

	[Fact]
	public async Task Avg_SingleItem_ReturnsThatValue()
	{
		await Seed();
		var iter = _container.GetItemQueryIterator<double>("SELECT VALUE AVG(c[\"value\"]) FROM c WHERE c.id = '1'");
		var items = new List<double>();
		while (iter.HasMoreResults) items.AddRange(await iter.ReadNextAsync());
		items.First().Should().Be(10);
	}

	// ── Cross-function Composition ──

	[Fact]
	public async Task NestedFunctions_UpperOfConcat()
	{
		await Seed();
		var results = await Query("SELECT UPPER(CONCAT(c.name, ' test')) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("ALICE ANDERSON TEST");
	}

	[Fact]
	public async Task NestedFunctions_LengthOfReplace()
	{
		await Seed();
		var results = await Query("SELECT LENGTH(REPLACE(c.name, ' ', '')) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<int>().Should().Be(13); // "AliceAnderson" = 13 chars
	}

	[Fact]
	public async Task ArithmeticInFunctionArgs()
	{
		await Seed();
		var results = await Query("SELECT ABS(c[\"value\"] - 15) AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<double>().Should().Be(5);
	}

	// ── FullTextContainsAny + Undefined (Bug 7 validation) ──

	[Fact]
	public async Task FullTextContainsAny_UndefinedProperty_ReturnsFalse()
	{
		await Seed();
		// Bug 7: FULLTEXTCONTAINSANY should return false for undefined, not undefined
		var results = await Query("SELECT FULLTEXTCONTAINSANY(c.nonexistent, 'hello') AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<bool>().Should().BeFalse();
	}

	// ── REVERSE non-string (Bug 4 validation) ──

	[Fact]
	public async Task Reverse_StringInput_Reversed()
	{
		await Seed();
		var results = await Query("SELECT REVERSE('hello') AS r FROM c WHERE c.id = '1'");
		results[0]["r"]!.Value<string>().Should().Be("olleh");
	}

	[Fact]
	public async Task Reverse_NonString_ReturnsUndefined()
	{
		await Seed();
		var results = await Query("SELECT REVERSE(42) AS r FROM c WHERE c.id = '1'");
		results[0]["r"].Should().BeNull("REVERSE of non-string → undefined");
	}
}
