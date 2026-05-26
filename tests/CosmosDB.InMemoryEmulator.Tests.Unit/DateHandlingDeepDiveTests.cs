using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive Tests
// ═══════════════════════════════════════════════════════════════════════════

public class DateHandlingDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task SeedOneItem()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));
	}

	private async Task<List<T>> Query<T>(string sql)
	{
		var iterator = _container.GetItemQueryIterator<T>(sql,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<T>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	private async Task<List<T>> QueryWithParams<T>(QueryDefinition query)
	{
		var iterator = _container.GetItemQueryIterator<T>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<T>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 1: Bug Fixes
	// ═════════════════════════════════════════════════════════════════════

	// BUG-A: GETCURRENTTICKS should return same value as GETCURRENTTICKSSTATIC within same query
	[Fact]
	public async Task GetCurrentTicks_MatchesGetCurrentTicksStatic()
	{
		await SeedOneItem();

		var results = await Query<JObject>(
			"SELECT GetCurrentTicks() as ticks, GetCurrentTicksStatic() as ticksStatic FROM c");

		var item = results.Single();
		item["ticks"]!.Value<long>().Should().Be(item["ticksStatic"]!.Value<long>());
	}

	// BUG-B: DATETIMEBIN invalid part should return undefined (omitted from SELECT VALUE)
	[Fact]
	public async Task DateTimeBin_InvalidPart_UndefinedOmittedFromSelectValue()
	{
		await SeedOneItem();

		var results = await Query<JToken>(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:00Z', 'invalid', 1) FROM c");

		results.Should().BeEmpty("invalid part should return undefined, omitted from SELECT VALUE");
	}

	// BUG-E: DATETIMEBIN non-string origin should return undefined
	[Fact]
	public async Task DateTimeBin_NumericOrigin_ReturnsUndefined()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", epoch = 0 }),
			new PartitionKey("a"));

		var results = await Query<JToken>(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:00Z', 'hh', 1, c.epoch) FROM c");

		results.Should().BeEmpty("non-string origin should return undefined");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 2: DATETIMEPART Weekday
	// ═════════════════════════════════════════════════════════════════════

	[Theory]
	[InlineData("2023-01-01T00:00:00Z", 1)]  // Sunday
	[InlineData("2023-01-02T00:00:00Z", 2)]  // Monday
	[InlineData("2023-01-04T00:00:00Z", 4)]  // Wednesday
	[InlineData("2023-01-07T00:00:00Z", 7)]  // Saturday
	public async Task DateTimePart_Weekday_Returns1Through7(string dt, long expected)
	{
		await SeedOneItem();
		var results = await Query<long>(
			$"SELECT VALUE DateTimePart('weekday', '{dt}') FROM c");
		results.Single().Should().Be(expected);
	}

	[Theory]
	[InlineData("weekday")]
	[InlineData("dw")]
	[InlineData("w")]
	public async Task DateTimePart_WeekdayAliases_AllWork(string alias)
	{
		await SeedOneItem();
		var results = await Query<long>(
			$"SELECT VALUE DateTimePart('{alias}', '2023-01-01T00:00:00Z') FROM c");
		results.Single().Should().Be(1); // Sunday = 1
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 3: DATETIMEADD Edge Cases
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task DateTimeAdd_OverflowYear9999_ReturnsUndefined()
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			"SELECT VALUE DateTimeAdd('yyyy', 1, '9999-12-31T00:00:00Z') FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task DateTimeAdd_UnderflowYear1_ReturnsUndefined()
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			"SELECT VALUE DateTimeAdd('yyyy', -1, '0001-01-01T00:00:00Z') FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task DateTimeAdd_24Months_CorrectResult()
	{
		await SeedOneItem();
		var results = await Query<string>(
			"SELECT VALUE DateTimeAdd('mm', 24, '2020-01-15T10:30:00.0000000Z') FROM c");
		results.Single().Should().Be("2022-01-15T10:30:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_ParameterizedQuery_WithDateValue()
	{
		await SeedOneItem();
		var query = new QueryDefinition(
			"SELECT VALUE DateTimeAdd('dd', 5, @dt) FROM c")
			.WithParameter("@dt", "2020-01-15T00:00:00.0000000Z");

		var results = await QueryWithParams<string>(query);
		results.Single().Should().Be("2020-01-20T00:00:00.0000000Z");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 4: DATETIMEDIFF Edge Cases
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task DateTimeDiff_PastSeconds_ReturnsNegative()
	{
		await SeedOneItem();
		var results = await Query<long>(
			"SELECT VALUE DateTimeDiff('ss', '2021-01-01T00:00:00Z', '2020-01-01T00:00:00Z') FROM c");
		results.Single().Should().BeLessThan(0);
	}

	[Fact]
	public async Task DateTimeDiff_FutureSeconds_ReturnsPositive()
	{
		await SeedOneItem();
		var results = await Query<long>(
			"SELECT VALUE DateTimeDiff('ss', '2020-01-01T00:00:00Z', '2021-01-01T00:00:00Z') FROM c");
		results.Single().Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task DateTimeDiff_WeekdayPart_ReturnsUndefined()
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			"SELECT VALUE DateTimeDiff('weekday', '2020-01-01T00:00:00Z', '2020-01-02T00:00:00Z') FROM c");
		results.Should().BeEmpty("weekday is not supported in DateTimeDiff");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 5: DATETIMEFROMPARTS Boundary Tests
	// ═════════════════════════════════════════════════════════════════════

	[Theory]
	[InlineData("2020, 0, 15")]        // Month = 0
	[InlineData("2020, 1, 0")]         // Day = 0
	[InlineData("2020, 1, 1, 24")]     // Hour > 23
	[InlineData("2020, 1, 1, 0, 60")]  // Minute > 59
	[InlineData("2020, 1, 1, 0, 0, 60")] // Second > 59
	public async Task DateTimeFromParts_OutOfRangeArgs_ReturnsUndefined(string args)
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			$"SELECT VALUE DateTimeFromParts({args}) FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task DateTimeFromParts_NegativeFraction_ReturnsUndefined()
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			"SELECT VALUE DateTimeFromParts(2020, 1, 1, 0, 0, 0, -1) FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task DateTimeFromParts_FractionOver9999999_ReturnsUndefined()
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			"SELECT VALUE DateTimeFromParts(2020, 1, 1, 0, 0, 0, 10000000) FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task DateTimeFromParts_ExtraArgsIgnored()
	{
		await SeedOneItem();
		// 8 args — the 8th should be ignored
		var results = await Query<string>(
			"SELECT VALUE DateTimeFromParts(2020, 6, 15, 10, 30, 45, 0, 999) FROM c");
		results.Single().Should().Be("2020-06-15T10:30:45.0000000Z");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 6: DATETIMEBIN Extended
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task DateTimeBin_OriginEqualsInput_ReturnsSameValue()
	{
		await SeedOneItem();
		var results = await Query<string>(
			"SELECT VALUE DateTimeBin('2021-01-01T00:00:00.0000000Z', 'dd', 7, '2021-01-01T00:00:00.0000000Z') FROM c");
		results.Single().Should().Be("2021-01-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_AcrossYearBoundary()
	{
		await SeedOneItem();
		var results = await Query<string>(
			"SELECT VALUE DateTimeBin('2021-06-15T00:00:00.0000000Z', 'yyyy', 2) FROM c");
		results.Single().Should().NotBeNullOrEmpty();
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 7: Conversion Edge Cases
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task TicksToDateTime_NegativeTicks_ReturnsUndefined()
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			"SELECT VALUE TicksToDateTime(-1) FROM c");
		results.Should().BeEmpty("negative ticks are before DateTime.MinValue");
	}

	[Fact]
	public async Task TimestampToDateTime_NonNumericArg_ReturnsUndefined()
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			"SELECT VALUE TimestampToDateTime('not-a-number') FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task TicksToDateTime_NonNumericArg_ReturnsUndefined()
	{
		await SeedOneItem();
		var results = await Query<JToken>(
			"SELECT VALUE TicksToDateTime('abc') FROM c");
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task DateTimeToTicks_NoZSuffix_VerifyUtcAssumption()
	{
		await SeedOneItem();
		var results = await Query<long>(
			"SELECT VALUE DateTimeToTicks('2020-01-01T00:00:00') FROM c");
		// Should assume UTC
		var expected = new System.DateTime(2020, 1, 1, 0, 0, 0, System.DateTimeKind.Utc).Ticks;
		results.Single().Should().Be(expected);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 8: GetCurrent* Static Parameter Injection
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task GetCurrentDateTime_IsConsistentWithinQuery()
	{
		await SeedOneItem();

		var results = await Query<JObject>(
			"SELECT GetCurrentDateTime() as dt, GetCurrentDateTimeStatic() as dtStatic FROM c");

		var item = results.Single();
		item["dt"]!.Value<string>().Should().Be(item["dtStatic"]!.Value<string>());
	}

	[Fact]
	public async Task GetCurrentTimestamp_IsConsistentWithinQuery()
	{
		await SeedOneItem();

		var results = await Query<JObject>(
			"SELECT GetCurrentTimestamp() as ts, GetCurrentTimestampStatic() as tsStatic FROM c");

		var item = results.Single();
		item["ts"]!.Value<long>().Should().Be(item["tsStatic"]!.Value<long>());
	}

	[Fact]
	public async Task GetCurrentTicksStatic_ReturnsPositiveValue()
	{
		await SeedOneItem();

		var results = await Query<long>(
			"SELECT VALUE GetCurrentTicksStatic() FROM c");

		results.Single().Should().BeGreaterThan(0);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 9: Integration / Composition
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task GroupBy_DateTimePart_GroupsByYear()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-06-15T00:00:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", ts = "2020-11-01T00:00:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", ts = "2021-03-01T00:00:00Z" }),
			new PartitionKey("a"));

		var results = await Query<JObject>(
			"SELECT DateTimePart('yyyy', c.ts) as yr, COUNT(1) as cnt FROM c GROUP BY DateTimePart('yyyy', c.ts)");

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task OrderBy_DateTimePart_OrdersByResult()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2021-03-15T00:00:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", ts = "2020-06-15T00:00:00Z" }),
			new PartitionKey("a"));

		var results = await Query<JObject>(
			"SELECT c.id FROM c ORDER BY DateTimePart('yyyy', c.ts)");

		results[0]["id"]!.Value<string>().Should().Be("2");
		results[1]["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task ThreeFunctionRoundTrip_DateTimeFromParts_ToTicks_ToDateTime()
	{
		await SeedOneItem();
		var results = await Query<string>(
			"SELECT VALUE TicksToDateTime(DateTimeToTicks(DateTimeFromParts(2023, 6, 15, 10, 30, 45, 1234567))) FROM c");
		results.Single().Should().Be("2023-06-15T10:30:45.1234567Z");
	}

	[Fact]
	public async Task DateTimeBin_InWhereClause()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2021-01-08T10:00:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", ts = "2021-01-15T10:00:00Z" }),
			new PartitionKey("a"));

		var results = await Query<JObject>(
			"SELECT c.id FROM c WHERE DateTimeBin(c.ts, 'dd', 7) = '2021-01-07T00:00:00.0000000Z'");

		results.Should().Contain(r => r["id"]!.Value<string>() == "1");
	}

	[Fact]
	public async Task Distinct_WithDateFunctionResults()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-06-15T10:30:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", ts = "2020-11-20T14:00:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", ts = "2020-03-01T08:00:00Z" }),
			new PartitionKey("a"));

		var results = await Query<long>(
			"SELECT DISTINCT VALUE DateTimePart('yyyy', c.ts) FROM c");

		results.Should().HaveCount(1);
		results.Single().Should().Be(2020);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  Phase 10: Cross-Platform / Format Edge Cases
	// ═════════════════════════════════════════════════════════════════════

	[Theory]
	[InlineData("2020-01-15T10:30:00", "2020-01-15T10:30:00.0000000Z")]       // No Z suffix
	[InlineData("2020-01-15T10:30:00.0000000Z", "2020-01-15T10:30:00.0000000Z")] // With Z
	public async Task DateTimeAdd_VariousInputFormats_ProducesConsistentOutput(string input, string expected)
	{
		await SeedOneItem();
		var results = await Query<string>(
			$"SELECT VALUE DateTimeAdd('dd', 0, '{input}') FROM c");
		results.Single().Should().Be(expected);
	}

	[Fact]
	public async Task DateTimeAdd_DateOnlyInput_ProducesFullDateTime()
	{
		await SeedOneItem();
		var results = await Query<string>(
			"SELECT VALUE DateTimeAdd('dd', 0, '2020-01-15') FROM c");
		results.Single().Should().Be("2020-01-15T00:00:00.0000000Z");
	}
}
