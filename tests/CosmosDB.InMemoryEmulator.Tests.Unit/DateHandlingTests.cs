using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class DateDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("partitionKey")]
	public string PartitionKey { get; set; } = default!;

	[JsonProperty("eventDate")]
	public DateTimeOffset EventDate { get; set; }
}

public class MetadataDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("partitionKey")]
	public string PartitionKey { get; set; } = default!;

	[JsonProperty("metadata")]
	public Dictionary<string, object> Metadata { get; set; } = default!;
}

public class DateHandlingTests
{
	private readonly InMemoryContainer _container = new("test-container", "/partitionKey");

	[Fact]
	public async Task DateTimeOffset_RoundTrips_WithOriginalOffset()
	{
		var original = new DateTimeOffset(2026, 3, 29, 16, 55, 46, TimeSpan.Zero);
		var document = new DateDocument
		{
			Id = "date-test-1",
			PartitionKey = "pk1",
			EventDate = original
		};

		await _container.CreateItemAsync(document, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<DateDocument>("date-test-1", new PartitionKey("pk1"));

		response.Resource.EventDate.Should().Be(original);
	}

	[Fact]
	public async Task DateTimeOffset_RoundTrips_WithPositiveOffset()
	{
		var original = new DateTimeOffset(2026, 3, 29, 17, 55, 46, TimeSpan.FromHours(1));
		var document = new DateDocument
		{
			Id = "date-test-2",
			PartitionKey = "pk1",
			EventDate = original
		};

		await _container.CreateItemAsync(document, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<DateDocument>("date-test-2", new PartitionKey("pk1"));

		response.Resource.EventDate.Should().Be(original);
	}

	[Fact]
	public async Task DateTimeOffset_PreservedInQuery()
	{
		var original = new DateTimeOffset(2026, 3, 29, 16, 55, 46, TimeSpan.Zero);
		var document = new DateDocument
		{
			Id = "date-test-3",
			PartitionKey = "pk1",
			EventDate = original
		};

		await _container.CreateItemAsync(document, new PartitionKey("pk1"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.id = 'date-test-3'");
		var iterator = _container.GetItemQueryIterator<DateDocument>(query);
		var results = await iterator.ReadNextAsync();

		results.Resource.Should().ContainSingle()
			.Which.EventDate.Should().Be(original);
	}

	[Fact]
	public async Task DateTimeOffset_InDictionary_PreservedAsString()
	{
		var original = new DateTimeOffset(2026, 3, 29, 16, 55, 46, TimeSpan.Zero);
		var document = new MetadataDocument
		{
			Id = "date-test-4",
			PartitionKey = "pk1",
			Metadata = new()
			{
				["deceasedDate"] = original
			}
		};

		await _container.CreateItemAsync(document, new PartitionKey("pk1"));

		var response = await _container.ReadItemAsync<MetadataDocument>("date-test-4", new PartitionKey("pk1"));

		var returnedValue = response.Resource.Metadata["deceasedDate"].ToString()!;
		var parsed = DateTimeOffset.Parse(returnedValue);
		parsed.Should().Be(original);
	}

	[Fact]
	public async Task DateTime_Utc_InDictionary_RoundTripsViaStreamRead()
	{
		var utcDate = new DateTime(2026, 3, 29, 19, 52, 37, DateTimeKind.Utc);
		var document = new MetadataDocument
		{
			Id = "date-test-stream",
			PartitionKey = "pk1",
			Metadata = new()
			{
				["deceasedDate"] = utcDate
			}
		};

		await _container.CreateItemAsync(document, new PartitionKey("pk1"));

		// Simulate the framework path: ReadItemStreamAsync → manual deserialization
		var streamResponse = await _container.ReadItemStreamAsync("date-test-stream", new PartitionKey("pk1"));
		using var reader = new System.IO.StreamReader(streamResponse.Content);
		var json = await reader.ReadToEndAsync();

		// Deserialize with default Newtonsoft settings (what the framework does)
		var deserialized = JsonConvert.DeserializeObject<MetadataDocument>(json);
		var dateValue = deserialized!.Metadata["deceasedDate"];

		// Check what type and value we get
		var actualType = dateValue.GetType().Name;
		if (dateValue is DateTime dt)
		{
			dt.Kind.Should().Be(DateTimeKind.Utc, $"DateTime should remain UTC (actual: {dt}, kind: {dt.Kind}, type: {actualType})");
			dt.Should().Be(utcDate);
		}
		else
		{
			// If it's a string (from DateParseHandling.None), verify it parses correctly
			var parsed = DateTime.Parse(dateValue.ToString()!).ToUniversalTime();
			parsed.Should().Be(utcDate, $"Value was {actualType}: {dateValue}");
		}
	}
}

// ─── DateTimeDiff ────────────────────────────────────────────────────────

public class DateTimeDiffTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Theory]
	[InlineData("day", "2020-01-01T00:00:00Z", "2020-01-10T00:00:00Z", 9)]
	[InlineData("hour", "2020-01-01T00:00:00Z", "2020-01-01T05:00:00Z", 5)]
	[InlineData("minute", "2020-01-01T00:00:00Z", "2020-01-01T00:30:00Z", 30)]
	[InlineData("second", "2020-01-01T00:00:00Z", "2020-01-01T00:00:45Z", 45)]
	[InlineData("millisecond", "2020-01-01T00:00:00.000Z", "2020-01-01T00:00:00.500Z", 500)]
	[InlineData("year", "2020-01-15T00:00:00Z", "2023-06-15T00:00:00Z", 3)]
	[InlineData("month", "2020-01-15T00:00:00Z", "2020-04-15T00:00:00Z", 3)]
	public async Task DateTimeDiff_ReturnsCorrectDifference(string part, string start, string end, long expected)
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", startDate = start, endDate = end }),
			new PartitionKey("a"));

		var query = new QueryDefinition(
			$"SELECT VALUE DateTimeDiff('{part}', c.startDate, c.endDate) FROM c");

		var iterator = _container.GetItemQueryIterator<long>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(expected);
	}

	[Fact]
	public async Task DateTimeDiff_NegativeDiff_ReturnsNegative()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", startDate = "2020-01-10T00:00:00Z", endDate = "2020-01-01T00:00:00Z" }),
			new PartitionKey("a"));

		var query = new QueryDefinition(
			"SELECT VALUE DateTimeDiff('day', c.startDate, c.endDate) FROM c");

		var iterator = _container.GetItemQueryIterator<long>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(-9);
	}
}

// ─── DateTimeFromParts ──────────────────────────────────────────────────

public class DateTimeFromPartsTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task DateTimeFromParts_ReturnsFormattedDateTime()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var query = new QueryDefinition(
			"SELECT VALUE DateTimeFromParts(2020, 3, 15, 10, 30, 0, 0) FROM c");

		var iterator = _container.GetItemQueryIterator<string>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("2020-03-15T10:30:00.0000000Z");
	}
}

// ─── DateTimeBin ────────────────────────────────────────────────────────

public class DateTimeBinTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task DateTimeBin_BinsByHour()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-06-15T10:35:22.1234567Z" }),
			new PartitionKey("a"));

		// DateTimeBin(dateTime, datePart, binSize [, origin])
		// Bin by 1 hour: 10:35 → 10:00
		var query = new QueryDefinition(
			"SELECT VALUE DateTimeBin(c.ts, 'hh', 1) FROM c");

		var iterator = _container.GetItemQueryIterator<string>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("2020-06-15T10:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_BinsByDay()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-06-15T10:35:22Z" }),
			new PartitionKey("a"));

		// Bin by 1 day: keeps date, zeros time
		var query = new QueryDefinition(
			"SELECT VALUE DateTimeBin(c.ts, 'dd', 1) FROM c");

		var iterator = _container.GetItemQueryIterator<string>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("2020-06-15T00:00:00.0000000Z");
	}
}

// ─── DateTime/Ticks Conversion Functions ────────────────────────────────

public class DateTimeTicksConversionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task GetCurrentTicks_ReturnsReasonableValue()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var query = new QueryDefinition("SELECT VALUE GetCurrentTicks() FROM c");

		var iterator = _container.GetItemQueryIterator<long>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Ticks for 2020-01-01 = 637134336000000000, should be well above that
		results.Should().ContainSingle().Which.Should().BeGreaterThan(637134336000000000);
	}

	[Fact]
	public async Task DateTimeToTicks_ConvertsCorrectly()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-01-01T00:00:00.0000000Z" }),
			new PartitionKey("a"));

		var query = new QueryDefinition("SELECT VALUE DateTimeToTicks(c.ts) FROM c");

		var iterator = _container.GetItemQueryIterator<long>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(637134336000000000);
	}

	[Fact]
	public async Task TicksToDateTime_ConvertsCorrectly()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ticks = 637134336000000000 }),
			new PartitionKey("a"));

		var query = new QueryDefinition("SELECT VALUE TicksToDateTime(c.ticks) FROM c");

		var iterator = _container.GetItemQueryIterator<string>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("2020-01-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeToTimestamp_ReturnsUnixMillis()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-01-01T00:00:00.0000000Z" }),
			new PartitionKey("a"));

		var query = new QueryDefinition("SELECT VALUE DateTimeToTimestamp(c.ts) FROM c");

		var iterator = _container.GetItemQueryIterator<long>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// 2020-01-01T00:00:00Z = 1577836800000 ms since epoch
		results.Should().ContainSingle().Which.Should().Be(1577836800000);
	}

	[Fact]
	public async Task TimestampToDateTime_ConvertsFromUnixMillis()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ms = 1577836800000 }),
			new PartitionKey("a"));

		var query = new QueryDefinition("SELECT VALUE TimestampToDateTime(c.ms) FROM c");

		var iterator = _container.GetItemQueryIterator<string>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be("2020-01-01T00:00:00.0000000Z");
	}
}

// ─── Static DateTime Functions ──────────────────────────────────────────

public class StaticDateTimeFunctionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task GetCurrentDateTimeStatic_ReturnsSameValueForAllItems()
	{
		for (var i = 0; i < 3; i++)
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a" }),
				new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<string>(
			"SELECT VALUE GetCurrentDateTimeStatic() FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		// All three values should be identical (static = same for entire query)
		results.Distinct().Should().ContainSingle();
	}

	[Fact]
	public async Task GetCurrentTicksStatic_ReturnsSameValueForAllItems()
	{
		for (var i = 0; i < 3; i++)
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a" }),
				new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE GetCurrentTicksStatic() FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		results.Distinct().Should().ContainSingle();
	}

	[Fact]
	public async Task GetCurrentTimestampStatic_ReturnsSameValueForAllItems()
	{
		for (var i = 0; i < 3; i++)
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a" }),
				new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE GetCurrentTimestampStatic() FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		results.Distinct().Should().ContainSingle();
	}
}

// ─── DateTimeBin Year/Month Support ─────────────────────────────────────

public class DateTimeBinYearMonthTests
{
	[Fact]
	public async Task DateTimeBin_Year_BinsToYearBoundary()
	{
		var container = new InMemoryContainer("test-container", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-07-15T10:30:00.0000000Z"}""")),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<string>(
			"SELECT VALUE DateTimeBin(c.dt, 'year', 1) FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Binning to 1 year => 2023-01-01T00:00:00.0000000Z
		results.Should().ContainSingle().Which.Should().Be("2023-01-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_Month_BinsToMonthBoundary()
	{
		var container = new InMemoryContainer("test-container", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-07-15T10:30:00.0000000Z"}""")),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<string>(
			"SELECT VALUE DateTimeBin(c.dt, 'month', 1) FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Binning to 1 month => 2023-07-01T00:00:00.0000000Z
		results.Should().ContainSingle().Which.Should().Be("2023-07-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_Quarter_BinsTo3MonthBoundary()
	{
		var container = new InMemoryContainer("test-container", "/pk");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a","dt":"2023-08-15T10:30:00.0000000Z"}""")),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<string>(
			"SELECT VALUE DateTimeBin(c.dt, 'month', 3) FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Binning to 3 months from origin 1970-01-01 =>
		// Aug 2023 is in the Q3 2023 bin starting at 2023-07-01
		results.Should().ContainSingle().Which.Should().Be("2023-07-01T00:00:00.0000000Z");
	}
}

// ─── DateTimeAdd Tests ──────────────────────────────────────────────────

public class DateTimeAddTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	private async Task<T> QuerySingleValue<T>(string sql)
	{
		await EnsureSeedItem();
		var iterator = _container.GetItemQueryIterator<T>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<T>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	private async Task EnsureSeedItem()
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }
	}

	[Fact]
	public async Task DateTimeAdd_Year_AddsCorrectly()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('yyyy', 1, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2021-07-03T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_Month_AddsCorrectly()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('mm', 1, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2020-08-03T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_Day_AddsCorrectly()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('dd', 1, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2020-07-04T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_Hour_AddsCorrectly()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('hh', 1, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2020-07-03T01:00:00.0000000Z");
	}

	[Theory]
	[InlineData("minute", 30)]
	[InlineData("mi", 30)]
	[InlineData("n", 30)]
	public async Task DateTimeAdd_Minute_AllAliases(string alias, int amount)
	{
		var result = await QuerySingleValue<string>(
			$"SELECT VALUE DateTimeAdd('{alias}', {amount}, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2020-07-03T00:30:00.0000000Z");
	}

	[Theory]
	[InlineData("second")]
	[InlineData("ss")]
	[InlineData("s")]
	public async Task DateTimeAdd_Second_AllAliases(string alias)
	{
		var result = await QuerySingleValue<string>(
			$"SELECT VALUE DateTimeAdd('{alias}', 45, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2020-07-03T00:00:45.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_Millisecond_AddsCorrectly()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('ms', 500, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2020-07-03T00:00:00.5000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_NegativeValue_Subtracts()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('yyyy', -1, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2019-07-03T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_NegativeSubtractExpression_MatchesCosmosDoc()
	{
		// Cosmos docs example: DATETIMEADD("ss", 5 * -5, "2020-07-03T00:00:00.0000000")
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('ss', -25, '2020-07-03T00:00:00.0000000') FROM c");
		result.Should().Be("2020-07-02T23:59:35.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_LeapYear_Jan31PlusOneMonth_NonLeap()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('mm', 1, '2023-01-31T00:00:00.0000000') FROM c");
		result.Should().Be("2023-02-28T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_LeapYear_Jan31PlusOneMonth_LeapYear()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('mm', 1, '2024-01-31T00:00:00.0000000') FROM c");
		result.Should().Be("2024-02-29T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_NullDate_ReturnsUndefined()
	{
		await _container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"n1","pk":"a","dt":null}""")),
			new PartitionKey("a"));

		// DateTimeAdd with null input returns undefined which is omitted from SELECT VALUE
		var iterator = _container.GetItemQueryIterator<JToken>(
			"SELECT VALUE DateTimeAdd('dd', 1, c.dt) FROM c WHERE c.id = 'n1'",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Undefined is omitted from SELECT VALUE projection
		results.Should().BeEmpty();
	}

	[Fact]
	public async Task DateTimeAdd_Microsecond_AddsCorrectly()
	{
		// 1 microsecond = 10 ticks = 0.0000010 seconds; 500 µs = 5000 ticks = 0.0005000s
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('mcs', 500, '2020-01-01T00:00:00.0000000') FROM c");
		result.Should().Be("2020-01-01T00:00:00.0005000Z");
	}

	[Fact]
	public async Task DateTimeAdd_Nanosecond_AddsCorrectly()
	{
		// 1 nanosecond = 0.01 ticks; .NET rounds to 100ns resolution
		// 500 nanoseconds = 5 ticks = 0.0000005 seconds
		var result = await QuerySingleValue<string>(
			"SELECT VALUE DateTimeAdd('ns', 500, '2020-01-01T00:00:00.0000000') FROM c");
		result.Should().Be("2020-01-01T00:00:00.0000005Z");
	}

	[Theory]
	[InlineData("year")]
	[InlineData("yyyy")]
	[InlineData("yy")]
	public async Task DateTimeAdd_YearAliases_AllWork(string alias)
	{
		var result = await QuerySingleValue<string>(
			$"SELECT VALUE DateTimeAdd('{alias}', 1, '2020-01-01T00:00:00.0000000') FROM c");
		result.Should().Be("2021-01-01T00:00:00.0000000Z");
	}

	[Theory]
	[InlineData("month")]
	[InlineData("mm")]
	[InlineData("m")]
	public async Task DateTimeAdd_MonthAliases_AllWork(string alias)
	{
		var result = await QuerySingleValue<string>(
			$"SELECT VALUE DateTimeAdd('{alias}', 1, '2020-01-01T00:00:00.0000000') FROM c");
		result.Should().Be("2020-02-01T00:00:00.0000000Z");
	}
}

// ─── DateTimePart Tests ─────────────────────────────────────────────────

public class DateTimePartTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	private async Task<long> QuerySingleLong(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<long>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	// Cosmos DB docs example: DATETIMEPART on "2016-05-29T08:30:00.1301617"
	[Theory]
	[InlineData("yyyy", 2016)]
	[InlineData("mm", 5)]
	[InlineData("dd", 29)]
	[InlineData("hh", 8)]
	[InlineData("mi", 30)]
	[InlineData("ss", 0)]
	[InlineData("ms", 130)]
	public async Task DateTimePart_ExtractsAllParts(string part, long expected)
	{
		var result = await QuerySingleLong(
			$"SELECT VALUE DateTimePart('{part}', '2016-05-29T08:30:00.1301617') FROM c");
		result.Should().Be(expected);
	}

	[Fact]
	public async Task DateTimePart_Microsecond_ExtractsCorrectly()
	{
		// Cosmos docs: DateTimePart("mcs", "2016-05-29T08:30:00.1301617") → 130161
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimePart('mcs', '2016-05-29T08:30:00.1301617') FROM c");
		result.Should().Be(130161);
	}

	[Fact]
	public async Task DateTimePart_Nanosecond_ExtractsCorrectly()
	{
		// Cosmos docs: DateTimePart("ns", "2016-05-29T08:30:00.1301617") → 130161700
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimePart('ns', '2016-05-29T08:30:00.1301617') FROM c");
		result.Should().Be(130161700);
	}

	[Theory]
	[InlineData("year")]
	[InlineData("yyyy")]
	[InlineData("yy")]
	public async Task DateTimePart_YearAliases_AllWork(string alias)
	{
		var result = await QuerySingleLong(
			$"SELECT VALUE DateTimePart('{alias}', '2020-06-15T00:00:00.0000000') FROM c");
		result.Should().Be(2020);
	}

	[Theory]
	[InlineData("month")]
	[InlineData("mm")]
	[InlineData("m")]
	public async Task DateTimePart_MonthAliases_AllWork(string alias)
	{
		var result = await QuerySingleLong(
			$"SELECT VALUE DateTimePart('{alias}', '2020-06-15T00:00:00.0000000') FROM c");
		result.Should().Be(6);
	}
}

// ─── DateTimeBin Extended Tests ─────────────────────────────────────────

public class DateTimeBinExtendedTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	private async Task<string> QuerySingleString(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<string>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	[Fact]
	public async Task DateTimeBin_7DayBins_DefaultOriginUnixEpoch()
	{
		// Cosmos DB docs example: DATETIMEBIN("2021-01-08T18:35:00.0000000", "dd", 7) → "2021-01-07T00:00:00.0000000Z"
		// 7-day bins from Unix epoch 1970-01-01
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:00.0000000', 'dd', 7) FROM c");
		result.Should().Be("2021-01-07T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_7DayBins_CustomWindowsEpochOrigin()
	{
		// Cosmos DB docs example with Windows epoch origin
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:00.0000000', 'dd', 7, '1601-01-01T00:00:00.0000000') FROM c");
		result.Should().Be("2021-01-04T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_Month_WithMAlias_BinsCorrectly()
	{
		// BUG-2 regression: "m" alias should bin by month, not return unchanged
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2023-07-15T10:30:00.0000000', 'm', 1) FROM c");
		result.Should().Be("2023-07-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_5Hour_BinsCorrectly()
	{
		// Cosmos DB docs: DATETIMEBIN("2021-01-08T18:35:00.0000000", "hh", 5) → "2021-01-08T15:00:00.0000000Z"
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:00.0000000', 'hh', 5) FROM c");
		result.Should().Be("2021-01-08T15:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_Minute_15MinBins()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-01-08T18:37:00.0000000', 'mi', 15) FROM c");
		result.Should().Be("2021-01-08T18:30:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_Second_30SecBins()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:42.0000000', 'ss', 30) FROM c");
		result.Should().Be("2021-01-08T18:35:30.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_BinDay_DefaultBinSize()
	{
		// Cosmos DB docs: DATETIMEBIN("2021-01-08T18:35:00.0000000", "dd") → "2021-01-08T00:00:00.0000000Z"
		// binSize defaults to 1
		// Note: Our parser passes binSize from args, and docs say default is 1.
		// This tests the explicit binSize=1 case which is equivalent.
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:00.0000000', 'dd', 1) FROM c");
		result.Should().Be("2021-01-08T00:00:00.0000000Z");
	}
}

// ─── DateTimeFromParts Extended Tests ───────────────────────────────────

public class DateTimeFromPartsExtendedTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	private async Task<string?> QuerySingleStringOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JToken>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JToken>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		var token = results.Single();
		return token.Type == JTokenType.Null ? null : token.ToString();
	}

	[Fact]
	public async Task DateTimeFromParts_MinArgs_YearMonthDay()
	{
		// BUG-3: Cosmos docs: DATETIMEFROMPARTS(2017, 4, 20) → "2017-04-20T00:00:00.0000000Z"
		var result = await QuerySingleStringOrNull(
			"SELECT VALUE DateTimeFromParts(2017, 4, 20) FROM c");
		result.Should().Be("2017-04-20T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeFromParts_PartialArgs_5()
	{
		// BUG-3: DATETIMEFROMPARTS(2017, 4, 20, 13, 15) → "2017-04-20T13:15:00.0000000Z"
		var result = await QuerySingleStringOrNull(
			"SELECT VALUE DateTimeFromParts(2017, 4, 20, 13, 15) FROM c");
		result.Should().Be("2017-04-20T13:15:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeFromParts_AllArgs_WithSubSecondFraction()
	{
		// BUG-4: DATETIMEFROMPARTS(2017, 4, 20, 13, 15, 20, 3456789) → "2017-04-20T13:15:20.3456789Z"
		// 7th arg is sub-second fraction in 100ns ticks, not milliseconds
		var result = await QuerySingleStringOrNull(
			"SELECT VALUE DateTimeFromParts(2017, 4, 20, 13, 15, 20, 3456789) FROM c");
		result.Should().Be("2017-04-20T13:15:20.3456789Z");
	}

	[Fact]
	public async Task DateTimeFromParts_ZeroFraction()
	{
		var result = await QuerySingleStringOrNull(
			"SELECT VALUE DateTimeFromParts(2020, 1, 1, 0, 0, 0, 0) FROM c");
		result.Should().Be("2020-01-01T00:00:00.0000000Z");
	}
}

// ─── DateTimeDiff Extended Tests ────────────────────────────────────────

public class DateTimeDiffExtendedTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	private async Task<long> QuerySingleLong(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<long>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	[Fact]
	public async Task DateTimeDiff_DocsExample_PastYears()
	{
		// Cosmos docs: DATETIMEDIFF("yyyy", "2019-02-04T16:00:00.0000000", "2018-03-05T05:00:00.0000000") → -1
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('yyyy', '2019-02-04T16:00:00.0000000', '2018-03-05T05:00:00.0000000') FROM c");
		result.Should().Be(-1);
	}

	[Fact]
	public async Task DateTimeDiff_DocsExample_PastMonths()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('mm', '2019-02-04T16:00:00.0000000', '2018-03-05T05:00:00.0000000') FROM c");
		result.Should().Be(-11);
	}

	[Fact]
	public async Task DateTimeDiff_DocsExample_FutureDays()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('dd', '2018-03-05T05:00:00.0000000', '2019-02-04T16:00:00.0000000') FROM c");
		result.Should().Be(336);
	}

	[Fact]
	public async Task DateTimeDiff_DocsExample_FutureHours()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('hh', '2018-03-05T05:00:00.0000000', '2019-02-04T16:00:00.0000000') FROM c");
		result.Should().Be(8075);
	}

	[Theory]
	[InlineData("year", "yyyy")]
	[InlineData("year", "yy")]
	[InlineData("month", "mm")]
	[InlineData("month", "m")]
	[InlineData("day", "dd")]
	[InlineData("day", "d")]
	[InlineData("hour", "hh")]
	[InlineData("minute", "mi")]
	[InlineData("minute", "n")]
	[InlineData("second", "ss")]
	[InlineData("second", "s")]
	[InlineData("millisecond", "ms")]
	public async Task DateTimeDiff_Aliases_ProduceSameResult(string longAlias, string shortAlias)
	{
		var resultLong = await QuerySingleLong(
			$"SELECT VALUE DateTimeDiff('{longAlias}', '2020-01-01T00:00:00.0000000', '2020-06-15T12:30:00.0000000') FROM c");
		var resultShort = await QuerySingleLong(
			$"SELECT VALUE DateTimeDiff('{shortAlias}', '2020-01-01T00:00:00.0000000', '2020-06-15T12:30:00.0000000') FROM c");
		resultLong.Should().Be(resultShort);
	}

	[Fact]
	public async Task DateTimeDiff_Microsecond_CalculatesCorrectly()
	{
		// 500ms = 500000 microseconds
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('mcs', '2020-01-01T00:00:00.0000000', '2020-01-01T00:00:00.5000000') FROM c");
		result.Should().Be(500000);
	}

	[Fact]
	public async Task DateTimeDiff_Nanosecond_CalculatesCorrectly()
	{
		// 500ms = 500000000 nanoseconds (limited by 100ns tick precision)
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('ns', '2020-01-01T00:00:00.0000000', '2020-01-01T00:00:00.5000000') FROM c");
		result.Should().Be(500000000);
	}

	[Fact]
	public async Task DateTimeDiff_Month_AcrossLeapYear()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('mm', '2024-01-15T00:00:00.0000000', '2024-03-15T00:00:00.0000000') FROM c");
		result.Should().Be(2);
	}
}

// ─── DateTimeDiff Boundary-Crossing Divergence ──────────────────────────

public class DateTimeDiffBoundaryCrossingTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	// ── BUG-5: DateTimeDiff uses interval truncation for sub-day parts ──
	// Real Cosmos DB counts boundary crossings: 23:59→00:01 crosses the hour
	// boundary once, so DATETIMEDIFF('hour',...) = 1.
	// Our emulator uses (long)TotalHours which truncates the 0.033h interval to 0.
	// Implementing boundary-crossing semantics for all sub-day parts is complex
	// and the interval-based approach matches for all whole-unit intervals.
	[Fact]
	public async Task DateTimeDiff_BoundaryCrossing_Hour_ShouldReturn1()
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE DateTimeDiff('hh', '2020-01-01T23:59:00.0000000', '2020-01-02T00:01:00.0000000') FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().Be(1);
	}

	[Fact]
	public async Task DateTimeDiff_BoundaryCrossing_Hour_EmulatorBehavior_NowCorrect()
	{
		// Previously divergent: emulator returned 0 for a 2-minute span crossing the hour boundary.
		// Now fixed: uses boundary-crossing semantics matching real Cosmos DB.
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE DateTimeDiff('hh', '2020-01-01T23:59:00.0000000', '2020-01-02T00:01:00.0000000') FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Emulator now correctly returns 1 (boundary crossing), matching real Cosmos DB
		results.Should().ContainSingle().Which.Should().Be(1);
	}
}

// ─── Conversion Round-Trip and Edge Case Tests ──────────────────────────

public class DateTimeConversionExtendedTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	private async Task EnsureSeedItem()
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }
	}

	[Fact]
	public async Task DateTimeToTicks_And_TicksToDateTime_RoundTrip()
	{
		await EnsureSeedItem();

		var ticksIterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE DateTimeToTicks('2023-06-15T10:30:45.1234567') FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var ticks = new List<long>();
		while (ticksIterator.HasMoreResults) ticks.AddRange(await ticksIterator.ReadNextAsync());

		var dtIterator = _container.GetItemQueryIterator<string>(
			$"SELECT VALUE TicksToDateTime({ticks.Single()}) FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var dates = new List<string>();
		while (dtIterator.HasMoreResults) dates.AddRange(await dtIterator.ReadNextAsync());

		dates.Single().Should().Be("2023-06-15T10:30:45.1234567Z");
	}

	[Fact]
	public async Task DateTimeToTimestamp_And_TimestampToDateTime_RoundTrip()
	{
		await EnsureSeedItem();

		var tsIterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE DateTimeToTimestamp('2023-06-15T10:30:45.0000000') FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var timestamps = new List<long>();
		while (tsIterator.HasMoreResults) timestamps.AddRange(await tsIterator.ReadNextAsync());

		var dtIterator = _container.GetItemQueryIterator<string>(
			$"SELECT VALUE TimestampToDateTime({timestamps.Single()}) FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var dates = new List<string>();
		while (dtIterator.HasMoreResults) dates.AddRange(await dtIterator.ReadNextAsync());

		dates.Single().Should().Be("2023-06-15T10:30:45.0000000Z");
	}

	[Fact]
	public async Task DateTimeToTimestamp_PreUnixEpoch_NegativeValue()
	{
		await EnsureSeedItem();

		var iterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE DateTimeToTimestamp('1969-06-15T00:00:00.0000000') FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Single().Should().BeNegative();
	}
}

// ─── GetCurrentDateTime / GetCurrentTimestamp Value Tests ───────────────

public class GetCurrentDateTimeFunctionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task GetCurrentDateTime_ReturnsIso8601InUtc()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<string>(
			"SELECT VALUE GetCurrentDateTime() FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		var result = results.Single();
		result.Should().EndWith("Z");
		result.Should().MatchRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z$");
	}

	[Fact]
	public async Task GetCurrentTimestamp_ReturnsReasonableUnixMs()
	{
		await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE GetCurrentTimestamp() FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		// Should be greater than 2020-01-01 timestamp (1577836800000)
		results.Single().Should().BeGreaterThan(1577836800000);
	}
}

// ─── Composed / Integration Date Query Tests ────────────────────────────

public class DateComposedQueryTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task DateFilter_WhereClause_FiltersOnDateComparison()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "old", pk = "a", ts = "2019-01-01T00:00:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "new", pk = "a", ts = "2021-06-01T00:00:00Z" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.ts > '2020-01-01T00:00:00Z'",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("new");
	}

	[Fact]
	public async Task DateTimeAdd_InsideDateTimeDiff_Composes()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-01-01T00:00:00.0000000Z" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE DateTimeDiff('dd', c.ts, DateTimeAdd('dd', 7, c.ts)) FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Single().Should().Be(7);
	}

	[Fact]
	public async Task OrderBy_DateTimeField_SortsChronologically()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "c", pk = "a", ts = "2022-03-01T00:00:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "a", pk = "a", ts = "2020-01-01T00:00:00Z" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "b", pk = "a", ts = "2021-06-15T00:00:00Z" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.ts ASC",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Select(r => r["id"]!.ToString()).Should().ContainInOrder("a", "b", "c");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 1: Bug Fix Verification Tests
// ═══════════════════════════════════════════════════════════════════════════

public class DateTimeFromPartsInvalidArgsTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<JObject?> QuerySingleOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	[Fact]
	public async Task DateTimeFromParts_NegativeYear_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeFromParts(-2000, 1, 1) FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeFromParts_InvalidMonth13_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeFromParts(2020, 13, 1) FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeFromParts_InvalidDay32_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeFromParts(2020, 1, 32) FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeFromParts_Feb29NonLeapYear_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeFromParts(2023, 2, 29) FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeFromParts_DocsExample_NegativeArgs_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeFromParts(-2000, -1, -1) FROM c");
		result.Should().BeNull();
	}
}

public class DateTimeAddInvalidPartTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<JObject?> QuerySingleOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	[Fact]
	public async Task DateTimeAdd_InvalidPart_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeAdd('invalid', 1, '2020-01-01T00:00:00.0000000Z') FROM c");
		result.Should().BeNull();
	}
}

public class DateTimeBinBugFixTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<string?> QuerySingleString(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<string>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	private async Task<JObject?> QuerySingleOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	[Fact]
	public async Task DateTimeBin_ZeroBinSize_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeBin('2021-06-28T17:24:29.2991234Z', 'hh', 0) FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeBin_NegativeBinSize_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeBin('2021-06-28T17:24:29.2991234Z', 'hh', -1) FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeBin_Microsecond_BinsCorrectly()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-06-28T17:24:29.1234567Z', 'mcs', 100) FROM c");
		result.Should().NotBeNull();
		result.Should().StartWith("2021-06-28T17:24:29.123");
	}

	[Fact]
	public async Task DateTimeBin_Nanosecond_BinsCorrectly()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-06-28T17:24:29.1234567Z', 'ns', 1000) FROM c");
		result.Should().NotBeNull();
		result.Should().StartWith("2021-06-28T17:24:29.123");
	}

	[Fact]
	public async Task DateTimeBin_TwoArgForm_DefaultBinSize1()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-06-28T17:24:29.2991234Z', 'hh') FROM c");
		result.Should().Be("2021-06-28T17:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_OriginBefore1601_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeBin('2021-06-28T17:24:29Z', 'hh', 1, '1500-01-01T00:00:00Z') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeBin_InvalidPart_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeBin('2021-06-28T17:24:29Z', 'invalid', 1) FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeBin_InvalidDateTime_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeBin('not-a-date', 'hh', 1) FROM c");
		result.Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 2: DateTimeAdd Extended
// ═══════════════════════════════════════════════════════════════════════════

public class DateTimeAddExtendedTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<string> QuerySingleString(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<string>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	private async Task<JObject?> QuerySingleOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	[Fact]
	public async Task DateTimeAdd_DayLongAlias_AddsCorrectly()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeAdd('day', 5, '2020-01-01T00:00:00.0000000Z') FROM c");
		result.Should().Be("2020-01-06T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_DAlias_AddsCorrectly()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeAdd('d', 5, '2020-01-01T00:00:00.0000000Z') FROM c");
		result.Should().Be("2020-01-06T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_HourLongAlias_AddsCorrectly()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeAdd('hour', 3, '2020-01-01T00:00:00.0000000Z') FROM c");
		result.Should().Be("2020-01-01T03:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_SubtractMonthCrossYearBoundary()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeAdd('mm', -2, '2020-01-15T00:00:00.0000000Z') FROM c");
		result.Should().Be("2019-11-15T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_AddMonth_Mar31Plus1_ClampsToApr30()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeAdd('mm', 1, '2020-03-31T00:00:00.0000000Z') FROM c");
		result.Should().Be("2020-04-30T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_InvalidDateTimeString_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeAdd('dd', 1, 'not-a-date') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeAdd_PreserveSubSecondPrecision()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeAdd('hh', 1, '2020-01-01T00:00:00.1234567Z') FROM c");
		result.Should().Be("2020-01-01T01:00:00.1234567Z");
	}

	[Fact]
	public async Task DateTimeAdd_ZeroAmount_ReturnsUnchanged()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeAdd('dd', 0, '2020-06-15T12:30:00.0000000Z') FROM c");
		result.Should().Be("2020-06-15T12:30:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeAdd_LargeYearAddition()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeAdd('yyyy', 1000, '1020-01-01T00:00:00.0000000Z') FROM c");
		result.Should().Be("2020-01-01T00:00:00.0000000Z");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 3: DateTimePart Alias Coverage
// ═══════════════════════════════════════════════════════════════════════════

public class DateTimePartAliasTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<long> QuerySingleLong(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<long>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	private async Task<JObject?> QuerySingleOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	[Theory]
	[InlineData("day", 29)]
	[InlineData("d", 29)]
	[InlineData("hour", 8)]
	[InlineData("hh", 8)]
	[InlineData("minute", 30)]
	[InlineData("mi", 30)]
	[InlineData("n", 30)]
	[InlineData("second", 0)]
	[InlineData("ss", 0)]
	[InlineData("s", 0)]
	[InlineData("millisecond", 130)]
	[InlineData("microsecond", 130161)]
	[InlineData("nanosecond", 130161700)]
	public async Task DateTimePart_AllAliases(string part, long expected)
	{
		var result = await QuerySingleLong(
			$"SELECT VALUE DateTimePart('{part}', '2016-05-29T08:30:00.1301617Z') FROM c");
		result.Should().Be(expected);
	}

	[Fact]
	public async Task DateTimePart_InvalidPart_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimePart('invalid', '2020-01-01T00:00:00Z') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimePart_InvalidDateTime_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimePart('yyyy', 'not-a-date') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimePart_MidnightZeroValues()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimePart('hh', '2020-01-01T00:00:00.0000000Z') FROM c");
		result.Should().Be(0);
	}

	[Fact]
	public async Task DateTimePart_EndOfDay_235959()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimePart('ss', '2020-12-31T23:59:59.9999999Z') FROM c");
		result.Should().Be(59);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 4: DateTimeDiff Extended
// ═══════════════════════════════════════════════════════════════════════════

public class DateTimeDiffExtendedDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<long> QuerySingleLong(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<long>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	private async Task<JObject?> QuerySingleOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	[Fact]
	public async Task DateTimeDiff_SameDateTime_ZeroForAllParts()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('yyyy', '2020-01-01T00:00:00Z', '2020-01-01T00:00:00Z') FROM c");
		result.Should().Be(0);
	}

	[Fact]
	public async Task DateTimeDiff_BoundaryCrossing_Minute()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('mi', '2020-01-01T12:59:30Z', '2020-01-01T13:00:30Z') FROM c");
		result.Should().Be(1);
	}

	[Fact]
	public async Task DateTimeDiff_BoundaryCrossing_Second()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('ss', '2020-01-01T00:00:00.9000000Z', '2020-01-01T00:00:01.1000000Z') FROM c");
		result.Should().Be(1);
	}

	[Fact]
	public async Task DateTimeDiff_BoundaryCrossing_Millisecond()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('ms', '2020-01-01T00:00:00.0009000Z', '2020-01-01T00:00:00.0011000Z') FROM c");
		result.Should().Be(1);
	}

	[Fact]
	public async Task DateTimeDiff_VeryLargeSpan_Centuries()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('yyyy', '1900-01-01T00:00:00Z', '2100-01-01T00:00:00Z') FROM c");
		result.Should().Be(200);
	}

	[Fact]
	public async Task DateTimeDiff_InvalidPart_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeDiff('invalid', '2020-01-01T00:00:00Z', '2021-01-01T00:00:00Z') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeDiff_InvalidStartDateTime_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeDiff('dd', 'not-a-date', '2020-01-01T00:00:00Z') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeDiff_InvalidEndDateTime_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeDiff('dd', '2020-01-01T00:00:00Z', 'not-a-date') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeDiff_DayWithPartialDays()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('dd', '2020-01-01T23:59:00Z', '2020-01-02T00:01:00Z') FROM c");
		result.Should().Be(1);
	}

	[Fact]
	public async Task DateTimeDiff_NegativeBoundaryCrossing_Hour()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('hh', '2020-01-01T13:00:30Z', '2020-01-01T12:59:30Z') FROM c");
		result.Should().Be(-1);
	}

	[Fact]
	public async Task DateTimeDiff_MonthPartialMonth()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('mm', '2020-01-31T00:00:00Z', '2020-02-15T00:00:00Z') FROM c");
		result.Should().Be(1);
	}

	[Fact]
	public async Task DateTimeDiff_YearPartialYear()
	{
		var result = await QuerySingleLong(
			"SELECT VALUE DateTimeDiff('yyyy', '2020-12-15T00:00:00Z', '2021-01-15T00:00:00Z') FROM c");
		result.Should().Be(1);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 5: DateTimeBin Extended
// ═══════════════════════════════════════════════════════════════════════════

public class DateTimeBinExtendedDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<string> QuerySingleString(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<string>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	[Theory]
	[InlineData("year", "2021-01-01T00:00:00.0000000Z")]
	[InlineData("yyyy", "2021-01-01T00:00:00.0000000Z")]
	[InlineData("month", "2021-06-01T00:00:00.0000000Z")]
	[InlineData("mm", "2021-06-01T00:00:00.0000000Z")]
	[InlineData("day", "2021-06-28T00:00:00.0000000Z")]
	[InlineData("dd", "2021-06-28T00:00:00.0000000Z")]
	[InlineData("hour", "2021-06-28T17:00:00.0000000Z")]
	[InlineData("hh", "2021-06-28T17:00:00.0000000Z")]
	[InlineData("minute", "2021-06-28T17:24:00.0000000Z")]
	[InlineData("mi", "2021-06-28T17:24:00.0000000Z")]
	[InlineData("second", "2021-06-28T17:24:29.0000000Z")]
	[InlineData("ss", "2021-06-28T17:24:29.0000000Z")]
	public async Task DateTimeBin_LongFormAliases(string part, string expected)
	{
		var result = await QuerySingleString(
			$"SELECT VALUE DateTimeBin('2021-06-28T17:24:29.2991234Z', '{part}', 1) FROM c");
		result.Should().Be(expected);
	}

	[Fact]
	public async Task DateTimeBin_FiveYearBins()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2023-06-15T00:00:00Z', 'yyyy', 5) FROM c");
		result.Should().Be("2020-01-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_MillisecondBins500()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-06-28T17:24:29.7501234Z', 'ms', 500) FROM c");
		result.Should().Be("2021-06-28T17:24:29.5000000Z");
	}

	[Fact]
	public async Task DateTimeBin_DocsExample_BinSecond()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:00.0000000Z', 'ss', 1) FROM c");
		result.Should().Be("2021-01-08T18:35:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeBin_DocsExample_BinHour()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeBin('2021-01-08T18:35:00.0000000Z', 'hh', 1) FROM c");
		result.Should().Be("2021-01-08T18:00:00.0000000Z");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 6: DateTimeFromParts Extended
// ═══════════════════════════════════════════════════════════════════════════

public class DateTimeFromPartsExtendedDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<string> QuerySingleString(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<string>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	private async Task<JObject?> QuerySingleOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	[Fact]
	public async Task DateTimeFromParts_4Args_YearMonthDayHour()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeFromParts(2020, 6, 15, 10) FROM c");
		result.Should().Be("2020-06-15T10:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeFromParts_6Args_NoFraction()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeFromParts(2020, 6, 15, 10, 30, 45) FROM c");
		result.Should().Be("2020-06-15T10:30:45.0000000Z");
	}

	[Fact]
	public async Task DateTimeFromParts_BoundaryValues_Dec31()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeFromParts(2020, 12, 31, 23, 59, 59, 9999999) FROM c");
		result.Should().Be("2020-12-31T23:59:59.9999999Z");
	}

	[Fact]
	public async Task DateTimeFromParts_MinimumDate()
	{
		var result = await QuerySingleString(
			"SELECT VALUE DateTimeFromParts(1, 1, 1) FROM c");
		result.Should().Be("0001-01-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeFromParts_LessThan3Args_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeFromParts(2020, 6) FROM c");
		result.Should().BeNull();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 7: Conversion Functions
// ═══════════════════════════════════════════════════════════════════════════

public class DateConversionDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	private async Task<T> QuerySingleValue<T>(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<T>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<T>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.Single();
	}

	private async Task<JObject?> QuerySingleOrNull(string sql)
	{
		try { await _container.ReadItemAsync<JObject>("1", new PartitionKey("a")); }
		catch { await _container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a")); }

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());
		return results.SingleOrDefault();
	}

	[Fact]
	public async Task TicksToDateTime_ZeroTicks_Returns0001_01_01()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE TicksToDateTime(0) FROM c");
		result.Should().Be("0001-01-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task TimestampToDateTime_ZeroMs_ReturnsUnixEpoch()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE TimestampToDateTime(0) FROM c");
		result.Should().Be("1970-01-01T00:00:00.0000000Z");
	}

	[Fact]
	public async Task TimestampToDateTime_NegativeMs_ReturnsPreEpoch()
	{
		var result = await QuerySingleValue<string>(
			"SELECT VALUE TimestampToDateTime(-86400000) FROM c");
		result.Should().Be("1969-12-31T00:00:00.0000000Z");
	}

	[Fact]
	public async Task DateTimeToTicks_InvalidString_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeToTicks('not-a-date') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeToTimestamp_InvalidString_ReturnsNull()
	{
		var result = await QuerySingleOrNull(
			"SELECT VALUE DateTimeToTimestamp('not-a-date') FROM c");
		result.Should().BeNull();
	}

	[Fact]
	public async Task DateTimeToTicks_RoundTripWithTicksToDateTime()
	{
		var ticks = await QuerySingleValue<long>(
			"SELECT VALUE DateTimeToTicks('2020-06-15T12:30:00.1234567Z') FROM c");
		var roundTrip = await QuerySingleValue<string>(
			$"SELECT VALUE TicksToDateTime({ticks}) FROM c");
		roundTrip.Should().Be("2020-06-15T12:30:00.1234567Z");
	}

	[Fact]
	public async Task DateTimeToTimestamp_RoundTripWithTimestampToDateTime()
	{
		var ms = await QuerySingleValue<long>(
			"SELECT VALUE DateTimeToTimestamp('2020-06-15T12:30:00.0000000Z') FROM c");
		var roundTrip = await QuerySingleValue<string>(
			$"SELECT VALUE TimestampToDateTime({ms}) FROM c");
		roundTrip.Should().Be("2020-06-15T12:30:00.0000000Z");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 8: GetCurrentDateTime
// ═══════════════════════════════════════════════════════════════════════════

public class GetCurrentDateTimeDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task GetCurrentDateTime_FormatHas7DecimalPlaces()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<string>(
			"SELECT VALUE GetCurrentDateTime() FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<string>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Single().Should().MatchRegex(@"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z");
	}

	[Fact]
	public async Task GetCurrentTicks_TypeIsLong()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<long>(
			"SELECT VALUE GetCurrentTicks() FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<long>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Single().Should().BeGreaterThan(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 9: Composed/Integration Tests
// ═══════════════════════════════════════════════════════════════════════════

public class DateFunctionIntegrationDeepDiveTests
{
	private readonly InMemoryContainer _container = new("test", "/pk");

	[Fact]
	public async Task DateTimePart_InWhereClause()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-06-15T00:00:00Z" }), new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", ts = "2021-06-15T00:00:00Z" }), new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE DateTimePart('yyyy', c.ts) = 2020",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task DateTimeAdd_InWhereComparison()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-01-01T00:00:00Z" }), new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", ts = "2020-12-01T00:00:00Z" }), new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE DateTimeAdd('dd', 30, c.ts) > '2020-01-15T00:00:00Z'",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task MultipleDateFunctions_InSingleSelect()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-06-15T10:30:00.0000000Z" }), new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT DateTimePart('yyyy', c.ts) as yr, DateTimePart('mm', c.ts) as mo FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Single()["yr"]!.Value<long>().Should().Be(2020);
		results.Single()["mo"]!.Value<long>().Should().Be(6);
	}

	[Fact]
	public async Task DateFunction_WithNullDocumentProperty_ReturnsNull()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = (string?)null }), new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT VALUE DateTimePart('yyyy', c.ts) FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.SingleOrDefault().Should().BeNull();
	}

	[Fact]
	public async Task DateTimeToTicks_InWhereClause()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", ts = "2020-01-01T00:00:00Z" }), new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", ts = "2021-06-15T00:00:00Z" }), new PartitionKey("a"));

		// 2020-06-01 ticks
		var cutoff = new DateTime(2020, 6, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
		var iterator = _container.GetItemQueryIterator<JObject>(
			$"SELECT * FROM c WHERE DateTimeToTicks(c.ts) > {cutoff}",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults) results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["id"]!.ToString().Should().Be("2");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Date Handling Deep Dive — Phase 10: DateTimeOffset Round-trip Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class DateTimeOffsetRoundTripDeepDiveTests
{
	[Fact]
	public async Task DateTimeOffset_NegativeOffset_RoundTrips()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var dto = new DateTimeOffset(2020, 6, 15, 10, 30, 0, TimeSpan.FromHours(-5));
		var doc = new DateDocument { Id = "1", PartitionKey = "pk1", EventDate = dto };
		await container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<DateDocument>("1", new PartitionKey("pk1"));
		read.Resource.EventDate.Should().Be(dto);
	}

	[Fact]
	public async Task DateTimeOffset_MaxOffset_RoundTrips()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var dto = new DateTimeOffset(2020, 6, 15, 10, 30, 0, TimeSpan.FromHours(14));
		var doc = new DateDocument { Id = "1", PartitionKey = "pk1", EventDate = dto };
		await container.CreateItemAsync(doc, new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<DateDocument>("1", new PartitionKey("pk1"));
		read.Resource.EventDate.Should().Be(dto);
	}
}
