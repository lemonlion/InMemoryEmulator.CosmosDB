using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.Tests.Infrastructure;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Regression tests for GitHub Issue #75 — Decimal scale not preserved on round-trip
/// and SUM aggregate returns platform-inconsistent decimal scales.
///
/// Bug 1: When a document contains a whole-number decimal (e.g. 1500m), Newtonsoft.Json
/// serialises it as "1500.0" (EnsureDecimalPlace adds a decimal point). Real Cosmos DB
/// normalises this to the integer 1500 (JavaScript engine behaviour). The in-memory emulator
/// was leaving it as 1500.0, so round-tripped values had an unexpected .0 suffix.
///
/// Bug 2: Fractional decimals with trailing zeros (e.g. 100.50m) were serialised correctly by
/// Newtonsoft.Json as "100.50", but then parsed through JObject with FloatParseHandling.Double,
/// converting 100.50 → double 100.5 and discarding the trailing zero on the way back out.
///
/// Bug 3: SUM aggregate over decimal amounts returned "750.0" on Linux and "750" on Windows
/// because the aggregate pipeline used double arithmetic and stored the result as JValue(double),
/// whose Newtonsoft.Json serialisation of a whole-number double is platform-dependent.
///
/// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/aggregate-sum
///   "The SUM system function returns the sum of all the values in an expression."
/// Real Cosmos DB (JavaScript engine) normalises 1500.0 → 1500 and SUM(250, 500) → 750
/// (integer representation, no .0 suffix).
/// </summary>
public class Issue75DecimalScaleTests : IAsyncLifetime
{
    private InMemoryCosmosResult _cosmos = null!;
    private Container _container = null!;

    private static readonly JsonSerializerSettings NewtonsoftSettings = new()
    {
        FloatParseHandling = FloatParseHandling.Decimal
    };

    public ValueTask InitializeAsync()
    {
        // Use a Newtonsoft.Json-based serialiser so that decimal values are round-tripped
        // through the same JSON pipeline as user code that relies on Newtonsoft.Json behaviour
        // (EnsureDecimalPlace for whole-number decimals, trailing-zero preservation for
        // fractional decimals).
        _cosmos = InMemoryCosmos.Create("issue75", "/partitionKey",
            configureOptions: opts => opts.Serializer = new Issue75NewtonsoftSerializer(NewtonsoftSettings));
        _container = _cosmos.Container;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _cosmos.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<List<T>> DrainQuery<T>(string sql, PartitionKey? pk = null)
    {
        var opts = pk is not null ? new QueryRequestOptions { PartitionKey = pk } : null;
        var iterator = _container.GetItemQueryIterator<T>(sql, requestOptions: opts);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Bug 1 — Whole-number decimal normalised to integer on round-trip
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Newtonsoft.Json serialises decimal 1500m as "1500.0" (EnsureDecimalPlace).
    /// Real Cosmos DB (JavaScript) normalises "1500.0" → integer 1500 during storage.
    /// The in-memory emulator must do the same so that read-back produces an integer
    /// token, not a float-with-unnecessary-decimal-point.
    ///
    /// Ref: JavaScript: JSON.stringify(JSON.parse("1500.0")) === "1500"
    /// </summary>
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task WholeNumberDecimal_RoundTrip_StoredAsIntegerNotFloat()
    {
        var doc = new { id = "1", partitionKey = "pk1", amount = 1500m };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var readBack = await _container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));

        // Ref: Real Cosmos DB stores whole-number JSON floats as integers (JavaScript normalisation).
        // "1500.0" must round-trip as integer 1500, not float 1500.0.
        readBack.Resource["amount"]!.Type.Should().Be(JTokenType.Integer);
        readBack.Resource["amount"]!.Value<long>().Should().Be(1500L);
    }

    /// <summary>
    /// Zero stored as a decimal literal (0m) should also round-trip as the integer 0,
    /// not as 0.0 (float with unnecessary decimal point).
    /// </summary>
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task ZeroDecimal_RoundTrip_StoredAsIntegerNotFloat()
    {
        var doc = new { id = "2", partitionKey = "pk1", amount = 0m };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var readBack = await _container.ReadItemAsync<JObject>("2", new PartitionKey("pk1"));

        readBack.Resource["amount"]!.Type.Should().Be(JTokenType.Integer);
        readBack.Resource["amount"]!.Value<long>().Should().Be(0L);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Bug 2 — Fractional decimal trailing zeros preserved through round-trip
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Newtonsoft.Json serialises decimal 100.50m as "100.50" (already has a decimal point,
    /// so EnsureDecimalPlace leaves it unchanged). FloatParseHandling.Double was discarding
    /// the trailing zero (100.50 → double 100.5 → "100.5"). FloatParseHandling.Decimal
    /// preserves the trailing zero because decimal remembers its scale.
    ///
    /// Note: this behaviour is intentionally in-memory-only. Real Cosmos DB's JavaScript
    /// engine normalises "100.50" to "100.5" on storage (JavaScript strips trailing zeros).
    /// The in-memory fix preserves scale to avoid silent data loss for test code that
    /// relies on decimal precision.
    /// </summary>
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task FractionalDecimalWithTrailingZero_RoundTrip_PreservesTrailingZero()
    {
        var doc = new { id = "3", partitionKey = "pk1", amount = 100.50m };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var readBack = await _container.ReadItemAsync<JObject>("3", new PartitionKey("pk1"));

        // With FloatParseHandling.Decimal in the server-side parser, "100.50" is stored as
        // decimal 100.50m, which serialises back to "100.50" preserving the trailing zero.
        readBack.Resource["amount"]!.ToString().Should().Be("100.50");
    }

    /// <summary>
    /// A fractional decimal with non-trivially significant trailing zeros (25.00m) should
    /// round-trip with both decimal places preserved.
    /// </summary>
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task FractionalDecimalWithTwoTrailingZeros_RoundTrip_PreservesScale()
    {
        var doc = new { id = "4", partitionKey = "pk1", amount = 25.00m };
        await _container.CreateItemAsync(doc, new PartitionKey("pk1"));

        var readBack = await _container.ReadItemAsync<JObject>("4", new PartitionKey("pk1"));

        // 25.00m serialised as "25.0" by Newtonsoft.Json (EnsureDecimalPlace: "25" → "25.0").
        // After fix: stored as integer 25 (whole-number normalisation), not "25.0".
        readBack.Resource["amount"]!.Type.Should().Be(JTokenType.Integer);
        readBack.Resource["amount"]!.Value<long>().Should().Be(25L);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Bug 3 — SUM aggregate returns consistent integer for whole-number results
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// SUM(250, 500) = 750. When the aggregate result is a whole number, it must be returned
    /// as the integer 750, not as float 750.0. The previous implementation stored the result as
    /// JValue(double 750.0), whose Newtonsoft.Json serialisation is platform-dependent
    /// ("750.0" on Linux, "750" on Windows), causing test flakiness across CI environments.
    ///
    /// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/aggregate-sum
    ///   Real Cosmos DB returns SUM(250, 500) as the integer 750 (JavaScript normalisation).
    /// </summary>
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task SumAggregate_WhenResultIsWholeNumber_ReturnsIntegerType()
    {
        await _container.CreateItemAsync(
            new { id = "s1", partitionKey = "pk2", amount = 250m }, new PartitionKey("pk2"));
        await _container.CreateItemAsync(
            new { id = "s2", partitionKey = "pk2", amount = 500m }, new PartitionKey("pk2"));

        var results = await DrainQuery<JToken>(
            "SELECT VALUE SUM(c.amount) FROM c", new PartitionKey("pk2"));

        results.Should().ContainSingle();
        // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/aggregate-sum
        // Whole-number SUM results must be integers, not floats.
        results[0].Type.Should().Be(JTokenType.Integer);
        results[0].Value<long>().Should().Be(750L);
    }

    /// <summary>
    /// SUM in a projection (SELECT SUM(c.amount) AS Total) must also return an integer
    /// when the result is a whole number.
    /// </summary>
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task SumAggregate_InProjection_WhenResultIsWholeNumber_ReturnsIntegerType()
    {
        await _container.CreateItemAsync(
            new { id = "s3", partitionKey = "pk3", amount = 250m }, new PartitionKey("pk3"));
        await _container.CreateItemAsync(
            new { id = "s4", partitionKey = "pk3", amount = 500m }, new PartitionKey("pk3"));

        var results = await DrainQuery<JObject>(
            "SELECT SUM(c.amount) AS Total FROM c", new PartitionKey("pk3"));

        results.Should().ContainSingle();
        results[0]["Total"]!.Type.Should().Be(JTokenType.Integer);
        results[0]["Total"]!.Value<long>().Should().Be(750L);
    }

    /// <summary>
    /// AVG aggregate where the result is a whole number should also return an integer type.
    /// AVG(500, 1000) = 750.
    /// </summary>
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task AvgAggregate_WhenResultIsWholeNumber_ReturnsIntegerType()
    {
        await _container.CreateItemAsync(
            new { id = "a1", partitionKey = "pk4", amount = 500m }, new PartitionKey("pk4"));
        await _container.CreateItemAsync(
            new { id = "a2", partitionKey = "pk4", amount = 1000m }, new PartitionKey("pk4"));

        var results = await DrainQuery<JToken>(
            "SELECT VALUE AVG(c.amount) FROM c", new PartitionKey("pk4"));

        results.Should().ContainSingle();
        results[0].Type.Should().Be(JTokenType.Integer);
        results[0].Value<long>().Should().Be(750L);
    }

    /// <summary>
    /// SUM aggregate where the result is a fractional number must remain a float (not be
    /// erroneously truncated to an integer). SUM(250.5, 499.5) = 750.0 — but this is a
    /// whole number so it should still be integer 750.
    /// SUM(250.3, 499.7) = 750.0 — same.
    /// SUM(250.3, 499.8) = 750.1 — must remain a float.
    /// </summary>
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    [Fact]
    public async Task SumAggregate_WhenResultIsFractional_RemainsFloat()
    {
        await _container.CreateItemAsync(
            new { id = "f1", partitionKey = "pk5", amount = 250.3m }, new PartitionKey("pk5"));
        await _container.CreateItemAsync(
            new { id = "f2", partitionKey = "pk5", amount = 499.8m }, new PartitionKey("pk5"));

        var results = await DrainQuery<JToken>(
            "SELECT VALUE SUM(c.amount) FROM c", new PartitionKey("pk5"));

        results.Should().ContainSingle();
        // 250.3 + 499.8 = 750.1 — fractional, must NOT be truncated to integer
        results[0].Type.Should().Be(JTokenType.Float);
        results[0].Value<decimal>().Should().Be(750.1m);
    }
}

/// <summary>
/// Newtonsoft.Json-based CosmosSerializer for Issue 75 repro tests.
/// Uses FloatParseHandling.Decimal on deserialisation so that fractional
/// decimal values round-trip with their trailing zeros preserved.
/// </summary>
internal sealed class Issue75NewtonsoftSerializer : CosmosSerializer
{
    private readonly JsonSerializer _serializer;

    public Issue75NewtonsoftSerializer(JsonSerializerSettings settings)
    {
        _serializer = JsonSerializer.Create(settings);
    }

    public override T FromStream<T>(Stream stream)
    {
        using var sr = new StreamReader(stream);
        using var jr = new JsonTextReader(sr) { FloatParseHandling = FloatParseHandling.Decimal };
        return _serializer.Deserialize<T>(jr)!;
    }

    public override Stream ToStream<T>(T input)
    {
        var ms = new MemoryStream();
        using (var sw = new StreamWriter(ms, leaveOpen: true))
        using (var jw = new JsonTextWriter(sw))
        {
            _serializer.Serialize(jw, input);
            jw.Flush();
        }
        ms.Position = 0;
        return ms;
    }
}
