using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Reproduction tests for decimal scale inconsistencies (GitHub issue #75).
///
/// Bug 1 - Round-trip scale modification (cross-platform, Windows AND Linux):
///   Whole numbers gain a trailing .0 (e.g., 1500m becomes 1500.0m).
///   Trailing zeros are stripped from fractional values (e.g., 100.50m becomes 100.5m).
///
/// Bug 2 - SUM aggregate platform inconsistency (Linux-specific):
///   On Windows, SUM(375, 375) returns 750m (ToString => "750").
///   On Linux, SUM(375, 375) returns 750.0m (ToString => "750.0").
///
/// Parity-validated: runs against both FakeCosmosHandler (in-memory) and real emulator.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class DecimalScaleTests(EmulatorSession session, ITestOutputHelper output) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("test-decimal-scale", "/partitionKey");
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Bug 1: Round-trip scale modification (cross-platform)
    // ═══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData(0, "0")]
    [InlineData(1, "1")]
    [InlineData(42, "42")]
    [InlineData(750, "750")]
    [InlineData(1500, "1500")]
    [InlineData(999999, "999999")]
    public async Task RoundTrip_WholeNumber_ShouldNotGainDecimalPlaces(int inputValue, string expectedToString)
    {
        var id = Guid.NewGuid().ToString();
        var document = new DecimalDoc { Id = id, PartitionKey = "pk", Amount = inputValue };

        await _container.CreateItemAsync(document, new PartitionKey("pk"));
        var response = await _container.ReadItemAsync<DecimalDoc>(id, new PartitionKey("pk"));

        output.WriteLine($"Input: {inputValue}m -> Round-trip: {response.Resource.Amount}m (ToString: \"{response.Resource.Amount}\")");

        response.Resource.Amount.Should().Be(inputValue, "the arithmetic value must be preserved");
        response.Resource.Amount.ToString().Should().Be(expectedToString,
            $"whole number {inputValue}m should not gain trailing decimal places after round-trip");
    }

    [Theory]
    [InlineData("100.50", "100.50")]
    [InlineData("25.00", "25.00")]
    [InlineData("0.10", "0.10")]
    [InlineData("1.00", "1.00")]
    [InlineData("999.990", "999.990")]
    public async Task RoundTrip_TrailingZeros_ShouldBePreserved(string inputValue, string expectedToString)
    {
        var id = Guid.NewGuid().ToString();
        var amount = decimal.Parse(inputValue);
        var document = new DecimalDoc { Id = id, PartitionKey = "pk", Amount = amount };

        await _container.CreateItemAsync(document, new PartitionKey("pk"));
        var response = await _container.ReadItemAsync<DecimalDoc>(id, new PartitionKey("pk"));

        output.WriteLine($"Input: {inputValue} -> Round-trip: {response.Resource.Amount} (ToString: \"{response.Resource.Amount}\")");

        response.Resource.Amount.Should().Be(amount, "the arithmetic value must be preserved");
        response.Resource.Amount.ToString().Should().Be(expectedToString,
            $"decimal {inputValue} should preserve trailing zeros after round-trip (scale must be maintained)");
    }

    [Theory]
    [InlineData("0.01", "0.01")]
    [InlineData("123.456", "123.456")]
    [InlineData("999.99", "999.99")]
    [InlineData("0.001", "0.001")]
    public async Task RoundTrip_SignificantFractionalDigits_ShouldBePreserved(string inputValue, string expectedToString)
    {
        var id = Guid.NewGuid().ToString();
        var amount = decimal.Parse(inputValue);
        var document = new DecimalDoc { Id = id, PartitionKey = "pk", Amount = amount };

        await _container.CreateItemAsync(document, new PartitionKey("pk"));
        var response = await _container.ReadItemAsync<DecimalDoc>(id, new PartitionKey("pk"));

        output.WriteLine($"Input: {inputValue} -> Round-trip: {response.Resource.Amount} (ToString: \"{response.Resource.Amount}\")");

        response.Resource.Amount.Should().Be(amount, "the arithmetic value must be preserved");
        response.Resource.Amount.ToString().Should().Be(expectedToString,
            $"decimal {inputValue} should preserve all significant digits after round-trip");
    }

    [Fact]
    public async Task RoundTrip_MultipleProperties_ShouldAllPreserveScale()
    {
        var id = Guid.NewGuid().ToString();
        var document = new MultiDecimalDoc
        {
            Id = id,
            PartitionKey = "pk",
            WholeNumber = 1500m,
            WithTrailingZero = 100.50m,
            PureZero = 0m,
            SmallFraction = 0.01m
        };

        await _container.CreateItemAsync(document, new PartitionKey("pk"));
        var response = await _container.ReadItemAsync<MultiDecimalDoc>(id, new PartitionKey("pk"));

        output.WriteLine($"WholeNumber: {document.WholeNumber} -> {response.Resource.WholeNumber}");
        output.WriteLine($"WithTrailingZero: {document.WithTrailingZero} -> {response.Resource.WithTrailingZero}");
        output.WriteLine($"PureZero: {document.PureZero} -> {response.Resource.PureZero}");
        output.WriteLine($"SmallFraction: {document.SmallFraction} -> {response.Resource.SmallFraction}");

        response.Resource.WholeNumber.ToString().Should().Be("1500", "whole number should not gain .0");
        response.Resource.WithTrailingZero.ToString().Should().Be("100.50", "trailing zero should be preserved");
        response.Resource.PureZero.ToString().Should().Be("0", "zero should remain 0 without .0");
        response.Resource.SmallFraction.ToString().Should().Be("0.01", "significant fractional digits should be preserved");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Bug 2: SUM aggregate platform inconsistency (Linux-specific)
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SumAggregate_TwoWholeNumbers_ShouldReturnWholeNumber()
    {
        await SeedDocuments(("a", 375m), ("b", 375m));

        var result = await RunSumQuery();

        output.WriteLine($"SUM(375 + 375) = {result}, ToString: \"{result}\"");

        result.Should().Be(750m, "arithmetic must be correct");
        result.ToString().Should().Be("750",
            "SUM of whole numbers should return a whole number without trailing .0");
    }

    [Fact]
    public async Task SumAggregate_MultipleWholeNumbers_ShouldReturnWholeNumber()
    {
        await SeedDocuments(("a", 100m), ("b", 200m), ("c", 300m), ("d", 400m), ("e", 500m));

        var result = await RunSumQuery();

        output.WriteLine($"SUM(100+200+300+400+500) = {result}, ToString: \"{result}\"");

        result.Should().Be(1500m, "arithmetic must be correct");
        result.ToString().Should().Be("1500",
            "SUM of multiple whole numbers should return a whole number");
    }

    [Fact]
    public async Task SumAggregate_SingleWholeNumber_ShouldReturnWholeNumber()
    {
        await SeedDocuments(("a", 1500m));

        var result = await RunSumQuery();

        output.WriteLine($"SUM(1500) = {result}, ToString: \"{result}\"");

        result.Should().Be(1500m, "arithmetic must be correct");
        result.ToString().Should().Be("1500",
            "SUM of a single whole number should not gain decimal places");
    }

    [Fact]
    public async Task SumAggregate_FractionalNumbers_ShouldPreserveMinimumScale()
    {
        await SeedDocuments(("a", 25.50m), ("b", 25.00m));

        var result = await RunSumQuery();

        output.WriteLine($"SUM(25.50 + 25.00) = {result}, ToString: \"{result}\"");

        result.Should().Be(50.5m, "arithmetic must be correct");
        result.ToString().Should().BeOneOf("50.5", "50.50",
            "SUM of fractional numbers should not add unexpected trailing zeros");
    }

    [Fact]
    public async Task SumAggregate_MixedWholeAndFractional_ShouldReturnFractionalResult()
    {
        await SeedDocuments(("a", 100m), ("b", 50.25m));

        var result = await RunSumQuery();

        output.WriteLine($"SUM(100 + 50.25) = {result}, ToString: \"{result}\"");

        result.Should().Be(150.25m, "arithmetic must be correct");
        result.ToString().Should().Be("150.25",
            "SUM with a fractional addend should produce a fractional result");
    }

    [Fact]
    public async Task SumAggregate_ResultUsedInStringInterpolation_ShouldProduceConsistentOutput()
    {
        await SeedDocuments(("a", 750m), ("b", 750m));

        var transactionDebitTotal = await RunSumQuery();
        var clearingFileDebitTotal = 1650m;

        var message = $"Clearing file debit total: {clearingFileDebitTotal}, transaction debit total: {transactionDebitTotal}";

        output.WriteLine($"Message: \"{message}\"");

        message.Should().Be("Clearing file debit total: 1650, transaction debit total: 1500",
            "string interpolation of SUM result should produce platform-consistent output without trailing .0");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Compound effect: LINQ Sum on round-tripped values
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LinqSum_OnRoundTrippedWholeNumbers_ShouldReturnWholeNumber()
    {
        await SeedDocuments(("a", 750m), ("b", 750m));

        var query = _container.GetItemQueryIterator<DecimalDoc>(
            new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = 'pk'"));

        var results = new List<DecimalDoc>();
        while (query.HasMoreResults)
        {
            var feed = await query.ReadNextAsync();
            results.AddRange(feed);
        }

        var sum = results.Sum(r => r.Amount);

        output.WriteLine($"Values after round-trip: {string.Join(", ", results.Select(r => $"\"{r.Amount}\""))}");
        output.WriteLine($"LINQ Sum: {sum}, ToString: \"{sum}\"");

        sum.Should().Be(1500m, "arithmetic must be correct");
        sum.ToString().Should().Be("1500",
            "LINQ Sum of whole-number decimals should not produce trailing decimal places");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private async Task SeedDocuments(params (string id, decimal value)[] items)
    {
        foreach (var (id, value) in items)
        {
            await _container.CreateItemAsync(
                new DecimalDoc { Id = id, PartitionKey = "pk", Amount = value },
                new PartitionKey("pk"));
        }
    }

    private async Task<decimal> RunSumQuery()
    {
        var query = _container.GetItemQueryIterator<SumResult>(
            new QueryDefinition("SELECT SUM(c.amount) as total FROM c WHERE c.partitionKey = 'pk'"));

        while (query.HasMoreResults)
        {
            var feed = await query.ReadNextAsync();
            var result = feed.FirstOrDefault();
            if (result is not null) return result.Total;
        }

        throw new InvalidOperationException("SUM query returned no results");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Models
    // ═══════════════════════════════════════════════════════════════════════════

    private class DecimalDoc
    {
        [JsonProperty("id")]
        public string Id { get; set; } = string.Empty;

        [JsonProperty("partitionKey")]
        public string PartitionKey { get; set; } = string.Empty;

        [JsonProperty("amount")]
        public decimal Amount { get; set; }
    }

    private class MultiDecimalDoc
    {
        [JsonProperty("id")]
        public string Id { get; set; } = string.Empty;

        [JsonProperty("partitionKey")]
        public string PartitionKey { get; set; } = string.Empty;

        [JsonProperty("wholeNumber")]
        public decimal WholeNumber { get; set; }

        [JsonProperty("withTrailingZero")]
        public decimal WithTrailingZero { get; set; }

        [JsonProperty("pureZero")]
        public decimal PureZero { get; set; }

        [JsonProperty("smallFraction")]
        public decimal SmallFraction { get; set; }
    }

    private class SumResult
    {
        [JsonProperty("total")]
        public decimal Total { get; set; }
    }
}
