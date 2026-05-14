using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;
using AwesomeAssertions;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Regression coverage for GitHub Issue #59 — `COUNT(c.obj["field"] > 0 ? 1 : undefined)`
/// must evaluate without throwing when `field` is a Cosmos SQL reserved word and the
/// document accesses it via double-quoted bracket notation.
/// </summary>
public class Issue59_CountBracketNotationTests
{
    [Fact]
    public async Task Sum_WithBracketNotation_OnReservedWordField_Works()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("transactions", "/id")
            .Build();

        var container = cosmos.Containers["transactions"];
        await SeedTransactions(container);

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT SUM(c.grossSettlementValue[\"value\"]) AS total FROM c");
        var page = await iterator.ReadNextAsync();
        page.First()["total"]!.Value<decimal>().Should().Be(100m);
    }

    [Fact]
    public async Task Count_WithBracketNotationAndTernary_OnReservedWordField_DoesNotThrow()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("transactions", "/id")
            .Build();

        var container = cosmos.Containers["transactions"];
        await SeedTransactions(container);

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT COUNT(c.grossSettlementValue[\"value\"] > 0 ? 1 : undefined) AS cnt FROM c");

        var page = await iterator.ReadNextAsync();
        page.First()["cnt"]!.Value<int>().Should().Be(1);
    }

    [Fact]
    public async Task Combined_CountAndSum_WithBracketNotation_OnReservedWordField_Works()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("transactions", "/id")
            .Build();

        var container = cosmos.Containers["transactions"];
        await SeedTransactions(container);
        await container.CreateItemAsync(
            new
            {
                id = "2",
                grossSettlementValue = new { value = -50m, currencyCode = "GBP" },
                transactionType = "Settlement"
            },
            new PartitionKey("2"));

        var iterator = container.GetItemQueryIterator<JObject>(
            "SELECT COUNT(c.grossSettlementValue[\"value\"] > 0 ? 1 : undefined) AS NumberTransactions, " +
            "SUM(c.grossSettlementValue[\"value\"]) AS CreditTotal " +
            "FROM c WHERE c.transactionType = 'Settlement'");

        var page = await iterator.ReadNextAsync();
        var row = page.First();
        row["NumberTransactions"]!.Value<int>().Should().Be(1);
        row["CreditTotal"]!.Value<decimal>().Should().Be(50m);
    }

    private static async Task SeedTransactions(Container container)
    {
        await container.CreateItemAsync(
            new
            {
                id = "1",
                grossSettlementValue = new { value = 100m, currencyCode = "GBP" },
                transactionType = "Settlement"
            },
            new PartitionKey("1"));
    }
}
