namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Trait name constants for categorising parity-validated tests.
/// Apply via <c>[Trait(TestTraits.Target, TestTraits.All)]</c>.
/// </summary>
public static class TestTraits
{
    /// <summary>Trait name for test target scope.</summary>
    public const string Target = "Target";

    /// <summary>Runs against both in-memory and emulator (default for FakeCosmosHandler tests).</summary>
    public const string All = "All";

    /// <summary>Only meaningful against in-memory (direct InMemoryContainer, fault injection, etc.).</summary>
    public const string InMemoryOnly = "InMemoryOnly";

    /// <summary>Documents a known divergence between in-memory and emulator.</summary>
    public const string KnownDivergence = "KnownDivergence";
}
