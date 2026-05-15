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

    /// <summary>
    /// Test is reproducibly flaky against the Linux Docker / Windows Cosmos DB
    /// emulators due to emulator-side instability (typically 503 responses where
    /// the in-memory backend returns the expected status). Excluded from
    /// emulator-target runs in scripts/run-tests.ps1 to keep CI signal clean.
    /// In-memory runs are unaffected — these tests still validate behaviour there.
    /// </summary>
    public const string EmulatorFlaky = "EmulatorFlaky";
}
