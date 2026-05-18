using System.Text.Json;
using System.Text.Json.Serialization;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Schema for <c>tests/emulator-containers.json</c> — the single source of truth
/// for which containers exist on the emulator during integration tests. The CI
/// warmup tool pre-creates every entry before tests run; <see cref="EmulatorTestFixture"/>
/// refuses to create containers not listed here so a new test adding a container
/// without updating the manifest fails loudly.
/// </summary>
public sealed record EmulatorContainerManifest(
    [property: JsonPropertyName("containers")] IReadOnlyList<EmulatorContainerSpec> Containers)
{
    /// <summary>
    /// Loads the manifest from <paramref name="path"/>. Throws on missing file or
    /// malformed JSON — drift between the manifest and tests should fail loudly.
    /// </summary>
    public static EmulatorContainerManifest Load(string path)
    {
        using var stream = File.OpenRead(path);
        return JsonSerializer.Deserialize<EmulatorContainerManifest>(stream, JsonOpts)
            ?? throw new InvalidOperationException(
                $"Emulator container manifest at '{path}' deserialised to null.");
    }

    /// <summary>
    /// Resolves the manifest path relative to the assembly location, walking up
    /// the source tree until a <c>tests/emulator-containers.json</c> is found.
    /// Used by tests that don't get the path injected from outside.
    /// </summary>
    public static string ResolveDefaultPath()
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 10 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, "tests", "emulator-containers.json");
            if (File.Exists(candidate)) return candidate;
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException(
            "Could not locate tests/emulator-containers.json by walking up from " +
            AppContext.BaseDirectory);
    }

    public EmulatorContainerSpec Get(string name) =>
        Containers.FirstOrDefault(c => c.Name == name)
        ?? throw new InvalidOperationException(
            $"Container '{name}' is not in tests/emulator-containers.json. " +
            "Add it to the manifest so the CI warmup pre-creates it; tests cannot " +
            "lazy-create containers in the emulator path because cold-start 503s " +
            "blow past the CI job timeout.");

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };
}

public sealed record EmulatorContainerSpec(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("partitionKeyPath")] string? PartitionKeyPath = null,
    [property: JsonPropertyName("partitionKeyPaths")] IReadOnlyList<string>? PartitionKeyPaths = null,
    [property: JsonPropertyName("defaultTimeToLive")] int? DefaultTimeToLive = null,
    [property: JsonPropertyName("uniqueKeyPaths")] IReadOnlyList<string>? UniqueKeyPaths = null,
    [property: JsonPropertyName("skipDataPlaneProbe")] bool? SkipDataPlaneProbe = null)
{
    /// <summary>
    /// Returns the canonical list of partition key paths regardless of whether
    /// the manifest used the flat or hierarchical form.
    /// </summary>
    public IReadOnlyList<string> Paths => PartitionKeyPaths is { Count: > 0 }
        ? PartitionKeyPaths
        : new[] { PartitionKeyPath
            ?? throw new InvalidOperationException(
                $"Manifest entry '{Name}' has no partitionKeyPath or partitionKeyPaths.") };
}
