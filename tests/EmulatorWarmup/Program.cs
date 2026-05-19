// Pre-creates every container listed in tests/emulator-containers.json against the
// emulator at $COSMOS_EMULATOR_ENDPOINT, with generous retries so the 503/1007 and
// 404/1013 warmup window is absorbed once per CI run instead of being charged to
// per-test fixtures. Intended to run *after* wait-for-emulator and *before* dotnet test.
//
// Exit codes:
//   0 — all containers created and write+read probe passed
//   1 — manifest unreadable / malformed
//   2 — a container failed to come online within the retry budget
//   3 — a container created but the data-plane probe failed
//
// Usage:
//   dotnet run --project tests/EmulatorWarmup -- <manifest-path>
//   COSMOS_EMULATOR_ENDPOINT=https://localhost:8081 dotnet run --project tests/EmulatorWarmup -- tests/emulator-containers.json

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.Tools.EmulatorWarmup;

internal static class Program
{
    private const string EmulatorKey =
        "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private const string DatabaseName = "parity-validation";

    private static async Task<int> Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.Error.WriteLine("Usage: EmulatorWarmup <manifest-path>");
            return 1;
        }

        Manifest manifest;
        try
        {
            await using var stream = File.OpenRead(args[0]);
            manifest = await JsonSerializer.DeserializeAsync<Manifest>(stream, JsonOpts)
                ?? throw new InvalidOperationException("Manifest deserialised to null");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Failed to read manifest at '{args[0]}': {ex.Message}");
            return 1;
        }

        var endpoint = Environment.GetEnvironmentVariable("COSMOS_EMULATOR_ENDPOINT")
                       ?? "https://localhost:8081";
        Console.WriteLine($"[Warmup] Endpoint={endpoint}, containers={manifest.Containers.Count}");

        using var client = CreateClient(endpoint);
        var totalSw = Stopwatch.StartNew();

        try
        {
            await RetryAsync(
                () => client.CreateDatabaseIfNotExistsAsync(DatabaseName),
                "CreateDatabase", maxAttempts: 60, maxBackoffSeconds: 15);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[Warmup] FATAL: database create failed: {ex.Message}");
            return 2;
        }

        var database = client.GetDatabase(DatabaseName);

        foreach (var spec in manifest.Containers)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await CreateAndProbeAsync(database, spec);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(
                    $"[Warmup] FAILED {spec.Name} after {sw.Elapsed.TotalSeconds:F0}s: {ex.Message}");
                return 3;
            }
            Console.WriteLine($"[Warmup] OK {spec.Name} ({sw.Elapsed.TotalSeconds:F0}s)");
        }

        Console.WriteLine(
            $"[Warmup] All {manifest.Containers.Count} containers ready in {totalSw.Elapsed.TotalSeconds:F0}s");
        return 0;
    }

    private static async Task CreateAndProbeAsync(Database database, ContainerSpec spec)
    {
        var props = BuildProperties(spec);
        await RetryAsync(
            () => database.CreateContainerIfNotExistsAsync(props),
            $"CreateContainer({spec.Name})", maxAttempts: 30, maxBackoffSeconds: 15);

        // Probe the data plane — partition routing for a freshly-created container
        // returns 404/1013 on point reads for tens of seconds after create succeeds.
        // Burning this here once means tests never see the slow-warmup window.
        if (spec.SkipDataPlaneProbe == true)
            return;

        var container = database.GetContainer(spec.Name);
        var probeId = $"__warmup__{Guid.NewGuid():N}";
        var (doc, pk) = BuildProbeDoc(probeId, spec);

        await RetryAsync(
            () => container.UpsertItemAsync(doc, pk),
            $"ProbeUpsert({spec.Name})", maxAttempts: 30, maxBackoffSeconds: 15);

        await RetryAsync(
            () => container.ReadItemAsync<JsonElement>(probeId, pk),
            $"ProbeRead({spec.Name})", maxAttempts: 30, maxBackoffSeconds: 15);

        try
        {
            await container.DeleteItemAsync<object>(probeId, pk);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            // Probe doc may have been read-once-then-gone — best-effort cleanup.
        }
    }

    private static ContainerProperties BuildProperties(ContainerSpec spec)
    {
        ContainerProperties props;
        if (spec.PartitionKeyPaths is { Count: > 0 })
        {
            props = new ContainerProperties(spec.Name, spec.PartitionKeyPaths);
        }
        else if (!string.IsNullOrEmpty(spec.PartitionKeyPath))
        {
            props = new ContainerProperties(spec.Name, spec.PartitionKeyPath);
        }
        else
        {
            throw new InvalidOperationException(
                $"Manifest entry '{spec.Name}' has no partitionKeyPath or partitionKeyPaths.");
        }

        if (spec.DefaultTimeToLive is not null)
            props.DefaultTimeToLive = spec.DefaultTimeToLive;

        if (spec.UniqueKeyPaths is { Count: > 0 })
        {
            var unique = new UniqueKey();
            foreach (var p in spec.UniqueKeyPaths)
                unique.Paths.Add(p);
            props.UniqueKeyPolicy = new UniqueKeyPolicy { UniqueKeys = { unique } };
        }

        return props;
    }

    private static (Dictionary<string, object?> Doc, PartitionKey Pk) BuildProbeDoc(
        string probeId, ContainerSpec spec)
    {
        var doc = new Dictionary<string, object?> { ["id"] = probeId };
        var pkBuilder = new PartitionKeyBuilder();
        var paths = spec.PartitionKeyPaths is { Count: > 0 }
            ? (IReadOnlyList<string>)spec.PartitionKeyPaths
            : new[] { spec.PartitionKeyPath! };

        foreach (var path in paths)
        {
            // Only flat top-level paths supported in the probe. Nested paths (e.g.
            // "/a/b") would need a JObject; manifest entries that need those should
            // set "skipDataPlaneProbe": true.
            var prop = path.TrimStart('/');
            doc[prop] = probeId;
            pkBuilder.Add(probeId);
        }

        return (doc, pkBuilder.Build());
    }

    private static CosmosClient CreateClient(string endpoint)
    {
        var options = new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Gateway,
            // Tight per-request timeout — we'd rather see fast retries than slow waits.
            RequestTimeout = TimeSpan.FromSeconds(30),
            MaxRetryAttemptsOnRateLimitedRequests = 15,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(60),
            HttpClientFactory = () =>
            {
                var inner = endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase)
                    ? new HttpClientHandler
                    {
                        ServerCertificateCustomValidationCallback =
                            HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                    }
                    : new HttpClientHandler();
                return new HttpClient(inner) { Timeout = TimeSpan.FromSeconds(60) };
            }
        };
        return new CosmosClient(endpoint, EmulatorKey, options);
    }

    private static async Task RetryAsync(
        Func<Task> op, string opName, int maxAttempts, double maxBackoffSeconds)
    {
        await RetryAsync<object?>(async () => { await op(); return null; },
            opName, maxAttempts, maxBackoffSeconds);
    }

    private static async Task<T> RetryAsync<T>(
        Func<Task<T>> op, string opName, int maxAttempts, double maxBackoffSeconds)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await op();
            }
            catch (Exception ex) when (attempt < maxAttempts && IsTransient(ex))
            {
                var delay = TimeSpan.FromSeconds(
                    Math.Min(Math.Pow(2, attempt - 1), maxBackoffSeconds));
                Console.WriteLine(
                    $"[Warmup] {opName} attempt {attempt}/{maxAttempts} transient " +
                    $"({ex.GetType().Name}: {Truncate(ex.Message, 120)}), retrying in {delay.TotalSeconds:F0}s");
                await Task.Delay(delay);
            }
        }
    }

    private static bool IsTransient(Exception ex) => ex switch
    {
        // 403/1008 — "Database Account does not exist" (Windows emulator startup).
        CosmosException ce when ce.StatusCode == HttpStatusCode.Forbidden
                              && ce.SubStatusCode == 1008 => true,
        // 404/1013 — "Collection is not yet available for read".
        CosmosException ce when ce.StatusCode == HttpStatusCode.NotFound
                              && ce.SubStatusCode == 1013 => true,
        // 404/0 — Windows emulator intermediate startup state: HTTP server is
        // reachable but internal metadata services haven't fully materialised.
        CosmosException ce when ce.StatusCode == HttpStatusCode.NotFound
                              && ce.SubStatusCode == 0 => true,
        CosmosException ce => ce.StatusCode is
            HttpStatusCode.ServiceUnavailable or
            HttpStatusCode.InternalServerError or
            HttpStatusCode.RequestTimeout or
            HttpStatusCode.TooManyRequests,
        HttpRequestException hre => !(hre.InnerException is SocketException se
                                      && IsFatalSocketError(se.SocketErrorCode)),
        SocketException se => !IsFatalSocketError(se.SocketErrorCode),
        _ => ex.InnerException != null && IsTransient(ex.InnerException),
    };

    private static bool IsFatalSocketError(SocketError code) => code is
        SocketError.ConnectionRefused or
        SocketError.ConnectionAborted or
        SocketError.ConnectionReset or
        SocketError.Shutdown;

    private static string Truncate(string s, int max) =>
        s.Length <= max ? s : s.AsSpan(0, max).ToString() + "…";

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    private sealed record Manifest(
        [property: JsonPropertyName("containers")] List<ContainerSpec> Containers);

    private sealed record ContainerSpec(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("partitionKeyPath")] string? PartitionKeyPath = null,
        [property: JsonPropertyName("partitionKeyPaths")] List<string>? PartitionKeyPaths = null,
        [property: JsonPropertyName("defaultTimeToLive")] int? DefaultTimeToLive = null,
        [property: JsonPropertyName("uniqueKeyPaths")] List<string>? UniqueKeyPaths = null,
        [property: JsonPropertyName("skipDataPlaneProbe")] bool? SkipDataPlaneProbe = null);
}
