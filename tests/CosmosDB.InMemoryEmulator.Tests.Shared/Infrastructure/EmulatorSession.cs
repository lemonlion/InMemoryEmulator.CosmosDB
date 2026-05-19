using System.Collections.Concurrent;
using System.Net.Sockets;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// xUnit collection fixture owning the expensive, long-lived state shared by
/// every emulator-backed integration test: the <see cref="CosmosClient"/>, the
/// parity database, warm-up state, and an HTTP-level concurrency gate.
///
/// One instance per test run — not per test class. Use in-memory runs create
/// this fixture too, but all emulator methods short-circuit because the
/// in-memory path never touches <see cref="EmulatorClient"/> or
/// <see cref="EmulatorDatabase"/>.
/// </summary>
public sealed class EmulatorSession : IAsyncLifetime
{
    internal const string DefaultEndpoint = "https://localhost:8081";
    internal const string Key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    internal const string DatabaseName = "parity-validation";

    // Keep concurrent outbound HTTP requests below the Dockerized Linux
    // emulator's comfortable saturation point. 8 is well below a 10-partition
    // emulator's capacity and low enough for CI's 3-partition config to cope.
    private const int MaxConcurrentHttpRequests = 8;

    private readonly SemaphoreSlim _httpGate = new(MaxConcurrentHttpRequests, MaxConcurrentHttpRequests);

    /// <summary>
    /// Per-run cache of emulator containers keyed by a deterministic name.
    /// Avoids per-test container create/delete churn that exhausts the
    /// emulator's finite partition pool (PARTITION_COUNT) on CI runners.
    /// </summary>
    internal readonly ConcurrentDictionary<string, Container> ContainerCache = new();

    public TestTarget Target { get; }
    public string Endpoint { get; }
    public bool IsEmulator => Target != TestTarget.InMemory;

    // Populated during InitializeAsync when targeting an emulator.
    public CosmosClient? EmulatorClient { get; private set; }
    public Database? EmulatorDatabase { get; private set; }

    /// <summary>
    /// The container manifest read at session init. Source of truth for which
    /// containers exist on the emulator. Empty manifest stub for in-memory runs.
    /// </summary>
    internal EmulatorContainerManifest Manifest { get; private set; } =
        new(Array.Empty<EmulatorContainerSpec>());

    public EmulatorSession()
    {
        Target = Environment.GetEnvironmentVariable("COSMOS_TEST_TARGET")?.ToLowerInvariant() switch
        {
            "emulator-linux" => TestTarget.EmulatorLinux,
            "emulator-windows" => TestTarget.EmulatorWindows,
            _ => TestTarget.InMemory
        };
        Endpoint = Environment.GetEnvironmentVariable("COSMOS_EMULATOR_ENDPOINT") ?? DefaultEndpoint;
    }

    public async ValueTask InitializeAsync()
    {
        if (!IsEmulator) return;

        EmulatorClient = CreateEmulatorClient(Endpoint, _httpGate);

        // Database must already exist (warmup tool creates it). CreateIfNotExists
        // is idempotent so this is cheap when the warmup already ran; useful safety
        // net for local runs where someone forgot to invoke the warmup first.
        var response = await EmulatorRetry.RunAsync(
            () => EmulatorClient.CreateDatabaseIfNotExistsAsync(DatabaseName),
            "CreateDatabase", maxRetries: 30, maxBackoffSeconds: 15);
        EmulatorDatabase = response.Database;

        // Load the manifest and populate ContainerCache with lazy SDK Container
        // references — these don't talk to the emulator until used. Tests then
        // look up by name; nothing is lazy-created.
        var manifestPath = Environment.GetEnvironmentVariable("COSMOS_EMULATOR_MANIFEST")
                           ?? EmulatorContainerManifest.ResolveDefaultPath();
        Manifest = EmulatorContainerManifest.Load(manifestPath);
        foreach (var spec in Manifest.Containers)
        {
            ContainerCache.TryAdd(spec.Name, EmulatorDatabase.GetContainer(spec.Name));
        }
        Console.WriteLine(
            $"[EmulatorSession] Ready ({Manifest.Containers.Count} containers from manifest).");
    }

    public ValueTask DisposeAsync()
    {
        // Containers are not deleted here — the warmup tool creates them; CI
        // emulators are ephemeral and torn down with the runner. Local dev
        // emulators benefit from cached partitions across re-runs.
        ContainerCache.Clear();
        EmulatorClient?.Dispose();
        _httpGate.Dispose();
        return ValueTask.CompletedTask;
    }

    private static CosmosClient CreateEmulatorClient(string endpoint, SemaphoreSlim httpGate)
    {
        var isHttps = endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase);

        var options = new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Gateway,
            // Bumped from the SDK defaults (9 attempts / 30s) to absorb 429/503
            // storms from the Linux emulator under concurrent test load.
            MaxRetryAttemptsOnRateLimitedRequests = 15,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(60),
            RequestTimeout = TimeSpan.FromSeconds(30),
            HttpClientFactory = () =>
            {
                HttpMessageHandler inner = isHttps
                    ? new HttpClientHandler
                    {
                        ServerCertificateCustomValidationCallback =
                            HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                    }
                    : new HttpClientHandler();

                return new HttpClient(new ConcurrencyGateHandler(httpGate, inner))
                {
                    Timeout = TimeSpan.FromSeconds(30)
                };
            }
        };

        return new CosmosClient(endpoint, Key, options);
    }

    /// <summary>
    /// Limits the number of in-flight HTTP requests to the emulator and
    /// short-circuits all requests once the emulator is confirmed dead.
    /// The Cosmos SDK fans out requests aggressively (bulk writes, query
    /// parallelism) and the Dockerized Linux emulator 429s or 503s when
    /// pushed past its partition-service capacity. Rather than tune each
    /// test, we cap at the HTTP layer — transparent to both SDK retry and
    /// individual tests.
    ///
    /// When the emulator crashes, the SDK's own retry loop (15 attempts,
    /// 60s wait) burns through minutes per test. The circuit breaker here
    /// detects consecutive fatal socket errors (connection refused before the
    /// process has bound the port; connection aborted / reset / shutdown after
    /// the process has died mid-connection) and immediately fails all
    /// subsequent requests so the test run aborts quickly.
    /// </summary>
    private sealed class ConcurrencyGateHandler : DelegatingHandler
    {
        private const int CircuitBreakerThreshold = 3;

        private readonly SemaphoreSlim _gate;
        private int _consecutiveDeadSocketErrors;
        private volatile bool _emulatorDead;

        public ConcurrencyGateHandler(SemaphoreSlim gate, HttpMessageHandler inner) : base(inner)
        {
            _gate = gate;
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (_emulatorDead)
                throw new HttpRequestException(
                    "Emulator circuit breaker tripped — the emulator process has crashed. " +
                    "All further requests are short-circuited.");

            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
                Interlocked.Exchange(ref _consecutiveDeadSocketErrors, 0);
                return response;
            }
            catch (HttpRequestException ex) when (IsEmulatorUnreachable(ex))
            {
                if (Interlocked.Increment(ref _consecutiveDeadSocketErrors) >= CircuitBreakerThreshold)
                {
                    _emulatorDead = true;
                    Console.WriteLine(
                        "[ConcurrencyGateHandler] Circuit breaker tripped after " +
                        $"{CircuitBreakerThreshold} consecutive dead-socket errors. " +
                        "The emulator process appears to have crashed.");
                }
                throw;
            }
            finally
            {
                _gate.Release();
            }
        }

        private static bool IsEmulatorUnreachable(HttpRequestException ex) =>
            ex.InnerException is SocketException se && IsDeadSocketError(se.SocketErrorCode);
    }

    /// <summary>
    /// Socket errors that mean the emulator process is no longer reachable —
    /// either the port has no listener (refused) or an established connection
    /// was severed by the peer (aborted / reset / shutdown). Treated as
    /// "emulator dead" by the circuit breakers.
    /// </summary>
    internal static bool IsDeadSocketError(SocketError code) => code is
        SocketError.ConnectionRefused or
        SocketError.ConnectionAborted or
        SocketError.ConnectionReset or
        SocketError.Shutdown;
}

/// <summary>
/// Collection name for tests that share the session. The actual
/// <c>[CollectionDefinition]</c> lives in each test assembly so xUnit's
/// analyzer can see both the attribute and the consumers in one compilation.
/// </summary>
public static class IntegrationCollection
{
    public const string Name = "Integration";
}

/// <summary>
/// Shared retry helper. Covers the transient errors the emulator produces
/// while partition services are still spinning up or while temporarily
/// saturated. <b>NotFound is intentionally excluded</b> — masking real
/// "resource doesn't exist" errors behind 88s of retries hides bugs.
/// </summary>
internal static class EmulatorRetry
{
    /// <summary>
    /// Number of consecutive dead-socket errors before we assume the emulator
    /// has crashed and abort immediately instead of burning through the full
    /// retry budget against a dead process.
    /// </summary>
    private const int EmulatorDownCircuitBreakerThreshold = 3;

    public static async Task<T> RunAsync<T>(
        Func<Task<T>> operation, string operationName, int maxRetries = 10, double maxBackoffSeconds = 10)
    {
        var consecutiveEmulatorDownErrors = 0;

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var result = await operation();
                consecutiveEmulatorDownErrors = 0;
                return result;
            }
            catch (Exception ex) when (attempt < maxRetries && IsTransient(ex))
            {
                if (IsEmulatorUnreachable(ex))
                {
                    consecutiveEmulatorDownErrors++;
                    if (consecutiveEmulatorDownErrors >= EmulatorDownCircuitBreakerThreshold)
                    {
                        throw new InvalidOperationException(
                            $"[EmulatorRetry] {operationName}: aborting after " +
                            $"{consecutiveEmulatorDownErrors} consecutive dead-socket errors. " +
                            "The emulator process appears to have crashed.", ex);
                    }
                }
                else
                {
                    consecutiveEmulatorDownErrors = 0;
                }

                var delay = TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempt), maxBackoffSeconds));
                Console.WriteLine(
                    $"[EmulatorRetry] {operationName} attempt {attempt + 1}/{maxRetries} failed " +
                    $"({ex.GetType().Name}), retrying in {delay.TotalSeconds:F0}s...");
                await Task.Delay(delay);
            }
        }
    }

    private static bool IsTransient(Exception ex) => ex switch
    {
        // 403/1008 = "Database Account does not exist" — the Windows emulator's HTTP server
        // can become reachable before its account is fully initialised. Retry until ready.
        CosmosException ce when ce.StatusCode == System.Net.HttpStatusCode.Forbidden
                              && ce.SubStatusCode == 1008 => true,
        // 404/0 = Windows emulator intermediate startup state: HTTP server is reachable
        // but internal metadata services haven't fully materialised yet.
        CosmosException ce when ce.StatusCode == System.Net.HttpStatusCode.NotFound
                              && ce.SubStatusCode == 0 => true,
        CosmosException ce => ce.StatusCode is
            System.Net.HttpStatusCode.ServiceUnavailable or
            System.Net.HttpStatusCode.InternalServerError or
            System.Net.HttpStatusCode.RequestTimeout or
            System.Net.HttpStatusCode.TooManyRequests,
        HttpRequestException hre => !IsEmulatorUnreachableCore(hre),
        SocketException se => !EmulatorSession.IsDeadSocketError(se.SocketErrorCode),
        _ => ex.InnerException != null && IsTransient(ex.InnerException),
    };

    /// <summary>
    /// Checks the full exception chain for indicators that the emulator
    /// process is unreachable (refused / aborted / reset / shutdown).
    /// All of these mean the process is dead, not temporarily overloaded.
    /// </summary>
    private static bool IsEmulatorUnreachable(Exception ex) => ex switch
    {
        HttpRequestException hre => IsEmulatorUnreachableCore(hre),
        SocketException se => EmulatorSession.IsDeadSocketError(se.SocketErrorCode),
        _ => ex.InnerException != null && IsEmulatorUnreachable(ex.InnerException),
    };

    private static bool IsEmulatorUnreachableCore(HttpRequestException hre) =>
        hre.InnerException is SocketException se && EmulatorSession.IsDeadSocketError(se.SocketErrorCode);
}
