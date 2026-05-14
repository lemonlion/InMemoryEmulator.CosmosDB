using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.Cosmos.Serialization.HybridRow;
using Microsoft.Azure.Cosmos.Serialization.HybridRow.IO;
using Microsoft.Azure.Cosmos.Serialization.HybridRow.Layouts;
using Microsoft.Azure.Cosmos.Serialization.HybridRow.RecordIO;
using HybridRowSchemas = Microsoft.Azure.Cosmos.Serialization.HybridRow.Schemas;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// A custom <see cref="HttpMessageHandler"/> that intercepts all Cosmos SDK HTTP
/// requests and serves responses from an <see cref="InMemoryContainer"/>, including
/// account metadata, collection metadata, partition key ranges, and query execution.
/// </summary>
/// <remarks>
/// <para>
/// Documents are distributed across partition key ranges using a deterministic MurmurHash3
/// hash of the document's partition key value. Each range receives only the documents
/// whose hash maps to it. The SDK fans out queries to all ranges and merges the results.
/// </para>
/// <para>
/// ResourceIds are generated via direct byte construction with deterministic IDs.
/// </para>
/// <para>
/// Fault injection: set <see cref="FaultInjector"/> to a delegate that inspects incoming
/// requests and optionally returns an error response. When the delegate returns a non-null
/// <see cref="HttpResponseMessage"/>, that response is returned immediately without
/// executing the normal handler logic. This enables testing retry policies, rate-limiting
/// (429), transient failures (503), and timeout scenarios.
/// </para>
/// <para>
/// Multi-container: use <see cref="CreateRouter"/> to create a routing handler that
/// dispatches requests to different handler instances based on the container name in the
/// URL path. This allows a single <see cref="CosmosClient"/> to query multiple containers.
/// </para>
/// <para>
/// SDK compatibility: call <see cref="VerifySdkCompatibilityAsync"/> once during test suite
/// setup to detect breaking changes in the Cosmos SDK's internal HTTP contract before they
/// cause silent data corruption.
/// </para>
/// </remarks>
/// <summary>
/// Configuration options for <see cref="FakeCosmosHandler"/>.
/// </summary>
public sealed class FakeCosmosHandlerOptions
{
    /// <summary>
    /// How long query result cache entries remain valid before eviction.
    /// Defaults to 5 minutes.
    /// </summary>
    public TimeSpan CacheTtl { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum number of query result cache entries before overflow eviction.
    /// Defaults to 100.
    /// </summary>
    public int CacheMaxEntries { get; init; } = 100;

    /// <summary>
    /// Number of partition key ranges to expose. The SDK sends a separate query
    /// per range; the handler serves the same data regardless. Increasing this
    /// exercises cross-partition fan-out paths in the SDK.
    /// Defaults to 1.
    /// </summary>
    public int PartitionKeyRangeCount { get; init; } = 1;

    /// <summary>
    /// Strategy for building query plan responses. Override to customise or fix
    /// query plan generation if a new SDK version changes the expected format.
    /// Defaults to <see cref="DefaultQueryPlanStrategy"/>.
    /// </summary>
    public IQueryPlanStrategy QueryPlanStrategy { get; init; } = new DefaultQueryPlanStrategy();

    /// <summary>
    /// Strategy for resolving HybridRow batch schemas. Override to customise
    /// batch schema resolution if a new SDK version changes the batch wire format.
    /// Defaults to <see cref="DefaultBatchSchemaStrategy"/>.
    /// </summary>
    public IBatchSchemaStrategy BatchSchemaStrategy { get; init; } = new DefaultBatchSchemaStrategy();
}

/// <summary>
/// Document type used by <see cref="FakeCosmosHandler.VerifySdkCompatibilityAsync"/>
/// to validate SDK compatibility. Public because NSubstitute proxy generation
/// requires accessible types.
/// </summary>
public sealed class CompatibilityDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = "";

    [JsonProperty("partitionKey")]
    public string PartitionKey { get; set; } = "";

    [JsonProperty("name")]
    public string Name { get; set; } = "";

    [JsonProperty("value")]
    public int Value { get; set; }
}

public class FakeCosmosHandler : HttpMessageHandler
{
    private readonly InMemoryContainer _container;
    private readonly ConcurrentBag<string> _requestLog = new();
    private readonly ConcurrentBag<string> _queryLog = new();
    private readonly ConcurrentBag<string> _unrecognisedHeaders = new();
    private readonly List<string> _sdkVersionWarnings = new();
    private readonly QueryResultCache _queryResultCache;
    private readonly string _collectionRid;
    private readonly string _databaseRid;
    private readonly int _partitionKeyRangeCount;
    private readonly string _partitionKeyPath;
    private readonly IQueryPlanStrategy _queryPlanStrategy;
    private readonly IBatchSchemaStrategy _batchSchemaStrategy;
    private static int _ridCounter;
    private const string PkRangesEtag = "\"pk-etag-1\"";

    /// <summary>
    /// The version of the <c>PartitionedQueryExecutionInfo</c> format returned by query plan responses.
    /// If the SDK starts expecting a different version, queries may fail.
    /// </summary>
    public const int QueryPlanVersion = 2;

    /// <summary>Minimum Cosmos SDK version that has been tested with this handler.</summary>
    public static readonly Version MinTestedSdkVersion = new(3, 35, 0, 0);

    /// <summary>Maximum Cosmos SDK version that has been tested with this handler.</summary>
    public static readonly Version MaxTestedSdkVersion = new(3, 58, 0, 0);

    /// <summary>
    /// Set of <c>x-ms-*</c> request headers that this handler recognises and processes.
    /// Any <c>x-ms-*</c> header not in this set is recorded in <see cref="UnrecognisedHeaders"/>.
    /// </summary>
    private static readonly HashSet<string> KnownRequestHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "A-IM",
        "x-ms-documentdb-partitionkey",
        "x-ms-documentdb-isquery",
        "x-ms-documentdb-is-upsert",
        "x-ms-cosmos-is-batch-request",
        "x-ms-cosmos-is-query-plan-request",
        "x-ms-max-item-count",
        "x-ms-continuation",
        "x-ms-documentdb-partitionkeyrangeid",
        "x-ms-date",
        "x-ms-version",
        "x-ms-documentdb-query-enablecrosspartition",
        "x-ms-documentdb-query-iscontinuationexpected",
        "x-ms-documentdb-query-parallelizecrosspartitionquery",
        "x-ms-documentdb-populatequerymetrics",
        "x-ms-cosmos-correlated-activityid",
        "x-ms-activity-id",
        "x-ms-cosmos-sdk-supportedcapabilities",
        "x-ms-session-token",
        "x-ms-consistency-level",
        "x-ms-request-charge",
        "x-ms-cosmos-internal-is-query",
        "x-ms-cosmos-query-version",
        "x-ms-documentdb-collection-rid",
        "x-ms-cosmos-batch-ordered-response",
        "x-ms-cosmos-batch-atomic",
        "x-ms-cosmos-batch-continue-on-error",
        "x-ms-cosmos-sdk-version",
        "x-ms-cosmos-intended-collection-rid",
        "x-ms-cosmos-priority-level",
        "x-ms-cosmos-allow-tentative-writes",
        "x-ms-cosmos-physical-partition-count",
        "x-ms-documentdb-responsecontinuationtokenlimitinkb",
        "x-ms-documentdb-content-serialization-format",
        "x-ms-cosmos-query-optimisticdirectexecute",
        "x-ms-cosmos-supported-serialization-formats",
        "x-ms-cosmos-supported-query-features",
    };

    /// <summary>
    /// AsyncLocal override for partition key. When set, <see cref="HandleQueryAsync"/>
    /// and <see cref="HandleReadFeedAsync"/> use this value when the standard
    /// <c>x-ms-documentdb-partitionkey</c> header is absent. This is necessary because
    /// the Cosmos SDK does not send the partition key header for prefix (hierarchical)
    /// partition key queries — it routes by partition key range ID instead.
    /// Used internally by the <see cref="PartitionKeyCapturingContainer"/> decorator.
    /// </summary>
    private readonly AsyncLocal<PartitionKey?> _partitionKeyOverride = new();

    /// <summary>
    /// Separate AsyncLocal for the LINQ <c>.ToFeedIterator()</c> path.
    /// Set eagerly by <see cref="PartitionKeyCapturingContainer"/> when
    /// <c>GetItemLinqQueryable</c> is called with a prefix partition key,
    /// because the <see cref="FeedIterator{T}"/> produced by <c>.ToFeedIterator()</c> is
    /// SDK-internal and cannot be wrapped. Uses a lower priority than
    /// <see cref="_partitionKeyOverride"/> so that <see cref="PkCapturingFeedIterator{T}"/>
    /// (used by <c>GetItemQueryIterator</c>) takes precedence when both are set.
    /// </summary>
    private readonly AsyncLocal<PartitionKey?> _partitionKeyHint = new();

    /// <summary>
    /// Sets a partition key hint for the LINQ <c>.ToFeedIterator()</c> path.
    /// Unlike the scoped override, this value persists until overwritten
    /// or the async context ends. It is used as a fallback when neither the HTTP
    /// partition key header nor the scoped override is present.
    /// </summary>
    internal void SetPartitionKeyHint(PartitionKey? partitionKey)
    {
        _partitionKeyHint.Value = partitionKey;
    }

    /// <summary>
    /// Sets a scoped partition key override for the current async context.
    /// Used internally by <see cref="PkCapturingFeedIterator{T}"/> to forward
    /// the partition key captured at the Container API surface.
    /// </summary>
    internal IDisposable WithPartitionKey(PartitionKey partitionKey)
    {
        _partitionKeyOverride.Value = partitionKey;
        return new PartitionKeyScope(_partitionKeyOverride);
    }

    private sealed class PartitionKeyScope(AsyncLocal<PartitionKey?> asyncLocal) : IDisposable
    {
        public void Dispose() => asyncLocal.Value = null;
    }

    /// <summary>Recorded HTTP requests in the form "METHOD /path".</summary>
    public IReadOnlyCollection<string> RequestLog => _requestLog;

    /// <summary>The backing in-memory container that stores all data for this handler.</summary>
    public Container BackingContainer => _container;

    /// <summary>Internal typed access to the backing container.</summary>
    internal InMemoryContainer BackingInMemoryContainer => _container;

    /// <summary>Recorded SQL query strings that were executed.</summary>
    public IReadOnlyCollection<string> QueryLog => _queryLog;

    /// <summary>
    /// <c>x-ms-*</c> request headers seen during request processing that are not in the
    /// known headers set. Populated as requests flow through <see cref="SendAsync"/>.
    /// Check this collection after a test run to detect new SDK headers that may need handling.
    /// </summary>
    public IReadOnlyCollection<string> UnrecognisedHeaders => _unrecognisedHeaders;

    /// <summary>
    /// Warnings generated during handler construction if the current Cosmos SDK version
    /// falls outside the tested range (<see cref="MinTestedSdkVersion"/> to
    /// <see cref="MaxTestedSdkVersion"/>).
    /// </summary>
    public IReadOnlyList<string> SdkVersionWarnings => _sdkVersionWarnings;

    /// <summary>
    /// Optional fault injection delegate. When set, it is called before normal request
    /// handling. If it returns a non-null response, that response is used immediately.
    /// By default, metadata requests (account, collection, pkranges) are excluded to avoid
    /// breaking SDK initialisation. Set <see cref="FaultInjectorIncludesMetadata"/> to
    /// <c>true</c> to also affect metadata routes.
    /// </summary>
    public Func<HttpRequestMessage, HttpResponseMessage?>? FaultInjector { get; set; }

    /// <summary>
    /// When <c>true</c>, the <see cref="FaultInjector"/> delegate is also invoked for
    /// metadata requests (account info, collection metadata, partition key ranges).
    /// Defaults to <c>false</c> so SDK initialisation is not disrupted.
    /// </summary>
    public bool FaultInjectorIncludesMetadata { get; set; }

    internal FakeCosmosHandler(InMemoryContainer container)
        : this(container, new FakeCosmosHandlerOptions())
    {
    }

    internal FakeCosmosHandler(InMemoryContainer container, FakeCosmosHandlerOptions options)
    {
        _container = container;
        _partitionKeyRangeCount = Math.Max(1, options.PartitionKeyRangeCount);
        _queryResultCache = new QueryResultCache(options.CacheTtl, options.CacheMaxEntries);
        (_databaseRid, _collectionRid) = GenerateResourceIds(container.Id);
        _partitionKeyPath = container.PartitionKeyPaths.FirstOrDefault()?.TrimStart('/') ?? "id";
        _queryPlanStrategy = options.QueryPlanStrategy;
        _batchSchemaStrategy = options.BatchSchemaStrategy;
        CheckSdkVersion();
    }

    private void CheckSdkVersion()
    {
        var sdkVersion = typeof(CosmosClient).Assembly.GetName().Version;
        if (sdkVersion is null) return;

        if (sdkVersion < MinTestedSdkVersion)
        {
            var warning = $"FakeCosmosHandler: Cosmos SDK {sdkVersion} is older than the minimum tested version " +
                $"({MinTestedSdkVersion}). Some features may not work correctly. " +
                $"Call VerifySdkCompatibilityAsync() to check for compatibility.";
            _sdkVersionWarnings.Add(warning);
            System.Diagnostics.Trace.TraceWarning(warning);
        }
        else if (sdkVersion > MaxTestedSdkVersion)
        {
            var warning = $"FakeCosmosHandler: Cosmos SDK {sdkVersion} is newer than the last tested version " +
                $"({MaxTestedSdkVersion}). Call VerifySdkCompatibilityAsync() in your test setup " +
                $"to check for compatibility.";
            _sdkVersionWarnings.Add(warning);
            System.Diagnostics.Trace.TraceWarning(warning);
        }
    }

    /// <summary>
    /// Returns true if the HybridRow batch schemas could be resolved. Used by
    /// <see cref="DefaultBatchSchemaStrategy"/> to probe availability without throwing.
    /// </summary>
    internal static bool BatchSchemasAvailable
    {
        get
        {
            _ = BatchSchemas.OperationLayout;
            return true;
        }
    }

    private static (string DbRid, string CollRid) GenerateResourceIds(string containerId)
    {
        // Cosmos RID format: DB = 4-byte little-endian uint, Collection = DB bytes + 4 more bytes.
        // Use atomic counter for the DB portion so every handler instance gets a unique RID,
        // even if containers share the same name. Collection portion uses MurmurHash3 of the ID.
        var instanceId = (uint)Interlocked.Increment(ref _ridCounter);
        var dbBytes = BitConverter.GetBytes(instanceId);
        var containerHash = PartitionKeyHash.MurmurHash3(containerId);
        var collBytes = new byte[8];
        Buffer.BlockCopy(dbBytes, 0, collBytes, 0, 4);
        Buffer.BlockCopy(BitConverter.GetBytes(containerHash), 0, collBytes, 4, 4);
        return (Convert.ToBase64String(dbBytes), Convert.ToBase64String(collBytes));
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var path = request.RequestUri?.AbsolutePath ?? "";
        var method = request.Method.Method;
        _requestLog.Add($"{method} {path}");

        // Detect unrecognised x-ms-* headers for SDK compatibility diagnostics.
        foreach (var header in request.Headers)
        {
            if (header.Key.StartsWith("x-ms-", StringComparison.OrdinalIgnoreCase) &&
                !KnownRequestHeaders.Contains(header.Key))
            {
                _unrecognisedHeaders.Add(header.Key);
            }
        }

        // Buffer request content so FaultInjector can safely read it without
        // consuming the stream that the handler needs later.
        if (FaultInjector is not null && request.Content is not null)
        {
            var body = await request.Content.ReadAsStringAsync(cancellationToken);
            var mediaType = request.Content.Headers.ContentType?.MediaType ?? "application/json";
            request.Content = new StringContent(body, Encoding.UTF8, mediaType);
        }

        if (FaultInjectorIncludesMetadata && FaultInjector is not null)
        {
            var earlyFault = FaultInjector(request);
            if (earlyFault is not null)
            {
                return earlyFault;
            }
        }

        if (method == "GET" && path is "/" or "")
        {
            return CreateJsonResponse(AccountMetadata);
        }

        if (path.Contains("/pkranges"))
        {
            return HandlePartitionKeyRanges(request);
        }

        if (path.Contains("/colls/") && !path.Contains("/docs"))
        {
            if (method == "GET")
            {
                return CreateJsonResponse(GetCollectionMetadata());
            }

            // PUT/DELETE only for container-level paths (not sub-resources like /sprocs, /triggers)
            if (Regex.IsMatch(path, @"/colls/[^/]+/?$"))
            {
                if (method == "PUT")
                    return CreateJsonResponse(GetCollectionMetadata());
                if (method == "DELETE")
                    return CreateNoContentResponse();
            }
        }

        // Database management: /dbs (list/create)
        if (Regex.IsMatch(path, @"^/dbs/?$"))
        {
            if (method == "POST")
            {
                var body = request.Content is not null
                    ? await request.Content.ReadAsStringAsync(cancellationToken)
                    : "{}";
                var dbName = JObject.Parse(body)["id"]?.ToString() ?? "fake-db";
                return CreateJsonResponse(GetDatabaseMetadata(dbName), HttpStatusCode.Created);
            }

            // GET /dbs → list databases
            var dbList = new JObject
            {
                ["_rid"] = "",
                ["Databases"] = new JArray(JObject.Parse(GetDatabaseMetadata("fake-db"))),
                ["_count"] = 1
            };
            return CreateJsonResponse(dbList.ToString(Formatting.None));
        }

        // Database CRUD: /dbs/{id} (read/replace/delete)
        if (Regex.IsMatch(path, @"^/dbs/[^/]+/?$"))
        {
            var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var dbName = segments.Length > 1 ? Uri.UnescapeDataString(segments[1]) : "fake-db";
            return method switch
            {
                "GET" or "PUT" => CreateJsonResponse(GetDatabaseMetadata(dbName)),
                "DELETE" => CreateNoContentResponse(),
                _ => new HttpResponseMessage(HttpStatusCode.MethodNotAllowed)
            };
        }

        // Container management: /dbs/{id}/colls (list/create containers)
        if (Regex.IsMatch(path, @"^/dbs/[^/]+/colls/?$"))
        {
            if (method == "POST")
            {
                return CreateJsonResponse(GetCollectionMetadata(), HttpStatusCode.Created);
            }

            // GET /dbs/{id}/colls → list containers
            var containerList = new JObject
            {
                ["_rid"] = _databaseRid,
                ["DocumentCollections"] = new JArray(JObject.Parse(GetCollectionMetadata())),
                ["_count"] = 1
            };
            return CreateJsonResponse(containerList.ToString(Formatting.None));
        }

        if (!FaultInjectorIncludesMetadata && FaultInjector is not null)
        {
            var faultResponse = FaultInjector(request);
            if (faultResponse is not null)
            {
                return faultResponse;
            }
        }

        // Document-specific routes: /docs/{id} (point read, replace, delete, patch)
        if (path.Contains("/docs/") && HasDocumentId(path))
        {
            switch (method)
            {
                case "GET":
                    return await HandlePointReadAsync(request, cancellationToken);
                case "PUT":
                    return await HandleReplaceAsync(request, cancellationToken);
                case "DELETE":
                    return await HandleDeleteAsync(request, cancellationToken);
                case "PATCH":
                    return await HandlePatchAsync(request, cancellationToken);
            }
        }

        // POST /docs (overloaded: batch, query plan, query, upsert, create)
        if (method == "POST" && path.Contains("/docs"))
        {
            if (IsBatchRequest(request))
            {
                return await HandleBatchAsync(request, cancellationToken);
            }

            if (request.Headers.TryGetValues("x-ms-cosmos-is-query-plan-request", out var qpValues) &&
                qpValues.Any(v => v.Equals("True", StringComparison.OrdinalIgnoreCase)))
            {
                return await HandleQueryPlanAsync(request, cancellationToken);
            }

            if (IsQueryRequest(request))
            {
                return await HandleQueryAsync(request, cancellationToken);
            }

            if (IsUpsertRequest(request))
            {
                return await HandleUpsertAsync(request, cancellationToken);
            }

            return await HandleCreateAsync(request, cancellationToken);
        }

        if (method == "GET" && path.Contains("/docs"))
        {
            return await HandleReadFeedAsync(request, cancellationToken);
        }

        // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/querying-offers
        //   ReadThroughputAsync queries the /offers endpoint to retrieve provisioned throughput.
        if (path.Contains("/offers"))
        {
            return HandleOffersRequest(request);
        }

        return new HttpResponseMessage(HttpStatusCode.NotFound)
        {
            Content = new StringContent(
                $"{{\"message\":\"FakeCosmosHandler: unrecognised route {method} {path}\"}}",
                Encoding.UTF8,
                "application/json")
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  CRUD route handlers
    // ═══════════════════════════════════════════════════════════════════════════

    private async Task<HttpResponseMessage> HandleCreateAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
        var result = await _container.CreateItemStreamAsync(stream, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandleUpsertAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
        var result = await _container.UpsertItemStreamAsync(stream, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandlePointReadAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = ExtractDocumentId(request);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        var result = await _container.ReadItemStreamAsync(id, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandleReplaceAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = ExtractDocumentId(request);
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(body));
        var result = await _container.ReplaceItemStreamAsync(stream, id, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandleDeleteAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = ExtractDocumentId(request);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        var result = await _container.DeleteItemStreamAsync(id, pk, BuildItemRequestOptions(request), cancellationToken);
        return ConvertToHttpResponse(result);
    }

    private async Task<HttpResponseMessage> HandlePatchAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = ExtractDocumentId(request);
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;
        var (operations, condition) = ParsePatchBody(body);
        var options = new PatchItemRequestOptions();
        if (condition is not null)
        {
            options.FilterPredicate = condition;
        }
        if (request.Headers.IfMatch.Any())
        {
            options.IfMatchEtag = request.Headers.IfMatch.First().Tag;
        }
        var result = await _container.PatchItemStreamAsync(id, pk, operations, options, cancellationToken);
        return ConvertToHttpResponse(result);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Transactional Batch handler
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Cosmos OperationType enum values used in the HybridRow batch wire protocol.
    /// Values from Microsoft.Azure.Documents.OperationType (in Microsoft.Azure.Cosmos.Direct assembly).
    /// </summary>
    private static class BatchOperationTypes
    {
        public const int Create = 0;
        public const int Patch = 1;
        public const int Read = 2;
        public const int Delete = 4;
        public const int Replace = 5;
        public const int Upsert = 20;
    }

    /// <summary>
    /// Lazily resolved HybridRow schema objects for batch request/response serialization.
    /// Constructs schemas from scratch using public HybridRow APIs to eliminate fragile
    /// reflection into SDK internals (BatchSchemaProvider). Falls back to reflection
    /// if the self-built approach fails, with clear error messages.
    /// </summary>
    private static class BatchSchemas
    {
        private static readonly Lazy<(Layout OperationLayout, Layout ResultLayout, LayoutResolverNamespace Resolver)> _schemas =
            new(BuildSchemas);

        public static Layout OperationLayout => _schemas.Value.OperationLayout;
        public static Layout ResultLayout => _schemas.Value.ResultLayout;
        public static LayoutResolverNamespace Resolver => _schemas.Value.Resolver;

        private static (Layout, Layout, LayoutResolverNamespace) BuildSchemas()
        {
            try
            {
                return BuildSchemasFromDefinition();
            }
            catch (Exception ex)
            {
                // Self-built schema construction failed. Fall back to reflecting into the
                // SDK's internal BatchSchemaProvider as a last resort.
                try
                {
                    return BuildSchemasFromReflection();
                }
                catch (Exception reflectionEx)
                {
                    throw new InvalidOperationException(
                        $"Failed to initialise HybridRow batch schemas. " +
                        $"Self-built schema error: {ex.Message}. " +
                        $"Reflection fallback error: {reflectionEx.Message}. " +
                        $"This may indicate an incompatible Cosmos SDK version " +
                        $"({typeof(CosmosClient).Assembly.GetName().Version}). " +
                        $"Batch operations will not work.", ex);
                }
            }
        }

        /// <summary>
        /// Build batch schemas from scratch using public HybridRow APIs.
        /// This approach defines the BatchOperation and BatchResult schemas programmatically,
        /// matching the wire format the Cosmos SDK expects, without reflecting into any internal types.
        /// </summary>
        private static (Layout, Layout, LayoutResolverNamespace) BuildSchemasFromDefinition()
        {
            var batchOperationSchema = new HybridRowSchemas.Schema
            {
                Name = "BatchOperation",
                SchemaId = new SchemaId(2145473648),
                Type = HybridRowSchemas.TypeKind.Schema,
                Properties =
                {
                    new HybridRowSchemas.Property { Path = "operationType", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Int32) { Storage = HybridRowSchemas.StorageKind.Fixed, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "resourceType", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Int32) { Storage = HybridRowSchemas.StorageKind.Fixed, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "partitionKey", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Utf8) { Storage = HybridRowSchemas.StorageKind.Variable, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "effectivePartitionKey", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Binary) { Storage = HybridRowSchemas.StorageKind.Variable, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "id", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Utf8) { Storage = HybridRowSchemas.StorageKind.Variable, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "binaryId", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Binary) { Storage = HybridRowSchemas.StorageKind.Variable, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "resourceBody", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Binary) { Storage = HybridRowSchemas.StorageKind.Variable, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "indexingDirective", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Utf8) { Storage = HybridRowSchemas.StorageKind.Sparse, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "ifMatch", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Utf8) { Storage = HybridRowSchemas.StorageKind.Sparse, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "ifNoneMatch", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Utf8) { Storage = HybridRowSchemas.StorageKind.Sparse, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "timeToLiveInSeconds", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Int32) { Storage = HybridRowSchemas.StorageKind.Sparse, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "minimalReturnPreference", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Boolean) { Storage = HybridRowSchemas.StorageKind.Sparse, Nullable = true } },
                },
            };

            var batchResultSchema = new HybridRowSchemas.Schema
            {
                Name = "BatchResult",
                SchemaId = new SchemaId(2145473649),
                Type = HybridRowSchemas.TypeKind.Schema,
                Properties =
                {
                    new HybridRowSchemas.Property { Path = "statusCode", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Int32) { Storage = HybridRowSchemas.StorageKind.Fixed, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "subStatusCode", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Int32) { Storage = HybridRowSchemas.StorageKind.Fixed, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "eTag", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Utf8) { Storage = HybridRowSchemas.StorageKind.Variable, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "resourceBody", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Binary) { Storage = HybridRowSchemas.StorageKind.Variable, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "retryAfterMilliseconds", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.UInt32) { Storage = HybridRowSchemas.StorageKind.Sparse, Nullable = true } },
                    new HybridRowSchemas.Property { Path = "requestCharge", PropertyType = new HybridRowSchemas.PrimitivePropertyType(HybridRowSchemas.TypeKind.Float64) { Storage = HybridRowSchemas.StorageKind.Sparse, Nullable = true } },
                },
            };

            var ns = new HybridRowSchemas.Namespace
            {
                Name = "Microsoft.Azure.Cosmos.BatchApi",
                Version = HybridRowSchemas.SchemaLanguageVersion.V2,
                Schemas = { batchOperationSchema, batchResultSchema },
            };

            var resolver = new LayoutResolverNamespace(ns, SystemSchema.LayoutResolver);
            var opLayout = resolver.Resolve(batchOperationSchema.SchemaId);
            var resultLayout = resolver.Resolve(batchResultSchema.SchemaId);
            return (opLayout, resultLayout, resolver);
        }

        /// <summary>
        /// Fallback: resolve batch schemas via reflection into the SDK's internal BatchSchemaProvider.
        /// </summary>
        private static (Layout, Layout, LayoutResolverNamespace) BuildSchemasFromReflection()
        {
            var bspType = typeof(CosmosClient).Assembly.GetType("Microsoft.Azure.Cosmos.BatchSchemaProvider")
                ?? throw new InvalidOperationException(
                    "Cannot find BatchSchemaProvider in the Cosmos SDK assembly. " +
                    "The SDK may have reorganised its internal types.");
            var flags = System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static;
            var opLayout = (Layout)(bspType.GetProperty("BatchOperationLayout", flags)?.GetValue(null)
                ?? throw new InvalidOperationException("BatchSchemaProvider.BatchOperationLayout not found."));
            var resultLayout = (Layout)(bspType.GetProperty("BatchResultLayout", flags)?.GetValue(null)
                ?? throw new InvalidOperationException("BatchSchemaProvider.BatchResultLayout not found."));
            var resolver = (LayoutResolverNamespace)(bspType.GetProperty("BatchLayoutResolver", flags)?.GetValue(null)
                ?? throw new InvalidOperationException("BatchSchemaProvider.BatchLayoutResolver not found."));
            return (opLayout, resultLayout, resolver);
        }
    }

    private record struct BatchOperation(int OperationType, string? Id, byte[]? ResourceBody, string? IfMatch, string? IfNoneMatch);

    private async Task<HttpResponseMessage> HandleBatchAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        // Check if batch schemas are available before attempting to parse
        if (!_batchSchemaStrategy.IsAvailable)
        {
            return new HttpResponseMessage(HttpStatusCode.NotImplemented)
            {
                Content = new StringContent(
                    $"{{\"message\":\"{(_batchSchemaStrategy.UnavailableReason ?? "Batch schemas unavailable").Replace("\"", "\\\"")}\"}}", 
                    Encoding.UTF8, "application/json")
            };
        }

        var pk = ExtractPartitionKey(request) ?? PartitionKey.None;

        // Parse the HybridRow/RecordIO binary request body to extract batch operations.
        var batchOps = new List<BatchOperation>();
        if (request.Content is not null)
        {
            var bodyBytes = await request.Content.ReadAsByteArrayAsync(cancellationToken);
            if (bodyBytes.Length == 0)
            {
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent("{\"message\":\"Empty batch request body.\"}", Encoding.UTF8, "application/json")
                };
            }

            using var bodyStream = new MemoryStream(bodyBytes);
            var batchLayoutResolver = BatchSchemas.Resolver;

            Func<ReadOnlyMemory<byte>, Result> recordVisitor = (ReadOnlyMemory<byte> record) =>
            {
                var row = new RowBuffer(record.Length);
                if (!row.ReadFrom(record.Span, HybridRowVersion.V1, batchLayoutResolver))
                    return Result.Failure;

                var reader = new RowReader(ref row);
                int opType = -1;
                string? id = null;
                byte[]? resourceBody = null;
                string? ifMatch = null;
                string? ifNoneMatch = null;

                while (reader.Read())
                {
                    switch (reader.Path)
                    {
                        case "operationType":
                            reader.ReadInt32(out int ot);
                            opType = ot;
                            break;
                        case "id":
                            reader.ReadString(out string idVal);
                            id = idVal;
                            break;
                        case "resourceBody":
                            reader.ReadBinary(out byte[] rb);
                            resourceBody = rb;
                            break;
                        case "ifMatch":
                            reader.ReadString(out string im);
                            ifMatch = im;
                            break;
                        case "ifNoneMatch":
                            reader.ReadString(out string inm);
                            ifNoneMatch = inm;
                            break;
                    }
                }

                batchOps.Add(new BatchOperation(opType, id, resourceBody, ifMatch, ifNoneMatch));
                return Result.Success;
            };

            var parseResult = await bodyStream.ReadRecordIOAsync(
                recordVisitor, resizer: new MemorySpanResizer<byte>(1024));

            if (parseResult != Result.Success)
            {
                return new HttpResponseMessage(HttpStatusCode.BadRequest)
                {
                    Content = new StringContent(
                        $"{{\"message\":\"Failed to parse batch request body.\"}}",
                        Encoding.UTF8, "application/json")
                };
            }
        }

        if (batchOps.Count == 0)
        {
            return new HttpResponseMessage(HttpStatusCode.BadRequest)
            {
                Content = new StringContent("{\"message\":\"Empty batch request.\"}", Encoding.UTF8, "application/json")
            };
        }

        // Execute the batch operations atomically using InMemoryTransactionalBatch.
        var batch = _container.CreateTransactionalBatch(pk);
        foreach (var op in batchOps)
        {
            switch (op.OperationType)
            {
                case BatchOperationTypes.Create:
                    if (op.ResourceBody is not null)
                        batch.CreateItemStream(new MemoryStream(op.ResourceBody), BuildBatchItemRequestOptions(op));
                    break;
                case BatchOperationTypes.Upsert:
                    if (op.ResourceBody is not null)
                        batch.UpsertItemStream(new MemoryStream(op.ResourceBody), BuildBatchItemRequestOptions(op));
                    break;
                case BatchOperationTypes.Replace:
                    if (op.Id is not null && op.ResourceBody is not null)
                        batch.ReplaceItemStream(op.Id, new MemoryStream(op.ResourceBody), BuildBatchItemRequestOptions(op));
                    break;
                case BatchOperationTypes.Delete:
                    if (op.Id is not null)
                        batch.DeleteItem(op.Id, BuildBatchItemRequestOptions(op));
                    break;
                case BatchOperationTypes.Read:
                    if (op.Id is not null)
                        batch.ReadItem(op.Id, BuildBatchItemRequestOptions(op));
                    break;
                case BatchOperationTypes.Patch:
                    if (op.Id is not null && op.ResourceBody is not null)
                    {
                        var patchJson = Encoding.UTF8.GetString(op.ResourceBody);
                        var (patchOps, condition) = ParsePatchBody(patchJson);
                        var patchOptions = new TransactionalBatchPatchItemRequestOptions();
                        if (condition is not null) patchOptions.FilterPredicate = condition;
                        if (op.IfMatch is not null) patchOptions.IfMatchEtag = op.IfMatch;
                        batch.PatchItem(op.Id, patchOps, patchOptions);
                    }
                    break;
            }
        }

        var batchResponse = await batch.ExecuteAsync(cancellationToken);

        // Build the HybridRow/RecordIO binary response.
        var batchResultLayout = BatchSchemas.ResultLayout;
        var batchLayoutResolverForResponse = BatchSchemas.Resolver;

        using var responseStream = new MemoryStream();
        var resizer = new MemorySpanResizer<byte>(256);

        await RecordIOStream.WriteRecordIOAsync(
            responseStream,
            default(Segment),
            (long index, out ReadOnlyMemory<byte> buffer) =>
            {
                if (index >= batchResponse.Count)
                {
                    buffer = default;
                    return Result.Success;
                }

                var opResult = batchResponse[(int)index];
                var row = new RowBuffer(256, resizer);
                row.InitLayout(HybridRowVersion.V1, batchResultLayout, batchLayoutResolverForResponse);
                var r = RowWriter.WriteBuffer(ref row, opResult, (ref RowWriter writer, TypeArgument _, TransactionalBatchOperationResult result) =>
                {
                    Result wr;
                    wr = writer.WriteInt32("statusCode", (int)result.StatusCode);
                    if (wr != Result.Success) return wr;

                    if (result.ETag is not null)
                    {
                        wr = writer.WriteString("eTag", result.ETag);
                        if (wr != Result.Success) return wr;
                    }

                    if (result.ResourceStream is not null)
                    {
                        using var ms = new MemoryStream();
                        result.ResourceStream.CopyTo(ms);
                        result.ResourceStream.Position = 0;
                        wr = writer.WriteBinary("resourceBody", ms.ToArray());
                        if (wr != Result.Success) return wr;
                    }

                    wr = writer.WriteFloat64("requestCharge", 1.0);
                    if (wr != Result.Success) return wr;

                    return Result.Success;
                });

                if (r != Result.Success)
                {
                    buffer = default;
                    return r;
                }

                buffer = resizer.Memory.Slice(0, row.Length);
                return Result.Success;
            },
            new MemorySpanResizer<byte>(128));

        var httpResponse = new HttpResponseMessage(batchResponse.StatusCode);
        httpResponse.Content = new ByteArrayContent(responseStream.ToArray());
        httpResponse.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
        httpResponse.Headers.Add("x-ms-request-charge", "1");
        httpResponse.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        httpResponse.Headers.Add("x-ms-session-token", _container.CurrentSessionToken);
        return httpResponse;
    }

    private static TransactionalBatchItemRequestOptions? BuildBatchItemRequestOptions(BatchOperation op)
    {
        if (op.IfMatch is null && op.IfNoneMatch is null)
            return null;
        return new TransactionalBatchItemRequestOptions
        {
            IfMatchEtag = op.IfMatch,
            IfNoneMatchEtag = op.IfNoneMatch
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  CRUD helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private HttpResponseMessage ConvertToHttpResponse(ResponseMessage cosmosResponse)
    {
        var httpResponse = new HttpResponseMessage(cosmosResponse.StatusCode);
        if (cosmosResponse.Content is not null)
        {
            using var reader = new StreamReader(cosmosResponse.Content);
            var json = reader.ReadToEnd();
            if (json.Length > 0)
            {
                httpResponse.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }
        }

        httpResponse.Headers.Add("x-ms-request-charge", "1");
        httpResponse.Headers.Add("x-ms-activity-id", cosmosResponse.Headers["x-ms-activity-id"] ?? Guid.NewGuid().ToString());
        httpResponse.Headers.Add("x-ms-session-token", _container.CurrentSessionToken);

        var subStatus = cosmosResponse.Headers["x-ms-substatus"];
        if (subStatus is not null)
        {
            httpResponse.Headers.Add("x-ms-substatus", subStatus);
        }

        var etag = cosmosResponse.Headers["ETag"];
        if (etag is not null)
        {
            httpResponse.Headers.TryAddWithoutValidation("etag", etag);
        }

        return httpResponse;
    }

    private static string ExtractDocumentId(HttpRequestMessage request)
    {
        var path = request.RequestUri?.AbsolutePath ?? "";
        var docsIndex = path.LastIndexOf("/docs/", StringComparison.OrdinalIgnoreCase);
        if (docsIndex >= 0)
        {
            var id = path[(docsIndex + 6)..];
            return Uri.UnescapeDataString(id.TrimEnd('/'));
        }
        return "";
    }

    private static bool HasDocumentId(string path)
    {
        var docsIndex = path.LastIndexOf("/docs/", StringComparison.OrdinalIgnoreCase);
        if (docsIndex < 0) return false;
        var afterDocs = path[(docsIndex + 6)..].TrimEnd('/');
        return afterDocs.Length > 0;
    }

    private static ItemRequestOptions BuildItemRequestOptions(HttpRequestMessage request)
    {
        var options = new ItemRequestOptions();
        if (request.Headers.IfMatch.Any())
        {
            options.IfMatchEtag = request.Headers.IfMatch.First().Tag;
        }
        if (request.Headers.IfNoneMatch.Any())
        {
            options.IfNoneMatchEtag = request.Headers.IfNoneMatch.First().Tag;
        }
        return options;
    }

    private static bool IsQueryRequest(HttpRequestMessage request)
    {
        var contentType = request.Content?.Headers?.ContentType?.MediaType ?? "";
        if (contentType.Contains("query+json", StringComparison.OrdinalIgnoreCase))
            return true;
        if (request.Headers.TryGetValues("x-ms-documentdb-isquery", out var values) &&
            values.Any(v => v.Equals("True", StringComparison.OrdinalIgnoreCase)))
            return true;
        return false;
    }

    private static bool IsUpsertRequest(HttpRequestMessage request)
    {
        return request.Headers.TryGetValues("x-ms-documentdb-is-upsert", out var values) &&
               values.Any(v => v.Equals("True", StringComparison.OrdinalIgnoreCase));
    }

    private static bool IsBatchRequest(HttpRequestMessage request)
    {
        return request.Headers.TryGetValues("x-ms-cosmos-is-batch-request", out var values) &&
               values.Any(v => v.Equals("True", StringComparison.OrdinalIgnoreCase));
    }

    private static (IReadOnlyList<PatchOperation> Operations, string? Condition) ParsePatchBody(string body)
    {
        var jObj = JObject.Parse(body);
        var operations = new List<PatchOperation>();
        var condition = jObj["condition"]?.ToString();

        var opsToken = jObj["operations"];
        if (opsToken is null)
            throw new InvalidOperationException("Patch body missing 'operations' array.");

        foreach (var op in opsToken.ToObject<JArray>()!)
        {
            var opType = op["op"]!.ToString().ToLowerInvariant();
            var opPath = op["path"]!.ToString();
            var value = op["value"];

            operations.Add(opType switch
            {
                "set" => PatchOperation.Set(opPath, value),
                "replace" => PatchOperation.Replace(opPath, value),
                "add" => PatchOperation.Add(opPath, value),
                "remove" => PatchOperation.Remove(opPath),
                "incr" => PatchOperation.Increment(opPath, value!.Value<double>()),
                "move" => PatchOperation.Move(op["from"]!.ToString(), opPath),
                _ => throw new InvalidOperationException($"Unknown patch operation: {opType}")
            });
        }

        return (operations, condition);
    }

    private HttpResponseMessage HandleOffersRequest(HttpRequestMessage request)
    {
        // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/querying-offers
        //   The SDK queries /offers to read provisioned throughput for a container.
        //   We return a single offer matching this container's throughput.
        var offer = new JObject
        {
            ["offerVersion"] = "V2",
            ["offerType"] = "Invalid",
            ["content"] = new JObject
            {
                ["offerThroughput"] = _container._throughput,
                ["offerIsRUPerMinuteThroughputEnabled"] = false
            },
            ["offerResourceId"] = _collectionRid,
            ["id"] = "offer-" + _collectionRid,
            ["_rid"] = "offer-" + _collectionRid,
            ["_self"] = "offers/offer-" + _collectionRid + "/",
            ["_etag"] = $"\"{Guid.NewGuid()}\"",
            ["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        };

        var wrapper = new JObject
        {
            ["_rid"] = "",
            ["Offers"] = new JArray(offer),
            ["_count"] = 1
        };

        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(wrapper.ToString(Formatting.None), Encoding.UTF8, "application/json")
        };
        response.Headers.Add("x-ms-request-charge", "1");
        response.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        return response;
    }

    private HttpResponseMessage HandlePartitionKeyRanges(HttpRequestMessage request)
    {
        if (request.Headers.IfNoneMatch.Any(etag => etag.Tag == PkRangesEtag))
        {
            var notModified = new HttpResponseMessage(HttpStatusCode.NotModified);
            notModified.Headers.Add("x-ms-request-charge", "0");
            notModified.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
            notModified.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue(PkRangesEtag);
            return notModified;
        }

        var response = CreateJsonResponse(GetPartitionKeyRanges());
        response.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue(PkRangesEtag);
        return response;
    }

    /// <summary>
    /// Handles the gateway query plan request that the SDK sends on non-Windows platforms
    /// (where the native ServiceInterop DLL is unavailable). Parses the SQL query and
    /// returns a <c>PartitionedQueryExecutionInfo</c> with accurate metadata so that the
    /// SDK builds the same execution pipeline (ORDER BY merge sort, aggregate accumulation,
    /// DISTINCT deduplication, etc.) as it would on Windows via ServiceInterop.
    /// </summary>
    private async Task<HttpResponseMessage> HandleQueryPlanAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var queryBody = JsonParseHelpers.ParseJson(body);
        var sqlQuery = queryBody["query"]?.ToString() ?? "SELECT * FROM c";

        CosmosSqlQuery? parsed = CosmosSqlParser.TryParse(sqlQuery, out var p) ? p : null;
        var queryPlan = _queryPlanStrategy.BuildQueryPlan(sqlQuery, parsed, _collectionRid);
        return CreateJsonResponse(queryPlan.ToString(Formatting.None));
    }

    private static bool HasAggregateInSelect(CosmosSqlQuery parsed)
    {
        return parsed.SelectFields.Any(field => ContainsAggregate(field.SqlExpr));
    }

    private static bool ContainsAggregate(SqlExpression? expr)
    {
        return expr switch
        {
            FunctionCallExpression func =>
                func.FunctionName.ToUpperInvariant() is "COUNT" or "COUNTIF" or "SUM" or "MIN" or "MAX" or "AVG"
                || func.Arguments.Any(ContainsAggregate),
            BinaryExpression bin => ContainsAggregate(bin.Left) || ContainsAggregate(bin.Right),
            UnaryExpression unary => ContainsAggregate(unary.Operand),
            TernaryExpression ternary => ContainsAggregate(ternary.Condition) || ContainsAggregate(ternary.IfTrue) || ContainsAggregate(ternary.IfFalse),
            CoalesceExpression coalesce => ContainsAggregate(coalesce.Left) || ContainsAggregate(coalesce.Right),
            _ => false
        };
    }

    private static bool ContainsAvg(SqlExpression? expr)
    {
        return expr switch
        {
            FunctionCallExpression func =>
                func.FunctionName.Equals("AVG", StringComparison.OrdinalIgnoreCase)
                || func.Arguments.Any(ContainsAvg),
            BinaryExpression bin => ContainsAvg(bin.Left) || ContainsAvg(bin.Right),
            UnaryExpression unary => ContainsAvg(unary.Operand),
            TernaryExpression ternary => ContainsAvg(ternary.Condition) || ContainsAvg(ternary.IfTrue) || ContainsAvg(ternary.IfFalse),
            CoalesceExpression coalesce => ContainsAvg(coalesce.Left) || ContainsAvg(coalesce.Right),
            _ => false
        };
    }

    private async Task<HttpResponseMessage> HandleQueryAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        var queryBody = JsonParseHelpers.ParseJson(body);
        var sqlQuery = queryBody["query"]?.ToString() ?? "SELECT * FROM c";
        _queryLog.Add(sqlQuery);

        var partitionKey = ExtractPartitionKey(request) ?? _partitionKeyOverride.Value ?? _partitionKeyHint.Value;
        var maxItemCount = ExtractMaxItemCount(request);
        var continuation = DecodeContinuation(request);
        var rangeId = ExtractPartitionKeyRangeId(request);

        List<JToken> allDocuments;
        string cacheKey;
        int offset;
        string? payloadPropertyName = null;

        if (continuation is not null && _queryResultCache.TryGet(continuation.Value.Key, out var cached))
        {
            allDocuments = cached;
            cacheKey = continuation.Value.Key;
            offset = continuation.Value.Offset;
        }
        else
        {
            offset = 0;
            cacheKey = Guid.NewGuid().ToString("N");

            if (CosmosSqlParser.TryParse(sqlQuery, out var parsed))
            {
                var orderByItemsField = parsed.SelectFields
                    .FirstOrDefault(field => IsOrderByItemsArray(field.SqlExpr));
                var isOrderByQuery = orderByItemsField is not null && parsed.OrderByFields is { Length: > 0 };

                var groupByItemsField = parsed.SelectFields
                    .FirstOrDefault(field => string.Equals(field.Alias, "groupByItems", StringComparison.OrdinalIgnoreCase));
                var isGroupByQuery = groupByItemsField is not null && parsed.GroupByFields is { Length: > 0 };

                if (isGroupByQuery)
                {
                    allDocuments = await HandleGroupByQueryAsync(
                        parsed, queryBody, partitionKey, cancellationToken);
                }
                else if (isOrderByQuery)
                {
                    var payloadField = parsed.SelectFields
                        .FirstOrDefault(field => string.Equals(field.Alias, "payload", StringComparison.OrdinalIgnoreCase))
                        ?? parsed.SelectFields
                            .FirstOrDefault(field => field != orderByItemsField);
                    var orderByAlias = orderByItemsField!.Alias ?? "orderByItems";
                    payloadPropertyName = payloadField?.Alias ?? "payload";

                    allDocuments = await HandleOrderByQueryAsync(
                        parsed, queryBody, partitionKey, orderByAlias, payloadPropertyName, cancellationToken);
                }
                else
                {
                    var simplifiedSql = CosmosSqlParser.SimplifySdkQuery(parsed);
                    var queryDef = BuildQueryDefinition(simplifiedSql, queryBody);
                    var requestOptions = partitionKey is not null
                        ? new QueryRequestOptions { PartitionKey = partitionKey }
                        : null;
                    allDocuments = await DrainIterator(
                        _container.GetItemQueryIterator<JToken>(queryDef, requestOptions: requestOptions),
                        cancellationToken);

                    // VALUE aggregate wrapping is no longer needed because the query plan
                    // now bypasses the SDK's AggregateQueryPipelineStage for VALUE
                    // aggregate queries. The container computes the final result directly
                    // and the raw value is returned without wire-format wrapping.
                }
            }
            else
            {
                var queryDef = BuildQueryDefinition(sqlQuery, queryBody);
                var requestOptions = partitionKey is not null
                    ? new QueryRequestOptions { PartitionKey = partitionKey }
                    : null;
                allDocuments = await DrainIterator(
                    _container.GetItemQueryIterator<JToken>(queryDef, requestOptions: requestOptions),
                    cancellationToken);
            }
        }

        allDocuments = FilterDocumentsByRange(allDocuments, rangeId, payloadPropertyName);

        return BuildPagedResponse(allDocuments, offset, maxItemCount, cacheKey);
    }

    private async Task<List<JToken>> HandleOrderByQueryAsync(
        CosmosSqlQuery parsed, JObject queryBody, PartitionKey? partitionKey,
        string orderByAlias, string payloadAlias, CancellationToken cancellationToken)
    {
        // Check if any ORDER BY expression is a function call (not a simple property path).
        // When this happens (e.g. ORDER BY VectorDistance(...)), the simplified SQL is
        // SELECT VALUE c FROM c ORDER BY <expr>. The raw documents won't contain the
        // computed expression value, so we need to include it in the query.
        var hasComplexOrderBy = parsed.OrderByFields?.Any(f => f.Field is null) ?? false;

        string simplifiedSql;
        List<string> complexOrderByAliases = [];
        try
        {
            if (hasComplexOrderBy)
            {
                // Build a query that includes both the document and the computed ORDER BY values
                simplifiedSql = BuildComplexOrderBySql(parsed, payloadAlias, out complexOrderByAliases);
            }
            else
            {
                simplifiedSql = CosmosSqlParser.SimplifySdkQuery(parsed);
            }
        }
        catch
        {
            // If SimplifySdkQuery fails (e.g. SDK changed internal format),
            // fall back to executing SQL with SELECT VALUE <alias> and ORDER BY from the parsed structure.
            simplifiedSql = BuildFallbackOrderBySql(parsed);
        }

        var queryDef = BuildQueryDefinition(simplifiedSql, queryBody);
        var requestOptions = partitionKey is not null
            ? new QueryRequestOptions { PartitionKey = partitionKey }
            : null;
        var feedIterator = _container.GetItemQueryIterator<JObject>(queryDef, requestOptions: requestOptions);
        var rawDocuments = new List<JObject>();
        while (feedIterator.HasMoreResults)
        {
            var page = await feedIterator.ReadNextAsync(cancellationToken);
            rawDocuments.AddRange(page);
        }

        List<string> orderByPaths;
        if (hasComplexOrderBy && complexOrderByAliases.Count > 0)
        {
            // For complex ORDER BY, the paths are the aliases we injected into the query
            orderByPaths = complexOrderByAliases;
        }
        else
        {
            try
            {
                orderByPaths = ExtractOrderByItemPaths(parsed);
            }
            catch
            {
                // Fallback: derive from ORDER BY fields if AST walking fails
                orderByPaths = parsed.OrderByFields?.Select(field =>
                    StripFromAlias(
                        field.Field ?? CosmosSqlParser.ExprToString(field.Expression),
                        parsed.FromAlias)).ToList() ?? [];
            }
        }

        var documents = new List<JToken>();
        // Determine if the payload is the full document or a projected expression.
        // For DISTINCT+ORDER BY, the SDK rewrites payload as {"name": c.name} (an ObjectLiteral),
        // meaning only the projected fields should be returned, not the full document.
        var payloadField = parsed.SelectFields
            .FirstOrDefault(f => string.Equals(f.Alias, payloadAlias, StringComparison.OrdinalIgnoreCase));
        var fromAlias = parsed.FromAlias ?? "c";

        foreach (var doc in rawDocuments)
        {
            var orderByItems = new JArray();
            foreach (var path in orderByPaths)
            {
                var value = doc.SelectToken(path)?.DeepClone() ?? JValue.CreateNull();
                orderByItems.Add(new JObject { ["item"] = value });
            }

            JToken payloadValue;
            if (hasComplexOrderBy)
            {
                // For complex ORDER BY queries, the doc contains _doc + _ob0/_ob1/... aliases.
                // Extract the _doc field as the full document, then apply payload projection.
                var fullDoc = doc["_doc"]?.DeepClone() ?? doc.DeepClone();
                // Remove the ORDER BY alias properties from the payload if they leaked in
                if (fullDoc is JObject fullDocObj)
                    foreach (var alias in complexOrderByAliases)
                        fullDocObj.Remove(alias);

                // Apply payload projection if the rewritten query specified a projected payload
                // (e.g. {"id": c.id, "score": VectorDistance(...)} AS payload)
                if (payloadField?.SqlExpr is ObjectLiteralExpression objLit)
                    payloadValue = BuildComplexPayloadValue((JObject)fullDoc, objLit, fromAlias, parsed, doc, complexOrderByAliases);
                else
                    payloadValue = fullDoc;
            }
            else
            {
                payloadValue = BuildPayloadValue(doc, payloadField, fromAlias);
            }

            var wrapped = new JObject
            {
                ["_rid"] = (hasComplexOrderBy
                    ? doc["_doc"]?["_rid"]?.ToString()
                    : doc["_rid"]?.ToString()) ?? Guid.NewGuid().ToString("N")[..8],
                [orderByAlias] = orderByItems,
                [payloadAlias] = payloadValue
            };
            documents.Add(wrapped);
        }

        return documents;
    }

    /// <summary>
    /// Builds the payload value for an ORDER BY-wrapped document.
    /// For simple ORDER BY, the payload is the full document.
    /// For DISTINCT+ORDER BY, the SDK rewrites the payload as an ObjectLiteral
    /// (e.g. {"name": c.name}), so we project just those fields.
    /// </summary>
    private static JToken BuildPayloadValue(JObject doc, SelectField? payloadField, string fromAlias)
    {
        if (payloadField?.SqlExpr is ObjectLiteralExpression objLiteral)
        {
            var projected = new JObject();
            foreach (var prop in objLiteral.Properties)
            {
                var exprStr = CosmosSqlParser.ExprToString(prop.Value);
                var path = exprStr;
                if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    path = path[(fromAlias.Length + 1)..];
                projected[prop.Key] = doc.SelectToken(path)?.DeepClone() ?? JValue.CreateNull();
            }
            return projected;
        }

        if (payloadField?.SqlExpr is IdentifierExpression id
            && !string.Equals(id.Name, fromAlias, StringComparison.OrdinalIgnoreCase)
            && !id.Name.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase)
            && !id.Name.StartsWith(fromAlias + "[", StringComparison.OrdinalIgnoreCase))
        {
            var path = id.Name;
            if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                path = path[(fromAlias.Length + 1)..];
            return doc.SelectToken(path)?.DeepClone() ?? JValue.CreateNull();
        }

        return doc.DeepClone();
    }

    /// <summary>
    /// Builds the payload for a complex ORDER BY query where the payload was projected
    /// as an ObjectLiteral (e.g. {"id": c.id, "score": VectorDistance(...)}).
    /// Simple property references are resolved from the full document; function call
    /// expressions are resolved from the computed ORDER BY aliases (_ob0, _ob1, ...).
    /// </summary>
    private static JToken BuildComplexPayloadValue(
        JObject fullDoc, ObjectLiteralExpression objLiteral, string fromAlias,
        CosmosSqlQuery parsed, JObject rawDoc, List<string> orderByAliases)
    {
        var projected = new JObject();
        foreach (var prop in objLiteral.Properties)
        {
            var exprStr = CosmosSqlParser.ExprToString(prop.Value);

            // Check if this expression matches one of the ORDER BY expressions —
            // if so, use the pre-computed _ob alias value from the raw result.
            JToken? resolved = null;
            if (parsed.OrderByFields is not null)
            {
                for (var i = 0; i < parsed.OrderByFields.Length && i < orderByAliases.Count; i++)
                {
                    var obExpr = parsed.OrderByFields[i].Field
                        ?? CosmosSqlParser.ExprToString(parsed.OrderByFields[i].Expression);
                    if (string.Equals(exprStr, obExpr, StringComparison.OrdinalIgnoreCase))
                    {
                        resolved = rawDoc[orderByAliases[i]]?.DeepClone();
                        break;
                    }
                }
            }

            if (resolved is not null)
            {
                projected[prop.Key] = resolved;
            }
            else
            {
                // Simple property path — resolve from the full document
                var path = exprStr;
                if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
                    path = path[(fromAlias.Length + 1)..];
                projected[prop.Key] = fullDoc.SelectToken(path)?.DeepClone() ?? JValue.CreateNull();
            }
        }
        return projected;
    }

    /// <summary>
    /// Handles GROUP BY queries that the SDK has rewritten to the groupByItems + payload format.
    /// Reconstructs the original GROUP BY query, executes on InMemoryContainer (which computes
    /// final aggregates), then wraps results in the format the SDK's GroupByQueryPipelineStage expects.
    /// </summary>
    private async Task<List<JToken>> HandleGroupByQueryAsync(
        CosmosSqlQuery parsed, JObject queryBody, PartitionKey? partitionKey,
        CancellationToken cancellationToken)
    {
        var originalSql = ReconstructGroupByQuery(parsed);
        var queryDef = BuildQueryDefinition(originalSql, queryBody);
        var requestOptions = partitionKey is not null
            ? new QueryRequestOptions { PartitionKey = partitionKey }
            : null;
        var rawResults = await DrainIterator(
            _container.GetItemQueryIterator<JObject>(queryDef, requestOptions: requestOptions),
            cancellationToken);

        // Extract aggregate type info from the payload ObjectLiteral
        var payloadField = parsed.SelectFields
            .FirstOrDefault(f => string.Equals(f.Alias, "payload", StringComparison.OrdinalIgnoreCase));
        var aggregateTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (payloadField?.SqlExpr is ObjectLiteralExpression payloadObj)
        {
            foreach (var prop in payloadObj.Properties)
            {
                if (prop.Value is ObjectLiteralExpression inner
                    && inner.Properties.Length == 1
                    && string.Equals(inner.Properties[0].Key, "item", StringComparison.OrdinalIgnoreCase)
                    && inner.Properties[0].Value is FunctionCallExpression func)
                {
                    aggregateTypes[prop.Key] = func.FunctionName.ToUpperInvariant();
                }
            }
        }

        // Extract GROUP BY key result-property names from the payload ObjectLiteral.
        // Non-aggregate properties in the payload correspond to the GROUP BY keys.
        var groupByResultKeys = new List<string>();
        if (payloadField?.SqlExpr is ObjectLiteralExpression payloadObjForKeys)
        {
            foreach (var prop in payloadObjForKeys.Properties)
            {
                if (!(prop.Value is ObjectLiteralExpression inner
                    && inner.Properties.Length == 1
                    && string.Equals(inner.Properties[0].Key, "item", StringComparison.OrdinalIgnoreCase)))
                {
                    groupByResultKeys.Add(prop.Key);
                }
            }
        }

        return rawResults.Select(doc =>
        {
            var jObj = doc as JObject ?? JObject.Parse(doc.ToString());

            var groupByItems = new JArray();
            foreach (var key in groupByResultKeys)
            {
                groupByItems.Add(new JObject { ["item"] = jObj.SelectToken(key)?.DeepClone() ?? JValue.CreateNull() });
            }

            var payload = new JObject();
            foreach (var prop in jObj.Properties())
            {
                if (aggregateTypes.TryGetValue(prop.Name, out var aggType))
                {
                    JToken itemValue = aggType == "AVG"
                        ? new JObject { ["sum"] = prop.Value, ["count"] = 1 }
                        : prop.Value.DeepClone();
                    payload[prop.Name] = new JObject { ["item"] = itemValue };
                }
                else
                {
                    payload[prop.Name] = prop.Value.DeepClone();
                }
            }

            return (JToken)new JObject
            {
                ["groupByItems"] = groupByItems,
                ["payload"] = payload
            };
        }).ToList();
    }

    /// <summary>
    /// Reconstructs the original GROUP BY SQL from the SDK-rewritten format.
    /// The SDK rewrites e.g. <c>SELECT c.name, COUNT(1) AS cnt FROM c GROUP BY c.name</c> to
    /// <c>SELECT [{"item": c.name}] AS groupByItems, {"name": c.name, "cnt": {"item": COUNT(1)}} AS payload FROM c GROUP BY c.name</c>.
    /// This method extracts the original SELECT fields from the payload ObjectLiteral.
    /// </summary>
    private static string ReconstructGroupByQuery(CosmosSqlQuery parsed)
    {
        var alias = parsed.FromAlias ?? "c";
        var payloadField = parsed.SelectFields
            .FirstOrDefault(f => string.Equals(f.Alias, "payload", StringComparison.OrdinalIgnoreCase));

        if (payloadField?.SqlExpr is not ObjectLiteralExpression payloadObj)
            throw new InvalidOperationException("Cannot reconstruct GROUP BY query: payload field not found.");

        var selectParts = new List<string>();
        foreach (var prop in payloadObj.Properties)
        {
            if (prop.Value is ObjectLiteralExpression inner
                && inner.Properties.Length == 1
                && string.Equals(inner.Properties[0].Key, "item", StringComparison.OrdinalIgnoreCase))
            {
                // Aggregate: {"alias": {"item": AGG(...)}}
                selectParts.Add($"{CosmosSqlParser.ExprToString(inner.Properties[0].Value)} AS {prop.Key}");
            }
            else
            {
                // Plain field: {"alias": expr}
                selectParts.Add($"{CosmosSqlParser.ExprToString(prop.Value)} AS {prop.Key}");
            }
        }

        var sb = new StringBuilder($"SELECT {string.Join(", ", selectParts)} FROM {alias}");

        if (parsed.WhereExpr is not null)
            sb.Append($" WHERE {CosmosSqlParser.ExprToString(parsed.WhereExpr)}");

        sb.Append($" GROUP BY {string.Join(", ", parsed.GroupByFields!)}");

        return sb.ToString();
    }

    /// <summary>
    /// Strips OFFSET ... LIMIT ... from a SQL query string so the SDK pipeline
    /// can apply OFFSET/LIMIT itself (avoiding double application).
    /// </summary>
    private static string StripOffsetLimit(string sql)
    {
        return Regex.Replace(sql, @"\s*OFFSET\s+\S+\s+LIMIT\s+\S+", "", RegexOptions.IgnoreCase).TrimEnd();
    }

    /// <summary>Static accessor for <see cref="DefaultQueryPlanStrategy"/>.</summary>
    internal static string StripOffsetLimitStatic(string sql) => StripOffsetLimit(sql);

    /// <summary>
    /// Builds the ORDER BY rewritten query in the format the SDK expects:
    /// <c>SELECT c._rid, [{"item": c.field}] AS orderByItems, c AS payload FROM c ... ORDER BY c.field ASC</c>
    /// </summary>
    private static string BuildOrderByRewrittenQuery(CosmosSqlQuery parsed)
    {
        var alias = parsed.FromAlias ?? "c";

        // Build orderByItems array: [{"item": c.field1}, {"item": c.field2}]
        var orderByItemsParts = parsed.OrderByFields!
            .Select(field => $"{{\"item\": {field.Field ?? CosmosSqlParser.ExprToString(field.Expression)}}}")
            .ToList();
        var orderByItemsArray = $"[{string.Join(", ", orderByItemsParts)}]";

        // Build SELECT with top-level fields: _rid, orderByItems, payload
        var sb = new StringBuilder($"SELECT {alias}._rid, ");
        sb.Append(orderByItemsArray);
        sb.Append(" AS orderByItems, ");

        // For ORDER BY queries with explicit projections, the payload must be the
        // projected SELECT fields (not the full document), so the SDK returns the
        // correct shape to the caller. This applies to:
        // 1. DISTINCT queries — for SDK deduplication
        // 2. Queries with computed expressions (e.g. VectorDistance(...) AS score) —
        //    the computed value doesn't exist in the document and must be projected.
        // All other cases (SELECT VALUE c, SELECT *, SELECT VALUE {obj}) use the full
        // document as payload — the SDK or caller handles field extraction.
        var hasComputedSelectField = parsed.SelectFields
            .Any(f => f.SqlExpr is FunctionCallExpression);
        if ((parsed.IsDistinct || hasComputedSelectField)
            && parsed.SelectFields.Length > 0
            && parsed.SelectFields.All(f => f.SqlExpr is not null))
        {
            var payloadParts = parsed.SelectFields.Select(f =>
            {
                var expr = CosmosSqlParser.ExprToString(f.SqlExpr);
                var key = f.Alias ?? f.Expression ?? expr;
                // Strip FROM alias prefix from the key (e.g. "c.id" → "id")
                // because Cosmos DB results use the property name without the alias.
                if (key.StartsWith(alias + ".", StringComparison.OrdinalIgnoreCase))
                    key = key[(alias.Length + 1)..];
                return $"\"{key}\": {expr}";
            });
            sb.Append($"{{{string.Join(", ", payloadParts)}}} AS payload");
        }
        else
        {
            sb.Append($"{alias} AS payload");
        }

        // FROM clause
        sb.Append($" FROM {alias}");

        // WHERE clause — reconstruct from the where expression if present
        if (parsed.WhereExpr is not null)
        {
            sb.Append(" WHERE ");
            sb.Append(CosmosSqlParser.ExprToString(parsed.WhereExpr));
        }

        // ORDER BY clause
        var orderByStr = string.Join(", ", parsed.OrderByFields!.Select(field =>
            $"{field.Field ?? CosmosSqlParser.ExprToString(field.Expression)} {(field.Ascending ? "ASC" : "DESC")}"));
        sb.Append($" ORDER BY {orderByStr}");

        return sb.ToString();
    }

    /// <summary>Static accessor for <see cref="DefaultQueryPlanStrategy"/>.</summary>
    internal static string BuildOrderByRewrittenQueryStatic(CosmosSqlQuery parsed) => BuildOrderByRewrittenQuery(parsed);

    /// <summary>
    /// Builds a SQL query that includes both the full document and computed ORDER BY
    /// expression values. Used when ORDER BY contains function calls (e.g. VectorDistance)
    /// that aren't simple property paths.
    /// Example output: SELECT c AS _doc, VectorDistance(c.emb, [1,0,0]) AS _ob0 FROM c
    ///                 ORDER BY VectorDistance(c.emb, [1,0,0]) ASC
    /// </summary>
    private static string BuildComplexOrderBySql(CosmosSqlQuery parsed, string payloadAlias, out List<string> orderByAliases)
    {
        var alias = parsed.FromAlias ?? "c";

        // Always select the full document as _doc. The payload projection (if any)
        // is applied in post-processing via BuildPayloadValue, just like non-complex
        // ORDER BY. This avoids inlining object literal expressions (e.g.
        // {"id": c.id, "score": VectorDistance(...)}) into SELECT, which the parser
        // cannot handle on re-parse.
        var sb = new StringBuilder($"SELECT {alias} AS _doc");

        orderByAliases = [];
        for (var i = 0; i < parsed.OrderByFields!.Length; i++)
        {
            var field = parsed.OrderByFields[i];
            var obAlias = $"_ob{i}";
            orderByAliases.Add(obAlias);
            var expr = field.Field ?? CosmosSqlParser.ExprToString(field.Expression);
            sb.Append($", {expr} AS {obAlias}");
        }

        sb.Append($" FROM {alias}");

        if (parsed.WhereExpr is not null)
        {
            var simplifiedWhere = CosmosSqlParser.SimplifySdkWhereExpression(parsed.WhereExpr, alias);
            if (simplifiedWhere is not null)
                sb.Append($" WHERE {CosmosSqlParser.ExprToString(simplifiedWhere)}");
        }

        var orderByStr = string.Join(", ", parsed.OrderByFields.Select(field =>
            $"{field.Field ?? CosmosSqlParser.ExprToString(field.Expression)} {(field.Ascending ? "ASC" : "DESC")}"));
        sb.Append($" ORDER BY {orderByStr}");

        return sb.ToString();
    }

    private static string BuildFallbackOrderBySql(CosmosSqlQuery parsed)
    {
        var sb = new StringBuilder("SELECT ");

        if (parsed.IsDistinct)
        {
            sb.Append("DISTINCT ");
        }

        if (parsed.TopCount.HasValue)
        {
            sb.Append($"TOP {parsed.TopCount.Value} ");
        }

        sb.Append($"VALUE {parsed.FromAlias} FROM {parsed.FromAlias}");

        if (parsed.OrderByFields is { Length: > 0 })
        {
            var orderByStr = string.Join(", ", parsed.OrderByFields.Select(field =>
                $"{field.Field ?? CosmosSqlParser.ExprToString(field.Expression)} {(field.Ascending ? "ASC" : "DESC")}"));
            sb.Append($" ORDER BY {orderByStr}");
        }

        return sb.ToString();
    }

    private static List<string> ExtractOrderByItemPaths(CosmosSqlQuery parsed)
    {
        var orderByItemsField = parsed.SelectFields
            .FirstOrDefault(field => IsOrderByItemsArray(field.SqlExpr));

        if (orderByItemsField?.SqlExpr is ArrayLiteralExpression arrayExpr)
        {
            var paths = new List<string>();
            foreach (var element in arrayExpr.Elements)
            {
                if (element is ObjectLiteralExpression obj)
                {
                    var itemProp = obj.Properties.FirstOrDefault(property =>
                        string.Equals(property.Key, "item", StringComparison.OrdinalIgnoreCase));
                    if (itemProp.Value is IdentifierExpression ident)
                    {
                        paths.Add(StripFromAlias(ident.Name, parsed.FromAlias));
                    }
                    else if (itemProp.Value is not null)
                    {
                        // Non-identifier expression (e.g. VectorDistance(...)) — use the
                        // corresponding ORDER BY field expression to reconstruct a path.
                        // The value will be looked up by evaluating the expression on the
                        // raw document, which won't produce a valid JPath. Instead, we
                        // return the expression string so the caller can handle it.
                        var exprStr = CosmosSqlParser.ExprToString(itemProp.Value);
                        paths.Add(StripFromAlias(exprStr, parsed.FromAlias));
                    }
                }
            }

            if (paths.Count > 0)
            {
                return paths;
            }
        }

        if (parsed.OrderByFields is { Length: > 0 })
        {
            return parsed.OrderByFields
                .Select(field => StripFromAlias(
                    field.Field ?? CosmosSqlParser.ExprToString(field.Expression),
                    parsed.FromAlias))
                .ToList();
        }

        return [];
    }

    private static string StripFromAlias(string path, string fromAlias)
    {
        if (path.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
        {
            return path[(fromAlias.Length + 1)..];
        }

        return path;
    }

    private static bool IsOrderByItemsArray(SqlExpression? expr)
    {
        return expr is ArrayLiteralExpression { Elements.Length: > 0 } arrayLiteral
            && arrayLiteral.Elements.All(element =>
                element is ObjectLiteralExpression obj
                && obj.Properties.Any(prop =>
                    string.Equals(prop.Key, "item", StringComparison.OrdinalIgnoreCase)));
    }

    private List<JToken> FilterDocumentsByRange(List<JToken> documents, string? rangeId, string? payloadPropertyName = null)
    {
        if (_partitionKeyRangeCount <= 1 || rangeId is null)
        {
            return documents;
        }

        if (!int.TryParse(rangeId, out var rangeIndex))
        {
            return documents;
        }
        return documents.Where(document => GetRangeIndex(document, payloadPropertyName) == rangeIndex).ToList();
    }

    private int GetRangeIndex(JToken document, string? payloadPropertyName)
    {
        if (document is not JObject obj)
        {
            return 0;
        }

        var targetDoc = payloadPropertyName is not null && obj[payloadPropertyName] is JObject payload
            ? payload : obj;
        var pkToken = targetDoc.SelectToken(_partitionKeyPath);
        var pkValue = pkToken is not null ? InMemoryContainer.JTokenToTypedKey(pkToken) ?? "" : "";
        return PartitionKeyHash.GetRangeIndex(pkValue, _partitionKeyRangeCount);
    }

    private async Task<HttpResponseMessage> HandleReadFeedAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-documents
        //   "A-IM: Incremental feed" header distinguishes change feed from regular read feed.
        if (IsChangeFeedRequest(request))
        {
            return await HandleChangeFeedAsync(request);
        }

        var maxItemCount = ExtractMaxItemCount(request);
        var continuation = DecodeContinuation(request);
        var rangeId = ExtractPartitionKeyRangeId(request);

        List<JToken> allDocuments;
        string cacheKey;
        int offset;

        if (continuation is not null && _queryResultCache.TryGet(continuation.Value.Key, out var cached))
        {
            allDocuments = cached;
            cacheKey = continuation.Value.Key;
            offset = continuation.Value.Offset;
        }
        else
        {
            offset = 0;
            cacheKey = Guid.NewGuid().ToString("N");

            var requestOptions = new QueryRequestOptions();
            var partitionKey = ExtractPartitionKey(request) ?? _partitionKeyOverride.Value ?? _partitionKeyHint.Value;
            if (partitionKey is not null)
            {
                requestOptions.PartitionKey = partitionKey;
            }

            allDocuments = await DrainIterator(
                _container.GetItemQueryIterator<JObject>(requestOptions: requestOptions),
                cancellationToken);
        }

        allDocuments = FilterDocumentsByRange(allDocuments, rangeId);

        return BuildPagedResponse(allDocuments, offset, maxItemCount, cacheKey);
    }

    /// <summary>
    /// Handles change feed HTTP requests (GET /docs with A-IM: Incremental feed).
    /// The SDK's ChangeFeedProcessor sends these requests and expects:
    /// - An <c>etag</c> response header containing a non-empty continuation token
    ///   (used by <c>AutoCheckpointer.CheckpointAsync</c>)
    /// - HTTP 200 with documents when changes exist
    /// - HTTP 304 Not Modified when no new changes are available
    /// </summary>
    /// <remarks>
    /// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-documents
    ///   "If-None-Match: *" means read from the beginning;
    ///   "If-None-Match: {etag}" means resume from that checkpoint position.
    ///   The response etag is the logical sequence number (LSN) of the last change returned.
    /// </remarks>
    private async Task<HttpResponseMessage> HandleChangeFeedAsync(HttpRequestMessage request)
    {
        var maxItemCount = ExtractMaxItemCount(request);
        var rangeId = ExtractPartitionKeyRangeId(request);

        // Determine checkpoint from If-None-Match header.
        // "*" or absent → read from beginning (checkpoint 0).
        // Quoted numeric string (e.g. "\"42\"") → resume from that position.
        long checkpoint = 0;
        if (request.Headers.IfNoneMatch.Count > 0)
        {
            var tag = request.Headers.IfNoneMatch.First().Tag;
            // Strip surrounding quotes: "\"42\"" → "42"
            var unquoted = tag?.Trim('"') ?? "";
            if (unquoted != "*" && long.TryParse(unquoted, out var parsed))
            {
                checkpoint = parsed;
            }
        }

        // Read changes from the backing container's change feed
        var iterator = _container.GetChangeFeedIterator<JObject>(checkpoint);
        var allChanges = new List<JObject>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            if (page.StatusCode == HttpStatusCode.NotModified)
                break;
            allChanges.AddRange(page);
        }
        var totalChanges = allChanges.Count;

        allChanges = FilterDocumentsByRange(allChanges.Cast<JToken>().ToList(), rangeId)
            .Cast<JObject>().ToList();

        // Apply maxItemCount paging
        var paged = maxItemCount > 0 && allChanges.Count > maxItemCount
            ? allChanges.Take(maxItemCount).ToList()
            : allChanges;

        var newCheckpoint = checkpoint + totalChanges;

        if (paged.Count == 0)
        {
            // No new changes — return 304 Not Modified with the current checkpoint as etag
            var notModified = new HttpResponseMessage(HttpStatusCode.NotModified);
            notModified.Headers.Add("x-ms-request-charge", "1");
            notModified.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
            notModified.Headers.Add("x-ms-item-count", "0");
            notModified.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue(
                $"\"{newCheckpoint}\"");
            return notModified;
        }

        var responseBody = new JObject
        {
            ["_rid"] = _collectionRid,
            ["Documents"] = new JArray(paged),
            ["_count"] = paged.Count
        };

        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(responseBody.ToString(), Encoding.UTF8, "application/json")
        };

        response.Headers.Add("x-ms-request-charge", "1");
        response.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        response.Headers.Add("x-ms-session-token", _container.CurrentSessionToken);
        response.Headers.Add("x-ms-item-count", paged.Count.ToString());

        // The SDK reads the etag header as the continuation token for CheckpointAsync.
        // This MUST be non-empty or DocumentServiceLeaseCheckpointerCore.CheckpointAsync throws.
        response.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue(
            $"\"{newCheckpoint}\"");

        // If there are more changes beyond this page, set continuation to signal more pages
        if (paged.Count < allChanges.Count)
        {
            response.Headers.Add("x-ms-continuation", $"\"{newCheckpoint}\"");
        }

        return response;
    }

    private static bool IsChangeFeedRequest(HttpRequestMessage request)
    {
        // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-documents
        //   The A-IM header with value "Incremental feed" indicates a change feed request.
        if (!request.Headers.TryGetValues("A-IM", out var values))
        {
            return false;
        }
        return values.Any(v => v.Contains("Incremental", StringComparison.OrdinalIgnoreCase));
    }

    private HttpResponseMessage BuildPagedResponse(
        List<JToken> allDocuments, int offset, int maxItemCount, string cacheKey)
    {
        var paged = allDocuments.Skip(offset).ToList();
        string? continuationToken = null;
        if (maxItemCount > 0 && paged.Count > maxItemCount)
        {
            paged = paged.Take(maxItemCount).ToList();
            _queryResultCache.Set(cacheKey, allDocuments);
            continuationToken = EncodeContinuation(cacheKey, offset + maxItemCount);
        }
        else
        {
            _queryResultCache.Remove(cacheKey);
        }

        var responseBody = new JObject
        {
            ["_rid"] = _collectionRid,
            ["Documents"] = new JArray(paged),
            ["_count"] = paged.Count
        };

        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(responseBody.ToString(), Encoding.UTF8, "application/json")
        };

        response.Headers.Add("x-ms-request-charge", "1");
        response.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        response.Headers.Add("x-ms-session-token", _container.CurrentSessionToken);
        response.Headers.Add("x-ms-item-count", paged.Count.ToString());

        if (continuationToken is not null)
        {
            response.Headers.Add("x-ms-continuation", continuationToken);
        }

        return response;
    }

    private static async Task<List<JToken>> DrainIterator<T>(
        FeedIterator<T> feedIterator, CancellationToken cancellationToken)
        where T : JToken
    {
        var allDocuments = new List<JToken>();
        while (feedIterator.HasMoreResults)
        {
            var page = await feedIterator.ReadNextAsync(cancellationToken);
            allDocuments.AddRange(page);
        }

        return allDocuments;
    }

    private static QueryDefinition BuildQueryDefinition(string sqlQuery, JObject queryBody)
    {
        var queryDef = new QueryDefinition(sqlQuery);
        if (queryBody["parameters"] is JArray parameters)
        {
            foreach (var parameter in parameters)
            {
                var paramName = parameter["name"]?.ToString();
                var paramValue = parameter["value"];
                if (paramName is not null)
                {
                    queryDef = queryDef.WithParameter(paramName, paramValue);
                }
            }
        }

        return queryDef;
    }

    private static PartitionKey? ExtractPartitionKey(HttpRequestMessage request)
    {
        if (request.Headers.TryGetValues("x-ms-documentdb-partitionkey", out var values))
        {
            var raw = values.FirstOrDefault();
            if (raw is not null)
            {
                try
                {
                    var arr = JArray.Parse(raw);
                    if (arr.Count == 1)
                    {
                        return arr[0].Type switch
                        {
                            JTokenType.String => new PartitionKey(arr[0].Value<string>()),
                            JTokenType.Integer or JTokenType.Float => new PartitionKey(arr[0].Value<double>()),
                            JTokenType.Boolean => new PartitionKey(arr[0].Value<bool>()),
                            JTokenType.Null => PartitionKey.Null,
                            JTokenType.Object => PartitionKey.None, // SDK sends [{}] for PartitionKey.None
                            _ => new PartitionKey(arr[0].ToString())
                        };
                    }

                    if (arr.Count > 1)
                    {
                        var builder = new PartitionKeyBuilder();
                        foreach (var token in arr)
                        {
                            switch (token.Type)
                            {
                                case JTokenType.String:
                                    builder.Add(token.Value<string>()!);
                                    break;
                                case JTokenType.Integer or JTokenType.Float:
                                    builder.Add(token.Value<double>());
                                    break;
                                case JTokenType.Boolean:
                                    builder.Add(token.Value<bool>());
                                    break;
                                case JTokenType.Null:
                                    builder.AddNullValue();
                                    break;
                                default:
                                    builder.Add(token.ToString());
                                    break;
                            }
                        }
                        return builder.Build();
                    }
                }
                catch
                {
                    // Ignore malformed partition key headers
                }
            }
        }

        return null;
    }

    private static int ExtractMaxItemCount(HttpRequestMessage request)
    {
        if (request.Headers.TryGetValues("x-ms-max-item-count", out var values) &&
            int.TryParse(values.FirstOrDefault(), out var count) && count > 0)
        {
            return count;
        }

        return 0;
    }

    private static string? ExtractPartitionKeyRangeId(HttpRequestMessage request)
    {
        if (request.Headers.TryGetValues("x-ms-documentdb-partitionkeyrangeid", out var values))
        {
            return values.FirstOrDefault();
        }

        return null;
    }

    private static (string Key, int Offset)? DecodeContinuation(HttpRequestMessage request)
    {
        if (!request.Headers.TryGetValues("x-ms-continuation", out var values))
        {
            return null;
        }

        var raw = values.FirstOrDefault();
        if (raw is null)
        {
            return null;
        }

        try
        {
            var json = Encoding.UTF8.GetString(Convert.FromBase64String(raw));
            var obj = JsonParseHelpers.ParseJson(json);
            var key = obj["key"]?.Value<string>();
            var offset = obj["offset"]?.Value<int>() ?? 0;
            return key is not null ? (key, offset) : null;
        }
        catch
        {
            return null;
        }
    }

    private static string EncodeContinuation(string cacheKey, int offset)
    {
        var json = $"{{\"v\":2,\"key\":\"{cacheKey}\",\"offset\":{offset}}}";
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
    }

    private HttpResponseMessage CreateJsonResponse(string json)
        => CreateJsonResponse(json, HttpStatusCode.OK);

    private HttpResponseMessage CreateJsonResponse(string json, HttpStatusCode statusCode)
    {
        var response = new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
        response.Headers.Add("x-ms-request-charge", "0");
        response.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        response.Headers.Add("x-ms-session-token", _container.CurrentSessionToken);
        return response;
    }

    private HttpResponseMessage CreateNoContentResponse()
    {
        var response = new HttpResponseMessage(HttpStatusCode.NoContent);
        response.Headers.Add("x-ms-request-charge", "0");
        response.Headers.Add("x-ms-activity-id", Guid.NewGuid().ToString());
        return response;
    }

    private string GetDatabaseMetadata(string databaseName)
    {
        var metadata = new JObject
        {
            ["id"] = databaseName,
            ["_rid"] = _databaseRid,
            ["_self"] = $"dbs/{_databaseRid}/",
            ["_etag"] = "\"00000000-0000-0000-0000-000000000000\"",
            ["_colls"] = "colls/",
            ["_users"] = "users/",
            ["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        };
        return metadata.ToString(Formatting.None);
    }

    private const string AccountMetadata = """
        {
            "id": "fake-account",
            "_rid": "",
            "databasesLink": "/dbs/",
            "mediaLink": "/media/",
            "addressesLink": "",
            "userConsistencyPolicy": { "defaultConsistencyLevel": "Session" },
            "writableLocations": [
                { "name": "East US", "databaseAccountEndpoint": "https://localhost:9999/" }
            ],
            "readableLocations": [
                { "name": "East US", "databaseAccountEndpoint": "https://localhost:9999/" }
            ],
            "systemReplicationPolicy": { "minReplicaSetSize": 1, "maxReplicaCount": 4 },
            "readPolicy": { "primaryReadCoefficient": 1, "secondaryReadCoefficient": 1 },
            "queryEngineConfiguration": "{\"maxSqlQueryInputLength\":262144,\"maxJoinsPerSqlQuery\":5,\"maxLogicalAndPerSqlQuery\":500,\"maxLogicalOrPerSqlQuery\":500,\"maxUdfRefPerSqlQuery\":10,\"maxInExpressionItemsCount\":16000,\"queryMaxInMemorySortDocumentCount\":500,\"maxQueryRequestTimeoutFraction\":0.9,\"sqlAllowNonFiniteNumbers\":false,\"sqlAllowAggregateFunctions\":true,\"sqlAllowSubQuery\":true,\"sqlAllowScalarSubQuery\":true,\"allowNewKeywords\":true,\"sqlAllowLike\":true,\"sqlAllowGroupByClause\":true,\"maxSpatialQueryCells\":12,\"spatialMaxGeometryPointCount\":256,\"sqlDisableOptimizationFlags\":0,\"sqlAllowTop\":true,\"enableSpatialIndexing\":true}"
        }
        """;

    private string GetCollectionMetadata()
    {
        var paths = new JArray(_container.PartitionKeyPaths.Select(path => (JToken)path));
        var policy = _container.IndexingPolicy;
        var includedPaths = new JArray(policy.IncludedPaths.Select(p => new JObject { ["path"] = p.Path }));
        var excludedPaths = new JArray(policy.ExcludedPaths.Select(p => new JObject { ["path"] = p.Path }));
        if (!excludedPaths.Any(p => p["path"]?.ToString() == "/\"_etag\"/?"))
            excludedPaths.Add(new JObject { ["path"] = "/\"_etag\"/?" });

        var indexingPolicyObj = new JObject
        {
            ["indexingMode"] = policy.IndexingMode.ToString().ToLowerInvariant(),
            ["automatic"] = policy.Automatic,
            ["includedPaths"] = includedPaths,
            ["excludedPaths"] = excludedPaths
        };

        if (policy.CompositeIndexes.Count > 0)
        {
            var compositeIndexes = new JArray(policy.CompositeIndexes.Select(indexSet =>
                new JArray(indexSet.Select(idx => new JObject
                {
                    ["path"] = idx.Path,
                    ["order"] = idx.Order.ToString().ToLowerInvariant()
                }))));
            indexingPolicyObj["compositeIndexes"] = compositeIndexes;
        }

        if (policy.SpatialIndexes.Count > 0)
        {
            var spatialIndexes = new JArray(policy.SpatialIndexes.Select(si => {
                var obj = new JObject { ["path"] = si.Path };
                if (si.SpatialTypes.Count > 0)
                    obj["types"] = new JArray(si.SpatialTypes.Select(t => (JToken)t.ToString()));
                return obj;
            }));
            indexingPolicyObj["spatialIndexes"] = spatialIndexes;
        }

        var metadata = new JObject
        {
            ["id"] = _container.Id,
            ["_rid"] = _collectionRid,
            ["_self"] = $"dbs/{_databaseRid}/colls/{_collectionRid}/",
            ["_etag"] = "\"00000000-0000-0000-0000-000000000000\"",
            ["_ts"] = 1700000000,
            ["partitionKey"] = new JObject
            {
                ["paths"] = paths,
                ["kind"] = paths.Count > 1 ? "MultiHash" : "Hash",
                ["version"] = 2
            },
            ["indexingPolicy"] = indexingPolicyObj,
            ["geospatialConfig"] = new JObject { ["type"] = "Geography" }
        };
        return metadata.ToString(Formatting.None);
    }

    private string GetPartitionKeyRanges()
    {
        var ranges = new JArray();
        var step = 0x1_0000_0000L / _partitionKeyRangeCount;
        for (var i = 0; i < _partitionKeyRangeCount; i++)
        {
            var minInclusive = PartitionKeyHash.RangeBoundaryToHex(i * step);
            var maxExclusive = i == _partitionKeyRangeCount - 1 ? "FF" : PartitionKeyHash.RangeBoundaryToHex((i + 1) * step);
            ranges.Add(new JObject
            {
                ["id"] = i.ToString(),
                ["_rid"] = _collectionRid,
                ["minInclusive"] = minInclusive,
                ["maxExclusive"] = maxExclusive,
                ["throughputFraction"] = 1.0 / _partitionKeyRangeCount,
                ["status"] = "online",
                ["_self"] = $"dbs/{_databaseRid}/colls/{_collectionRid}/pkranges/{i}/",
                ["_ts"] = 1700000000,
                ["_etag"] = PkRangesEtag.Trim('"')
            });
        }

        var result = new JObject
        {
            ["_rid"] = _collectionRid,
            ["PartitionKeyRanges"] = ranges,
            ["_count"] = _partitionKeyRangeCount
        };
        return result.ToString(Formatting.None);
    }

    /// <summary>
    /// Creates a routing <see cref="HttpMessageHandler"/> that dispatches requests to
    /// different <see cref="FakeCosmosHandler"/> instances based on the container name
    /// in the URL path. This enables a single <see cref="CosmosClient"/> to query
    /// multiple in-memory containers.
    /// </summary>
    /// <param name="handlers">
    /// A dictionary mapping container names to their handlers. The first entry is used
    /// as the default for account-level requests (e.g. GET /).
    /// </param>
    internal static HttpMessageHandler CreateRouter(
        IReadOnlyDictionary<string, FakeCosmosHandler> handlers)
    {
        return new RoutingHandler(handlers);
    }

    internal const string FakeConnectionString = "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;";

    /// <summary>
    /// Wraps an existing <see cref="CosmosClient"/> so that containers returned by
    /// <see cref="CosmosClient.GetContainer"/> automatically capture prefix partition
    /// keys for hierarchical PK queries. Use this when you have already constructed
    /// a <see cref="CosmosClient"/> with a custom HTTP pipeline (e.g. tracking handlers)
    /// and want to add prefix PK support on top.
    /// <para>
    /// <example>
    /// <code>
    /// var trackingHandler = new CosmosTrackingMessageHandler(opts, fakeHandler);
    /// var innerClient = new CosmosClient(connStr, new CosmosClientOptions
    /// {
    ///     ConnectionMode = ConnectionMode.Gateway,
    ///     HttpClientFactory = () => new HttpClient(trackingHandler)
    /// });
    /// var client = handler.WrapClient(innerClient);
    /// </code>
    /// </example>
    /// </para>
    /// </summary>
    /// <param name="innerClient">The <see cref="CosmosClient"/> to wrap.</param>
    /// <returns>
    /// A <see cref="CosmosClient"/> whose <see cref="CosmosClient.GetContainer"/>
    /// returns containers with automatic prefix PK capturing.
    /// </returns>
    internal CosmosClient WrapClient(CosmosClient innerClient)
        => WrapClient(innerClient, new Dictionary<string, FakeCosmosHandler> { [_container.Id] = this });

    /// <summary>
    /// Wraps an existing <see cref="CosmosClient"/> with prefix partition key capturing
    /// for multiple containers. Each container returned by <see cref="CosmosClient.GetContainer"/>
    /// will be matched against the <paramref name="handlers"/> dictionary to find the
    /// correct <see cref="FakeCosmosHandler"/> for PK capture.
    /// </summary>
    /// <param name="innerClient">The <see cref="CosmosClient"/> to wrap.</param>
    /// <param name="handlers">
    /// A dictionary mapping container names (or "database/container" keys) to their handlers.
    /// </param>
    internal static CosmosClient WrapClient(
        CosmosClient innerClient,
        IReadOnlyDictionary<string, FakeCosmosHandler> handlers)
        => new PkAwareCosmosClient(innerClient, handlers);

    /// <summary>
    /// Creates a <see cref="CosmosClient"/> backed by this handler that automatically
    /// captures prefix partition keys for hierarchical PK queries. This is a convenience
    /// method that builds the <see cref="CosmosClient"/> and wraps it in a single call.
    /// <para>
    /// For custom HTTP pipelines (e.g. tracking or logging handlers), construct the
    /// <see cref="CosmosClient"/> yourself and use <see cref="WrapClient(CosmosClient)"/> instead.
    /// </para>
    /// </summary>
    internal CosmosClient CreateClient()
        => CreateClient(new Dictionary<string, FakeCosmosHandler> { [_container.Id] = this });

    /// <summary>
    /// Creates a <see cref="CosmosClient"/> with a routing handler that dispatches
    /// to multiple <see cref="FakeCosmosHandler"/> instances, with automatic prefix
    /// partition key capturing for hierarchical PK queries.
    /// <para>
    /// For custom HTTP pipelines, construct the <see cref="CosmosClient"/> yourself
    /// with <see cref="CreateRouter"/> and use
    /// <see cref="WrapClient(CosmosClient, IReadOnlyDictionary{string, FakeCosmosHandler})"/> instead.
    /// </para>
    /// </summary>
    internal static CosmosClient CreateClient(
        Dictionary<string, FakeCosmosHandler> handlers)
    {
        HttpMessageHandler httpHandler = handlers.Count == 1
            ? handlers.Values.First()
            : CreateRouter(handlers);

        var innerClient = new CosmosClient(
            FakeConnectionString,
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                HttpClientFactory = () => new HttpClient(httpHandler)
            });

        return WrapClient(innerClient, handlers);
    }

    /// <summary>
    /// Runs a self-test to verify that the current Cosmos SDK version still uses the
    /// HTTP contract this handler expects (URL patterns, header names, response formats,
    /// ORDER BY wrapping, pagination, aggregates). Call this once during test suite setup
    /// to detect SDK breaking changes early, rather than getting silent data corruption.
    /// </summary>
    public static async Task VerifySdkCompatibilityAsync()
    {
        var sdkVersion = typeof(CosmosClient).Assembly.GetName().Version?.ToString() ?? "unknown";

        var container = new InMemoryContainer("compat-check", "/partitionKey");
        await container.CreateItemAsync(
            new CompatibilityDocument { Id = "1", PartitionKey = "pk", Name = "Alice", Value = 10 },
            new PartitionKey("pk"));
        await container.CreateItemAsync(
            new CompatibilityDocument { Id = "2", PartitionKey = "pk", Name = "Bob", Value = 20 },
            new PartitionKey("pk"));
        await container.CreateItemAsync(
            new CompatibilityDocument { Id = "3", PartitionKey = "pk", Name = "Charlie", Value = 30 },
            new PartitionKey("pk"));

        using var handler = new FakeCosmosHandler(container);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });

        var cosmosContainer = client.GetContainer("fakeDb", "compat-check");

        var allItems = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>().ToFeedIterator());
        if (allItems.Count != 3)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): expected 3 items from basic query but got {allItems.Count}. " +
                "The Cosmos SDK may have changed its internal HTTP contract.");
        }

        var ordered = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>()
                .OrderBy(document => document.Name)
                .ToFeedIterator());
        if (ordered.Count != 3 || ordered[0].Name != "Alice" || ordered[2].Name != "Charlie")
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): ORDER BY query returned unexpected results. " +
                "The Cosmos SDK ORDER BY response format may have changed.");
        }

        var filtered = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>()
                .Where(document => document.Value > 15)
                .ToFeedIterator());
        if (filtered.Count != 2)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): expected 2 items from filtered query but got {filtered.Count}. " +
                "The Cosmos SDK may have changed its query or header format.");
        }

        var paginatedItems = new List<CompatibilityDocument>();
        var pageIterator = cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>(
                requestOptions: new QueryRequestOptions { MaxItemCount = 1 })
            .ToFeedIterator();
        var pageCount = 0;
        while (pageIterator.HasMoreResults)
        {
            var page = await pageIterator.ReadNextAsync();
            paginatedItems.AddRange(page);
            pageCount++;
        }

        if (paginatedItems.Count != 3)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): paginated query returned {paginatedItems.Count} items instead of 3. " +
                "The Cosmos SDK may have changed its pagination or continuation token format.");
        }

        if (pageCount < 2)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): expected multiple pages with MaxItemCount=1 but got {pageCount} page(s). " +
                "The Cosmos SDK may have changed how it sends the x-ms-max-item-count header.");
        }

        var countResult = await cosmosContainer.GetItemLinqQueryable<CompatibilityDocument>().CountAsync();
        if (countResult.Resource != 3)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): CountAsync returned {countResult.Resource} instead of 3. " +
                "The Cosmos SDK may have changed its aggregate query format.");
        }

        if (!handler.RequestLog.Any(entry => entry.Contains("/pkranges")))
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): no partition key range request was detected. " +
                "The Cosmos SDK may have changed how it discovers partition key ranges.");
        }

        if (!handler.QueryLog.Any())
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): no query was logged. " +
                "The Cosmos SDK may have changed how it sends query requests.");
        }

        // CRUD roundtrip — verifies that create/read/delete HTTP routes work through the SDK
        var crudDoc = new CompatibilityDocument { Id = "crud-test", PartitionKey = "pk", Name = "CrudTest", Value = 99 };
        var createResponse = await cosmosContainer.CreateItemAsync(crudDoc, new PartitionKey("pk"));
        if (createResponse.StatusCode != HttpStatusCode.Created)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): CreateItemAsync returned {createResponse.StatusCode} instead of Created. " +
                "The Cosmos SDK may have changed its CRUD HTTP contract.");
        }

        var readResponse = await cosmosContainer.ReadItemAsync<CompatibilityDocument>("crud-test", new PartitionKey("pk"));
        if (readResponse.Resource?.Name != "CrudTest")
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): ReadItemAsync returned unexpected resource. " +
                "The Cosmos SDK may have changed its point-read HTTP contract.");
        }

        var deleteResponse = await cosmosContainer.DeleteItemAsync<CompatibilityDocument>("crud-test", new PartitionKey("pk"));
        if (deleteResponse.StatusCode != HttpStatusCode.NoContent)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): DeleteItemAsync returned {deleteResponse.StatusCode} instead of NoContent. " +
                "The Cosmos SDK may have changed its delete HTTP contract.");
        }

        // DISTINCT query (before upsert so item count is deterministic)
        var distinctItems = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemQueryIterator<dynamic>(
                new QueryDefinition("SELECT DISTINCT c.name FROM c")));
        if (distinctItems.Count != 3)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): DISTINCT query returned {distinctItems.Count} items instead of 3. " +
                "The Cosmos SDK may have changed its DISTINCT query handling.");
        }

        // OFFSET/LIMIT query (before upsert so item count is deterministic)
        var offsetLimitItems = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemQueryIterator<CompatibilityDocument>(
                new QueryDefinition("SELECT * FROM c OFFSET 1 LIMIT 2")));
        if (offsetLimitItems.Count != 2)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): OFFSET/LIMIT query returned {offsetLimitItems.Count} items instead of 2. " +
                "The Cosmos SDK may have changed its OFFSET/LIMIT handling.");
        }

        // Upsert roundtrip
        var upsertDoc = new CompatibilityDocument { Id = "upsert-test", PartitionKey = "pk", Name = "Upserted", Value = 42 };
        var upsertResponse = await cosmosContainer.UpsertItemAsync(upsertDoc, new PartitionKey("pk"));
        if (upsertResponse.StatusCode is not (HttpStatusCode.Created or HttpStatusCode.OK))
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): UpsertItemAsync returned {upsertResponse.StatusCode}. " +
                "The Cosmos SDK may have changed its upsert HTTP contract.");
        }

        // Patch roundtrip
        var patchOps = new[] { PatchOperation.Set("/value", 99) };
        var patchResponse = await cosmosContainer.PatchItemAsync<CompatibilityDocument>(
            "upsert-test", new PartitionKey("pk"), patchOps);
        if (patchResponse.Resource?.Value != 99)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): PatchItemAsync returned unexpected value. " +
                "The Cosmos SDK may have changed its patch HTTP contract.");
        }

        // Clean up upsert-test doc
        await cosmosContainer.DeleteItemAsync<CompatibilityDocument>("upsert-test", new PartitionKey("pk"));

        // Transactional batch roundtrip
        var batchDoc1 = new CompatibilityDocument { Id = "batch-1", PartitionKey = "pk", Name = "B1", Value = 1 };
        var batchDoc2 = new CompatibilityDocument { Id = "batch-2", PartitionKey = "pk", Name = "B2", Value = 2 };
        var batchResponse = await cosmosContainer.CreateTransactionalBatch(new PartitionKey("pk"))
            .CreateItem(batchDoc1)
            .CreateItem(batchDoc2)
            .ExecuteAsync();

        if (!batchResponse.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): TransactionalBatch returned " +
                $"{batchResponse.StatusCode}. The Cosmos SDK may have changed its batch wire format " +
                "(HybridRow schemas or RecordIO encoding).");
        }

        if (batchResponse.Count != 2)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): batch returned {batchResponse.Count} " +
                "results instead of 2.");
        }

        // Clean up batch docs
        await cosmosContainer.DeleteItemAsync<CompatibilityDocument>("batch-1", new PartitionKey("pk"));
        await cosmosContainer.DeleteItemAsync<CompatibilityDocument>("batch-2", new PartitionKey("pk"));

        // ReadMany roundtrip
        var readManyResult = await cosmosContainer.ReadManyItemsAsync<CompatibilityDocument>(
            new[] { ("1", new PartitionKey("pk")), ("2", new PartitionKey("pk")) });

        if (readManyResult.StatusCode != HttpStatusCode.OK)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): ReadManyItemsAsync returned " +
                $"{readManyResult.StatusCode}. The Cosmos SDK may have changed its ReadMany HTTP contract.");
        }

        if (readManyResult.Count != 2)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): ReadMany returned {readManyResult.Count} " +
                "items instead of 2.");
        }

        // Change feed roundtrip (uses backing container API since GetChangeFeedIterator
        // doesn't work through FakeCosmosHandler — it doesn't implement A-IM protocol)
        var changeFeedIterator = container.GetChangeFeedIterator<CompatibilityDocument>(0);
        var changeFeedItems = new List<CompatibilityDocument>();
        while (changeFeedIterator.HasMoreResults)
        {
            var page = await changeFeedIterator.ReadNextAsync();
            changeFeedItems.AddRange(page);
        }

        if (changeFeedItems.Count < 3)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): Change feed returned {changeFeedItems.Count} " +
                "items instead of at least 3. The change feed infrastructure may be broken.");
        }

        // GROUP BY query
        var groupByItems = await DrainFeedIteratorAsync(
            cosmosContainer.GetItemQueryIterator<dynamic>(
                new QueryDefinition("SELECT c.partitionKey, COUNT(1) AS cnt FROM c GROUP BY c.partitionKey")));

        if (groupByItems.Count < 1)
        {
            throw new InvalidOperationException(
                $"SDK compatibility check failed (v{sdkVersion}): GROUP BY query returned no results. " +
                "The Cosmos SDK may have changed its GROUP BY handling.");
        }

        // Verify no unrecognised headers were seen during the test
        if (handler.UnrecognisedHeaders.Count > 0)
        {
            var unknownHeaders = string.Join(", ", handler.UnrecognisedHeaders.Distinct());
            System.Diagnostics.Trace.TraceWarning(
                $"FakeCosmosHandler: SDK v{sdkVersion} sent unrecognised x-ms-* headers: {unknownHeaders}. " +
                "These may indicate new SDK features that are not yet handled.");
        }
    }

    private static async Task<List<T>> DrainFeedIteratorAsync<T>(FeedIterator<T> iterator)
    {
        var items = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            items.AddRange(page);
        }

        return items;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _queryResultCache.Clear();
            _container.Dispose();
        }

        base.Dispose(disposing);
    }

    /// <summary>
    /// Shared registry for dynamic container management. Holds the mutable dictionaries
    /// that both RoutingHandler and InMemoryCosmosResult reference.
    /// </summary>
    internal sealed class DynamicContainerRegistry
    {
        public ConcurrentDictionary<string, FakeCosmosHandler> Handlers { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<string, InMemoryContainer> BackingContainers { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<string, Container> SdkContainers { get; } = new(StringComparer.Ordinal);
        public HashSet<string> KnownDatabases { get; } = new(StringComparer.Ordinal);

        /// <summary>
        /// Maps database name → (container name → registry key).
        /// Registry keys are plain container names for default DB, compound "db/container" for explicit databases.
        /// </summary>
        public ConcurrentDictionary<string, ConcurrentDictionary<string, string>> DatabaseContainerKeys { get; } = new(StringComparer.Ordinal);

        /// <summary>Set of database names that use compound registry keys (added via AddDatabase).</summary>
        public HashSet<string> CompoundKeyDatabases { get; } = new(StringComparer.Ordinal);

        /// <summary>Client reference set after client creation so dynamic containers can register SDK containers.</summary>
        public CosmosClient? Client { get; set; }

        /// <summary>Database name used for dynamic container registration.</summary>
        public string DatabaseName { get; set; } = "default";
    }

    internal sealed class RoutingHandler : HttpMessageHandler
    {
        private readonly DynamicContainerRegistry _registry;
        private readonly FakeCosmosHandler _default;

        public RoutingHandler(IReadOnlyDictionary<string, FakeCosmosHandler> handlers)
            : this(handlers, new DynamicContainerRegistry())
        {
        }

        public RoutingHandler(IReadOnlyDictionary<string, FakeCosmosHandler> handlers, DynamicContainerRegistry registry)
        {
            if (!handlers.Any())
                throw new ArgumentException("At least one handler must be registered.", nameof(handlers));
            _registry = registry;
            foreach (var kvp in handlers)
                _registry.Handlers[kvp.Key] = kvp.Value;
            _default = handlers.Values.First();
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri?.AbsolutePath ?? "";
            var method = request.Method.Method;

            if (path is "/" or "")
            {
                return await InvokeHandlerAsync(_default, request, cancellationToken);
            }

            // Container creation: POST /dbs/{db}/colls
            // Container listing: GET /dbs/{db}/colls
            var collsMatch = Regex.Match(path, @"^/dbs/([^/]+)/colls/?$");
            if (collsMatch.Success)
            {
                var dbNameForColls = collsMatch.Groups[1].Value;
                if (method == "POST")
                    return await HandleCreateContainerAsync(request, dbNameForColls, cancellationToken);
                if (method == "GET")
                    return HandleListContainers(dbNameForColls);
            }

            // Database management: /dbs
            if (Regex.IsMatch(path, @"^/dbs/?$"))
            {
                return await HandleDatabaseListOrCreate(request, method, cancellationToken);
            }

            // Database CRUD: /dbs/{id}
            if (Regex.IsMatch(path, @"^/dbs/[^/]+/?$") && !path.Contains("/colls"))
            {
                return HandleDatabaseCrud(request, path, method);
            }

            var match = Regex.Match(path, @"/dbs/([^/]+)/colls/([^/]+)");
            if (match.Success)
            {
                var dbName = match.Groups[1].Value;
                var containerName = match.Groups[2].Value;

                // Container-level operations (not sub-resources)
                if (Regex.IsMatch(path, @"/colls/[^/]+/?$"))
                {
                    // DELETE container
                    if (method == "DELETE")
                    {
                        return HandleDeleteContainer(dbName, containerName);
                    }

                    // GET container metadata
                    if (method == "GET")
                    {
                        return HandleReadContainer(dbName, containerName);
                    }

                    // PUT container (replace)
                    if (method == "PUT")
                    {
                        return await HandleReplaceContainer(request, dbName, containerName, cancellationToken);
                    }
                }

                // Container listing: GET /dbs/{db}/colls (handled above, but also match here for safety)

                // Try database+container compound key first, then fall back to container-only
                var compoundKey = $"{dbName}/{containerName}";
                if (_registry.Handlers.TryGetValue(compoundKey, out var compoundHandler))
                {
                    return await InvokeHandlerAsync(compoundHandler, request, cancellationToken);
                }

                if (_registry.Handlers.TryGetValue(containerName, out var handler))
                {
                    return await InvokeHandlerAsync(handler, request, cancellationToken);
                }

                // SDK internal routes use base64-encoded RIDs (e.g. "AQAAAA==") for
                // partition key range and other metadata requests. Fall back to the
                // default handler for these rather than throwing.
                if (containerName.Contains('=') || path.Contains("/pkranges"))
                {
                    return await InvokeHandlerAsync(_default, request, cancellationToken);
                }

                return new HttpResponseMessage(HttpStatusCode.NotFound)
                {
                    Content = new StringContent(
                        $"{{\"message\":\"Container '{containerName}' not found. " +
                        $"Available containers: {string.Join(", ", _registry.Handlers.Keys.OrderBy(k => k))}\"}}",
                        Encoding.UTF8, "application/json")
                };
            }

            return await InvokeHandlerAsync(_default, request, cancellationToken);
        }

        private async Task<HttpResponseMessage> HandleCreateContainerAsync(
            HttpRequestMessage request, string dbName, CancellationToken cancellationToken)
        {
            var body = request.Content is not null
                ? await request.Content.ReadAsStringAsync(cancellationToken)
                : "{}";
            var containerProps = JsonConvert.DeserializeObject<ContainerProperties>(body)
                ?? new ContainerProperties("unknown", "/id");

            var containerName = containerProps.Id;
            var registryKey = GetNewRegistryKey(dbName, containerName);

            // IfNotExists: check x-ms-cosmos-is-upsert or use existence check
            var isIfNotExists = request.Headers.TryGetValues("x-ms-documentdb-is-upsert", out var upsertValues) &&
                                upsertValues.Any(v => v.Equals("True", StringComparison.OrdinalIgnoreCase));

            if (_registry.Handlers.ContainsKey(registryKey))
            {
                if (isIfNotExists)
                {
                    // Return existing container metadata
                    if (_registry.Handlers.TryGetValue(registryKey, out var existingHandler))
                    {
                        return CreateContainerMetadataResponse(existingHandler, HttpStatusCode.OK);
                    }
                }
                // If not IfNotExists, SDK sends 409 Conflict
                return new HttpResponseMessage(HttpStatusCode.Conflict)
                {
                    Content = new StringContent(
                        $"{{\"message\":\"Container '{containerName}' already exists.\"}}",
                        Encoding.UTF8, "application/json")
                };
            }

            // Create the new container
            var newContainer = new InMemoryContainer(containerProps);

            // Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/create-a-collection
            //   "x-ms-offer-throughput: The user specified throughput for the collection"
            if (request.Headers.TryGetValues("x-ms-offer-throughput", out var throughputValues) &&
                int.TryParse(throughputValues.FirstOrDefault(), out var throughputValue))
            {
                newContainer._throughput = throughputValue;
            }

            var newHandler = new FakeCosmosHandler(newContainer);

            _registry.Handlers[registryKey] = newHandler;
            _registry.BackingContainers[registryKey] = newContainer;

            // Track in database container keys
            var dbMap = _registry.DatabaseContainerKeys.GetOrAdd(dbName, _ => new(StringComparer.Ordinal));
            dbMap[containerName] = registryKey;

            // Register the SDK container if client is available
            if (_registry.Client is not null)
            {
                _registry.SdkContainers[registryKey] =
                    _registry.Client.GetContainer(dbName, containerName);
            }

            return CreateContainerMetadataResponse(newHandler, HttpStatusCode.Created);
        }

        private HttpResponseMessage HandleDeleteContainer(string dbName, string containerName)
        {
            var registryKey = ResolveExistingRegistryKey(dbName, containerName);
            if (registryKey is null || !_registry.Handlers.TryRemove(registryKey, out var handler))
            {
                return new HttpResponseMessage(HttpStatusCode.NotFound)
                {
                    Content = new StringContent(
                        $"{{\"message\":\"Container '{containerName}' not found.\"}}",
                        Encoding.UTF8, "application/json")
                };
            }

            _registry.BackingContainers.TryRemove(registryKey, out _);
            _registry.SdkContainers.TryRemove(registryKey, out _);

            // Remove from database container keys
            if (_registry.DatabaseContainerKeys.TryGetValue(dbName, out var dbMap))
                dbMap.TryRemove(containerName, out _);

            // Mark the backing container as deleted so future operations through
            // any cached handler reference return 404
            handler.BackingInMemoryContainer._isDeleted = true;

            return new HttpResponseMessage(HttpStatusCode.NoContent)
            {
                Headers = { { "x-ms-request-charge", "1" } }
            };
        }

        private HttpResponseMessage HandleReadContainer(string dbName, string containerName)
        {
            var registryKey = ResolveExistingRegistryKey(dbName, containerName);
            if (registryKey is not null && _registry.Handlers.TryGetValue(registryKey, out var handler))
            {
                return CreateContainerMetadataResponse(handler, HttpStatusCode.OK);
            }

            return new HttpResponseMessage(HttpStatusCode.NotFound)
            {
                Content = new StringContent(
                    $"{{\"message\":\"Container '{containerName}' not found.\"}}",
                    Encoding.UTF8, "application/json")
            };
        }

        private HttpResponseMessage HandleListContainers(string dbName)
        {
            var containerArray = new JArray();

            if (_registry.DatabaseContainerKeys.TryGetValue(dbName, out var containerKeys))
            {
                foreach (var (_, registryKey) in containerKeys)
                {
                    if (_registry.Handlers.TryGetValue(registryKey, out var h))
                        containerArray.Add(JObject.Parse(h.GetCollectionMetadata()));
                }
            }
            else
            {
                // Fallback for backward compatibility (no DatabaseContainerKeys populated)
                foreach (var handler in _registry.Handlers.Values)
                {
                    containerArray.Add(JObject.Parse(handler.GetCollectionMetadata()));
                }
            }

            var result = new JObject
            {
                ["_rid"] = _default._databaseRid,
                ["DocumentCollections"] = containerArray,
                ["_count"] = containerArray.Count
            };

            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(result.ToString(Formatting.None), Encoding.UTF8, "application/json")
            };
            response.Headers.TryAddWithoutValidation("x-ms-request-charge", "1");
            return response;
        }

        private async Task<HttpResponseMessage> HandleReplaceContainer(
            HttpRequestMessage request, string dbName, string containerName, CancellationToken cancellationToken)
        {
            var registryKey = ResolveExistingRegistryKey(dbName, containerName);
            if (registryKey is null || !_registry.Handlers.TryGetValue(registryKey, out var handler))
            {
                return new HttpResponseMessage(HttpStatusCode.NotFound);
            }

            // Parse the request body to check for PK path changes
            if (request.Content is not null)
            {
                var body = await request.Content.ReadAsStringAsync(cancellationToken);
                var props = JObject.Parse(body);
                var newPkPath = props.SelectToken("partitionKey.paths[0]")?.ToString();
                if (newPkPath is not null)
                {
                    var existingPkPath = handler.BackingInMemoryContainer.PartitionKeyPaths.FirstOrDefault();
                    if (existingPkPath is not null && !string.Equals(newPkPath, existingPkPath, StringComparison.Ordinal))
                    {
                        return new HttpResponseMessage(HttpStatusCode.BadRequest)
                        {
                            Content = new StringContent(
                                $"{{\"message\":\"Partition key path cannot be changed. " +
                                $"Existing: '{existingPkPath}', Requested: '{newPkPath}'\"}}",
                                Encoding.UTF8, "application/json")
                        };
                    }
                }

                // Apply mutable properties
                var container = handler.BackingInMemoryContainer;
                if (props["defaultTtl"] is { Type: JTokenType.Integer } ttlToken)
                {
                    var ttl = ttlToken.Value<int>();
                    if (ttl != 0) container.DefaultTimeToLive = ttl;
                }
                else if (props["defaultTtl"] is { Type: JTokenType.Null })
                {
                    container.DefaultTimeToLive = null;
                }
            }

            return CreateContainerMetadataResponse(handler, HttpStatusCode.OK);
        }

        private async Task<HttpResponseMessage> HandleDatabaseListOrCreate(
            HttpRequestMessage request, string method, CancellationToken cancellationToken)
        {
            // Delegate to the default handler for database operations
            return await InvokeHandlerAsync(_default, request, cancellationToken);
        }

        private HttpResponseMessage HandleDatabaseCrud(
            HttpRequestMessage request, string path, string method)
        {
            var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var dbName = segments.Length > 1 ? Uri.UnescapeDataString(segments[1]) : "fake-db";
            _registry.KnownDatabases.Add(dbName);

            return method switch
            {
                "GET" or "PUT" => _default.CreateJsonResponse(_default.GetDatabaseMetadata(dbName)),
                "DELETE" => new HttpResponseMessage(HttpStatusCode.NoContent) { Headers = { { "x-ms-request-charge", "1" } } },
                _ => new HttpResponseMessage(HttpStatusCode.MethodNotAllowed)
            };
        }

        /// <summary>Finds the registry key for an existing container, trying compound then plain.</summary>
        private string? ResolveExistingRegistryKey(string dbName, string containerName)
        {
            var compoundKey = $"{dbName}/{containerName}";
            if (_registry.Handlers.ContainsKey(compoundKey))
                return compoundKey;
            if (_registry.Handlers.ContainsKey(containerName))
                return containerName;
            return null;
        }

        /// <summary>Determines the registry key for a new container registration.</summary>
        private string GetNewRegistryKey(string dbName, string containerName)
        {
            return _registry.CompoundKeyDatabases.Contains(dbName)
                ? $"{dbName}/{containerName}"
                : containerName;
        }

        private static HttpResponseMessage CreateContainerMetadataResponse(
            FakeCosmosHandler handler, HttpStatusCode statusCode)
        {
            var metadata = handler.GetCollectionMetadata();
            var response = new HttpResponseMessage(statusCode)
            {
                Content = new StringContent(metadata, Encoding.UTF8, "application/json")
            };
            response.Headers.TryAddWithoutValidation("x-ms-request-charge", "1");
            response.Headers.TryAddWithoutValidation("etag", $"\"{Guid.NewGuid()}\"");
            return response;
        }

        private static Task<HttpResponseMessage> InvokeHandlerAsync(
            FakeCosmosHandler handler, HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            return invoker.SendAsync(request, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                foreach (var handler in _registry.Handlers.Values)
                {
                    handler.Dispose();
                }
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// Thread-safe query result cache with TTL-based eviction and bounded size.
    /// Prevents unbounded memory growth when consumers abandon mid-page iteration.
    /// </summary>
    private sealed class QueryResultCache
    {
        private readonly ConcurrentDictionary<string, CacheEntry> _entries = new();
        private readonly TimeSpan _ttl;
        private readonly int _maxEntries;

        public QueryResultCache(TimeSpan ttl, int maxEntries)
        {
            _ttl = ttl;
            _maxEntries = maxEntries;
        }

        public bool TryGet(string key, out List<JToken> value)
        {
            EvictStale();
            if (_entries.TryGetValue(key, out var entry) && !entry.IsExpired(_ttl))
            {
                entry.Touch();
                value = entry.Items;
                return true;
            }

            _entries.TryRemove(key, out _);
            value = null!;
            return false;
        }

        public void Set(string key, List<JToken> items)
        {
            EvictStale();
            _entries[key] = new CacheEntry(items);
        }

        public void Remove(string key)
        {
            _entries.TryRemove(key, out _);
        }

        public void Clear()
        {
            _entries.Clear();
        }

        private void EvictStale()
        {
            var keysToRemove = _entries
                .Where(pair => pair.Value.IsExpired(_ttl))
                .Select(pair => pair.Key)
                .ToList();

            foreach (var key in keysToRemove)
            {
                _entries.TryRemove(key, out _);
            }

            // If still over capacity after TTL eviction, remove oldest entries
            if (_entries.Count > _maxEntries)
            {
                var excess = _entries
                    .OrderBy(pair => pair.Value.LastAccessed)
                    .Take(_entries.Count - _maxEntries)
                    .Select(pair => pair.Key)
                    .ToList();

                foreach (var key in excess)
                {
                    _entries.TryRemove(key, out _);
                }
            }
        }

        private sealed class CacheEntry
        {
            public List<JToken> Items { get; }
            public DateTime CreatedAt { get; }
            public DateTime LastAccessed { get; private set; }

            public CacheEntry(List<JToken> items)
            {
                Items = items;
                CreatedAt = DateTime.UtcNow;
                LastAccessed = DateTime.UtcNow;
            }

            public bool IsExpired(TimeSpan ttl) => DateTime.UtcNow - CreatedAt > ttl;

            public void Touch() => LastAccessed = DateTime.UtcNow;
        }
    }
}
