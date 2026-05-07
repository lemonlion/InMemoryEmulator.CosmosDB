using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Provides test-only operations on a container: stored procedure, UDF, and trigger
/// registration, state import/export, point-in-time restore, and item reset.
/// Implemented by <see cref="InMemoryContainer"/>.
/// </summary>
public interface IContainerTestSetup
{
    // ─── C# delegate registrations ────────────────────────────────────────────

    /// <summary>
    /// Registers a stored procedure handler invoked by <c>ExecuteStoredProcedureAsync</c>.
    /// </summary>
    void RegisterStoredProcedure(string id, Func<PartitionKey, dynamic[], string> handler);

    /// <summary>
    /// Registers a user-defined function callable in SQL queries as <c>udf.name(args)</c>.
    /// </summary>
    void RegisterUdf(string name, Func<object[], object> handler);

    /// <summary>
    /// Registers a pre- or post-trigger handler.
    /// For pre-triggers, the handler receives and returns the (possibly modified) document.
    /// For post-triggers, wrap an <see cref="Action{JObject}"/> in a lambda that returns the input.
    /// </summary>
    void RegisterTrigger(string id, TriggerType type, TriggerOperation operation,
        Func<JObject, JObject> handler);

    // ─── JS body registrations (requires JsTriggers package) ──────────────────

    /// <summary>
    /// Registers a stored procedure from a JavaScript function body.
    /// Requires the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    /// <exception cref="NotImplementedException">
    /// Thrown when the JsTriggers package is not installed.
    /// </exception>
    void RegisterStoredProcedure(string id, string jsBody);

    /// <summary>
    /// Registers a UDF from a JavaScript function body.
    /// Requires the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    /// <exception cref="NotImplementedException">
    /// Thrown when the JsTriggers package is not installed.
    /// </exception>
    void RegisterUdf(string name, string jsBody);

    /// <summary>
    /// Registers a trigger from a JavaScript function body.
    /// Requires the <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    /// <exception cref="NotImplementedException">
    /// Thrown when the JsTriggers package is not installed.
    /// </exception>
    void RegisterTrigger(string id, TriggerType type, TriggerOperation operation,
        string jsBody);

    // ─── State management ─────────────────────────────────────────────────────

    /// <summary>Exports the current container state as a JSON string.</summary>
    string ExportState();

    /// <summary>Exports the current container state to a file.</summary>
    void ExportStateToFile(string path);

    /// <summary>Imports container state from a JSON string, replacing all existing data.</summary>
    void ImportState(string json);

    /// <summary>Imports container state from a file, replacing all existing data.</summary>
    void ImportStateFromFile(string path);

    /// <summary>
    /// Restores the container to its state at the specified point in time
    /// by replaying the change feed.
    /// </summary>
    void RestoreToPointInTime(DateTimeOffset timestamp);

    // ─── Reset ────────────────────────────────────────────────────────────────

    /// <summary>Removes all items, ETags, timestamps, and change feed entries.</summary>
    void ClearItems();

    // ─── Container configuration ──────────────────────────────────────────────

    /// <summary>
    /// Container-level default TTL in seconds. When set, items expire after this duration
    /// unless overridden by a per-item <c>_ttl</c> property. Set to <c>null</c> to disable.
    /// Setting to 0 throws BadRequest — use -1 for "enabled, no default expiry".
    /// </summary>
    int? DefaultTimeToLive { get; set; }

    /// <summary>
    /// The unique key policy for this container. Unique keys enforce uniqueness constraints
    /// on paths within a logical partition. Set before inserting items.
    /// </summary>
    UniqueKeyPolicy UniqueKeyPolicy { get; set; }

    /// <summary>
    /// Number of feed ranges returned by <c>GetFeedRangesAsync</c>. Defaults to 1.
    /// Set to a higher value to simulate multiple physical partitions.
    /// </summary>
    int FeedRangeCount { get; set; }

    /// <summary>
    /// Maximum number of entries retained in the change feed log. Defaults to 1000.
    /// Set to 0 to disable eviction (unbounded growth).
    /// </summary>
    int MaxChangeFeedSize { get; set; }

    /// <summary>
    /// When set, the container automatically saves its state to this file path on
    /// disposal and loads state from it on creation.
    /// </summary>
    string StateFilePath { get; set; }

    /// <summary>Returns the number of non-expired items currently stored.</summary>
    int ItemCount { get; }

    /// <summary>The partition key path(s) for this container.</summary>
    IReadOnlyList<string> PartitionKeyPaths { get; }

    /// <summary>
    /// Sets the JavaScript trigger engine. Requires the
    /// <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    IJsTriggerEngine JsTriggerEngine { get; set; }

    /// <summary>
    /// Sets the stored procedure engine. Requires the
    /// <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
    /// </summary>
    ISprocEngine SprocEngine { get; set; }
}
