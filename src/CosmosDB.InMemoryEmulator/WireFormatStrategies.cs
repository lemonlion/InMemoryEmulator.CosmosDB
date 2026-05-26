using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Strategy for building the query plan response returned to the Cosmos SDK.
/// The SDK sends a query plan request on non-Windows platforms (where ServiceInterop
/// is unavailable) and uses the response to configure its internal pipeline stages.
/// <para>
/// Override this if a new SDK version changes the expected query plan format and you
/// need a workaround before the library is updated.
/// </para>
/// </summary>
public interface IQueryPlanStrategy
{
	/// <summary>
	/// Builds a <c>PartitionedQueryExecutionInfo</c> JSON object for the given SQL query.
	/// </summary>
	/// <param name="sqlQuery">The raw SQL query string.</param>
	/// <param name="parsed">The parsed query (null if parsing failed).</param>
	/// <param name="collectionRid">The collection resource ID for query ranges.</param>
	/// <returns>The complete query plan JSON object.</returns>
	JObject BuildQueryPlan(string sqlQuery, CosmosSqlQuery? parsed, string collectionRid);
}

/// <summary>
/// Strategy for resolving HybridRow batch schemas used by the transactional batch
/// wire protocol. The Cosmos SDK encodes batch operations in HybridRow format with
/// specific schema IDs.
/// <para>
/// Override this if a new SDK version changes the batch schema format and you need
/// a workaround before the library is updated.
/// </para>
/// </summary>
public interface IBatchSchemaStrategy
{
	/// <summary>
	/// Returns true if batch schema resolution succeeded and batch operations are supported.
	/// </summary>
	bool IsAvailable { get; }

	/// <summary>
	/// A human-readable message describing why batch schemas are unavailable,
	/// or null if they are available.
	/// </summary>
	string? UnavailableReason { get; }
}
