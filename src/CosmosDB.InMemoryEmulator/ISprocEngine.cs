using System.Collections.Generic;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Engine for executing JavaScript stored procedure bodies.
/// Implemented by the optional <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
/// </summary>
public interface ISprocEngine
{
	/// <summary>
	/// Executes a stored procedure JavaScript body and returns the result set via <c>response.setBody()</c>.
	/// </summary>
	/// <param name="jsBody">The JavaScript function body.</param>
	/// <param name="partitionKey">The partition key the sproc is scoped to.</param>
	/// <param name="args">Arguments passed by the caller.</param>
	/// <returns>The JSON string result (from <c>response.setBody()</c>), or null if setBody was not called.</returns>
	string? Execute(string jsBody, PartitionKey partitionKey, dynamic[] args);

	/// <summary>
	/// Executes a stored procedure JavaScript body with access to the collection context for CRUD operations.
	/// </summary>
	string? Execute(string jsBody, PartitionKey partitionKey, dynamic[] args, ICollectionContext context);

	/// <summary>
	/// Log messages captured from <c>console.log()</c> calls during the most recent <see cref="Execute"/> invocation.
	/// </summary>
	IReadOnlyList<string> CapturedLogs { get; }
}
