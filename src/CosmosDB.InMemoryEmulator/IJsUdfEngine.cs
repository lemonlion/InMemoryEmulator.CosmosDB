namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Engine for executing JavaScript UDF bodies.
/// Implemented alongside <see cref="IJsTriggerEngine"/> by the JsTriggers package.
/// </summary>
public interface IJsUdfEngine
{
	object? ExecuteUdf(string jsBody, object[] args);
}
