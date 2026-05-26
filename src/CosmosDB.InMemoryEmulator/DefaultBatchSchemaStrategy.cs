using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Default implementation of <see cref="IBatchSchemaStrategy"/> that resolves HybridRow
/// schemas using the self-built approach first, then falls back to reflection into the
/// SDK's internal BatchSchemaProvider. If both approaches fail, <see cref="IsAvailable"/>
/// returns false and batch requests return 501 Not Implemented instead of throwing.
/// </summary>
public sealed class DefaultBatchSchemaStrategy : IBatchSchemaStrategy
{
	private readonly Lazy<(bool Available, string? Reason)> _state = new(Probe);

	/// <inheritdoc />
	public bool IsAvailable => _state.Value.Available;

	/// <inheritdoc />
	public string? UnavailableReason => _state.Value.Reason;

	private static (bool, string?) Probe()
	{
		try
		{
			// Force the BatchSchemas lazy to evaluate — if it throws, schemas are unavailable.
			_ = FakeCosmosHandler.BatchSchemasAvailable;
			return (true, null);
		}
		catch (Exception ex)
		{
			var sdkVersion = typeof(CosmosClient).Assembly.GetName().Version?.ToString() ?? "unknown";
			return (false,
				$"HybridRow batch schemas could not be resolved (SDK v{sdkVersion}). " +
				$"Batch operations are unavailable. Error: {ex.Message}");
		}
	}
}
