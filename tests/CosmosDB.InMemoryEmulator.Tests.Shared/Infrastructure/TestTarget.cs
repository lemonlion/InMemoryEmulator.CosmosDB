namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Identifies which backend a parity-validated test runs against.
/// Controlled by the <c>COSMOS_TEST_TARGET</c> environment variable.
/// </summary>
public enum TestTarget
{
	/// <summary>
	/// Default — FakeCosmosHandler backed by InMemoryContainer.
	/// Full SDK HTTP pipeline without a real emulator.
	/// </summary>
	InMemory,

	/// <summary>
	/// Linux Docker legacy emulator (mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest).
	/// SDK falls back to gateway HTTP for query plans.
	/// </summary>
	EmulatorLinux,

	/// <summary>
	/// Windows Docker emulator (mcr.microsoft.com/cosmosdb/windows/azure-cosmos-emulator).
	/// Requires Docker Desktop in Windows containers mode.
	/// Highest feature parity with real Azure Cosmos DB.
	/// </summary>
	EmulatorWindows
}
