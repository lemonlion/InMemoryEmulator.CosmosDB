using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Abstraction over container creation for parity-validated tests.
/// In-memory tests use FakeCosmosHandler; emulator tests create real containers.
/// </summary>
public interface ITestContainerFixture : IAsyncDisposable
{
	/// <summary>The backend this fixture targets.</summary>
	TestTarget Target { get; }

	/// <summary>True when running against a real Cosmos DB emulator.</summary>
	bool IsEmulator { get; }

	/// <summary>
	/// Creates a container with a single partition key path.
	/// </summary>
	/// <param name="containerName">Logical container name (unique suffix added for emulator).</param>
	/// <param name="partitionKeyPath">Partition key path, e.g. "/partitionKey".</param>
	/// <param name="configure">Optional callback to configure <see cref="ContainerProperties"/>
	/// (e.g. TTL, unique keys, computed properties).</param>
	/// <returns>A <see cref="Container"/> backed by the selected target.</returns>
	Task<Container> CreateContainerAsync(
		string containerName,
		string partitionKeyPath,
		Action<ContainerProperties>? configure = null);

	/// <summary>
	/// Creates a container with hierarchical (composite) partition key paths.
	/// </summary>
	Task<Container> CreateContainerAsync(
		string containerName,
		IReadOnlyList<string> partitionKeyPaths,
		Action<ContainerProperties>? configure = null);
}
