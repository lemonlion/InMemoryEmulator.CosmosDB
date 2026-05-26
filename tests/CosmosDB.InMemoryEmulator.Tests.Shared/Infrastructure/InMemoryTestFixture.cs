using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Fixture that creates containers backed by FakeCosmosHandler + InMemoryContainer.
/// Uses the real CosmosClient SDK pipeline (HTTP, serialization, LINQ→SQL)
/// for an apples-to-apples comparison with the emulator fixture.
/// </summary>
public sealed class InMemoryTestFixture : ITestContainerFixture
{
	private readonly List<InMemoryCosmosResult> _tracked = [];

	public TestTarget Target => TestTarget.InMemory;
	public bool IsEmulator => false;

	public Task<Container> CreateContainerAsync(
		string containerName,
		string partitionKeyPath,
		Action<ContainerProperties>? configure = null)
	{
		var result = InMemoryCosmos.Create(containerName, partitionKeyPath,
			configureContainer: setup => ApplyContainerProperties(setup, containerName, partitionKeyPath, configure));
		_tracked.Add(result);
		return Task.FromResult(result.Container);
	}

	public Task<Container> CreateContainerAsync(
		string containerName,
		IReadOnlyList<string> partitionKeyPaths,
		Action<ContainerProperties>? configure = null)
	{
		var result = InMemoryCosmos.Create(containerName, partitionKeyPaths.ToArray(),
			configureContainer: setup => ApplyContainerProperties(setup, containerName, partitionKeyPaths, configure));
		_tracked.Add(result);
		return Task.FromResult(result.Container);
	}

	private static void ApplyContainerProperties(
		IContainerTestSetup setup,
		string containerName,
		object partitionKeyPaths,
		Action<ContainerProperties>? configure)
	{
		if (configure is null) return;

		// Create a ContainerProperties to capture what the configure callback wants to set
		var props = partitionKeyPaths is IReadOnlyList<string> paths
			? new ContainerProperties(containerName, paths)
			: new ContainerProperties(containerName, (string)partitionKeyPaths);
		configure(props);

		// Apply the captured settings to the IContainerTestSetup
		if (props.DefaultTimeToLive.HasValue)
			setup.DefaultTimeToLive = props.DefaultTimeToLive;
		if (props.UniqueKeyPolicy?.UniqueKeys?.Count > 0)
			setup.UniqueKeyPolicy = props.UniqueKeyPolicy;
	}

	public ValueTask DisposeAsync()
	{
		foreach (var result in _tracked)
			result.Dispose();
		_tracked.Clear();
		return ValueTask.CompletedTask;
	}
}
