using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Configuration for a single in-memory Cosmos container.
/// </summary>
public record ContainerConfig(
	string ContainerName,
	string PartitionKeyPath = "/id",
	string? DatabaseName = null,
	ContainerProperties? ContainerProperties = null);
