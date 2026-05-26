using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Options for <see cref="ServiceCollectionExtensions.UseInMemoryCosmosDB"/>.
/// Configures how CosmosClient and Container registrations are replaced with in-memory equivalents.
/// </summary>
public class InMemoryCosmosOptions
{
	/// <summary>
	/// Container configurations. If empty, the method registers a single default
	/// container with partition key "/id".
	/// </summary>
	public List<ContainerConfig> Containers { get; } = new();

	/// <summary>
	/// The database name to use. Falls back to "in-memory-db" if not specified.
	/// </summary>
	public string? DatabaseName { get; set; }

	/// <summary>
	/// When true (default), calls <see cref="InMemoryFeedIteratorSetup.Register"/> so that
	/// <c>.ToFeedIteratorOverridable()</c> works for LINQ queries.
	/// </summary>
	public bool RegisterFeedIteratorSetup { get; set; } = true;

	/// <summary>
	/// Callback invoked with the <see cref="CosmosClient"/> after it is created.
	/// Use this to capture the client reference for test assertions, etc.
	/// </summary>
	public Action<CosmosClient>? OnClientCreated { get; set; }

	/// <summary>
	/// Callback invoked with each <see cref="FakeCosmosHandler"/> after it is created.
	/// Use this to capture handler references for fault injection, request logging,
	/// or to access the backing <see cref="InMemoryContainer"/> via
	/// <see cref="FakeCosmosHandler.BackingContainer"/>.
	/// </summary>
	public Action<string, FakeCosmosHandler>? OnHandlerCreated { get; set; }

	/// <summary>
	/// Optional function that wraps the final <see cref="HttpMessageHandler"/>
	/// (the <see cref="FakeCosmosHandler"/> or multi-container router) before it is
	/// passed to <see cref="CosmosClientOptions.HttpClientFactory"/>.
	/// <para>
	/// Use this to insert a <see cref="DelegatingHandler"/> into the pipeline.
	/// The input is the handler that serves in-memory responses; the return value
	/// replaces it as the outermost handler in the <see cref="HttpClient"/>.
	/// </para>
	/// <para>
	/// When <c>null</c> (the default), the handler is used as-is — no behaviour change
	/// for existing consumers.
	/// </para>
	/// <para>
	/// Only applies to <see cref="ServiceCollectionExtensions.UseInMemoryCosmosDB"/>.
	/// Ignored by <c>UseInMemoryCosmosDB&lt;TClient&gt;()</c> (no HTTP pipeline).
	/// </para>
	/// </summary>
	public Func<HttpMessageHandler, HttpMessageHandler>? HttpMessageHandlerWrapper { get; set; }

	/// <summary>
	/// When set, each container automatically loads its state from this directory on creation
	/// and saves its state back on disposal. Files are named <c>{DatabaseName}_{ContainerName}.json</c>.
	/// If the directory or file does not exist, the container starts empty—state is saved on first disposal.
	/// <para>
	/// This enables persisting container data between test runs without any manual
	/// <see cref="InMemoryContainer.ExportState"/>/<see cref="InMemoryContainer.ImportState"/> calls.
	/// </para>
	/// </summary>
	public string? StatePersistenceDirectory { get; set; }

	/// <summary>
	/// Adds a container configuration.
	/// </summary>
	public InMemoryCosmosOptions AddContainer(
		string containerName,
		string partitionKeyPath = "/id",
		string? databaseName = null)
	{
		Containers.Add(new ContainerConfig(containerName, partitionKeyPath, databaseName));
		return this;
	}

	/// <summary>
	/// Adds a container configuration using full <see cref="ContainerProperties"/>,
	/// which supports UniqueKeyPolicy, DefaultTimeToLive, hierarchical partition keys, etc.
	/// </summary>
	public InMemoryCosmosOptions AddContainer(
		ContainerProperties containerProperties,
		string? databaseName = null)
	{
		string pkPath;
		try { pkPath = containerProperties.PartitionKeyPath; }
		catch (NotImplementedException) { pkPath = containerProperties.PartitionKeyPaths[0]; }

		Containers.Add(new ContainerConfig(
			containerProperties.Id,
			pkPath,
			databaseName,
			containerProperties));
		return this;
	}

	/// <summary>
	/// Sets <see cref="HttpMessageHandlerWrapper"/> to the specified function.
	/// The function receives the <see cref="FakeCosmosHandler"/> (or multi-container
	/// router) and must return the handler to use as the outermost handler in
	/// the <see cref="HttpClient"/>.
	/// </summary>
	public InMemoryCosmosOptions WithHttpMessageHandlerWrapper(
		Func<HttpMessageHandler, HttpMessageHandler> wrapper)
	{
		HttpMessageHandlerWrapper = wrapper;
		return this;
	}
}
