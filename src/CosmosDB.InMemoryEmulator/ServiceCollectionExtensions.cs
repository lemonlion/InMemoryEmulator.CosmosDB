using System.Reflection;
using Castle.DynamicProxy;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Extension methods for replacing Cosmos DB service registrations with in-memory equivalents.
/// Designed to be called from <c>ConfigureTestServices</c> inside <c>WebApplicationFactory</c>.
/// </summary>
public static class ServiceCollectionExtensions
{
	private const string DefaultDatabaseName = "in-memory-db";
	private const string FakeConnectionString = "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;";

	/// <summary>
	/// Replaces all registered <see cref="CosmosClient"/> and <see cref="Container"/>
	/// instances in the service collection with in-memory equivalents backed by
	/// <see cref="FakeCosmosHandler"/>. This provides the highest fidelity: a real
	/// <see cref="CosmosClient"/> exercises the full SDK HTTP pipeline, with
	/// <see cref="FakeCosmosHandler"/> intercepting requests in-process.
	/// LINQ <c>.ToFeedIterator()</c> works without any production code changes.
	/// </summary>
	public static IServiceCollection UseInMemoryCosmosDB(
		this IServiceCollection services,
		Action<InMemoryCosmosOptions>? configure = null)
	{
		var options = new InMemoryCosmosOptions();
		configure?.Invoke(options);

		var databaseName = options.DatabaseName ?? DefaultDatabaseName;

		// Determine how many Container registrations existed and their lifetime
		var existingContainerDescriptors = services
			.Where(d => d.ServiceType == typeof(Container))
			.ToList();
		var containerLifetime = existingContainerDescriptors.FirstOrDefault()?.Lifetime
								?? ServiceLifetime.Singleton;

		// Always replace CosmosClient
		services.RemoveAll<CosmosClient>();

		// Determine container configs
		List<ContainerConfig> containerConfigs;
		if (options.Containers.Count > 0)
		{
			containerConfigs = options.Containers;
			// Explicit mode: remove existing Container registrations
			services.RemoveAll<Container>();
		}
		else if (existingContainerDescriptors.Count == 0)
		{
			// No existing registrations and no explicit config: create a default container
			containerConfigs = [new ContainerConfig("in-memory-container")];
		}
		else
		{
			// Auto-detect mode: no explicit containers, but there are existing registrations.
			// We still need to create a handler. Use a default container.
			containerConfigs = [new ContainerConfig("in-memory-container")];
		}

		// Create InMemoryContainers and FakeCosmosHandlers
		var handlers = new Dictionary<string, FakeCosmosHandler>();
		foreach (var config in containerConfigs)
		{
			var container = config.ContainerProperties != null
				? new InMemoryContainer(config.ContainerProperties)
				: new InMemoryContainer(config.ContainerName, config.PartitionKeyPath);

			if (options.StatePersistenceDirectory is not null)
			{
				var dbName = config.DatabaseName ?? databaseName;
				var fileName = $"{dbName}_{config.ContainerName}.json";
				container.StateFilePath = Path.Combine(options.StatePersistenceDirectory, fileName);
				container.LoadPersistedState();
			}

			var handler = new FakeCosmosHandler(container);
			// Use compound key (db/container) when databaseName is specified
			if (config.DatabaseName is not null)
			{
				handlers[$"{config.DatabaseName}/{config.ContainerName}"] = handler;
			}
			// Also register container-only key (for backward compat and single-db scenarios),
			// but only if no other container with the same name but different database exists.
			if (!handlers.ContainsKey(config.ContainerName))
			{
				handlers[config.ContainerName] = handler;
			}
			options.OnHandlerCreated?.Invoke(config.ContainerName, handler);
		}

		// Build the HTTP handler (single or router)
		HttpMessageHandler httpHandler = handlers.Count == 1
			? handlers.Values.First()
			: FakeCosmosHandler.CreateRouter(handlers);

		// Apply optional wrapper (e.g. DelegatingHandler for logging/tracking)
		var finalHandler = options.HttpMessageHandlerWrapper?.Invoke(httpHandler) ?? httpHandler;

		// Create a real CosmosClient with the FakeCosmosHandler, wrapped for prefix PK support
		var innerClient = new CosmosClient(
			FakeConnectionString,
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				HttpClientFactory = () => new HttpClient(finalHandler)
			});
		var client = FakeCosmosHandler.WrapClient(innerClient, handlers);

		options.OnClientCreated?.Invoke(client);

		// Register Container(s) in DI
		if (options.Containers.Count > 0 || existingContainerDescriptors.Count == 0)
		{
			foreach (var config in containerConfigs)
			{
				var dbName = config.DatabaseName ?? databaseName;
				var containerName = config.ContainerName;
				services.Add(new ServiceDescriptor(
					typeof(Container),
					_ => client.GetContainer(dbName, containerName),
					containerLifetime));
			}
		}
		// else: auto-detect mode — keep existing Container registrations as-is

		// Register the real CosmosClient backed by FakeCosmosHandler
		services.AddSingleton<CosmosClient>(client);

		// No need for InMemoryFeedIteratorSetup — FakeCosmosHandler handles .ToFeedIterator() natively

		return services;
	}

	/// <summary>
	/// Replaces all registered <see cref="Container"/> instances in the service collection
	/// with in-memory equivalents backed by <see cref="FakeCosmosHandler"/>.
	/// Does NOT replace <see cref="CosmosClient"/> — any existing client registration is preserved.
	/// A hidden internal <see cref="CosmosClient"/> is created for the in-memory containers
	/// so that <c>.ToFeedIterator()</c> works without any production code changes.
	/// </summary>
	public static IServiceCollection UseInMemoryCosmosContainers(
		this IServiceCollection services,
		Action<InMemoryContainerOptions>? configure = null)
	{
		var options = new InMemoryContainerOptions();
		configure?.Invoke(options);

		var databaseName = options.DatabaseName ?? DefaultDatabaseName;

		// Determine existing lifetime
		var existingDescriptor = services.FirstOrDefault(d => d.ServiceType == typeof(Container));
		var lifetime = existingDescriptor?.Lifetime ?? ServiceLifetime.Singleton;

		// Remove existing Container registrations
		services.RemoveAll<Container>();

		// Determine container configs
		var containerConfigs = options.Containers.Count > 0
			? options.Containers
			: [new ContainerConfig("in-memory-container")];

		// Create InMemoryContainers and FakeCosmosHandlers
		var handlers = new Dictionary<string, FakeCosmosHandler>();
		foreach (var config in containerConfigs)
		{
			var container = config.ContainerProperties != null
				? new InMemoryContainer(config.ContainerProperties)
				: new InMemoryContainer(config.ContainerName, config.PartitionKeyPath);

			if (options.StatePersistenceDirectory is not null)
			{
				var fileName = $"{config.ContainerName}.json";
				container.StateFilePath = Path.Combine(options.StatePersistenceDirectory, fileName);
				container.LoadPersistedState();
			}

			options.OnContainerCreated?.Invoke(container);

			var handler = new FakeCosmosHandler(container);
			if (!handlers.ContainsKey(config.ContainerName))
			{
				handlers[config.ContainerName] = handler;
			}
			options.OnHandlerCreated?.Invoke(config.ContainerName, handler);
		}

		// Build the HTTP handler (single or router)
		HttpMessageHandler httpHandler = handlers.Count == 1
			? handlers.Values.First()
			: FakeCosmosHandler.CreateRouter(handlers);

		// Apply optional wrapper (e.g. DelegatingHandler for logging/tracking)
		var finalHandler = options.HttpMessageHandlerWrapper?.Invoke(httpHandler) ?? httpHandler;

		// Create a hidden internal CosmosClient with the FakeCosmosHandler
		var innerClient = new CosmosClient(
			FakeConnectionString,
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				HttpClientFactory = () => new HttpClient(finalHandler)
			});
		var client = FakeCosmosHandler.WrapClient(innerClient, handlers);

		// Register Container(s) in DI — but NOT the CosmosClient
		foreach (var config in containerConfigs)
		{
			var containerName = config.ContainerName;
			services.Add(new ServiceDescriptor(
				typeof(Container),
				_ => client.GetContainer(databaseName, containerName),
				lifetime));
		}

		return services;
	}

	/// <summary>
	/// Replaces a typed <typeparamref name="TClient"/> registration with an in-memory equivalent
	/// backed by <see cref="FakeCosmosHandler"/>. Designed for Pattern 2 (SCA.Common style) where
	/// multiple typed <see cref="CosmosClient"/> subclasses are registered in DI and repos resolve
	/// the specific typed client.
	/// <para>
	/// <typeparamref name="TClient"/> must extend <see cref="CosmosClient"/> and have a constructor
	/// accepting <c>(string connectionString, CosmosClientOptions options)</c>. A Castle.Core
	/// dynamic proxy is created that intercepts <see cref="CosmosClient.GetContainer"/> to provide
	/// transparent prefix partition key support for hierarchical partition keys.
	/// </para>
	/// <para>
	/// <b>No production code changes are needed.</b> Pass your real production typed client directly:
	/// <code>
	/// services.UseInMemoryCosmosDB&lt;EmployeeCosmosClient&gt;(o =&gt;
	///     o.AddContainer("employees", "/id"));
	/// </code>
	/// </para>
	/// <para>
	/// Does NOT register <see cref="Container"/> in DI — Pattern 2 repos call
	/// <c>client.GetContainer()</c> directly. Does NOT register as the base <see cref="CosmosClient"/>
	/// type — each typed client is independent.
	/// </para>
	/// </summary>
	public static IServiceCollection UseInMemoryCosmosDB<TClient>(
		this IServiceCollection services,
		Action<InMemoryCosmosOptions>? configure = null)
		where TClient : CosmosClient
	{
		var options = new InMemoryCosmosOptions();
		configure?.Invoke(options);

		var databaseName = options.DatabaseName ?? DefaultDatabaseName;

		// Determine existing lifetime
		var existingDescriptor = services.FirstOrDefault(d => d.ServiceType == typeof(TClient));
		var lifetime = existingDescriptor?.Lifetime ?? ServiceLifetime.Singleton;

		// Remove existing registration for the typed client
		services.RemoveAll<TClient>();

		// Determine container configs
		var containerConfigs = options.Containers.Count > 0
			? options.Containers
			: [new ContainerConfig("in-memory-container")];

		// Create InMemoryContainers and FakeCosmosHandlers
		var handlers = new Dictionary<string, FakeCosmosHandler>();
		foreach (var config in containerConfigs)
		{
			var container = config.ContainerProperties != null
				? new InMemoryContainer(config.ContainerProperties)
				: new InMemoryContainer(config.ContainerName, config.PartitionKeyPath);

			if (options.StatePersistenceDirectory is not null)
			{
				var dbName = config.DatabaseName ?? databaseName;
				var fileName = $"{dbName}_{config.ContainerName}.json";
				container.StateFilePath = Path.Combine(options.StatePersistenceDirectory, fileName);
				container.LoadPersistedState();
			}

			var handler = new FakeCosmosHandler(container);
			if (config.DatabaseName is not null)
			{
				handlers[$"{config.DatabaseName}/{config.ContainerName}"] = handler;
			}
			if (!handlers.ContainsKey(config.ContainerName))
			{
				handlers[config.ContainerName] = handler;
			}
			options.OnHandlerCreated?.Invoke(config.ContainerName, handler);
		}

		// Build the HTTP handler (single or router)
		HttpMessageHandler httpHandler = handlers.Count == 1
			? handlers.Values.First()
			: FakeCosmosHandler.CreateRouter(handlers);

		// Apply optional wrapper (e.g. DelegatingHandler for logging/tracking)
		var finalHandler = options.HttpMessageHandlerWrapper?.Invoke(httpHandler) ?? httpHandler;

		// Build CosmosClientOptions with the FakeCosmosHandler
		var cosmosClientOptions = new CosmosClientOptions
		{
			ConnectionMode = ConnectionMode.Gateway,
			HttpClientFactory = () => new HttpClient(finalHandler)
		};

		// Create a Castle.Core dynamic proxy of TClient that intercepts GetContainer()
		// to wrap results in PartitionKeyCapturingContainer for hierarchical PK prefix support.
		TClient client;
		try
		{
			var generator = new ProxyGenerator();
			var interceptor = new GetContainerInterceptor(handlers);
			var proxyOptions = new ProxyGenerationOptions(new GetContainerOnlyHook());
			client = (TClient)generator.CreateClassProxy(
				typeof(TClient),
				Type.EmptyTypes,
				proxyOptions,
				new object[] { FakeConnectionString, cosmosClientOptions },
				interceptor);
		}
		catch (Exception ex) when (ex is not OutOfMemoryException)
		{
			throw new InvalidOperationException(
				$"UseInMemoryCosmosDB<{typeof(TClient).Name}>() failed to create a dynamic proxy. " +
				$"Ensure that '{typeof(TClient).Name}' is not sealed and has a public constructor " +
				$"accepting (string connectionString, CosmosClientOptions options). " +
				$"See inner exception for details.", ex);
		}

		options.OnClientCreated?.Invoke(client);

		// Register as the typed client only — not as base CosmosClient
		services.Add(new ServiceDescriptor(typeof(TClient), _ => client, lifetime));

		return services;
	}

	/// <summary>
	/// Castle.Core interceptor that wraps <see cref="CosmosClient.GetContainer"/> results
	/// in <see cref="PartitionKeyCapturingContainer"/> for transparent hierarchical partition
	/// key prefix support.
	/// </summary>
	private sealed class GetContainerInterceptor(
		IReadOnlyDictionary<string, FakeCosmosHandler> handlers) : IInterceptor
	{
		public void Intercept(IInvocation invocation)
		{
			invocation.Proceed();
			if (invocation.Method.Name == nameof(CosmosClient.GetContainer)
				&& invocation.ReturnValue is Container container)
			{
				var databaseId = (string)invocation.Arguments[0];
				var containerId = (string)invocation.Arguments[1];
				if (handlers.TryGetValue(containerId, out var handler) ||
					handlers.TryGetValue($"{databaseId}/{containerId}", out handler))
				{
					invocation.ReturnValue = new PartitionKeyCapturingContainer(container, handler);
				}
			}
		}
	}

	/// <summary>
	/// Restricts Castle.Core to only proxy the <c>GetContainer</c> method, avoiding
	/// <see cref="TypeLoadException"/> from <c>internal virtual</c> members on
	/// <see cref="CosmosClient"/>.
	/// </summary>
	private sealed class GetContainerOnlyHook : IProxyGenerationHook
	{
		public bool ShouldInterceptMethod(Type type, MethodInfo methodInfo)
			=> methodInfo.Name == nameof(CosmosClient.GetContainer);

		public void NonProxyableMemberNotification(Type type, MemberInfo memberInfo) { }
		public void MethodsInspected() { }
	}
}
