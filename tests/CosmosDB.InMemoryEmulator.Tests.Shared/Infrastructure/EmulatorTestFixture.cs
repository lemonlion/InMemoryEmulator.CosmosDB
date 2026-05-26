using System.Net;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Per-test-class fixture that returns real <see cref="Container"/> references
/// for the emulator-backed integration suite. Containers are NOT created lazily
/// here — they are pre-created in bulk by <c>tests/EmulatorWarmup</c> before
/// <c>dotnet test</c> starts (see <c>scripts/run-tests.ps1</c>). This keeps the
/// emulator's 503/1007 + 404/1013 cold-start cost off the per-test path so test
/// classes don't accumulate to the 45m CI job timeout.
///
/// Tests look up their container by name; the fixture refuses unknown names so a
/// new test adding a container without registering it in
/// <c>tests/emulator-containers.json</c> fails loudly instead of silently
/// retrying through 503s.
/// </summary>
public sealed class EmulatorTestFixture : ITestContainerFixture
{
	private readonly EmulatorSession _session;

	public TestTarget Target => _session.Target;
	public bool IsEmulator => true;

	public EmulatorTestFixture(EmulatorSession session)
	{
		_session = session;
		if (!session.IsEmulator || session.EmulatorClient is null || session.EmulatorDatabase is null)
			throw new InvalidOperationException(
				$"EmulatorTestFixture requires an initialised emulator session. Target={session.Target}");
	}

	public Task<Container> CreateContainerAsync(
		string containerName,
		string partitionKeyPath,
		Action<ContainerProperties>? configure = null)
		=> GetOrAssertAsync(containerName, new[] { partitionKeyPath });

	public Task<Container> CreateContainerAsync(
		string containerName,
		IReadOnlyList<string> partitionKeyPaths,
		Action<ContainerProperties>? configure = null)
		=> GetOrAssertAsync(containerName, partitionKeyPaths);

	private async Task<Container> GetOrAssertAsync(
		string containerName, IReadOnlyList<string> requestedPaths)
	{
		var spec = _session.Manifest.Get(containerName);
		AssertPathsMatch(spec, requestedPaths);

		if (!_session.ContainerCache.TryGetValue(containerName, out var existing))
		{
			// The warmup tool should have created this; if it isn't in the cache
			// something has gone wrong with the run-tests.ps1 warmup step.
			throw new InvalidOperationException(
				$"Container '{containerName}' is in the manifest but was not pre-created. " +
				"Check that scripts/run-tests.ps1 invoked tests/EmulatorWarmup before tests started.");
		}

		await CleanContainerAsync(existing, spec.Paths[0]);
		return existing;
	}

	private static void AssertPathsMatch(EmulatorContainerSpec spec, IReadOnlyList<string> requested)
	{
		var manifestPaths = spec.Paths;
		if (manifestPaths.Count != requested.Count
			|| !manifestPaths.SequenceEqual(requested, StringComparer.Ordinal))
		{
			throw new InvalidOperationException(
				$"Container '{spec.Name}' partition key path mismatch. " +
				$"Manifest: [{string.Join(", ", manifestPaths)}]. " +
				$"Test requested: [{string.Join(", ", requested)}]. " +
				"Update tests/emulator-containers.json or the test to agree.");
		}
	}

	/// <summary>
	/// Removes all documents from a cached container to maintain per-test isolation.
	/// Faster than dropping and recreating the container (which exhausts the partition pool).
	/// </summary>
	private static async Task CleanContainerAsync(Container container, string partitionKeyPath)
	{
		var pkProp = partitionKeyPath.TrimStart('/');
		// Use a small page size to avoid oversized responses
		var query = container.GetItemQueryIterator<JObject>(
			$"SELECT c.id, c[\"{pkProp}\"] AS __pk FROM c",
			requestOptions: new QueryRequestOptions { MaxItemCount = 100 });

		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			foreach (var item in page)
			{
				var id = item["id"]?.ToString();
				var pkValue = item["__pk"];
				if (id is null) continue;

				var pk = pkValue?.Type switch
				{
					JTokenType.String => new PartitionKey(pkValue.ToString()),
					JTokenType.Integer => new PartitionKey(pkValue.Value<long>()),
					JTokenType.Float => new PartitionKey(pkValue.Value<double>()),
					JTokenType.Boolean => new PartitionKey(pkValue.Value<bool>()),
					JTokenType.Null or null => PartitionKey.None,
					_ => new PartitionKey(pkValue.ToString())
				};

				try
				{
					await container.DeleteItemAsync<object>(id, pk);
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
				{
					// Item may have been deleted by TTL or another concurrent operation
				}
			}
		}
	}

	// No per-test container deletion needed: containers are managed by the
	// warmup tool and cleaned at the end of the test run by EmulatorSession.
	public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
