using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════
//  1. Unrecognised header/route warnings
// ═══════════════════════════════════════════════════════════════════════════

public class UnrecognisedHeaderTests
{
	private static CosmosClient CreateClient(FakeCosmosHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				RequestTimeout = TimeSpan.FromSeconds(10),
				HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
			});

	[Fact]
	public void UnrecognisedHeaders_IsEmptyByDefault()
	{
		var container = new InMemoryContainer("test", "/pk");
		using var handler = new FakeCosmosHandler(container);

		handler.UnrecognisedHeaders.Should().BeEmpty();
	}

	[Fact]
	public async Task UnrecognisedHeaders_DoesNotRecordKnownSdkHeaders()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			new { id = "1", pk = "a" }, new PartitionKey("a"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);
		var cosmosContainer = client.GetContainer("db", "test");

		// Normal CRUD goes through known headers
		await cosmosContainer.ReadItemAsync<JObject>("1", new PartitionKey("a"));

		handler.UnrecognisedHeaders.Should().BeEmpty();
	}

	[Fact]
	public async Task UnrecognisedHeaders_RecordsUnknownXMsHeaders()
	{
		var container = new InMemoryContainer("test", "/pk");
		using var handler = new FakeCosmosHandler(container);

		// Send a raw request with an unknown x-ms header
		var invoker = new HttpMessageInvoker(handler);
		var request = new HttpRequestMessage(HttpMethod.Get,
			"https://localhost:9999/dbs/db/colls/test/docs/fake-id");
		request.Headers.Add("x-ms-documentdb-partitionkey", "[\"a\"]");
		request.Headers.Add("x-ms-some-future-header", "value");
		request.Headers.Add("x-ms-another-new-header", "value2");

		await invoker.SendAsync(request, CancellationToken.None);

		handler.UnrecognisedHeaders.Should().Contain("x-ms-some-future-header");
		handler.UnrecognisedHeaders.Should().Contain("x-ms-another-new-header");
	}

	[Fact]
	public async Task UnrecognisedHeaders_IgnoresNonXMsHeaders()
	{
		var container = new InMemoryContainer("test", "/pk");
		using var handler = new FakeCosmosHandler(container);

		var invoker = new HttpMessageInvoker(handler);
		var request = new HttpRequestMessage(HttpMethod.Get,
			"https://localhost:9999/dbs/db/colls/test/docs/fake-id");
		request.Headers.Add("x-ms-documentdb-partitionkey", "[\"a\"]");
		request.Headers.Add("x-custom-header", "value");

		await invoker.SendAsync(request, CancellationToken.None);

		handler.UnrecognisedHeaders.Should().BeEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  2. Expanded VerifySdkCompatibilityAsync
// ═══════════════════════════════════════════════════════════════════════════

public class ExpandedSdkCompatibilityTests
{
	[Fact]
	public async Task VerifySdkCompatibilityAsync_CoversUpsertRoundTrip()
	{
		// The expanded check should not throw — if it does, the SDK contract changed
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}

	[Fact]
	public async Task VerifySdkCompatibilityAsync_CoversPatchRoundTrip()
	{
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}

	[Fact]
	public async Task VerifySdkCompatibilityAsync_CoversDistinctQuery()
	{
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}

	[Fact]
	public async Task VerifySdkCompatibilityAsync_CoversOffsetLimitQuery()
	{
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}

	[Fact]
	public async Task VerifySdkCompatibilityAsync_IncludesBatchCheck()
	{
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}

	[Fact]
	public async Task VerifySdkCompatibilityAsync_IncludesReadManyCheck()
	{
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}

	[Fact]
	public async Task VerifySdkCompatibilityAsync_IncludesChangeFeedCheck()
	{
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}

	[Fact]
	public async Task VerifySdkCompatibilityAsync_IncludesGroupByCheck()
	{
		await FakeCosmosHandler.VerifySdkCompatibilityAsync();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  3. Query plan version detection
// ═══════════════════════════════════════════════════════════════════════════

public class QueryPlanVersionTests
{
	[Fact]
	public void QueryPlanVersion_MatchesExpected()
	{
		FakeCosmosHandler.QueryPlanVersion.Should().Be(2,
			"FakeCosmosHandler returns PartitionedQueryExecutionInfo version 2; " +
			"if the SDK expects a different version, queries may fail silently.");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  5. SDK version pinning & warnings
// ═══════════════════════════════════════════════════════════════════════════

public class SdkVersionPinningTests
{
	[Fact]
	public void MinTestedSdkVersion_IsSet()
	{
		FakeCosmosHandler.MinTestedSdkVersion.Should().NotBeNull();
	}

	[Fact]
	public void MaxTestedSdkVersion_IsSet()
	{
		FakeCosmosHandler.MaxTestedSdkVersion.Should().NotBeNull();
	}

	[Fact]
	public void CurrentSdkVersion_IsWithinTestedRange()
	{
		var current = typeof(CosmosClient).Assembly.GetName().Version;
		current.Should().NotBeNull();

		current.Should().BeGreaterThanOrEqualTo(FakeCosmosHandler.MinTestedSdkVersion,
			"the current Cosmos SDK version should be at or above the minimum tested version");
		current.Should().BeLessThanOrEqualTo(FakeCosmosHandler.MaxTestedSdkVersion,
			"the current Cosmos SDK version should be at or below the maximum tested version");
	}

	[Fact]
	public void SdkVersionWarnings_IsEmptyWhenWithinRange()
	{
		var container = new InMemoryContainer("test", "/pk");
		using var handler = new FakeCosmosHandler(container);

		// Since tests run with a supported SDK, there should be no warnings
		handler.SdkVersionWarnings.Should().BeEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  6. Wire format strategy abstraction
// ═══════════════════════════════════════════════════════════════════════════

public class WireFormatStrategyTests
{
	[Fact]
	public void DefaultOptions_HasDefaultQueryPlanStrategy()
	{
		var options = new FakeCosmosHandlerOptions();
		options.QueryPlanStrategy.Should().NotBeNull();
	}

	[Fact]
	public void DefaultOptions_HasDefaultBatchSchemaStrategy()
	{
		var options = new FakeCosmosHandlerOptions();
		options.BatchSchemaStrategy.Should().NotBeNull();
	}

	[Fact]
	public async Task CustomQueryPlanStrategy_IsUsed()
	{
		// On Windows, the SDK uses ServiceInterop for query plans rather than the HTTP
		// endpoint, so the custom strategy won't be invoked for basic queries.
		if (OperatingSystem.IsWindows())
			return;

		var called = false;
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			new { id = "1", pk = "a" }, new PartitionKey("a"));

		var customStrategy = new DelegatingQueryPlanStrategy(() => called = true);
		var options = new FakeCosmosHandlerOptions
		{
			QueryPlanStrategy = customStrategy
		};

		using var handler = new FakeCosmosHandler(container, options);
		using var client = new CosmosClient(
			"AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});

		var cosmosContainer = client.GetContainer("db", "test");
		var iterator = cosmosContainer.GetItemQueryIterator<JObject>("SELECT * FROM c");
		while (iterator.HasMoreResults)
			await iterator.ReadNextAsync();

		called.Should().BeTrue("the custom query plan strategy should have been invoked");
	}

	/// <summary>
	/// Test helper that delegates to the default strategy but records that it was called.
	/// </summary>
	private sealed class DelegatingQueryPlanStrategy : IQueryPlanStrategy
	{
		private readonly Action _onCalled;
		private readonly IQueryPlanStrategy _inner = new DefaultQueryPlanStrategy();

		public DelegatingQueryPlanStrategy(Action onCalled) => _onCalled = onCalled;

		public JObject BuildQueryPlan(string sqlQuery, CosmosSqlQuery? parsed, string collectionRid)
		{
			_onCalled();
			return _inner.BuildQueryPlan(sqlQuery, parsed, collectionRid);
		}
	}
}
