using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.Tests.Infrastructure;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Integration tests for ChangeFeedProcessor through the SDK HTTP pipeline.
/// Verifies that the processor receives changes when built from an SDK Container
/// (which routes through FakeCosmosHandler), not just from InMemoryContainer directly.
///
/// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/list-documents
///   The change feed uses GET /docs with A-IM: Incremental feed header.
///   The SDK's AutoCheckpointer requires a non-empty continuation token
///   (etag) in the response to call CheckpointAsync successfully.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class FakeCosmosHandlerChangeFeedProcessorTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("changefeed-processor", "/partitionKey");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	[Fact]
	public async Task ChangeFeedProcessor_TypedHandler_ReceivesChanges()
	{
		// Arrange — seed an item before the processor starts
		await _container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Seeded" },
			new PartitionKey("pk1"));

		var received = new List<TestDocument>();
		var handlerInvoked = new TaskCompletionSource<bool>();

		var processor = _container
			.GetChangeFeedProcessorBuilder<TestDocument>("test-processor",
				(context, changes, ct) =>
				{
					received.AddRange(changes);
					handlerInvoked.TrySetResult(true);
					return Task.CompletedTask;
				})
			.WithInMemoryLeaseContainer()
			.WithStartTime(DateTime.MinValue.ToUniversalTime())
			.Build();

		// Act
		await processor.StartAsync();

		// Wait for the handler to be invoked (or timeout)
		var completed = await Task.WhenAny(handlerInvoked.Task, Task.Delay(TimeSpan.FromSeconds(5)));
		await processor.StopAsync();

		// Assert
		completed.Should().Be(handlerInvoked.Task,
			"the change feed processor handler should have been invoked within 5 seconds");
		received.Should().ContainSingle(d => d.Id == "1" && d.Name == "Seeded");
	}

	[Fact]
	public async Task ChangeFeedProcessor_TypedHandler_ReceivesItemsCreatedAfterStart()
	{
		var received = new List<TestDocument>();
		var handlerInvoked = new TaskCompletionSource<bool>();

		var processor = _container
			.GetChangeFeedProcessorBuilder<TestDocument>("test-processor",
				(context, changes, ct) =>
				{
					received.AddRange(changes);
					handlerInvoked.TrySetResult(true);
					return Task.CompletedTask;
				})
			.WithInMemoryLeaseContainer()
			.WithStartTime(DateTime.MinValue.ToUniversalTime())
			.Build();

		await processor.StartAsync();

		// Create item after processor has started
		await _container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Dynamic" },
			new PartitionKey("pk1"));

		var completed = await Task.WhenAny(handlerInvoked.Task, Task.Delay(TimeSpan.FromSeconds(10)));
		await processor.StopAsync();

		completed.Should().Be(handlerInvoked.Task,
			"the change feed processor handler should have been invoked within 10 seconds");
		received.Should().ContainSingle(d => d.Id == "2" && d.Name == "Dynamic");
	}

	[Fact]
	public async Task ChangeFeedProcessor_DoesNotThrowEmptyContinuationToken()
	{
		// This test specifically validates the bug report: the SDK's AutoCheckpointer
		// calls CheckpointAsync which requires a non-empty continuation token.
		// If FakeCosmosHandler returns an empty/null continuation token for change feed
		// responses, the processor throws ArgumentException internally.

		await _container.CreateItemAsync(
			new TestDocument { Id = "3", PartitionKey = "pk1", Name = "TokenTest" },
			new PartitionKey("pk1"));

		Exception? capturedError = null;
		var errorReceived = new TaskCompletionSource<bool>();
		var handlerInvoked = new TaskCompletionSource<bool>();

		var processor = _container
			.GetChangeFeedProcessorBuilder<TestDocument>("test-processor",
				(context, changes, ct) =>
				{
					handlerInvoked.TrySetResult(true);
					return Task.CompletedTask;
				})
			.WithInMemoryLeaseContainer()
			.WithStartTime(DateTime.MinValue.ToUniversalTime())
			.WithErrorNotification((leaseToken, ex) =>
			{
				capturedError = ex;
				errorReceived.TrySetResult(true);
				return Task.CompletedTask;
			})
			.Build();

		await processor.StartAsync();

		// Wait for either the handler to succeed or an error to surface
		var completed = await Task.WhenAny(
			handlerInvoked.Task,
			errorReceived.Task,
			Task.Delay(TimeSpan.FromSeconds(5)));

		await processor.StopAsync();

		// The handler should have been invoked, not the error notification
		capturedError.Should().BeNull(
			"the change feed processor should not throw an empty continuation token error, " +
			$"but got: {capturedError}");
		handlerInvoked.Task.IsCompletedSuccessfully.Should().BeTrue(
			"the change feed processor handler should have been invoked");
	}
}
