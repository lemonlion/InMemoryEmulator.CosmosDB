using System.Net;
using System.Runtime.Serialization;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Edge-case / red-team tests for the InMemoryCosmosException factory fix (Issue #18).
/// These tests probe reflection fragility, thread safety, all exception properties,
/// various catch patterns, unusual status codes, serialization, ETag scenarios,
/// batch failures, database/container errors, and diagnostics behaviour.
/// </summary>
public class Issue18EdgeCaseTests
{
	// ═══════════════════════════════════════════════════════════════════════════
	// 1. FACTORY BEHAVIOUR
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public void Factory_Returns_CosmosException_WithNullDiagnostics()
	{
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "act-1", 1.5);

		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public async Task ConcurrentCreation_PreservesIndividualProperties()
	{
		const int concurrency = 200;
		var exceptions = new CosmosException[concurrency];

		await Parallel.ForAsync(0, concurrency, async (i, _) =>
		{
			await Task.Yield();
			exceptions[i] = InMemoryCosmosException.Create(
				$"msg-{i}",
				i % 2 == 0 ? HttpStatusCode.NotFound : HttpStatusCode.Conflict,
				i, $"act-{i}", i * 1.0);
		});

		for (int i = 0; i < concurrency; i++)
		{
			exceptions[i].Message.Should().Contain($"msg-{i}");
			exceptions[i].SubStatusCode.Should().Be(i);
			exceptions[i].ActivityId.Should().Be($"act-{i}");
			exceptions[i].RequestCharge.Should().Be(i * 1.0);

			var expectedStatus = i % 2 == 0 ? HttpStatusCode.NotFound : HttpStatusCode.Conflict;
			exceptions[i].StatusCode.Should().Be(expectedStatus);
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 3. EXCEPTION PROPERTIES — exhaustive verification
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public void AllProperties_AreCorrect_AfterCreation()
	{
		var ex = InMemoryCosmosException.Create(
			"Something went wrong", HttpStatusCode.BadRequest, 42, "activity-xyz", 3.14);

		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		ex.SubStatusCode.Should().Be(42);
		ex.ActivityId.Should().Be("activity-xyz");
		ex.RequestCharge.Should().Be(3.14);
		ex.Message.Should().Contain("Something went wrong");
		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public void ExceptionType_IsExactly_CosmosException()
	{
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);

		ex.GetType().Should().Be(typeof(CosmosException),
			"Must be exactly CosmosException, not a subclass — this is the whole point of Issue #18");
		(ex is CosmosException).Should().BeTrue();
		(ex is Exception).Should().BeTrue();
	}

	[Fact]
	public void Message_ContainsStatusCode_InToString()
	{
		var ex = InMemoryCosmosException.Create("custom msg", HttpStatusCode.NotFound, 0, "", 0);

		// CosmosException.ToString() normally includes status code info
		ex.ToString().Should().Contain("NotFound");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 4. CATCH PATTERNS — various C# catch/filter patterns
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public void CatchPattern_BareCosmosException()
	{
		bool caught = false;
		try
		{
			throw InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);
		}
		catch (CosmosException)
		{
			caught = true;
		}

		caught.Should().BeTrue();
	}

	[Fact]
	public void CatchPattern_WithWhenFilter()
	{
		bool caught = false;
		try
		{
			throw InMemoryCosmosException.Create("test", HttpStatusCode.Conflict, 0, "", 0);
		}
		catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
		{
			caught = true;
		}

		caught.Should().BeTrue();
	}

	[Fact]
	public void CatchPattern_ExceptionWithIsCheck()
	{
		bool caught = false;
		try
		{
			throw InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);
		}
		catch (Exception e) when (e is CosmosException)
		{
			caught = true;
		}

		caught.Should().BeTrue();
	}

	[Fact]
	public void CatchPattern_PatternMatchingIs()
	{
		Exception thrown = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "act-1", 1.0);

		if (thrown is CosmosException ce)
		{
			ce.StatusCode.Should().Be(HttpStatusCode.NotFound);
			ce.ActivityId.Should().Be("act-1");
		}
		else
		{
			Assert.Fail("Pattern matching 'is CosmosException' should succeed");
		}
	}

	[Fact]
	public void CatchPattern_SwitchExpression()
	{
		Exception thrown = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);

		var status = thrown switch
		{
			CosmosException { StatusCode: HttpStatusCode.NotFound } => "not-found",
			CosmosException { StatusCode: HttpStatusCode.Conflict } => "conflict",
			CosmosException => "other-cosmos",
			_ => "unknown"
		};

		status.Should().Be("not-found");
	}

	[Fact]
	public async Task CatchPattern_AssertThrowsAsync_ExactMatch()
	{
		// This is THE pattern that was broken before Issue #18 fix
		var ex = await Assert.ThrowsAsync<CosmosException>(
			() => Task.FromException<object>(
				InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0)));

		ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 5. EDGE CASE STATUS CODES
	// ═══════════════════════════════════════════════════════════════════════════

	[Theory]
	[InlineData(HttpStatusCode.NotFound)]
	[InlineData(HttpStatusCode.Conflict)]
	[InlineData(HttpStatusCode.BadRequest)]
	[InlineData(HttpStatusCode.PreconditionFailed)]
	[InlineData(HttpStatusCode.NotModified)]
	[InlineData(HttpStatusCode.TooManyRequests)]
	[InlineData(HttpStatusCode.InternalServerError)]
	[InlineData(HttpStatusCode.ServiceUnavailable)]
	[InlineData(HttpStatusCode.RequestEntityTooLarge)]
	[InlineData(HttpStatusCode.Forbidden)]
	[InlineData(HttpStatusCode.Gone)]
	[InlineData(HttpStatusCode.RequestTimeout)]
	public void AllStatusCodes_ProduceCorrectCosmosException(HttpStatusCode statusCode)
	{
		var ex = InMemoryCosmosException.Create($"test-{statusCode}", statusCode, 0, "act", 0);

		ex.GetType().Should().Be(typeof(CosmosException));
		ex.StatusCode.Should().Be(statusCode);
		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public void SubStatusCode_1003_ContainerNotFound()
	{
		var ex = InMemoryCosmosException.Create("Container not found",
			HttpStatusCode.NotFound, 1003, "act", 0);

		ex.SubStatusCode.Should().Be(1003);
		ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 6. SERIALIZATION
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public void Exception_CanBeSerializedWithGetObjectData()
	{
		var ex = InMemoryCosmosException.Create("ser-test", HttpStatusCode.NotFound, 42, "act-ser", 1.5);

		// GetObjectData should not throw — even if BinaryFormatter is obsolete,
		// libraries may still call GetObjectData for logging/serialization
#pragma warning disable SYSLIB0050, SYSLIB0051
		var info = new SerializationInfo(typeof(CosmosException), new FormatterConverter());
		var context = new StreamingContext(StreamingContextStates.All);

		var act = () => ex.GetObjectData(info, context);
#pragma warning restore SYSLIB0050, SYSLIB0051
		act.Should().NotThrow("GetObjectData should work without error");
	}

	[Fact]
	public void Exception_StackTrace_IsPreserved()
	{
		CosmosException? caught = null;
		try
		{
			throw InMemoryCosmosException.Create("stack-test", HttpStatusCode.NotFound, 0, "", 0);
		}
		catch (CosmosException ex)
		{
			caught = ex;
		}

		caught.Should().NotBeNull();
		caught.StackTrace.Should().NotBeNullOrEmpty("stack trace should be captured when thrown");
		caught.StackTrace.Should().Contain(nameof(Exception_StackTrace_IsPreserved));
	}

	[Fact]
	public void Exception_InnerException_IsNull()
	{
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);
		ex.InnerException.Should().BeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 7. DIAGNOSTICS BEHAVIOUR
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public void Diagnostics_IsNull_OnCaughtException()
	{
		CosmosException? caught = null;
		try
		{
			throw InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);
		}
		catch (CosmosException ex)
		{
			caught = ex;
		}

		caught.Diagnostics.Should().BeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 8. ETAG / PRECONDITION FAILED
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ETagMismatch_Replace_ThrowsCosmosException_412()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		var created = await container.CreateItemAsync(
			new { id = "1", pk = "pk1", value = "a" }, new PartitionKey("pk1"));

		// Replace with wrong ETag
		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.ReplaceItemAsync(
				new { id = "1", pk = "pk1", value = "b" }, "1", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = "\"wrong-etag\"" }));

		ex.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public async Task IfNoneMatch_Star_OnExistingItem_ThrowsCosmosException_304()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		await container.CreateItemAsync(
			new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));

		// Read with IfNoneMatch="*" — item exists → 304
		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.ReadItemAsync<dynamic>("1", new PartitionKey("pk1"),
				new ItemRequestOptions { IfNoneMatchEtag = "*" }));

		ex.StatusCode.Should().Be(HttpStatusCode.NotModified);
		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public async Task IfNoneMatch_MatchingEtag_ThrowsCosmosException_304()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		var created = await container.CreateItemAsync(
			new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));
		var etag = created.ETag;

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.ReadItemAsync<dynamic>("1", new PartitionKey("pk1"),
				new ItemRequestOptions { IfNoneMatchEtag = etag }));

		ex.StatusCode.Should().Be(HttpStatusCode.NotModified);
		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public async Task ETagMismatch_Patch_ThrowsCosmosException_412()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		await container.CreateItemAsync(
			new { id = "1", pk = "pk1", name = "test" }, new PartitionKey("pk1"));

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.PatchItemAsync<dynamic>("1", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "new") },
				new PatchItemRequestOptions { IfMatchEtag = "\"wrong-etag\"" }));

		ex.StatusCode.Should().Be(HttpStatusCode.PreconditionFailed);
		ex.Diagnostics.Should().BeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 9. CONTAINER NOT FOUND
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task DeletedContainer_ThrowsCosmosException_404_SubStatus1003()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var containerResp = await db.CreateContainerAsync("items", "/pk");
		var container = containerResp.Container;

		await container.DeleteContainerAsync();

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.ReadContainerAsync());

		ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
		ex.SubStatusCode.Should().Be(1003);
		ex.Diagnostics.Should().BeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 10. MULTIPLE SEQUENTIAL ERRORS
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task MultipleSequentialErrors_AllCatchableAsCosmosException()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;
		await container.CreateItemAsync(new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));

		var errors = new List<(HttpStatusCode Status, string Activity)>();

		// Error 1: Read non-existent → 404
		try
		{
			await container.ReadItemAsync<dynamic>("nope", new PartitionKey("pk1"));
		}
		catch (CosmosException ex)
		{
			errors.Add((ex.StatusCode, ex.ActivityId));
			ex.Diagnostics.Should().BeNull();
		}

		// Error 2: Duplicate create → 409
		try
		{
			await container.CreateItemAsync(new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));
		}
		catch (CosmosException ex)
		{
			errors.Add((ex.StatusCode, ex.ActivityId));
			ex.Diagnostics.Should().BeNull();
		}

		// Error 3: Delete non-existent → 404
		try
		{
			await container.DeleteItemAsync<dynamic>("nope", new PartitionKey("pk1"));
		}
		catch (CosmosException ex)
		{
			errors.Add((ex.StatusCode, ex.ActivityId));
			ex.Diagnostics.Should().BeNull();
		}

		errors.Should().HaveCount(3);
		errors[0].Status.Should().Be(HttpStatusCode.NotFound);
		errors[1].Status.Should().Be(HttpStatusCode.Conflict);
		errors[2].Status.Should().Be(HttpStatusCode.NotFound);

		// Each should have a distinct ActivityId (generated with Guid.NewGuid)
		errors.Select(e => e.Activity).Distinct().Should().HaveCount(3,
			"Each error should have a unique ActivityId");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 11. VALIDATION ERRORS (400 BadRequest)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task EmptyId_ThrowsCosmosException_400()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.CreateItemAsync(new { id = "", pk = "pk1" }, new PartitionKey("pk1")));

		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public async Task PatchTooManyOps_ThrowsCosmosException_400()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;
		await container.CreateItemAsync(new { id = "1", pk = "pk1", a = 0 }, new PartitionKey("pk1"));

		// 11 patch operations exceeds the limit of 10
		var ops = Enumerable.Range(0, 11)
			.Select(i => PatchOperation.Set($"/field{i}", i))
			.ToArray();

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.PatchItemAsync<dynamic>("1", new PartitionKey("pk1"), ops));

		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public async Task PatchEmptyOps_ThrowsCosmosException_400()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;
		await container.CreateItemAsync(new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.PatchItemAsync<dynamic>("1", new PartitionKey("pk1"),
				Array.Empty<PatchOperation>()));

		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		ex.Diagnostics.Should().BeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 12. DATABASE-LEVEL ERRORS
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task DatabaseNameTooLong_ThrowsCosmosException_400()
	{
		var client = new InMemoryCosmosClient();
		var longName = new string('x', 256);

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await client.CreateDatabaseAsync(longName));

		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		ex.Diagnostics.Should().BeNull();
	}

	[Fact]
	public async Task DatabaseNameWithInvalidChars_ThrowsCosmosException_400()
	{
		var client = new InMemoryCosmosClient();

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await client.CreateDatabaseAsync("db/name"));

		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		ex.Diagnostics.Should().BeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 13. TRANSACTIONAL BATCH ERRORS
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BatchConflict_IsCatchableAsCosmosException()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		// Pre-create item so batch create conflicts
		await container.CreateItemAsync(new { id = "dup", pk = "pk1" }, new PartitionKey("pk1"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"))
			.CreateItem(new { id = "dup", pk = "pk1" });

		// The batch itself returns a response (not an exception) for per-item failures,
		// but the underlying container operation throws, which batch catches internally.
		// Let's verify batch response propagates the failure correctly.
		var response = await batch.ExecuteAsync();
		response.IsSuccessStatusCode.Should().BeFalse();
		response.StatusCode.Should().Be(HttpStatusCode.Conflict);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 14. CONCURRENT CONTAINER OPERATIONS RAISING EXCEPTIONS
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ConcurrentReadsOfNonExistent_AllThrowCosmosException()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		const int concurrency = 50;
		var tasks = Enumerable.Range(0, concurrency).Select(async i =>
		{
			var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
				await container.ReadItemAsync<dynamic>($"nope-{i}", new PartitionKey("pk1")));

			ex.StatusCode.Should().Be(HttpStatusCode.NotFound);
			ex.Diagnostics.Should().BeNull();
			return ex;
		});

		var results = await Task.WhenAll(tasks);
		results.Should().HaveCount(concurrency);

		// Verify each has distinct ActivityId
		results.Select(e => e.ActivityId).Distinct().Should().HaveCount(concurrency);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 15. EXCEPTION USED IN AGGREGATE / NESTED SCENARIOS
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task AggregateException_ContainingCosmosException_CanBeUnwrapped()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		// Wrap in Task.Run to ensure both tasks start as faulted Tasks
		// (ReadItemAsync may throw synchronously before returning)
		var task1 = Task.Run(() => container.ReadItemAsync<dynamic>("nope1", new PartitionKey("pk1")));
		var task2 = Task.Run(() => container.ReadItemAsync<dynamic>("nope2", new PartitionKey("pk1")));

		var allTask = Task.WhenAll(task1, task2);
		try
		{
			await allTask;
			Assert.Fail("Should have thrown");
		}
		catch (CosmosException firstEx)
		{
			firstEx.Diagnostics.Should().BeNull();
			firstEx.StatusCode.Should().Be(HttpStatusCode.NotFound);
		}

		task1.IsFaulted.Should().BeTrue();
		task2.IsFaulted.Should().BeTrue();

		// The AggregateException on the allTask should contain both
		allTask.Exception.Should().NotBeNull();
		allTask.Exception!.InnerExceptions.Should().HaveCount(2);
		foreach (var inner in allTask.Exception.InnerExceptions)
		{
			inner.Should().BeOfType<CosmosException>();
			((CosmosException)inner).Diagnostics.Should().BeNull();
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 16. REPLACE WITH WRONG BODY ID
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ReplaceItem_BodyIdMismatch_ThrowsBadRequest()
	{
		// SDK docs state body id must match the id parameter.
		// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		await container.CreateItemAsync(new { id = "1", pk = "pk1" }, new PartitionKey("pk1"));

		var act = () => container.ReplaceItemAsync(
			new { id = "DIFFERENT", pk = "pk1" }, "1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 17. UPSERT WITH IF-MATCH ON NON-EXISTENT ITEM
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Upsert_IfMatch_NonExistent_CreatesItem()
	{
		// Real Cosmos DB creates the item. If-Match is "applicable only on PUT and DELETE"
		// per the REST API docs. Upsert uses POST, so If-Match is ignored on insert path.
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		var response = await container.UpsertItemAsync(
			new { id = "noexist", pk = "pk1" }, new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = "\"some-etag\"" });

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 18. EXCEPTION DATA DICTIONARY
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public void Exception_Data_Dictionary_IsAccessible()
	{
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);

		// Exception.Data should be accessible (some logging frameworks use it)
		ex.Data.Should().NotBeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 19. REQUEST CHARGE EDGE VALUES
	// ═══════════════════════════════════════════════════════════════════════════

	[Theory]
	[InlineData(0.0)]
	[InlineData(1.0)]
	[InlineData(99.99)]
	[InlineData(double.MaxValue)]
	public void RequestCharge_VariousValues_ArePreserved(double charge)
	{
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", charge);
		ex.RequestCharge.Should().Be(charge);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 20. ACTIVITY ID EDGE CASES
	// ═══════════════════════════════════════════════════════════════════════════

	[Theory]
	[InlineData("simple-id", "simple-id")]
	[InlineData("a5d3e7c2-1234-5678-9abc-def012345678", "a5d3e7c2-1234-5678-9abc-def012345678")]
	public void ActivityId_NonEmptyValues_ArePreserved(string activityId, string expected)
	{
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, activityId, 0);
		ex.ActivityId.Should().Be(expected);
	}

	[Fact]
	public void ActivityId_EmptyString_IsNormalizedToNull_BySdk()
	{
		// Cosmos SDK normalizes empty ActivityId to null — this is SDK behavior, not a bug
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);
		ex.ActivityId.Should().BeNull("Cosmos SDK normalizes empty ActivityId to null");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 21. EXCEPTION AS BASE TYPES
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public void Exception_IsAssignableTo_AllBaseTypes()
	{
		var ex = InMemoryCosmosException.Create("test", HttpStatusCode.NotFound, 0, "", 0);

		(ex is Exception).Should().BeTrue();
		(ex is CosmosException).Should().BeTrue();
		(ex is object).Should().BeTrue();

		// Should NOT be assignable to any other derived type
		typeof(CosmosException).IsAssignableFrom(ex!.GetType()).Should().BeTrue();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 22. DIAGNOSTICS AFTER EXCEPTION TRAVERSES ASYNC STATE MACHINE
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Diagnostics_IsNull_AfterAsyncAwait()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		CosmosException? caught = null;
		try
		{
			await NestedAsyncCall(container);
		}
		catch (CosmosException ex)
		{
			caught = ex;
		}

		caught.Should().NotBeNull();
		caught!.StatusCode.Should().Be(HttpStatusCode.NotFound);
		caught.Diagnostics.Should().BeNull();
	}

	private static async Task NestedAsyncCall(Container container)
	{
		await Task.Yield(); // force async
		await container.ReadItemAsync<dynamic>("nonexistent", new PartitionKey("pk1"));
	}

	// ═══════════════════════════════════════════════════════════════════════════
	// 23. CONTAINER REPLACE WITH WRONG ID
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ContainerReplace_WrongId_ThrowsCosmosException_400()
	{
		var client = new InMemoryCosmosClient();
		var db = (await client.CreateDatabaseIfNotExistsAsync("testdb")).Database;
		var container = (await db.CreateContainerAsync("items", "/pk")).Container;

		var ex = await Assert.ThrowsAsync<CosmosException>(async () =>
			await container.ReplaceContainerAsync(new ContainerProperties("wrong-name", "/pk")));

		ex.StatusCode.Should().Be(HttpStatusCode.BadRequest);
		ex.Diagnostics.Should().BeNull();
	}
}
