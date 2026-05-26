using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for bulk operations — the pattern where <c>CosmosClientOptions.AllowBulkExecution = true</c>
/// is set and many concurrent point operations are fired via <c>Task.WhenAll</c>.
/// The SDK internally batches these into efficient service calls. The in-memory emulator
/// must handle this concurrency correctly.
/// </summary>
public class BulkOperationTests
{
	[Fact]
	public async Task BulkCreate_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 500).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(500);

		// Verify all items are readable
		for (var i = 0; i < 500; i++)
		{
			var response = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
			response.Resource.Name.Should().Be($"Item{i}");
		}
	}

	[Fact]
	public async Task BulkUpsert_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// First round: all new items
		var createTasks = Enumerable.Range(0, 500).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var createResults = await Task.WhenAll(createTasks);
		createResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(500);

		// Second round: all updates
		var updateTasks = Enumerable.Range(0, 500).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
				new PartitionKey("pk1")));

		var updateResults = await Task.WhenAll(updateTasks);
		updateResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
		container.ItemCount.Should().Be(500);
	}

	[Fact]
	public async Task BulkDelete_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create items
		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		container.ItemCount.Should().Be(200);

		// Bulk delete
		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NoContent);
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task BulkReplace_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create items
		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		// Bulk replace
		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
				$"{i}",
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		// Verify all items were replaced
		for (var i = 0; i < 200; i++)
		{
			var response = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
			response.Resource.Name.Should().Be($"Replaced{i}");
		}
	}

	[Fact]
	public async Task BulkPatch_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create items
		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = 0 },
				new PartitionKey("pk1"));

		// Bulk patch
		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.PatchItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		// Verify all items were patched
		for (var i = 0; i < 200; i++)
		{
			var response = await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
			response.Resource.Name.Should().Be($"Patched{i}");
		}
	}

	[Fact]
	public async Task BulkRead_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create items
		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		// Bulk read
		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		for (var i = 0; i < 200; i++)
			results[i].Resource.Name.Should().Be($"Item{i}");
	}

	[Fact]
	public async Task BulkCreate_MixedPartitionKeys_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 500).Select(i =>
		{
			var pk = $"pk{i % 10}";
			return container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = pk, Name = $"Item{i}" },
				new PartitionKey(pk));
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(500);

		// Verify distribution: each PK has 50 items
		for (var pk = 0; pk < 10; pk++)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = @pk")
					.WithParameter("@pk", $"pk{pk}"),
				requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey($"pk{pk}") });

			var items = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				items.AddRange(page);
			}

			items.Should().HaveCount(50);
		}
	}

	[Fact]
	public async Task BulkCreate_DuplicateIds_SomeConflict()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create 100 items where IDs 0-49 are unique, and IDs 50-99 duplicate IDs 0-49
		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var id = $"{i % 50}"; // IDs 0-49 used twice
			return Task.Run(async () =>
			{
				try
				{
					var response = await container.CreateItemAsync(
						new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Item{i}" },
						new PartitionKey("pk1"));
					return (StatusCode: response.StatusCode, IsSuccess: true);
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
				{
					return (StatusCode: HttpStatusCode.Conflict, IsSuccess: false);
				}
			});
		});

		var results = await Task.WhenAll(tasks);

		var successes = results.Count(r => r.IsSuccess);
		var conflicts = results.Count(r => !r.IsSuccess);

		successes.Should().Be(50);
		conflicts.Should().Be(50);
		container.ItemCount.Should().Be(50);
	}

	[Fact]
	public async Task BulkMixedOperations_CreateUpsertDeleteReplace_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create items for replace and delete
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"existing-{i}", PartitionKey = "pk1", Name = $"Existing{i}" },
				new PartitionKey("pk1"));

		// Mixed concurrent operations
		var createTasks = Enumerable.Range(0, 50).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"new-{i}", PartitionKey = "pk1", Name = $"New{i}" },
				new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

		var upsertTasks = Enumerable.Range(0, 50).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"upserted-{i}", PartitionKey = "pk1", Name = $"Upserted{i}" },
				new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

		var deleteTasks = Enumerable.Range(0, 25).Select(i =>
			container.DeleteItemAsync<TestDocument>($"existing-{i}", new PartitionKey("pk1"))
			.ContinueWith(t => t.Result.StatusCode));

		var replaceTasks = Enumerable.Range(25, 25).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = $"existing-{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
				$"existing-{i}",
				new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

		var allTasks = createTasks
			.Concat(upsertTasks)
			.Concat(deleteTasks)
			.Concat(replaceTasks);

		var results = await Task.WhenAll(allTasks);

		// Verify individual operation status codes
		var createResults = results.Take(50).ToList();
		createResults.Should().OnlyContain(s => s == HttpStatusCode.Created);

		var upsertResults = results.Skip(50).Take(50).ToList();
		upsertResults.Should().OnlyContain(s => s == HttpStatusCode.Created);

		var deleteResults = results.Skip(100).Take(25).ToList();
		deleteResults.Should().OnlyContain(s => s == HttpStatusCode.NoContent);

		var replaceResults = results.Skip(125).Take(25).ToList();
		replaceResults.Should().OnlyContain(s => s == HttpStatusCode.OK);

		// Final count: 50 pre-created - 25 deleted + 50 new + 50 upserted = 125
		container.ItemCount.Should().Be(125);
	}

	[Fact]
	public void InMemoryCosmosClient_ClientOptions_AllowBulkExecution_CanBeSet()
	{
		var client = new InMemoryCosmosClient();

		// AllowBulkExecution is a property on CosmosClientOptions — should be settable
		client.ClientOptions.AllowBulkExecution = true;
		client.ClientOptions.AllowBulkExecution.Should().BeTrue();

		client.ClientOptions.AllowBulkExecution = false;
		client.ClientOptions.AllowBulkExecution.Should().BeFalse();
	}

	[Fact]
	public async Task BulkCreate_ViaCosmosClient_WithAllowBulkExecution_AllSucceed()
	{
		var client = new InMemoryCosmosClient();
		client.ClientOptions.AllowBulkExecution = true;

		var database = client.GetDatabase("test-db");
		await database.CreateContainerIfNotExistsAsync("test-container", "/partitionKey");
		var container = database.GetContainer("test-container");

		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
	}

	[Fact]
	public async Task BulkOperations_ChangeFeed_RecordsAllChanges()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var checkpoint = container.GetChangeFeedCheckpoint();

		// Bulk create
		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		await Task.WhenAll(tasks);

		// Read change feed
		var iterator = container.GetChangeFeedIterator<TestDocument>(checkpoint);
		var changes = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			changes.AddRange(page);
		}

		changes.Should().HaveCount(200);
	}

	[Fact]
	public async Task BulkOperations_UniqueKeyViolation_ReturnsConflict()
	{
		var properties = new ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		// Create items with unique names concurrently, but some share the same name
		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var name = $"Name{i % 50}"; // 50 unique names, each used twice
			return Task.Run(async () =>
			{
				try
				{
					await container.CreateItemAsync(
						new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = name },
						new PartitionKey("pk1"));
					return true;
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
				{
					return false;
				}
			});
		});

		var results = await Task.WhenAll(tasks);
		results.Count(r => r).Should().Be(50);
		results.Count(r => !r).Should().Be(50);
	}

	[Fact]
	public async Task BulkOperations_ETags_UpdatedPerOperation()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Bulk create
		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		// Each item should have a unique ETag
		var etags = results.Select(r => r.ETag).ToList();
		etags.Should().OnlyHaveUniqueItems();
		etags.Should().NotContain(string.Empty);
		etags.Should().NotContainNulls();
	}

	[Fact]
	public async Task BulkCreate_StreamVariant_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 200).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
			var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			return container.CreateItemStreamAsync(stream, new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(200);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 1: Stream Variant Coverage
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkUpsert_StreamVariant_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 200).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
			return container.UpsertItemStreamAsync(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

		// Re-upsert all (updates)
		var updateTasks = Enumerable.Range(0, 200).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" });
			return container.UpsertItemStreamAsync(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
		});

		var updateResults = await Task.WhenAll(updateTasks);
		updateResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task BulkReplace_StreamVariant_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 200).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" });
			return container.ReplaceItemStreamAsync(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)), $"{i}", new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task BulkDelete_StreamVariant_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.DeleteItemStreamAsync($"{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NoContent);
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task BulkPatch_StreamVariant_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", Value = 0 },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.PatchItemStreamAsync($"{i}", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task BulkRead_StreamVariant_ManyItems_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.ReadItemStreamAsync($"{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 2: Error Handling / Negative Paths
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkReplace_NonExistentItems_AllReturn404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.ReplaceItemAsync(
						new TestDocument { Id = $"missing-{i}", PartitionKey = "pk1", Name = "X" },
						$"missing-{i}", new PartitionKey("pk1")));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task BulkDelete_NonExistentItems_AllReturn404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.DeleteItemAsync<TestDocument>($"missing-{i}", new PartitionKey("pk1")));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task BulkPatch_NonExistentItems_AllReturn404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.PatchItemAsync<TestDocument>($"missing-{i}", new PartitionKey("pk1"),
						new[] { PatchOperation.Set("/name", "X") }));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task BulkRead_NonExistentItems_AllReturn404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.ReadItemAsync<TestDocument>($"missing-{i}", new PartitionKey("pk1")));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.NotFound);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 3: Request Options & Conditional Operations
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkReplace_WithCorrectIfMatchEtag_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var createResults = new List<ItemResponse<TestDocument>>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			createResults.Add(r);
		}

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
				$"{i}", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = createResults[i].ETag }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task BulkReplace_WithStaleIfMatchEtag_AllFail412()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var etags = new List<string>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			etags.Add(r.ETag);
		}

		// Mutate all items so ETags change
		for (var i = 0; i < 100; i++)
			await container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Mutated{i}" },
				$"{i}", new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.ReplaceItemAsync(
						new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Stale{i}" },
						$"{i}", new PartitionKey("pk1"),
						new ItemRequestOptions { IfMatchEtag = etags[i] }));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task BulkCreate_WithEnableContentResponseOnWriteFalse_ResponseResourceIsNull()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"),
				new ItemRequestOptions { EnableContentResponseOnWrite = false }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		results.Should().OnlyContain(r => r.Resource == null);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 4: Concurrency Edge Cases
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_SameIdDifferentPartitionKeys_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = "shared-id", PartitionKey = $"pk{i}", Name = $"Item{i}" },
				new PartitionKey($"pk{i}")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(50);
	}

	[Fact]
	public async Task BulkUpsert_ConcurrentOnSameItem_LastWriterWins()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = "same-id", PartitionKey = "pk1", Name = $"Version{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		// All should succeed (mix of 201 Created and 200 OK)
		results.Should().OnlyContain(r =>
			r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);

		// Only one item should exist
		container.ItemCount.Should().Be(1);

		// Final document should be consistent
		var final = await container.ReadItemAsync<TestDocument>("same-id", new PartitionKey("pk1"));
		final.Resource.Name.Should().StartWith("Version");
	}

	[Fact]
	public async Task BulkCreate_EmptyBatch_Succeeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Empty<Task<ItemResponse<TestDocument>>>();
		var results = await Task.WhenAll(tasks);

		results.Should().BeEmpty();
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task BulkCreate_SingleItem_Succeeds()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = new[]
		{
			container.CreateItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Solo" },
				new PartitionKey("pk1"))
		};

		var results = await Task.WhenAll(tasks);
		results.Should().ContainSingle().Which.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 5: Data Integrity & Metadata
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_SystemMetadata_TsAndEtagSetOnAllItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		await Task.WhenAll(tasks);

		for (var i = 0; i < 100; i++)
		{
			var response = await container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>($"{i}", new PartitionKey("pk1"));
			var doc = response.Resource;
			doc["_etag"]!.ToString().Should().NotBeNullOrEmpty();
			((long)doc["_ts"]!).Should().BeGreaterThan(0);
		}
	}

	[Fact]
	public async Task BulkUpsert_ETags_ChangeOnUpdate()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var createTasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var originalEtags = (await Task.WhenAll(createTasks)).Select(r => r.ETag).ToList();

		var upsertTasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
				new PartitionKey("pk1")));

		var newEtags = (await Task.WhenAll(upsertTasks)).Select(r => r.ETag).ToList();

		for (var i = 0; i < 100; i++)
			newEtags[i].Should().NotBe(originalEtags[i]);
	}

	[Fact]
	public async Task BulkCreate_ResponseMetadata_RequestChargeAndHeaders()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		foreach (var r in results)
		{
			r.RequestCharge.Should().BeGreaterThan(0);
			r.Headers.Should().NotBeNull();
		}
	}

	[Fact]
	public async Task BulkCreate_SpecialCharactersInIds_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var ids = new[] { "hello world", "café", "item-1.0", "a/b", "emoji\u2764", "dot.dash-under_score" };
		var tasks = ids.Select(id =>
			container.CreateItemAsync(
				new TestDocument { Id = id, PartitionKey = "pk1", Name = id },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

		foreach (var id in ids)
		{
			var response = await container.ReadItemAsync<TestDocument>(id, new PartitionKey("pk1"));
			response.Resource.Id.Should().Be(id);
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 6: Change Feed Interactions
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkUpsert_ChangeFeed_RecordsAllUpdates()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Bulk create (100 new items)
		var createTasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));
		await Task.WhenAll(createTasks);

		// Bulk update same items
		var updateTasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
				new PartitionKey("pk1")));
		await Task.WhenAll(updateTasks);

		// Incremental change feed deduplicates to the latest version per item,
		// so 100 items each updated once = 100 entries (not 200)
		var iterator = container.GetChangeFeedIterator<TestDocument>(ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var changes = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(page);
		}

		// Incremental change feed returns latest version per item
		changes.Should().HaveCount(100);
		changes.Should().OnlyContain(c => c.Name.StartsWith("Updated"));
	}

	[Fact]
	public async Task BulkDelete_ChangeFeed_RecordsTombstones()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var checkpointAfterCreate = container.GetChangeFeedCheckpoint();

		// Bulk delete all
		var deleteTasks = Enumerable.Range(0, 100).Select(i =>
			container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));
		await Task.WhenAll(deleteTasks);

		var checkpointAfterDelete = container.GetChangeFeedCheckpoint();

		// Each delete creates a tombstone entry
		(checkpointAfterDelete - checkpointAfterCreate).Should().Be(100);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 7: Partition Key Variants
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_WithPartitionKeyNone_ExtractsFromDocument()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk{i % 5}", Name = $"Item{i}" }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(100);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Phase 8: DeleteAllItemsByPartitionKey Integration
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_ThenDeleteAllByPartitionKey_ContainerEmpty()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 500).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));
		await Task.WhenAll(tasks);
		container.ItemCount.Should().Be(500);

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));
		container.ItemCount.Should().Be(0);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 1: Stream Error Handling
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkReplace_StreamVariant_NonExistentItems_AllReturn404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"missing-{i}", PartitionKey = "pk1", Name = "X" });
			return container.ReplaceItemStreamAsync(
				new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
				$"missing-{i}", new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task BulkDelete_StreamVariant_NonExistentItems_AllReturn404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.DeleteItemStreamAsync($"missing-{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task BulkRead_StreamVariant_NonExistentItems_AllReturn404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.ReadItemStreamAsync($"missing-{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task BulkPatch_StreamVariant_NonExistentItems_AllReturn404()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.PatchItemStreamAsync($"missing-{i}", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "X") }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task BulkCreate_StreamVariant_DuplicateIds_SomeConflict()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var id = $"{i % 50}";
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = id, PartitionKey = "pk1", Name = $"Item{i}" });
			return container.CreateItemStreamAsync(
				new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
				new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);

		var successes = results.Count(r => r.StatusCode == HttpStatusCode.Created);
		var conflicts = results.Count(r => r.StatusCode == HttpStatusCode.Conflict);

		successes.Should().Be(50);
		conflicts.Should().Be(50);
		container.ItemCount.Should().Be(50);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 2: Document Size Limits
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_OversizedDocuments_Typed_AllReturn413()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create a string that makes the document > 2MB
		var bigValue = new string('x', 2 * 1024 * 1024);

		var tasks = Enumerable.Range(0, 10).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.CreateItemAsync(
						JObject.FromObject(new { id = $"{i}", partitionKey = "pk1", data = bigValue }),
						new PartitionKey("pk1")));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.RequestEntityTooLarge);
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task BulkCreate_OversizedDocuments_Stream_AllReturn413()
	{
		// Stream methods return a ResponseMessage with 413 status code
		// instead of throwing CosmosException.
		var container = new InMemoryContainer("test", "/partitionKey");

		var bigValue = new string('x', 2 * 1024 * 1024);

		var tasks = Enumerable.Range(0, 10).Select(i =>
			Task.Run(async () =>
			{
				var json = JsonConvert.SerializeObject(
					new { id = $"{i}", partitionKey = "pk1", data = bigValue });
				var response = await container.CreateItemStreamAsync(
					new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
					new PartitionKey("pk1"));
				return response.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.RequestEntityTooLarge);
	}

	[Fact]
	public async Task BulkUpsert_OversizedDocuments_AllReturn413()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var bigValue = new string('x', 2 * 1024 * 1024);

		var tasks = Enumerable.Range(0, 10).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.UpsertItemAsync(
						JObject.FromObject(new { id = $"{i}", partitionKey = "pk1", data = bigValue }),
						new PartitionKey("pk1")));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.RequestEntityTooLarge);
	}

	[Fact]
	public async Task BulkCreate_DocumentAtExactSizeLimit_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// The JSON overhead is: {"id":"0","partitionKey":"pk1","data":"..."} ~50 bytes
		// We want total to be just under 2MB
		var padding = 2 * 1024 * 1024 - 200; // leave room for JSON envelope
		var value = new string('a', padding);

		var tasks = Enumerable.Range(0, 5).Select(i =>
			container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", partitionKey = "pk1", data = value }),
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(5);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 3: ETag & Conditional Operations
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkUpsert_WithIfMatchEtag_Star_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
				new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = "*" }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task BulkCreate_WithIfNoneMatchEtag_Star_DuplicateIds_AllReturn409()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		// IfNoneMatch * on read means "only if NOT cached" — on create it's not a standard
		// operation, but the emulator's CheckIfNoneMatch will throw 304 NotModified when an item
		// with a matching etag exists. For creates, the 409 Conflict from duplicate ID will fire first.
		var tasks = Enumerable.Range(0, 50).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.CreateItemAsync(
						new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Dup{i}" },
						new PartitionKey("pk1")));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.Conflict);
	}

	[Fact]
	public async Task BulkDelete_WithIfMatchEtag_Correct_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var createResults = new List<ItemResponse<TestDocument>>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			createResults.Add(r);
		}

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = createResults[i].ETag }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NoContent);
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task BulkDelete_WithIfMatchEtag_Stale_AllFail412()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var oldEtags = new List<string>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			oldEtags.Add(r.ETag);
		}

		// Mutate all items so ETags change
		for (var i = 0; i < 100; i++)
			await container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Mutated{i}" },
				$"{i}", new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
						new ItemRequestOptions { IfMatchEtag = oldEtags[i] }));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.PreconditionFailed);
		container.ItemCount.Should().Be(100);
	}

	[Fact]
	public async Task BulkRead_WithIfNoneMatchEtag_Current_AllReturn304()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var createResults = new List<ItemResponse<TestDocument>>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			createResults.Add(r);
		}

		var tasks = Enumerable.Range(0, 100).Select(i =>
			Task.Run(async () =>
			{
				var ex = await Assert.ThrowsAnyAsync<CosmosException>(() =>
					container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
						new ItemRequestOptions { IfNoneMatchEtag = createResults[i].ETag }));
				return ex.StatusCode;
			}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(s => s == HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task BulkRead_WithIfNoneMatchEtag_Stale_AllReturnData()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var oldEtags = new List<string>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			oldEtags.Add(r.ETag);
		}

		// Mutate all items so ETags change
		for (var i = 0; i < 100; i++)
			await container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
				$"{i}", new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
				new ItemRequestOptions { IfNoneMatchEtag = oldEtags[i] }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		for (var i = 0; i < 100; i++)
			results[i].Resource.Name.Should().Be($"Updated{i}");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 4: EnableContentResponseOnWrite (All Operations)
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkUpsert_WithEnableContentResponseOnWriteFalse_ResponseResourceIsNull()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"),
				new ItemRequestOptions { EnableContentResponseOnWrite = false }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		results.Should().OnlyContain(r => r.Resource == null);
	}

	[Fact]
	public async Task BulkReplace_WithEnableContentResponseOnWriteFalse_ResponseResourceIsNull()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
				$"{i}", new PartitionKey("pk1"),
				new ItemRequestOptions { EnableContentResponseOnWrite = false }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
		results.Should().OnlyContain(r => r.Resource == null);
	}

	[Fact]
	public async Task BulkPatch_WithEnableContentResponseOnWriteFalse_ResponseResourceIsNull()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.PatchItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") },
				new PatchItemRequestOptions { EnableContentResponseOnWrite = false }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
		results.Should().OnlyContain(r => r.Resource == null);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 5: CancellationToken Handling
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_WithAlreadyCancelledToken_AllThrowOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var cts = new CancellationTokenSource();
		cts.Cancel();

		var tasks = Enumerable.Range(0, 50).Select(i =>
			Task.Run(async () =>
			{
				await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
					container.CreateItemAsync(
						new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
						new PartitionKey("pk1"),
						cancellationToken: cts.Token));
			}));

		await Task.WhenAll(tasks);
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task BulkRead_WithAlreadyCancelledToken_AllThrowOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var cts = new CancellationTokenSource();
		cts.Cancel();

		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			Task.Run(async () =>
			{
				await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
					container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
						cancellationToken: cts.Token));
			}));

		await Task.WhenAll(tasks);
	}

	[Fact(Skip = "InMemoryContainer operations are synchronous and complete instantly — " +
				 "ThrowIfCancellationRequested() fires at the start of each method so mid-flight " +
				 "cancellation cannot be observed. All tasks complete before the token fires.")]
	public async Task BulkCreate_CancelDuringExecution_SomeSucceedSomeCancelled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var cts = new CancellationTokenSource();

		var tasks = Enumerable.Range(0, 200).Select(i =>
			Task.Run(async () =>
			{
				try
				{
					await container.CreateItemAsync(
						new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
						new PartitionKey("pk1"),
						cancellationToken: cts.Token);
					return true;
				}
				catch (OperationCanceledException)
				{
					return false;
				}
			}));

		_ = Task.Delay(1).ContinueWith(_ => cts.Cancel());
		var results = await Task.WhenAll(tasks);

		var successes = results.Count(r => r);
		var cancellations = results.Count(r => !r);
		(successes + cancellations).Should().Be(200);
		container.ItemCount.Should().Be(successes);
	}

	/// <summary>
	/// Divergent behaviour: Mid-flight cancellation is not observable because InMemoryContainer
	/// operations are synchronous. All 200 items are created successfully.
	/// </summary>
	[Fact]
	public async Task BulkCreate_CancelDuringExecution_SomeSucceedSomeCancelled_DivergentBehaviour()
	{
		// Real Cosmos DB: CancellationToken can interrupt network I/O mid-flight, leading to
		// a mix of succeeded and cancelled operations.
		// InMemoryContainer: Operations are in-memory and complete synchronously. The
		// ThrowIfCancellationRequested() check at the top of each method only fires if the
		// token was ALREADY cancelled before the method is entered. Since tasks are dispatched
		// near-simultaneously and complete instantly, all 200 succeed.
		var container = new InMemoryContainer("test", "/partitionKey");
		var cts = new CancellationTokenSource();

		var tasks = Enumerable.Range(0, 200).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"),
				cancellationToken: cts.Token));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(200);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 6: Concurrency Race Conditions
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkReplace_ConcurrentOnSameItem_LastWriterWins()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "target", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = "target", PartitionKey = "pk1", Name = $"Version{i}" },
				"target", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
		container.ItemCount.Should().Be(1);

		var final = await container.ReadItemAsync<TestDocument>("target", new PartitionKey("pk1"));
		final.Resource.Name.Should().StartWith("Version");
	}

	[Fact]
	public async Task BulkDelete_ConcurrentOnSameItem_OneSucceedsRestFail()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "target", PartitionKey = "pk1", Name = "ToDelete" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(_ =>
			Task.Run(async () =>
			{
				try
				{
					await container.DeleteItemAsync<TestDocument>("target", new PartitionKey("pk1"));
					return true;
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
				{
					return false;
				}
			}));

		var results = await Task.WhenAll(tasks);
		results.Count(r => r).Should().BeGreaterThanOrEqualTo(1);
		results.Count(r => !r).Should().BeGreaterThanOrEqualTo(0);
		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task BulkPatch_ConcurrentOnSameItem_AtomicIncrement()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "counter", PartitionKey = "pk1", Name = "Counter", Value = 0 },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(_ =>
			container.PatchItemAsync<TestDocument>("counter", new PartitionKey("pk1"),
				new[] { PatchOperation.Increment("/value", 1) }));

		await Task.WhenAll(tasks);
		var final = await container.ReadItemAsync<TestDocument>("counter", new PartitionKey("pk1"));
		final.Resource.Value.Should().Be(50);
	}

	[Fact]
	public async Task BulkCreate_ThenImmediateDelete_SameItems_NoCrash()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// For each ID 0-49: one task creates, another task deletes
		var tasks = Enumerable.Range(0, 50).SelectMany(i => new[]
		{
			Task.Run(async () =>
			{
				try
				{
					await container.CreateItemAsync(
						new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
						new PartitionKey("pk1"));
				}
				catch (Exception) { /* Create can fail due to race with delete or conflict */ }
			}),
			Task.Run(async () =>
			{
				try
				{
					await container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
				}
				catch (Exception) { /* Delete can fail if item not yet created or already deleted */ }
			})
		});

		await Task.WhenAll(tasks);

		// Each item is either present or not — container state should be consistent
		container.ItemCount.Should().BeGreaterThanOrEqualTo(0);
		container.ItemCount.Should().BeLessThanOrEqualTo(50);
	}

	[Fact]
	public async Task BulkUpsert_MixOfNewAndExisting_StatusCodesCorrect()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create IDs 0-49
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		// Upsert IDs 0-99 (0-49 are updates, 50-99 are creates)
		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Upserted{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		var updates = results.Where(r => r.StatusCode == HttpStatusCode.OK).ToList();
		var creates = results.Where(r => r.StatusCode == HttpStatusCode.Created).ToList();

		updates.Count.Should().Be(50);
		creates.Count.Should().Be(50);
		container.ItemCount.Should().Be(100);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 7: Unique Key Edge Cases
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkUpsert_UniqueKeyViolation_SomeConflict()
	{
		var properties = new ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		// 100 items where 50 pairs share the same name but different IDs
		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var name = $"Name{i % 50}";
			return Task.Run(async () =>
			{
				try
				{
					await container.UpsertItemAsync(
						new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = name },
						new PartitionKey("pk1"));
					return true;
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
				{
					return false;
				}
			});
		});

		var results = await Task.WhenAll(tasks);
		results.Count(r => r).Should().Be(50);
		results.Count(r => !r).Should().Be(50);
	}

	[Fact]
	public async Task BulkUpsert_SameIdSameUniqueKey_AlwaysSucceeds()
	{
		var properties = new ContainerProperties("test", "/partitionKey")
		{
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		// Upsert same ID with same name 50 times — self-update should not violate unique key
		var tasks = Enumerable.Range(0, 50).Select(_ =>
			container.UpsertItemAsync(
				new TestDocument { Id = "same-id", PartitionKey = "pk1", Name = "SameName" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r =>
			r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);
		container.ItemCount.Should().Be(1);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 8: Null/Missing Partition Key Variants
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_UnicodeInPartitionKeys_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var unicodePKs = new[] { "🎉", "中文", "café", "عربي", "🚀🌍" };

		var tasks = unicodePKs.SelectMany((pk, pkIdx) =>
			Enumerable.Range(0, 10).Select(i =>
				container.CreateItemAsync(
					new TestDocument { Id = $"{pkIdx}-{i}", PartitionKey = pk, Name = $"Item{pkIdx}-{i}" },
					new PartitionKey(pk))));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(50);

		// Verify each PK group is queryable
		foreach (var pk in unicodePKs)
		{
			var iter = container.GetItemQueryIterator<TestDocument>(
				new QueryDefinition("SELECT * FROM c"),
				requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(pk) });

			var items = new List<TestDocument>();
			while (iter.HasMoreResults)
			{
				var page = await iter.ReadNextAsync();
				items.AddRange(page);
			}

			items.Should().HaveCount(10);
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 9: Hierarchical Partition Keys
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_HierarchicalPartitionKey_AllSucceed()
	{
		var properties = new ContainerProperties("test", new List<string> { "/tenantId", "/customerId" });
		var container = new InMemoryContainer(properties);

		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var tenantId = $"tenant{i % 5}";
			var customerId = $"customer{i % 20}";
			var doc = JObject.FromObject(new { id = $"{i}", tenantId, customerId, name = $"Item{i}" });
			var pk = new PartitionKeyBuilder().Add(tenantId).Add(customerId).Build();
			return container.CreateItemAsync(doc, pk);
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(100);
	}

	[Fact]
	public async Task BulkUpsert_HierarchicalPartitionKey_MixedCreateAndUpdate()
	{
		var properties = new ContainerProperties("test", new List<string> { "/tenantId", "/customerId" });
		var container = new InMemoryContainer(properties);

		// First round: all new
		var createTasks = Enumerable.Range(0, 50).Select(i =>
		{
			var pk = new PartitionKeyBuilder().Add("t1").Add($"c{i}").Build();
			return container.UpsertItemAsync(
				JObject.FromObject(new { id = $"{i}", tenantId = "t1", customerId = $"c{i}", name = $"Item{i}" }), pk);
		});

		var createResults = await Task.WhenAll(createTasks);
		createResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

		// Second round: updates
		var updateTasks = Enumerable.Range(0, 50).Select(i =>
		{
			var pk = new PartitionKeyBuilder().Add("t1").Add($"c{i}").Build();
			return container.UpsertItemAsync(
				JObject.FromObject(new { id = $"{i}", tenantId = "t1", customerId = $"c{i}", name = $"Updated{i}" }), pk);
		});

		var updateResults = await Task.WhenAll(updateTasks);
		updateResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
		container.ItemCount.Should().Be(50);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 10: Response Metadata & Diagnostics
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_ActivityId_UniquePerOperation()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		var activityIds = results.Select(r => r.ActivityId).ToList();
		activityIds.Should().OnlyHaveUniqueItems();
		activityIds.Should().NotContain(string.Empty);
		activityIds.Should().NotContainNulls();
	}

	[Fact]
	public async Task BulkUpsert_ResponseMetadata_RequestChargeAndHeaders()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		foreach (var r in results)
		{
			r.RequestCharge.Should().BeGreaterThan(0);
			r.Headers.Should().NotBeNull();
		}
	}

	[Fact]
	public async Task BulkReplace_ResponseMetadata_RequestChargeAndHeaders()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
				$"{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		foreach (var r in results)
		{
			r.RequestCharge.Should().BeGreaterThan(0);
			r.Headers.Should().NotBeNull();
		}
	}

	[Fact]
	public async Task BulkDelete_ResponseMetadata_StatusCodeConsistent()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		foreach (var r in results)
		{
			r.StatusCode.Should().Be(HttpStatusCode.NoContent);
			r.Headers.Should().NotBeNull();
		}
	}

	[Fact]
	public async Task BulkPatch_ResponseMetadata_RequestChargeAndHeaders()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.PatchItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") }));

		var results = await Task.WhenAll(tasks);

		foreach (var r in results)
		{
			r.RequestCharge.Should().BeGreaterThan(0);
			r.Headers.Should().NotBeNull();
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 11: Change Feed Interaction Gaps
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkMixedOps_ChangeFeed_AllOperationsRecorded()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create 50
		var createTasks = Enumerable.Range(0, 50).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));
		await Task.WhenAll(createTasks);

		// Upsert 50 (updates)
		var upsertTasks = Enumerable.Range(0, 50).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
				new PartitionKey("pk1")));
		await Task.WhenAll(upsertTasks);

		var checkpointBeforeDelete = container.GetChangeFeedCheckpoint();

		// Delete 25
		var deleteTasks = Enumerable.Range(0, 25).Select(i =>
			container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));
		await Task.WhenAll(deleteTasks);

		var checkpointAfterDelete = container.GetChangeFeedCheckpoint();

		// Verify 125 total change feed entries: 50 creates + 50 updates + 25 deletes
		checkpointAfterDelete.Should().Be(125);

		// Verify delete tombstones
		(checkpointAfterDelete - checkpointBeforeDelete).Should().Be(25);
	}

	[Fact]
	public async Task BulkCreate_ChangeFeed_StreamIterator_HasContent()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));
		await Task.WhenAll(tasks);

		var iterator = container.GetChangeFeedStreamIterator(ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var totalBytes = 0;
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			if (response.StatusCode == HttpStatusCode.NotModified) break;
			response.IsSuccessStatusCode.Should().BeTrue();
			if (response.Content is not null)
			{
				using var reader = new StreamReader(response.Content);
				var body = await reader.ReadToEndAsync();
				totalBytes += body.Length;
			}
		}

		totalBytes.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task BulkDelete_ChangeFeed_VerifyTombstoneContent()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var checkpoint = container.GetChangeFeedCheckpoint();

		var deleteTasks = Enumerable.Range(0, 50).Select(i =>
			container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));
		await Task.WhenAll(deleteTasks);

		// Read change feed from checkpoint — should see tombstones
		var iterator = container.GetChangeFeedIterator<Newtonsoft.Json.Linq.JObject>(checkpoint);
		var tombstones = new List<Newtonsoft.Json.Linq.JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			tombstones.AddRange(page);
		}

		tombstones.Should().HaveCount(50);
		foreach (var tombstone in tombstones)
		{
			tombstone["id"]!.ToString().Should().NotBeNullOrEmpty();
			// Tombstones should contain the deleted indicator
			var hasDeletedFlag = tombstone["_deleted"] != null;
			var hasTtlExpired = tombstone["_ttlExpired"] != null;
			(hasDeletedFlag || hasTtlExpired).Should().BeTrue(
				"tombstone should indicate deletion via _deleted or _ttlExpired");
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 12: TransactionalBatch Interleaving
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_ConcurrentWithTransactionalBatch_NoDeadlock()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// TransactionalBatch on "batchPK"
		var batchTask = Task.Run(async () =>
		{
			var batch = container.CreateTransactionalBatch(new PartitionKey("batchPK"));
			for (var i = 0; i < 5; i++)
				batch.CreateItem(new TestDocument { Id = $"batch-{i}", PartitionKey = "batchPK", Name = $"Batch{i}" });
			return await batch.ExecuteAsync();
		});

		// Concurrent bulk creates on "bulkPK"
		var bulkTasks = Enumerable.Range(0, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"bulk-{i}", PartitionKey = "bulkPK", Name = $"Bulk{i}" },
				new PartitionKey("bulkPK")));

		var allTasks = bulkTasks.ToList<Task>();
		allTasks.Add(batchTask);
		await Task.WhenAll(allTasks);

		var batchResult = await batchTask;
		batchResult.IsSuccessStatusCode.Should().BeTrue();
		container.ItemCount.Should().Be(105);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 13: Bulk with TTL
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_WithContainerTTL_ItemsExpireAfterRead()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			DefaultTimeToLive = 1 // 1 second
		};

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));
		await Task.WhenAll(tasks);

		container.ItemCount.Should().Be(50);

		// Wait for TTL to expire
		await Task.Delay(2000);

		// Reading triggers lazy eviction
		for (var i = 0; i < 50; i++)
		{
			try
			{
				await container.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				// Expected — item expired
			}
		}

		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task BulkCreate_WithPerItemTTL_ItemsExpireIndependently()
	{
		var container = new InMemoryContainer("test", "/partitionKey")
		{
			DefaultTimeToLive = -1 // TTL enabled, but no default expiry
		};

		// Half with short TTL, half with long TTL
		var shortTtlTasks = Enumerable.Range(0, 25).Select(i =>
			container.CreateItemAsync(
				JObject.FromObject(new { id = $"short-{i}", partitionKey = "pk1", name = $"Short{i}", _ttl = 1 }),
				new PartitionKey("pk1")));

		var longTtlTasks = Enumerable.Range(0, 25).Select(i =>
			container.CreateItemAsync(
				JObject.FromObject(new { id = $"long-{i}", partitionKey = "pk1", name = $"Long{i}", _ttl = 3600 }),
				new PartitionKey("pk1")));

		await Task.WhenAll(shortTtlTasks.Concat(longTtlTasks));
		container.ItemCount.Should().Be(50);

		await Task.Delay(2000);

		// Trigger lazy eviction by reading short-TTL items
		for (var i = 0; i < 25; i++)
		{
			try
			{
				await container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>($"short-{i}", new PartitionKey("pk1"));
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				// Expected
			}
		}

		// Long-TTL items should still be present
		for (var i = 0; i < 25; i++)
		{
			var response = await container.ReadItemAsync<Newtonsoft.Json.Linq.JObject>($"long-{i}", new PartitionKey("pk1"));
			response.StatusCode.Should().Be(HttpStatusCode.OK);
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 14: CosmosClient Integration (Typed + Stream)
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_ViaCosmosClient_StreamVariant_AllSucceed()
	{
		var client = new InMemoryCosmosClient();
		client.ClientOptions.AllowBulkExecution = true;

		var database = client.GetDatabase("test-db");
		await database.CreateContainerIfNotExistsAsync("test-container", "/partitionKey");
		var container = database.GetContainer("test-container");

		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
			return container.CreateItemStreamAsync(
				new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
				new PartitionKey("pk1"));
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
	}

	[Fact]
	public async Task BulkUpsert_ViaCosmosClient_AllSucceed()
	{
		var client = new InMemoryCosmosClient();
		client.ClientOptions.AllowBulkExecution = true;

		var database = client.GetDatabase("test-db");
		await database.CreateContainerIfNotExistsAsync("test-container", "/partitionKey");
		var container = database.GetContainer("test-container");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
	}

	[Fact]
	public async Task BulkMixedOps_ViaCosmosClient_AllSucceed()
	{
		var client = new InMemoryCosmosClient();
		client.ClientOptions.AllowBulkExecution = true;

		var database = client.GetDatabase("test-db");
		await database.CreateContainerIfNotExistsAsync("test-container", "/partitionKey");
		var container = database.GetContainer("test-container");

		// Create 50 via client
		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"existing-{i}", PartitionKey = "pk1", Name = $"Existing{i}" },
				new PartitionKey("pk1"));

		// Mixed concurrent operations
		var createTasks = Enumerable.Range(0, 25).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"new-{i}", PartitionKey = "pk1", Name = $"New{i}" },
				new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

		var upsertTasks = Enumerable.Range(0, 25).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"upserted-{i}", PartitionKey = "pk1", Name = $"Upserted{i}" },
				new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

		var deleteTasks = Enumerable.Range(0, 25).Select(i =>
			container.DeleteItemAsync<TestDocument>($"existing-{i}", new PartitionKey("pk1"))
				.ContinueWith(t => t.Result.StatusCode));

		var allTasks = createTasks.Concat(upsertTasks).Concat(deleteTasks);
		var results = await Task.WhenAll(allTasks);

		var createResults = results.Take(25).ToList();
		createResults.Should().OnlyContain(s => s == HttpStatusCode.Created);

		var upsertResults = results.Skip(25).Take(25).ToList();
		upsertResults.Should().OnlyContain(s => s == HttpStatusCode.Created);

		var deleteResults = results.Skip(50).Take(25).ToList();
		deleteResults.Should().OnlyContain(s => s == HttpStatusCode.NoContent);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 15: Patch Conditional Operations in Bulk
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkPatch_WithFilterPredicate_OnlyPatchesMatchingItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create 100 items: 50 active, 50 inactive
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", IsActive = i < 50 },
				new PartitionKey("pk1"));

		// Patch all 100 with filter predicate
		var tasks = Enumerable.Range(0, 100).Select(i =>
			Task.Run(async () =>
			{
				try
				{
					var response = await container.PatchItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"),
						new[] { PatchOperation.Set("/name", "Patched") },
						new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.isActive = true" });
					return (StatusCode: response.StatusCode, Success: true);
				}
				catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
				{
					return (StatusCode: ex.StatusCode, Success: false);
				}
			}));

		var results = await Task.WhenAll(tasks);

		results.Count(r => r.Success).Should().Be(50);
		results.Count(r => !r.Success).Should().Be(50);
	}

	[Fact]
	public async Task BulkPatch_WithFilterPredicate_Stream_OnlyPatchesMatching()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}", IsActive = i < 50 },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.PatchItemStreamAsync($"{i}", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", "Patched") },
				new PatchItemRequestOptions { FilterPredicate = "FROM c WHERE c.isActive = true" }));

		var results = await Task.WhenAll(tasks);

		results.Count(r => r.StatusCode == HttpStatusCode.OK).Should().Be(50);
		results.Count(r => r.StatusCode == HttpStatusCode.PreconditionFailed).Should().Be(50);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 16: Large Batch Stress Tests
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_1000Items_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 1000).Select(i =>
		{
			var pk = $"pk{i % 20}";
			return container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = pk, Name = $"Item{i}" },
				new PartitionKey(pk));
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		container.ItemCount.Should().Be(1000);
	}

	[Fact]
	public async Task BulkUpsert_1000Items_CreateThenUpdate_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create round
		var createTasks = Enumerable.Range(0, 1000).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));
		var createResults = await Task.WhenAll(createTasks);
		createResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);

		// Update round
		var updateTasks = Enumerable.Range(0, 1000).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
				new PartitionKey("pk1")));
		var updateResults = await Task.WhenAll(updateTasks);
		updateResults.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		container.ItemCount.Should().Be(1000);
	}

	[Fact]
	public async Task BulkMixedOps_1000Items_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-create items for replace and delete
		for (var i = 0; i < 500; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"pre-{i}", PartitionKey = "pk1", Name = $"Pre{i}" },
				new PartitionKey("pk1"));

		var createTasks = Enumerable.Range(0, 250).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"new-{i}", PartitionKey = "pk1", Name = $"New{i}" },
				new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

		var upsertTasks = Enumerable.Range(0, 250).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"upserted-{i}", PartitionKey = "pk1", Name = $"Upserted{i}" },
				new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

		var replaceTasks = Enumerable.Range(0, 250).Select(i =>
			container.ReplaceItemAsync(
				new TestDocument { Id = $"pre-{i}", PartitionKey = "pk1", Name = $"Replaced{i}" },
				$"pre-{i}", new PartitionKey("pk1")).ContinueWith(t => t.Result.StatusCode));

		var deleteTasks = Enumerable.Range(250, 250).Select(i =>
			container.DeleteItemAsync<TestDocument>($"pre-{i}", new PartitionKey("pk1"))
				.ContinueWith(t => t.Result.StatusCode));

		var allTasks = createTasks.Concat(upsertTasks).Concat(replaceTasks).Concat(deleteTasks);
		var results = await Task.WhenAll(allTasks);

		// 500 pre-created - 250 deleted + 250 new + 250 upserted = 750
		container.ItemCount.Should().Be(750);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase 17: DeleteAllItemsByPartitionKey Concurrency
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task DeleteAllItemsByPartitionKey_ConcurrentWithBulkCreate_DifferentPKs_NoCorruption()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Pre-populate PK "B" with 200 items
		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"b-{i}", PartitionKey = "B", Name = $"B-Item{i}" },
				new PartitionKey("B"));

		// Concurrently: create on PK "A" + delete all on PK "B"
		var createTasks = Enumerable.Range(0, 200).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"a-{i}", PartitionKey = "A", Name = $"A-Item{i}" },
				new PartitionKey("A")));

		var deleteTask = container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("B"));

		var allConcurrentTasks = createTasks.ToList<Task>();
		allConcurrentTasks.Add(deleteTask);
		await Task.WhenAll(allConcurrentTasks);

		// PK "A" items should all be present
		var iterA = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("A") });

		var aItems = new List<TestDocument>();
		while (iterA.HasMoreResults)
		{
			var page = await iterA.ReadNextAsync();
			aItems.AddRange(page);
		}

		aItems.Should().HaveCount(200);

		// PK "B" items should be gone
		var iterB = container.GetItemQueryIterator<TestDocument>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("B") });

		var bItems = new List<TestDocument>();
		while (iterB.HasMoreResults)
		{
			var page = await iterB.ReadNextAsync();
			bItems.AddRange(page);
		}

		bItems.Should().BeEmpty();
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase A: ReadMany Bulk Operations
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkReadMany_Typed_AllItemsFound()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 10).Select(batch =>
		{
			var items = Enumerable.Range(batch * 20, 20)
				.Select(i => ($"{i}", new PartitionKey("pk1")))
				.ToList();
			return container.ReadManyItemsAsync<TestDocument>(items);
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.Count == 20);
		results.SelectMany(r => r).Select(d => d.Id).Distinct().Should().HaveCount(200);
	}

	[Fact]
	public async Task BulkReadMany_Stream_AllItemsFound()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 200; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 10).Select(batch =>
		{
			var items = Enumerable.Range(batch * 20, 20)
				.Select(i => ($"{i}", new PartitionKey("pk1")))
				.ToList();
			return container.ReadManyItemsStreamAsync(items);
		});

		var results = await Task.WhenAll(tasks);
		foreach (var r in results)
		{
			r.StatusCode.Should().Be(HttpStatusCode.OK);
			using var reader = new StreamReader(r.Content);
			var envelope = JObject.Parse(await reader.ReadToEndAsync());
			((int)envelope["_count"]!).Should().Be(20);
		}
	}

	[Fact]
	public async Task BulkReadMany_Typed_SomeItemsMissing_ReturnsFoundOnly()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		// Only create even IDs
		for (var i = 0; i < 100; i += 2)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var allIds = Enumerable.Range(0, 100)
			.Select(i => ($"{i}", new PartitionKey("pk1")))
			.ToList();

		var response = await container.ReadManyItemsAsync<TestDocument>(allIds);
		response.Count.Should().Be(50); // only the even ones
	}

	[Fact]
	public async Task BulkReadMany_Typed_ConcurrentWithWrites_NoCrash()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var readTasks = Enumerable.Range(0, 10).Select(_ =>
		{
			var items = Enumerable.Range(0, 100)
				.Select(i => ($"{i}", new PartitionKey("pk1")))
				.ToList();
			return container.ReadManyItemsAsync<TestDocument>(items);
		});

		var createTasks = Enumerable.Range(100, 100).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"New{i}" },
				new PartitionKey("pk1")));

		var upsertTasks = Enumerable.Range(0, 50).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Updated{i}" },
				new PartitionKey("pk1")));

		var allTasks = new List<Task>();
		allTasks.AddRange(readTasks);
		allTasks.AddRange(createTasks);
		allTasks.AddRange(upsertTasks);

		await Task.WhenAll(allTasks);
		container.ItemCount.Should().Be(200);
	}

	[Fact]
	public async Task BulkReadMany_Typed_WithIfNoneMatchEtag_Returns304()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var allIds = Enumerable.Range(0, 10)
			.Select(i => ($"{i}", new PartitionKey("pk1")))
			.ToList();

		// First read to get composite ETag
		var firstResponse = await container.ReadManyItemsAsync<TestDocument>(allIds);
		firstResponse.Count.Should().Be(10);
		var compositeEtag = firstResponse.ETag;
		compositeEtag.Should().NotBeNullOrEmpty();

		// Second read with IfNoneMatchEtag = composite -> 304
		var secondResponse = await container.ReadManyItemsAsync<TestDocument>(allIds,
			new ReadManyRequestOptions { IfNoneMatchEtag = compositeEtag });
		secondResponse.StatusCode.Should().Be(HttpStatusCode.NotModified);
		secondResponse.Count.Should().Be(0);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase B: PatchItemStreamAsync Concurrency
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkPatch_StreamVariant_ConcurrentIncrement_AllApplied()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "counter", PartitionKey = "pk1", Value = 0 },
			new PartitionKey("pk1"));

		var gate = new ManualResetEventSlim(false);
		var tasks = Enumerable.Range(0, 50).Select(_ => Task.Run(async () =>
		{
			gate.Wait();
			var patchOps = new[] { PatchOperation.Increment("/value", 1) };
			var json = JsonConvert.SerializeObject(patchOps);
			return await container.PatchItemStreamAsync(
				"counter", new PartitionKey("pk1"), patchOps);
		}));

		var taskList = tasks.ToList();
		gate.Set();
		var results = await Task.WhenAll(taskList);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		var final = await container.ReadItemAsync<TestDocument>("counter", new PartitionKey("pk1"));
		final.Resource.Value.Should().Be(50);
	}

	[Fact]
	public async Task BulkPatch_StreamVariant_ConcurrentSet_LastWriterWins()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "target", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i => Task.Run(async () =>
		{
			var patchOps = new[] { PatchOperation.Set("/name", $"Version{i}") };
			return await container.PatchItemStreamAsync(
				"target", new PartitionKey("pk1"), patchOps);
		}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);

		var final = await container.ReadItemAsync<TestDocument>("target", new PartitionKey("pk1"));
		final.Resource.Name.Should().StartWith("Version");
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase C: CancellationToken Regression Guards
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkReadMany_StreamVariant_CancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var cts = new CancellationTokenSource();
		cts.Cancel();

		var allIds = Enumerable.Range(0, 10)
			.Select(i => ($"{i}", new PartitionKey("pk1")))
			.ToList();

		await Assert.ThrowsAsync<OperationCanceledException>(() =>
			container.ReadManyItemsStreamAsync(allIds, cancellationToken: cts.Token));
	}

	[Fact]
	public async Task BulkReadMany_Typed_CancelledToken_ThrowsOperationCancelled()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var cts = new CancellationTokenSource();
		cts.Cancel();

		var allIds = Enumerable.Range(0, 10)
			.Select(i => ($"{i}", new PartitionKey("pk1")))
			.ToList();

		await Assert.ThrowsAsync<OperationCanceledException>(() =>
			container.ReadManyItemsAsync<TestDocument>(allIds, cancellationToken: cts.Token));
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase D: ReadMany Concurrent with Deletes
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkReadMany_ConcurrentWithDeletes_NoException()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var readTasks = Enumerable.Range(0, 20).Select(_ =>
		{
			var items = Enumerable.Range(0, 100)
				.Select(i => ($"{i}", new PartitionKey("pk1")))
				.ToList();
			return container.ReadManyItemsAsync<TestDocument>(items);
		});

		var deleteTasks = Enumerable.Range(0, 100).Select(i =>
			container.DeleteItemAsync<TestDocument>($"{i}", new PartitionKey("pk1")));

		var allTasks = new List<Task>();
		allTasks.AddRange(readTasks);
		allTasks.AddRange(deleteTasks);

		await Task.WhenAll(allTasks);
		container.ItemCount.Should().Be(0);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase E: Stream Conditional ETag Operations
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkReplace_StreamVariant_WithCorrectIfMatchEtag_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var etags = new List<string>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			etags.Add(r.ETag);
		}

		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Replaced{i}" });
			return container.ReplaceItemStreamAsync(
				new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
				$"{i}", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = etags[i] });
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task BulkReplace_StreamVariant_WithStaleIfMatchEtag_AllFail412()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var etags = new List<string>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			etags.Add(r.ETag);
		}

		// Mutate all to make ETags stale
		for (var i = 0; i < 100; i++)
			await container.ReplaceItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Mutated{i}" },
				$"{i}", new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Stale{i}" });
			return container.ReplaceItemStreamAsync(
				new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
				$"{i}", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = etags[i] });
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.PreconditionFailed);
	}

	[Fact]
	public async Task BulkRead_StreamVariant_WithIfNoneMatchEtag_Returns304()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var etags = new List<string>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			etags.Add(r.ETag);
		}

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.ReadItemStreamAsync($"{i}", new PartitionKey("pk1"),
				new ItemRequestOptions { IfNoneMatchEtag = etags[i] }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NotModified);
	}

	[Fact]
	public async Task BulkUpsert_StreamVariant_WithIfMatchEtag_Star_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 100; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 100).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Upserted{i}" });
			return container.UpsertItemStreamAsync(
				new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
				new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = "*" });
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
	}

	[Fact]
	public async Task BulkDelete_StreamVariant_WithCorrectIfMatchEtag_AllSucceed()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var etags = new List<string>();
		for (var i = 0; i < 100; i++)
		{
			var r = await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));
			etags.Add(r.ETag);
		}

		var tasks = Enumerable.Range(0, 100).Select(i =>
			container.DeleteItemStreamAsync($"{i}", new PartitionKey("pk1"),
				new ItemRequestOptions { IfMatchEtag = etags[i] }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.NoContent);
		container.ItemCount.Should().Be(0);
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase F: State Export/Import During Bulk Operations
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ExportState_DuringBulkWrites_DoesNotThrow()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var cts = new CancellationTokenSource();

		var createTask = Task.Run(async () =>
		{
			for (var i = 0; i < 200; i++)
				await container.CreateItemAsync(
					new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
					new PartitionKey("pk1"));
		});

		var exportTask = Task.Run(() =>
		{
			var exports = new List<string>();
			while (!createTask.IsCompleted)
			{
				var state = container.ExportState();
				state.Should().NotBeNullOrEmpty();
				exports.Add(state);
			}
			return exports;
		});

		await createTask;
		var allExports = await exportTask;
		allExports.Should().NotBeEmpty();
	}

	[Fact]
	public async Task ImportState_AfterBulkWrites_RestoresCorrectly()
	{
		var source = new InMemoryContainer("source", "/partitionKey");

		var tasks = Enumerable.Range(0, 100).Select(i =>
			source.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));
		await Task.WhenAll(tasks);

		var state = source.ExportState();

		var target = new InMemoryContainer("target", "/partitionKey");
		target.ImportState(state);

		for (var i = 0; i < 100; i++)
		{
			var r = await target.ReadItemAsync<TestDocument>($"{i}", new PartitionKey("pk1"));
			r.Resource.Name.Should().Be($"Item{i}");
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase G: Stream EnableContentResponseOnWrite
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task BulkCreate_StreamVariant_EnableContentResponseOnWriteFalse_EmptyBody()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
			return container.CreateItemStreamAsync(
				new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
				new PartitionKey("pk1"),
				new ItemRequestOptions { EnableContentResponseOnWrite = false });
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		foreach (var r in results)
		{
			if (r.Content != null)
			{
				using var reader = new StreamReader(r.Content);
				var body = await reader.ReadToEndAsync();
				body.Should().BeNullOrEmpty();
			}
		}
	}

	[Fact]
	public async Task BulkUpsert_StreamVariant_EnableContentResponseOnWriteFalse_EmptyBody()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		var tasks = Enumerable.Range(0, 50).Select(i =>
		{
			var json = JsonConvert.SerializeObject(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" });
			return container.UpsertItemStreamAsync(
				new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)),
				new PartitionKey("pk1"),
				new ItemRequestOptions { EnableContentResponseOnWrite = false });
		});

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
		foreach (var r in results)
		{
			if (r.Content != null)
			{
				using var reader = new StreamReader(r.Content);
				var body = await reader.ReadToEndAsync();
				body.Should().BeNullOrEmpty();
			}
		}
	}

	[Fact]
	public async Task BulkPatch_StreamVariant_EnableContentResponseOnWriteFalse_EmptyBody()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		for (var i = 0; i < 50; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var tasks = Enumerable.Range(0, 50).Select(i =>
			container.PatchItemStreamAsync(
				$"{i}", new PartitionKey("pk1"),
				new[] { PatchOperation.Set("/name", $"Patched{i}") },
				new PatchItemRequestOptions { EnableContentResponseOnWrite = false }));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.OK);
		foreach (var r in results)
		{
			if (r.Content != null)
			{
				using var reader = new StreamReader(r.Content);
				var body = await reader.ReadToEndAsync();
				body.Should().BeNullOrEmpty();
			}
		}
	}

	// ══════════════════════════════════════════════════════════════════════════
	// Deep-Dive Phase H: ReadMany Composite ETag Mismatch
	// ══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ReadMany_CompositeEtag_MismatchReturnsData()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1"));

		var allIds = Enumerable.Range(0, 10)
			.Select(i => ($"{i}", new PartitionKey("pk1")))
			.ToList();

		// Get composite ETag
		var firstResponse = await container.ReadManyItemsAsync<TestDocument>(allIds);
		var compositeEtag = firstResponse.ETag;

		// Mutate one item to change composite ETag
		await container.UpsertItemAsync(
			new TestDocument { Id = "0", PartitionKey = "pk1", Name = "Mutated" },
			new PartitionKey("pk1"));

		// ReadMany with old composite ETag -> should return data (200) since etag changed
		var secondResponse = await container.ReadManyItemsAsync<TestDocument>(allIds,
			new ReadManyRequestOptions { IfNoneMatchEtag = compositeEtag });
		secondResponse.StatusCode.Should().Be(HttpStatusCode.OK);
		secondResponse.Count.Should().Be(10);
	}
}
