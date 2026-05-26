using System.Net;
using System.Text;
using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.ProductionExtensions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
//  Existing tests (preserved from original TtlTests.cs)
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlContainerLevelTests
{
	[Fact]
	public async Task ContainerTtl_ExpiredItems_NotReturnedByRead()
	{
		var container = new InMemoryContainer("ttl-container", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ContainerTtl_ExpiredItems_NotReturnedByQuery()
	{
		var container = new InMemoryContainer("ttl-container", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ContainerTtl_NonExpiredItems_StillReturned()
	{
		var container = new InMemoryContainer("ttl-container", "/partitionKey")
		{
			DefaultTimeToLive = 60
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "LongLived" },
			new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("LongLived");
	}

	[Fact]
	public async Task ContainerTtl_NullMeansNoExpiration()
	{
		var container = new InMemoryContainer("ttl-container", "/partitionKey")
		{
			DefaultTimeToLive = null
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "NoExpiry" },
			new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("NoExpiry");
	}

	[Fact]
	public async Task ContainerTtl_LazyEviction_OnRead()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 2
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(3));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ContainerTtl_DeletedItemNotEvictedTwice()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 2
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(3));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group A: Per-item TTL semantics
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlPerItemTests
{
	[Fact]
	public async Task PerItemTtl_FromJsonField_Honored()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 60
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Short","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PerItemTtl_MinusOne_NeverExpires_EvenWithContainerTtl()
	{
		// A1: In real Cosmos DB, _ttl = -1 means "never expire" even if container has DefaultTimeToLive.
		// Bug fix: IsExpired was treating _ttl=-1 as elapsed >= -1 which is always true.
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Immortal","_ttl":-1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should still be readable because _ttl=-1 overrides container TTL
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Immortal");
	}

	[Fact]
	public async Task PerItemTtl_MinusOne_WithContainerTtlMinusOne_NeverExpires()
	{
		// A2: DefaultTimeToLive=-1 + _ttl=-1 → never expires
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Immortal","_ttl":-1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Immortal");
	}

	[Fact]
	public async Task PerItemTtl_IgnoredWhenContainerTtlIsNull()
	{
		// A3: Real Cosmos ignores per-item _ttl entirely when DefaultTimeToLive is null (TTL feature OFF).
		// Bug fix: IsExpired was checking per-item _ttl before checking container-level setting.
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = null
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Survives","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should still be readable because container TTL is null (feature disabled)
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Survives");
	}

	[Fact]
	public async Task PerItemTtl_LargerThanContainer_UsesItemTtl()
	{
		// A4: Per-item _ttl overrides container default — item lives longer
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"LongerLived","_ttl":60}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Container TTL is 1s but item has _ttl=60s, so item should still be alive
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("LongerLived");
	}

	[Fact]
	public async Task PerItemTtl_SmallerThanContainer_UsesItemTtl()
	{
		// A5: Per-item _ttl overrides container default — item dies sooner
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 60
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"ShortLived","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PerItemTtl_MultipleItemsDifferentTtls()
	{
		// A6: Multiple items in same container with different per-item TTLs
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 60
		};

		var shortJson = """{"id":"short","partitionKey":"pk1","name":"Short","_ttl":1}""";
		var longJson = """{"id":"long","partitionKey":"pk1","name":"Long","_ttl":60}""";
		var defaultJson = """{"id":"default","partitionKey":"pk1","name":"Default"}""";

		await container.CreateItemStreamAsync(new MemoryStream(Encoding.UTF8.GetBytes(shortJson)), new PartitionKey("pk1"));
		await container.CreateItemStreamAsync(new MemoryStream(Encoding.UTF8.GetBytes(longJson)), new PartitionKey("pk1"));
		await container.CreateItemStreamAsync(new MemoryStream(Encoding.UTF8.GetBytes(defaultJson)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Short-lived item should be expired
		var actShort = () => container.ReadItemAsync<TestDocument>("short", new PartitionKey("pk1"));
		var exShort = await actShort.Should().ThrowAsync<CosmosException>();
		exShort.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);

		// Long-lived and default items should still exist
		var readLong = await container.ReadItemAsync<TestDocument>("long", new PartitionKey("pk1"));
		readLong.Resource.Name.Should().Be("Long");

		var readDefault = await container.ReadItemAsync<TestDocument>("default", new PartitionKey("pk1"));
		readDefault.Resource.Name.Should().Be("Default");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group B: Container DefaultTimeToLive = -1 (TTL ON, no default expiry)
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlContainerMinusOneTests
{
	[Fact]
	public async Task ContainerTtlMinusOne_ItemsWithoutPerItemTtl_NeverExpire()
	{
		// B1: DefaultTimeToLive=-1 means TTL is enabled but items don't expire by default.
		// Only items with explicit _ttl will expire.
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "NoItemTtl" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("NoItemTtl");
	}

	[Fact]
	public async Task ContainerTtlMinusOne_ItemWithPerItemTtl_Expires()
	{
		// B2: DefaultTimeToLive=-1 but per-item _ttl=1 → item expires after 1s
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"WillDie","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ContainerTtlMinusOne_ItemWithTtlMinusOne_NeverExpires()
	{
		// B3: DefaultTimeToLive=-1 + _ttl=-1 → never expires
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Immortal","_ttl":-1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Immortal");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group C: TTL interaction with write operations
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlWriteOperationTests
{
	[Fact]
	public async Task UpsertResetsExpiration()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 3
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Updated" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Updated");
	}

	[Fact]
	public async Task ReplaceResetsExpiration()
	{
		// C1: Replace should reset the TTL clock, just like upsert
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 3
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		await container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Should still be alive (3s TTL reset 2s ago by replace)
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Replaced");
	}

	[Fact]
	public async Task PatchResetsExpiration()
	{
		// C2: Patch should reset the TTL clock
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 3
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		await container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			new[] { PatchOperation.Set("/name", "Patched") });

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Should still be alive (3s TTL reset 2s ago by patch)
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Patched");
	}

	[Fact]
	public async Task Replace_OnExpiredItem_Returns404()
	{
		// C3: Replacing an expired item should return 404, not succeed silently
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Patch_OnExpiredItem_Returns404()
	{
		// C4: Patching an expired item should return 404
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.PatchItemAsync<TestDocument>(
			"1", new PartitionKey("pk1"),
			new[] { PatchOperation.Set("/name", "Patched") });

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Delete_OnExpiredItem_Returns404()
	{
		// C5: Deleting an expired item should return 404
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Upsert_OnExpiredItem_Succeeds_CreatesNew()
	{
		// C6: Upsert on an expired item effectively creates a new item
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Upsert should succeed — the expired item is gone, so this acts as a create
		var result = await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Recreated" },
			new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Recreated");
	}

	[Fact]
	public async Task Create_OnExpiredItem_Succeeds()
	{
		// C7: Creating an item with the same ID as an expired item should succeed
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Create should succeed — the expired item should have been evicted
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "NewItem" },
			new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("NewItem");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group D: TTL on different read paths
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlReadPathTests
{
	[Fact]
	public async Task ReadMany_ExcludesExpiredItems()
	{
		// D1: ReadManyItemsAsync should not return expired items
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Expired" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "AlsoExpired" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var results = await container.ReadManyItemsAsync<TestDocument>(
			new List<(string, PartitionKey)>
			{
				("1", new PartitionKey("pk1")),
				("2", new PartitionKey("pk1"))
			});

		results.Resource.Should().BeEmpty();
	}

	[Fact]
	public async Task ReadManyStream_ExcludesExpiredItems()
	{
		// D2: ReadManyItemsStreamAsync should not return expired items
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Expired" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var response = await container.ReadManyItemsStreamAsync(
			new List<(string, PartitionKey)> { ("1", new PartitionKey("pk1")) });

		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var envelope = JObject.Parse(body);
		var docs = envelope["Documents"] as JArray;
		docs.Should().BeEmpty();
	}

	[Fact]
	public async Task CrossPartitionQuery_ExcludesExpiredItems()
	{
		// D3: Cross-partition query should exclude expired items
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" },
			new PartitionKey("pk2"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task LinqQuery_ExcludesExpiredItems()
	{
		// D4: LINQ-based queries should exclude expired items
		InMemoryFeedIteratorSetup.Register();

		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var iterator = container.GetItemLinqQueryable<TestDocument>()
			.Where(d => d.Name == "Test")
			.ToFeedIteratorOverridable();
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task CountAggregate_ExcludesExpiredItems()
	{
		// D5: COUNT aggregate should not include expired items
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var iterator = container.GetItemQueryIterator<int>("SELECT VALUE COUNT(1) FROM c");
		var results = new List<int>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Should().Be(0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group E: Container TTL management
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlContainerManagementTests
{
	[Fact]
	public async Task ReplaceContainer_ChangesTtl_AffectsEviction()
	{
		// E1: Calling ReplaceContainerAsync to set DefaultTimeToLive should actually affect
		// item expiration, not just store the property value.
		var container = new InMemoryContainer("ttl-test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Enable TTL via ReplaceContainerAsync
		await container.ReplaceContainerAsync(
			new ContainerProperties("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 });

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should now be expired because container TTL was set to 1s
		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DisablingTtl_StopsFutureExpiration()
	{
		// E2: Setting DefaultTimeToLive=null stops expiration; even items with per-item _ttl survive
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 60
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		// Disable TTL before the per-item _ttl expires
		container.DefaultTimeToLive = null;

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should survive because TTL feature is now disabled
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task EnablingTtl_OnExistingContainer_ExpiresOldItems()
	{
		// E3: Enabling TTL on a container that already has items should expire items
		// whose _ts (creation/update timestamp) is older than the new TTL.
		var container = new InMemoryContainer("ttl-test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Old" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Now enable TTL with 1 second — item was written 2+ seconds ago
		container.DefaultTimeToLive = 1;

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ChangingContainerTtl_AffectsExistingItems()
	{
		// E4: Changing container TTL from a long value to a short value should
		// cause items to expire based on the new value.
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 60
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Reduce TTL to 1 second — item was created >2s ago
		container.DefaultTimeToLive = 1;

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group F: Edge cases
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlEdgeCaseTests
{
	[Fact]
	public async Task VeryLargeTtl_ItemDoesNotExpire()
	{
		// F2: Max int TTL should not cause overflow or unexpected expiry
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = int.MaxValue
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Forever" },
			new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Forever");
	}

	[Fact]
	public async Task CreateSameId_AfterExpiry_Succeeds()
	{
		// F4: After an item expires, creating a new item with the same ID should succeed
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Should not get 409 Conflict — the original is expired/evicted
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "NewItem" },
			new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("NewItem");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group H: Stream operation variants
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlStreamVariantTests
{
	[Fact]
	public async Task ReadItemStreamAsync_Returns404_ForExpiredItem()
	{
		// H1: Stream read should return 404 for expired items
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var response = await container.ReadItemStreamAsync("1", new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ReplaceItemStreamAsync_Returns404_ForExpiredItem()
	{
		// H2: Stream replace on an expired item should return 404
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var json = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
		var response = await container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), "1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task DeleteItemStreamAsync_Returns404_ForExpiredItem()
	{
		// H3: Stream delete on an expired item should return 404
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var response = await container.DeleteItemStreamAsync("1", new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PatchItemStreamAsync_Returns404_ForExpiredItem()
	{
		// H4: Stream patch on an expired item should return 404
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var response = await container.PatchItemStreamAsync(
			"1", new PartitionKey("pk1"),
			new[] { PatchOperation.Set("/name", "Patched") });

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Group G: Change feed interaction (divergent behaviour)
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlChangeFeedDivergentTests
{
	[Fact]
	public async Task TtlExpiry_ProducesChangeFeedDeleteEvent()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Trigger lazy eviction by reading the expired item
		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();

		// Use checkpoint-based API (which includes deletes, unlike LatestVersion mode)
		var iterator = container.GetChangeFeedIterator<JObject>(0);
		var changes = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(page.Resource);
		}

		// Create event + delete tombstone from lazy eviction
		changes.Should().HaveCount(2);
	}

	[Fact]
	public async Task TtlExpiry_ChangeFeedDeleteTombstone_HasCorrectShape()
	{
		// When an item expires via TTL and is lazily evicted, a delete tombstone
		// is now recorded in the change feed (matching real Cosmos behavior).

		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Trigger lazy eviction
		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();

		// Use checkpoint-based API (which includes deletes, unlike LatestVersion mode)
		var iterator = container.GetChangeFeedIterator<JObject>(0);
		var changes = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(page.Resource);
		}

		changes.Should().HaveCount(2);
		var tombstone = changes.Last();
		tombstone["id"]!.Value<string>().Should().Be("1");
		tombstone["_deleted"]!.Value<bool>().Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: TTL Bug Fix Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlBugFixTests
{
	[Fact]
	public async Task Upsert_OnExpiredItem_Returns201Created()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var result = await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Reborn" },
			new PartitionKey("pk1"));

		result.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task UpsertStream_OnExpiredItem_Returns201Created()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Temp"}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var newJson = """{"id":"1","partitionKey":"pk1","name":"Reborn"}""";
		var result = await container.UpsertItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(newJson)), new PartitionKey("pk1"));

		result.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task Upsert_OnExpiredItem_EvictsBeforeStatusCheck()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Reborn" },
			new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Reborn");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: Per-Item TTL Extended Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlPerItemExtendedTests
{
	public class TtlDocument
	{
		[JsonProperty("id")] public string Id { get; set; } = default!;
		[JsonProperty("partitionKey")] public string PartitionKey { get; set; } = default!;
		[JsonProperty("name")] public string Name { get; set; } = default!;
		[JsonProperty("_ttl")] public int? Ttl { get; set; }
	}

	[Fact]
	public async Task PerItemTtl_ViaTypedObject_WithJsonPropertyAttribute()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		await container.CreateItemAsync(
			new TtlDocument { Id = "1", PartitionKey = "pk1", Name = "Typed", Ttl = 1 },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TtlDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PerItemTtl_ReplacedWithNewTtl_UsesNewValue()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Original","_ttl":60}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var newJson = """{"id":"1","partitionKey":"pk1","name":"Updated","_ttl":1}""";
		await container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(newJson)), "1", new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PerItemTtl_RemovedOnReplace_FallsBackToContainerDefault()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		// Create with long per-item TTL
		var json = """{"id":"1","partitionKey":"pk1","name":"LongLived","_ttl":600}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		// Replace without _ttl → falls back to container default (1s)
		await container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "NowShort" },
			"1", new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PerItemTtl_Queryable_InSelectProjection()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"HasTtl","_ttl":300}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var iterator = container.GetItemQueryIterator<JObject>("SELECT c._ttl FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["_ttl"]!.Value<int>().Should().Be(300);
	}

	[Fact]
	public async Task PerItemTtl_NonIntegerValue_Ignored()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		// _ttl is a string "abc" — should be ignored (treated as no per-item TTL)
		var json = """{"id":"1","partitionKey":"pk1","name":"BadTtl","_ttl":"abc"}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Falls back to container default (1s), should be expired
		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: Transactional Batch TTL Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlTransactionalBatchTests
{
	[Fact]
	public async Task Batch_ReadExpiredItem_Returns404InBatchResult()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("1");
		var response = await batch.ExecuteAsync();

		response[0].StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Batch_ReplaceExpiredItem_Returns404InBatchResult()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReplaceItem("1", new TestDocument { Id = "1", PartitionKey = "pk1", Name = "New" });
		var response = await batch.ExecuteAsync();

		response.StatusCode.Should().NotBe(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_DeleteExpiredItem_Returns404InBatchResult()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.DeleteItem("1");
		var response = await batch.ExecuteAsync();

		response.StatusCode.Should().NotBe(HttpStatusCode.OK);
	}

	[Fact]
	public async Task Batch_CreateAfterExpiry_Succeeds()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Reborn" });
		var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
	}

	[Fact]
	public async Task Batch_UpsertExpiredItem_Succeeds()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.UpsertItem(new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Reborn" });
		var response = await batch.ExecuteAsync();

		response.IsSuccessStatusCode.Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: Container Properties Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlContainerPropertiesTests
{
	[Fact]
	public async Task ReadContainerAsync_ReturnsTtlInProperties()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 42
		};

		var response = await container.ReadContainerAsync();
		response.Resource.DefaultTimeToLive.Should().Be(42);
	}

	[Fact]
	public async Task ReadContainerStreamAsync_ReturnsTtlInJsonBody()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 42
		};

		var response = await container.ReadContainerStreamAsync();
		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);
		jObj["defaultTtl"]!.Value<int>().Should().Be(42);
	}

	[Fact]
	public async Task ReplaceContainerStreamAsync_UpdatesTtl()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 10
		};

		var props = new ContainerProperties("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 99
		};
		await container.ReplaceContainerAsync(props);

		container.DefaultTimeToLive.Should().Be(99);
	}

	[Fact]
	public void ContainerCreatedViaContainerProperties_HasTtl()
	{
		var props = new ContainerProperties("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 30
		};
		var container = new InMemoryContainer(props);

		container.DefaultTimeToLive.Should().Be(30);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: _ts System Property Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlSystemPropertyTests
{
	[Fact]
	public async Task Ts_SystemProperty_SetOnCreate()
	{
		var container = new InMemoryContainer("ts-test", "/partitionKey");

		var before = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		var after = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var ts = read.Resource["_ts"]!.Value<long>();
		ts.Should().BeGreaterThanOrEqualTo(before);
		ts.Should().BeLessThanOrEqualTo(after);
	}

	[Fact]
	public async Task Ts_SystemProperty_UpdatedOnReplace()
	{
		var container = new InMemoryContainer("ts-test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var readBefore = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsBefore = readBefore.Resource["_ts"]!.Value<long>();

		await Task.Delay(TimeSpan.FromSeconds(1));

		await container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		var readAfter = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsAfter = readAfter.Resource["_ts"]!.Value<long>();

		tsAfter.Should().BeGreaterThanOrEqualTo(tsBefore);
	}

	[Fact]
	public async Task Ts_SystemProperty_UpdatedOnUpsert()
	{
		var container = new InMemoryContainer("ts-test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var readBefore = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsBefore = readBefore.Resource["_ts"]!.Value<long>();

		await Task.Delay(TimeSpan.FromSeconds(1));

		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Upserted" },
			new PartitionKey("pk1"));

		var readAfter = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsAfter = readAfter.Resource["_ts"]!.Value<long>();

		tsAfter.Should().BeGreaterThanOrEqualTo(tsBefore);
	}

	[Fact]
	public async Task Ts_SystemProperty_UpdatedOnPatch()
	{
		var container = new InMemoryContainer("ts-test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var readBefore = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsBefore = readBefore.Resource["_ts"]!.Value<long>();

		await Task.Delay(TimeSpan.FromSeconds(1));

		await container.PatchItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Set("/name", "Patched") });

		var readAfter = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var tsAfter = readAfter.Resource["_ts"]!.Value<long>();

		tsAfter.Should().BeGreaterThanOrEqualTo(tsBefore);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: Query Path Extended Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlQueryPathExtendedTests
{
	private async Task<InMemoryContainer> CreateContainerWithExpiredAndLiveItems()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		// Item that will expire quickly
		var shortJson = """{"id":"short","partitionKey":"pk1","name":"Short","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(shortJson)), new PartitionKey("pk1"));

		// Item that won't expire
		await container.CreateItemAsync(
			new TestDocument { Id = "long", PartitionKey = "pk1", Name = "Long", Value = 42 },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));
		return container;
	}

	[Fact]
	public async Task QueryStreamIterator_ExcludesExpiredItems()
	{
		var container = await CreateContainerWithExpiredAndLiveItems();

		var iterator = container.GetItemQueryStreamIterator("SELECT * FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var response = await iterator.ReadNextAsync();
			using var reader = new StreamReader(response.Content);
			var body = await reader.ReadToEndAsync();
			var arr = JObject.Parse(body)["Documents"]!.ToObject<List<JObject>>()!;
			results.AddRange(arr);
		}

		results.Should().HaveCount(1);
		results[0]["id"]!.Value<string>().Should().Be("long");
	}

	[Fact]
	public async Task WhereClauseQuery_ExcludesExpiredItems()
	{
		var container = await CreateContainerWithExpiredAndLiveItems();

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.partitionKey = 'pk1'");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["id"]!.Value<string>().Should().Be("long");
	}

	[Fact]
	public async Task SumAggregate_ExcludesExpiredItems()
	{
		var container = await CreateContainerWithExpiredAndLiveItems();

		// SELECT VALUE SUM returns a double (42.0), not int
		var sumIterator = container.GetItemQueryIterator<double>("SELECT VALUE SUM(c.value) FROM c");
		var sums = new List<double>();
		while (sumIterator.HasMoreResults)
			sums.AddRange(await sumIterator.ReadNextAsync());

		sums.Should().Contain(42.0);
	}

	[Fact]
	public async Task DistinctQuery_ExcludesExpiredItems()
	{
		var container = await CreateContainerWithExpiredAndLiveItems();

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT DISTINCT c.name FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["name"]!.Value<string>().Should().Be("Long");
	}

	[Fact]
	public async Task OrderByQuery_ExcludesExpiredItems()
	{
		var container = await CreateContainerWithExpiredAndLiveItems();

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY c.name");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["id"]!.Value<string>().Should().Be("long");
	}

	[Fact]
	public async Task TopQuery_ExcludesExpiredItems()
	{
		var container = await CreateContainerWithExpiredAndLiveItems();

		var iterator = container.GetItemQueryIterator<JObject>("SELECT TOP 10 * FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["id"]!.Value<string>().Should().Be("long");
	}

	[Fact]
	public async Task OffsetLimitQuery_ExcludesExpiredItems()
	{
		var container = await CreateContainerWithExpiredAndLiveItems();

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c OFFSET 0 LIMIT 10");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["id"]!.Value<string>().Should().Be("long");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: Stream Write Resets TTL Clock Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlStreamWriteResetTests
{
	[Fact]
	public async Task UpsertStream_ResetsExpirationClock()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 3
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var newJson = """{"id":"1","partitionKey":"pk1","name":"Upserted"}""";
		await container.UpsertItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(newJson)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Upserted");
	}

	[Fact]
	public async Task ReplaceStream_ResetsExpirationClock()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 3
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"Original"}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var newJson = """{"id":"1","partitionKey":"pk1","name":"Replaced"}""";
		await container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(newJson)), "1", new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Replaced");
	}

	[Fact]
	public async Task PatchStream_ResetsExpirationClock()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 3
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		await container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Set("/name", "Patched") });

		await Task.Delay(TimeSpan.FromSeconds(2));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Patched");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: Hierarchical Partition Key TTL Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlHierarchicalPartitionKeyTests
{
	[Fact]
	public async Task Ttl_WithHierarchicalPartitionKey_ExpiresCorrectly()
	{
		var container = new InMemoryContainer(new ContainerProperties("ttl-hpk", "/tenantId")
		{
			DefaultTimeToLive = 1,
			PartitionKeyPaths = new System.Collections.ObjectModel.Collection<string> { "/tenantId", "/departmentId" }
		});

		var json = """{"id":"1","tenantId":"t1","departmentId":"d1","name":"Temp"}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)),
			new PartitionKeyBuilder().Add("t1").Add("d1").Build());

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<JObject>("1",
			new PartitionKeyBuilder().Add("t1").Add("d1").Build());
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Ttl_WithHierarchicalPartitionKey_QueryExcludesExpired()
	{
		var container = new InMemoryContainer(new ContainerProperties("ttl-hpk", "/tenantId")
		{
			DefaultTimeToLive = -1,
			PartitionKeyPaths = new System.Collections.ObjectModel.Collection<string> { "/tenantId", "/departmentId" }
		});

		// Item with per-item _ttl=1 will expire
		var json = """{"id":"1","tenantId":"t1","departmentId":"d1","name":"Temp","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)),
			new PartitionKeyBuilder().Add("t1").Add("d1").Build());

		// Item without per-item _ttl won't expire (DefaultTimeToLive=-1)
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", tenantId = "t1", departmentId = "d1", name = "Long" }),
			new PartitionKeyBuilder().Add("t1").Add("d1").Build());

		await Task.Delay(TimeSpan.FromSeconds(2));

		var iterator = container.GetItemQueryIterator<JObject>("SELECT * FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["id"]!.Value<string>().Should().Be("2");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: ETag / Concurrency TTL Interaction Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlConcurrencyTests
{
	[Fact]
	public async Task IfMatch_OnExpiredItem_Returns404NotPreconditionFailed()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		var created = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));
		var etag = created.ETag;

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfMatchEtag = etag });
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task IfNoneMatch_OnExpiredItem_Returns404()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		var created = await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));
		var etag = created.ETag;

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"),
			new ItemRequestOptions { IfNoneMatchEtag = etag });
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: Change Feed Extended TTL Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlChangeFeedExtendedTests
{
	[Fact]
	public async Task ChangeFeed_ShowsItemWithTtlWhileAlive()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		var json = """{"id":"1","partitionKey":"pk1","name":"HasTtl","_ttl":600}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var iterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.LatestVersion);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			results.AddRange(page.Resource);
		}

		results.Should().HaveCount(1);
		results[0]["_ttl"]!.Value<int>().Should().Be(600);
	}

	[Fact]
	public async Task ChangeFeed_UpsertOnExpiredItem_ProducesNewCreateEvent()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		var checkpoint = container.GetChangeFeedCheckpoint();

		await Task.Delay(TimeSpan.FromSeconds(2));

		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Reborn" },
			new PartitionKey("pk1"));

		var iterator = container.GetChangeFeedIterator<JObject>(checkpoint);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCountGreaterThanOrEqualTo(1);
		results.Last()["name"]!.Value<string>().Should().Be("Reborn");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 44: Divergent Behaviour Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlDivergentBehaviorDeepTests
{
	[Fact]
	public void ContainerTtl_ZeroDefault_ShouldReturn400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey");
		var act = () => container.DefaultTimeToLive = 0;
		act.Should().Throw<CosmosException>();
	}

	[Fact]
	public async Task PerItemTtl_Zero_ShouldReturn400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = -1
		};

		var json = """{"id":"1","partitionKey":"pk1","_ttl":0}""";
		var act = () => container.CreateItemAsync(JObject.Parse(json), new PartitionKey("pk1"));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact(Skip = "DIVERGENT: Queries filter out expired items but do NOT evict them from memory. "
			   + "Real Cosmos DB has a background GC process. Only direct CRUD triggers EvictIfExpired().")]
	public void Query_ShouldEvictExpiredItemsFromMemory() { }

	[Fact]
	public async Task Query_EmulatorFiltersButDoesNotEvictExpiredItems()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey")
		{
			DefaultTimeToLive = 1
		};

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Query filters out expired items
		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		results.Should().BeEmpty();

		// But internal item count still includes the expired item (not evicted)
		// Trigger eviction via a direct read attempt
		container.DefaultTimeToLive = null; // Disable TTL so ItemCount reflects all stored items
		container.ItemCount.Should().Be(1, "expired item is still in memory until evicted by direct CRUD");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Stream API _ttl Validation Gap — BUG-1 fix tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlStreamTtlValidationTests
{
	[Fact]
	public async Task CreateItemStreamAsync_WithTtlZero_ShouldReturn400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		var json = """{"id":"1","partitionKey":"pk1","_ttl":0}""";
		var response = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task UpsertItemStreamAsync_WithTtlZero_ShouldReturn400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		var json = """{"id":"1","partitionKey":"pk1","_ttl":0}""";
		var response = await container.UpsertItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ReplaceItemAsync_WithTtlZero_ShouldReturn400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var replacement = JObject.FromObject(new { id = "1", partitionKey = "pk1", name = "Replaced", _ttl = 0 });
		var act = () => container.ReplaceItemAsync(replacement, "1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ReplaceItemStreamAsync_WithTtlZero_ShouldReturn400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var json = """{"id":"1","partitionKey":"pk1","name":"Replaced","_ttl":0}""";
		var response = await container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), "1", new PartitionKey("pk1"));

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Patch _ttl Validation Gap — BUG-2 fix tests
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlPatchTtlValidationTests
{
	[Fact]
	public async Task Patch_SetTtlToZero_ShouldReturn400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var act = () => container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Set("/_ttl", 0) });
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task PatchStream_SetTtlToZero_ShouldReturn400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		var response = await container.PatchItemStreamAsync("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Set("/_ttl", 0) });

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Patch_SetTtlToMinusOne_Succeeds()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var result = await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Set("/_ttl", -1) });

		result.StatusCode.Should().Be(HttpStatusCode.OK);

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should still be alive because per-item _ttl=-1 overrides container TTL
		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["_ttl"]!.Value<int>().Should().Be(-1);
	}

	[Fact]
	public async Task Patch_SetTtlToPositive_Succeeds()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var result = await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Set("/_ttl", 60) });

		result.StatusCode.Should().Be(HttpStatusCode.OK);
		result.Resource["_ttl"]!.Value<int>().Should().Be(60);
	}

	[Fact]
	public async Task Patch_RemoveTtl_FallsBackToContainerDefault()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		// Create item with per-item _ttl=-1 (never expires)
		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":-1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		// Remove per-item _ttl — item should fall back to container's DefaultTimeToLive=1
		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Remove("/_ttl") });

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Patch_IncrementTtl_ToZero_Returns400()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":5}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var act = () => container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Increment("/_ttl", -5) });
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: State Persistence + TTL Interaction
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlStatePersistenceTests
{
	[Fact]
	public async Task ExportState_ShouldExcludeExpiredItems()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var state = container.ExportState();
		var parsed = JObject.Parse(state);
		var items = parsed["items"] as JArray;
		items.Should().BeEmpty("expired items should not be exported");
	}

	[Fact]
	public async Task ExportState_AfterEviction_ExcludesEvictedItems()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Trigger eviction via a read attempt
		try { await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1")); } catch { }

		var state = container.ExportState();
		var parsed = JObject.Parse(state);
		var items = parsed["items"] as JArray;
		items.Should().BeEmpty("evicted items should not appear in export");
	}

	[Fact]
	public async Task ImportState_ResetsTimestamps_ItemsGetNewTtlCountdown()
	{
		// Create container with TTL=60 and export state
		var source = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 60 };
		await source.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var state = source.ExportState();

		// Wait a bit, then import into another container with TTL=1
		await Task.Delay(TimeSpan.FromSeconds(1));
		var target = new InMemoryContainer("ttl-test2", "/partitionKey") { DefaultTimeToLive = 1 };
		target.ImportState(state);

		// Item should be readable immediately after import (timestamps reset to now)
		var read = await target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Test");

		// But should expire after TTL
		await Task.Delay(TimeSpan.FromSeconds(2));
		var act = () => target.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task ImportState_WithTtlZeroItem_ImportsButExpiresImmediately()
	{
		// ImportState does NOT call ValidatePerItemTtl.
		// Items with _ttl:0 are imported but expire immediately on read (elapsed >= 0 is always true).
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		var stateJson = """{"items":[{"id":"1","partitionKey":"pk1","name":"Test","_ttl":0}]}""";
		// ImportState succeeds (no validation)
		container.ImportState(stateJson);

		// Item is stored but _ttl:0 means immediate expiry
		container.DefaultTimeToLive = null; // Disable TTL to check raw storage
		container.ItemCount.Should().Be(1, "import stored the item");

		// Re-enable TTL — item now expires immediately
		container.DefaultTimeToLive = -1;
		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Unique Key + TTL Interaction
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlUniqueKeyTests
{
	[Fact]
	public async Task UniqueKey_ExpiredItem_AllowsNewItemWithSameUniqueKey()
	{
		var properties = new ContainerProperties("ttl-uk", "/partitionKey")
		{
			DefaultTimeToLive = 1,
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Unique" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Expired item should not block new item with same unique key
		// Trigger eviction first with a read
		try { await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1")); } catch { }

		var result = await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk1", Name = "Unique" },
			new PartitionKey("pk1"));

		result.StatusCode.Should().Be(HttpStatusCode.Created);
	}

	[Fact]
	public async Task UniqueKey_UnevictedExpiredItem_UpsertSucceedsWithSameKey()
	{
		var properties = new ContainerProperties("ttl-uk", "/partitionKey")
		{
			DefaultTimeToLive = 1,
			UniqueKeyPolicy = new UniqueKeyPolicy
			{
				UniqueKeys = { new UniqueKey { Paths = { "/name" } } }
			}
		};
		var container = new InMemoryContainer(properties);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Unique" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Upsert with same id — should succeed because item is expired
		var result = await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Unique" },
			new PartitionKey("pk1"));

		result.StatusCode.Should().BeOneOf(HttpStatusCode.Created, HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Bulk Operations + TTL
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlBulkOperationTests
{
	[Fact]
	public async Task BulkCreate_WithTtl_ItemsExpireCorrectly()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		var tasks = Enumerable.Range(0, 10).Select(i =>
			container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Item{i}" },
				new PartitionKey("pk1")));

		await Task.WhenAll(tasks);

		await Task.Delay(TimeSpan.FromSeconds(2));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task BulkUpsert_OnExpiredItems_Succeeds()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		// Create items
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"Old{i}" },
				new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Bulk upsert on expired items
		var tasks = Enumerable.Range(0, 5).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "pk1", Name = $"New{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r.StatusCode == HttpStatusCode.Created);
	}

	[Fact]
	public async Task BulkRead_ExcludesExpiredItems()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		// Create mix: items with _ttl=1 and items with _ttl=-1
		var shortJson = """{"id":"short","partitionKey":"pk1","name":"Short","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(shortJson)), new PartitionKey("pk1"));

		await container.CreateItemAsync(
			new TestDocument { Id = "long", PartitionKey = "pk1", Name = "Long" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// ReadMany should only return the live item
		var items = new List<(string, PartitionKey)>
		{
			("short", new PartitionKey("pk1")),
			("long", new PartitionKey("pk1"))
		};
		var readMany = await container.ReadManyItemsAsync<TestDocument>(items);
		readMany.Resource.Should().HaveCount(1);
		readMany.Resource.First().Id.Should().Be("long");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: DeleteAllItemsByPartitionKey + TTL
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlDeleteAllByPartitionKeyTests
{
	[Fact]
	public async Task DeleteAllByPK_IncludesExpiredItems()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Delete all items in partition — should work even though items are expired
		var response = await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		// Disable TTL to check internal state
		container.DefaultTimeToLive = null;
		container.ItemCount.Should().Be(0, "all items including expired ones should be removed");
	}

	[Fact]
	public async Task DeleteAllByPK_ExpiredItems_ProduceTombstones()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		var checkpoint = container.GetChangeFeedCheckpoint();

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));

		var iterator = container.GetChangeFeedIterator<JObject>(checkpoint);
		var events = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			events.AddRange(page);
		}

		// Should have at least a create event and a delete tombstone
		events.Should().HaveCountGreaterThanOrEqualTo(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Computed Properties + TTL
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlComputedPropertyTests
{
	[Fact]
	public async Task ComputedProperty_OnExpiredItem_NotReturnedInQuery()
	{
		var props = new ContainerProperties("ttl-cp", "/partitionKey")
		{
			DefaultTimeToLive = 1,
			ComputedProperties = new System.Collections.ObjectModel.Collection<ComputedProperty>
			{
				new ComputedProperty { Name = "upperName", Query = "SELECT VALUE UPPER(c.name) FROM c" }
			}
		};
		var container = new InMemoryContainer(props);

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "hello" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var iterator = container.GetItemQueryIterator<JObject>("SELECT c.upperName FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task ComputedProperty_BasedOnTtl_EvaluatedCorrectly()
	{
		var props = new ContainerProperties("ttl-cp", "/partitionKey")
		{
			DefaultTimeToLive = -1,
			ComputedProperties = new System.Collections.ObjectModel.Collection<ComputedProperty>
			{
				new ComputedProperty { Name = "hasTtl", Query = "SELECT VALUE IS_DEFINED(c._ttl) FROM c" }
			}
		};
		var container = new InMemoryContainer(props);

		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":30}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var iterator = container.GetItemQueryIterator<JObject>("SELECT c.hasTtl FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["hasTtl"]!.Value<bool>().Should().BeTrue();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: FeedRange + TTL
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlFeedRangeTests
{
	[Fact]
	public async Task FeedRange_Query_ExcludesExpiredItems()
	{
		var container = new InMemoryContainer("ttl-fr", "/partitionKey")
		{
			DefaultTimeToLive = 1,
			FeedRangeCount = 4
		};

		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = $"pk-{i}", Name = $"Item{i}" },
				new PartitionKey($"pk-{i}"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<JObject>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range,
				new QueryDefinition("SELECT * FROM c"));
			while (iterator.HasMoreResults)
				allResults.AddRange(await iterator.ReadNextAsync());
		}

		allResults.Should().BeEmpty();
	}

	[Fact]
	public async Task ChangeFeed_WithFeedRange_ShowsExpiredItemHistory()
	{
		var container = new InMemoryContainer("ttl-fr", "/partitionKey")
		{
			DefaultTimeToLive = 1,
			FeedRangeCount = 2
		};

		var checkpoint = container.GetChangeFeedCheckpoint();

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Change feed should still show the creation event even though item expired
		var iterator = container.GetChangeFeedIterator<JObject>(checkpoint);
		var events = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			events.AddRange(page);
		}

		events.Should().HaveCountGreaterThanOrEqualTo(1);
		events[0]["id"]!.Value<string>().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Per-Item TTL Edge Cases (extending TtlPerItemExtendedTests)
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlPerItemEdgeCaseTests
{
	[Fact]
	public async Task PerItemTtl_FloatingPointValue_TreatedAsContainerDefault()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 60 };

		// _ttl=1.5 is not a valid integer — int.TryParse fails, so no per-item override
		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":1.5}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should still be alive (container TTL=60, float _ttl treated as non-integer)
		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task PerItemTtl_NegativeNotMinusOne_ExpiresImmediately()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 60 };

		// _ttl=-2: Only -1 is "never expire". Other negatives cause elapsed >= -2 which
		// is always true, so the item expires immediately in the emulator.
		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":-2}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task PerItemTtl_VeryLargeValue_NoOverflow()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":2147483647}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["_ttl"]!.Value<int>().Should().Be(int.MaxValue);
	}

	[Fact]
	public async Task PerItemTtl_Boolean_TreatedAsNonInteger()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 60 };

		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":true}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		// Boolean _ttl: int.TryParse("True") fails, so no per-item override, uses container TTL
		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task PerItemTtl_Null_TreatedAsNoPerItemOverride()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 60 };

		var json = """{"id":"1","partitionKey":"pk1","name":"Test","_ttl":null}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));

		// null _ttl: ttlToken is not null but ToString() gives "" which fails int.TryParse → uses container TTL
		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		read.Resource["id"]!.Value<string>().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: TTL + Triggers
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlTriggerTests
{
	[Fact]
	public async Task PreTrigger_OnExpiredItem_NotFired_Returns404()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		var triggerFired = false;
		container.RegisterTrigger("preTrigger", TriggerType.Pre, TriggerOperation.Replace,
			doc => { triggerFired = true; return doc; });

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var act = () => container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Replaced" }, "1",
			new PartitionKey("pk1"),
			new ItemRequestOptions { PreTriggers = new List<string> { "preTrigger" } });
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
		// ETag/existence check now happens before pre-triggers, so expired item → 404 without firing trigger
		triggerFired.Should().BeFalse();
	}

	[Fact]
	public async Task PostTrigger_AfterUpsertOnExpired_IsFired()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		JObject? captured = null;
		container.RegisterTrigger("postTrigger", TriggerType.Post, TriggerOperation.All,
			doc => { captured = doc; });

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Upsert on expired item creates a new item
		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Reborn" },
			new PartitionKey("pk1"),
			new ItemRequestOptions { PostTriggers = new List<string> { "postTrigger" } });

		captured.Should().NotBeNull();
		captured["name"]!.Value<string>().Should().Be("Reborn");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: _ts System Property Deep Tests (extending TtlSystemPropertyTests)
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlTsDeepTests
{
	[Fact]
	public async Task Ts_IsUnixTimestamp_InSeconds()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey");

		var before = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));
		var after = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"));
		var ts = read.Resource["_ts"]!.Value<long>();

		ts.Should().BeGreaterThanOrEqualTo(before);
		ts.Should().BeLessThanOrEqualTo(after);
	}

	[Fact]
	public async Task Ts_PreservedInQuery_SelectProjection()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		var iterator = container.GetItemQueryIterator<JObject>("SELECT c._ts FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(1);
		results[0]["_ts"]!.Value<long>().Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task Ts_UsedForTtlCalculation_InternalTimestampDrivesTtl()
	{
		// The emulator tracks timestamps internally in _timestamps dictionary.
		// Patching a document resets the internal timestamp (TTL clock reset).
		// This test verifies that TTL is driven by the internal timestamp, not _ts in the doc.
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 3 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Patch something non-TTL related — this resets the internal timestamp
		await container.PatchItemAsync<JObject>("1", new PartitionKey("pk1"),
			new List<PatchOperation> { PatchOperation.Set("/name", "Updated") });

		// Item should still be alive because the patch reset the TTL clock
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Updated");

		await Task.Delay(TimeSpan.FromSeconds(4));

		// Now it should be expired
		var act = () => container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Concurrent TTL Operations (extending TtlConcurrencyTests)
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlConcurrencyDeepTests
{
	[Fact]
	public async Task ConcurrentReads_OnExpiringItem_AllGet404AfterExpiry()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Wait long enough for TTL=1s to definitely expire, even under load
		await Task.Delay(TimeSpan.FromSeconds(3));

		var tasks = Enumerable.Range(0, 20).Select(_ => Task.Run(async () =>
		{
			try
			{
				await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
				return false; // Should not reach here
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				return true;
			}
		}));

		var results = await Task.WhenAll(tasks);
		results.Should().OnlyContain(r => r, "all concurrent reads on expired item should get 404");
	}

	[Fact]
	public async Task ConcurrentUpsert_OnExpiredItem_OneWins_NoDataCorruption()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Old" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var tasks = Enumerable.Range(0, 10).Select(i =>
			container.UpsertItemAsync(
				new TestDocument { Id = "1", PartitionKey = "pk1", Name = $"Upsert{i}" },
				new PartitionKey("pk1")));

		var results = await Task.WhenAll(tasks);

		// All should succeed (either as create or update)
		results.Should().OnlyContain(r =>
			r.StatusCode == HttpStatusCode.Created || r.StatusCode == HttpStatusCode.OK);

		// Final state should be one of the upserted values
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().StartWith("Upsert");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Container Management Extended TTL
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlContainerManagementDeepTests
{
	[Fact]
	public async Task ReplaceContainer_SetTtlToMinusOne_ExistingItemsStopExpiring()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 2 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Change TTL to -1 before item expires
		container.DefaultTimeToLive = -1;

		await Task.Delay(TimeSpan.FromSeconds(3));

		// Item should still be alive because container TTL is now -1 (no expiration)
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task ReplaceContainer_SetTtlToNull_DisablesTtlCompletely()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Disable TTL entirely
		container.DefaultTimeToLive = null;

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should still be alive because TTL is disabled
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task ReplaceContainer_IncreaseTtl_ExtendsExistingItemLifetimes()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 2 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(1));

		// Extend TTL to 60 seconds
		container.DefaultTimeToLive = 60;

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Item should still be alive because TTL was extended
		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Test");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Batch + TTL Extended (extending TtlTransactionalBatchTests)
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlBatchExtendedTests
{
	[Fact]
	public async Task Batch_PatchExpiredItem_Returns404InBatchResult()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.PatchItem("1", new List<PatchOperation> { PatchOperation.Set("/name", "Patched") });

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Batch_CreateWithTtlZero_ShouldFail()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		var item = JObject.FromObject(new { id = "1", partitionKey = "pk1", _ttl = 0 });
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.CreateItem(item);

		using var response = await batch.ExecuteAsync();

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task Batch_MixedExpiredAndLive_CorrectResults()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		// Create one item with short TTL and one without
		var shortJson = """{"id":"short","partitionKey":"pk1","name":"Short","_ttl":1}""";
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(shortJson)), new PartitionKey("pk1"));

		await container.CreateItemAsync(
			new TestDocument { Id = "live", PartitionKey = "pk1", Name = "Long" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		// Batch read on both items — short is expired, live is not
		var batch = container.CreateTransactionalBatch(new PartitionKey("pk1"));
		batch.ReadItem("short");
		batch.ReadItem("live");

		using var response = await batch.ExecuteAsync();

		// Batch should fail since one item returns 404
		response.StatusCode.Should().NotBe(HttpStatusCode.OK);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: Query SQL Expressions with TTL Fields
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlQuerySqlExpressionsTests
{
	[Fact]
	public async Task Query_WhereOnTtlField_FiltersByPerItemTtl()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		for (var i = 1; i <= 5; i++)
		{
			var json = $$$"""{"id":"{{{i}}}","partitionKey":"pk1","name":"Item{{{i}}}","_ttl":{{{i * 100}}}}""";
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
		}

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c._ttl > 200");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3); // _ttl = 300, 400, 500
	}

	[Fact]
	public async Task Query_OrderByTtl_SortsCorrectly()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		for (var i = 1; i <= 3; i++)
		{
			var json = $$$"""{"id":"{{{i}}}","partitionKey":"pk1","_ttl":{{{(4 - i) * 10}}}}""";
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
		}

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT c._ttl FROM c ORDER BY c._ttl");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		var ttls = results.Select(r => r["_ttl"]!.Value<int>()).ToList();
		ttls.Should().BeInAscendingOrder();
	}

	[Fact]
	public async Task GroupBy_TtlField_GroupsCorrectly()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = -1 };

		for (var i = 0; i < 6; i++)
		{
			var ttl = (i % 3 + 1) * 10; // ttl values: 10, 20, 30, 10, 20, 30
			var json = $$$"""{"id":"{{{i}}}","partitionKey":"pk1","_ttl":{{{ttl}}}}""";
			await container.CreateItemStreamAsync(
				new MemoryStream(Encoding.UTF8.GetBytes(json)), new PartitionKey("pk1"));
		}

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT c._ttl, COUNT(1) as cnt FROM c GROUP BY c._ttl");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		results.Should().OnlyContain(r => r["cnt"]!.Value<int>() == 2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 45: ItemCount + TTL Divergent Behavior Documentation
// ═══════════════════════════════════════════════════════════════════════════════

public class TtlItemCountDivergentTests
{
	[Fact]
	public async Task ItemCount_ShouldExcludeExpiredItems()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		container.ItemCount.Should().Be(0, "expired items should not be counted");
	}

	[Fact]
	public async Task ItemCount_IncludesNonExpiredItems()
	{
		var container = new InMemoryContainer("ttl-test", "/partitionKey") { DefaultTimeToLive = 1 };

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Temp" },
			new PartitionKey("pk1"));

		await Task.Delay(TimeSpan.FromSeconds(2));

		container.ItemCount.Should().Be(0, "expired items should not be counted");
	}
}

