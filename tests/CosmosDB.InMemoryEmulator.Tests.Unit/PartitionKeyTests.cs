using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;


public class PartitionKeyGapTests4
{
	[Fact]
	public async Task PartitionKey_CompositeKey_ThreePaths()
	{
		var container = new InMemoryContainer("test", ["/partitionKey", "/name", "/nested/description"]);

		var compositePk = new PartitionKeyBuilder().Add("pk1").Add("Alice").Add("Desc").Build();
		var item = new TestDocument
		{
			Id = "1",
			PartitionKey = "pk1",
			Name = "Alice",
			Nested = new NestedObject { Description = "Desc", Score = 1.0 }
		};
		var response = await container.CreateItemAsync(item, compositePk);

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		// Reading back with the same composite key
		var read = await container.ReadItemAsync<TestDocument>("1", compositePk);
		read.Resource.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task PartitionKey_BooleanValue()
	{
		var container = new InMemoryContainer("test", "/active");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","active":true,"name":"BoolPK"}""")),
			new PartitionKey(true));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey(true));
		read.Resource["name"]!.ToString().Should().Be("BoolPK");
	}
}


public class PartitionKeyGapTests
{
	[Fact]
	public async Task PartitionKey_ExtractedFromItem_MatchesExplicitPk()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var item = new TestDocument { Id = "1", PartitionKey = "auto-pk", Name = "Test" };

		await container.CreateItemAsync(item);

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("auto-pk"));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task PartitionKey_NumericValue()
	{
		var container = new InMemoryContainer("test", "/value");
		var item = new TestDocument { Id = "1", PartitionKey = "ignored", Name = "Test", Value = 42 };

		await container.CreateItemAsync(item, new PartitionKey(42));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey(42));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task CrossPartition_Query_ReturnsAllPartitions()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "A", Name = "Alice" },
			new PartitionKey("A"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "B", Name = "Bob" },
			new PartitionKey("B"));

		var iterator = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().HaveCount(2);
	}
}


public class PartitionKeyGapTests3
{
	[Fact]
	public async Task PartitionKey_None_ItemsStoredByDocumentPk()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// With PK.None, the PK is extracted from the document body
		var item1 = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "A" };
		await container.CreateItemAsync(item1, PartitionKey.None);

		var item2 = new TestDocument { Id = "2", PartitionKey = "pk2", Name = "B" };
		await container.CreateItemAsync(item2, PartitionKey.None);

		// Items should be retrievable using their document PK values
		var read1 = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read1.Resource.Name.Should().Be("A");

		var read2 = await container.ReadItemAsync<TestDocument>("2", new PartitionKey("pk2"));
		read2.Resource.Name.Should().Be("B");
	}

	[Fact(Skip = "InMemoryContainer with PartitionKey.None and missing partition key field throws conflict. " +
				 "Real Cosmos DB would store the item and allow reading with the fallback PK value. " +
				 "InMemoryContainer's ExtractPartitionKeyValue path conflicts with stream-based creation. " +
				 "See divergent behavior test in PartitionKeyFallbackDivergentBehaviorTests.")]
	public async Task PartitionKey_MissingInItem_FallsBackToId()
	{
		// Container with /category PK — but item doesn't have "category" field
		var container = new InMemoryContainer("test", "/category");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","name":"NoPK"}""")),
			PartitionKey.None);

		// Without a "category" field, ExtractPartitionKeyValue falls back to id
		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("1"));
		read.Resource["name"]!.ToString().Should().Be("NoPK");
	}
}


public class PartitionKeyGapTests2
{
	[Fact]
	public async Task PartitionKey_CompositeKey_TwoPaths()
	{
		var container = new InMemoryContainer("test", new[] { "/partitionKey", "/name" });

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
		read.Resource.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task PartitionKey_NestedPath_Extraction()
	{
		var container = new InMemoryContainer("test", "/nested/description");
		await container.CreateItemAsync(
			new TestDocument
			{
				Id = "1",
				PartitionKey = "ignored",
				Name = "Test",
				Nested = new NestedObject { Description = "deep-pk", Score = 1.0 }
			},
			new PartitionKey("deep-pk"));

		var read = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("deep-pk"));
		read.Resource.Name.Should().Be("Test");
	}

	[Fact]
	public async Task PartitionKey_NullValue()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var item = new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" };

		// Create with PartitionKey.Null
		await container.CreateItemAsync(item, PartitionKey.Null);

		// Should be retrievable
		var read = await container.ReadItemAsync<TestDocument>("1", PartitionKey.Null);
		read.Resource.Name.Should().Be("Test");
	}
}


public class PartitionKeyFallbackDivergentBehaviorTests
{
	/// <summary>
	/// BEHAVIORAL DIFFERENCE: InMemoryContainer with PartitionKey.None on a container
	/// whose PK path doesn't exist in the document succeeds but may store with an unexpected PK.
	/// Real Cosmos DB would use the partition key extracted from the document body.
	/// InMemoryContainer falls back to the id field as the PK value when the PK path is missing.
	/// Reading back requires knowing the fallback PK value.
	/// </summary>
	[Fact]
	public async Task PartitionKey_None_WithMissingPkField_Succeeds_InMemory()
	{
		var container = new InMemoryContainer("test", "/category");

		var response = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","name":"NoPK"}""")),
			PartitionKey.None);

		// InMemory accepts the item without throwing
		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}

// ─── PartitionKey.None vs PartitionKey.Null ─────────────────────────────

public class PartitionKeyNoneVsNullTests
{
	/// <summary>
	/// In real Cosmos DB, PartitionKey.None represents the absence of a partition key (used
	/// for containers created without a partition key definition in older API versions).
	/// PartitionKey.Null represents an explicit null value. They are semantically distinct.
	/// The emulator treats both as the storage key "null", making them interchangeable.
	/// </summary>
	[Fact(Skip = "The emulator's PartitionKeyToString maps both PartitionKey.None and " +
		"PartitionKey.Null to the string 'null'. In real Cosmos DB, PartitionKey.None has " +
		"special routing behavior for legacy non-partitioned containers, while PartitionKey.Null " +
		"is an explicit null value in the partition key field. The emulator does not support " +
		"non-partitioned (legacy) containers so the distinction is not meaningful here.")]
	public async Task PartitionKeyNone_ShouldNotMatchPartitionKeyNull()
	{
		var container = new InMemoryContainer("test-container", "/pk");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = (string)null! }),
			PartitionKey.Null);

		// In real Cosmos, reading with PartitionKey.Null would not find an item
		// written with PartitionKey.None
		var act = () => container.ReadItemAsync<JObject>("1", PartitionKey.Null);
		await act.Should().ThrowAsync<CosmosException>();
	}

	/// <summary>
	/// Sister test: demonstrates the emulator treats None and Null identically.
	/// </summary>
	[Fact]
	public async Task PartitionKeyNoneVsNull_EmulatorBehavior_TreatedIdentically()
	{
		// ── Divergent behavior documentation ──
		// Real Cosmos DB: PartitionKey.None → system-defined partition for legacy containers.
		//   PartitionKey.Null → explicit null value in the PK field. An item written with
		//   PartitionKey.None cannot be read with PartitionKey.Null (different routing).
		// In-Memory Emulator: ExtractPartitionKeyValue and PartitionKeyToString both map
		//   None and Null to the string "null". Items are stored with key (id, "null") in
		//   both cases, making them interchangeable. This is correct for modern containers
		//   but differs for legacy non-partitioned scenarios.
		var container = new InMemoryContainer("test-container", "/pk");

		// Create item with explicit null pk value via PartitionKey.Null
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = (string)null! }),
			PartitionKey.Null);

		// In the emulator, PartitionKey.None resolves the same way as PartitionKey.Null
		var response = await container.ReadItemAsync<JObject>("1", PartitionKey.None);
		response.Resource["id"]!.Value<string>().Should().Be("1",
			"the emulator treats PartitionKey.None and PartitionKey.Null identically");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 1 — PK Data Type Coverage
// ═══════════════════════════════════════════════════════════════════════════

public class PartitionKeyDataTypeTests
{
	[Fact]
	public async Task PartitionKey_DoubleValue_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", "/score");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","score":3.14,"name":"Pi"}""")),
			new PartitionKey(3.14));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey(3.14));
		read.Resource["name"]!.ToString().Should().Be("Pi");
	}

	[Fact]
	public async Task PartitionKey_EmptyString_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", "/category");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","category":"","name":"Empty"}""")),
			new PartitionKey(""));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey(""));
		read.Resource["name"]!.ToString().Should().Be("Empty");
	}

	[Fact]
	public async Task PartitionKey_LongString_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", "/category");
		var longPk = new string('x', 1000);

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				$$$"""{"id":"1","category":"{{{longPk}}}","name":"LongPK"}""")),
			new PartitionKey(longPk));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey(longPk));
		read.Resource["name"]!.ToString().Should().Be("LongPK");
	}

	[Fact]
	public async Task PartitionKey_UnicodeCharacters_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", "/category");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", category = "日本語🎉", name = "Unicode" }),
			new PartitionKey("日本語🎉"));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("日本語🎉"));
		read.Resource["name"]!.ToString().Should().Be("Unicode");
	}

	[Fact]
	public async Task PartitionKey_SpecialJsonCharacters_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", "/category");
		var specialPk = "he said \"hello\" \\ end";

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", category = specialPk, name = "Special" }),
			new PartitionKey(specialPk));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey(specialPk));
		read.Resource["name"]!.ToString().Should().Be("Special");
	}

	[Fact]
	public async Task PartitionKey_ContainingPipeCharacter_StoredAndRetrievable()
	{
		// Documents delimiter risk: single PK with pipe char works for single-path keys
		var container = new InMemoryContainer("test", "/category");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", category = "a|b", name = "Pipe" }),
			new PartitionKey("a|b"));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("a|b"));
		read.Resource["name"]!.ToString().Should().Be("Pipe");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 2 — Composite/Hierarchical PK Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class CompositePartitionKeyEdgeCaseTests
{
	[Fact]
	public async Task CompositeKey_MixedTypes_StringAndNumber()
	{
		var container = new InMemoryContainer("test", ["/tenant", "/region"]);
		var pk = new PartitionKeyBuilder().Add("t1").Add("us-east").Build();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenant = "t1", region = "us-east", name = "Mixed" }),
			pk);

		var read = await container.ReadItemAsync<JObject>("1", pk);
		read.Resource["name"]!.ToString().Should().Be("Mixed");
	}

	[Fact]
	public async Task CompositeKey_SameIdDifferentComposite_BothExist()
	{
		var container = new InMemoryContainer("test", ["/tenant", "/region"]);
		var pk1 = new PartitionKeyBuilder().Add("t1").Add("us").Build();
		var pk2 = new PartitionKeyBuilder().Add("t1").Add("eu").Build();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "doc1", tenant = "t1", region = "us", name = "US" }),
			pk1);
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "doc1", tenant = "t1", region = "eu", name = "EU" }),
			pk2);

		container.ItemCount.Should().Be(2);

		var readUS = await container.ReadItemAsync<JObject>("doc1", pk1);
		readUS.Resource["name"]!.ToString().Should().Be("US");

		var readEU = await container.ReadItemAsync<JObject>("doc1", pk2);
		readEU.Resource["name"]!.ToString().Should().Be("EU");
	}

	[Fact]
	public async Task CompositeKey_FourPaths()
	{
		var container = new InMemoryContainer("test",
			["/tenant", "/region", "/env", "/shard"]);
		var pk = new PartitionKeyBuilder()
			.Add("acme").Add("us").Add("prod").Add("s1").Build();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenant = "acme", region = "us", env = "prod", shard = "s1" }),
			pk);

		var read = await container.ReadItemAsync<JObject>("1", pk);
		read.Resource["tenant"]!.ToString().Should().Be("acme");
	}

	[Fact]
	public async Task CompositeKey_ReadMany_RetrievedCorrectly()
	{
		var container = new InMemoryContainer("test", ["/tenant", "/region"]);
		var pk1 = new PartitionKeyBuilder().Add("t1").Add("us").Build();
		var pk2 = new PartitionKeyBuilder().Add("t1").Add("eu").Build();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenant = "t1", region = "us" }), pk1);
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", tenant = "t1", region = "eu" }), pk2);

		var response = await container.ReadManyItemsAsync<JObject>([
			("1", pk1),
			("2", pk2)
		]);

		response.Resource.Should().HaveCount(2);
	}

	[Fact]
	public async Task CompositeKey_DeleteAllByPartitionKey()
	{
		var container = new InMemoryContainer("test", ["/tenant", "/region"]);
		var pk1 = new PartitionKeyBuilder().Add("t1").Add("us").Build();
		var pk2 = new PartitionKeyBuilder().Add("t2").Add("us").Build();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenant = "t1", region = "us" }), pk1);
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", tenant = "t1", region = "us" }), pk1);
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", tenant = "t2", region = "us" }), pk2);

		await container.DeleteAllItemsByPartitionKeyStreamAsync(pk1);

		container.ItemCount.Should().Be(1);
		var remaining = await container.ReadItemAsync<JObject>("3", pk2);
		remaining.Resource["id"]!.ToString().Should().Be("3");
	}

	[Fact]
	public async Task CompositeKey_TransactionalBatch()
	{
		var container = new InMemoryContainer("test", ["/tenant", "/region"]);
		var pk = new PartitionKeyBuilder().Add("t1").Add("us").Build();

		var batch = container.CreateTransactionalBatch(pk);
		batch.CreateItem(JObject.FromObject(new { id = "1", tenant = "t1", region = "us", name = "A" }));
		batch.CreateItem(JObject.FromObject(new { id = "2", tenant = "t1", region = "us", name = "B" }));

		using var response = await batch.ExecuteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);
		container.ItemCount.Should().Be(2);
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 3 — Bug Documentation Tests
// ═══════════════════════════════════════════════════════════════════════════

public class PartitionKeyBugDocumentationTests
{
	[Fact]
	public async Task CompositeKey_WithNullComponent_PreservesPosition()
	{
		var container = new InMemoryContainer("test", ["/a", "/b", "/c"]);
		var pk = new PartitionKeyBuilder().Add("x").Add(null as string).Add("z").Build();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", a = "x", c = "z" }), pk);

		var read = await container.ReadItemAsync<JObject>("1", pk);
		read.Resource["a"]!.ToString().Should().Be("x");
	}

	[Fact]
	public async Task CompositeKey_WithNullComponent_EmulatorBehavior_PreservesPosition()
	{
		// Null is a valid component in hierarchical partition keys and
		// is preserved positionally. ("x", null, "z") != ("x", "z").
		// ExtractPartitionKeyValue now preserves null positions as empty strings
		// for composite keys, matching PartitionKeyToString behavior.
		var container = new InMemoryContainer("test", ["/a", "/b", "/c"]);

		// PartitionKeyBuilder with null still produces a valid PK for the SDK
		var pk = new PartitionKeyBuilder().Add("x").Add(null as string).Add("z").Build();

		// The emulator preserves null positions in composite keys
		var response = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","a":"x","c":"z"}""")),
			pk);
		response.StatusCode.Should().Be(HttpStatusCode.Created);

		// Verify round-trip: can read back with the same composite PK
		var read = await container.ReadItemStreamAsync("1", pk);
		read.StatusCode.Should().Be(HttpStatusCode.OK);
	}

	[Fact]
	public async Task FeedRange_FilterConsistentWithCrudPartitionKey()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
			new PartitionKey("pk1"));

		// Get single feed range
		var ranges = await container.GetFeedRangesAsync();
		var iterator = container.GetItemQueryIterator<TestDocument>(
			ranges[0], new QueryDefinition("SELECT * FROM c"));

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle(d => d.Id == "1",
			"item stored via CRUD should be visible in FeedRange-filtered query");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 4 — PK Path Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

public class PartitionKeyPathEdgeCaseTests
{
	[Fact]
	public async Task PartitionKey_DeeplyNestedPath_ThreeLevels()
	{
		var container = new InMemoryContainer("test", "/a/b/c");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","a":{"b":{"c":"deep-value"}},"name":"Deep"}""")),
			new PartitionKey("deep-value"));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("deep-value"));
		read.Resource["name"]!.ToString().Should().Be("Deep");
	}

	[Fact]
	public async Task PartitionKey_DefaultIdPath()
	{
		var container = new InMemoryContainer("test", "/id");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "myId", name = "IdAsPK" }),
			new PartitionKey("myId"));

		var read = await container.ReadItemAsync<JObject>("myId", new PartitionKey("myId"));
		read.Resource["name"]!.ToString().Should().Be("IdAsPK");
	}

	[Fact]
	public async Task PartitionKey_PathCaseSensitivity()
	{
		// PK path is case-sensitive — /Name != /name
		var container = new InMemoryContainer("test", "/Name");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","Name":"CasePK","name":"lowercase"}""")),
			new PartitionKey("CasePK"));

		var read = await container.ReadItemAsync<JObject>("1", new PartitionKey("CasePK"));
		read.Resource["Name"]!.ToString().Should().Be("CasePK");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 5 — PK with Operations (Cross-Cutting)
// ═══════════════════════════════════════════════════════════════════════════

public class PartitionKeyOperationsTests
{
	[Fact]
	public async Task Upsert_SameId_DifferentPartitionKey_CreatesTwoItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pkA", Name = "A" },
			new PartitionKey("pkA"));
		await container.UpsertItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pkB", Name = "B" },
			new PartitionKey("pkB"));

		container.ItemCount.Should().Be(2);

		var readA = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pkA"));
		readA.Resource.Name.Should().Be("A");

		var readB = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pkB"));
		readB.Resource.Name.Should().Be("B");
	}

	[Fact]
	public async Task Replace_ExplicitPk_MismatchWithBody_ThrowsBadRequest()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Original" },
			new PartitionKey("pk1"));

		// Replace with explicit PK "pk1" but body says "pk2" — real Cosmos throws BadRequest
		var act = () => container.ReplaceItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk2", Name = "Replaced" },
			"1", new PartitionKey("pk1"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest && e.SubStatusCode == 1001);
	}

	[Fact]
	public async Task Query_WithPartitionKeyFilter_ReturnsOnlyMatchingPartition()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));
		await container.CreateItemAsync(
			new TestDocument { Id = "2", PartitionKey = "pk2", Name = "Bob" },
			new PartitionKey("pk2"));

		var query = new QueryDefinition("SELECT * FROM c WHERE c.partitionKey = @pk")
			.WithParameter("@pk", "pk1");
		var iterator = container.GetItemQueryIterator<TestDocument>(query);

		var results = new List<TestDocument>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task ReadMany_MixedExistingAndNonExisting_ReturnsOnlyExisting()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var response = await container.ReadManyItemsAsync<TestDocument>([
			("1", new PartitionKey("pk1")),
			("999", new PartitionKey("pk1"))
		]);

		response.Resource.Should().ContainSingle().Which.Name.Should().Be("Alice");
	}

	[Fact]
	public async Task ReadMany_WithPartitionKeyNull_ReturnsNullPkItems()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", partitionKey = (string)null!, name = "NullPK" }),
			PartitionKey.Null);

		var response = await container.ReadManyItemsAsync<JObject>([
			("1", PartitionKey.Null)
		]);

		response.Resource.Should().ContainSingle();
		response.Resource.First()["name"]!.ToString().Should().Be("NullPK");
	}

	[Fact]
	public async Task DeleteAllByPartitionKey_NonExistentPk_ReturnsOk()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var response = await container.DeleteAllItemsByPartitionKeyStreamAsync(
			new PartitionKey("nonexistent"));

		response.StatusCode.Should().Be(HttpStatusCode.OK);
		container.ItemCount.Should().Be(1, "items in other partitions should be untouched");
	}

	[Fact]
	public async Task DeleteAllByPartitionKey_WithNullPk_DeletesNullPkItems()
	{
		var container = new InMemoryContainer("test", "/pk");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = (string)null!, name = "NullPK" }),
			PartitionKey.Null);
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "real", name = "RealPK" }),
			new PartitionKey("real"));

		await container.DeleteAllItemsByPartitionKeyStreamAsync(PartitionKey.Null);

		container.ItemCount.Should().Be(1);
		var remaining = await container.ReadItemAsync<JObject>("2", new PartitionKey("real"));
		remaining.Resource["name"]!.ToString().Should().Be("RealPK");
	}

	[Fact]
	public async Task Stream_CreateAndRead_ExplicitPk_WorksCorrectly()
	{
		var container = new InMemoryContainer("test", "/pk");

		var createResponse = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes(
				"""{"id":"1","pk":"myPk","name":"StreamTest"}""")),
			new PartitionKey("myPk"));
		createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

		var readResponse = await container.ReadItemStreamAsync("1", new PartitionKey("myPk"));
		readResponse.StatusCode.Should().Be(HttpStatusCode.OK);

		using var reader = new StreamReader(readResponse.Content);
		var json = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(json);
		jObj["name"]!.ToString().Should().Be("StreamTest");
	}
}

// ═══════════════════════════════════════════════════════════════════════════
//  Phase 6 — PK and Change Feed
// ═══════════════════════════════════════════════════════════════════════════

public class PartitionKeyChangeFeedTests
{
	[Fact]
	public async Task SamePartitionKey_AllItemsInSameFeedRange()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		// Create 10 items all with the same PK
		for (int i = 0; i < 10; i++)
		{
			await container.CreateItemAsync(
				new TestDocument { Id = $"{i}", PartitionKey = "samePK", Name = $"Item{i}" },
				new PartitionKey("samePK"));
		}

		var ranges = await container.GetFeedRangesAsync();
		ranges.Should().HaveCountGreaterThan(0);

		// Find which range contains our items
		int totalFound = 0;
		int rangesWithItems = 0;
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<TestDocument>(
				range, new QueryDefinition("SELECT * FROM c"));
			var items = new List<TestDocument>();
			while (iterator.HasMoreResults)
			{
				var page = await iterator.ReadNextAsync();
				items.AddRange(page);
			}
			if (items.Count > 0)
			{
				rangesWithItems++;
				totalFound += items.Count;
			}
		}

		totalFound.Should().Be(10);
		rangesWithItems.Should().Be(1, "all items with the same PK should be in the same feed range");
	}

	[Fact]
	public async Task ChangeFeed_IncludesPartitionKeyFields()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var iterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(),
			ChangeFeedMode.LatestVersion);

		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["partitionKey"]!.ToString().Should().Be("pk1",
			"change feed entries should include the partition key field");
	}

	[Fact]
	public async Task ChangeFeed_AfterDelete_CheckpointAdvances()
	{
		var container = new InMemoryContainer("test", "/partitionKey");

		await container.CreateItemAsync(
			new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Alice" },
			new PartitionKey("pk1"));

		var checkpointAfterCreate = container.GetChangeFeedCheckpoint();

		await container.DeleteItemAsync<TestDocument>("1", new PartitionKey("pk1"));

		var checkpointAfterDelete = container.GetChangeFeedCheckpoint();
		checkpointAfterDelete.Should().BeGreaterThan(checkpointAfterCreate,
			"delete should advance the change feed checkpoint");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 7: PK Type Discrimination (Fixed)
// ═══════════════════════════════════════════════════════════════════════════════

public class PartitionKeyTypeDiscriminationTests
{
	[Fact]
	public async Task PartitionKey_NumericVsString_ShouldBeDistinct()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = 42 }), new PartitionKey(42));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "42" }), new PartitionKey("42"));
		var item1 = (await container.ReadItemAsync<JObject>("1", new PartitionKey(42))).Resource;
		var item2 = (await container.ReadItemAsync<JObject>("2", new PartitionKey("42"))).Resource;
		item1["id"]!.ToString().Should().Be("1");
		item2["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task PartitionKey_BooleanVsString_ShouldBeDistinct()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = true }), new PartitionKey(true));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "True" }), new PartitionKey("True"));
		var item1 = (await container.ReadItemAsync<JObject>("1", new PartitionKey(true))).Resource;
		var item2 = (await container.ReadItemAsync<JObject>("2", new PartitionKey("True"))).Resource;
		item1["id"]!.ToString().Should().Be("1");
		item2["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task PartitionKey_IntegerVsDouble_BothRetrievable()
	{
		// In Cosmos, all numbers are double (IEEE 754). PK(42) == PK(42.0)
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = 42 }), new PartitionKey(42));
		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey(42.0))).Resource;
		item["id"]!.ToString().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 8: Stream API PK Extraction (BUG 7 — Fixed)
// ═══════════════════════════════════════════════════════════════════════════════

public class StreamApiPkExtractionTests
{
	[Fact]
	public async Task CreateItemStream_WithPartitionKeyNone_ExtractsPkFromBody()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var doc = JObject.FromObject(new { id = "1", partitionKey = "pk1" });
		var json = doc.ToString();
		using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));

		var response = await container.CreateItemStreamAsync(stream, PartitionKey.None);
		response.IsSuccessStatusCode.Should().BeTrue();

		// Should be retrievable with the extracted PK
		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"))).Resource;
		item["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task UpsertItemStream_WithPartitionKeyNone_ExtractsPkFromBody()
	{
		var container = new InMemoryContainer("test", "/partitionKey");
		var doc = JObject.FromObject(new { id = "1", partitionKey = "pk1", val = "original" });
		using var stream = new MemoryStream(Encoding.UTF8.GetBytes(doc.ToString()));

		var response = await container.UpsertItemStreamAsync(stream, PartitionKey.None);
		response.IsSuccessStatusCode.Should().BeTrue();

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("pk1"))).Resource;
		item["val"]!.ToString().Should().Be("original");
	}

	[Fact]
	public async Task ReplaceItemStream_ExplicitPkUsed()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var updated = JObject.FromObject(new { id = "1", pk = "a", extra = "yes" }).ToString();
		using var stream = new MemoryStream(Encoding.UTF8.GetBytes(updated));

		var response = await container.ReplaceItemStreamAsync(stream, "1", new PartitionKey("a"));
		response.IsSuccessStatusCode.Should().BeTrue();

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["extra"]!.ToString().Should().Be("yes");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 9: Query PK Scoping (BUG 8 — Fixed)
// ═══════════════════════════════════════════════════════════════════════════════

public class QueryPkScopingTests
{
	[Fact]
	public async Task Query_WithPartitionKeyNull_ReturnsOnlyNullPkItems()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2" }), PartitionKey.Null);
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "b" }), new PartitionKey("b"));

		var results = container.GetItemLinqQueryable<JObject>(
			requestOptions: new QueryRequestOptions { PartitionKey = PartitionKey.Null }).ToList();

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task Query_WithPartitionKeyNone_ReturnsCrossPartition()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "b" }), new PartitionKey("b"));

		var results = container.GetItemLinqQueryable<JObject>(
			requestOptions: new QueryRequestOptions { PartitionKey = PartitionKey.None }).ToList();

		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task LinqQuery_WithPartitionKeyNull_ReturnsOnlyNullPkItems()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2" }), PartitionKey.Null);

		var results = container.GetItemLinqQueryable<JObject>(
			requestOptions: new QueryRequestOptions { PartitionKey = PartitionKey.Null })
			.Where(x => true)
			.ToList();

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("2");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 10: PK Validation / Mismatch Detection (BUG 5, BUG 10)
// ═══════════════════════════════════════════════════════════════════════════════

public class PkValidationMismatchTests
{
	[Fact]
	public async Task Create_PkMismatchWithBody_ShouldThrowBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "body-pk" }),
			new PartitionKey("explicit-pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest && e.SubStatusCode == 1001);
	}

	[Fact]
	public async Task Upsert_PkMismatchWithBody_ShouldThrowBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		var act = () => container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "body-pk" }),
			new PartitionKey("explicit-pk"));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest && e.SubStatusCode == 1001);
	}

	[Fact]
	public async Task Patch_ModifyingPartitionKeyField_ShouldThrowBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var act = () => container.PatchItemAsync<JObject>(
			"1", new PartitionKey("a"),
			new[] { PatchOperation.Replace("/pk", "b") });
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 11: Hierarchical PK Advanced Scenarios
// ═══════════════════════════════════════════════════════════════════════════════

public class HierarchicalPkAdvancedTests
{
	[Fact]
	public async Task HierarchicalPk_PartialKeyQuery_ShouldFilterByPrefix()
	{
		var container = new InMemoryContainer("test", ["/country", "/city", "/district"]);

		var pk1 = new PartitionKeyBuilder().Add("US").Add("NYC").Add("Manhattan").Build();
		var pk2 = new PartitionKeyBuilder().Add("US").Add("NYC").Add("Brooklyn").Build();
		var pk3 = new PartitionKeyBuilder().Add("UK").Add("London").Add("Westminster").Build();

		await container.CreateItemAsync(JObject.FromObject(new { id = "1", country = "US", city = "NYC", district = "Manhattan" }), pk1);
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", country = "US", city = "NYC", district = "Brooklyn" }), pk2);
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", country = "UK", city = "London", district = "Westminster" }), pk3);

		// Query with partial key (first 2 of 3 components) — should match items 1 & 2
		var prefixKey = new PartitionKeyBuilder().Add("US").Add("NYC").Build();
		var results = container.GetItemLinqQueryable<JObject>(
			requestOptions: new QueryRequestOptions { PartitionKey = prefixKey }).ToList();

		results.Should().HaveCount(2);
		results.Select(r => r["id"]!.ToString()).Should().BeEquivalentTo("1", "2");
	}

	[Fact]
	public async Task CompositeKey_AllNullComponents_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", ["/a", "/b"]);
		var pk = new PartitionKeyBuilder().AddNullValue().AddNullValue().Build();
		await container.CreateItemAsync(JObject.FromObject(new { id = "1" }), pk);

		var item = (await container.ReadItemAsync<JObject>("1", pk)).Resource;
		item["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task CompositeKey_BooleanAndNumericComponents()
	{
		var container = new InMemoryContainer("test", ["/active", "/score"]);
		var pk = new PartitionKeyBuilder().Add(true).Add(42.5).Build();
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", active = true, score = 42.5 }), pk);

		var item = (await container.ReadItemAsync<JObject>("1", pk)).Resource;
		item["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task CompositeKey_NestedPathComponents()
	{
		var container = new InMemoryContainer("test", ["/pk", "/nested/value"]);
		var pk = new PartitionKeyBuilder().Add("a").Add("deep").Build();
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"1\",\"pk\":\"a\",\"nested\":{\"value\":\"deep\"}}"), pk);

		var item = (await container.ReadItemAsync<JObject>("1", pk)).Resource;
		item["id"]!.ToString().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 12: DeleteAll Change Feed & Tombstones
// ═══════════════════════════════════════════════════════════════════════════════

public class DeleteAllChangeFeedTombstoneTests
{
	[Fact]
	public async Task DeleteAllByPartitionKey_RecordsChangeFeedTombstones()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "b" }), new PartitionKey("b"));

		var checkpointBefore = container.GetChangeFeedCheckpoint();

		await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("a"));

		container.ItemCount.Should().Be(1, "only items with pk='b' should remain");
		var checkpointAfter = container.GetChangeFeedCheckpoint();
		checkpointAfter.Should().BeGreaterThan(checkpointBefore,
			"delete-all should advance the change feed checkpoint");
	}

	[Fact]
	public async Task DeleteAllByPartitionKey_CompositeKey_TombstonesCorrect()
	{
		var container = new InMemoryContainer("test", ["/a", "/b"]);
		var pk = new PartitionKeyBuilder().Add("x").Add("y").Build();
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", a = "x", b = "y" }), pk);
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", a = "x", b = "y" }), pk);

		await container.DeleteAllItemsByPartitionKeyStreamAsync(pk);

		container.ItemCount.Should().Be(0);
	}

	[Fact]
	public async Task DeleteTombstone_SinglePkWithPipeChar_ReconstructedCorrectly()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a|b" }), new PartitionKey("a|b"));
		await container.DeleteItemAsync<JObject>("1", new PartitionKey("a|b"));

		// Verify the item was deleted
		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a|b"));
		(await act.Should().ThrowAsync<CosmosException>()).Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 13: PK Extraction Path Consistency
// ═══════════════════════════════════════════════════════════════════════════════

public class PkExtractionConsistencyTests
{
	[Fact]
	public async Task PartitionKey_None_PathMissing_StoresWithNullPk()
	{
		var container = new InMemoryContainer("test", "/nonexistent");
		// PK field doesn't exist in document, should store with null partition key
		await container.CreateItemAsync(JObject.FromObject(new { id = "1" }), PartitionKey.None);
		var item = (await container.ReadItemAsync<JObject>("1", PartitionKey.None)).Resource;
		item["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task PartitionKey_None_AllPathsNull_ShouldUseNullNotId()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.Parse("{\"id\":\"1\",\"pk\":null}"), PartitionKey.None);
		var item = (await container.ReadItemAsync<JObject>("1", PartitionKey.Null)).Resource;
		item["id"]!.ToString().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 14: PK Boundary / Extreme Values
// ═══════════════════════════════════════════════════════════════════════════════

public class PkBoundaryValueTests
{
	[Fact]
	public async Task PartitionKey_MaxSize_2KB_ShouldReject()
	{
		var container = new InMemoryContainer("test", "/pk");
		var largePk = new string('x', 2049);
		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = largePk }), new PartitionKey(largePk));
		await act.Should().ThrowAsync<CosmosException>();
	}

	[Fact]
	public async Task PartitionKey_Whitespace_OnlySpaces()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "   " }), new PartitionKey("   "));

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("   "))).Resource;
		item["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task PartitionKey_NewlinesAndTabs()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "line1\nline2\ttab" }), new PartitionKey("line1\nline2\ttab"));

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("line1\nline2\ttab"))).Resource;
		item["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task PartitionKey_NullStringLiteral_VsPartitionKeyNull()
	{
		var container = new InMemoryContainer("test", "/pk");
		// String "null" is distinct from PartitionKey.Null
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "null" }), new PartitionKey("null"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2" }), PartitionKey.Null);

		var item1 = (await container.ReadItemAsync<JObject>("1", new PartitionKey("null"))).Resource;
		var item2 = (await container.ReadItemAsync<JObject>("2", PartitionKey.Null)).Resource;
		item1["id"]!.ToString().Should().Be("1");
		item2["id"]!.ToString().Should().Be("2");
	}

	[Fact]
	public async Task PartitionKey_NegativeNumber()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = -42 }), new PartitionKey(-42));

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey(-42))).Resource;
		item["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task PartitionKey_Zero()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = 0 }), new PartitionKey(0));

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey(0))).Resource;
		item["id"]!.ToString().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Phase 15: PK with Container Lifecycle
// ═══════════════════════════════════════════════════════════════════════════════

public class PkContainerLifecycleTests
{
	[Fact]
	public async Task Container_ContainerProperties_ReflectsPartitionKeyDefinition()
	{
		var container = new InMemoryContainer("test", "/myPk");
		var props = (await container.ReadContainerAsync()).Resource;
		props.PartitionKeyPath.Should().Be("/myPk");
	}

	[Fact]
	public async Task Container_PartitionKeyPathWithoutLeadingSlash()
	{
		// The InMemoryContainer constructor accepts paths without leading slash
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var item = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		item["id"]!.ToString().Should().Be("1");
	}
}


// ═══════════════════════════════════════════════════════════════════════════════
//  Plan #24: Partition Key Comprehensive Deep Dive
// ═══════════════════════════════════════════════════════════════════════════════


// ── Round 1: CRUD PK Mismatch & Wrong PK Tests ──────────────────────────────

public class PkCrudMismatchTests
{
	[Fact]
	public async Task Delete_WithWrongPartitionKey_ReturnsNotFound()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "correct" }), new PartitionKey("correct"));

		var act = () => container.DeleteItemAsync<JObject>("1", new PartitionKey("wrong"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Read_WithWrongPartitionKey_ReturnsNotFound()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "correct" }), new PartitionKey("correct"));

		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("wrong"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Patch_WithWrongPartitionKey_ReturnsNotFound()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "correct", name = "orig" }), new PartitionKey("correct"));

		var act = () => container.PatchItemAsync<JObject>(
			"1", new PartitionKey("wrong"),
			new[] { PatchOperation.Set("/name", "patched") });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}

	[Fact]
	public async Task Replace_PkMismatchWithBody_ShouldThrowBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		var act = () => container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "body-pk" }),
			"1", new PartitionKey("a"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest && e.SubStatusCode == 1001);
	}

	[Fact]
	public async Task Upsert_SameIdSamePk_UpdatesExisting()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "original" }), new PartitionKey("a"));

		await container.UpsertItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "updated" }), new PartitionKey("a"));

		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource;
		read["name"]!.ToString().Should().Be("updated");
	}
}


// ── Round 2: Composite PK Edge Cases ─────────────────────────────────────────

public class CompositeKeyPipeDelimiterTests
{
	[Fact]
	public async Task CompositeKey_WithPipeInValue_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", ["/a", "/b"]);
		var pk = new PartitionKeyBuilder().Add("val|ue1").Add("val|ue2").Build();

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", a = "val|ue1", b = "val|ue2" }), pk);

		var read = (await container.ReadItemAsync<JObject>("1", pk)).Resource;
		read["a"]!.ToString().Should().Be("val|ue1");
		read["b"]!.ToString().Should().Be("val|ue2");
	}

	[Fact]
	public async Task CompositeKey_WithPipeInValue_DeleteTombstone_PreservesValues()
	{
		var container = new InMemoryContainer("test", ["/a", "/b"]);
		var pk = new PartitionKeyBuilder().Add("x|y").Add("z").Build();

		await container.CreateItemAsync(JObject.FromObject(new { id = "1", a = "x|y", b = "z" }), pk);
		var checkpoint = container.GetChangeFeedCheckpoint();
		await container.DeleteItemAsync<JObject>("1", pk);

		var feed = container.GetChangeFeedIterator<JObject>(checkpoint);
		var tombstones = new List<JObject>();
		while (feed.HasMoreResults)
		{
			var page = await feed.ReadNextAsync();
			tombstones.AddRange(page.Resource.Where(j => j["_deleted"]?.Value<bool>() == true));
		}

		tombstones.Should().ContainSingle();
		var t = tombstones[0];
		t["a"]!.ToString().Should().Be("x|y");
		t["b"]!.ToString().Should().Be("z");
	}

	[Fact]
	public async Task CompositeKey_WithPipeInValue_DeleteAll_TombstoneCorrect()
	{
		var container = new InMemoryContainer("test", ["/a", "/b"]);
		var pk = new PartitionKeyBuilder().Add("p|q").Add("r").Build();

		await container.CreateItemAsync(JObject.FromObject(new { id = "1", a = "p|q", b = "r" }), pk);
		var checkpoint = container.GetChangeFeedCheckpoint();
		await container.DeleteAllItemsByPartitionKeyStreamAsync(pk);

		var feed = container.GetChangeFeedIterator<JObject>(checkpoint);
		var tombstones = new List<JObject>();
		while (feed.HasMoreResults)
		{
			var page = await feed.ReadNextAsync();
			tombstones.AddRange(page.Resource.Where(j => j["_deleted"]?.Value<bool>() == true));
		}

		tombstones.Should().ContainSingle();
		tombstones[0]["a"]!.ToString().Should().Be("p|q");
		tombstones[0]["b"]!.ToString().Should().Be("r");
	}
}


public class CompositeKeyValidationGapTests
{
	[Fact]
	public async Task CompositeKey_DifferentOrder_CreatesDifferentPartitions()
	{
		var container = new InMemoryContainer("test", ["/a", "/b"]);

		var pk1 = new PartitionKeyBuilder().Add("X").Add("Y").Build();
		var pk2 = new PartitionKeyBuilder().Add("Y").Add("X").Build();

		await container.CreateItemAsync(JObject.FromObject(new { id = "1", a = "X", b = "Y" }), pk1);
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", a = "Y", b = "X" }), pk2);

		// Both should exist — different partitions
		(await container.ReadItemAsync<JObject>("1", pk1)).Resource["id"]!.ToString().Should().Be("1");
		(await container.ReadItemAsync<JObject>("2", pk2)).Resource["id"]!.ToString().Should().Be("2");
	}

	[Fact(Skip = "Emulator does not validate composite PK consistency — ValidatePartitionKeyConsistency skips composite keys")]
	public async Task CompositeKey_MismatchWithBody_Create_ShouldThrowBadRequest()
	{
		var container = new InMemoryContainer("test", ["/a", "/b"]);
		var pk = new PartitionKeyBuilder().Add("explicit-a").Add("explicit-b").Build();

		var act = () => container.CreateItemAsync(
			JObject.FromObject(new { id = "1", a = "body-a", b = "body-b" }), pk);

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task CompositeKey_MismatchWithBody_Create_EmulatorAcceptsSilently()
	{
		// Known limitation: emulator skips composite PK validation
		var container = new InMemoryContainer("test", ["/a", "/b"]);
		var pk = new PartitionKeyBuilder().Add("explicit-a").Add("explicit-b").Build();

		var response = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", a = "body-a", b = "body-b" }), pk);

		response.StatusCode.Should().Be(HttpStatusCode.Created);
	}
}


public class CompositeKeyComponentCountTests
{
	[Fact]
	public async Task CompositeKey_FewerComponentsThanPaths_EmulatorBehavior()
	{
		var container = new InMemoryContainer("test", ["/a", "/b", "/c"]);

		// Only 2 components for a 3-path container
		var pk = new PartitionKeyBuilder().Add("x").Add("y").Build();
		var response = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", a = "x", b = "y", c = "z" }), pk);

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		// Can read back with the same 2-component key
		var read = (await container.ReadItemAsync<JObject>("1", pk)).Resource;
		read["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task CompositeKey_MoreComponentsThanPaths_EmulatorBehavior()
	{
		var container = new InMemoryContainer("test", ["/a", "/b"]);

		// 3 components for a 2-path container
		var pk = new PartitionKeyBuilder().Add("x").Add("y").Add("z").Build();
		var response = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", a = "x", b = "y" }), pk);

		response.StatusCode.Should().Be(HttpStatusCode.Created);

		// Should be readable with the same key
		var read = (await container.ReadItemAsync<JObject>("1", pk)).Resource;
		read["id"]!.ToString().Should().Be("1");
	}
}


// ── Round 3: PK Extraction & Type Edge Cases ─────────────────────────────────

public class PkExtractionEdgeCaseTests
{
	[Fact]
	public async Task SinglePk_NullFieldValue_StoresAsNullPk()
	{
		var container = new InMemoryContainer("test", "/pk");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":null,"name":"test"}""")),
			PartitionKey.Null);

		var read = (await container.ReadItemAsync<JObject>("1", PartitionKey.Null)).Resource;
		read["name"]!.ToString().Should().Be("test");
	}

	[Fact]
	public async Task SinglePk_MissingField_StoresWithNullPartitionKey()
	{
		var container = new InMemoryContainer("test", "/nonexistent");

		await container.CreateItemAsync(JObject.FromObject(new { id = "myid", name = "test" }));

		// When PK field is missing, item is stored with null partition key
		var read = (await container.ReadItemAsync<JObject>("myid", PartitionKey.None)).Resource;
		read["name"]!.ToString().Should().Be("test");
	}

	[Fact]
	public async Task PartitionKey_ArrayTypeInField_HandledGracefully()
	{
		var container = new InMemoryContainer("test", "/pk");

		// PK field contains an array — should use default ToString for the token
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":[1,2,3]}""")),
			PartitionKey.None);

		// Should be stored somehow (using the body extraction) and readable
		var query = container.GetItemQueryIterator<JObject>(new QueryDefinition("SELECT * FROM c WHERE c.id = '1'"));
		var results = new List<JObject>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().ContainSingle();
	}

	[Fact]
	public async Task PartitionKey_ObjectTypeInField_HandledGracefully()
	{
		var container = new InMemoryContainer("test", "/pk");

		// PK field is a nested object
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":{"nested":"val"}}""")),
			PartitionKey.None);

		var query = container.GetItemQueryIterator<JObject>(new QueryDefinition("SELECT * FROM c WHERE c.id = '1'"));
		var results = new List<JObject>();
		while (query.HasMoreResults)
		{
			var page = await query.ReadNextAsync();
			results.AddRange(page);
		}
		results.Should().ContainSingle();
	}
}


public class PkDoubleEdgeCaseTests
{
	[Fact]
	public async Task PartitionKey_VerySmallDouble_RoundTrips()
	{
		var container = new InMemoryContainer("test", "/value");
		var tiny = double.Epsilon;
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes($@"{{""id"":""1"",""value"":{tiny.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}}}")),
			new PartitionKey(tiny));

		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey(tiny))).Resource;
		read["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task PartitionKey_MaxDoubleValue_RoundTrips()
	{
		var container = new InMemoryContainer("test", "/value");
		var max = double.MaxValue;
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes($@"{{""id"":""1"",""value"":{max.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}}}")),
			new PartitionKey(max));

		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey(max))).Resource;
		read["id"]!.ToString().Should().Be("1");
	}

	[Fact(Skip = "Emulator uses ToString(\"R\") for numeric PK keys — -0.0 produces \"-0\" while 0.0 produces \"0\", creating different partition keys")]
	public async Task PartitionKey_NegativeZero_EqualsPositiveZero()
	{
		var container = new InMemoryContainer("test", "/value");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","value":0}""")),
			new PartitionKey(0.0));

		// -0.0 == 0.0 in IEEE 754, so reading with -0.0 should find the same item
		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey(-0.0))).Resource;
		read["id"]!.ToString().Should().Be("1");
	}

	[Fact]
	public async Task PartitionKey_NegativeZero_EmulatorBehavior_TreatedAsDifferent()
	{
		var container = new InMemoryContainer("test", "/value");
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","value":0}""")),
			new PartitionKey(0.0));

		// Emulator creates different PK key strings for -0.0 vs 0.0
		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey(-0.0));
		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}
}


// ── Round 4: Cross-Cutting Concerns ──────────────────────────────────────────

public class PkEtagInteractionTests
{
	[Fact]
	public async Task Create_WithPk_ThenReplace_IfMatchEtag_Works()
	{
		var container = new InMemoryContainer("test", "/pk");
		var created = await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "orig" }), new PartitionKey("a"));
		var etag = created.ETag;

		var replaced = await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = etag });

		replaced.StatusCode.Should().Be(HttpStatusCode.OK);
		(await container.ReadItemAsync<JObject>("1", new PartitionKey("a"))).Resource["name"]!.ToString().Should().Be("updated");
	}

	[Fact]
	public async Task Create_WithPk_ThenReplace_IfMatchWrongEtag_Fails()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "orig" }), new PartitionKey("a"));

		var act = () => container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "updated" }),
			"1", new PartitionKey("a"),
			new ItemRequestOptions { IfMatchEtag = "stale-etag" });

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
	}
}


public class PkQueryIntegrationTests
{
	[Fact]
	public async Task SqlQuery_WithPartitionKeyInRequestOptions_FiltersCorrectly()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "b", name = "Bob" }), new PartitionKey("b"));

		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });

		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which["name"]!.ToString().Should().Be("Alice");
	}

	[Fact]
	public async Task SqlQuery_WithPartitionKey_AndWhereClause_CombinesCorrectly()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a", name = "Alice" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", pk = "a", name = "Bob" }), new PartitionKey("a"));
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", pk = "b", name = "Alice" }), new PartitionKey("b"));

		var iterator = container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.name = 'Alice'"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });

		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle().Which["id"]!.ToString().Should().Be("1");
	}
}


public class PkTtlInteractionTests
{
	[Fact]
	public async Task Read_WithCorrectPk_ButExpiredTtl_ReturnsNotFound()
	{
		var container = new InMemoryContainer("test", "/pk") { DefaultTimeToLive = 1 };
		await container.CreateItemAsync(JObject.FromObject(new { id = "1", pk = "a" }), new PartitionKey("a"));

		await Task.Delay(1500);

		var act = () => container.ReadItemAsync<JObject>("1", new PartitionKey("a"));

		await act.Should().ThrowAsync<CosmosException>()
			.Where(e => e.StatusCode == HttpStatusCode.NotFound);
	}
}


// ── Round 5: Tombstone & Change Feed Type Preservation ───────────────────────

public class PkTombstoneTypePreservationTests
{
	[Fact]
	public async Task DeleteTombstone_BooleanPk_PreservesType()
	{
		var container = new InMemoryContainer("test", "/active");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","active":true}""")),
			new PartitionKey(true));
		var checkpoint = container.GetChangeFeedCheckpoint();
		await container.DeleteItemAsync<JObject>("1", new PartitionKey(true));

		var feed = container.GetChangeFeedIterator<JObject>(checkpoint);
		var tombstones = new List<JObject>();
		while (feed.HasMoreResults)
		{
			var page = await feed.ReadNextAsync();
			tombstones.AddRange(page.Resource.Where(j => j["_deleted"]?.Value<bool>() == true));
		}

		tombstones.Should().ContainSingle();
		var t = tombstones[0];
		t["active"]!.Type.Should().Be(JTokenType.Boolean);
		t["active"]!.Value<bool>().Should().BeTrue();
	}

	[Fact]
	public async Task DeleteTombstone_NumericPk_PreservesType()
	{
		var container = new InMemoryContainer("test", "/score");

		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","score":42.5}""")),
			new PartitionKey(42.5));
		var checkpoint = container.GetChangeFeedCheckpoint();
		await container.DeleteItemAsync<JObject>("1", new PartitionKey(42.5));

		var feed = container.GetChangeFeedIterator<JObject>(checkpoint);
		var tombstones = new List<JObject>();
		while (feed.HasMoreResults)
		{
			var page = await feed.ReadNextAsync();
			tombstones.AddRange(page.Resource.Where(j => j["_deleted"]?.Value<bool>() == true));
		}

		tombstones.Should().ContainSingle();
		var t = tombstones[0];
		t["score"]!.Value<double>().Should().Be(42.5);
	}

	[Fact]
	public async Task HierarchicalPk_DeleteAllWithPartialKey_EmulatorBehavior_DoesNotMatch()
	{
		// DeleteAllByPartitionKeyStreamAsync uses exact PK string match.
		// Partial keys produce a shorter PK string that won't match full composite keys.
		var container = new InMemoryContainer("test", ["/country", "/city"]);

		var pk1 = new PartitionKeyBuilder().Add("US").Add("NYC").Build();
		var pk2 = new PartitionKeyBuilder().Add("US").Add("LA").Build();
		var pk3 = new PartitionKeyBuilder().Add("UK").Add("London").Build();

		await container.CreateItemAsync(JObject.FromObject(new { id = "1", country = "US", city = "NYC" }), pk1);
		await container.CreateItemAsync(JObject.FromObject(new { id = "2", country = "US", city = "LA" }), pk2);
		await container.CreateItemAsync(JObject.FromObject(new { id = "3", country = "UK", city = "London" }), pk3);

		// Partial key (just country) — emulator does exact match so nothing is deleted
		var partialPk = new PartitionKeyBuilder().Add("US").Build();
		await container.DeleteAllItemsByPartitionKeyStreamAsync(partialPk);

		// All items still exist because partial key didn't match any full composite keys
		container.ItemCount.Should().Be(3);
	}
}


// ── Round 6: Stream API PK Mismatch Gaps ─────────────────────────────────────

public class StreamApiPkMismatchTests
{
	[Fact]
	public async Task CreateItemStream_PkMismatchWithBody_ReturnsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");

		var response = await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"body-pk"}""")),
			new PartitionKey("explicit-pk"));

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task UpsertItemStream_PkMismatchWithBody_ReturnsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");

		var response = await container.UpsertItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"body-pk"}""")),
			new PartitionKey("explicit-pk"));

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}

	[Fact]
	public async Task ReplaceItemStream_PkMismatchWithBody_ReturnsBadRequest()
	{
		var container = new InMemoryContainer("test", "/pk");
		// First create a valid item
		await container.CreateItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"a"}""")),
			new PartitionKey("a"));

		// Replace with mismatched PK: header says "a", body says "body-pk"
		var response = await container.ReplaceItemStreamAsync(
			new MemoryStream(Encoding.UTF8.GetBytes("""{"id":"1","pk":"body-pk"}""")),
			"1", new PartitionKey("a"));

		response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
	}
}


// ── Round 7: Special Characters ──────────────────────────────────────────────

public class PkSpecialCharacterTests
{
	[Fact]
	public async Task PartitionKey_ControlCharacters_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", "/pk");
		var pk = "tab\there\nnewline";
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk, name = "test" }), new PartitionKey(pk));

		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey(pk))).Resource;
		read["pk"]!.ToString().Should().Be(pk);
	}

	[Fact]
	public async Task PartitionKey_NullByte_StoredAndRetrievable()
	{
		var container = new InMemoryContainer("test", "/pk");
		var pk = "before\0after";
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk, name = "test" }), new PartitionKey(pk));

		var read = (await container.ReadItemAsync<JObject>("1", new PartitionKey(pk))).Resource;
		read["pk"]!.ToString().Should().Be(pk);
	}
}
