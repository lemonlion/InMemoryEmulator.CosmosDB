using AwesomeAssertions;
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class PatchArrayPathResolutionTests
{
	#region Test Models

	public class DocWithItems
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("partitionKey")] public string PartitionKey { get; set; } = "";
		[JsonProperty("items")] public List<ItemEntry> Items { get; set; } = new();
		[JsonProperty("topVal")] public string TopVal { get; set; } = "";
	}

	public class ItemEntry
	{
		[JsonProperty("name")] public string Name { get; set; } = "";
		[JsonProperty("count")] public int Count { get; set; }
		[JsonProperty("val")] public string Val { get; set; } = "";
		[JsonProperty("existingProp")] public string ExistingProp { get; set; } = "";
		[JsonProperty("propToRemove")] public string PropToRemove { get; set; } = "";
		[JsonProperty("children")] public List<string> Children { get; set; } = new();
	}

	public class DocWithDeepNesting
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("partitionKey")] public string PartitionKey { get; set; } = "";
		[JsonProperty("a")] public List<LevelB> A { get; set; } = new();
		[JsonProperty("result")] public string Result { get; set; } = "";
	}

	public class LevelB
	{
		[JsonProperty("b")] public List<LevelC> B { get; set; } = new();
	}

	public class LevelC
	{
		[JsonProperty("c")] public string C { get; set; } = "";
		[JsonProperty("value")] public int Value { get; set; }
		[JsonProperty("newField")] public string NewField { get; set; } = "";
	}

	public class DocWithMatrix
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("partitionKey")] public string PartitionKey { get; set; } = "";
		[JsonProperty("matrix")] public List<List<int>> Matrix { get; set; } = new();
	}

	public class DocWithRootAndNestedTransactions
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("partitionKey")] public string PartitionKey { get; set; } = "";
		[JsonProperty("transactions")] public Dictionary<string, string> Transactions { get; set; } = new();
		[JsonProperty("runs")] public List<RunEntry> Runs { get; set; } = new();
		[JsonProperty("count")] public int Count { get; set; }
	}

	public class RunEntry
	{
		[JsonProperty("status")] public string Status { get; set; } = "";
		[JsonProperty("transactions")] public List<string> Transactions { get; set; } = new();
		[JsonProperty("count")] public int Count { get; set; }
	}

	#endregion

	private static InMemoryContainer CreateContainer() => new("test", "/partitionKey");
	private static PartitionKey PK(string val) => new(val);

	#region 1.1 Set Operations on Array-Nested Paths

	[Fact]
	public async Task Set_PropertyInsideArrayElement_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Name = "original" } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Set("/items/0/name", "updated") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Name.Should().Be("updated");
	}

	[Fact]
	public async Task Set_DeeplyNestedArrayElement_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithDeepNesting
		{
			Id = "1",
			PartitionKey = "pk",
			A = new List<LevelB>
			{
				new() { B = new List<LevelC> { new() { C = "original" }, new() { C = "other" } } }
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithDeepNesting>("1", PK("pk"),
			new[] { PatchOperation.Set("/a/0/b/1/c", "updated") });

		var result = await container.ReadItemAsync<DocWithDeepNesting>("1", PK("pk"));
		result.Resource.A[0].B[0].C.Should().Be("original");
		result.Resource.A[0].B[1].C.Should().Be("updated");
	}

	[Fact]
	public async Task Set_ArrayElementAtVariousIndices_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = Enumerable.Range(0, 6).Select(i => new ItemEntry { Name = $"item{i}" }).ToList()
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[]
			{
				PatchOperation.Set("/items/0/name", "zero"),
				PatchOperation.Set("/items/1/name", "one"),
				PatchOperation.Set("/items/5/name", "five"),
			});

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Name.Should().Be("zero");
		result.Resource.Items[1].Name.Should().Be("one");
		result.Resource.Items[2].Name.Should().Be("item2");
		result.Resource.Items[5].Name.Should().Be("five");
	}

	[Fact]
	public async Task Set_RootAndNestedShareTerminalName_DoesNotCorrupt()
	{
		var container = CreateContainer();
		var doc = new DocWithRootAndNestedTransactions
		{
			Id = "1",
			PartitionKey = "pk",
			Transactions = new Dictionary<string, string> { ["a"] = "1" },
			Runs = new List<RunEntry> { new() { Status = "In", Transactions = new List<string> { "old" } } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithRootAndNestedTransactions>("1", PK("pk"),
			new[]
			{
				PatchOperation.Set("/transactions", new Dictionary<string, string> { ["b"] = "2" }),
				PatchOperation.Set("/runs/0/transactions", new List<string> { "new1", "new2" }),
			});

		var result = await container.ReadItemAsync<DocWithRootAndNestedTransactions>("1", PK("pk"));
		result.Resource.Transactions.Should().HaveCount(1);
		result.Resource.Transactions["b"].Should().Be("2");
		result.Resource.Runs[0].Transactions.Should().BeEquivalentTo(new[] { "new1", "new2" });
	}

	[Fact]
	public async Task Set_MultipleNestedPathsInSamePatch_AllResolveCorrectly()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry>
			{
				new() { Name = "a", Count = 1, Val = "x" },
				new() { Name = "b", Count = 2, Val = "y" },
				new() { Name = "c", Count = 3, Val = "z" },
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[]
			{
				PatchOperation.Set("/items/0/name", "A"),
				PatchOperation.Set("/items/1/val", "Y"),
				PatchOperation.Set("/items/2/count", 30),
			});

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Name.Should().Be("A");
		result.Resource.Items[1].Val.Should().Be("Y");
		result.Resource.Items[2].Count.Should().Be(30);
	}

	[Fact]
	public async Task Set_PropertyOnArrayRootVsInsideElement_BothWork()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Name = "orig" } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		// First set inside an element
		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Set("/items/0/name", "updated") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Name.Should().Be("updated");

		// Then replace the whole array
		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Set("/items", new List<ItemEntry> { new() { Name = "replaced" } }) });

		result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items.Should().HaveCount(1);
		result.Resource.Items[0].Name.Should().Be("replaced");
	}

	#endregion

	#region 1.2 Add Operations on Array-Nested Paths

	[Fact]
	public async Task Add_PropertyToExistingArrayElement_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Name = "item0" } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Add("/items/0/val", "added-value") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Val.Should().Be("added-value");
	}

	[Fact]
	public async Task Add_NewElementToNestedArray_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Name = "item0", Children = new List<string> { "child0" } } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Add("/items/0/children/-", "child1") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Children.Should().BeEquivalentTo(new[] { "child0", "child1" });
	}

	[Fact]
	public async Task Add_DeeplyNestedNewProperty_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithDeepNesting
		{
			Id = "1",
			PartitionKey = "pk",
			A = new List<LevelB>
			{
				new() { B = new List<LevelC> { new() { C = "c0" }, new() { C = "c1" } } }
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithDeepNesting>("1", PK("pk"),
			new[] { PatchOperation.Add("/a/0/b/1/newField", "hello") });

		var result = await container.ReadItemAsync<DocWithDeepNesting>("1", PK("pk"));
		result.Resource.A[0].B[1].NewField.Should().Be("hello");
	}

	#endregion

	#region 1.3 Replace Operations on Array-Nested Paths

	[Fact]
	public async Task Replace_PropertyInsideArrayElement_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { ExistingProp = "original" } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Replace("/items/0/existingProp", "replaced") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].ExistingProp.Should().Be("replaced");
	}

	[Fact]
	public async Task Replace_EntireArrayElement_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Name = "old", Count = 1 } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Replace("/items/0", new ItemEntry { Name = "new", Count = 99 }) });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Name.Should().Be("new");
		result.Resource.Items[0].Count.Should().Be(99);
	}

	#endregion

	#region 1.4 Remove Operations on Array-Nested Paths

	[Fact]
	public async Task Remove_PropertyFromArrayElement_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { PropToRemove = "removeMe" } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Remove("/items/0/propToRemove") });

		// Read raw to verify removal
		var stream = await container.ReadItemStreamAsync("1", PK("pk"));
		using var reader = new System.IO.StreamReader(stream.Content);
		var jObj = JObject.Parse(await reader.ReadToEndAsync());
		jObj.SelectToken("items[0].propToRemove").Should().BeNull();
	}

	[Fact]
	public async Task Remove_DeeplyNestedProperty_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithDeepNesting
		{
			Id = "1",
			PartitionKey = "pk",
			A = new List<LevelB>
			{
				new() { B = new List<LevelC> { new() { C = "original" }, new() { C = "toRemove" } } }
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithDeepNesting>("1", PK("pk"),
			new[] { PatchOperation.Remove("/a/0/b/1/c") });

		var stream = await container.ReadItemStreamAsync("1", PK("pk"));
		using var reader = new System.IO.StreamReader(stream.Content);
		var jObj = JObject.Parse(await reader.ReadToEndAsync());
		jObj.SelectToken("a[0].b[1].c").Should().BeNull();
		jObj.SelectToken("a[0].b[0].c")?.ToString().Should().Be("original");
	}

	#endregion

	#region 1.5 Increment Operations on Array-Nested Paths

	[Fact]
	public async Task Increment_NumberInsideArrayElement_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Count = 10 } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Increment("/items/0/count", 5) });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Count.Should().Be(15);
	}

	[Fact]
	public async Task Increment_DeeplyNestedNumber_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithDeepNesting
		{
			Id = "1",
			PartitionKey = "pk",
			A = new List<LevelB>
			{
				new() { B = new List<LevelC> { new() { Value = 100 }, new() { Value = 200 } } }
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithDeepNesting>("1", PK("pk"),
			new[] { PatchOperation.Increment("/a/0/b/1/value", 50) });

		var result = await container.ReadItemAsync<DocWithDeepNesting>("1", PK("pk"));
		result.Resource.A[0].B[0].Value.Should().Be(100);
		result.Resource.A[0].B[1].Value.Should().Be(250);
	}

	#endregion

	#region 1.6 Move Operations on Array-Nested Paths

	[Fact]
	public async Task Move_FromArrayElementToRoot_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Val = "moveMe" } },
			TopVal = ""
		};
		await container.CreateItemAsync(doc, PK("pk"));

		// Move(from, path) — from=/items/0/val (source), path=/topVal (destination)
		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Move("/items/0/val", "/topVal") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.TopVal.Should().Be("moveMe");
	}

	[Fact]
	public async Task Move_FromRootToArrayElement_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Val = "" } },
			TopVal = "rootValue"
		};
		await container.CreateItemAsync(doc, PK("pk"));

		// Move(from, path) — from=/topVal (source), path=/items/0/val (destination)
		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Move("/topVal", "/items/0/val") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Val.Should().Be("rootValue");
	}

	[Fact]
	public async Task Move_BetweenArrayElements_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry>
			{
				new() { Val = "source" },
				new() { Val = "target" }
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		// Move(from, path) — from=/items/0/val (source), path=/items/1/val (destination)
		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Move("/items/0/val", "/items/1/val") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[1].Val.Should().Be("source");
	}

	[Fact]
	public async Task Move_DeeplyNestedSource_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithDeepNesting
		{
			Id = "1",
			PartitionKey = "pk",
			A = new List<LevelB>
			{
				new() { B = new List<LevelC> { new() { C = "c0" }, new() { C = "deepValue" } } }
			},
			Result = ""
		};
		await container.CreateItemAsync(doc, PK("pk"));

		// Move(from, path) — from=/a/0/b/1/c (source), path=/result (destination)
		await container.PatchItemAsync<DocWithDeepNesting>("1", PK("pk"),
			new[] { PatchOperation.Move("/a/0/b/1/c", "/result") });

		var result = await container.ReadItemAsync<DocWithDeepNesting>("1", PK("pk"));
		result.Resource.Result.Should().Be("deepValue");
	}

	[Fact]
	public async Task Move_DeeplyNestedTarget_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithDeepNesting
		{
			Id = "1",
			PartitionKey = "pk",
			A = new List<LevelB>
			{
				new() { B = new List<LevelC> { new() { C = "c0" }, new() { C = "" } } }
			},
			Result = "moveThis"
		};
		await container.CreateItemAsync(doc, PK("pk"));

		// Move(from, path) — from=/result (source), path=/a/0/b/1/c (destination)
		await container.PatchItemAsync<DocWithDeepNesting>("1", PK("pk"),
			new[] { PatchOperation.Move("/result", "/a/0/b/1/c") });

		var result = await container.ReadItemAsync<DocWithDeepNesting>("1", PK("pk"));
		result.Resource.A[0].B[1].C.Should().Be("moveThis");
	}

	#endregion

	#region 2.1 Path Segment Edge Cases

	[Fact]
	public async Task Path_WithConsecutiveNumericSegments_ResolvesCorrectly()
	{
		var container = CreateContainer();
		var doc = new DocWithMatrix
		{
			Id = "1",
			PartitionKey = "pk",
			Matrix = new List<List<int>> { new() { 10, 20, 30 }, new() { 40, 50, 60 } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithMatrix>("1", PK("pk"),
			new[] { PatchOperation.Set("/matrix/0/1", 99) });

		var result = await container.ReadItemAsync<DocWithMatrix>("1", PK("pk"));
		result.Resource.Matrix[0][0].Should().Be(10);
		result.Resource.Matrix[0][1].Should().Be(99);
		result.Resource.Matrix[0][2].Should().Be(30);
	}

	[Fact]
	public async Task Path_WithLargeArrayIndex_Works()
	{
		var container = CreateContainer();
		var items = Enumerable.Range(0, 100).Select(i => new ItemEntry { Name = $"item{i}" }).ToList();
		var doc = new DocWithItems { Id = "1", PartitionKey = "pk", Items = items };
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Set("/items/99/name", "last") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[99].Name.Should().Be("last");
		result.Resource.Items[0].Name.Should().Be("item0");
	}

	[Fact]
	public async Task Path_WithSingleSegment_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems { Id = "1", PartitionKey = "pk", TopVal = "original" };
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Set("/topVal", "updated") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.TopVal.Should().Be("updated");
	}

	[Fact]
	public async Task Path_WithTwoSegments_OneNumeric_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() { Name = "original" } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Replace("/items/0", new ItemEntry { Name = "replaced" }) });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Name.Should().Be("replaced");
	}

	#endregion

	#region 2.2 Document Shape Variations

	[Fact]
	public async Task Patch_ObjectContainingArrayOfObjects_Set_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry>
			{
				new() { Name = "a", Count = 1 },
				new() { Name = "b", Count = 2 }
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[]
			{
				PatchOperation.Set("/items/0/name", "A"),
				PatchOperation.Set("/items/1/count", 20)
			});

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Name.Should().Be("A");
		result.Resource.Items[1].Count.Should().Be(20);
	}

	[Fact]
	public async Task Patch_ArrayOfArrays_Set_Works()
	{
		var container = CreateContainer();
		var doc = new DocWithMatrix
		{
			Id = "1",
			PartitionKey = "pk",
			Matrix = new List<List<int>> { new() { 1, 2 }, new() { 3, 4 } }
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithMatrix>("1", PK("pk"),
			new[]
			{
				PatchOperation.Set("/matrix/0/0", 10),
				PatchOperation.Set("/matrix/1/1", 40)
			});

		var result = await container.ReadItemAsync<DocWithMatrix>("1", PK("pk"));
		result.Resource.Matrix[0][0].Should().Be(10);
		result.Resource.Matrix[0][1].Should().Be(2);
		result.Resource.Matrix[1][0].Should().Be(3);
		result.Resource.Matrix[1][1].Should().Be(40);
	}

	[Fact]
	public async Task Patch_DeeplyNestedMixedStructure_AllOpsWork()
	{
		var container = CreateContainer();
		var doc = new DocWithDeepNesting
		{
			Id = "1",
			PartitionKey = "pk",
			A = new List<LevelB>
			{
				new()
				{
					B = new List<LevelC>
					{
						new() { C = "level5-val", Value = 1 },
						new() { C = "other", Value = 2 }
					}
				}
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		// Set, increment, remove across deep paths
		await container.PatchItemAsync<DocWithDeepNesting>("1", PK("pk"),
			new[]
			{
				PatchOperation.Set("/a/0/b/0/c", "updated"),
				PatchOperation.Increment("/a/0/b/1/value", 10),
				PatchOperation.Remove("/a/0/b/1/c"),
			});

		var result = await container.ReadItemAsync<DocWithDeepNesting>("1", PK("pk"));
		result.Resource.A[0].B[0].C.Should().Be("updated");
		result.Resource.A[0].B[1].Value.Should().Be(12);

		// Verify c was removed from b[1]
		var stream = await container.ReadItemStreamAsync("1", PK("pk"));
		using var reader = new System.IO.StreamReader(stream.Content);
		var jObj = JObject.Parse(await reader.ReadToEndAsync());
		jObj.SelectToken("a[0].b[1].c").Should().BeNull();
	}

	[Fact]
	public async Task Patch_EmptyArrayElement_AddProperty_Works()
	{
		var container = CreateContainer();
		// Create doc with an empty object in the array
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry> { new() } // all defaults (empty strings, 0)
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[] { PatchOperation.Set("/items/0/name", "populated") });

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].Name.Should().Be("populated");
	}

	#endregion

	#region 1.7 Mixed Operations Across Root and Nested Paths

	[Fact]
	public async Task MixedOps_SetRootAndArrayNested_SameTerminalName_NoCorruption()
	{
		var container = CreateContainer();
		var doc = new DocWithRootAndNestedTransactions
		{
			Id = "1",
			PartitionKey = "pk",
			Transactions = new Dictionary<string, string> { ["k1"] = "v1" },
			Runs = new List<RunEntry>
			{
				new() { Status = "Pending", Transactions = new List<string> { "t1" } },
				new() { Status = "Done", Transactions = new List<string> { "t2" } }
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithRootAndNestedTransactions>("1", PK("pk"),
			new[]
			{
				PatchOperation.Set("/transactions", new Dictionary<string, string> { ["k2"] = "v2" }),
				PatchOperation.Set("/runs/0/transactions", new List<string> { "t1-new" }),
				PatchOperation.Set("/runs/1/status", "Active"),
				PatchOperation.Set("/runs/1/transactions", new List<string> { "t2-new", "t3-new" }),
			});

		var result = await container.ReadItemAsync<DocWithRootAndNestedTransactions>("1", PK("pk"));
		result.Resource.Transactions.Should().HaveCount(1);
		result.Resource.Transactions["k2"].Should().Be("v2");
		result.Resource.Runs[0].Transactions.Should().BeEquivalentTo(new[] { "t1-new" });
		result.Resource.Runs[1].Status.Should().Be("Active");
		result.Resource.Runs[1].Transactions.Should().BeEquivalentTo(new[] { "t2-new", "t3-new" });
	}

	[Fact]
	public async Task MixedOps_AddRemoveReplaceAcrossDepths_AllCorrect()
	{
		var container = CreateContainer();
		var doc = new DocWithItems
		{
			Id = "1",
			PartitionKey = "pk",
			Items = new List<ItemEntry>
			{
				new() { Name = "item0", Count = 5, ExistingProp = "old", PropToRemove = "bye" },
			}
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithItems>("1", PK("pk"),
			new[]
			{
				PatchOperation.Replace("/items/0/existingProp", "new"),
				PatchOperation.Remove("/items/0/propToRemove"),
				PatchOperation.Add("/items/0/val", "added"),
				PatchOperation.Increment("/items/0/count", 3),
			});

		var result = await container.ReadItemAsync<DocWithItems>("1", PK("pk"));
		result.Resource.Items[0].ExistingProp.Should().Be("new");
		result.Resource.Items[0].Val.Should().Be("added");
		result.Resource.Items[0].Count.Should().Be(8);

		// Verify removal via raw JSON
		var stream = await container.ReadItemStreamAsync("1", PK("pk"));
		using var reader = new System.IO.StreamReader(stream.Content);
		var jObj = JObject.Parse(await reader.ReadToEndAsync());
		jObj.SelectToken("items[0].propToRemove").Should().BeNull();
	}

	[Fact]
	public async Task MixedOps_IncrementAtRootAndNested_BothWork()
	{
		var container = CreateContainer();
		var doc = new DocWithRootAndNestedTransactions
		{
			Id = "1",
			PartitionKey = "pk",
			Count = 10,
			Runs = new List<RunEntry> { new() { Count = 20 } },
			Transactions = new Dictionary<string, string>()
		};
		await container.CreateItemAsync(doc, PK("pk"));

		await container.PatchItemAsync<DocWithRootAndNestedTransactions>("1", PK("pk"),
			new[]
			{
				PatchOperation.Increment("/count", 5),
				PatchOperation.Increment("/runs/0/count", 10),
			});

		var result = await container.ReadItemAsync<DocWithRootAndNestedTransactions>("1", PK("pk"));
		result.Resource.Count.Should().Be(15);
		result.Resource.Runs[0].Count.Should().Be(30);
	}

	#endregion
}
