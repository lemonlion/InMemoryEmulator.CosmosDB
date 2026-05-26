using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Integration tests for Issue #9: Newtonsoft.Json case-insensitive deserialization
/// conflicts with case-sensitive property names. These tests run through the full
/// CosmosClient → FakeCosmosHandler → InMemoryContainer pipeline, validating that
/// the SDK HTTP layer preserves exact JSON property casing.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerCaseSensitivePropertyTests(EmulatorSession session) : IAsyncLifetime
{
	private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
	private Container _container = null!;

	public async ValueTask InitializeAsync()
	{
		_container = await _fixture.CreateContainerAsync("issue9-case", "/p");
	}

	public async ValueTask DisposeAsync()
	{
		await _fixture.DisposeAsync();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Test model classes
	// ═══════════════════════════════════════════════════════════════════════════

	private class EquinoxStyleEvent
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("p")] public string P { get; set; } = "";
		public string d { get; set; } = "";
		public string D { get; set; } = "";
		public string m { get; set; } = "";
		public string M { get; set; } = "";
	}

	public enum Priority { Low, Medium, High, Critical }

	private class EnumDocument
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("p")] public string P { get; set; } = "";
		public Priority Priority { get; set; }
	}

	private class MixedCaseAlphabet
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("p")] public string P { get; set; } = "";
		public string a { get; set; } = "";
		public string A { get; set; } = "";
		public string aa { get; set; } = "";
		public string AA { get; set; } = "";
		public string aA { get; set; } = "";
		public string Aa { get; set; } = "";
	}

	private class PascalCaseDoc
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("p")] public string P { get; set; } = "";
		public string FirstName { get; set; } = "";
		public string LastName { get; set; } = "";
		public int ItemCount { get; set; }
	}

	private class JsonPropertyOverrideDoc
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("p")] public string P { get; set; } = "";
		[JsonProperty("custom_name")] public string PascalCaseProperty { get; set; } = "";
		[JsonProperty("LOUD_NAME")] public string quietProperty { get; set; } = "";
		[JsonProperty("MiXeD")] public string AnotherProperty { get; set; } = "";
	}

	private class NestedCaseSensitive
	{
		[JsonProperty("id")] public string Id { get; set; } = "";
		[JsonProperty("p")] public string P { get; set; } = "";
		public InnerLevel Level1 { get; set; } = new();
	}

	private class InnerLevel
	{
		public string x { get; set; } = "";
		public string X { get; set; } = "";
		public DeepLevel Deep { get; set; } = new();
	}

	private class DeepLevel
	{
		public string y { get; set; } = "";
		public string Y { get; set; } = "";
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Core issue: case-sensitive property round-trip via JObject
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task CaseSensitiveProperties_RoundTripCorrectly_ViaJObject()
	{
		var jObj = new JObject
		{
			["id"] = "rt-jo-1",
			["p"] = "stream-1",
			["d"] = "lowercase-data",
			["D"] = "uppercase-data",
			["m"] = "lowercase-meta",
			["M"] = "uppercase-meta"
		};

		await _container.CreateItemAsync(jObj, new PartitionKey("stream-1"));

		var response = await _container.ReadItemAsync<JObject>("rt-jo-1", new PartitionKey("stream-1"));
		var result = response.Resource;

		result["d"]!.ToString().Should().Be("lowercase-data");
		result["D"]!.ToString().Should().Be("uppercase-data");
		result["m"]!.ToString().Should().Be("lowercase-meta");
		result["M"]!.ToString().Should().Be("uppercase-meta");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Deserialization to strongly-typed class with case-sensitive properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task CaseSensitiveProperties_DeserializeCorrectly_ToStrongType()
	{
		var jObj = new JObject
		{
			["id"] = "rt-st-1",
			["p"] = "stream-1",
			["d"] = "lowercase-data",
			["D"] = "uppercase-data",
			["m"] = "lowercase-meta",
			["M"] = "uppercase-meta"
		};

		await _container.CreateItemAsync(jObj, new PartitionKey("stream-1"));

		var response = await _container.ReadItemAsync<EquinoxStyleEvent>(
			"rt-st-1", new PartitionKey("stream-1"));

		var result = response.Resource;
		result.d.Should().Be("lowercase-data");
		result.D.Should().Be("uppercase-data");
		result.m.Should().Be("lowercase-meta");
		result.M.Should().Be("uppercase-meta");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Mixed casing combinations: a, A, aa, AA, aA, Aa
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task MixedCaseCombinations_AllSurviveRoundTrip_ViaStrongType()
	{
		var item = new MixedCaseAlphabet
		{
			Id = "mc-st-1",
			P = "p1",
			a = "lower-a",
			A = "upper-A",
			aa = "lower-aa",
			AA = "upper-AA",
			aA = "lower-a-upper-A",
			Aa = "upper-A-lower-a"
		};

		await _container.CreateItemAsync(item, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<MixedCaseAlphabet>("mc-st-1", new PartitionKey("p1"));
		var result = response.Resource;

		result.a.Should().Be("lower-a");
		result.A.Should().Be("upper-A");
		result.aa.Should().Be("lower-aa");
		result.AA.Should().Be("upper-AA");
		result.aA.Should().Be("lower-a-upper-A");
		result.Aa.Should().Be("upper-A-lower-a");
	}

	[Fact]
	public async Task MixedCaseCombinations_AllSurviveRoundTrip_ViaJObject()
	{
		var jObj = new JObject
		{
			["id"] = "mc-jo-1",
			["p"] = "p1",
			["a"] = "1",
			["A"] = "2",
			["aa"] = "3",
			["AA"] = "4",
			["aA"] = "5",
			["Aa"] = "6"
		};

		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<JObject>("mc-jo-1", new PartitionKey("p1"));
		var result = response.Resource;

		result["a"]!.ToString().Should().Be("1");
		result["A"]!.ToString().Should().Be("2");
		result["aa"]!.ToString().Should().Be("3");
		result["AA"]!.ToString().Should().Be("4");
		result["aA"]!.ToString().Should().Be("5");
		result["Aa"]!.ToString().Should().Be("6");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  PascalCase properties NOT transformed (regression guard)
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task PascalCaseProperties_NotCamelCased_AfterFix()
	{
		var item = new PascalCaseDoc
		{
			Id = "pc-1",
			P = "p1",
			FirstName = "Jane",
			LastName = "Doe",
			ItemCount = 42
		};

		await _container.CreateItemAsync(item, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<JObject>("pc-1", new PartitionKey("p1"));
		var result = response.Resource;

		result["FirstName"]!.ToString().Should().Be("Jane");
		result["LastName"]!.ToString().Should().Be("Doe");
		result["ItemCount"]!.Value<int>().Should().Be(42);

		result["firstName"].Should().BeNull();
		result["lastName"].Should().BeNull();
		result["itemCount"].Should().BeNull();
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  System properties still work
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SystemProperties_StillPopulated_WithoutNamingStrategy()
	{
		var jObj = new JObject
		{
			["id"] = "sp-1",
			["p"] = "p1",
			["Data"] = "hello"
		};

		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<JObject>("sp-1", new PartitionKey("p1"));
		var result = response.Resource;

		result["_etag"].Should().NotBeNull();
		result["_ts"].Should().NotBeNull();
		result["_rid"].Should().NotBeNull();
		result["Data"]!.ToString().Should().Be("hello");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Enum serialization still works
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task EnumProperties_RoundTripCorrectly()
	{
		var item = new EnumDocument
		{
			Id = "en-1",
			P = "p1",
			Priority = Priority.Critical
		};

		await _container.CreateItemAsync(item, new PartitionKey("p1"));

		// Through the SDK pipeline, enums serialize as their integer value
		// unless the CosmosClient is explicitly configured with StringEnumConverter
		var typed = await _container.ReadItemAsync<EnumDocument>("en-1", new PartitionKey("p1"));
		typed.Resource.Priority.Should().Be(Priority.Critical);
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Upsert preserves case-sensitive properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task CaseSensitiveProperties_PreservedAfterUpsert()
	{
		var jObj = new JObject
		{
			["id"] = "up-1",
			["p"] = "p1",
			["d"] = "original-lowercase",
			["D"] = "original-uppercase"
		};

		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));

		var upsertObj = new JObject
		{
			["id"] = "up-1",
			["p"] = "p1",
			["d"] = "updated-lowercase",
			["D"] = "updated-uppercase"
		};

		await _container.UpsertItemAsync(upsertObj, new PartitionKey("p1"));

		var response = await _container.ReadItemAsync<JObject>("up-1", new PartitionKey("p1"));
		response.Resource["d"]!.ToString().Should().Be("updated-lowercase");
		response.Resource["D"]!.ToString().Should().Be("updated-uppercase");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Replace preserves case-sensitive properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task CaseSensitiveProperties_PreservedAfterReplace()
	{
		var jObj = new JObject
		{
			["id"] = "rp-1",
			["p"] = "p1",
			["d"] = "original-lowercase",
			["D"] = "original-uppercase"
		};

		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));

		var replaceObj = new JObject
		{
			["id"] = "rp-1",
			["p"] = "p1",
			["d"] = "replaced-lowercase",
			["D"] = "replaced-uppercase"
		};

		await _container.ReplaceItemAsync(replaceObj, "rp-1", new PartitionKey("p1"));

		var response = await _container.ReadItemAsync<JObject>("rp-1", new PartitionKey("p1"));
		response.Resource["d"]!.ToString().Should().Be("replaced-lowercase");
		response.Resource["D"]!.ToString().Should().Be("replaced-uppercase");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  SQL queries referencing case-sensitive properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SqlQuery_SelectBothCasings_ReturnsDistinctValues()
	{
		var jObj = new JObject
		{
			["id"] = "sq-1",
			["p"] = "p1",
			["d"] = "lower-val",
			["D"] = "UPPER-val"
		};
		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));

		var query = new QueryDefinition("SELECT c.d, c.D FROM c WHERE c.id = 'sq-1'");
		var iter = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["d"]!.ToString().Should().Be("lower-val");
		results[0]["D"]!.ToString().Should().Be("UPPER-val");
	}

	[Fact]
	public async Task SqlQuery_FilterOnCaseSensitiveProperty()
	{
		await _container.CreateItemAsync(new JObject
		{
			["id"] = "sf-1",
			["p"] = "p1",
			["val"] = "match",
			["Val"] = "no-match"
		}, new PartitionKey("p1"));

		await _container.CreateItemAsync(new JObject
		{
			["id"] = "sf-2",
			["p"] = "p1",
			["val"] = "no-match",
			["Val"] = "match"
		}, new PartitionKey("p1"));

		var query = new QueryDefinition("SELECT c.id FROM c WHERE c.val = 'match'");
		var iter = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["id"]!.ToString().Should().Be("sf-1");
	}

	[Fact]
	public async Task SqlQuery_PascalCasePropertyName_NotCamelCased()
	{
		var item = new PascalCaseDoc
		{
			Id = "sq-pc-1",
			P = "p1",
			FirstName = "Ada",
			LastName = "Lovelace",
			ItemCount = 1
		};
		await _container.CreateItemAsync(item, new PartitionKey("p1"));

		var query = new QueryDefinition("SELECT c.FirstName, c.LastName FROM c WHERE c.id = 'sq-pc-1'");
		var iter = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["FirstName"]!.ToString().Should().Be("Ada");
		results[0]["LastName"]!.ToString().Should().Be("Lovelace");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Patch operations on case-sensitive property paths
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task Patch_SetCaseSensitiveProperty_OnlyAffectsTargetCasing()
	{
		var jObj = new JObject
		{
			["id"] = "pa-1",
			["p"] = "p1",
			["d"] = "original",
			["D"] = "ORIGINAL"
		};
		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));

		var patchOps = new[] { PatchOperation.Set("/d", "patched-lower") };
		await _container.PatchItemAsync<JObject>("pa-1", new PartitionKey("p1"), patchOps);

		var response = await _container.ReadItemAsync<JObject>("pa-1", new PartitionKey("p1"));
		response.Resource["d"]!.ToString().Should().Be("patched-lower");
		response.Resource["D"]!.ToString().Should().Be("ORIGINAL");
	}

	[Fact]
	public async Task Patch_NestedCaseSensitivePath_UpdatesCorrectProperty()
	{
		var jObj = new JObject
		{
			["id"] = "pa-n-1",
			["p"] = "p1",
			["nested"] = new JObject
			{
				["v"] = "lower-original",
				["V"] = "UPPER-original"
			}
		};
		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));

		var patchOps = new[] { PatchOperation.Set("/nested/v", "lower-patched") };
		await _container.PatchItemAsync<JObject>("pa-n-1", new PartitionKey("p1"), patchOps);

		var response = await _container.ReadItemAsync<JObject>("pa-n-1", new PartitionKey("p1"));
		var nested = response.Resource["nested"] as JObject;
		nested!["v"]!.ToString().Should().Be("lower-patched");
		nested["V"]!.ToString().Should().Be("UPPER-original");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  [JsonProperty] attribute interaction
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task JsonPropertyAttribute_OverridesPropertyName_Correctly()
	{
		var item = new JsonPropertyOverrideDoc
		{
			Id = "jp-1",
			P = "p1",
			PascalCaseProperty = "custom-value",
			quietProperty = "loud-value",
			AnotherProperty = "mixed-value"
		};

		await _container.CreateItemAsync(item, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<JObject>("jp-1", new PartitionKey("p1"));
		var result = response.Resource;

		result["custom_name"]!.ToString().Should().Be("custom-value");
		result["LOUD_NAME"]!.ToString().Should().Be("loud-value");
		result["MiXeD"]!.ToString().Should().Be("mixed-value");

		result["PascalCaseProperty"].Should().BeNull();
		result["quietProperty"].Should().BeNull();
		result["AnotherProperty"].Should().BeNull();
	}

	[Fact]
	public async Task JsonPropertyAttribute_RoundTripsCorrectly()
	{
		var item = new JsonPropertyOverrideDoc
		{
			Id = "jp-rt-1",
			P = "p1",
			PascalCaseProperty = "val1",
			quietProperty = "val2",
			AnotherProperty = "val3"
		};

		await _container.CreateItemAsync(item, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<JsonPropertyOverrideDoc>("jp-rt-1", new PartitionKey("p1"));
		var result = response.Resource;

		result.PascalCaseProperty.Should().Be("val1");
		result.quietProperty.Should().Be("val2");
		result.AnotherProperty.Should().Be("val3");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Deeply nested objects with case-sensitive properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task DeepNesting_PreservesCaseSensitiveProperties()
	{
		var item = new NestedCaseSensitive
		{
			Id = "dn-1",
			P = "p1",
			Level1 = new InnerLevel
			{
				x = "inner-lower",
				X = "inner-UPPER",
				Deep = new DeepLevel
				{
					y = "deep-lower",
					Y = "deep-UPPER"
				}
			}
		};

		await _container.CreateItemAsync(item, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<JObject>("dn-1", new PartitionKey("p1"));
		var result = response.Resource;

		var level1 = result["Level1"] as JObject;
		level1.Should().NotBeNull();
		level1!["x"]!.ToString().Should().Be("inner-lower");
		level1["X"]!.ToString().Should().Be("inner-UPPER");

		var deep = level1["Deep"] as JObject;
		deep.Should().NotBeNull();
		deep!["y"]!.ToString().Should().Be("deep-lower");
		deep["Y"]!.ToString().Should().Be("deep-UPPER");
	}

	[Fact]
	public async Task DeepNesting_StrongTypeRoundTrip()
	{
		var item = new NestedCaseSensitive
		{
			Id = "dn-rt-1",
			P = "p1",
			Level1 = new InnerLevel
			{
				x = "a",
				X = "B",
				Deep = new DeepLevel { y = "c", Y = "D" }
			}
		};

		await _container.CreateItemAsync(item, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<NestedCaseSensitive>("dn-rt-1", new PartitionKey("p1"));
		var result = response.Resource;

		result.Level1.x.Should().Be("a");
		result.Level1.X.Should().Be("B");
		result.Level1.Deep.y.Should().Be("c");
		result.Level1.Deep.Y.Should().Be("D");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Arrays containing objects with case-sensitive properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task ArraysOfObjects_PreserveCaseSensitiveProperties()
	{
		var jObj = new JObject
		{
			["id"] = "arr-1",
			["p"] = "p1",
			["items"] = new JArray(
				new JObject { ["x"] = "a1", ["X"] = "A1" },
				new JObject { ["x"] = "a2", ["X"] = "A2" }
			)
		};

		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));
		var response = await _container.ReadItemAsync<JObject>("arr-1", new PartitionKey("p1"));
		var items = response.Resource["items"] as JArray;

		items.Should().NotBeNull();
		items!.Count.Should().Be(2);
		items[0]["x"]!.ToString().Should().Be("a1");
		items[0]["X"]!.ToString().Should().Be("A1");
		items[1]["x"]!.ToString().Should().Be("a2");
		items[1]["X"]!.ToString().Should().Be("A2");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  SQL query on nested case-sensitive properties
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SqlQuery_OnNestedCaseSensitiveProperties()
	{
		var jObj = new JObject
		{
			["id"] = "sq-n-1",
			["p"] = "p1",
			["nested"] = new JObject
			{
				["x"] = "find-me",
				["X"] = "not-me"
			}
		};
		await _container.CreateItemAsync(jObj, new PartitionKey("p1"));

		var query = new QueryDefinition(
			"SELECT c.nested.x AS lowerVal, c.nested.X AS upperVal FROM c WHERE c.id = 'sq-n-1'");
		var iter = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iter.HasMoreResults)
		{
			var page = await iter.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["lowerVal"]!.ToString().Should().Be("find-me");
		results[0]["upperVal"]!.ToString().Should().Be("not-me");
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  All 52 single-letter properties (a-z, A-Z) preserved
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task SingleLetterProperties_AllTwentySixPreserved()
	{
		var jObj = new JObject { ["id"] = "26-1", ["p"] = "lower-p" };

		for (var c = 'a'; c <= 'z'; c++)
		{
			jObj[c.ToString()] = $"lower-{c}";
			jObj[char.ToUpper(c).ToString()] = $"upper-{char.ToUpper(c)}";
		}

		await _container.CreateItemAsync(jObj, new PartitionKey("lower-p"));
		var response = await _container.ReadItemAsync<JObject>("26-1", new PartitionKey("lower-p"));
		var result = response.Resource;

		for (var c = 'a'; c <= 'z'; c++)
		{
			result[c.ToString()]!.ToString().Should().Be($"lower-{c}",
				$"lowercase property '{c}' should be preserved");
			result[char.ToUpper(c).ToString()]!.ToString().Should().Be($"upper-{char.ToUpper(c)}",
				$"uppercase property '{char.ToUpper(c)}' should be preserved");
		}
	}

	// ═══════════════════════════════════════════════════════════════════════════
	//  Multiple documents with different property casing
	// ═══════════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task MultipleDocuments_WithDifferentPropertyCasing_IndependentlyCorrect()
	{
		await _container.CreateItemAsync(new JObject
		{
			["id"] = "md-1",
			["p"] = "p1",
			["Status"] = "Active"
		}, new PartitionKey("p1"));

		await _container.CreateItemAsync(new JObject
		{
			["id"] = "md-2",
			["p"] = "p1",
			["status"] = "inactive"
		}, new PartitionKey("p1"));

		var r1 = await _container.ReadItemAsync<JObject>("md-1", new PartitionKey("p1"));
		var r2 = await _container.ReadItemAsync<JObject>("md-2", new PartitionKey("p1"));

		r1.Resource["Status"]!.ToString().Should().Be("Active");
		r1.Resource["status"].Should().BeNull();

		r2.Resource["status"]!.ToString().Should().Be("inactive");
		r2.Resource["Status"].Should().BeNull();
	}
}
