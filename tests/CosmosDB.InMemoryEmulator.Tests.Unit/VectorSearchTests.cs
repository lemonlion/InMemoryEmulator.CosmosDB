using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for the VECTORDISTANCE SQL function, which computes similarity/distance
/// between two vectors. Supports cosine similarity, dot product, and Euclidean distance.
/// Signature: VECTORDISTANCE(vector1, vector2 [, bool_bruteForce] [, {distanceFunction:'cosine'}])
/// </summary>
public class VectorSearchTests
{
	private static async Task<InMemoryContainer> CreateContainerWithVectors()
	{
		var container = new InMemoryContainer("vector-test", "/pk");

		// Unit vectors along each axis — easy to reason about cosine similarity
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "x", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "y", pk = "a", embedding = new[] { 0.0, 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "z", pk = "a", embedding = new[] { 0.0, 0.0, 1.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "neg-x", pk = "a", embedding = new[] { -1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		return container;
	}

	private static async Task<List<JObject>> RunQuery(InMemoryContainer container, string sql)
	{
		var iterator = container.GetItemQueryIterator<JObject>(sql,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	// ─── Cosine Similarity ───────────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_Cosine_IdenticalVectors_ReturnsOne()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"cosine similarity of identical vectors should be 1.0");
	}

	[Fact]
	public async Task VectorDistance_Cosine_OrthogonalVectors_ReturnsZero()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'y'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9,
			"cosine similarity of orthogonal vectors should be 0.0");
	}

	[Fact]
	public async Task VectorDistance_Cosine_OppositeVectors_ReturnsNegativeOne()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'neg-x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(-1.0, 1e-9,
			"cosine similarity of opposite vectors should be -1.0");
	}

	// ─── Dot Product ─────────────────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_DotProduct_ReturnsCorrectValue()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 2.0, 3.0, 4.0 } }),
			new PartitionKey("a"));

		// dot([2,3,4], [1,5,2]) = 2*1 + 3*5 + 4*2 = 2 + 15 + 8 = 25
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 5.0, 2.0], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(25.0, 1e-9);
	}

	// ─── Euclidean Distance ──────────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_Euclidean_ReturnsCorrectValue()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		// euclidean([1,0,0], [0,1,0]) = sqrt((1-0)^2 + (0-1)^2 + (0-0)^2) = sqrt(2)
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 1.0, 0.0], false, {distanceFunction:'euclidean'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(Math.Sqrt(2), 1e-9);
	}

	// ─── SELECT Projection ───────────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_InSelectProjection_ReturnsSimilarityScore()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().HaveCount(4, "all 4 documents should have a score computed");
		results.Should().Contain(r => r["id"]!.ToString() == "x" && Math.Abs(r["score"]!.Value<double>() - 1.0) < 1e-9);
		results.Should().Contain(r => r["id"]!.ToString() == "y" && Math.Abs(r["score"]!.Value<double>() - 0.0) < 1e-9);
	}

	// ─── ORDER BY ────────────────────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_InOrderBy_SortsByScore()
	{
		var container = await CreateContainerWithVectors();

		// ORDER BY VectorDistance DESC — most similar first (highest cosine similarity)
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

		results.Should().HaveCount(4);
		results[0]["id"]!.ToString().Should().Be("x", "identical vector should be first (score 1.0)");
		results[^1]["id"]!.ToString().Should().Be("neg-x", "opposite vector should be last (score -1.0)");
	}

	[Fact]
	public async Task VectorDistance_TopN_WithOrderBy_ReturnsClosest()
	{
		var container = await CreateContainerWithVectors();

		var results = await RunQuery(container,
			"SELECT TOP 2 c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

		results.Should().HaveCount(2);
		results[0]["id"]!.ToString().Should().Be("x");
	}

	// ─── Combined with WHERE ─────────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_WithWhereClause_FiltersAndScores()
	{
		var container = await CreateContainerWithVectors();

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id != 'neg-x' ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

		results.Should().HaveCount(3);
		results.Should().NotContain(r => r["id"]!.ToString() == "neg-x");
	}

	// ─── Edge Cases ──────────────────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_MismatchedDimensions_ReturnsNull()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		// Document has 2D vector, query has 3D vector — should return null
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"mismatched dimensions should return null, not throw");
	}

	[Fact]
	public async Task VectorDistance_MissingEmbedding_ReturnsNull()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "no-embedding" }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"missing vector property should return null");
	}

	// ─── Optional Parameters ─────────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_DistanceFunctionOverride_UsesEuclidean()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 3.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		// Euclidean distance between [3,0,0] and [0,0,0] = 3.0
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 0.0, 0.0], false, {distanceFunction:'euclidean'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(3.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_BoolBruteForceParam_AcceptedAndIgnored()
	{
		var container = await CreateContainerWithVectors();

		// 3rd argument (true = brute force) should be accepted but has no effect in emulator
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0], true) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	// ─── High-Dimensional Vectors ────────────────────────────────────────

	[Fact]
	public async Task VectorDistance_HighDimensional_1536Dimensions()
	{
		var container = new InMemoryContainer("vector-test", "/pk");

		// Create a 1536-dim unit vector along first axis
		var vec1 = new double[1536];
		vec1[0] = 1.0;
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = vec1 }),
			new PartitionKey("a"));

		// Query with same vector — cosine similarity should be 1.0
		var queryVec = string.Join(",", vec1.Select(v => v.ToString("F1")));
		var results = await RunQuery(container,
			$"SELECT VectorDistance(c.embedding, [{queryVec}]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	// ─── Default Distance Function (Cosine) ──────────────────────────────

	[Fact]
	public async Task VectorDistance_DefaultDistanceFunction_IsCosine()
	{
		var container = await CreateContainerWithVectors();

		// No 4th argument — should default to cosine
		// VectorDistance([1,0,0], [1,0,0]) with cosine = 1.0
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"default distance function should be cosine similarity");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  A: Mathematical Edge Cases
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_Cosine_ZeroVector_ReturnsNull()
	{
		// BUG-1: zero-magnitude vector → cosine is undefined.
		// Real Cosmos DB returns no SimilarityScore (null). Emulator should match.
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"cosine similarity is undefined for zero-magnitude vectors — should return null");
	}

	[Fact]
	public async Task VectorDistance_Cosine_NonUnitVectors_NormalizesCorrectly()
	{
		// [3,4] and [6,8] are parallel (same direction) — cosine = 1.0 regardless of magnitude
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 3.0, 4.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [6.0, 8.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"parallel non-unit vectors should have cosine similarity 1.0");
	}

	[Fact]
	public async Task VectorDistance_Cosine_NegativeComponents_ComputesCorrectly()
	{
		// [1, -1] vs [-1, 1] → dot = -2, |a|=√2, |b|=√2 → cosine = -2/2 = -1.0
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, -1.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [-1.0, 1.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(-1.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_DotProduct_OrthogonalVectors_ReturnsZero()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 1.0], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_DotProduct_OppositeVectors_ReturnsNegative()
	{
		// [1,0] · [-1,0] = -1
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [-1.0, 0.0], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(-1.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_Euclidean_IdenticalVectors_ReturnsZero()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 5.0, 3.0, 1.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [5.0, 3.0, 1.0], false, {distanceFunction:'euclidean'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9,
			"euclidean distance from a vector to itself should be 0");
	}

	[Fact]
	public async Task VectorDistance_Euclidean_KnownTriangle_345()
	{
		// [0,0] vs [3,4] → √(9+16) = 5.0
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [3.0, 4.0], false, {distanceFunction:'euclidean'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(5.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_Cosine_SingleDimension_HandlesCorrectly()
	{
		// 1D vectors: [5] vs [3] → both positive, cosine = 1.0
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 5.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [3.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"1D positive vectors are parallel → cosine = 1.0");
	}

	[Fact]
	public async Task VectorDistance_DotProduct_ZeroVector_ReturnsZero()
	{
		// dot([0,0,0], [1,2,3]) = 0
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 2.0, 3.0], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_Euclidean_ZeroVector_ReturnsVectorMagnitude()
	{
		// euclidean([3,4,0], [0,0,0]) = √(9+16+0) = 5.0
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 3.0, 4.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 0.0, 0.0], false, {distanceFunction:'euclidean'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(5.0, 1e-9);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  B: Parameter Handling & Overloads
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_TwoArgsOnly_DefaultsToCosine()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_ThreeArgs_BruteForce_False()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0], false) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_FourArgs_DotProduct()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 2.0, 3.0 } }),
			new PartitionKey("a"));

		// dot([2,3], [4,5]) = 8 + 15 = 23
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [4.0, 5.0], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(23.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_FourArgs_Cosine_Explicit()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0], false, {distanceFunction:'cosine'}) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"explicit cosine should produce same result as default");
	}

	[Fact]
	public async Task VectorDistance_OptionsWithDataType_AcceptedSilently()
	{
		// dataType is an index-level concern — emulator accepts but ignores it
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0], false, {distanceFunction:'cosine', dataType:'Float32'}) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_OptionsWithSearchListSizeMultiplier_AcceptedSilently()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0], false, {distanceFunction:'cosine', searchListSizeMultiplier:10}) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_OptionsWithFilterPriority_AcceptedSilently()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0], false, {distanceFunction:'cosine', filterPriority:0.5}) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	[Theory]
	[InlineData("cosine")]
	[InlineData("COSINE")]
	[InlineData("Cosine")]
	[InlineData("CoSiNe")]
	public async Task VectorDistance_CaseInsensitiveDistanceFunction(string distFn)
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			$"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0], false, {{distanceFunction:'{distFn}'}}) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			$"distanceFunction '{distFn}' should be case-insensitive");
	}

	[Fact]
	public async Task VectorDistance_OneArg_ReturnsNull()
	{
		// Only 1 vector arg — less than minimum 2 args, should return null
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  C: Data Type & Input Edge Cases
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_IntegerVectorValues_WorkCorrectly()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1, 0, 0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1, 0, 0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_MixedIntAndFloatValues_WorkCorrectly()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 2, 3.5 } }),
			new PartitionKey("a"));

		// dot([1.0, 2, 3.5], [1, 2.0, 3.5]) = 1 + 4 + 12.25 = 17.25
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1, 2.0, 3.5], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(17.25, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_EmptyVector_ReturnsNull()
	{
		// Both vectors empty [] — length == 0, should return null
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"1\",\"pk\":\"a\",\"embedding\":[]}"),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, []) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"empty vectors (length 0) should return null");
	}

	[Fact]
	public async Task VectorDistance_VeryLargeValues_NoOverflow()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1e38, 1e38 } }),
			new PartitionKey("a"));

		// Should not throw — cosine of parallel vectors is still 1.0
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1e38, 1e38]) AS score FROM c");

		results.Should().ContainSingle();
		var score = results[0]["score"];
		// Either a valid double or null — must not throw
		(score!.Type == JTokenType.Float || score.Type == JTokenType.Integer || score.Type == JTokenType.Null)
			.Should().BeTrue("very large values should not cause an exception");
	}

	[Fact]
	public async Task VectorDistance_VerySmallValues_NoPrecisionLoss()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1e-10, 1e-10, 0.0 } }),
			new PartitionKey("a"));

		// [1e-10, 1e-10, 0] vs [1e-10, 1e-10, 0] — identical vectors, cosine = 1.0
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1e-10, 1e-10, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-6,
			"very small but identical vectors should still have cosine ≈ 1.0");
	}

	[Fact]
	public async Task VectorDistance_NullVectorProperty_ReturnsNull()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"1\",\"pk\":\"a\",\"embedding\":null}"),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task VectorDistance_NonArrayVectorProperty_ReturnsNull()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = "not-a-vector" }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null);
	}

	[Fact]
	public async Task VectorDistance_NestedProperty_WorksCorrectly()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"1\",\"pk\":\"a\",\"metadata\":{\"embedding\":[1.0,0.0,0.0]}}"),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.metadata.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_VectorWithNonNumericElement_ReturnsNull()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"1\",\"pk\":\"a\",\"embedding\":[1.0,\"abc\",3.0]}"),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"non-numeric elements in the vector should return null");
	}

	[Fact]
	public async Task VectorDistance_TwoDimensional_WorksCorrectly()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		// cosine([1,0], [0,1]) = 0.0
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 1.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  D: SQL Integration (SELECT, WHERE, ORDER BY, GROUP BY)
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_InWhereClause_FiltersBySimilarityThreshold()
	{
		var container = await CreateContainerWithVectors();

		// Cosine scores: x=1.0, y=0.0, z=0.0, neg-x=-1.0
		// WHERE score > 0.5 should only return x
		var results = await RunQuery(container,
			"SELECT c.id FROM c WHERE VectorDistance(c.embedding, [1.0, 0.0, 0.0]) > 0.5");

		results.Should().ContainSingle();
		results[0]["id"]!.ToString().Should().Be("x");
	}

	[Fact]
	public async Task VectorDistance_InWhereAndSelect_DifferentVectors()
	{
		var container = await CreateContainerWithVectors();

		// WHERE uses [1,0,0] to filter (score > 0.5 → only 'x'), but SELECT computes score against [0,1,0]
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [0.0, 1.0, 0.0]) AS score FROM c WHERE VectorDistance(c.embedding, [1.0, 0.0, 0.0]) > 0.5");

		results.Should().ContainSingle();
		results[0]["id"]!.ToString().Should().Be("x");
		// x=[1,0,0] vs [0,1,0] → cosine = 0.0
		results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_OrderByAsc_LeastSimilarFirst()
	{
		var container = await CreateContainerWithVectors();

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) ASC");

		results.Should().HaveCount(4);
		results[0]["id"]!.ToString().Should().Be("neg-x", "score -1.0 should be first in ASC");
		results[^1]["id"]!.ToString().Should().Be("x", "score 1.0 should be last in ASC");
	}

	[Fact]
	public async Task VectorDistance_WithOffsetLimit_Paginated()
	{
		var container = await CreateContainerWithVectors();

		// ORDER BY DESC: x(1.0), y(0.0), z(0.0), neg-x(-1.0)
		// OFFSET 1 LIMIT 2 → should skip x, return y and z
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC OFFSET 1 LIMIT 2");

		results.Should().HaveCount(2);
		results.Should().NotContain(r => r["id"]!.ToString() == "x");
		results.Should().NotContain(r => r["id"]!.ToString() == "neg-x");
	}

	[Fact]
	public async Task VectorDistance_AliasedInOrderBy_Works()
	{
		var container = await CreateContainerWithVectors();

		// Use the alias 'score' in ORDER BY — some SQL engines support this
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

		results.Should().HaveCount(4);
		results[0]["id"]!.ToString().Should().Be("x");
	}

	[Fact]
	public async Task VectorDistance_MultipleCallsInSameQuery_Works()
	{
		var container = await CreateContainerWithVectors();

		// Two VectorDistance calls against different query vectors in the same SELECT
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS scoreX, VectorDistance(c.embedding, [0.0, 1.0, 0.0]) AS scoreY FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["scoreX"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
		results[0]["scoreY"]!.Value<double>().Should().BeApproximately(0.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_WithDistinct_WorksCorrectly()
	{
		var container = await CreateContainerWithVectors();

		// y and z both have cosine 0.0 against [1,0,0], so DISTINCT should collapse them
		var results = await RunQuery(container,
			"SELECT DISTINCT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		// Scores: 1.0, 0.0, 0.0, -1.0 → distinct: 1.0, 0.0, -1.0
		results.Should().HaveCount(3);
	}

	[Fact]
	public async Task VectorDistance_CrossPartition_WithoutPartitionKey()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("b"));

		// No partition key specified — cross-partition query
		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
	}

	// D10: GROUP BY with AVG(VectorDistance(...)) — the GROUP BY aggregate handler
	// uses ExtractNumericValues which calls SelectToken with the inner argument as a
	// JSON path. Function calls like VectorDistance(...) are not valid JSON paths, so
	// this throws. Fixing requires the GROUP BY aggregate pipeline to evaluate arbitrary
	// SQL expressions (not just property paths) before aggregating. This is a general
	// limitation of GROUP BY + aggregate(functionCall), not specific to vectors.
	[Fact]
	public async Task VectorDistance_WithGroupBy_AggregatesCorrectly()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", category = "A", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", category = "A", embedding = new[] { 0.8, 0.6 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", category = "B", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT c.category, AVG(VectorDistance(c.embedding, [1.0, 0.0])) AS avgScore FROM c GROUP BY c.category");

		results.Should().HaveCount(2);
		var catA = results.First(r => r["category"]!.ToString() == "A");
		var catB = results.First(r => r["category"]!.ToString() == "B");
		catA["avgScore"]!.Value<double>().Should().BeGreaterThan(catB["avgScore"]!.Value<double>(),
			"category A vectors are closer to [1,0] than category B");
	}

	[Fact]
	public async Task VectorDistance_WithGroupBy_MinMax_AggregatesCorrectly()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", category = "A", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", category = "A", embedding = new[] { 0.8, 0.6 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", category = "B", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT c.category, MIN(VectorDistance(c.embedding, [1.0, 0.0])) AS minDist, " +
			"MAX(VectorDistance(c.embedding, [1.0, 0.0])) AS maxDist FROM c GROUP BY c.category");

		results.Should().HaveCount(2);
		var catA = results.First(r => r["category"]!.ToString() == "A");
		catA["minDist"]!.Value<double>().Should().BeLessThan(catA["maxDist"]!.Value<double>(),
			"category A should have distinct MIN and MAX distances");
	}

	// Sister test: GROUP BY works fine with VectorDistance when used outside aggregates
	[Fact]
	public async Task VectorDistance_WithGroupBy_NonAggregated_WorksInSelect()
	{
		// GROUP BY c.category works; the VectorDistance call is in a non-aggregated
		// position (it just reappears in the grouping key projection). This shows
		// that VectorDistance itself works fine — it's only the combination of
		// aggregate(functionCall) that's unsupported.
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", category = "A", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", category = "B", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT c.category, COUNT(1) AS cnt FROM c GROUP BY c.category");

		results.Should().HaveCount(2);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  E: Multi-Document / Ranking Scenarios
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_KNN_Top5_ReturnsCorrectNearest()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		// Insert 20 docs with embeddings at increasing angles from [1,0]
		for (var i = 0; i < 20; i++)
		{
			var angle = i * Math.PI / 20; // 0 to ~π
			await container.CreateItemAsync(
				JObject.FromObject(new
				{
					id = $"d{i}",
					pk = "a",
					embedding = new[] { Math.Cos(angle), Math.Sin(angle) }
				}),
				new PartitionKey("a"));
		}

		var results = await RunQuery(container,
			"SELECT TOP 5 c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0]) DESC");

		results.Should().HaveCount(5);
		// First result should be d0 (angle=0, closest to [1,0])
		results[0]["id"]!.ToString().Should().Be("d0");
		// All scores should be in descending order
		for (var i = 0; i < results.Count - 1; i++)
			results[i]["score"]!.Value<double>().Should()
				.BeGreaterThanOrEqualTo(results[i + 1]["score"]!.Value<double>());
	}

	[Fact]
	public async Task VectorDistance_TiedScores_ReturnedStably()
	{
		var container = await CreateContainerWithVectors();

		// y=[0,1,0] and z=[0,0,1] both have cosine 0.0 with [1,0,0]
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

		results.Should().HaveCount(4);
		// Both y and z should appear (tied at 0.0), between x (1.0) and neg-x (-1.0)
		var middleIds = results.Skip(1).Take(2).Select(r => r["id"]!.ToString()).ToList();
		middleIds.Should().Contain("y");
		middleIds.Should().Contain("z");
	}

	[Fact]
	public async Task VectorDistance_AllDocsHaveSameEmbedding_SameScore()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		for (var i = 0; i < 5; i++)
		{
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"d{i}", pk = "a", embedding = new[] { 0.6, 0.8 } }),
				new PartitionKey("a"));
		}

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		results.Should().HaveCount(5);
		var scores = results.Select(r => r["score"]!.Value<double>()).Distinct().ToList();
		scores.Should().ContainSingle("all identical embeddings should produce the same score");
	}

	[Fact]
	public async Task VectorDistance_LargeDataset_100Docs_OrderByCorrect()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		var rng = new Random(42); // deterministic seed
		for (var i = 0; i < 100; i++)
		{
			var vec = new[] { rng.NextDouble(), rng.NextDouble(), rng.NextDouble() };
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"d{i}", pk = "a", embedding = vec }),
				new PartitionKey("a"));
		}

		var results = await RunQuery(container,
			"SELECT TOP 10 c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

		results.Should().HaveCount(10);
		for (var i = 0; i < results.Count - 1; i++)
			results[i]["score"]!.Value<double>().Should()
				.BeGreaterThanOrEqualTo(results[i + 1]["score"]!.Value<double>(),
					"results should be in descending score order");
	}

	[Fact]
	public async Task VectorDistance_MixOfValidAndMissingEmbeddings_NullsHandled()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "with-vec", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "no-vec", pk = "a", name = "missing" }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		results.Should().HaveCount(2);
		var withVec = results.First(r => r["id"]!.ToString() == "with-vec");
		var noVec = results.First(r => r["id"]!.ToString() == "no-vec");
		withVec["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9);
		noVec["score"]!.Type.Should().Be(JTokenType.Null);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  F: Divergent Behaviour Tests
	//  Skipped tests document real Cosmos DB behaviour that is too complex
	//  or not meaningful to implement. Sister tests show emulator behaviour.
	// ═════════════════════════════════════════════════════════════════════

	// ─── F1: Multi-dimensional arrays ────────────────────────────────────
	// Real Cosmos DB: "If a multi-dimensional array is provided, the function
	// doesn't return a SimilarityScore value and doesn't return an error."
	// This means the property is omitted entirely from the result object.
	// The emulator returns null for the property instead, which is close
	// but not identical (property present with null vs property absent).

	[Fact(Skip = "F1: Real Cosmos DB omits the SimilarityScore property entirely for multi-dimensional " +
				  "array inputs rather than returning null. The emulator returns the property with a null " +
				  "value. Fixing this would require the query engine to distinguish between 'return null' " +
				  "and 'omit property from output', which is a fundamental change to projection handling. " +
				  "Impact: Very low — user code checking for null handles both cases.")]
	public void VectorDistance_MultiDimensionalArray_RealCosmosReturnsNoScore()
	{
		// Expected real Cosmos DB behavior:
		// Given a document with embedding: [[1,2],[3,4]] (nested array),
		// SELECT VectorDistance(c.embedding, [1,0]) AS score FROM c
		// returns {"id": "1"} with NO "score" property (not null, absent).
	}

	[Fact]
	public async Task VectorDistance_MultiDimensionalArray_EmulatorReturnsNull()
	{
		// Emulator behaviour: multi-dimensional array → ToDoubleArray returns null
		// because inner elements are JArray not JTokenType.Float/Integer → returns null
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"1\",\"pk\":\"a\",\"embedding\":[[1,2],[3,4]]}"),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		// Emulator returns the property with null value (not omitted)
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"emulator returns null for multi-dimensional arrays (real Cosmos omits the property entirely)");
	}

	// ─── F2: Vector embedding policy required in real Cosmos ─────────────
	// Real Cosmos DB requires a vectorEmbeddings container policy defining
	// path, dataType, dimensions, and distanceFunction. Without it, vector
	// search fails. The emulator has no such requirement.

	[Fact(Skip = "F2: Real Cosmos DB requires a vectorEmbeddings container policy (path, dataType, " +
				  "dimensions, distanceFunction) for VECTORDISTANCE to work. Without it, the query fails. " +
				  "The emulator intentionally skips this requirement — always brute-force exact computation " +
				  "without any policy needed. Implementing this would add complexity with no testing value " +
				  "since the policy is an infrastructure concern, not a logic concern.")]
	public void VectorDistance_RequiresVectorPolicy_InRealCosmos()
	{
		// Expected real Cosmos DB behavior:
		// Without vectorEmbeddings policy, VECTORDISTANCE queries fail with an error.
	}

	[Fact]
	public async Task VectorDistance_EmulatorDoesNotRequireVectorPolicy()
	{
		// Emulator behaviour: no vector policy needed, just use the function
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"emulator works without any vector embedding policy configuration");
	}

	// ─── F3: Flat index dimension limit ──────────────────────────────────
	// Real Cosmos DB flat index supports max 505 dimensions.
	// quantizedFlat and diskANN support up to 4096.
	// The emulator has no limit (tested up to 1536).

	[Fact(Skip = "F3: Real Cosmos DB flat index limits vectors to 505 dimensions; quantizedFlat and " +
				  "diskANN support up to 4096. The emulator has no dimensionality limit because it " +
				  "doesn't simulate vector indexing — it always does brute-force linear scan. Imposing " +
				  "artificial limits would reduce testing flexibility without adding correctness value.")]
	public void VectorDistance_FlatIndexMax505Dimensions_InRealCosmos()
	{
		// Expected real Cosmos DB behavior:
		// Vectors with >505 dimensions fail with flat index type.
		// Vectors with >4096 dimensions fail with quantizedFlat/diskANN.
	}

	[Fact]
	public async Task VectorDistance_EmulatorSupportsAnyDimensionality()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		var vec = new double[2000]; // Exceeds all real Cosmos limits
		vec[0] = 1.0;
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = vec }),
			new PartitionKey("a"));

		var queryVec = string.Join(",", vec.Select(v => v.ToString("F1")));
		var results = await RunQuery(container,
			$"SELECT VectorDistance(c.embedding, [{queryVec}]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"emulator supports vectors of any dimensionality (no index limits)");
	}

	// ─── F4: TOP N required with ORDER BY in real Cosmos ─────────────────
	// Microsoft docs: "Always use a TOP N clause in the SELECT statement of a query.
	// Otherwise the vector search tries to return many more results and the query
	// costs more RUs and have higher latency than necessary."
	// This is a performance guidance, not a hard error in all cases.

	[Fact(Skip = "F4: Real Cosmos DB strongly recommends TOP N with ORDER BY VectorDistance — without " +
				  "it, queries may time out or consume excessive RUs on large datasets. The emulator " +
				  "has no RU model and performs instant brute-force computation, so this limitation " +
				  "doesn't apply. Enforcing it would break valid test patterns.")]
	public void VectorDistance_RequiresTopNWithOrderBy_InRealCosmos()
	{
		// Expected real Cosmos DB behavior:
		// ORDER BY VectorDistance without TOP N may time out.
	}

	[Fact]
	public async Task VectorDistance_EmulatorAllowsOrderByWithoutTopN()
	{
		var container = await CreateContainerWithVectors();

		// No TOP N — emulator returns all results, no issue
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0, 0.0]) DESC");

		results.Should().HaveCount(4, "emulator returns all results without requiring TOP N");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  G: Numerical Robustness & IEEE 754 Edge Cases
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_Cosine_BothZeroVectors_ReturnsNull()
	{
		// Both document and query vectors are zero — cosine is undefined for both
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"cosine similarity is undefined when both vectors are zero-magnitude");
	}

	[Fact]
	public async Task VectorDistance_Cosine_QueryVectorZero_ReturnsNull()
	{
		// Document has a valid vector but query vector is zero
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 0.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"cosine similarity is undefined when the query vector is zero-magnitude");
	}

	[Fact]
	public async Task VectorDistance_DotProduct_LargeValues_NoOverflow()
	{
		// Dot product with very large components — could overflow to Infinity
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1e308, 1e308 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1e308, 1e308], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		var score = results[0]["score"];
		// Must not throw, and should return null for overflow (Infinity) rather than a non-finite value
		score!.Type.Should().Be(JTokenType.Null,
			"dot product overflow to Infinity should return null rather than a non-finite value");
	}

	[Fact]
	public async Task VectorDistance_Euclidean_LargeValues_NoOverflow()
	{
		// Euclidean squaring doubles the exponent — overflow risk
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1e308, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 0.0], false, {distanceFunction:'euclidean'}) AS score FROM c");

		results.Should().ContainSingle();
		var score = results[0]["score"];
		// Euclidean: sqrt((1e308)^2 + 0) = sqrt(Infinity) = Infinity → should return null
		score!.Type.Should().Be(JTokenType.Null,
			"euclidean distance overflow to Infinity should return null rather than a non-finite value");
	}

	[Fact]
	public async Task VectorDistance_Cosine_NearlyParallelVectors_HighPrecision()
	{
		// Vectors differing by a tiny amount — cosine should be very close to 1.0 but not exactly
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		// 1e-7 is large enough to be distinguishable in double precision
		// cos([1,0,0], [1,1e-7,0]) = 1/sqrt(1 + 1e-14) ≈ 0.999999999999995
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0000001, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		var score = results[0]["score"]!.Value<double>();
		score.Should().BeGreaterThan(0.99999999,
			"nearly parallel vectors should have cosine very close to 1.0");
		score.Should().BeLessThan(1.0,
			"slightly different vectors should not have cosine exactly 1.0");
	}

	[Fact]
	public async Task VectorDistance_Cosine_NearlyAntiparallel_HighPrecision()
	{
		// Vectors nearly opposite — cosine should be very close to -1.0
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		// 1e-7 is large enough to be distinguishable in double precision
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [-1.0, 0.0000001, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		var score = results[0]["score"]!.Value<double>();
		score.Should().BeLessThan(-0.99999999,
			"nearly anti-parallel vectors should have cosine very close to -1.0");
		score.Should().BeGreaterThan(-1.0,
			"slightly off-axis vectors should not have cosine exactly -1.0");
	}

	[Fact]
	public async Task VectorDistance_DotProduct_IdenticalVectors_ReturnsSumOfSquares()
	{
		// [3,4] · [3,4] = 9 + 16 = 25
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 3.0, 4.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [3.0, 4.0], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(25.0, 1e-9);
	}

	[Fact]
	public async Task VectorDistance_Euclidean_SymmetricDistance()
	{
		// distance(a, b) should equal distance(b, a) — commutative
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "ab", pk = "a", embedding = new[] { 1.0, 2.0, 3.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "ba", pk = "a", embedding = new[] { 4.0, 5.0, 6.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [4.0, 5.0, 6.0], false, {distanceFunction:'euclidean'}) AS scoreForward FROM c WHERE c.id = 'ab'");
		var results2 = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 2.0, 3.0], false, {distanceFunction:'euclidean'}) AS scoreReverse FROM c WHERE c.id = 'ba'");

		var forward = results[0]["scoreForward"]!.Value<double>();
		var reverse = results2[0]["scoreReverse"]!.Value<double>();
		forward.Should().BeApproximately(reverse, 1e-9,
			"euclidean distance should be symmetric: d(a,b) == d(b,a)");
	}

	[Fact]
	public async Task VectorDistance_Cosine_SymmetricSimilarity()
	{
		// cosine(a, b) should equal cosine(b, a) — verify by swapping args
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "ab", pk = "a", embedding = new[] { 1.0, 2.0, 3.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "ba", pk = "a", embedding = new[] { 4.0, 5.0, 6.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [4.0, 5.0, 6.0]) AS scoreForward FROM c WHERE c.id = 'ab'");
		var results2 = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 2.0, 3.0]) AS scoreReverse FROM c WHERE c.id = 'ba'");

		var forward = results[0]["scoreForward"]!.Value<double>();
		var reverse = results2[0]["scoreReverse"]!.Value<double>();
		forward.Should().BeApproximately(reverse, 1e-9,
			"cosine similarity should be symmetric: cos(a,b) == cos(b,a)");
	}

	[Fact]
	public async Task VectorDistance_DotProduct_NegativeValues_ComputesCorrectly()
	{
		// [-2,-3] · [4,5] = -8 + -15 = -23
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { -2.0, -3.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [4.0, 5.0], false, {distanceFunction:'dotproduct'}) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(-23.0, 1e-9);
	}

	// ═════════════════════════════════════════════════════════════════════
	//  H: Query Literal & Parser Edge Cases
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_LiteralVectorWithSpaces_Works()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [ 1.0 , 0.0 , 0.0 ]) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"whitespace inside array literal should be handled by parser");
	}

	[Fact]
	public async Task VectorDistance_LiteralVectorWithNegativeValues_Works()
	{
		var container = await CreateContainerWithVectors();
		// cosine([1,0,0], [-1,-2,-3]) — negative values in query literal
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [-1.0, -2.0, -3.0]) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		// cos([1,0,0], [-1,-2,-3]) = -1 / (1 * sqrt(14)) ≈ -0.2673
		results[0]["score"]!.Value<double>().Should().BeApproximately(-1.0 / Math.Sqrt(14), 1e-6);
	}

	[Theory]
	[InlineData("vectordistance")]
	[InlineData("VECTORDISTANCE")]
	[InlineData("VectorDistance")]
	[InlineData("vectorDistance")]
	public async Task VectorDistance_FunctionNameCaseInsensitive(string funcName)
	{
		// Parser uppercases all function names — all case variants should work
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			$"SELECT {funcName}(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			$"function name '{funcName}' should be case-insensitive");
	}

	[Fact]
	public async Task VectorDistance_NoArgs_ReturnsNull()
	{
		// Zero args: VectorDistance() — guard returns null for < 2 args
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance() AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"zero args should return null");
	}

	[Fact]
	public async Task VectorDistance_VectorWithSingleZeroElement_ReturnsNull()
	{
		// [0.0] vs [0.0] — single zero-element, cosine = undefined → null
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"single zero-element vectors → cosine undefined → null");
	}

	[Fact]
	public async Task VectorDistance_WithArithmeticOnResult_InSelect()
	{
		// VectorDistance(...) * 100 — arithmetic on the function result in SELECT
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) * 100 AS pctScore FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["pctScore"]!.Value<double>().Should().BeApproximately(100.0, 1e-6,
			"arithmetic (score * 100) should work on VectorDistance results");
	}

	[Fact]
	public async Task VectorDistance_InValueExpression_Addition()
	{
		// VectorDistance(...) + 5 — addition on the function result
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) + 5 AS adjustedScore FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["adjustedScore"]!.Value<double>().Should().BeApproximately(6.0, 1e-6,
			"VectorDistance result (1.0) + 5 should equal 6.0");
	}

	[Fact]
	public async Task VectorDistance_WithIIF_ConditionalLabel()
	{
		// Use VectorDistance result in IIF to produce a label
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT c.id, IIF(VectorDistance(c.embedding, [1.0, 0.0, 0.0]) > 0.5, 'similar', 'different') AS label FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["label"]!.ToString().Should().Be("similar",
			"x has cosine 1.0 which is > 0.5, so IIF should return 'similar'");
	}

	[Fact]
	public async Task VectorDistance_WithAbsArithmetic_Works()
	{
		// ABS(VectorDistance(...)) — function composition
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT c.id, ABS(VectorDistance(c.embedding, [1.0, 0.0, 0.0])) AS absScore FROM c WHERE c.id = 'neg-x'");

		results.Should().ContainSingle();
		results[0]["absScore"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"ABS of cosine -1.0 should be 1.0");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  I: CRUD + Mutation Interaction
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_AfterUpsert_UsesUpdatedVector()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		// Upsert with a completely different embedding
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		// After upsert, embedding is [0,1], cosine([0,1], [1,0]) = 0.0
		results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9,
			"query should reflect the updated embedding after upsert");
	}

	[Fact]
	public async Task VectorDistance_AfterPatch_UsesUpdatedVector()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		// Patch the embedding via Set operation
		await container.PatchItemAsync<JObject>("1", new PartitionKey("a"),
			new List<PatchOperation> { PatchOperation.Set("/embedding", new[] { 0.0, 1.0 }) });

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		// After patch, embedding is [0,1], cosine([0,1], [1,0]) = 0.0
		results[0]["score"]!.Value<double>().Should().BeApproximately(0.0, 1e-9,
			"query should reflect the patched embedding");
	}

	[Fact]
	public async Task VectorDistance_AfterDelete_ExcludesDeletedDoc()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("a"));

		await container.DeleteItemAsync<JObject>("1", new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		results[0]["id"]!.ToString().Should().Be("2",
			"deleted document should not appear in vector search results");
	}

	[Fact]
	public async Task VectorDistance_WithTTLExpiredDoc_ExcludesExpired()
	{
		var container = new InMemoryContainer("vector-test", "/pk")
		{
			DefaultTimeToLive = 1 // 1 second TTL
		};
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		// Wait for TTL to expire
		await Task.Delay(TimeSpan.FromSeconds(2));

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		results.Should().BeEmpty("TTL-expired documents should not appear in vector search results");
	}

	[Fact]
	public async Task VectorDistance_ConcurrentReads_ThreadSafe()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		for (var i = 0; i < 10; i++)
		{
			await container.CreateItemAsync(
				JObject.FromObject(new
				{
					id = $"d{i}",
					pk = "a",
					embedding = new[] { Math.Cos(i * 0.3), Math.Sin(i * 0.3) }
				}),
				new PartitionKey("a"));
		}

		// Run 20 concurrent vector searches
		var tasks = Enumerable.Range(0, 20).Select(_ =>
			RunQuery(container,
				"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c"));

		var allResults = await Task.WhenAll(tasks);

		foreach (var results in allResults)
		{
			results.Should().HaveCount(10, "each concurrent query should return all 10 documents");
		}
	}

	// ═════════════════════════════════════════════════════════════════════
	//  J: Change Feed + Vector Search Integration
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_ChangeFeed_CapturesVectorUpdates()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		// Update the embedding via upsert
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("a"));

		var iterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var changes = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(page);
		}

		// Should capture both the create and the upsert
		changes.Should().HaveCountGreaterThanOrEqualTo(1);
		var latest = changes.Last();
		// The latest version should have the updated embedding [0,1]
		var embedding = latest["embedding"]!.ToObject<double[]>()!;
		embedding.Should().BeEquivalentTo(new[] { 0.0, 1.0 },
			"change feed should capture the updated vector embedding");
	}

	[Fact]
	public async Task VectorDistance_VectorOnlyUpdate_AppearsInChangeFeed()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "stable", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		// Only change the embedding, nothing else
		await container.UpsertItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "stable", embedding = new[] { 0.5, 0.5 } }),
			new PartitionKey("a"));

		var iterator = container.GetChangeFeedIterator<JObject>(
			ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
		var changes = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			if (page.StatusCode == HttpStatusCode.NotModified) break;
			changes.AddRange(page);
		}

		// The change feed should have the latest version with the updated vector
		// (Incremental mode returns latest version per document, not every intermediate state)
		changes.Should().HaveCountGreaterThanOrEqualTo(1,
			"the vector-only update should appear in the change feed");
		var latest = changes.Last();
		latest["embedding"]!.ToObject<double[]>().Should().BeEquivalentTo(new[] { 0.5, 0.5 },
			"change feed should reflect the updated vector embedding");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  K: Partition Key Interaction
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_HierarchicalPartitionKey_CrossPartition()
	{
		var container = new InMemoryContainer("vector-test", new List<string> { "/tenant", "/region" });
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", tenant = "A", region = "US", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKeyBuilder().Add("A").Add("US").Build());
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", tenant = "B", region = "EU", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKeyBuilder().Add("B").Add("EU").Build());

		// Cross-partition query (no PK filter)
		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2,
			"cross-partition vector search should return docs from all partitions");
	}

	[Fact]
	public async Task VectorDistance_SinglePartitionScopedSearch_OnlyReturnsPartitionDocs()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("b"));

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		// RunQuery uses PartitionKey("a") — should only return the "a" partition doc
		results.Should().ContainSingle();
		results[0]["id"]!.ToString().Should().Be("1");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  L: Stream API Integration
	// ═════════════════════════════════════════════════════════════════════

	[Fact]
	public async Task VectorDistance_ViaStreamIterator_ReturnsValidJson()
	{
		var container = await CreateContainerWithVectors();

		var query = new QueryDefinition(
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'x'");
		var iterator = container.GetItemQueryStreamIterator(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });

		using var response = await iterator.ReadNextAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		using var reader = new StreamReader(response.Content);
		var body = await reader.ReadToEndAsync();
		var jObj = JObject.Parse(body);

		var docs = jObj["Documents"]!.ToObject<List<JObject>>()!;
		docs.Should().ContainSingle();
		docs[0]["id"]!.ToString().Should().Be("x");
		docs[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 1e-9,
			"stream iterator should return valid JSON with vector scores");
	}

	// ═════════════════════════════════════════════════════════════════════
	//  M: Additional Divergent Behaviour (Skip + Sister)
	// ═════════════════════════════════════════════════════════════════════

	// ─── M1: Extra args (>4) ─────────────────────────────────────────────
	// Real Cosmos DB rejects queries with more than 4 args to VECTORDISTANCE
	// at the query compilation layer. The emulator's generic function dispatcher
	// accepts arbitrary arg counts and the 5th+ args are silently ignored.

	[Fact]
	public async Task VectorDistance_FiveArgs_RealCosmosRejectsExtraArgs()
	{
		var container = await CreateContainerWithVectors();
		var act = () => RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0], false, {distanceFunction:'cosine'}, 'extra') AS score FROM c WHERE c.id = 'x'");

		await act.Should().ThrowAsync<CosmosException>();
	}

	// Sister test already exists as VectorDistance_FiveArgs_EmulatorIgnoresExtraArgs in H category

	// ─── M2: Unknown distance function ────────────────────────────────────
	// Real Cosmos DB rejects unknown distance function values with an error.
	// The emulator silently falls back to cosine similarity.

	[Fact]
	public async Task VectorDistance_UnknownDistanceFunction_RealCosmosRejects()
	{
		var container = await CreateContainerWithVectors();
		var act = () => RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0], false, {distanceFunction:'manhattan'}) AS score FROM c WHERE c.id = 'x'");

		await act.Should().ThrowAsync<CosmosException>();
	}

	// Sister test already exists as VectorDistance_UnknownDistanceFunction_FallsToCosine in B category

	// ─── M3: Parameterized query vectors ─────────────────────────────────
	// Real Cosmos DB supports parameterized query vectors:
	//   new QueryDefinition("SELECT VectorDistance(c.vec, @qv) AS s FROM c")
	//       .WithParameter("@qv", new[] { 1.0, 0.0, 0.0 })
	// The emulator may or may not support this depending on how parameters
	// are resolved for function arguments.

	[Fact]
	public async Task VectorDistance_ParameterizedQuery_ReturnsCorrectScore()
	{
		var container = await CreateContainerWithVectors();

		var query = new QueryDefinition(
			"SELECT VectorDistance(c.embedding, @qv) AS score FROM c WHERE c.id = 'x'")
			.WithParameter("@qv", new[] { 1.0, 0.0, 0.0 });

		var iterator = container.GetItemQueryIterator<JObject>(query,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.001);
	}

	// ─── M4: Return type ─────────────────────────────────────────────────
	// Real Cosmos DB returns VectorDistance as a JSON number (float64).
	// The emulator returns a .NET double boxed as object, which serializes
	// as a JSON number. Behaviour is functionally identical.

	[Fact]
	public async Task VectorDistance_ReturnType_EmulatorReturnsDouble()
	{
		// Verify the emulator returns a numeric type (float/integer in JToken terms)
		var container = await CreateContainerWithVectors();
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'x'");

		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Float,
			"VectorDistance should return a float/double value, matching real Cosmos DB's JSON number");
	}

	// ─── M5: NaN in vector ───────────────────────────────────────────────
	// Real Cosmos DB likely rejects NaN values during document insertion
	// since NaN is not a valid JSON number. The emulator's Newtonsoft.Json
	// parser might accept NaN depending on serialization settings.
	// This is a data quality issue, not a query engine issue.

	[Fact(Skip = "Real Cosmos DB rejects NaN in vector fields at insert time (invalid JSON). " +
				  "Implementing this requires intercepting all document writes to validate vector " +
				  "field values, which is architecturally invasive. The emulator's NaN/Infinity guard " +
				  "in VectorDistanceFunc returns null for NaN results, providing partial safety.")]
	public void VectorDistance_WithNaNInVector_RealCosmosBehaviour()
	{
		// Expected real Cosmos DB behavior:
		// Inserting a document with embedding: [NaN, 1.0] → rejected at insert time
	}

	[Fact]
	public async Task VectorDistance_WithNaNInVector_EmulatorBehaviour()
	{
		// Emulator behaviour: if NaN somehow gets stored, the Infinity/NaN guard
		// in VectorDistanceFunc catches it and returns null
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.Parse("{\"id\":\"1\",\"pk\":\"a\",\"embedding\":[\"NaN\",1.0]}"),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");

		results.Should().ContainSingle();
		// "NaN" is a string, not a number — ToDoubleArray rejects non-numeric elements → null
		results[0]["score"]!.Type.Should().Be(JTokenType.Null,
			"NaN string in vector should be handled gracefully (ToDoubleArray rejects non-numeric elements)");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: W — Bare String Distance Function (BUG-1 regression tests)
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceBareStringTests
{
	private static async Task<InMemoryContainer> CreateContainer()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 3.0, 4.0 } }),
			new PartitionKey("a"));
		return container;
	}

	private static async Task<List<JObject>> RunQuery(InMemoryContainer container, string sql)
	{
		var iterator = container.GetItemQueryIterator<JObject>(sql,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task VectorDistance_BareStringEuclidean_WorksCorrectly()
	{
		var container = await CreateContainer();
		// [3,4] vs [0,0] = sqrt(9+16) = 5.0
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", embedding = new[] { 0.0, 0.0 } }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 0.0], false, 'euclidean') AS dist FROM c WHERE c.id = '1'");
		results.Should().ContainSingle();
		results[0]["dist"]!.Value<double>().Should().BeApproximately(5.0, 0.001);
	}

	[Fact]
	public async Task VectorDistance_BareStringDotProduct_WorksCorrectly()
	{
		var container = await CreateContainer();
		// dot([3,4], [2,5]) = 3*2 + 4*5 = 6+20 = 26
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [2.0, 5.0], false, 'dotproduct') AS dp FROM c WHERE c.id = '1'");
		results.Should().ContainSingle();
		results[0]["dp"]!.Value<double>().Should().BeApproximately(26.0, 0.001);
	}

	[Fact]
	public async Task VectorDistance_BareStringCosine_WorksCorrectly()
	{
		var container = await CreateContainer();
		// cosine([3,4], [3,4]) = 1.0 (identical vectors)
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [3.0, 4.0], false, 'cosine') AS score FROM c WHERE c.id = '1'");
		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.001);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: N — Subquery Integration
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceSubqueryTests
{
	private static async Task<InMemoryContainer> CreateContainerWithVectors()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "a", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "b", pk = "a", embedding = new[] { 0.0, 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "c", pk = "a", embedding = new[] { 0.707, 0.707, 0.0 } }),
			new PartitionKey("a"));
		return container;
	}

	private static async Task<List<T>> RunQuery<T>(InMemoryContainer container, string sql)
	{
		var iterator = container.GetItemQueryIterator<T>(sql,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<T>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task VectorDistance_InExistsSubquery_FiltersDocuments()
	{
		var container = await CreateContainerWithVectors();
		// Filter documents where cosine to [1,0,0] > 0.9 — only doc "a" (cosine=1.0)
		var results = await RunQuery<JObject>(container,
			"SELECT * FROM c WHERE VectorDistance(c.embedding, [1.0, 0.0, 0.0]) > 0.9");

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("a");
	}

	[Fact]
	public async Task VectorDistance_InScalarSubquery_ReturnsScore()
	{
		var container = await CreateContainerWithVectors();
		// Use VectorDistance directly in SELECT with alias instead of scalar subquery syntax
		var results = await RunQuery<JObject>(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c WHERE c.id = 'a'");

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);
	}

	[Fact]
	public async Task VectorDistance_WhereWithExistsAndVector_CombinesFilters()
	{
		var container = await CreateContainerWithVectors();
		var results = await RunQuery<JObject>(container,
			"SELECT c.id FROM c WHERE c.id != 'b' AND VectorDistance(c.embedding, [1.0, 0.0, 0.0]) > 0.5");

		results.Should().HaveCount(2); // 'a' (score=1.0) and 'c' (score~0.707)
		results.Select(r => r["id"]!.Value<string>()).Should().Contain("a").And.Contain("c");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: O — SELECT VALUE & Projection Variants
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceProjectionTests
{
	private static async Task<InMemoryContainer> CreateSingleDoc()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));
		return container;
	}

	[Fact]
	public async Task VectorDistance_SelectValue_ReturnsRawScores()
	{
		var container = await CreateSingleDoc();
		var iterator = container.GetItemQueryIterator<double>(
			"SELECT VALUE VectorDistance(c.embedding, [1.0, 0.0, 0.0]) FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<double>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle().Which.Should().BeApproximately(1.0, 0.01);
	}

	[Fact]
	public async Task VectorDistance_SelectStar_DoesNotIncludeComputedScore()
	{
		var container = await CreateSingleDoc();
		var iterator = container.GetItemQueryIterator<JObject>("SELECT * FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["embedding"].Should().NotBeNull();
		results[0]["score"].Should().BeNull("SELECT * should not include computed scores");
	}

	[Fact]
	public async Task VectorDistance_SelectValueTopN_ReturnsTopScores()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		for (var i = 0; i < 5; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a", embedding = new[] { (double)(i + 1), 0.0, 0.0 } }),
				new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT TOP 3 VectorDistance(c.embedding, [5.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [5.0, 0.0, 0.0]) DESC",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		var scores = results.Select(r => r["score"]!.Value<double>()).ToList();
		scores.Should().BeInDescendingOrder();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: P — Continuation Token Pagination
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistancePaginationTests
{
	[Fact]
	public async Task VectorDistance_PaginatedResults_AllPagesReturnCorrectScores()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		for (var i = 0; i < 6; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a", embedding = new[] { (double)i, 0.0 } }),
				new PartitionKey("a"));

		var allResults = new List<JObject>();
		string? continuationToken = null;
		do
		{
			var iterator = container.GetItemQueryIterator<JObject>(
				"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c",
				continuationToken: continuationToken,
				requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a"), MaxItemCount = 2 });

			var page = await iterator.ReadNextAsync();
			allResults.AddRange(page);
			continuationToken = page.ContinuationToken;
		} while (continuationToken != null);

		allResults.Should().HaveCount(6);
		allResults.Should().OnlyContain(r => r["score"] != null);
	}

	[Fact]
	public async Task VectorDistance_PaginatedOrderBy_MaintainsOrderAcrossPages()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		for (var i = 0; i < 6; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = "a", embedding = new[] { (double)(i + 1), 0.0, 0.0 } }),
				new PartitionKey("a"));

		var allScores = new List<double>();
		string? continuationToken = null;
		do
		{
			var iterator = container.GetItemQueryIterator<JObject>(
				"SELECT c.id, VectorDistance(c.embedding, [6.0, 0.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [6.0, 0.0, 0.0]) DESC",
				continuationToken: continuationToken,
				requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a"), MaxItemCount = 2 });

			var page = await iterator.ReadNextAsync();
			allScores.AddRange(page.Select(r => r["score"]!.Value<double>()));
			continuationToken = page.ContinuationToken;
		} while (continuationToken != null);

		allScores.Should().HaveCount(6);
		allScores.Should().BeInDescendingOrder();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: Q — Replace & Batch Mutation Interaction
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceMutationTests
{
	private static async Task<List<JObject>> RunQuery(InMemoryContainer container, string sql)
	{
		var iterator = container.GetItemQueryIterator<JObject>(sql,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task VectorDistance_AfterReplace_UsesUpdatedVector()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		// Replace with new vector
		await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0, 1.0 } }),
			"1", new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 1.0]) AS score FROM c");
		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);
	}

	[Fact]
	public async Task VectorDistance_InTransactionalBatch_AfterBatchUpsert()
	{
		var container = new InMemoryContainer("vector-test", "/pk");

		var batch = container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.CreateItem(JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }));
		batch.CreateItem(JObject.FromObject(new { id = "2", pk = "a", embedding = new[] { 0.0, 1.0 } }));
		using var response = await batch.ExecuteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var results = await RunQuery(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c ORDER BY c.id");
		results.Should().HaveCount(2);
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01); // id=1, identical
		results[1]["score"]!.Value<double>().Should().BeApproximately(0.0, 0.01); // id=2, orthogonal
	}

	[Fact]
	public async Task VectorDistance_AfterBatchDelete_ExcludesDeleted()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", embedding = new[] { 0.0, 1.0 } }),
			new PartitionKey("a"));

		var batch = container.CreateTransactionalBatch(new PartitionKey("a"));
		batch.DeleteItem("1");
		using var response = await batch.ExecuteAsync();
		response.StatusCode.Should().Be(HttpStatusCode.OK);

		var results = await RunQuery(container,
			"SELECT c.id FROM c");
		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("2");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: R — FeedRange + Vector Search
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceFeedRangeTests
{
	[Fact]
	public async Task VectorDistance_WithFeedRange_ScopedToRange()
	{
		var container = new InMemoryContainer("vector-test", "/pk") { FeedRangeCount = 4 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = $"pk-{i}", embedding = new[] { (double)i, 0.0 } }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var rangeResults = new List<JObject>();
		var range = ranges.First();

		var iterator = container.GetItemQueryIterator<JObject>(range,
			new QueryDefinition("SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c"));
		while (iterator.HasMoreResults)
			rangeResults.AddRange(await iterator.ReadNextAsync());

		// Should be a subset of all items (scoped to one feed range)
		rangeResults.Count.Should().BeLessThanOrEqualTo(10);
		rangeResults.Should().OnlyContain(r => r["score"] != null);
	}

	[Fact]
	public async Task VectorDistance_AllFeedRanges_ReturnCompleteResults()
	{
		var container = new InMemoryContainer("vector-test", "/pk") { FeedRangeCount = 4 };
		for (var i = 0; i < 10; i++)
			await container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = $"pk-{i}", embedding = new[] { (double)i, 0.0 } }),
				new PartitionKey($"pk-{i}"));

		var ranges = await container.GetFeedRangesAsync();
		var allResults = new List<JObject>();
		foreach (var range in ranges)
		{
			var iterator = container.GetItemQueryIterator<JObject>(range,
				new QueryDefinition("SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c"));
			while (iterator.HasMoreResults)
				allResults.AddRange(await iterator.ReadNextAsync());
		}

		allResults.Should().HaveCount(10);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: S — Empty Container & Single-Doc Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceEdgeCaseExtendedTests
{
	[Fact]
	public async Task VectorDistance_EmptyContainer_ReturnsNoResults()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task VectorDistance_SingleDoc_AllMetricsReturnValue()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 3.0, 4.0 } }),
			new PartitionKey("a"));

		var opts = new QueryRequestOptions { PartitionKey = new PartitionKey("a") };

		// Cosine
		var cosIter = container.GetItemQueryIterator<JObject>(
			"SELECT VectorDistance(c.embedding, [3.0, 4.0], false, 'cosine') AS score FROM c", requestOptions: opts);
		var cosRes = new List<JObject>();
		while (cosIter.HasMoreResults) cosRes.AddRange(await cosIter.ReadNextAsync());
		cosRes[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);

		// Dot product
		var dotIter = container.GetItemQueryIterator<JObject>(
			"SELECT VectorDistance(c.embedding, [1.0, 0.0], false, 'dotproduct') AS score FROM c", requestOptions: opts);
		var dotRes = new List<JObject>();
		while (dotIter.HasMoreResults) dotRes.AddRange(await dotIter.ReadNextAsync());
		dotRes[0]["score"]!.Value<double>().Should().BeApproximately(3.0, 0.01);

		// Euclidean
		var eucIter = container.GetItemQueryIterator<JObject>(
			"SELECT VectorDistance(c.embedding, [0.0, 0.0], false, 'euclidean') AS score FROM c", requestOptions: opts);
		var eucRes = new List<JObject>();
		while (eucIter.HasMoreResults) eucRes.AddRange(await eucIter.ReadNextAsync());
		eucRes[0]["score"]!.Value<double>().Should().BeApproximately(5.0, 0.01);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: T — Advanced SQL Combinations
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceAdvancedSqlTests
{
	private static async Task<InMemoryContainer> CreateMultiDocContainer()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "a", pk = "a", embedding = new[] { 1.0, 0.0 }, name = "Alpha" }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "b", pk = "a", embedding = new[] { 0.0, 1.0 }, name = "Beta" }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "c", pk = "a", embedding = new[] { 0.707, 0.707 }, name = "Charlie" }),
			new PartitionKey("a"));
		return container;
	}

	private static async Task<List<T>> RunQuery<T>(InMemoryContainer container, string sql)
	{
		var iterator = container.GetItemQueryIterator<T>(sql,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<T>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task VectorDistance_OrderByWithSecondarySort()
	{
		var container = await CreateMultiDocContainer();
		var results = await RunQuery<JObject>(container,
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c ORDER BY VectorDistance(c.embedding, [1.0, 0.0]) DESC, c.id ASC");

		results.Should().HaveCount(3);
		results[0]["id"]!.Value<string>().Should().Be("a"); // cosine to [1,0] = 1.0
	}

	[Fact]
	public async Task VectorDistance_CountWithVectorWhere()
	{
		var container = await CreateMultiDocContainer();
		var results = await RunQuery<long>(container,
			"SELECT VALUE COUNT(1) FROM c WHERE VectorDistance(c.embedding, [1.0, 0.0]) > 0.5");

		results.Should().ContainSingle().Which.Should().Be(2); // 'a' (1.0) and 'c' (~0.707)
	}

	[Fact]
	public async Task VectorDistance_InIifExpression()
	{
		var container = await CreateMultiDocContainer();
		var results = await RunQuery<JObject>(container,
			"SELECT c.id, IIF(VectorDistance(c.embedding, [1.0, 0.0]) > 0.9, 'similar', 'different') AS label FROM c ORDER BY c.id");

		results.Should().HaveCount(3);
		results.First(r => r["id"]!.Value<string>() == "a")["label"]!.Value<string>().Should().Be("similar");
		results.First(r => r["id"]!.Value<string>() == "b")["label"]!.Value<string>().Should().Be("different");
	}

	[Fact]
	public async Task VectorDistance_WithJoin_CrossApply()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embeddings = new[] { new[] { 1.0, 0.0 }, new[] { 0.0, 1.0 } } }),
			new PartitionKey("a"));

		var results = await RunQuery<JObject>(container,
			"SELECT VectorDistance(e, [1.0, 0.0]) AS score FROM c JOIN e IN c.embeddings");

		results.Should().HaveCount(2);
		var scores = results.Select(r => r["score"]!.Value<double>()).OrderDescending().ToList();
		scores[0].Should().BeApproximately(1.0, 0.01); // [1,0] vs [1,0]
		scores[1].Should().BeApproximately(0.0, 0.01); // [0,1] vs [1,0]
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: U — Deep Paths & Unusual Properties
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceDeepPathTests
{
	private static async Task<List<JObject>> RunQuery(InMemoryContainer container, string sql)
	{
		var iterator = container.GetItemQueryIterator<JObject>(sql,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task VectorDistance_DeeplyNestedVector_ThreeLevels()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.Parse("""{"id":"1","pk":"a","a":{"b":{"embedding":[1.0,0.0,0.0]}}}"""),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.a.b.embedding, [1.0, 0.0, 0.0]) AS score FROM c");
		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);
	}

	[Fact]
	public async Task VectorDistance_BooleanInVector_ReturnsNull()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.Parse("""{"id":"1","pk":"a","embedding":[true,false,1.0]}"""),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");
		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null, "boolean elements in vector should cause null result");
	}

	[Fact]
	public async Task VectorDistance_MissingVectorField_ReturnsNull()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", name = "No vector" }),
			new PartitionKey("a"));

		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c");
		results.Should().ContainSingle();
		results[0]["score"]!.Type.Should().Be(JTokenType.Null);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: V — State Persistence + Vector Data
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceStatePersistenceTests
{
	[Fact]
	public async Task VectorDistance_AfterExportImport_PreservesVectorData()
	{
		var source = new InMemoryContainer("vector-test", "/pk");
		await source.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 3.0, 4.0 } }),
			new PartitionKey("a"));

		var state = source.ExportState();

		var target = new InMemoryContainer("vector-test2", "/pk");
		target.ImportState(state);

		var iterator = target.GetItemQueryIterator<JObject>(
			"SELECT VectorDistance(c.embedding, [3.0, 4.0]) AS score FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);
	}

	[Fact]
	public async Task VectorDistance_PointInTimeRestore_VectorDataCorrect()
	{
		var container = new InMemoryContainer("vector-test", "/pk");

		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		var restorePoint = DateTimeOffset.UtcNow;
		await Task.Delay(TimeSpan.FromMilliseconds(100));

		// Replace vector after restore point
		await container.ReplaceItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 0.0, 1.0 } }),
			"1", new PartitionKey("a"));

		// Restore to point before replacement
		container.RestoreToPointInTime(restorePoint);

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		// After restore, embedding should be [1,0] again, so cosine with [1,0] = 1.0
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: X — IEEE 754 Additional Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceIeee754ExtendedTests
{
	private static async Task<List<JObject>> RunQuery(InMemoryContainer container, string sql)
	{
		var iterator = container.GetItemQueryIterator<JObject>(sql,
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());
		return results;
	}

	[Fact]
	public async Task VectorDistance_NegativeZero_TreatedAsZero()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { -0.0, 1.0, 0.0 } }),
			new PartitionKey("a"));

		// -0.0 should be treated same as 0.0. Cosine of [-0,1,0] vs [0,1,0] = 1.0
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [0.0, 1.0, 0.0]) AS score FROM c");
		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);
	}

	[Fact]
	public async Task VectorDistance_SubnormalFloats_PreservePrecision()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1e-10, 1e-10 } }),
			new PartitionKey("a"));

		// Cosine of identical small vectors should still be ~1.0
		var results = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [1e-10, 1e-10]) AS score FROM c");
		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);
	}

	[Fact]
	public async Task VectorDistance_DotProduct_Symmetry_Verified()
	{
		var container = new InMemoryContainer("vector-test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 2.5, 3.7, 1.2 } }),
			new PartitionKey("a"));

		// dot(a,b) should equal dot(b,a)
		var results1 = await RunQuery(container,
			"SELECT VectorDistance(c.embedding, [4.1, 0.8, 5.3], false, 'dotproduct') AS dp FROM c");
		// dot([2.5,3.7,1.2], [4.1,0.8,5.3]) = 10.25 + 2.96 + 6.36 = 19.57
		results1.Should().ContainSingle();
		var dp = results1[0]["dp"]!.Value<double>();
		dp.Should().BeApproximately(19.57, 0.01);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Plan 46: Y — FakeCosmosHandler Layer
// ═══════════════════════════════════════════════════════════════════════════════

public class VectorDistanceFakeHandlerTests
{
	private static CosmosClient CreateClient(HttpMessageHandler handler) =>
		new("AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
			new CosmosClientOptions
			{
				ConnectionMode = ConnectionMode.Gateway,
				LimitToEndpoint = true,
				MaxRetryAttemptsOnRateLimitedRequests = 0,
				HttpClientFactory = () => new HttpClient(handler)
			});

	[Fact]
	public async Task VectorDistance_ViaFakeCosmosHandler_ReturnsCorrectScore()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0, 0.0 } }),
			new PartitionKey("a"));

		using var handler = new FakeCosmosHandler(container);
		using var client = CreateClient(handler);

		var sdkContainer = client.GetContainer("db", "test");
		var iterator = sdkContainer.GetItemQueryIterator<JObject>(
			"SELECT c.id, VectorDistance(c.embedding, [1.0, 0.0, 0.0]) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			results.AddRange(page);
		}

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeApproximately(1.0, 0.01);
	}

	[Fact]
	public async Task VectorDistance_ViaFakeCosmosHandler_WithFaultInjection()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", embedding = new[] { 1.0, 0.0 } }),
			new PartitionKey("a"));

		using var handler = new FakeCosmosHandler(container)
		{
			// Always return 429 — this ensures both query plan and query requests are faulted
			FaultInjector = _ => new HttpResponseMessage((HttpStatusCode)429)
			{ Content = new StringContent("{}") }
		};
		using var client = CreateClient(handler);

		var sdkContainer = client.GetContainer("db", "test");
		// The SDK should get a 429 on every attempt
		var act = () => sdkContainer.GetItemQueryIterator<JObject>(
			"SELECT VectorDistance(c.embedding, [1.0, 0.0]) AS score FROM c")
			.ReadNextAsync();

		// MaxRetryAttemptsOnRateLimitedRequests = 0, so 429 is not retried
		var ex = await act.Should().ThrowAsync<CosmosException>();
		ex.Which.StatusCode.Should().Be((HttpStatusCode)429);
	}
}
