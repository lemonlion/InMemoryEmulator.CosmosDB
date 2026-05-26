using System.Net;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

// ════════════════════════════════════════════════════════════════════════════════
// Full-Text Search Functions — Approximate implementation using case-insensitive
// word matching instead of the real Cosmos DB NLP tokenizer + BM25 engine.
//
// Real Cosmos DB requires a full-text indexing policy on the container and performs
// stemming, stop-word removal, and BM25 relevance scoring.  The emulator uses
// simple case-insensitive Contains/word matching as an approximation so that
// queries exercising these functions compile and produce reasonable results.
//
// Reference: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/full-text-search
// ════════════════════════════════════════════════════════════════════════════════

public class FullTextContainsTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContains_MatchingTerm_ReturnsDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", description = "The quick brown fox jumps over the lazy dog" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.description, 'fox')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task FullTextContains_NonMatchingTerm_ReturnsEmpty()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", description = "The quick brown fox" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.description, 'elephant')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task FullTextContains_IsCaseInsensitive()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", description = "Cosmos Database Service" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.description, 'cosmos')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContains_NullField_ReturnsEmpty()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.description, 'test')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task FullTextContains_MultipleDocuments_FiltersCorrectly()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB is a NoSQL database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "SQL Server is a relational database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", text = "Azure Functions is serverless" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'database')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results.Select(r => r["id"]!.Value<string>()).Should().BeEquivalentTo(["1", "2"]);
	}

	[Fact]
	public async Task FullTextContains_WithPartitionKey_RespectsFilter()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "Hello cosmos" }),
			new PartitionKey("b"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'hello')",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}
}

public class FullTextContainsAllTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContainsAll_AllTermsPresent_ReturnsDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB is a fast NoSQL database" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, 'cosmos', 'database')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAll_SomeTermsMissing_ReturnsEmpty()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB is fast" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, 'cosmos', 'elephant')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task FullTextContainsAll_SingleTerm_WorksLikeContains()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, 'world')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAll_IsCaseInsensitive()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "AZURE COSMOS DATABASE" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, 'azure', 'cosmos', 'database')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}
}

public class FullTextContainsAnyTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContainsAny_OneTermMatches_ReturnsDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'elephant', 'cosmos')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAny_NoTermsMatch_ReturnsEmpty()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'elephant', 'giraffe')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task FullTextContainsAny_AllTermsMatch_ReturnsDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB database" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'cosmos', 'database')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAny_FiltersMultipleDocumentsCorrectly()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "NoSQL database on Azure" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "SQL relational engine" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", text = "Graph processing system" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'database', 'engine')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results.Select(r => r["id"]!.Value<string>()).Should().BeEquivalentTo(["1", "2"]);
	}
}

public class FullTextScoreTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextScore_ReturnsNumericValue()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database is a great database" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT FullTextScore(c.text, ['database', 'cosmos']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		var score = results[0]["score"]!.Value<double>();
		score.Should().BeGreaterThan(0);
	}

	[Fact]
	public async Task FullTextScore_MoreMatchingTerms_HigherScore()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database is fast" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos database is a great database service for cosmos" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['database', 'cosmos']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		var scores = results.ToDictionary(r => r["id"]!.Value<string>()!, r => r["score"]!.Value<double>());
		scores["2"].Should().BeGreaterThan(scores["1"],
			"document 2 has more occurrences of the search terms");
	}

	[Fact]
	public async Task FullTextScore_NoMatchingTerms_ReturnsZero()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT FullTextScore(c.text, ['elephant', 'giraffe']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().Be(0);
	}
}

public class OrderByRankTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task OrderByRank_SortsByFullTextScoreDescending()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure cloud platform" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos database is a database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", text = "Cosmos database service for database storage of database records" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY RANK FullTextScore(c.text, ['database', 'cosmos'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(3);
		// Document 3 has most occurrences (3x database + 1x cosmos = 4), should be first
		// Document 2 has (2x database + 1x cosmos = 3), should be second
		// Document 1 has 0 matches, should be last
		results[0]["id"]!.Value<string>().Should().Be("3");
		results[1]["id"]!.Value<string>().Should().Be("2");
		results[2]["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task OrderByRank_WithWhereClause_FiltersAndSorts()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos DB overview", category = "docs" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos DB database guide for database", category = "docs" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", text = "Unrelated topic", category = "other" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE c.category = 'docs' ORDER BY RANK FullTextScore(c.text, ['database'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results[0]["id"]!.Value<string>().Should().Be("2");
		results[1]["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task OrderByRank_WithTopClause_LimitsResults()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Low relevance" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "database database database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", text = "database service" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT TOP 2 * FROM c ORDER BY RANK FullTextScore(c.text, ['database'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results[0]["id"]!.Value<string>().Should().Be("2");
		results[1]["id"]!.Value<string>().Should().Be("3");
	}

	[Fact]
	public async Task OrderByRank_WithSelectScore_ReturnsSortedWithScores()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos database cosmos database cosmos" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['cosmos', 'database']) AS score FROM c ORDER BY RANK FullTextScore(c.text, ['cosmos', 'database'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results[0]["id"]!.Value<string>().Should().Be("2");
		var score1 = results[0]["score"]!.Value<double>();
		var score2 = results[1]["score"]!.Value<double>();
		score1.Should().BeGreaterThan(score2);
	}
}

public class FullTextCombinedTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContains_CombinedWithOtherWhereConditions()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database", active = true }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "Cosmos database", active = false }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'cosmos') AND c.active = true");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task FullTextContainsAny_CombinedWithContainsAll()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", title = "Azure Cosmos", body = "A fast database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", title = "Azure Functions", body = "Serverless compute" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.title, 'cosmos', 'functions') AND FullTextContainsAll(c.body, 'fast', 'database')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Divergent Behaviour Tests
//
// These tests document where the emulator's approximate full-text search
// intentionally differs from real Cosmos DB.
// ════════════════════════════════════════════════════════════════════════════════

public class FullTextSearchDivergentBehaviorTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	/// <summary>
	/// DIVERGENT BEHAVIOUR #1 — No full-text indexing policy required.
	///
	/// Real Cosmos DB: Calling FULLTEXTCONTAINS on a container without a full-text
	/// indexing policy throws a BadRequest error:
	///   "Full-text search queries are not supported for accounts or containers
	///    without a full-text index policy."
	///
	/// InMemoryEmulator: Full-text functions work on any container without any
	/// indexing configuration. This is intentional — the emulator approximates
	/// the function behaviour using simple string matching so tests can exercise
	/// their query logic without needing to configure indexing policies.
	/// </summary>
	[Fact]
	public async Task FullTextContains_WorksWithoutFullTextIndexPolicy()
	{
		// No full-text index policy configured on this container — real Cosmos would reject this.
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
			new PartitionKey("a"));

		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'hello')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Emulator returns results; real Cosmos DB would throw BadRequest
		results.Should().ContainSingle();
	}

	/// <summary>
	/// DIVERGENT BEHAVIOUR #2 — Naive term-frequency scoring vs BM25.
	///
	/// Real Cosmos DB: FULLTEXTSCORE uses the BM25 (Best Matching 25) algorithm
	/// which considers term frequency, inverse document frequency, and document
	/// length normalization. A term appearing 3x in a short document scores
	/// differently than 3x in a long document.
	///
	/// InMemoryEmulator: Uses a simple case-insensitive count of how many times
	/// each search term appears in the field text. The score is the sum of all
	/// term occurrence counts. No IDF or length normalization is applied.
	///
	/// This means:
	/// - Relative ordering is usually correct (more occurrences = higher score)
	/// - Absolute score values will differ significantly
	/// - Edge cases with document length variation may produce different orderings
	///   since BM25 penalises very long documents
	/// </summary>
	[Fact]
	public async Task FullTextScore_UsesNaiveTermFrequency_NotBM25()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "short", pk = "a", text = "database database database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new
			{
				id = "long",
				pk = "a",
				text = "database database database plus many other words to make this a much longer document that would score differently under BM25 length normalization"
			}),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['database']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Both have 3 occurrences of "database" — emulator scores them equally.
		// Real BM25 would score the shorter document higher due to length normalization.
		var scores = results.ToDictionary(r => r["id"]!.Value<string>()!, r => r["score"]!.Value<double>());
		scores["short"].Should().Be(scores["long"],
			"the emulator uses naive term-frequency counting without BM25 length normalization");
	}

	/// <summary>
	/// DIVERGENT BEHAVIOUR #3 — No tokenization/stemming.
	///
	/// Real Cosmos DB: The full-text engine tokenizes text and applies stemming,
	/// so searching for "running" would match "runs", "ran", "runner" etc.
	///
	/// InMemoryEmulator: Uses literal case-insensitive substring matching, so
	/// "running" only matches text containing the exact substring "running".
	/// </summary>
	[Fact]
	public async Task FullTextContains_NoStemming_LiteralMatchOnly()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "The runner runs quickly" }),
			new PartitionKey("a"));

		// Search for "running" — real Cosmos with stemming might match "runner"/"runs"
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'running')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Emulator does literal matching — "running" is not a substring of "The runner runs quickly"
		results.Should().BeEmpty(
			"the emulator uses literal substring matching without stemming");
	}
}

public class FullTextSearchSkippedTests
{
	/// <summary>
	/// Real Cosmos DB requires a full-text indexing policy on the container definition.
	/// The emulator does not require or validate this configuration.
	///
	/// In real Cosmos DB, you must configure the container with a full-text policy:
	/// <code>
	/// new ContainerProperties("container", "/pk")
	/// {
	///     FullTextPolicy = new FullTextPolicy
	///     {
	///         DefaultLanguage = "en-US",
	///         FullTextPaths = { new FullTextPath { Path = "/text", Language = "en-US" } }
	///     },
	///     IndexingPolicy = new IndexingPolicy
	///     {
	///         FullTextIndexes = { new FullTextIndexPath { Path = "/text" } }
	///     }
	/// }
	/// </code>
	///
	/// Without this configuration, all FULLTEXT* queries return HTTP 400.
	/// The emulator skips this validation entirely to keep test setup simple.
	/// </summary>
	[Fact(Skip = "Full-text indexing policy validation is not implemented. " +
		"Real Cosmos DB requires a FullTextPolicy and FullTextIndexes on the container's " +
		"IndexingPolicy. Without it, all FULLTEXT* queries return HTTP 400 BadRequest. " +
		"The emulator intentionally skips this validation so queries work without " +
		"indexing policy configuration. See FullTextSearchDivergentBehaviorTests for details.")]
	public async Task FullTextContains_WithoutFullTextIndex_ShouldThrow400()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Hello" }),
			new PartitionKey("a"));

		// In real Cosmos DB, this would throw CosmosException with StatusCode 400
		var act = async () =>
		{
			var iterator = container.GetItemQueryIterator<JObject>(
				"SELECT * FROM c WHERE FullTextContains(c.text, 'hello')");
			while (iterator.HasMoreResults)
				await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	/// <summary>
	/// DIVERGENT BEHAVIOUR — FULLTEXTSCORE allowed in SELECT projection.
	///
	/// Real Cosmos DB: The FULLTEXTSCORE function "can't be part of a projection
	/// (for example, SELECT FullTextScore(c.text, 'keyword') AS Score FROM c is invalid)."
	/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/fulltextscore#remarks
	/// It can ONLY be used inside ORDER BY RANK or as an argument to RRF.
	///
	/// InMemoryEmulator: Allows FULLTEXTSCORE in any SELECT projection, which is
	/// useful for debugging relevance scores during development. This will succeed
	/// in the emulator but would fail with a BadRequest in real Cosmos DB.
	/// </summary>
	[Fact(Skip = "FULLTEXTSCORE in SELECT projection is not valid in real Cosmos DB. " +
		"Per Microsoft docs: 'This function can't be part of a projection'. " +
		"The emulator intentionally allows it for debugging convenience. " +
		"See FullTextScore_InProjection_WorksInEmulator for the emulator behaviour.")]
	public async Task FullTextScore_InProjection_ShouldBeInvalid()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database" }),
			new PartitionKey("a"));

		// In real Cosmos DB, this would throw CosmosException with StatusCode 400
		// because FULLTEXTSCORE cannot appear in a SELECT projection.
		var act = async () =>
		{
			var iterator = container.GetItemQueryIterator<JObject>(
				"SELECT FullTextScore(c.text, ['database']) AS score FROM c");
			while (iterator.HasMoreResults)
				await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<CosmosException>();
	}

	/// <summary>
	/// DIVERGENT BEHAVIOUR — RRF (Reciprocal Rank Fusion) is not implemented.
	///
	/// Real Cosmos DB: The RRF function fuses multiple scoring functions (e.g.
	/// FullTextScore + VectorDistance) using Reciprocal Rank Fusion for hybrid search:
	///   SELECT TOP 10 * FROM c ORDER BY RANK RRF(FullTextScore(c.text, 'keyword'), VectorDistance(c.vector, [1,2,3]))
	/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/rrf
	///
	/// InMemoryEmulator: RRF is not recognised. The parser will fail because RRF
	/// is not registered as a known function. This is a known gap.
	/// </summary>
	[Fact(Skip = "RRF (Reciprocal Rank Fusion) is not implemented. " +
		"Real Cosmos DB supports ORDER BY RANK RRF(FullTextScore(...), VectorDistance(...)) " +
		"for hybrid search combining full-text and vector similarity scoring. " +
		"The emulator does not recognise the RRF function. " +
		"See RRF_NotSupported_ThrowsOnParse for the current emulator behaviour.")]
	public async Task RRF_BasicFusion_ShouldCombineScores()
	{
		var container = new InMemoryContainer("test", "/pk");
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database service" }),
			new PartitionKey("a"));
		await container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "Another database record" }),
			new PartitionKey("a"));

		// In real Cosmos DB, RRF fuses two FullTextScore functions:
		var iterator = container.GetItemQueryIterator<JObject>(
			"SELECT TOP 10 * FROM c ORDER BY RANK RRF(FullTextScore(c.text, 'cosmos'), FullTextScore(c.text, 'database'))");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Additional Divergent Behaviour Tests — v2.0.5
//
// These tests document additional areas where the emulator's approximate
// full-text search intentionally differs from real Cosmos DB.
// ════════════════════════════════════════════════════════════════════════════════

public class FullTextSearchAdditionalDivergentBehaviorTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	/// <summary>
	/// DIVERGENT BEHAVIOUR — FULLTEXTSCORE in SELECT projection works in emulator.
	///
	/// This is the sister test for FullTextScore_InProjection_ShouldBeInvalid (skipped).
	/// Real Cosmos DB rejects FULLTEXTSCORE in projections — it can only appear inside
	/// ORDER BY RANK or as an argument to RRF. The emulator allows it, which is useful
	/// for debugging relevance scores during development.
	///
	/// Real Cosmos DB docs state:
	///   "This function can't be part of a projection (for example,
	///    SELECT FullTextScore(c.text, 'keyword') AS Score FROM c is invalid)."
	///
	/// The emulator intentionally diverges here for developer convenience.
	/// </summary>
	[Fact]
	public async Task FullTextScore_InProjection_WorksInEmulator()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database is a great database" }),
			new PartitionKey("a"));

		// Real Cosmos DB would reject this with HTTP 400.
		// The emulator allows it and returns the naive term-frequency score.
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT FullTextScore(c.text, ['database', 'cosmos']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		// 2 occurrences of "database" + 1 occurrence of "cosmos" = 3.0
		results[0]["score"]!.Value<double>().Should().Be(3.0,
			"emulator allows FULLTEXTSCORE in SELECT projection (real Cosmos DB would reject this)");
	}

	/// <summary>
	/// DIVERGENT BEHAVIOUR — Substring matching instead of word-boundary tokenization.
	///
	/// Real Cosmos DB: Full-text search tokenizes text at word boundaries. Searching for
	/// "data" would NOT match a document containing "database" because they are different
	/// tokens. The search term must match a complete token (after stemming).
	///
	/// InMemoryEmulator: Uses string.Contains() for substring matching. So "data" DOES
	/// match "database" because "data" is a substring of "database". This is different
	/// from both stemming (which maps word forms to a root) and whole-word matching.
	///
	/// This is documented in Known Limitations § 13 (Full-Text Search Uses Naive Matching).
	/// </summary>
	[Fact]
	public async Task FullTextContains_SubstringMatchesMidWord_DivergentFromRealCosmos()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "The database stores records" }),
			new PartitionKey("a"));

		// Search for "data" — this is a substring of "database" but not a separate word/token.
		// Real Cosmos DB would NOT match because "data" and "database" are different tokens.
		// The emulator DOES match because it uses substring Contains(), not tokenization.
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'data')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle(
			"the emulator uses substring matching — 'data' matches 'database'. " +
			"Real Cosmos DB uses word-boundary tokenization so 'data' would NOT match 'database'.");
	}

	/// <summary>
	/// DIVERGENT BEHAVIOUR — RRF (Reciprocal Rank Fusion) not supported.
	///
	/// This is the sister test for RRF_BasicFusion_ShouldCombineScores (skipped).
	/// The emulator does not implement the RRF function. Attempting to use it
	/// results in a parse/evaluation error because RRF is not a registered function.
	///
	/// Real Cosmos DB supports: ORDER BY RANK RRF(FullTextScore(...), VectorDistance(...))
	/// for hybrid search fusing full-text and vector similarity scores.
	/// See: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/rrf
	/// </summary>
	[Fact]
	public async Task RRF_NotSupported_ThrowsOnParse()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database" }),
			new PartitionKey("a"));

		// RRF is not implemented — the parser/evaluator does not recognise it.
		// This will throw because RRF is not a known function in the emulator.
		var act = async () =>
		{
			var iterator = _container.GetItemQueryIterator<JObject>(
				"SELECT * FROM c ORDER BY RANK RRF(FullTextScore(c.text, 'cosmos'), FullTextScore(c.text, 'database'))");
			while (iterator.HasMoreResults)
				await iterator.ReadNextAsync();
		};

		await act.Should().ThrowAsync<Exception>(
			"RRF is not implemented in the emulator — the query should fail");
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Parity Tests — v2.0.5
//
// These tests fill coverage gaps where one FTS function had tests that its
// sibling functions were missing.
// ════════════════════════════════════════════════════════════════════════════════

public class FullTextContainsAllParityTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContainsAll_NullField_ReturnsEmpty()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.description, 'test', 'data')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task FullTextContainsAll_WithPartitionKey_RespectsFilter()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database service" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "Cosmos database platform" }),
			new PartitionKey("b"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, 'cosmos', 'database')",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}
}

public class FullTextContainsAnyParityTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContainsAny_NullField_ReturnsEmpty()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.description, 'test', 'data')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task FullTextContainsAny_IsCaseInsensitive()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "AZURE COSMOS DATABASE" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'azure', 'elephant')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAny_SingleTerm_WorksLikeContains()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'world')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAny_WithPartitionKey_RespectsFilter()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "Hello cosmos" }),
			new PartitionKey("b"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'world', 'elephant')",
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}
}

public class FullTextScoreParityTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextScore_NullField_ReturnsZero()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT FullTextScore(c.description, ['test']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().Be(0);
	}

	[Fact]
	public async Task FullTextScore_SingleSearchTerm_ReturnsCount()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "database database database" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT FullTextScore(c.text, ['database']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().Be(3);
	}

	[Fact]
	public async Task FullTextScore_IsCaseInsensitive()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "COSMOS Database cosmos" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT FullTextScore(c.text, ['cosmos']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		// "COSMOS" and "cosmos" both match — 2 occurrences
		results[0]["score"]!.Value<double>().Should().Be(2);
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Edge Case Tests — v2.0.5
//
// Boundary conditions, special characters, nested paths, multi-word phrases,
// logical operators, variadic arguments, and non-string field types.
// ════════════════════════════════════════════════════════════════════════════════

public class FullTextEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContains_EmptyStringTerm_MatchesEverything()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Hello world" }),
			new PartitionKey("a"));

		// Empty string is a substring of all strings in .NET: "Hello".Contains("") == true
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, '')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContains_EmptyStringField_ReturnsEmpty()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'hello')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task FullTextContains_NestedPropertyPath()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", metadata = new { description = "Azure Cosmos database" } }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.metadata.description, 'cosmos')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContains_MultiWordPhrase()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "The quick brown fox jumps over" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "brown jumps quick fox" }),
			new PartitionKey("a"));

		// Multi-word phrase — substring matching means the exact phrase must appear contiguously
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'quick brown')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Only doc 1 has the exact substring "quick brown"
		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task FullTextContains_SpecialCharacters()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "C# is great! Don't you think? café résumé" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'café')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContains_WithNotOperator()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "Azure functions" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE NOT FullTextContains(c.text, 'cosmos')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("2");
	}

	[Fact]
	public async Task FullTextContains_WithOrOperator()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "Azure functions" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", text = "SQL Server" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'cosmos') OR FullTextContains(c.text, 'functions')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results.Select(r => r["id"]!.Value<string>()).Should().BeEquivalentTo(["1", "2"]);
	}

	[Fact]
	public async Task FullTextContainsAll_ManyTerms()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos DB is a fast scalable NoSQL database service" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, 'azure', 'cosmos', 'fast', 'scalable', 'database')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAny_ManyTerms()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Simple text" }),
			new PartitionKey("a"));

		// None of the first 4 terms match, but "text" does
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'alpha', 'bravo', 'charlie', 'delta', 'text')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}
}

public class OrderByRankEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task OrderByRank_WithOffsetLimit_ReturnsPage()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "database database database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", text = "database database" }),
			new PartitionKey("a"));

		// RANK order: id=2 (3), id=3 (2), id=1 (1). OFFSET 1 LIMIT 1 → id=3
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY RANK FullTextScore(c.text, ['database']) OFFSET 1 LIMIT 1");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("3");
	}

	[Fact]
	public async Task OrderByRank_TiedScores_ReturnsAllDocuments()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "database service" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "database platform" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY RANK FullTextScore(c.text, ['database'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// Both have score of 1 — both should be returned
		results.Should().HaveCount(2);
	}

	[Fact]
	public async Task OrderByRank_EmptyContainer_ReturnsEmpty()
	{
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY RANK FullTextScore(c.text, ['database'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task OrderByRank_SingleDocument_ReturnsThatDocument()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "database" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c ORDER BY RANK FullTextScore(c.text, ['database'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task OrderByRank_WithDistinct_ReturnsUniqueResults()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "database database database", category = "docs" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "database service", category = "docs" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "a", text = "database platform", category = "api" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT DISTINCT c.category FROM c ORDER BY RANK FullTextScore(c.text, ['database'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results.Select(r => r["category"]!.Value<string>()).Should().BeEquivalentTo(["docs", "api"]);
	}
}

public class FullTextParameterizedQueryTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContains_WithParameterizedTerm()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Azure Cosmos database" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "a", text = "SQL Server engine" }),
			new PartitionKey("a"));

		var query = new QueryDefinition("SELECT * FROM c WHERE FullTextContains(c.text, @term)")
			.WithParameter("@term", "cosmos");
		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.Value<string>().Should().Be("1");
	}

	[Fact]
	public async Task FullTextScore_WithParameterizedTerms()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Cosmos database service" }),
			new PartitionKey("a"));

		var query = new QueryDefinition(
			"SELECT c.id, FullTextScore(c.text, [@t1, @t2]) AS score FROM c")
			.WithParameter("@t1", "cosmos")
			.WithParameter("@t2", "database");
		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["score"]!.Value<double>().Should().BeGreaterThan(0);
	}
}

public class FullTextNonStringFieldTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContains_NumericField_ConvertsToString()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", value = 12345 }),
			new PartitionKey("a"));

		// The implementation calls args[0]?.ToString() which converts 12345 → "12345"
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.value, '234')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		// "234" is a substring of "12345"
		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContains_BooleanField_ConvertsToString()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", active = true }),
			new PartitionKey("a"));

		// Boolean.ToString() produces "True" (capital T in .NET)
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.active, 'rue')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContains_ArrayField_UsesJsonRepresentation()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", tags = new[] { "cosmos", "database" } }),
			new PartitionKey("a"));

		// JArray.ToString() produces a JSON array string representation
		// The search term needs to match something in that string
		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.tags, 'cosmos')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category A — FullTextScore Edge Cases (CountOccurrences bugs)
// ═══════════════════════════════════════════════════════════════════════════════

public class FullTextScoreEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextScore_EmptyTermInArray_DoesNotHang()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "The quick brown fox" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['', 'fox']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		((double)results[0]["score"]!).Should().Be(1.0);
	}

	[Fact]
	public async Task FullTextScore_AllEmptyTerms_ReturnsZero()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Some content here" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['', '']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		((double)results[0]["score"]!).Should().Be(0.0);
	}

	[Fact]
	public async Task FullTextScore_OverlappingTerms_CountsNonOverlapping()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "aaa" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['aa']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		((double)results[0]["score"]!).Should().Be(1.0);
	}

	[Fact]
	public async Task FullTextScore_WithPartitionKey_RespectsFilter()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos cosmos cosmos" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "cosmos" }),
			new PartitionKey("b"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT c.id, FullTextScore(c.text, ['cosmos']) AS score FROM c WHERE c.pk = @pk")
				.WithParameter("@pk", "a"),
			requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("a") });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.ToString().Should().Be("1");
		((double)results[0]["score"]!).Should().Be(3.0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category B — FullTextContains Additional Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class FullTextContainsAdditionalEdgeCaseTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContains_WhitespaceOnlyTerm_Matches()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "hello world" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, ' ')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle("space character is a valid substring");
	}

	[Fact]
	public async Task FullTextContains_UnicodeCharacters_CaseInsensitive()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "Über cool database" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'über')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle("OrdinalIgnoreCase should match Ü/ü");
	}

	[Fact]
	public async Task FullTextContains_VeryLongText_PerformsReasonably()
	{
		var longText = string.Join(" ", Enumerable.Repeat("word", 10_000)) + " needle";
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = longText }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'needle')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContains_TermLongerThanText_ReturnsFalse()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "hi" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'this is a much longer search term')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category C — FullTextContainsAll Additional Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class FullTextContainsAllAdditionalTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContainsAll_DuplicateTerms_MatchesOnce()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos database" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, 'cosmos', 'cosmos')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle("duplicate terms should still match");
	}

	[Fact]
	public async Task FullTextContainsAll_EmptyStringTerm_MatchesAll()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "anything" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, '', 'anything')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle("empty string is a substring of everything");
	}

	[Fact]
	public async Task FullTextContainsAll_ZeroTerms_ReturnsFalse()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "anything" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text)");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty("zero search terms returns false (args.Length < 2 guard)");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category D — FullTextContainsAny Additional Edge Cases
// ═══════════════════════════════════════════════════════════════════════════════

public class FullTextContainsAnyAdditionalTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContainsAny_DuplicateTerms_StillMatches()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos database" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, 'cosmos', 'cosmos')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAny_EmptyStringTerm_MatchesAll()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "anything" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, '', 'nope')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle("empty string is a substring of everything");
	}

	[Fact]
	public async Task FullTextContainsAny_ZeroTerms_ReturnsFalse()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "anything" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text)");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty("zero search terms returns false");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category E — ORDER BY RANK Advanced Scenarios
// ═══════════════════════════════════════════════════════════════════════════════

public class OrderByRankAdvancedTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task OrderByRank_WithDistinct_HigherRankedDocumentPreferred()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos cosmos cosmos", category = "db" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "cosmos", category = "db" }),
			new PartitionKey("b"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT DISTINCT c.category FROM c ORDER BY RANK FullTextScore(c.text, ['cosmos'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["category"]!.ToString().Should().Be("db");
	}

	[Fact]
	public async Task OrderByRank_WithWhereFullTextContains_CombinesFiltering()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos database engine" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "cosmos cosmos cosmos" }),
			new PartitionKey("b"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", text = "unrelated content" }),
			new PartitionKey("c"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'cosmos') ORDER BY RANK FullTextScore(c.text, ['cosmos'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2, "only docs containing 'cosmos' are returned");
		results[0]["id"]!.ToString().Should().Be("2", "doc with 3 occurrences ranks first");
		results[1]["id"]!.ToString().Should().Be("1", "doc with 1 occurrence ranks second");
	}

	[Fact]
	public async Task OrderByRank_WithTopAndOffset_CombinedPagination()
	{
		for (var i = 1; i <= 10; i++)
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"{i}", pk = $"p{i}", text = string.Concat(Enumerable.Repeat("cosmos ", i)).Trim() }),
				new PartitionKey($"p{i}"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT TOP 5 * FROM c ORDER BY RANK FullTextScore(c.text, ['cosmos'])");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(5);
		results[0]["id"]!.ToString().Should().Be("10");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category F — Cross-Function Interaction Tests
// ═══════════════════════════════════════════════════════════════════════════════

public class FullTextCrossFunctionTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContains_WithIN_Operator()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos database", type = "tech" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "cosmos cloud", type = "cloud" }),
			new PartitionKey("b"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "3", pk = "c", text = "cosmos service", type = "other" }),
			new PartitionKey("c"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'cosmos') AND c.type IN ('tech', 'cloud')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results.Select(r => r["id"]!.ToString()).Should().BeEquivalentTo("1", "2");
	}

	[Fact]
	public async Task FullTextContains_WithArrayContains()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos database", tags = new[] { "db", "nosql" } }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "cosmos cloud", tags = new[] { "cloud" } }),
			new PartitionKey("b"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'cosmos') AND ARRAY_CONTAINS(c.tags, 'nosql')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		results[0]["id"]!.ToString().Should().Be("1");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category G — FullTextScore Scoring Accuracy
// ═══════════════════════════════════════════════════════════════════════════════

public class FullTextScoreAccuracyTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextScore_MultipleOccurrences_ExactCount()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "db db db" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['db']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		((double)results[0]["score"]!).Should().Be(3.0);
	}

	[Fact]
	public async Task FullTextScore_MultipleTerms_SumsCorrectly()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "hello world hello" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['hello', 'world']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		((double)results[0]["score"]!).Should().Be(3.0);
	}

	[Fact]
	public async Task FullTextScore_CaseVariations_CountedCorrectly()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "DB Db dB db" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT c.id, FullTextScore(c.text, ['db']) AS score FROM c");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
		((double)results[0]["score"]!).Should().Be(4.0);
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category H — Parameterized Query Gaps
// ═══════════════════════════════════════════════════════════════════════════════

public class FullTextParameterizedQueryAdditionalTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task FullTextContainsAll_WithParameterizedTerms()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos database engine" }),
			new PartitionKey("a"));

		var query = new QueryDefinition(
			"SELECT * FROM c WHERE FullTextContainsAll(c.text, @t1, @t2)")
			.WithParameter("@t1", "cosmos")
			.WithParameter("@t2", "engine");
		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task FullTextContainsAny_WithParameterizedTerms()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos database" }),
			new PartitionKey("a"));

		var query = new QueryDefinition(
			"SELECT * FROM c WHERE FullTextContainsAny(c.text, @t1, @t2)")
			.WithParameter("@t1", "nonexistent")
			.WithParameter("@t2", "cosmos");
		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle();
	}

	[Fact]
	public async Task OrderByRank_WithParameterizedScore()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "cosmos cosmos cosmos" }),
			new PartitionKey("a"));
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "2", pk = "b", text = "cosmos" }),
			new PartitionKey("b"));

		var query = new QueryDefinition(
			"SELECT * FROM c ORDER BY RANK FullTextScore(c.text, [@term])")
			.WithParameter("@term", "cosmos");
		var iterator = _container.GetItemQueryIterator<JObject>(query);
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().HaveCount(2);
		results[0]["id"]!.ToString().Should().Be("1", "higher score ranks first");
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
//  Category I — Divergent Behavior (Stop Words)
// ═══════════════════════════════════════════════════════════════════════════════

public class FullTextStopWordDivergentTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact(Skip = "Real Cosmos DB removes stop words during text analysis. " +
		"Searching for common words like 'the' may return no results or be ignored. " +
		"The emulator treats all words equally — 'the' is matched like any other substring.")]
	public async Task FullTextContains_StopWordRemoval_ShouldIgnoreCommonWords()
	{
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "The quick brown fox" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'the')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().BeEmpty();
	}

	[Fact]
	public async Task FullTextContains_StopWords_EmulatorMatchesAll()
	{
		// DIVERGENT BEHAVIOUR: The emulator treats stop words like "the" as
		// regular substrings. Real Cosmos DB removes stop words during text
		// analysis, so searching for "the" may return no results.
		await _container.CreateItemAsync(
			JObject.FromObject(new { id = "1", pk = "a", text = "The quick brown fox" }),
			new PartitionKey("a"));

		var iterator = _container.GetItemQueryIterator<JObject>(
			"SELECT * FROM c WHERE FullTextContains(c.text, 'the')");
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
			results.AddRange(await iterator.ReadNextAsync());

		results.Should().ContainSingle("emulator does substring matching on 'the'");
	}
}
