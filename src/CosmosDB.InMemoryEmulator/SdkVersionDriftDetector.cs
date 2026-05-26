using System.Text.Json.Serialization;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Runs a comprehensive SDK drift detection check, collecting compatibility results
/// and unrecognised headers into a structured <see cref="DriftReport"/> that can be
/// serialised to JSON for CI artifact consumption.
/// </summary>
public static class SdkVersionDriftDetector
{
	public static async Task<DriftReport> RunAsync()
	{
		var sdkVersion = typeof(CosmosClient).Assembly.GetName().Version?.ToString() ?? "unknown";
		var report = new DriftReport
		{
			SdkVersion = sdkVersion,
			TestSuiteVersion = typeof(FakeCosmosHandler).Assembly.GetName().Version?.ToString() ?? "unknown",
			MinTestedVersion = FakeCosmosHandler.MinTestedSdkVersion.ToString(),
			MaxTestedVersion = FakeCosmosHandler.MaxTestedSdkVersion.ToString(),
			IsWithinTestedRange = IsWithinRange(sdkVersion),
		};

		try
		{
			await FakeCosmosHandler.VerifySdkCompatibilityAsync();
			report.CompatibilityPassed = true;
		}
		catch (Exception ex)
		{
			report.CompatibilityPassed = false;
			report.CompatibilityError = ex.Message;
		}

		// Run a handler to collect unrecognised headers across multiple code paths
		var container = new InMemoryContainer("drift-check", "/pk");
		await container.CreateItemAsync(new { id = "1", pk = "a" }, new PartitionKey("a"));
		using var handler = new FakeCosmosHandler(container);
		using var client = handler.CreateClient();
		var cosmosContainer = client.GetContainer("db", "drift-check");

		// Exercise read path
		await cosmosContainer.ReadItemAsync<dynamic>("1", new PartitionKey("a"));

		// Exercise query path
		var iterator = cosmosContainer.GetItemQueryIterator<dynamic>("SELECT * FROM c");
		while (iterator.HasMoreResults) await iterator.ReadNextAsync();

		report.UnrecognisedHeaders = handler.UnrecognisedHeaders.Distinct().ToList();
		return report;
	}

	private static bool IsWithinRange(string sdkVersionString)
	{
		if (!Version.TryParse(sdkVersionString, out var sdkVersion))
			return false;

		return sdkVersion >= FakeCosmosHandler.MinTestedSdkVersion
			&& sdkVersion <= FakeCosmosHandler.MaxTestedSdkVersion;
	}
}

/// <summary>
/// Structured report produced by <see cref="SdkVersionDriftDetector.RunAsync"/>.
/// Serialisable to JSON for CI artifact consumption.
/// </summary>
public sealed class DriftReport
{
	[JsonPropertyName("sdkVersion")]
	public string SdkVersion { get; set; } = "";

	[JsonPropertyName("testSuiteVersion")]
	public string TestSuiteVersion { get; set; } = "";

	[JsonPropertyName("minTestedVersion")]
	public string MinTestedVersion { get; set; } = "";

	[JsonPropertyName("maxTestedVersion")]
	public string MaxTestedVersion { get; set; } = "";

	[JsonPropertyName("isWithinTestedRange")]
	public bool IsWithinTestedRange { get; set; }

	[JsonPropertyName("compatibilityPassed")]
	public bool CompatibilityPassed { get; set; }

	[JsonPropertyName("compatibilityError")]
	public string? CompatibilityError { get; set; }

	[JsonPropertyName("unrecognisedHeaders")]
	public List<string> UnrecognisedHeaders { get; set; } = [];
}
