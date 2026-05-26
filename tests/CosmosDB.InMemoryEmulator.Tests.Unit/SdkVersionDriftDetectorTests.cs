using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class SdkVersionDriftDetectorTests
{
	[Fact]
	public async Task DriftDetector_ProducesStructuredReport()
	{
		var report = await SdkVersionDriftDetector.RunAsync();

		report.SdkVersion.Should().NotBeNullOrEmpty();
		report.CompatibilityPassed.Should().BeTrue();
		report.UnrecognisedHeaders.Should().BeEmpty();
		report.TestSuiteVersion.Should().NotBeNullOrEmpty();
		report.MinTestedVersion.Should().NotBeNullOrEmpty();
		report.MaxTestedVersion.Should().NotBeNullOrEmpty();
		report.IsWithinTestedRange.Should().BeTrue();
		report.CompatibilityError.Should().BeNull();
	}

	[Fact]
	public async Task DriftDetector_ReportIsSerializableToJson()
	{
		var report = await SdkVersionDriftDetector.RunAsync();
		var json = System.Text.Json.JsonSerializer.Serialize(report);

		json.Should().Contain("sdkVersion");
		json.Should().Contain("compatibilityPassed");
		json.Should().Contain("unrecognisedHeaders");
		json.Should().Contain("isWithinTestedRange");
	}

	[Fact]
	public async Task DriftDetector_ExercisesMultipleCodePaths()
	{
		var report = await SdkVersionDriftDetector.RunAsync();

		// The detector should exercise read, query, and CRUD paths
		// and still report success
		report.CompatibilityPassed.Should().BeTrue();
		report.UnrecognisedHeaders.Should().BeEmpty();
	}
}
