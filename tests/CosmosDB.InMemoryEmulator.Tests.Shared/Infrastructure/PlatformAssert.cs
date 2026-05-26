using AwesomeAssertions;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Assertion helper for tests with known divergences between in-memory and emulator.
/// Keeps both expected values visible in the test for self-documenting divergences.
/// </summary>
public static class PlatformAssert
{
	/// <summary>
	/// Asserts that <paramref name="actual"/> matches the expected value for the current target.
	/// </summary>
	/// <param name="fixture">The fixture identifying the current target.</param>
	/// <param name="actual">The actual value produced by the test.</param>
	/// <param name="expectedInMemory">Expected value when running against in-memory.</param>
	/// <param name="expectedEmulator">Expected value when running against the real emulator.</param>
	/// <param name="divergenceReason">Human-readable explanation of why the values differ.</param>
	public static void AssertForTarget<T>(
		ITestContainerFixture fixture,
		T actual,
		T expectedInMemory,
		T expectedEmulator,
		string divergenceReason)
	{
		var expected = fixture.IsEmulator ? expectedEmulator : expectedInMemory;
		actual.Should().Be(expected,
			because: fixture.IsEmulator
				? "real emulator behavior"
				: $"in-memory divergence: {divergenceReason}");
	}
}
