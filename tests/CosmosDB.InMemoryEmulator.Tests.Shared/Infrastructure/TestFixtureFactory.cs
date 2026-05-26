namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Creates the per-test-class <see cref="ITestContainerFixture"/> backed by
/// the shared <see cref="EmulatorSession"/>.
/// </summary>
public static class TestFixtureFactory
{
	/// <summary>
	/// Creates a per-test-class fixture. The target (in-memory vs emulator)
	/// is determined by the session, which reads <c>COSMOS_TEST_TARGET</c>
	/// once at construction.
	/// </summary>
	public static ITestContainerFixture Create(EmulatorSession session) =>
		session.IsEmulator
			? new EmulatorTestFixture(session)
			: new InMemoryTestFixture();
}
