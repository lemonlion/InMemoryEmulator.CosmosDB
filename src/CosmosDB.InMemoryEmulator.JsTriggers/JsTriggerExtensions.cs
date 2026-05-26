namespace CosmosDB.InMemoryEmulator.JsTriggers;

/// <summary>
/// Extension methods for configuring JavaScript support on <see cref="IContainerTestSetup"/>.
/// </summary>
public static class JsTriggerExtensions
{
	/// <summary>
	/// Enables JavaScript trigger body interpretation using the Jint engine.
	/// Call this on an <see cref="IContainerTestSetup"/> to allow triggers registered
	/// via <c>CreateTriggerAsync</c> (with a JS body) to execute.
	/// </summary>
	public static IContainerTestSetup UseJsTriggers(this IContainerTestSetup container)
	{
		container.JsTriggerEngine = new JintTriggerEngine();
		return container;
	}

	/// <summary>
	/// Enables JavaScript stored procedure execution using the Jint engine.
	/// Call this on an <see cref="IContainerTestSetup"/> to allow stored procedures created
	/// via <c>CreateStoredProcedureAsync</c> (with a JS body) to execute when no C# handler is registered.
	/// </summary>
	public static IContainerTestSetup UseJsStoredProcedures(this IContainerTestSetup container)
	{
		container.SprocEngine = new JintSprocEngine();
		return container;
	}
}
