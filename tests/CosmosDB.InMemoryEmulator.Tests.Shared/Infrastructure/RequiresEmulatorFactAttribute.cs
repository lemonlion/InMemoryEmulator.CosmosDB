using System.Runtime.CompilerServices;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Skips the test when no Cosmos DB emulator is reachable.
/// Use on tests that MUST run against a real emulator (not in-memory).
/// </summary>
public sealed class RequiresEmulatorFactAttribute : FactAttribute
{
	public RequiresEmulatorFactAttribute(
		[CallerFilePath] string? sourceFilePath = null,
		[CallerLineNumber] int sourceLineNumber = -1)
		: base(sourceFilePath, sourceLineNumber)
	{
		if (!EmulatorDetector.IsAvailable)
			Skip = "Cosmos DB emulator not available at localhost:8081";
	}
}
