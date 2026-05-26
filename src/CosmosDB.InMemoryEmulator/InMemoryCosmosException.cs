#nullable disable
using System.Net;
using Microsoft.Azure.Cosmos;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Factory for creating <see cref="CosmosException"/> instances that match the exact
/// <see cref="CosmosException"/> type (not a subclass).
/// <para>
/// Previous versions threw a subclass (<c>InMemoryCosmosException</c>) which broke
/// <c>Assert.ThrowsAsync&lt;CosmosException&gt;</c> and similar exact-type-match
/// assertions because the thrown type was not <see cref="CosmosException"/> itself.
/// This factory returns a plain <see cref="CosmosException"/> so that all standard
/// SDK error-handling patterns work unchanged.
/// </para>
/// <para>
/// <b>Note:</b> <see cref="CosmosException.Diagnostics"/> will be <c>null</c> on
/// exceptions created by this factory because the Cosmos SDK does not expose a public
/// way to set diagnostics. This avoids using reflection on SDK internals.
/// </para>
/// </summary>
public static class InMemoryCosmosException
{
	/// <summary>
	/// Creates a new <see cref="CosmosException"/>.
	/// <para>
	/// <see cref="CosmosException.Diagnostics"/> will be <c>null</c> because the SDK
	/// does not provide a public constructor or setter that accepts diagnostics.
	/// </para>
	/// </summary>
	public static CosmosException Create(string message, HttpStatusCode statusCode, int subStatusCode, string activityId, double requestCharge)
	{
		return new CosmosException(message, statusCode, subStatusCode, activityId, requestCharge);
	}
}
