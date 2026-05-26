using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Engine for executing JavaScript trigger bodies.
/// Implemented by the optional <c>CosmosDB.InMemoryEmulator.JsTriggers</c> package.
/// </summary>
public interface IJsTriggerEngine
{
	/// <summary>
	/// Executes a pre-trigger JavaScript body. Returns the (possibly modified) document.
	/// </summary>
	JObject ExecutePreTrigger(string jsBody, JObject document);

	/// <summary>
	/// Executes a pre-trigger JavaScript body with access to the collection context for CRUD operations.
	/// </summary>
	JObject ExecutePreTrigger(string jsBody, JObject document, ICollectionContext context);

	/// <summary>
	/// Executes a post-trigger JavaScript body with access to the committed document.
	/// Returns a modified response body if <c>response.setBody()</c> was called, or null otherwise.
	/// </summary>
	JObject? ExecutePostTrigger(string jsBody, JObject document);

	/// <summary>
	/// Executes a post-trigger JavaScript body with access to the collection context for CRUD operations.
	/// </summary>
	JObject? ExecutePostTrigger(string jsBody, JObject document, ICollectionContext context);
}
