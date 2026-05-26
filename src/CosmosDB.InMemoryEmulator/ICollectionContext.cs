#nullable disable
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Provides partition-scoped document CRUD operations for JS server-side scripts.
/// </summary>
public interface ICollectionContext
{
	JObject CreateDocument(JObject document);
	JObject ReadDocument(string id);
	IReadOnlyList<JObject> QueryDocuments(string sql);
	JObject ReplaceDocument(string id, JObject document);
	void DeleteDocument(string id);
	string SelfLink { get; }
}
