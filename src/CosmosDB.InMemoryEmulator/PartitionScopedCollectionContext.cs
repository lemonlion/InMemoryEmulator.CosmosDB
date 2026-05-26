#nullable disable
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

internal sealed class PartitionScopedCollectionContext : ICollectionContext
{
	private readonly InMemoryContainer _container;
	private readonly PartitionKey _partitionKey;

	public PartitionScopedCollectionContext(InMemoryContainer container, PartitionKey partitionKey)
	{
		_container = container;
		_partitionKey = partitionKey;
	}

	public string SelfLink => $"dbs/db/colls/{_container.Id}";

	public JObject CreateDocument(JObject document)
	{
		var response = _container.CreateItemAsync(document, _partitionKey).GetAwaiter().GetResult();
		var json = JsonConvert.SerializeObject(response.Resource);
		return JObject.Parse(json);
	}

	public JObject ReadDocument(string id)
	{
		var response = _container.ReadItemAsync<JObject>(id, _partitionKey).GetAwaiter().GetResult();
		return response.Resource;
	}

	public IReadOnlyList<JObject> QueryDocuments(string sql)
	{
		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition(sql),
			requestOptions: new QueryRequestOptions { PartitionKey = _partitionKey });
		var results = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = iterator.ReadNextAsync().GetAwaiter().GetResult();
			results.AddRange(page);
		}
		return results;
	}

	public JObject ReplaceDocument(string id, JObject document)
	{
		var response = _container.ReplaceItemAsync(document, id, _partitionKey).GetAwaiter().GetResult();
		var json = JsonConvert.SerializeObject(response.Resource);
		return JObject.Parse(json);
	}

	public void DeleteDocument(string id)
	{
		_container.DeleteItemAsync<JObject>(id, _partitionKey).GetAwaiter().GetResult();
	}
}
