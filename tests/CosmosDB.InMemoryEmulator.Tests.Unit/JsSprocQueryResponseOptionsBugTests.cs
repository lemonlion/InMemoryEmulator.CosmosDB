using AwesomeAssertions;
using CosmosDB.InMemoryEmulator.JsTriggers;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Reproduces a bug where the JavaScript stored procedure <c>queryDocuments</c> callback
/// receives <c>null</c>/<c>undefined</c> for the <c>responseOptions</c> parameter.
/// Real Cosmos DB always passes <c>(err, docs, responseOptions)</c> where responseOptions
/// has a <c>continuation</c> property for pagination.
/// </summary>
public class JsSprocQueryResponseOptionsBugTests
{
	private readonly InMemoryContainer _container = new("test-container", "/pk");

	[Fact]
	public async Task QueryDocuments_Callback_ShouldReceiveResponseOptions()
	{
		_container.UseJsStoredProcedures();

		// Insert some test documents
		for (int i = 0; i < 3; i++)
		{
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"doc{i}", pk = "a", value = i }),
				new PartitionKey("a"));
		}

		// This stored procedure checks whether responseOptions is provided in the callback.
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spCheckResponseOptions",
			Body = @"function() {
                var collection = getContext().getCollection();
                collection.queryDocuments(
                    collection.getSelfLink(),
                    'SELECT * FROM c',
                    {},
                    function(err, docs, responseOptions) {
                        if (err) throw err;
                        var result = {
                            docCount: docs.length,
                            hasResponseOptions: (responseOptions !== null && responseOptions !== undefined),
                            responseOptionsType: typeof responseOptions,
                            hasContinuation: (responseOptions !== null && responseOptions !== undefined && 'continuation' in responseOptions)
                        };
                        getContext().getResponse().setBody(JSON.stringify(result));
                    });
                }"
		});

		var response = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spCheckResponseOptions", new PartitionKey("a"), Array.Empty<dynamic>());

		var result = JObject.Parse(response.Resource);

		result["docCount"]!.Value<int>().Should().Be(3);
		result["hasResponseOptions"]!.Value<bool>().Should().BeTrue(
			"real Cosmos DB always passes responseOptions as the third argument to the queryDocuments callback");
		result["responseOptionsType"]!.Value<string>().Should().Be("object",
			"responseOptions should be an object, not undefined or null");
		result["hasContinuation"]!.Value<bool>().Should().BeTrue(
			"responseOptions should have a 'continuation' property for pagination support");
	}

	[Fact]
	public async Task QueryDocuments_BulkDeletePattern_ShouldWorkWithContinuation()
	{
		_container.UseJsStoredProcedures();

		// Insert test documents
		for (int i = 0; i < 5; i++)
		{
			await _container.CreateItemAsync(
				JObject.FromObject(new { id = $"doc{i}", pk = "a", status = "old" }),
				new PartitionKey("a"));
		}

		// Eveneum-style BulkDelete pattern that relies on responseOptions.continuation
		await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
		{
			Id = "spBulkDeleteWithContinuation",
			Body = @"function(query) {
                var collection = getContext().getCollection();
                var response = getContext().getResponse();
                var deletedCount = 0;

                function doQuery(continuation) {
                    var requestOptions = { continuation: continuation };
                    collection.queryDocuments(
                        collection.getSelfLink(),
                        query,
                        requestOptions,
                        function(err, docs, responseOptions) {
                            if (err) throw err;
                            for (var i = 0; i < docs.length; i++) {
                                collection.deleteDocument(
                                    docs[i]._self || (collection.getSelfLink() + '/docs/' + docs[i].id),
                                    {},
                                    function(delErr) { if (delErr) throw delErr; });
                                deletedCount++;
                            }
                            if (responseOptions && responseOptions.continuation) {
                                doQuery(responseOptions.continuation);
                            } else {
                                response.setBody(JSON.stringify(deletedCount));
                            }
                        });
                }

                doQuery(undefined);
            }"
		});

		var result = await _container.Scripts.ExecuteStoredProcedureAsync<string>(
			"spBulkDeleteWithContinuation", new PartitionKey("a"),
			new dynamic[] { "SELECT * FROM c WHERE c.status = 'old'" });

		result.Resource.Should().Be("5",
			"all 5 documents should be deleted via the continuation-based bulk delete pattern");

		// Verify all documents were deleted
		var iterator = _container.GetItemQueryIterator<JObject>(
			new QueryDefinition("SELECT * FROM c WHERE c.pk = 'a'"));
		var remaining = new List<JObject>();
		while (iterator.HasMoreResults)
		{
			var page = await iterator.ReadNextAsync();
			remaining.AddRange(page);
		}
		remaining.Should().BeEmpty("all documents should have been deleted by the stored procedure");
	}
}
