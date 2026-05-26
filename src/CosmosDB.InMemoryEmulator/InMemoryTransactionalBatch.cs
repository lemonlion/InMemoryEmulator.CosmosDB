using System.Net;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="TransactionalBatch"/> for testing.
/// Executes batch operations atomically against an <see cref="InMemoryContainer"/>
/// with automatic rollback on failure. Supports <c>CreateItem</c>, <c>UpsertItem</c>,
/// <c>ReplaceItem</c>, <c>DeleteItem</c>, <c>ReadItem</c>, <c>PatchItem</c>,
/// and their stream variants.
/// </summary>
/// <remarks>
/// <para>
/// Enforces Cosmos DB batch limits: max 100 operations and 2 MB total payload.
/// On failure, all preceding operations are rolled back and marked as
/// <see cref="HttpStatusCode.FailedDependency"/>, matching real Cosmos DB behaviour.
/// </para>
/// </remarks>
internal class InMemoryTransactionalBatch : TransactionalBatch
{
	private static readonly JsonSerializerSettings JsonSettings = new()
	{
		TypeNameHandling = TypeNameHandling.None,
		DateParseHandling = DateParseHandling.None,
		ContractResolver = new DefaultContractResolver(),
		Converters = { new StringEnumConverter { AllowIntegerValues = true } }
	};

	private const int MaxBatchOperations = 100;
	private const int MaxBatchSizeBytes = 2 * 1024 * 1024;

	private enum BatchOpType { Create, Read, Upsert, Replace, Delete, Patch, CreateStream, UpsertStream, ReplaceStream }

	private readonly InMemoryContainer _container;
	private readonly PartitionKey _partitionKey;
	private readonly List<(Func<Task> Execute, BatchOpType Type)> _operations = new();
	private long _estimatedBatchSize;
	private readonly Dictionary<int, string> _readResults = new();
	private readonly Dictionary<int, string> _writeEtags = new();
	private readonly Dictionary<int, HttpStatusCode> _opStatusCodes = new();

	public InMemoryTransactionalBatch(InMemoryContainer container, PartitionKey partitionKey)
	{
		_container = container;
		_partitionKey = partitionKey;
	}

	public override TransactionalBatch CreateItem<T>(T item, TransactionalBatchItemRequestOptions? requestOptions = null)
	{
		var json = JsonConvert.SerializeObject(item, JsonSettings);
		_estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
		var opIndex = _operations.Count;
		_operations.Add((async () =>
		{
			var result = await _container.CreateItemAsync(item, _partitionKey, ToItemRequestOptions(requestOptions));
			_writeEtags[opIndex] = result.ETag;
			_readResults[opIndex] = JsonConvert.SerializeObject(result.Resource, JsonSettings);
		}, BatchOpType.Create));
		return this;
	}

	public override TransactionalBatch UpsertItem<T>(T item, TransactionalBatchItemRequestOptions? requestOptions = null)
	{
		var json = JsonConvert.SerializeObject(item, JsonSettings);
		_estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
		var opIndex = _operations.Count;
		_operations.Add((async () =>
		{
			var result = await _container.UpsertItemAsync(item, _partitionKey, ToItemRequestOptions(requestOptions));
			_writeEtags[opIndex] = result.ETag;
			_opStatusCodes[opIndex] = result.StatusCode;
			_readResults[opIndex] = JsonConvert.SerializeObject(result.Resource, JsonSettings);
		}, BatchOpType.Upsert));
		return this;
	}

	public override TransactionalBatch ReplaceItem<T>(string id, T item, TransactionalBatchItemRequestOptions? requestOptions = null)
	{
		var json = JsonConvert.SerializeObject(item, JsonSettings);
		_estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
		var opIndex = _operations.Count;
		_operations.Add((async () =>
		{
			var result = await _container.ReplaceItemAsync(item, id, _partitionKey, ToItemRequestOptions(requestOptions));
			_writeEtags[opIndex] = result.ETag;
			_readResults[opIndex] = JsonConvert.SerializeObject(result.Resource, JsonSettings);
		}, BatchOpType.Replace));
		return this;
	}

	public override TransactionalBatch DeleteItem(string id, TransactionalBatchItemRequestOptions? requestOptions = null)
	{
		_estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(id) + 128;
		_operations.Add((async () => await _container.DeleteItemAsync<object>(id, _partitionKey, ToItemRequestOptions(requestOptions)), BatchOpType.Delete));
		return this;
	}

	public override TransactionalBatch ReadItem(string id, TransactionalBatchItemRequestOptions? requestOptions = null)
	{
		_estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(id) + 128;
		var operationIndex = _operations.Count;
		_operations.Add((async () =>
		{
			var result = await _container.ReadItemAsync<object>(id, _partitionKey, ToItemRequestOptions(requestOptions));
			_readResults[operationIndex] = JsonConvert.SerializeObject(result.Resource, JsonSettings);
		}, BatchOpType.Read));
		return this;
	}

	private static ItemRequestOptions? ToItemRequestOptions(TransactionalBatchItemRequestOptions? options)
	{
		if (options is null)
		{
			return null;
		}

		return new ItemRequestOptions
		{
			IfMatchEtag = options.IfMatchEtag,
			IfNoneMatchEtag = options.IfNoneMatchEtag,
			EnableContentResponseOnWrite = options.EnableContentResponseOnWrite,
			PreTriggers = ExtractTriggerHeader(options.Properties, "x-ms-pre-trigger-include"),
			PostTriggers = ExtractTriggerHeader(options.Properties, "x-ms-post-trigger-include"),
		};
	}

	private static IEnumerable<string>? ExtractTriggerHeader(IReadOnlyDictionary<string, object>? properties, string headerName)
	{
		if (properties is not null
			&& properties.TryGetValue(headerName, out var value)
			&& value is IEnumerable<string> triggers)
			return triggers;
		return null;
	}

	public override async Task<TransactionalBatchResponse> ExecuteAsync(CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();

		if (_operations.Count > MaxBatchOperations)
		{
			throw InMemoryCosmosException.Create("Batch request has more operations than what is supported.",
				HttpStatusCode.BadRequest, 0, string.Empty, 0);
		}

		if (_estimatedBatchSize > MaxBatchSizeBytes)
		{
			var overSizeResults = new List<(HttpStatusCode status, bool isSuccess)>();
			for (var i = 0; i < _operations.Count; i++)
				overSizeResults.Add((HttpStatusCode.FailedDependency, false));
			return new InMemoryBatchResponse(HttpStatusCode.RequestEntityTooLarge, false, overSizeResults, _readResults, _writeEtags,
				"Request size is too large.", _container.CurrentSessionToken);
		}

		var itemsSnapshot = _container.SnapshotItems();
		var etagsSnapshot = _container.SnapshotEtags();
		var timestampsSnapshot = _container.SnapshotTimestamps();
		var changeFeedCount = _container.GetChangeFeedCount();
		_container.BeginBatchTracking();

		var operationResults = new List<(HttpStatusCode status, bool isSuccess)>();
		var failedIndex = -1;
		HttpStatusCode failedStatusCode = default;

		for (var i = 0; i < _operations.Count; i++)
		{
			try
			{
				var (execute, opType) = _operations[i];
				await execute();
				var statusCode = opType switch
				{
					BatchOpType.Read => HttpStatusCode.OK,
					BatchOpType.Delete => HttpStatusCode.NoContent,
					BatchOpType.Replace => HttpStatusCode.OK,
					BatchOpType.Patch => HttpStatusCode.OK,
					BatchOpType.Upsert => _opStatusCodes.TryGetValue(i, out var sc) ? sc : HttpStatusCode.Created,
					_ => HttpStatusCode.Created
				};
				operationResults.Add((statusCode, true));
			}
			catch (CosmosException ex)
			{
				failedIndex = i;
				failedStatusCode = ex.StatusCode;
				operationResults.Add((ex.StatusCode, false));
				break;
			}
		}

		if (failedIndex >= 0)
		{
			var touchedKeys = _container.EndBatchTracking();
			_container.RestoreSnapshot(itemsSnapshot, etagsSnapshot, timestampsSnapshot, changeFeedCount, touchedKeys);
			for (var i = 0; i < failedIndex; i++)
			{
				operationResults[i] = (HttpStatusCode.FailedDependency, false);
			}

			for (var i = failedIndex + 1; i < _operations.Count; i++)
			{
				operationResults.Add((HttpStatusCode.FailedDependency, false));
			}

			return new InMemoryBatchResponse(failedStatusCode, false, operationResults, _readResults, _writeEtags,
				$"Batch operation at index {failedIndex} failed with status code {(int)failedStatusCode}.", _container.CurrentSessionToken);
		}

		_container.EndBatchTracking();
		return new InMemoryBatchResponse(HttpStatusCode.OK, true, operationResults, _readResults, _writeEtags, sessionToken: _container.CurrentSessionToken);
	}

	public override Task<TransactionalBatchResponse> ExecuteAsync(TransactionalBatchRequestOptions requestOptions, CancellationToken cancellationToken = default)
		=> ExecuteAsync(cancellationToken);

	#region Unimplemented overrides

	public override TransactionalBatch CreateItemStream(Stream streamPayload, TransactionalBatchItemRequestOptions? requestOptions = null)
	{
		var json = new StreamReader(streamPayload).ReadToEnd();
		_estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
		var opIndex = _operations.Count;
		_operations.Add((async () =>
		{
			var jObj = JsonParseHelpers.ParseJson(json);
			var id = jObj["id"]?.ToString() ?? Guid.NewGuid().ToString();
			var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			var response = await _container.CreateItemStreamAsync(stream, _partitionKey, ToItemRequestOptions(requestOptions));
			if (!response.IsSuccessStatusCode)
				throw InMemoryCosmosException.Create(response.ErrorMessage ?? "Stream operation failed.", response.StatusCode, 0, string.Empty, 0);
			_writeEtags[opIndex] = response.Headers.ETag;
			if (response.Content != null)
			{
				using var responseReader = new StreamReader(response.Content);
				_readResults[opIndex] = await responseReader.ReadToEndAsync();
			}
			else
			{
				_readResults[opIndex] = json;
			}
		}, BatchOpType.Create));
		return this;
	}

	public override TransactionalBatch UpsertItemStream(Stream streamPayload, TransactionalBatchItemRequestOptions? requestOptions = null)
	{
		var json = new StreamReader(streamPayload).ReadToEnd();
		_estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
		var opIndex = _operations.Count;
		_operations.Add((async () =>
		{
			var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			var response = await _container.UpsertItemStreamAsync(stream, _partitionKey, ToItemRequestOptions(requestOptions));
			if (!response.IsSuccessStatusCode)
				throw InMemoryCosmosException.Create(response.ErrorMessage ?? "Stream operation failed.", response.StatusCode, 0, string.Empty, 0);
			_writeEtags[opIndex] = response.Headers.ETag;
			_opStatusCodes[opIndex] = response.StatusCode;
			if (response.Content != null)
			{
				using var responseReader = new StreamReader(response.Content);
				_readResults[opIndex] = await responseReader.ReadToEndAsync();
			}
			else
			{
				_readResults[opIndex] = json;
			}
		}, BatchOpType.Upsert));
		return this;
	}

	public override TransactionalBatch ReplaceItemStream(string id, Stream streamPayload, TransactionalBatchItemRequestOptions? requestOptions = null)
	{
		var json = new StreamReader(streamPayload).ReadToEnd();
		_estimatedBatchSize += System.Text.Encoding.UTF8.GetByteCount(json);
		var opIndex = _operations.Count;
		_operations.Add((async () =>
		{
			var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
			var response = await _container.ReplaceItemStreamAsync(stream, id, _partitionKey, ToItemRequestOptions(requestOptions));
			if (!response.IsSuccessStatusCode)
				throw InMemoryCosmosException.Create(response.ErrorMessage ?? "Stream operation failed.", response.StatusCode, 0, string.Empty, 0);
			_writeEtags[opIndex] = response.Headers.ETag;
			if (response.Content != null)
			{
				using var responseReader = new StreamReader(response.Content);
				_readResults[opIndex] = await responseReader.ReadToEndAsync();
			}
			else
			{
				_readResults[opIndex] = json;
			}
		}, BatchOpType.Replace));
		return this;
	}

	public override TransactionalBatch PatchItem(string id, IReadOnlyList<PatchOperation> patchOperations, TransactionalBatchPatchItemRequestOptions? requestOptions = null)
	{
		var estimatedSize = patchOperations.Sum(op =>
		{
			var json = JsonConvert.SerializeObject(op, JsonSettings);
			return System.Text.Encoding.UTF8.GetByteCount(json);
		});
		_estimatedBatchSize += estimatedSize;
		var opIndex = _operations.Count;
		_operations.Add((async () =>
		{
			PatchItemRequestOptions? patchOptions = null;
			if (requestOptions is not null)
			{
				patchOptions = new PatchItemRequestOptions
				{
					IfMatchEtag = requestOptions.IfMatchEtag,
					IfNoneMatchEtag = requestOptions.IfNoneMatchEtag,
					FilterPredicate = requestOptions.FilterPredicate,
					PreTriggers = ExtractTriggerHeader(requestOptions.Properties, "x-ms-pre-trigger-include"),
					PostTriggers = ExtractTriggerHeader(requestOptions.Properties, "x-ms-post-trigger-include"),
				};
			}
			var result = await _container.PatchItemAsync<object>(id, _partitionKey, patchOperations, patchOptions);
			_writeEtags[opIndex] = result.ETag;
			_readResults[opIndex] = JsonConvert.SerializeObject(result.Resource, JsonSettings);
		}, BatchOpType.Patch));
		return this;
	}

	#endregion

	private sealed class InMemoryBatchResponse : TransactionalBatchResponse
	{
		private readonly HttpStatusCode _statusCode;
		private readonly bool _isSuccess;
		private readonly List<(HttpStatusCode status, bool isSuccess)> _operationResults;
		private readonly Dictionary<int, string> _readResults;
		private readonly Dictionary<int, string> _writeEtags;
		private readonly string? _errorMessage;

		public InMemoryBatchResponse(
			HttpStatusCode statusCode,
			bool isSuccess,
			List<(HttpStatusCode status, bool isSuccess)> operationResults,
			Dictionary<int, string> readResults,
			Dictionary<int, string> writeEtags,
			string? errorMessage = null,
			string? sessionToken = null)
		{
			_statusCode = statusCode;
			_isSuccess = isSuccess;
			_operationResults = operationResults;
			_readResults = readResults;
			_writeEtags = writeEtags;
			_errorMessage = errorMessage;
			_headers["x-ms-activity-id"] = ActivityId;
			_headers["x-ms-request-charge"] = RequestCharge.ToString(System.Globalization.CultureInfo.InvariantCulture);
			_headers["x-ms-session-token"] = sessionToken ?? "0:0#0";
		}

		public override HttpStatusCode StatusCode => _statusCode;
		public override bool IsSuccessStatusCode => _isSuccess;
		public override int Count => _operationResults.Count;
		public override double RequestCharge => 1d;
		public override string ActivityId { get; } = Guid.NewGuid().ToString();
		public override CosmosDiagnostics Diagnostics => new InMemoryBatchDiagnostics();
		public override Headers Headers => _headers;
		private readonly Headers _headers = new Headers();
		public override string? ErrorMessage => _errorMessage;
		public override TimeSpan? RetryAfter => TimeSpan.Zero;

		public override TransactionalBatchOperationResult this[int index]
		{
			get
			{
				if (index < 0 || index >= _operationResults.Count) return null!;
				var (status, isSuccess) = _operationResults[index];
				var result = Substitute.For<TransactionalBatchOperationResult>();
				result.StatusCode.Returns(status);
				result.IsSuccessStatusCode.Returns(isSuccess);
				if (_writeEtags.TryGetValue(index, out var etag))
				{
					result.ETag.Returns(etag);
				}
				if (_readResults.TryGetValue(index, out var json))
				{
					result.ResourceStream.Returns(_ => new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)));
				}
				return result;
			}
		}

		public override TransactionalBatchOperationResult<T> GetOperationResultAtIndex<T>(int index)
		{
			var (status, isSuccess) = _operationResults[index];
			var result = Substitute.For<TransactionalBatchOperationResult<T>>();
			result.StatusCode.Returns(status);
			result.IsSuccessStatusCode.Returns(isSuccess);

			if (_readResults.TryGetValue(index, out var json))
			{
				result.Resource.Returns(JsonConvert.DeserializeObject<T>(json, JsonSettings)!);
			}

			if (_writeEtags.TryGetValue(index, out var etag))
			{
				result.ETag.Returns(etag);
			}

			return result;
		}

		public override IEnumerator<TransactionalBatchOperationResult> GetEnumerator()
		{
			for (var i = 0; i < _operationResults.Count; i++)
			{
				yield return this[i];
			}
		}

		protected override void Dispose(bool disposing) { }
	}

	private sealed class InMemoryBatchDiagnostics : CosmosDiagnostics
	{
		public override TimeSpan GetClientElapsedTime() => TimeSpan.Zero;
		public override IReadOnlyList<(string regionName, Uri uri)> GetContactedRegions() => Array.Empty<(string, Uri)>();
		public override string ToString() => "{}";
	}
}
