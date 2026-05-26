using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

#nullable disable

/// <summary>
/// In-memory implementation of <see cref="FeedIterator{T}"/> that pages through a
/// pre-computed list of items. Supports pagination via <c>maxItemCount</c>,
/// continuation tokens (offset-based), and deferred evaluation via factory delegates.
/// </summary>
/// <typeparam name="T">The item type returned by each page.</typeparam>
public class InMemoryFeedIterator<T> : FeedIterator<T>
{
	private IReadOnlyList<T> _items;
	private readonly Func<IReadOnlyList<T>> _factory;
	private readonly int? _maxItemCount;
	private int _offset;
	private bool _hasReadFirstPage;

	/// <summary>
	/// Creates a feed iterator from a pre-computed list of items.
	/// </summary>
	/// <param name="items">The complete list of items to paginate.</param>
	/// <param name="maxItemCount">Maximum items per page. If null, all items are returned in one page.</param>
	/// <param name="initialOffset">The starting offset (for continuation token support).</param>
	public InMemoryFeedIterator(IReadOnlyList<T> items, int? maxItemCount = null, int initialOffset = 0)
	{
		_items = items;
		_maxItemCount = maxItemCount;
		_offset = initialOffset;
	}

	/// <summary>
	/// Creates a feed iterator from an enumerable source, materialising it eagerly.
	/// </summary>
	/// <param name="source">The items to paginate.</param>
	/// <param name="maxItemCount">Maximum items per page.</param>
	public InMemoryFeedIterator(IEnumerable<T> source, int? maxItemCount = null)
	{
		_items = Materialize(source);
		_maxItemCount = maxItemCount;
	}

	/// <summary>
	/// Creates a feed iterator with deferred evaluation. The factory is called
	/// on the first access to produce the list of items.
	/// </summary>
	/// <param name="factory">A factory that produces the items on first access.</param>
	/// <param name="maxItemCount">Maximum items per page.</param>
	public InMemoryFeedIterator(Func<IReadOnlyList<T>> factory, int? maxItemCount = null)
	{
		_factory = factory;
		_maxItemCount = maxItemCount;
	}

	private static IReadOnlyList<T> Materialize(IEnumerable<T> source)
	{
		try
		{
			return (source as IReadOnlyList<T>) ?? source.ToList();
		}
		catch (Exception ex)
		{
			throw new InvalidOperationException(
				$"Failed to materialize InMemoryFeedIterator<{typeof(T).Name}> source enumerable. " +
				"This may indicate an error in the LINQ query or the underlying data source.", ex);
		}
	}

	private int PageSize => _maxItemCount is > 0 ? _maxItemCount.Value : EnsureItems().Count;

	/// <summary>
	/// When true, <see cref="FeedResponse{T}.IndexMetrics"/> returns a synthetic stub
	/// instead of null, matching real Cosmos DB behaviour when PopulateIndexMetrics is requested.
	/// </summary>
	public bool PopulateIndexMetrics { get; set; }

	/// <summary>
	/// When true, <see cref="HasMoreResults"/> returns true before the first page is read,
	/// even when the result set is empty. This matches real Cosmos DB query iterator behavior.
	/// </summary>
	public bool GuaranteeFirstPage { get; set; }

	public override bool HasMoreResults => (GuaranteeFirstPage && !_hasReadFirstPage) || _offset < EnsureItems().Count;

	public override Task<FeedResponse<T>> ReadNextAsync(CancellationToken cancellationToken = default)
	{
		cancellationToken.ThrowIfCancellationRequested();
		_hasReadFirstPage = true;
		var items = EnsureItems();
		var page = items.Skip(_offset).Take(PageSize).ToList();
		_offset += page.Count;
		if (_offset >= items.Count)
		{
			_offset = items.Count;
		}

		var continuationToken = _offset < items.Count ? _offset.ToString() : null;
		return Task.FromResult<FeedResponse<T>>(new InMemoryFeedResponse<T>(page, continuationToken, PopulateIndexMetrics));
	}

	private IReadOnlyList<T> EnsureItems()
	{
		if (_items == null && _factory != null)
		{
			_items = _factory();
		}
		return _items ?? Array.Empty<T>();
	}

	private static readonly CosmosDiagnostics FakeDiagnostics = new InMemoryFeedDiagnostics();

	private sealed class InMemoryFeedDiagnostics : CosmosDiagnostics
	{
		public override TimeSpan GetClientElapsedTime() => TimeSpan.Zero;
		public override IReadOnlyList<(string regionName, Uri uri)> GetContactedRegions() => Array.Empty<(string, Uri)>();
		public override string ToString() => "{}";
	}

	private sealed class InMemoryFeedResponse<TItem> : FeedResponse<TItem>
	{
		private readonly IReadOnlyList<TItem> _items;
		private readonly bool _populateIndexMetrics;

		public InMemoryFeedResponse(IReadOnlyList<TItem> items, string continuationToken = null, bool populateIndexMetrics = false)
		{
			_items = items;
			_populateIndexMetrics = populateIndexMetrics;
			ContinuationToken = continuationToken;
			ActivityId = Guid.NewGuid().ToString();
			var headers = new Headers();
			headers["x-ms-activity-id"] = ActivityId;
			headers["x-ms-request-charge"] = RequestCharge.ToString(System.Globalization.CultureInfo.InvariantCulture);
			headers["x-ms-item-count"] = items.Count.ToString();
			if (continuationToken != null) headers["x-ms-continuation"] = continuationToken;
			Headers = headers;
		}

		public override Headers Headers { get; }
		public override IEnumerable<TItem> Resource => _items;
		public override HttpStatusCode StatusCode => HttpStatusCode.OK;
		public override CosmosDiagnostics Diagnostics => FakeDiagnostics;
		public override int Count => _items.Count;
		public override string IndexMetrics => _populateIndexMetrics
			? "{\"UtilizedSingleIndexes\":[],\"PotentialSingleIndexes\":[],\"UtilizedCompositeIndexes\":[],\"PotentialCompositeIndexes\":[]}"
			: null!;
		public override string ContinuationToken { get; }
		public override double RequestCharge => 1;
		public override string ActivityId { get; }
		public override string ETag => null!;

		public override IEnumerator<TItem> GetEnumerator() => _items.GetEnumerator();
	}
}
