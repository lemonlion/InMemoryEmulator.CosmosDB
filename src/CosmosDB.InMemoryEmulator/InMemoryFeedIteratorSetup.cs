using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Threading;
using CosmosDB.InMemoryEmulator.ProductionExtensions;

#pragma warning disable CS0618 // Obsolete members - InMemoryFeedIteratorSetup must reference the deprecated ProductionExtensions for backward compat

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Wires up the <see cref="CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory"/> delegate
/// so that <c>.ToFeedIteratorOverridable()</c> returns an <see cref="InMemoryFeedIterator{T}"/>
/// backed by the LINQ <see cref="IQueryable{T}"/> rather than calling into the real Cosmos SDK.
///
/// <b>When is this needed?</b>
/// If any production code uses <c>container.GetItemLinqQueryable&lt;T&gt;()</c> followed by
/// <c>.ToFeedIteratorOverridable()</c>, this setup ensures those queries execute in-memory during
/// component tests. Without this registration, <c>.ToFeedIteratorOverridable()</c> would fall
/// through to <c>.ToFeedIterator()</c>, which requires a real Cosmos connection.
///
/// <b>Usage:</b>
/// Call <c>InMemoryFeedIteratorSetup.Register()</c> once during test fixture initialisation, e.g.:
/// <code>
/// // In your InMemoryDatabaseClientFactory constructor or test setup:
/// InMemoryFeedIteratorSetup.Register();
/// </code>
///
/// <b>Important:</b>
/// If production code uses <c>.ToFeedIterator()</c> directly (without the "Overridable" variant),
/// it will bypass this factory and fail when running against an in-memory container. Always use
/// <c>.ToFeedIteratorOverridable()</c> in production code to ensure testability.
/// </summary>
public static class InMemoryFeedIteratorSetup
{
	private static readonly MethodInfo CreateMethod = typeof(InMemoryFeedIteratorSetup)
		.GetMethod(nameof(CreateInMemoryFeedIterator), BindingFlags.NonPublic | BindingFlags.Static)!;

	private static readonly ConcurrentDictionary<Type, MethodInfo> MethodCache = new();

	/// <summary>
	/// Registers the in-memory feed iterator factory so that
	/// <c>.ToFeedIteratorOverridable()</c> returns an <see cref="InMemoryFeedIterator{T}"/>
	/// backed by LINQ-to-Objects rather than requiring a real Cosmos connection.
	/// Call this once during test fixture initialisation.
	/// </summary>
	public static void Register()
	{
		Func<object, object> factory = queryable =>
		{
			var queryableType = queryable.GetType();
			var elementType = queryableType
				.GetInterfaces()
				.Concat([queryableType])
				.Where(interfaceType => interfaceType.IsGenericType && interfaceType.GetGenericTypeDefinition() == typeof(IQueryable<>))
				.Select(interfaceType => interfaceType.GetGenericArguments()[0])
				.FirstOrDefault();

			if (elementType is null)
			{
				throw new InvalidOperationException(
					$"Cannot create InMemoryFeedIterator: the queryable type '{queryableType.FullName}' " +
					"does not implement IQueryable<T>. Ensure you are passing a valid LINQ queryable.");
			}

			var method = MethodCache.GetOrAdd(elementType, type => CreateMethod.MakeGenericMethod(type));
			return method.Invoke(null, [queryable])!;
		};

		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory = factory;
		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory = factory;
	}

	/// <summary>
	/// Clears both the AsyncLocal and static fallback factories, reverting
	/// <c>.ToFeedIteratorOverridable()</c> to its default production behaviour
	/// (delegating to the real Cosmos SDK's <c>.ToFeedIterator()</c>).
	/// </summary>
	public static void Deregister()
	{
		CosmosOverridableFeedIteratorExtensions.FeedIteratorFactory = null;
		CosmosOverridableFeedIteratorExtensions.StaticFallbackFactory = null;
		MethodCache.Clear();
	}

	/// <summary>
	/// Side-channel for passing MaxItemCount from GetItemLinqQueryable to the feed iterator factory.
	/// Set by InMemoryContainer.GetItemLinqQueryable, consumed by CreateInMemoryFeedIterator.
	/// </summary>
	internal static readonly AsyncLocal<int?> MaxItemCountLocal = new();

	internal static int? LastMaxItemCount
	{
		get => MaxItemCountLocal.Value;
		set => MaxItemCountLocal.Value = value;
	}

	private static InMemoryFeedIterator<T> CreateInMemoryFeedIterator<T>(IQueryable<T> queryable)
	{
		var maxItemCount = MaxItemCountLocal.Value;
		MaxItemCountLocal.Value = null;
		return new InMemoryFeedIterator<T>(queryable.AsEnumerable(), maxItemCount) { GuaranteeFirstPage = true };
	}
}
