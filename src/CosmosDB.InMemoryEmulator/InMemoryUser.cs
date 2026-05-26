#nullable disable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NSubstitute;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="User"/> for testing.
/// Manages permissions in a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// and returns synthetic responses. No actual authorization is enforced.
/// </summary>
public sealed class InMemoryUser : User
{
	private readonly ConcurrentDictionary<string, PermissionProperties> _permissions = new();
	private readonly Action _onDeleted;
	private readonly ConcurrentDictionary<string, InMemoryUser> _parentUsers;
	private string _id;

	public InMemoryUser(string id, Action onDeleted = null)
	{
		_id = id;
		_onDeleted = onDeleted;
	}

	/// <summary>
	/// Creates a proxy user that checks <paramref name="parentUsers"/> for existence on ReadAsync.
	/// If the user is not in the dictionary, ReadAsync throws 404 NotFound.
	/// </summary>
	internal InMemoryUser(string id, Action onDeleted, ConcurrentDictionary<string, InMemoryUser> parentUsers)
	{
		_id = id;
		_onDeleted = onDeleted;
		_parentUsers = parentUsers;
	}

	public override string Id => _id;

	public override Task<UserResponse> ReadAsync(
		RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		if (_parentUsers is not null && !_parentUsers.ContainsKey(_id))
		{
			throw InMemoryCosmosException.Create(
				$"User with id '{_id}' not found.",
				HttpStatusCode.NotFound, 0, Guid.NewGuid().ToString(), 0);
		}

		return Task.FromResult(BuildUserResponse(CreateUserProperties(_id), HttpStatusCode.OK));
	}

	public override Task<UserResponse> ReplaceAsync(
		UserProperties userProperties,
		RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		if (userProperties.Id != _id)
			throw InMemoryCosmosException.Create($"Replacing user id is not allowed.", HttpStatusCode.BadRequest, 0, string.Empty, 0);

		_id = userProperties.Id;
		return Task.FromResult(BuildUserResponse(CreateUserProperties(_id), HttpStatusCode.OK));
	}

	public override Task<UserResponse> DeleteAsync(
		RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		_onDeleted?.Invoke();
		return Task.FromResult(BuildUserResponse(null, HttpStatusCode.NoContent));
	}

	public override Permission GetPermission(string id)
	{
		return new InMemoryPermission(id, _permissions);
	}

	public override Task<PermissionResponse> CreatePermissionAsync(
		PermissionProperties permissionProperties,
		int? tokenExpiryInSeconds = null,
		RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		var props = InMemoryPermission.WithSyntheticMetadata(permissionProperties);
		if (!_permissions.TryAdd(props.Id, props))
			throw InMemoryCosmosException.Create($"Permission '{props.Id}' already exists.", HttpStatusCode.Conflict, 0, string.Empty, 0);

		var perm = new InMemoryPermission(props.Id, _permissions);
		var response = Substitute.For<PermissionResponse>();
		response.StatusCode.Returns(HttpStatusCode.Created);
		response.Resource.Returns(props);
		response.Permission.Returns(perm);
		return Task.FromResult(response);
	}

	public override Task<PermissionResponse> UpsertPermissionAsync(
		PermissionProperties permissionProperties,
		int? tokenExpiryInSeconds = null,
		RequestOptions requestOptions = null,
		CancellationToken cancellationToken = default)
	{
		var props = InMemoryPermission.WithSyntheticMetadata(permissionProperties);
		var isNew = !_permissions.ContainsKey(props.Id);
		_permissions[props.Id] = props;

		var perm = new InMemoryPermission(props.Id, _permissions);
		var response = Substitute.For<PermissionResponse>();
		response.StatusCode.Returns(isNew ? HttpStatusCode.Created : HttpStatusCode.OK);
		response.Resource.Returns(props);
		response.Permission.Returns(perm);
		return Task.FromResult(response);
	}

	public override FeedIterator<T> GetPermissionQueryIterator<T>(
		string queryText = null,
		string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		return new InMemoryFeedIterator<T>(
			() => _permissions.Values.Cast<T>().ToList());
	}

	public override FeedIterator<T> GetPermissionQueryIterator<T>(
		QueryDefinition queryDefinition,
		string continuationToken = null,
		QueryRequestOptions requestOptions = null)
	{
		return GetPermissionQueryIterator<T>((string)null, continuationToken, requestOptions);
	}

	internal static UserProperties CreateUserProperties(string id)
	{
		var json = new JObject
		{
			["id"] = id,
			["_etag"] = $"\"{Guid.NewGuid()}\"",
			["_ts"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
		};
		return JsonConvert.DeserializeObject<UserProperties>(json.ToString());
	}

	private UserResponse BuildUserResponse(UserProperties props, HttpStatusCode statusCode)
	{
		var response = Substitute.For<UserResponse>();
		response.StatusCode.Returns(statusCode);
		response.Resource.Returns(props);
		response.User.Returns(this);
		return response;
	}
}
