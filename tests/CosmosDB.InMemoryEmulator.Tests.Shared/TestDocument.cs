using Newtonsoft.Json;

namespace CosmosDB.InMemoryEmulator.Tests;

public class TestDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("partitionKey")]
	public string PartitionKey { get; set; } = default!;

	[JsonProperty("name")]
	public string Name { get; set; } = default!;

	[JsonProperty("value")]
	public int Value { get; set; }

	[JsonProperty("isActive")]
	public bool IsActive { get; set; } = true;

	[JsonProperty("tags")]
	public string[] Tags { get; set; } = [];

	[JsonProperty("nested")]
	public NestedObject? Nested { get; set; }
}

public class NestedObject
{
	[JsonProperty("description")]
	public string Description { get; set; } = default!;

	[JsonProperty("score")]
	public double Score { get; set; }
}

public class GeoDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("partitionKey")]
	public string PartitionKey { get; set; } = default!;

	[JsonProperty("name")]
	public string? Name { get; set; }

	[JsonProperty("location")]
	public GeoJsonGeometry? Location { get; set; }

	[JsonProperty("area")]
	public GeoJsonGeometry? Area { get; set; }
}

public class GeoJsonGeometry
{
	[JsonProperty("type")]
	public string Type { get; set; } = default!;

	[JsonProperty("coordinates")]
	public object Coordinates { get; set; } = default!;
}

public class UdfDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("partitionKey")]
	public string PartitionKey { get; set; } = default!;

	[JsonProperty("value")]
	public double Value { get; set; }

	[JsonProperty("x")]
	public double X { get; set; }

	[JsonProperty("y")]
	public double Y { get; set; }
}

public class MultiJoinDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("pk")]
	public string Pk { get; set; } = default!;

	[JsonProperty("colors")]
	public string[] Colors { get; set; } = [];

	[JsonProperty("sizes")]
	public string[] Sizes { get; set; } = [];
}

public class CustomKeyDocument
{
	[JsonProperty("id")]
	public string Id { get; set; } = default!;

	[JsonProperty("customKey")]
	public string CustomKey { get; set; } = default!;

	[JsonProperty("name")]
	public string Name { get; set; } = default!;
}
