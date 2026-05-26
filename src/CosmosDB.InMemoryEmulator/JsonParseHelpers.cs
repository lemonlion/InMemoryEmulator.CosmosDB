using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

internal static class JsonParseHelpers
{
	internal static JObject ParseJson(string json)
	{
		using var reader = new JsonTextReader(new StringReader(json))
		{
			DateParseHandling = DateParseHandling.None
		};
		return (JObject)NormalizeNumbers(JObject.Load(reader));
	}

	internal static JToken ParseJsonToken(string json)
	{
		using var reader = new JsonTextReader(new StringReader(json))
		{
			DateParseHandling = DateParseHandling.None
		};
		return NormalizeNumbers(JToken.Load(reader));
	}

	// Ref: JavaScript: JSON.stringify(JSON.parse("1500.0")) === "1500"
	//   Real Cosmos DB uses a JavaScript engine (IEEE 754 double) which normalises whole-number
	//   JSON floats to integers. e.g. "1500.0" → 1500, "0.0" → 0.
	//   Fractional values (e.g. "100.50" → double 100.5) already round-trip correctly because
	//   doubles do not preserve trailing zeros, so no special handling is needed for them.
	private static JToken NormalizeNumbers(JToken token)
	{
		switch (token.Type)
		{
			case JTokenType.Float:
				var d = token.Value<double>();
				if (!double.IsNaN(d) && !double.IsInfinity(d) && d == Math.Truncate(d)
					&& d >= long.MinValue && d <= (double)long.MaxValue)
					return new JValue((long)d);
				return token;
			case JTokenType.Object:
				var obj = (JObject)token;
				foreach (var prop in obj.Properties().ToList())
					obj[prop.Name] = NormalizeNumbers(prop.Value);
				return obj;
			case JTokenType.Array:
				var arr = (JArray)token;
				for (var i = 0; i < arr.Count; i++)
					arr[i] = NormalizeNumbers(arr[i]);
				return arr;
			default:
				return token;
		}
	}
}
