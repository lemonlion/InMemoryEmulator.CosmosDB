using System.Globalization;
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
        var obj = JObject.Load(reader);

        // Normalize numeric values to match Cosmos DB's minimal JSON representation.
        // Cosmos DB stores numbers without trailing zeros: 1500.0 → 1500, 100.50 → 100.5.
        // We serialize with a custom writer that strips trailing zeros, then reparse so the
        // internal JValue representations have the correct scale for subsequent ToString() calls.
        var normalized = ToCosmosJson(obj);
        using var reader2 = new JsonTextReader(new StringReader(normalized))
        {
            DateParseHandling = DateParseHandling.None
        };
        return JObject.Load(reader2);
    }

    internal static JToken ParseJsonToken(string json)
    {
        using var reader = new JsonTextReader(new StringReader(json))
        {
            DateParseHandling = DateParseHandling.None
        };
        var token = JToken.Load(reader);

        if (token is JObject or JArray)
        {
            var normalized = ToCosmosJson(token);
            using var reader2 = new JsonTextReader(new StringReader(normalized))
            {
                DateParseHandling = DateParseHandling.None
            };
            return JToken.Load(reader2);
        }

        return token;
    }

    /// <summary>
    /// Serializes a JToken to JSON using Cosmos DB's minimal numeric representation.
    /// Whole numbers are written without .0, trailing fractional zeros are stripped.
    /// </summary>
    /// <remarks>
    /// Ref: https://learn.microsoft.com/en-us/rest/api/cosmos-db/documents
    ///   Cosmos DB uses JSON (RFC 8259) for document storage. JSON numbers have no
    ///   concept of "scale" — 1500.0 and 1500 are the same value. Cosmos normalizes
    ///   to the minimal representation on storage.
    /// </remarks>
    internal static string ToCosmosJson(JToken token)
    {
        using var sw = new StringWriter();
        using var writer = new CosmosJsonWriter(sw) { Formatting = Formatting.None };
        token.WriteTo(writer);
        return sw.ToString();
    }

    /// <summary>
    /// Normalizes a numeric aggregate result to match Cosmos DB's output format.
    /// Whole-number results are returned as long; fractional results as double
    /// with trailing zeros stripped.
    /// </summary>
    internal static object NormalizeNumericResult(double value)
    {
        // Ref: Observed Cosmos DB emulator behavior — SUM/AVG of whole numbers
        // returns integers (no .0), e.g. SUM(375, 375) = 750 not 750.0.
        // We use strict < for MaxValue because (double)long.MaxValue rounds up to 2^63,
        // which overflows the long cast on .NET 8 (wraps to long.MinValue).
        if (value == Math.Truncate(value) && value >= long.MinValue && value < (double)long.MaxValue)
            return (long)value;
        return value;
    }

    /// <summary>
    /// Custom JsonTextWriter that writes numbers in their minimal JSON representation,
    /// matching how Cosmos DB stores numeric values.
    /// </summary>
    private sealed class CosmosJsonWriter : JsonTextWriter
    {
        public CosmosJsonWriter(TextWriter writer) : base(writer) { }

        public override void WriteValue(decimal value)
        {
            if (value == decimal.Truncate(value) && value >= long.MinValue && value <= long.MaxValue)
            {
                // Whole number — write as integer (no .0 suffix)
                WriteValue((long)value);
            }
            else
            {
                // Fractional — write minimal representation (strip trailing zeros)
                WriteRawValue(value.ToString("G29", CultureInfo.InvariantCulture));
            }
        }

        public override void WriteValue(double value)
        {
            // Strict < for MaxValue: (double)long.MaxValue rounds up to 2^63 which
            // cannot be cast to long without overflow on .NET 8.
            if (value == Math.Truncate(value) && value >= long.MinValue && value < (double)long.MaxValue)
            {
                // Whole number — write as integer
                WriteValue((long)value);
            }
            else
            {
                base.WriteValue(value);
            }
        }
    }
}
