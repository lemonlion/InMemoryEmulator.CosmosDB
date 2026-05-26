using System.Net;
using Jint;
using Jint.Native;
using Jint.Runtime;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator.JsTriggers;

/// <summary>
/// Jint-based implementation of <see cref="IJsTriggerEngine"/>.
/// Executes Cosmos DB trigger JavaScript bodies using the Jint interpreter.
/// Provides the Cosmos DB server-side API: getContext(), getRequest()/getResponse(), getBody(), setBody().
/// </summary>
public class JintTriggerEngine : IJsTriggerEngine, IJsUdfEngine
{
	public JObject ExecutePreTrigger(string jsBody, JObject document)
		=> ExecutePreTrigger(jsBody, document, null!);

	public JObject ExecutePreTrigger(string jsBody, JObject document, ICollectionContext context)
	{
		var bodyJson = document.ToString(Newtonsoft.Json.Formatting.None);
		JsValue? updatedBody = null;

		var engine = new Engine(options =>
		{
			options.TimeoutInterval(TimeSpan.FromSeconds(5));
			options.MaxStatements(10_000);
		});

		// Parse the document into JS land once
		var jsDoc = engine.Evaluate($"({bodyJson})");

		// Wire up the Cosmos DB server-side pre-trigger API
		engine.SetValue("__getBody", new Func<JsValue>(() => jsDoc));
		engine.SetValue("__setBody", new Action<JsValue>(val => updatedBody = val));

		JintSprocEngine.WireCollectionContext(engine, context);

		engine.Execute("""
            function getContext() {
                return {
                    getRequest: function() {
                        return {
                            getBody: function() { return __getBody(); },
                            setBody: function(doc) { __setBody(doc); }
                        };
                    },
                    getResponse: function() {
                        throw new Error("getResponse() is not available in pre-triggers.");
                    },
                    getCollection: function() { return __collection; }
                };
            }
            """);

		try
		{
			engine.Execute(jsBody);
			InvokeFirstFunction(engine, jsBody);
		}
		catch (JavaScriptException ex)
		{
			throw InMemoryCosmosException.Create(
				$"Pre-trigger failed: {ex.Message}",
				HttpStatusCode.BadRequest, 0, string.Empty, 0);
		}

		if (updatedBody is not null)
		{
			engine.SetValue("__toSerialize", updatedBody);
			var json = engine.Evaluate("JSON.stringify(__toSerialize)").AsString();
			return JObject.Parse(json);
		}

		return document;
	}

	public JObject? ExecutePostTrigger(string jsBody, JObject document)
		=> ExecutePostTrigger(jsBody, document, null!);

	public JObject? ExecutePostTrigger(string jsBody, JObject document, ICollectionContext context)
	{
		var bodyJson = document.ToString(Newtonsoft.Json.Formatting.None);
		JsValue? updatedBody = null;

		var engine = new Engine(options =>
		{
			options.TimeoutInterval(TimeSpan.FromSeconds(5));
			options.MaxStatements(10_000);
		});

		// Parse the document into JS land once
		var jsDoc = engine.Evaluate($"({bodyJson})");

		// Wire up the Cosmos DB server-side post-trigger API
		// Post-triggers have access to both getResponse() and getRequest()
		engine.SetValue("__getBody", new Func<JsValue>(() => jsDoc));
		engine.SetValue("__setBody", new Action<JsValue>(val => updatedBody = val));

		JintSprocEngine.WireCollectionContext(engine, context);

		engine.Execute("""
            function getContext() {
                return {
                    getResponse: function() {
                        return {
                            getBody: function() { return __getBody(); },
                            setBody: function(doc) { __setBody(doc); }
                        };
                    },
                    getRequest: function() {
                        return {
                            getBody: function() { return __getBody(); }
                        };
                    },
                    getCollection: function() { return __collection; }
                };
            }
            """);

		try
		{
			engine.Execute(jsBody);
			InvokeFirstFunction(engine, jsBody);
		}
		catch (JavaScriptException ex)
		{
			throw InMemoryCosmosException.Create(
				$"Post-trigger failed: {ex.Message}",
				HttpStatusCode.BadRequest, 0, string.Empty, 0);
		}

		if (updatedBody is not null)
		{
			engine.SetValue("__toSerialize", updatedBody);
			var json = engine.Evaluate("JSON.stringify(__toSerialize)").AsString();
			return JObject.Parse(json);
		}

		return null;
	}

	private static void InvokeFirstFunction(Engine engine, string jsBody)
	{
		var matches = System.Text.RegularExpressions.Regex.Matches(jsBody, @"\bfunction\s+(\w+)\s*\(");
		if (matches.Count > 0)
		{
			engine.Invoke(matches[0].Groups[1].Value);
		}
	}

	public object? ExecuteUdf(string jsBody, object[] args)
	{
		var engine = new Engine(options =>
		{
			options.TimeoutInterval(TimeSpan.FromSeconds(5));
			options.MaxStatements(10_000);
		});

		try
		{
			// Find the function name
			var matches = System.Text.RegularExpressions.Regex.Matches(jsBody, @"\bfunction\s+(\w+)\s*\(");
			if (matches.Count > 0)
			{
				engine.Execute(jsBody);
				var jsArgs = ConvertArgsToJsValues(engine, args);
				var result = engine.Invoke(matches[0].Groups[1].Value, jsArgs);
				return ConvertJsResult(result);
			}
			else
			{
				var func = engine.Evaluate($"({jsBody})");
				var jsArgs = ConvertArgsToJsValues(engine, args);
				var result = engine.Invoke(func, jsArgs);
				return ConvertJsResult(result);
			}
		}
		catch (JavaScriptException ex)
		{
			throw InMemoryCosmosException.Create(
				$"UDF execution failed: {ex.Message}",
				HttpStatusCode.BadRequest, 0, string.Empty, 0);
		}
	}

	private static JsValue[] ConvertArgsToJsValues(Engine engine, object[] args)
	{
		var jsArgs = new JsValue[args?.Length ?? 0];
		for (var i = 0; i < jsArgs.Length; i++)
		{
			var arg = args![i];
			if (arg is null) jsArgs[i] = JsValue.Null;
			else if (arg is string s) jsArgs[i] = new JsString(s);
			else if (arg is int iv) jsArgs[i] = new JsNumber(iv);
			else if (arg is long lv) jsArgs[i] = new JsNumber(lv);
			else if (arg is double dv) jsArgs[i] = new JsNumber(dv);
			else if (arg is bool bv) jsArgs[i] = bv ? JsBoolean.True : JsBoolean.False;
			else jsArgs[i] = engine.Evaluate($"({Newtonsoft.Json.JsonConvert.SerializeObject(arg)})");
		}
		return jsArgs;
	}

	private static object? ConvertJsResult(JsValue result)
	{
		if (result.IsNull() || result.IsUndefined()) return null;
		if (result.IsBoolean()) return result.AsBoolean();
		if (result.IsNumber()) return result.AsNumber();
		if (result.IsString()) return result.AsString();
		// For objects/arrays, serialize to JSON and parse as JToken for proper deserialization
		var engine = new Engine();
		engine.SetValue("__val", result);
		var json = engine.Evaluate("JSON.stringify(__val)").AsString();
		return Newtonsoft.Json.Linq.JToken.Parse(json);
	}
}
