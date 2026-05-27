using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// Default implementation of <see cref="IQueryPlanStrategy"/> that builds
/// <c>PartitionedQueryExecutionInfo</c> using the current Cosmos SDK wire format (v2).
/// </summary>
public sealed class DefaultQueryPlanStrategy : IQueryPlanStrategy
{
	/// <inheritdoc />
	public JObject BuildQueryPlan(string sqlQuery, CosmosSqlQuery? parsed, string collectionRid)
	{
		var queryInfo = new JObject
		{
			["distinctType"] = "None",
			["top"] = null,
			["offset"] = null,
			["limit"] = null,
			["orderBy"] = new JArray(),
			["orderByExpressions"] = new JArray(),
			["groupByExpressions"] = new JArray(),
			["groupByAliases"] = new JArray(),
			["aggregates"] = new JArray(),
			["groupByAliasToAggregateType"] = new JObject(),
			["rewrittenQuery"] = "",
			["hasSelectValue"] = false,
			["hasNonStreamingOrderBy"] = false
		};

		if (parsed is not null)
		{
			BuildFromParsedQuery(queryInfo, parsed, sqlQuery);
		}
		else
		{
			queryInfo["rewrittenQuery"] = sqlQuery;
		}

		// COUNT(DISTINCT ...) — dCountInfo
		AddCountDistinctInfo(queryInfo, sqlQuery);

		return new JObject
		{
			["partitionedQueryExecutionInfoVersion"] = FakeCosmosHandler.QueryPlanVersion,
			["queryInfo"] = queryInfo,
			["queryRanges"] = new JArray(new JObject
			{
				["min"] = "",
				["max"] = "FF",
				["isMinInclusive"] = true,
				["isMaxInclusive"] = false
			})
		};
	}

	private static void BuildFromParsedQuery(JObject queryInfo, CosmosSqlQuery parsed, string sqlQuery)
	{
		// ORDER BY
		if (parsed.OrderByFields is { Length: > 0 })
		{
			var orderByArr = new JArray();
			var orderByExprArr = new JArray();
			foreach (var field in parsed.OrderByFields)
			{
				orderByArr.Add(field.Ascending ? "Ascending" : "Descending");
				orderByExprArr.Add(field.Field ?? CosmosSqlParser.ExprToString(field.Expression));
			}

			queryInfo["orderBy"] = orderByArr;
			queryInfo["orderByExpressions"] = orderByExprArr;
			queryInfo["hasNonStreamingOrderBy"] = true;
		}

		// TOP
		if (parsed.TopCount.HasValue)
		{
			queryInfo["top"] = parsed.TopCount.Value;
		}

		// OFFSET / LIMIT
		if (parsed.Offset.HasValue)
		{
			queryInfo["offset"] = parsed.Offset.Value;
		}

		if (parsed.Limit.HasValue)
		{
			queryInfo["limit"] = parsed.Limit.Value;
		}

		// DISTINCT
		if (parsed.IsDistinct)
		{
			var isOrdered = false;
			if (parsed.IsValueSelect && parsed.OrderByFields is { Length: > 0 } && parsed.SelectFields.Length == 1)
			{
				var selectExpr = CosmosSqlParser.ExprToString(parsed.SelectFields[0].SqlExpr);
				var orderExpr = parsed.OrderByFields[0].Field ?? CosmosSqlParser.ExprToString(parsed.OrderByFields[0].Expression);
				isOrdered = string.Equals(selectExpr, orderExpr, StringComparison.OrdinalIgnoreCase);
			}
			queryInfo["distinctType"] = isOrdered ? "Ordered" : "Unordered";
		}

		// GROUP BY
		if (parsed.GroupByFields is { Length: > 0 })
		{
			queryInfo["groupByExpressions"] = new JArray(parsed.GroupByFields);
			var aliases = new JArray();
			foreach (var sf in parsed.SelectFields)
			{
				if (sf.Alias is not null)
					aliases.Add(sf.Alias);
				else if (sf.Expression is not null)
					aliases.Add(sf.Expression);
			}
			queryInfo["groupByAliases"] = aliases;
		}

		// Aggregates
		var aggregates = new JArray();
		var groupByAliasToAgg = new JObject();
		foreach (var field in parsed.SelectFields)
		{
			DetectAggregates(field.SqlExpr, aggregates, groupByAliasToAgg, field.Alias);
		}

		if (aggregates.Count > 0)
		{
			queryInfo["aggregates"] = aggregates;
		}

		if (groupByAliasToAgg.Count > 0)
		{
			queryInfo["groupByAliasToAggregateType"] = groupByAliasToAgg;
		}

		// SELECT VALUE
		if (parsed.IsValueSelect)
		{
			queryInfo["hasSelectValue"] = true;
		}

		// Suppress SDK pipeline for GROUP BY, multi-aggregate, and VALUE aggregate
		// queries. Also bypass any query referencing COUNTIF — the SDK pipeline
		// doesn't recognize COUNTIF and tries to apply its built-in aggregate
		// accumulator, which expects a {payload: {alias: {item: value}}} envelope
		// the handler only produces for GROUP BY responses.
		// Additionally bypass when literal expressions appear alongside aggregates
		// (e.g. SELECT 42 AS X, COUNT(1) AS N) — the SDK's AggregateQueryPipelineStage
		// doesn't handle non-aggregate expression fields and crashes with
		// "Underlying object does not have an 'payload' field".
		// Also bypass single-aggregate projection queries (e.g. SELECT SUM(c.x) AS Total FROM c)
		// for the same reason — the SDK's AggregateQueryPipelineStage expects a {payload: ...}
		// envelope that the emulator doesn't produce for projection-style aggregate queries.
		var isGroupByBypass = parsed.GroupByFields is { Length: > 0 };
		var aggregateFieldCount = parsed.SelectFields.Count(f => ContainsAggregate(f.SqlExpr));
		var isMultiAggregateBypass = !isGroupByBypass && !parsed.IsValueSelect
			&& (aggregateFieldCount > 1 || aggregates.Count > 1);
		var isValueAggregateBypass = !isGroupByBypass && parsed.IsValueSelect && aggregateFieldCount > 0;
		var isCountIfBypass = !isGroupByBypass
			&& parsed.SelectFields.Any(f => ContainsCountIf(f.SqlExpr));
		// Bypass when there are non-aggregate expression fields (literals, function calls)
		// alongside at least one aggregate — the SDK pipeline can't project these.
		var isLiteralWithAggregateBypass = !isGroupByBypass && !parsed.IsValueSelect
			&& aggregateFieldCount > 0
			&& parsed.SelectFields.Any(f => !ContainsAggregate(f.SqlExpr)
				&& f.SqlExpr is not null and not IdentifierExpression and not PropertyAccessExpression);
		// Bypass single-aggregate projection: SELECT SUM(c.x) AS Total FROM c
		// The emulator returns results directly without the payload envelope the SDK expects.
		var isSingleAggregateProjectionBypass = !isGroupByBypass && !parsed.IsValueSelect
			&& aggregateFieldCount == 1 && aggregates.Count == 1;

		if (isGroupByBypass || isMultiAggregateBypass || isValueAggregateBypass || isCountIfBypass || isLiteralWithAggregateBypass || isSingleAggregateProjectionBypass)
		{
			queryInfo["groupByExpressions"] = new JArray();
			queryInfo["groupByAliases"] = new JArray();
			queryInfo["groupByAliasToAggregateType"] = new JObject();
			queryInfo["aggregates"] = new JArray();
			if (isGroupByBypass)
			{
				queryInfo["hasNonStreamingOrderBy"] = false;
				queryInfo["orderBy"] = new JArray();
				queryInfo["orderByExpressions"] = new JArray();
			}
		}

		// Rewritten query
		if (parsed.OrderByFields is { Length: > 0 })
		{
			queryInfo["rewrittenQuery"] = FakeCosmosHandler.BuildOrderByRewrittenQueryStatic(parsed);
		}
		else if (parsed.Offset.HasValue || parsed.Limit.HasValue)
		{
			queryInfo["rewrittenQuery"] = FakeCosmosHandler.StripOffsetLimitStatic(sqlQuery);
		}
		else
		{
			queryInfo["rewrittenQuery"] = sqlQuery;
		}
	}

	internal static void DetectAggregates(
		SqlExpression? expr, JArray aggregates, JObject groupByAliasToAgg, string? alias)
	{
		if (expr is FunctionCallExpression func)
		{
			var name = func.FunctionName.ToUpperInvariant();
			string? aggType = name switch
			{
				"COUNT" => "Count",
				"COUNTIF" => "CountIf",
				"SUM" => "Sum",
				"MIN" => "Min",
				"MAX" => "Max",
				"AVG" => "Average",
				_ => null
			};

			if (aggType is not null)
			{
				if (!aggregates.Any(a => a.ToString() == aggType))
				{
					aggregates.Add(aggType);
				}

				if (alias is not null)
				{
					groupByAliasToAgg[alias] = aggType;
				}
			}
			else
			{
				foreach (var arg in func.Arguments)
				{
					DetectAggregates(arg, aggregates, groupByAliasToAgg, alias);
				}
			}
		}
		else if (expr is BinaryExpression bin)
		{
			DetectAggregates(bin.Left, aggregates, groupByAliasToAgg, alias);
			DetectAggregates(bin.Right, aggregates, groupByAliasToAgg, alias);
		}
		else if (expr is UnaryExpression unary)
		{
			DetectAggregates(unary.Operand, aggregates, groupByAliasToAgg, alias);
		}
		else if (expr is TernaryExpression ternary)
		{
			DetectAggregates(ternary.Condition, aggregates, groupByAliasToAgg, alias);
			DetectAggregates(ternary.IfTrue, aggregates, groupByAliasToAgg, alias);
			DetectAggregates(ternary.IfFalse, aggregates, groupByAliasToAgg, alias);
		}
		else if (expr is CoalesceExpression coalesce)
		{
			DetectAggregates(coalesce.Left, aggregates, groupByAliasToAgg, alias);
			DetectAggregates(coalesce.Right, aggregates, groupByAliasToAgg, alias);
		}
	}

	internal static bool ContainsAggregate(SqlExpression? expr)
	{
		return expr switch
		{
			FunctionCallExpression func =>
				func.FunctionName.ToUpperInvariant() is "COUNT" or "COUNTIF" or "SUM" or "MIN" or "MAX" or "AVG"
				|| func.Arguments.Any(ContainsAggregate),
			BinaryExpression bin => ContainsAggregate(bin.Left) || ContainsAggregate(bin.Right),
			UnaryExpression unary => ContainsAggregate(unary.Operand),
			TernaryExpression ternary => ContainsAggregate(ternary.Condition) || ContainsAggregate(ternary.IfTrue) || ContainsAggregate(ternary.IfFalse),
			CoalesceExpression coalesce => ContainsAggregate(coalesce.Left) || ContainsAggregate(coalesce.Right),
			_ => false
		};
	}

	private static bool ContainsCountIf(SqlExpression? expr)
	{
		return expr switch
		{
			FunctionCallExpression func =>
				func.FunctionName.Equals("COUNTIF", StringComparison.OrdinalIgnoreCase)
				|| func.Arguments.Any(ContainsCountIf),
			BinaryExpression bin => ContainsCountIf(bin.Left) || ContainsCountIf(bin.Right),
			UnaryExpression unary => ContainsCountIf(unary.Operand),
			TernaryExpression ternary => ContainsCountIf(ternary.Condition) || ContainsCountIf(ternary.IfTrue) || ContainsCountIf(ternary.IfFalse),
			CoalesceExpression coalesce => ContainsCountIf(coalesce.Left) || ContainsCountIf(coalesce.Right),
			_ => false
		};
	}

	private static void AddCountDistinctInfo(JObject queryInfo, string sqlQuery)
	{
		var countDistinctMatch = Regex.Match(sqlQuery,
			@"COUNT\s*\(\s*DISTINCT\s+(.+?)\s*\)", RegexOptions.IgnoreCase);
		if (!countDistinctMatch.Success) return;

		var distinctExpr = countDistinctMatch.Groups[1].Value.Trim();

		var aliasMatch = Regex.Match(sqlQuery,
			@"\bFROM\s+\w+\s+(?:AS\s+)?(\w+)", RegexOptions.IgnoreCase);
		if (!aliasMatch.Success)
			aliasMatch = Regex.Match(sqlQuery, @"\bFROM\s+(\w+)", RegexOptions.IgnoreCase);
		var fromAlias = aliasMatch.Success ? aliasMatch.Groups[1].Value : "c";

		var distinctPath = distinctExpr;
		if (distinctPath.StartsWith(fromAlias + ".", StringComparison.OrdinalIgnoreCase))
			distinctPath = distinctPath[(fromAlias.Length + 1)..];

		var isSimplePath = Regex.IsMatch(distinctPath, @"^[\w.]+$");

		queryInfo["dCountInfo"] = new JObject
		{
			["dCountAlias"] = "$1",
			["dCountExpressionBase"] = isSimplePath
				? new JObject { ["kind"] = "PropertyRef", ["propertyPath"] = distinctPath }
				: new JObject { ["kind"] = "Expression", ["expression"] = distinctExpr }
		};
	}
}
