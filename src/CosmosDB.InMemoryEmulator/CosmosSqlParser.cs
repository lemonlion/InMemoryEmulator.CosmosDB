#nullable disable
using System.Collections.Concurrent;
using Superpower;
using Superpower.Model;
using Superpower.Parsers;
using Superpower.Tokenizers;

namespace CosmosDB.InMemoryEmulator;

// ──────────────────────────────────────────────
//  Token kinds
// ──────────────────────────────────────────────

public enum CosmosSqlToken
{
	// Keywords
	Select,
	Distinct,
	Top,
	Value,
	As,
	From,
	Join,
	In,
	Where,
	And,
	Or,
	Not,
	Between,
	Like,
	Is,
	Null,
	Defined,
	Undefined,
	True,
	False,
	Exists,
	Order,
	By,
	Asc,
	Desc,
	Group,
	Having,
	Offset,
	Limit,
	Array,
	Escape,

	// Literals & identifiers
	Identifier,
	StringLiteral,
	DoubleQuotedString,
	NumberLiteral,
	Parameter,

	// Operators & punctuation
	Star,
	Comma,
	Dot,
	OpenParen,
	CloseParen,
	OpenBracket,
	CloseBracket,
	Equals,
	NotEquals,
	LessThanOrEqual,
	GreaterThanOrEqual,
	LessThan,
	GreaterThan,
	Plus,
	Minus,
	Slash,
	Percent,
	Ampersand,
	Pipe,
	Caret,
	Tilde,
	QuestionQuestion,
	Question,
	Colon,
	DoublePipe,
	OpenBrace,
	CloseBrace,
}

// ──────────────────────────────────────────────
//  AST nodes
// ──────────────────────────────────────────────

public enum ComparisonOp
{
	Equal,
	NotEqual,
	LessThan,
	GreaterThan,
	LessThanOrEqual,
	GreaterThanOrEqual,
	Like,
}

public sealed record SelectField(string Expression, string Alias, SqlExpression SqlExpr = null);

public sealed record JoinClause(string Alias, string SourceAlias, string ArrayField);

public sealed record OrderByField(string Field, bool Ascending, SqlExpression Expression = null);

// ── Expression tree ──

public abstract record SqlExpression;

public sealed record LiteralExpression(object Value) : SqlExpression;

/// <summary>
/// Represents the Cosmos SQL <c>undefined</c> keyword. Distinguished from the
/// string literal <c>'undefined'</c> so the evaluator can map it to the runtime
/// undefined sentinel rather than a defined string value.
/// </summary>
public sealed record UndefinedLiteralExpression : SqlExpression;

public sealed record IdentifierExpression(string Name) : SqlExpression;

public sealed record ParameterExpression(string Name) : SqlExpression;

public sealed record PropertyAccessExpression(SqlExpression Object, string Property) : SqlExpression;

public sealed record IndexAccessExpression(SqlExpression Object, SqlExpression Index) : SqlExpression;

public sealed record BinaryExpression(SqlExpression Left, BinaryOp Operator, SqlExpression Right) : SqlExpression;

public sealed record UnaryExpression(UnaryOp Operator, SqlExpression Operand) : SqlExpression;

public sealed record FunctionCallExpression(string FunctionName, SqlExpression[] Arguments) : SqlExpression;

public sealed record BetweenExpression(SqlExpression Value, SqlExpression Low, SqlExpression High) : SqlExpression;

public sealed record InExpression(SqlExpression Value, SqlExpression[] List) : SqlExpression;

public sealed record LikeExpression(SqlExpression Value, SqlExpression Pattern, string EscapeChar) : SqlExpression;

public sealed record ExistsExpression(string RawSubquery) : SqlExpression;

public sealed record SubqueryExpression(CosmosSqlQuery Subquery) : SqlExpression;

public sealed record TernaryExpression(SqlExpression Condition, SqlExpression IfTrue, SqlExpression IfFalse) : SqlExpression;

public sealed record CoalesceExpression(SqlExpression Left, SqlExpression Right) : SqlExpression;

public sealed record ObjectLiteralExpression(KeyValuePair<string, SqlExpression>[] Properties) : SqlExpression;

public sealed record ArrayLiteralExpression(SqlExpression[] Elements) : SqlExpression;

public enum BinaryOp
{
	Equal, NotEqual, LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual,
	And, Or,
	Add, Subtract, Multiply, Divide, Modulo,
	BitwiseAnd, BitwiseOr, BitwiseXor,
	Like,
	StringConcat,
}

public enum UnaryOp
{
	Not, Negate, BitwiseNot,
}

// ── Backward-compatible WhereExpression wrappers ──

public abstract record WhereExpression;

public sealed record ComparisonCondition(string Left, ComparisonOp Operator, string Right) : WhereExpression;

public sealed record FunctionCondition(string FunctionName, string[] Arguments) : WhereExpression;

public sealed record AndCondition(WhereExpression Left, WhereExpression Right) : WhereExpression;

public sealed record OrCondition(WhereExpression Left, WhereExpression Right) : WhereExpression;

public sealed record NotCondition(WhereExpression Inner) : WhereExpression;

public sealed record ExistsCondition(string RawSubquery) : WhereExpression;

public sealed record SqlExpressionCondition(SqlExpression Expression) : WhereExpression;

// ── Query ──

public sealed record CosmosSqlQuery(
	SelectField[] SelectFields,
	bool IsSelectAll,
	int? TopCount,
	string FromAlias,
	JoinClause Join,
	WhereExpression Where,
	int? Offset,
	int? Limit,
	OrderByClause OrderBy = null,
	bool IsDistinct = false,
	bool IsValueSelect = false,
	JoinClause[] Joins = null,
	OrderByField[] OrderByFields = null,
	string[] GroupByFields = null,
	SqlExpression[] GroupByExpressions = null,
	WhereExpression Having = null,
	SqlExpression WhereExpr = null,
	SqlExpression HavingExpr = null,
	string FromSource = null,
	SqlExpression RankExpression = null);

public sealed record OrderByClause(string Field, bool Ascending);

// ──────────────────────────────────────────────
//  Tokenizer
// ──────────────────────────────────────────────

public static class CosmosSqlTokenizer
{
	private static readonly TextParser<Unit> StringLiteralToken =
		from open in Character.EqualTo('\'')
		from content in Character.EqualTo('\'').Then(_ => Character.EqualTo('\'')).Try()
			.Or(Character.Except('\''))
			.Many()
		from close in Character.EqualTo('\'')
		select Unit.Value;

	private static readonly TextParser<Unit> DoubleQuotedStringToken =
		from open in Character.EqualTo('"')
		from content in Character.Except('"').Many()
		from close in Character.EqualTo('"')
		select Unit.Value;

	private static readonly TextParser<Unit> ParameterToken =
		from at in Character.EqualTo('@')
		from rest in Character.LetterOrDigit.Or(Character.EqualTo('_')).AtLeastOnce()
		select Unit.Value;

	private static readonly TextParser<Unit> NumberToken =
		(from number in Numerics.Decimal
		 from exp in (
			 from e in Character.EqualTo('e').Or(Character.EqualTo('E'))
			 from sign in Character.EqualTo('+').Or(Character.EqualTo('-')).OptionalOrDefault()
			 from digits in Character.Digit.AtLeastOnce()
			 select Unit.Value
		 ).OptionalOrDefault()
		 select Unit.Value);

	// Ref: EF Core Cosmos provider uses $type as a discriminator column.
	//   Allow $ as a valid identifier character to support queries like c.$type.
	private static readonly TextParser<Unit> IdentOrKeyword =
		from first in Character.Letter.Or(Character.EqualTo('_')).Or(Character.EqualTo('$'))
		from rest in Character.LetterOrDigit.Or(Character.EqualTo('_')).Or(Character.EqualTo('$')).Many()
		select Unit.Value;

	// Quoted identifiers like [My Column] are not used by the Cosmos SDK's LINQ provider
	// (it uses root["name"] with double-quoted strings instead). Removing this tokenizer rule
	// allows [expr, expr] array literals and [0] numeric indexers to tokenize correctly.
	// If needed in the future, require the content to contain a space to disambiguate.

	private static readonly Dictionary<string, CosmosSqlToken> Keywords = new(StringComparer.OrdinalIgnoreCase)
	{
		["SELECT"] = CosmosSqlToken.Select,
		["DISTINCT"] = CosmosSqlToken.Distinct,
		["TOP"] = CosmosSqlToken.Top,
		["VALUE"] = CosmosSqlToken.Value,
		["AS"] = CosmosSqlToken.As,
		["FROM"] = CosmosSqlToken.From,
		["JOIN"] = CosmosSqlToken.Join,
		["IN"] = CosmosSqlToken.In,
		["WHERE"] = CosmosSqlToken.Where,
		["AND"] = CosmosSqlToken.And,
		["OR"] = CosmosSqlToken.Or,
		["NOT"] = CosmosSqlToken.Not,
		["BETWEEN"] = CosmosSqlToken.Between,
		["LIKE"] = CosmosSqlToken.Like,
		["IS"] = CosmosSqlToken.Is,
		["NULL"] = CosmosSqlToken.Null,
		["DEFINED"] = CosmosSqlToken.Defined,
		["UNDEFINED"] = CosmosSqlToken.Undefined,
		["TRUE"] = CosmosSqlToken.True,
		["FALSE"] = CosmosSqlToken.False,
		["EXISTS"] = CosmosSqlToken.Exists,
		["ORDER"] = CosmosSqlToken.Order,
		["BY"] = CosmosSqlToken.By,
		["ASC"] = CosmosSqlToken.Asc,
		["DESC"] = CosmosSqlToken.Desc,
		["GROUP"] = CosmosSqlToken.Group,
		["HAVING"] = CosmosSqlToken.Having,
		["OFFSET"] = CosmosSqlToken.Offset,
		["LIMIT"] = CosmosSqlToken.Limit,
		["ARRAY"] = CosmosSqlToken.Array,
		["ESCAPE"] = CosmosSqlToken.Escape,
	};

	private static readonly Tokenizer<CosmosSqlToken> Inner = new TokenizerBuilder<CosmosSqlToken>()
		.Ignore(Span.WhiteSpace)
		.Match(StringLiteralToken, CosmosSqlToken.StringLiteral)
		.Match(DoubleQuotedStringToken, CosmosSqlToken.DoubleQuotedString)
		.Match(ParameterToken, CosmosSqlToken.Parameter)
		.Match(NumberToken, CosmosSqlToken.NumberLiteral)
		.Match(IdentOrKeyword, CosmosSqlToken.Identifier)
		.Match(Character.EqualTo('*'), CosmosSqlToken.Star)
		.Match(Character.EqualTo(','), CosmosSqlToken.Comma)
		.Match(Character.EqualTo('.'), CosmosSqlToken.Dot)
		.Match(Character.EqualTo('('), CosmosSqlToken.OpenParen)
		.Match(Character.EqualTo(')'), CosmosSqlToken.CloseParen)
		.Match(Character.EqualTo('['), CosmosSqlToken.OpenBracket)
		.Match(Character.EqualTo(']'), CosmosSqlToken.CloseBracket)
		.Match(Span.EqualTo("!="), CosmosSqlToken.NotEquals)
		.Match(Span.EqualTo("<>"), CosmosSqlToken.NotEquals)
		.Match(Span.EqualTo("<="), CosmosSqlToken.LessThanOrEqual)
		.Match(Span.EqualTo(">="), CosmosSqlToken.GreaterThanOrEqual)
		.Match(Span.EqualTo("??"), CosmosSqlToken.QuestionQuestion)
		.Match(Span.EqualTo("||"), CosmosSqlToken.DoublePipe)
		.Match(Character.EqualTo('='), CosmosSqlToken.Equals)
		.Match(Character.EqualTo('<'), CosmosSqlToken.LessThan)
		.Match(Character.EqualTo('>'), CosmosSqlToken.GreaterThan)
		.Match(Character.EqualTo('+'), CosmosSqlToken.Plus)
		.Match(Character.EqualTo('-'), CosmosSqlToken.Minus)
		.Match(Character.EqualTo('/'), CosmosSqlToken.Slash)
		.Match(Character.EqualTo('%'), CosmosSqlToken.Percent)
		.Match(Character.EqualTo('&'), CosmosSqlToken.Ampersand)
		.Match(Character.EqualTo('|'), CosmosSqlToken.Pipe)
		.Match(Character.EqualTo('^'), CosmosSqlToken.Caret)
		.Match(Character.EqualTo('~'), CosmosSqlToken.Tilde)
		.Match(Character.EqualTo('?'), CosmosSqlToken.Question)
		.Match(Character.EqualTo(':'), CosmosSqlToken.Colon)
		.Match(Character.EqualTo('{'), CosmosSqlToken.OpenBrace)
		.Match(Character.EqualTo('}'), CosmosSqlToken.CloseBrace)
		.Build();

	public static TokenList<CosmosSqlToken> Tokenize(string input)
	{
		var tokens = Inner.Tokenize(input);
		var remapped = tokens.Select(token =>
		{
			if (token.Kind == CosmosSqlToken.Identifier &&
				Keywords.TryGetValue(token.Span.ToStringValue(), out var keyword))
			{
				return new Token<CosmosSqlToken>(keyword, token.Span);
			}
			return token;
		}).ToArray();
		return new TokenList<CosmosSqlToken>(remapped);
	}
}

// ──────────────────────────────────────────────
//  Parser (token-based combinators)
// ──────────────────────────────────────────────

public static class CosmosSqlParser
{
	private static readonly HashSet<string> LegacyFunctionNames = new(StringComparer.OrdinalIgnoreCase)
	{
		"STARTSWITH", "ENDSWITH", "CONTAINS", "ARRAY_CONTAINS", "IS_DEFINED", "IS_NULL",
	};

	// ── Helpers ──

	private static string TokenSpanToString(Token<CosmosSqlToken> token) =>
		token.Span.ToStringValue();

	private static TokenListParser<CosmosSqlToken, string> AnyIdentifierOrKeyword =>
		Token.EqualTo(CosmosSqlToken.Identifier).Select(TokenSpanToString)
			.Or(Token.EqualTo(CosmosSqlToken.Value).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.Array).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.Defined).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.Asc).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.Desc).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.Top).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.Limit).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.Offset).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.Null).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.True).Select(TokenSpanToString))
			.Or(Token.EqualTo(CosmosSqlToken.False).Select(TokenSpanToString));

	// ── Dotted path: ident.ident.ident ──

	private static readonly TokenListParser<CosmosSqlToken, string> DottedPath =
		from first in AnyIdentifierOrKeyword
		from rest in (
			from dot in Token.EqualTo(CosmosSqlToken.Dot)
			from next in AnyIdentifierOrKeyword
			select "." + next
		).Many()
		select first + string.Concat(rest);

	// ── Dotted path with optional array indexing: ident.ident[0].ident ──

	// Helper: a bracketed string inside [ ] can be single- or double-quoted
	private static readonly TokenListParser<CosmosSqlToken, string> BracketedStringContent =
		Token.EqualTo(CosmosSqlToken.StringLiteral).Select(t => t.Span.ToStringValue()[1..^1])
			.Or(Token.EqualTo(CosmosSqlToken.DoubleQuotedString).Select(t => t.Span.ToStringValue()[1..^1]));

	private static readonly TokenListParser<CosmosSqlToken, string> DottedPathWithIndex =
		from first in AnyIdentifierOrKeyword
		from rest in (
			from dot in Token.EqualTo(CosmosSqlToken.Dot)
			from next in AnyIdentifierOrKeyword
			select "." + next
		).Or(
			(from open in Token.EqualTo(CosmosSqlToken.OpenBracket)
			 from idx in Token.EqualTo(CosmosSqlToken.NumberLiteral).Select(TokenSpanToString)
			 from close in Token.EqualTo(CosmosSqlToken.CloseBracket)
			 select "[" + idx + "]").Try()
		).Or(
			(from open in Token.EqualTo(CosmosSqlToken.OpenBracket)
			 from str in BracketedStringContent
			 from close in Token.EqualTo(CosmosSqlToken.CloseBracket)
			 select "." + str).Try()
		).Many()
		select first + string.Concat(rest);

	// ── Primary expression ──

	// String literal expression: accepts both single-quoted (Cosmos SQL standard) and
	// double-quoted strings (used by the SDK's LINQ provider in generated queries).
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> StringLiteral =
		Token.EqualTo(CosmosSqlToken.StringLiteral)
			.Select(t =>
			{
				var raw = t.Span.ToStringValue();
				var unquoted = raw[1..^1].Replace("''", "'");
				return (SqlExpression)new LiteralExpression(unquoted);
			})
		.Or(Token.EqualTo(CosmosSqlToken.DoubleQuotedString)
			.Select(t =>
			{
				var raw = t.Span.ToStringValue();
				var unquoted = raw[1..^1];
				return (SqlExpression)new LiteralExpression(unquoted);
			}));

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> NumberLiteral =
		Token.EqualTo(CosmosSqlToken.NumberLiteral)
			.Select(t =>
			{
				var raw = t.Span.ToStringValue();
				if (long.TryParse(raw, out var longVal))
				{
					return (SqlExpression)new LiteralExpression(longVal);
				}

				return (SqlExpression)new LiteralExpression(double.Parse(raw, System.Globalization.CultureInfo.InvariantCulture));
			});

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> TrueLiteral =
		Token.EqualTo(CosmosSqlToken.True).Select(_ => (SqlExpression)new LiteralExpression(true));

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> FalseLiteral =
		Token.EqualTo(CosmosSqlToken.False).Select(_ => (SqlExpression)new LiteralExpression(false));

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> NullLiteral =
		Token.EqualTo(CosmosSqlToken.Null).Select(_ => (SqlExpression)new LiteralExpression(null));

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> UndefinedLiteral =
		Token.EqualTo(CosmosSqlToken.Undefined).Select(_ => (SqlExpression)new UndefinedLiteralExpression());

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> ParameterExpr =
		Token.EqualTo(CosmosSqlToken.Parameter)
			.Select(t => (SqlExpression)new ParameterExpression(t.Span.ToStringValue()));

	// Function call: FUNC_NAME(args...)
	// Supports * as a function argument (e.g. COUNT(*))
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> FunctionCall =
		from name in AnyIdentifierOrKeyword
		from open in Token.EqualTo(CosmosSqlToken.OpenParen)
		from args in Token.EqualTo(CosmosSqlToken.Star).Select(_ => (SqlExpression)new IdentifierExpression("*"))
			.Or(Superpower.Parse.Ref(() => Expr))
			.ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
		from close in Token.EqualTo(CosmosSqlToken.CloseParen)
		select (SqlExpression)new FunctionCallExpression(name.ToUpperInvariant(), args);

	// Dotted function call: namespace.FUNC_NAME(args...) — supports udf.xxx()
	// UDF names preserve original casing (Cosmos DB UDFs are case-sensitive);
	// built-in dotted functions (e.g. ST_DISTANCE) are uppercased.
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> DottedFunctionCall =
		from first in AnyIdentifierOrKeyword
		from dot in Token.EqualTo(CosmosSqlToken.Dot)
		from second in AnyIdentifierOrKeyword
		from open in Token.EqualTo(CosmosSqlToken.OpenParen)
		from args in Superpower.Parse.Ref(() => Expr).ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
		from close in Token.EqualTo(CosmosSqlToken.CloseParen)
		select (SqlExpression)new FunctionCallExpression(
			first.Equals("udf", StringComparison.OrdinalIgnoreCase)
				? "UDF." + second   // preserve UDF name casing
				: (first + "." + second).ToUpperInvariant(), args);

	// EXISTS( subquery-text )
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> ExistsExpr =
		from kw in Token.EqualTo(CosmosSqlToken.Exists)
		from open in Token.EqualTo(CosmosSqlToken.OpenParen)
		from inner in CaptureBalancedParens()
		from close in Token.EqualTo(CosmosSqlToken.CloseParen)
		select (SqlExpression)new ExistsExpression(inner);

	// Subquery expression: (SELECT ...) — a full SELECT statement inside parentheses
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> SubqueryParens =
		from open in Token.EqualTo(CosmosSqlToken.OpenParen)
		from subquery in Superpower.Parse.Ref(() => QueryParser)
		from close in Token.EqualTo(CosmosSqlToken.CloseParen)
		select (SqlExpression)new SubqueryExpression(subquery);

	// Parenthesized expression (try subquery first, fall back to regular expression)
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> Parens =
		SubqueryParens.Try()
		.Or(
			from open in Token.EqualTo(CosmosSqlToken.OpenParen)
			from expr in Superpower.Parse.Ref(() => Expr)
			from close in Token.EqualTo(CosmosSqlToken.CloseParen)
			select expr);

	// Object literal: { key: expr, key: expr, ... }
	// Keys can be identifiers or double-quoted strings (for SDK-generated queries like {"item": root.value})
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> ObjectLiteral =
		from open in Token.EqualTo(CosmosSqlToken.OpenBrace)
		from props in (
			from key in AnyIdentifierOrKeyword
				.Or(Token.EqualTo(CosmosSqlToken.DoubleQuotedString).Select(t => t.Span.ToStringValue()[1..^1]))
				.Or(Token.EqualTo(CosmosSqlToken.StringLiteral).Select(t => t.Span.ToStringValue()[1..^1]))
			from colon in Token.EqualTo(CosmosSqlToken.Colon)
			from value in Superpower.Parse.Ref(() => Expr)
			select new KeyValuePair<string, SqlExpression>(key, value)
		).ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
		from close in Token.EqualTo(CosmosSqlToken.CloseBrace)
		select (SqlExpression)new ObjectLiteralExpression(props);

	// Array literal: [ expr, expr, ... ]
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> ArrayLiteral =
		from open in Token.EqualTo(CosmosSqlToken.OpenBracket)
		from elements in Superpower.Parse.Ref(() => Expr).ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
		from close in Token.EqualTo(CosmosSqlToken.CloseBracket)
		select (SqlExpression)new ArrayLiteralExpression(elements);

	// Identifier or dotted path
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> IdentExpr =
		DottedPathWithIndex.Select(path => (SqlExpression)new IdentifierExpression(path));

	// ARRAY(subquery): ARRAY keyword followed by a parenthesised SELECT
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> ArraySubqueryCall =
		from name in Token.EqualTo(CosmosSqlToken.Array)
		from open in Token.EqualTo(CosmosSqlToken.OpenParen)
		from subquery in Superpower.Parse.Ref(() => QueryParser)
		from close in Token.EqualTo(CosmosSqlToken.CloseParen)
		select (SqlExpression)new FunctionCallExpression("ARRAY", [new SubqueryExpression(subquery)]);

	// Primary: literal | parameter | function call | exists | parens | object | array | ident
	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> Primary =
		StringLiteral
			.Or(NumberLiteral)
			.Or(TrueLiteral)
			.Or(FalseLiteral)
			.Or(NullLiteral)
			.Or(UndefinedLiteral)
			.Or(ParameterExpr)
			.Or(ExistsExpr.Try())
			.Or(ArraySubqueryCall.Try())
			.Or(DottedFunctionCall.Try())
			.Or(FunctionCall.Try())
			.Or(Parens)
			.Or(ObjectLiteral.Try())
			.Or(ArrayLiteral.Try())
			.Or(IdentExpr);

	// ── Unary ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> UnaryExpr =
		(from op in Token.EqualTo(CosmosSqlToken.Not)
		 from operand in Superpower.Parse.Ref(() => UnaryExpr)
		 select (SqlExpression)new UnaryExpression(UnaryOp.Not, operand))
		.Or(
			from op in Token.EqualTo(CosmosSqlToken.Minus)
			from operand in Primary
			select (SqlExpression)new UnaryExpression(UnaryOp.Negate, operand))
		.Or(
			from op in Token.EqualTo(CosmosSqlToken.Tilde)
			from operand in Primary
			select (SqlExpression)new UnaryExpression(UnaryOp.BitwiseNot, operand))
		.Or(Primary);

	// ── Multiplicative: *, /, % ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> Multiplicative =
		Superpower.Parse.Chain(
			Token.EqualTo(CosmosSqlToken.Star).Select(_ => BinaryOp.Multiply)
				.Or(Token.EqualTo(CosmosSqlToken.Slash).Select(_ => BinaryOp.Divide))
				.Or(Token.EqualTo(CosmosSqlToken.Percent).Select(_ => BinaryOp.Modulo)),
			UnaryExpr,
			(op, left, right) => new BinaryExpression(left, op, right));

	// ── Additive: +, - ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> Additive =
		Superpower.Parse.Chain(
			Token.EqualTo(CosmosSqlToken.Plus).Select(_ => BinaryOp.Add)
				.Or(Token.EqualTo(CosmosSqlToken.Minus).Select(_ => BinaryOp.Subtract)),
			Multiplicative,
			(op, left, right) => new BinaryExpression(left, op, right));

	// ── Bitwise AND: & ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> BitwiseAndExpr =
		Superpower.Parse.Chain(
			Token.EqualTo(CosmosSqlToken.Ampersand).Select(_ => BinaryOp.BitwiseAnd),
			Additive,
			(op, left, right) => new BinaryExpression(left, op, right));

	// ── Bitwise XOR: ^ ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> BitwiseXorExpr =
		Superpower.Parse.Chain(
			Token.EqualTo(CosmosSqlToken.Caret).Select(_ => BinaryOp.BitwiseXor),
			BitwiseAndExpr,
			(op, left, right) => new BinaryExpression(left, op, right));

	// ── Bitwise OR: | ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> BitwiseOrExpr =
		Superpower.Parse.Chain(
			Token.EqualTo(CosmosSqlToken.Pipe).Select(_ => BinaryOp.BitwiseOr),
			BitwiseXorExpr,
			(op, left, right) => new BinaryExpression(left, op, right));

	// ── String concat: || ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> StringConcatExpr =
		Superpower.Parse.Chain(
			Token.EqualTo(CosmosSqlToken.DoublePipe).Select(_ => BinaryOp.StringConcat),
			BitwiseOrExpr,
			(op, left, right) => new BinaryExpression(left, op, right));

	// ── Comparison: =, !=, <, >, <=, >=, LIKE ──

	private static readonly TokenListParser<CosmosSqlToken, BinaryOp> CompOp =
		Token.EqualTo(CosmosSqlToken.Equals).Select(_ => BinaryOp.Equal)
			.Or(Token.EqualTo(CosmosSqlToken.NotEquals).Select(_ => BinaryOp.NotEqual))
			.Or(Token.EqualTo(CosmosSqlToken.LessThanOrEqual).Select(_ => BinaryOp.LessThanOrEqual))
			.Or(Token.EqualTo(CosmosSqlToken.GreaterThanOrEqual).Select(_ => BinaryOp.GreaterThanOrEqual))
			.Or(Token.EqualTo(CosmosSqlToken.LessThan).Select(_ => BinaryOp.LessThan))
			.Or(Token.EqualTo(CosmosSqlToken.GreaterThan).Select(_ => BinaryOp.GreaterThan))
			.Or(Token.EqualTo(CosmosSqlToken.Like).Select(_ => BinaryOp.Like));

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> Comparison =
		from left in StringConcatExpr
		from rest in (
			// BETWEEN low AND high
			(from kw in Token.EqualTo(CosmosSqlToken.Between)
			 from low in StringConcatExpr
			 from and in Token.EqualTo(CosmosSqlToken.And)
			 from high in StringConcatExpr
			 select (Func<SqlExpression, SqlExpression>)(l => new BetweenExpression(l, low, high)))
			// NOT BETWEEN low AND high
			.Or(
				(from not in Token.EqualTo(CosmosSqlToken.Not)
				 from kw in Token.EqualTo(CosmosSqlToken.Between)
				 from low in StringConcatExpr
				 from and in Token.EqualTo(CosmosSqlToken.And)
				 from high in StringConcatExpr
				 select (Func<SqlExpression, SqlExpression>)(l => new UnaryExpression(UnaryOp.Not, new BetweenExpression(l, low, high))))
				.Try())
			// IN (val, val, ...)
			.Or(
				from kw in Token.EqualTo(CosmosSqlToken.In)
				from open in Token.EqualTo(CosmosSqlToken.OpenParen)
				from vals in Superpower.Parse.Ref(() => Expr).ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
				from close in Token.EqualTo(CosmosSqlToken.CloseParen)
				select (Func<SqlExpression, SqlExpression>)(l => new InExpression(l, vals)))
			// NOT IN (val, val, ...)
			.Or(
				(from not in Token.EqualTo(CosmosSqlToken.Not)
				 from kw in Token.EqualTo(CosmosSqlToken.In)
				 from open in Token.EqualTo(CosmosSqlToken.OpenParen)
				 from vals in Superpower.Parse.Ref(() => Expr).ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
				 from close in Token.EqualTo(CosmosSqlToken.CloseParen)
				 select (Func<SqlExpression, SqlExpression>)(l => new UnaryExpression(UnaryOp.Not, new InExpression(l, vals))))
				.Try())
			// LIKE pattern ESCAPE char
			.Or(
				(from kw in Token.EqualTo(CosmosSqlToken.Like)
				 from pattern in StringConcatExpr
				 from esc in Token.EqualTo(CosmosSqlToken.Escape)
				 from escChar in Token.EqualTo(CosmosSqlToken.StringLiteral)
				 select (Func<SqlExpression, SqlExpression>)(l => new LikeExpression(l, pattern, escChar.Span.ToStringValue()[1..^1])))
				.Try())
			// NOT LIKE pattern ESCAPE char
			.Or(
				(from not in Token.EqualTo(CosmosSqlToken.Not)
				 from kw in Token.EqualTo(CosmosSqlToken.Like)
				 from pattern in StringConcatExpr
				 from esc in Token.EqualTo(CosmosSqlToken.Escape)
				 from escChar in Token.EqualTo(CosmosSqlToken.StringLiteral)
				 select (Func<SqlExpression, SqlExpression>)(l => new UnaryExpression(UnaryOp.Not, new LikeExpression(l, pattern, escChar.Span.ToStringValue()[1..^1]))))
				.Try())
			// NOT LIKE pattern (without ESCAPE)
			.Or(
				(from not in Token.EqualTo(CosmosSqlToken.Not)
				 from kw in Token.EqualTo(CosmosSqlToken.Like)
				 from pattern in StringConcatExpr
				 select (Func<SqlExpression, SqlExpression>)(l => new UnaryExpression(UnaryOp.Not, new BinaryExpression(l, BinaryOp.Like, pattern))))
				.Try())
			// IS NOT NULL (must come before IS NULL to avoid consuming IS and failing on NOT)
			.Or(
				(from is_ in Token.EqualTo(CosmosSqlToken.Is)
				 from not_ in Token.EqualTo(CosmosSqlToken.Not)
				 from null_ in Token.EqualTo(CosmosSqlToken.Null)
				 select (Func<SqlExpression, SqlExpression>)(l => new BinaryExpression(l, BinaryOp.NotEqual, new LiteralExpression(null))))
				.Try())
			// IS NULL
			.Or(
				from is_ in Token.EqualTo(CosmosSqlToken.Is)
				from null_ in Token.EqualTo(CosmosSqlToken.Null)
				select (Func<SqlExpression, SqlExpression>)(l => new BinaryExpression(l, BinaryOp.Equal, new LiteralExpression(null))))
			// op right
			.Or(
				from op in CompOp
				from right in StringConcatExpr
				select (Func<SqlExpression, SqlExpression>)(l => new BinaryExpression(l, op, right)))
		).OptionalOrDefault(null)
		select rest != null ? rest(left) : left;

	// ── Logical AND ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> AndExpr =
		Superpower.Parse.Chain(
			Token.EqualTo(CosmosSqlToken.And).Select(_ => BinaryOp.And),
			Comparison,
			(op, left, right) => new BinaryExpression(left, op, right));

	// ── Logical OR ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> OrExpr =
		Superpower.Parse.Chain(
			Token.EqualTo(CosmosSqlToken.Or).Select(_ => BinaryOp.Or),
			AndExpr,
			(op, left, right) => new BinaryExpression(left, op, right));

	// ── Null coalesce: ?? ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> CoalesceExpr =
		from left in OrExpr
		from rest in (
			from op in Token.EqualTo(CosmosSqlToken.QuestionQuestion)
			from right in Superpower.Parse.Ref(() => CoalesceExpr)
			select right
		).OptionalOrDefault(null)
		select rest != null ? new CoalesceExpression(left, rest) : left;

	// ── Ternary: cond ? then : else ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> TernaryExpr =
		from cond in CoalesceExpr
		from rest in (
			from q in Token.EqualTo(CosmosSqlToken.Question)
			from ifTrue in Superpower.Parse.Ref(() => Expr)
			from colon in Token.EqualTo(CosmosSqlToken.Colon)
			from ifFalse in Superpower.Parse.Ref(() => Expr)
			select new { ifTrue, ifFalse }
		).OptionalOrDefault(null)
		select rest != null ? (SqlExpression)new TernaryExpression(cond, rest.ifTrue, rest.ifFalse) : cond;

	// ── Top-level expression ──

	private static readonly TokenListParser<CosmosSqlToken, SqlExpression> Expr = TernaryExpr;

	// ──────────────────────────────────────────────
	//  SELECT field parsing
	// ──────────────────────────────────────────────

	private static readonly TokenListParser<CosmosSqlToken, SelectField> StarField =
		Token.EqualTo(CosmosSqlToken.Star).Select(_ => new SelectField("*", null));

	// Handle "c.*" (alias DOT STAR) as equivalent to "*"
	private static readonly TokenListParser<CosmosSqlToken, SelectField> AliasDotStarField =
		from ident in AnyIdentifierOrKeyword
		from dot in Token.EqualTo(CosmosSqlToken.Dot)
		from star in Token.EqualTo(CosmosSqlToken.Star)
		select new SelectField("*", null);

	private static readonly TokenListParser<CosmosSqlToken, SelectField> ExpressionField =
		from expr in Expr
		from alias in (
			from as_ in Token.EqualTo(CosmosSqlToken.As)
			from name in AnyIdentifierOrKeyword
			select name
		).OptionalOrDefault(null)
		select new SelectField(ExprToString(expr), alias, expr);

	private static readonly TokenListParser<CosmosSqlToken, SelectField> SingleSelectField =
		StarField.Try().Or(AliasDotStarField.Try()).Or(ExpressionField);

	// ──────────────────────────────────────────────
	//  JOIN clause parsing
	// ──────────────────────────────────────────────

	private static readonly TokenListParser<CosmosSqlToken, JoinClause> JoinParser =
		from kw in Token.EqualTo(CosmosSqlToken.Join)
		from alias in AnyIdentifierOrKeyword
		from in_ in Token.EqualTo(CosmosSqlToken.In)
		from source in DottedPathWithIndex
		select ParseJoinSource(alias, source);

	// ──────────────────────────────────────────────
	//  ORDER BY parsing
	// ──────────────────────────────────────────────

	private static readonly TokenListParser<CosmosSqlToken, OrderByField> OrderByFieldParser =
		(from expr in Superpower.Parse.Ref(() => Expr)
		 from dir in Token.EqualTo(CosmosSqlToken.Asc).Select(_ => true)
			 .Or(Token.EqualTo(CosmosSqlToken.Desc).Select(_ => false))
			 .OptionalOrDefault(true)
		 select expr is IdentifierExpression ident
			 ? new OrderByField(ident.Name, dir)
			 : new OrderByField(null, dir, expr)).Try()
		.Or(
		from field in DottedPathWithIndex
		from dir in Token.EqualTo(CosmosSqlToken.Asc).Select(_ => true)
			.Or(Token.EqualTo(CosmosSqlToken.Desc).Select(_ => false))
			.OptionalOrDefault(true)
		select new OrderByField(field, dir));

	// ──────────────────────────────────────────────
	//  Full query parsing
	// ──────────────────────────────────────────────

	private static readonly TokenListParser<CosmosSqlToken, CosmosSqlQuery> QueryParser =
		from select_ in Token.EqualTo(CosmosSqlToken.Select)
		from distinct in Token.EqualTo(CosmosSqlToken.Distinct).OptionalOrDefault()
		from top in (
			from topKw in Token.EqualTo(CosmosSqlToken.Top)
			from n in Token.EqualTo(CosmosSqlToken.NumberLiteral).Select(t => int.Parse(t.Span.ToStringValue()))
			select (int?)n
		).OptionalOrDefault(null)
		from value_ in Token.EqualTo(CosmosSqlToken.Value).OptionalOrDefault()
		from fields in SingleSelectField.ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
		from fromKw in Token.EqualTo(CosmosSqlToken.From)
		from fromFirstIdent in AnyIdentifierOrKeyword
		from fromClause in (
			// FROM alias IN source.path
			from in_ in Token.EqualTo(CosmosSqlToken.In)
			from source in DottedPath
			select (Alias: fromFirstIdent, Source: (string)source)
		).Try().Or(
			// FROM source AS alias
			from as_ in Token.EqualTo(CosmosSqlToken.As)
			from alias in AnyIdentifierOrKeyword
			select (Alias: alias, Source: (string)null)
		).Try().Or(
			// FROM source alias  (implicit alias without AS keyword — only plain identifiers)
			from alias in Token.EqualTo(CosmosSqlToken.Identifier).Select(TokenSpanToString)
			select (Alias: alias, Source: (string)null)
		).Try().OptionalOrDefault((Alias: fromFirstIdent, Source: (string)null))
		from joins in JoinParser.Many()
		from where_ in (
			from whereKw in Token.EqualTo(CosmosSqlToken.Where)
			from expr in Expr
			select expr
		).OptionalOrDefault(null)
		from groupBy in (
			from groupKw in Token.EqualTo(CosmosSqlToken.Group)
			from byKw in Token.EqualTo(CosmosSqlToken.By)
			from groupFields in Expr.ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
			select groupFields
		).OptionalOrDefault(null)
		from having in (
			from havingKw in Token.EqualTo(CosmosSqlToken.Having)
			from expr in Expr
			select expr
		).OptionalOrDefault(null)
		from orderByResult in (
			from orderKw in Token.EqualTo(CosmosSqlToken.Order)
			from byKw in Token.EqualTo(CosmosSqlToken.By)
			from result in (
				from rankKw in Token.EqualTo(CosmosSqlToken.Identifier)
					.Where(t => string.Equals(t.Span.ToStringValue(), "RANK", StringComparison.OrdinalIgnoreCase))
				from expr in Expr
				select (Fields: (OrderByField[])null, RankExpr: (SqlExpression)expr)
			).Try().Or(
				from orderFields in OrderByFieldParser.ManyDelimitedBy(Token.EqualTo(CosmosSqlToken.Comma))
				select (Fields: orderFields, RankExpr: (SqlExpression)null)
			)
			select result
		).OptionalOrDefault(default)
		from offset in (
			from offsetKw in Token.EqualTo(CosmosSqlToken.Offset)
			from n in Token.EqualTo(CosmosSqlToken.NumberLiteral).Select(t => int.Parse(t.Span.ToStringValue()))
			select (int?)n
		).OptionalOrDefault(null)
		from limit in (
			from limitKw in Token.EqualTo(CosmosSqlToken.Limit)
			from n in Token.EqualTo(CosmosSqlToken.NumberLiteral).Select(t => int.Parse(t.Span.ToStringValue()))
			select (int?)n
		).OptionalOrDefault(null)
		select BuildQuery(
			distinct.HasValue, top, value_.HasValue, fields,
			fromClause.Alias, fromClause.Source, joins, where_,
			groupBy?.Select(ExprToString).ToArray(),
			groupBy?.ToArray(),
			having, orderByResult.Fields, offset, limit, orderByResult.RankExpr);

	// ──────────────────────────────────────────────
	//  Public API
	// ──────────────────────────────────────────────

	private const int ParseCacheMaxSize = 5000;
	private static readonly ConcurrentDictionary<string, CosmosSqlQuery> ParseCache = new();

	public static CosmosSqlQuery Parse(string sql)
	{
		if (ParseCache.TryGetValue(sql, out var cached))
		{
			return cached;
		}

		CosmosSqlQuery parsed;
		try
		{
			var tokens = CosmosSqlTokenizer.Tokenize(sql);
			parsed = QueryParser.Parse(tokens);
		}
		catch (Exception ex)
		{
			throw new NotSupportedException($"Failed to parse Cosmos SQL query: {sql}", ex);
		}

		if (ParseCache.Count < ParseCacheMaxSize)
		{
			ParseCache.TryAdd(sql, parsed);
		}

		return parsed;
	}

	public static bool TryParse(string sql, out CosmosSqlQuery result)
	{
		try
		{
			result = Parse(sql);
			return true;
		}
		catch
		{
			result = null;
			return false;
		}
	}

	// Backward-compatible: convert SqlExpression tree to WhereExpression tree
	public static WhereExpression ToWhereExpression(SqlExpression expr)
	{
		if (expr is null)
		{
			return null;
		}

		switch (expr)
		{
			case BinaryExpression bin when bin.Operator == BinaryOp.And:
				return new AndCondition(ToWhereExpression(bin.Left), ToWhereExpression(bin.Right));

			case BinaryExpression bin when bin.Operator == BinaryOp.Or:
				return new OrCondition(ToWhereExpression(bin.Left), ToWhereExpression(bin.Right));

			case UnaryExpression { Operator: UnaryOp.Not } unary:
				return new NotCondition(ToWhereExpression(unary.Operand));

			case ExistsExpression exists:
				return new ExistsCondition(exists.RawSubquery);

			case FunctionCallExpression func:
				if (func.FunctionName.StartsWith("UDF.", StringComparison.OrdinalIgnoreCase) ||
					func.FunctionName.StartsWith("ST_", StringComparison.OrdinalIgnoreCase))
				{
					return new SqlExpressionCondition(func);
				}

				var hasComplexArgs = func.Arguments.Any(a => a is ObjectLiteralExpression or ArrayLiteralExpression or FunctionCallExpression);
				if (hasComplexArgs)
				{
					return new SqlExpressionCondition(func);
				}

				if (LegacyFunctionNames.Contains(func.FunctionName))
				{
					var args = func.Arguments.Select(ExprToString).ToArray();
					return new FunctionCondition(func.FunctionName, args);
				}

				return new SqlExpressionCondition(func);

			case BinaryExpression bin when IsComparisonOp(bin.Operator):
				if (ContainsFunctionCall(bin.Left) || ContainsFunctionCall(bin.Right) ||
					ContainsArithmetic(bin.Left) || ContainsArithmetic(bin.Right) ||
					ContainsComplexExpression(bin.Left) || ContainsComplexExpression(bin.Right))
				{
					return new SqlExpressionCondition(bin);
				}

				var compOp = bin.Operator switch
				{
					BinaryOp.Equal => ComparisonOp.Equal,
					BinaryOp.NotEqual => ComparisonOp.NotEqual,
					BinaryOp.LessThan => ComparisonOp.LessThan,
					BinaryOp.GreaterThan => ComparisonOp.GreaterThan,
					BinaryOp.LessThanOrEqual => ComparisonOp.LessThanOrEqual,
					BinaryOp.GreaterThanOrEqual => ComparisonOp.GreaterThanOrEqual,
					BinaryOp.Like => ComparisonOp.Like,
					_ => ComparisonOp.Equal,
				};
				return new ComparisonCondition(ExprToString(bin.Left), compOp, ExprToString(bin.Right));

			default:
				return new SqlExpressionCondition(expr);
		}
	}

	/// <summary>
	/// Removes SDK-injected <c>IS_DEFINED(alias)</c> and literal <c>true</c> nodes from
	/// AND chains in a WHERE expression AST, returning the simplified user condition.
	/// Only strips <c>IS_DEFINED()</c> calls whose argument is exactly the FROM alias
	/// (the SDK injects <c>IS_DEFINED(root)</c> for ORDER BY, never <c>IS_DEFINED(root.field)</c>),
	/// preserving any legitimate <c>IS_DEFINED()</c> from user code.
	/// Returns <c>null</c> when the entire expression reduces to nothing.
	/// </summary>
	public static SqlExpression SimplifySdkWhereExpression(
		SqlExpression expr, string fromAlias = null)
	{
		return SimplifyCore(expr, fromAlias);
	}

	private static SqlExpression SimplifyCore(SqlExpression expr, string fromAlias)
	{
		if (expr is null)
		{
			return null;
		}

		if (expr is LiteralExpression { Value: true })
		{
			return null;
		}

		if (expr is FunctionCallExpression func &&
			string.Equals(func.FunctionName, "IS_DEFINED", StringComparison.OrdinalIgnoreCase) &&
			func.Arguments.Length == 1)
		{
			var argStr = ExprToString(func.Arguments[0]);
			// Only strip when: no alias specified (backward-compat), or arg IS the FROM alias exactly.
			// The SDK injects IS_DEFINED(root) — never IS_DEFINED(root.field) — so this
			// preserves any user-written IS_DEFINED on specific field paths.
			if (fromAlias is null || string.Equals(argStr, fromAlias, StringComparison.OrdinalIgnoreCase))
			{
				return null;
			}
		}

		if (expr is BinaryExpression { Operator: BinaryOp.And } and)
		{
			var left = SimplifyCore(and.Left, fromAlias);
			var right = SimplifyCore(and.Right, fromAlias);
			if (left is null && right is null)
			{
				return null;
			}

			if (left is null)
			{
				return right;
			}

			if (right is null)
			{
				return left;
			}

			return new BinaryExpression(left, BinaryOp.And, right);
		}

		return expr;
	}

	/// <summary>
	/// Rebuilds a Superpower-parsed <see cref="CosmosSqlQuery"/> into clean SQL that
	/// <see cref="InMemoryContainer"/> can execute. Strips SDK-injected WHERE clauses
	/// (<c>IS_DEFINED(alias)</c>, literal <c>true</c>), normalises bracket notation
	/// (<c>root["name"]</c>) to dot notation (<c>root.name</c>) via the AST, and
	/// reconstructs SELECT, WHERE, ORDER BY, TOP, OFFSET/LIMIT, DISTINCT, and GROUP BY
	/// clauses from the parsed structure.
	/// <para>
	/// For ORDER BY queries where the SDK rewrites the SELECT to include <c>orderByItems</c>
	/// and <c>payload</c>, this emits <c>SELECT VALUE alias</c> to return full documents.
	/// For all other queries, the original SELECT expressions are emitted from the AST.
	/// </para>
	/// </summary>
	public static string SimplifySdkQuery(CosmosSqlQuery parsed)
	{
		var fromAlias = parsed.FromAlias;

		var isOrderByQuery = parsed.SelectFields.Any(field =>
			string.Equals(field.Alias, "orderByItems", StringComparison.OrdinalIgnoreCase));

		// SELECT clause
		var sb = new System.Text.StringBuilder("SELECT ");
		if (parsed.IsDistinct)
		{
			sb.Append("DISTINCT ");
		}

		if (parsed.TopCount.HasValue)
		{
			sb.Append($"TOP {parsed.TopCount.Value} ");
		}

		if (isOrderByQuery)
		{
			sb.Append($"VALUE {fromAlias}");
		}
		else if (parsed.IsValueSelect)
		{
			var selectExprs = parsed.SelectFields
				.Select(field => ExprToString(field.SqlExpr ?? new IdentifierExpression(field.Expression)))
				.ToArray();
			sb.Append("VALUE ");
			sb.Append(string.Join(", ", selectExprs));
		}
		else if (parsed.IsSelectAll)
		{
			sb.Append('*');
		}
		else
		{
			var selectParts = parsed.SelectFields.Select(field =>
			{
				var expr = field.SqlExpr is not null ? ExprToString(field.SqlExpr) : field.Expression;
				return field.Alias is not null ? $"{expr} AS {field.Alias}" : expr;
			});
			sb.Append(string.Join(", ", selectParts));
		}

		// FROM
		if (parsed.FromSource is not null)
		{
			sb.Append($" FROM {fromAlias} IN {parsed.FromSource}");
		}
		else
		{
			sb.Append($" FROM {fromAlias}");
		}

		// JOINs
		if (parsed.Joins is { Length: > 0 })
		{
			foreach (var join in parsed.Joins)
			{
				sb.Append($" JOIN {join.Alias} IN {join.SourceAlias}.{join.ArrayField}");
			}
		}

		// WHERE — strip SDK-injected nodes
		var simplifiedWhere = SimplifySdkWhereExpression(parsed.WhereExpr, fromAlias);
		if (simplifiedWhere is not null)
		{
			sb.Append($" WHERE {ExprToString(simplifiedWhere)}");
		}

		// GROUP BY
		if (parsed.GroupByFields is { Length: > 0 })
		{
			sb.Append($" GROUP BY {string.Join(", ", parsed.GroupByFields)}");
		}

		// HAVING
		if (parsed.HavingExpr is not null)
		{
			sb.Append($" HAVING {ExprToString(parsed.HavingExpr)}");
		}

		// ORDER BY
		if (parsed.OrderByFields is { Length: > 0 })
		{
			var orderByStr = string.Join(", ", parsed.OrderByFields.Select(field =>
				$"{field.Field ?? ExprToString(field.Expression)} {(field.Ascending ? "ASC" : "DESC")}"));
			sb.Append($" ORDER BY {orderByStr}");
		}
		else if (parsed.RankExpression is not null)
		{
			sb.Append($" ORDER BY RANK {ExprToString(parsed.RankExpression)}");
		}

		// OFFSET / LIMIT
		if (parsed.Offset.HasValue)
		{
			sb.Append($" OFFSET {parsed.Offset.Value}");
		}

		if (parsed.Limit.HasValue)
		{
			sb.Append($" LIMIT {parsed.Limit.Value}");
		}

		return sb.ToString();
	}

	// ──────────────────────────────────────────────
	//  Internal helpers
	// ──────────────────────────────────────────────

	internal static WhereExpression ParseWhereExpression(string expression)
	{
		var wrappedSql = $"SELECT * FROM c WHERE {expression}";
		var query = Parse(wrappedSql);
		return query.Where;
	}

	private static bool IsComparisonOp(BinaryOp op) =>
		op is BinaryOp.Equal or BinaryOp.NotEqual or BinaryOp.LessThan or BinaryOp.GreaterThan
			or BinaryOp.LessThanOrEqual or BinaryOp.GreaterThanOrEqual or BinaryOp.Like;

	private static bool ContainsFunctionCall(SqlExpression expr) =>
		expr switch
		{
			FunctionCallExpression => true,
			BinaryExpression bin => ContainsFunctionCall(bin.Left) || ContainsFunctionCall(bin.Right),
			UnaryExpression unary => ContainsFunctionCall(unary.Operand),
			_ => false
		};

	private static bool ContainsArithmetic(SqlExpression expr) =>
		expr switch
		{
			BinaryExpression bin when !IsComparisonOp(bin.Operator) => true,
			BinaryExpression bin => ContainsArithmetic(bin.Left) || ContainsArithmetic(bin.Right),
			UnaryExpression unary => ContainsArithmetic(unary.Operand),
			_ => false
		};

	/// <summary>
	/// Returns true when the expression contains a subquery, coalesce, or ternary —
	/// any of which require full expression evaluation rather than string-based ResolveValue.
	/// </summary>
	private static bool ContainsComplexExpression(SqlExpression expr) =>
		expr switch
		{
			SubqueryExpression => true,
			CoalesceExpression => true,
			TernaryExpression => true,
			BinaryExpression bin => ContainsComplexExpression(bin.Left) || ContainsComplexExpression(bin.Right),
			UnaryExpression unary => ContainsComplexExpression(unary.Operand),
			_ => false
		};

	public static string ExprToString(SqlExpression expr)
	{
		return expr switch
		{
			LiteralExpression { Value: null } => "null",
			LiteralExpression { Value: string s } => $"'{s}'",
			LiteralExpression { Value: bool b } => b ? "true" : "false",
			LiteralExpression lit => lit.Value?.ToString() ?? "null",
			UndefinedLiteralExpression => "undefined",
			IdentifierExpression ident => ident.Name,
			ParameterExpression param => param.Name,
			PropertyAccessExpression prop => $"{ExprToString(prop.Object)}.{prop.Property}",
			IndexAccessExpression idx => $"{ExprToString(idx.Object)}[{ExprToString(idx.Index)}]",
			FunctionCallExpression func => $"{func.FunctionName}({string.Join(", ", func.Arguments.Select(ExprToString))})",
			BinaryExpression { Operator: BinaryOp.And or BinaryOp.Or } bin =>
				$"({WrapIfLowPrecedence(bin.Left)} {BinaryOpToString(bin.Operator)} {WrapIfLowPrecedence(bin.Right)})",
			BinaryExpression bin => $"{WrapIfLowPrecedence(bin.Left)} {BinaryOpToString(bin.Operator)} {WrapIfLowPrecedence(bin.Right)}",
			UnaryExpression unary => $"{UnaryOpToString(unary.Operator)} {WrapIfLowPrecedence(unary.Operand)}",
			BetweenExpression betw => $"{WrapIfLowPrecedence(betw.Value)} BETWEEN {ExprToString(betw.Low)} AND {ExprToString(betw.High)}",
			InExpression inExpr => $"{WrapIfLowPrecedence(inExpr.Value)} IN ({string.Join(", ", inExpr.List.Select(ExprToString))})",
			LikeExpression like => like.EscapeChar is not null
				? $"{WrapIfLowPrecedence(like.Value)} LIKE {ExprToString(like.Pattern)} ESCAPE '{like.EscapeChar}'"
				: $"{WrapIfLowPrecedence(like.Value)} LIKE {ExprToString(like.Pattern)}",
			ExistsExpression exists => $"EXISTS({exists.RawSubquery})",
			SubqueryExpression sub => $"({SubqueryToString(sub.Subquery)})",
			TernaryExpression tern => $"{ExprToString(tern.Condition)} ? {ExprToString(tern.IfTrue)} : {ExprToString(tern.IfFalse)}",
			CoalesceExpression coal => $"{ExprToString(coal.Left)} ?? {ExprToString(coal.Right)}",
			ObjectLiteralExpression obj => "{" + string.Join(", ", obj.Properties.Select(p => $"{p.Key}: {ExprToString(p.Value)}")) + "}",
			ArrayLiteralExpression arr => "[" + string.Join(", ", arr.Elements.Select(ExprToString)) + "]",
			_ => expr.ToString(),
		};
	}

	/// <summary>
	/// Wraps the expression in parentheses if it has lower precedence than binary/unary operators
	/// (i.e., ternary or coalesce expressions). This ensures that re-parsing the serialized string
	/// produces the same AST.
	/// </summary>
	// Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/ternary-coalesce-operators
	//   Ternary (?) and Coalesce (??) have the lowest operator precedence in Cosmos DB SQL.
	private static string WrapIfLowPrecedence(SqlExpression expr) =>
		expr is TernaryExpression or CoalesceExpression
			? $"({ExprToString(expr)})"
			: ExprToString(expr);

	private static string SubqueryToString(CosmosSqlQuery sub)
	{
		var sb = new System.Text.StringBuilder("SELECT ");
		if (sub.IsDistinct)
		{
			sb.Append("DISTINCT ");
		}

		if (sub.TopCount.HasValue)
		{
			sb.Append($"TOP {sub.TopCount.Value} ");
		}

		if (sub.IsValueSelect)
		{
			sb.Append("VALUE ");
		}

		if (sub.IsSelectAll)
		{
			sb.Append('*');
		}
		else
		{
			var selectParts = sub.SelectFields.Select(field =>
			{
				var expr = field.SqlExpr is not null ? ExprToString(field.SqlExpr) : field.Expression;
				return field.Alias is not null ? $"{expr} AS {field.Alias}" : expr;
			});
			sb.Append(string.Join(", ", selectParts));
		}

		if (sub.FromSource is not null)
		{
			sb.Append($" FROM {sub.FromAlias} IN {sub.FromSource}");
		}
		else
		{
			sb.Append($" FROM {sub.FromAlias}");
		}

		if (sub.Joins is { Length: > 0 })
		{
			foreach (var join in sub.Joins)
			{
				sb.Append($" JOIN {join.Alias} IN {join.SourceAlias}.{join.ArrayField}");
			}
		}

		if (sub.WhereExpr is not null)
		{
			sb.Append($" WHERE {ExprToString(sub.WhereExpr)}");
		}

		if (sub.GroupByFields is { Length: > 0 })
		{
			sb.Append($" GROUP BY {string.Join(", ", sub.GroupByFields)}");
		}

		if (sub.HavingExpr is not null)
		{
			sb.Append($" HAVING {ExprToString(sub.HavingExpr)}");
		}

		if (sub.OrderByFields is { Length: > 0 })
		{
			var orderByStr = string.Join(", ", sub.OrderByFields.Select(field =>
				$"{field.Field} {(field.Ascending ? "ASC" : "DESC")}"));
			sb.Append($" ORDER BY {orderByStr}");
		}

		if (sub.Offset.HasValue)
		{
			sb.Append($" OFFSET {sub.Offset.Value}");
		}

		if (sub.Limit.HasValue)
		{
			sb.Append($" LIMIT {sub.Limit.Value}");
		}

		return sb.ToString();
	}

	private static string BinaryOpToString(BinaryOp op) => op switch
	{
		BinaryOp.Equal => "=",
		BinaryOp.NotEqual => "!=",
		BinaryOp.LessThan => "<",
		BinaryOp.GreaterThan => ">",
		BinaryOp.LessThanOrEqual => "<=",
		BinaryOp.GreaterThanOrEqual => ">=",
		BinaryOp.And => "AND",
		BinaryOp.Or => "OR",
		BinaryOp.Add => "+",
		BinaryOp.Subtract => "-",
		BinaryOp.Multiply => "*",
		BinaryOp.Divide => "/",
		BinaryOp.Modulo => "%",
		BinaryOp.Like => "LIKE",
		BinaryOp.StringConcat => "||",
		BinaryOp.BitwiseAnd => "&",
		BinaryOp.BitwiseOr => "|",
		BinaryOp.BitwiseXor => "^",
		_ => op.ToString(),
	};

	private static string UnaryOpToString(UnaryOp op) => op switch
	{
		UnaryOp.Not => "NOT",
		UnaryOp.Negate => "-",
		UnaryOp.BitwiseNot => "~",
		_ => op.ToString(),
	};

	private static JoinClause ParseJoinSource(string alias, string source)
	{
		var dotIdx = source.IndexOf('.');
		if (dotIdx < 0)
		{
			return new JoinClause(alias, source, source);
		}

		return new JoinClause(alias, source[..dotIdx], source[(dotIdx + 1)..]);
	}

	private static CosmosSqlQuery BuildQuery(
		bool isDistinct, int? top, bool isValue,
		SelectField[] fields, string fromAlias, string fromSource,
		JoinClause[] joins, SqlExpression whereExpr,
		string[] groupBy, SqlExpression[] groupByExpressions, SqlExpression havingExpr,
		OrderByField[] orderByFields,
		int? offset, int? limit, SqlExpression rankExpr = null)
	{
		var isSelectAll = fields.Length == 1 && fields[0].Expression == "*";
		var where = whereExpr != null ? ToWhereExpression(whereExpr) : null;
		// HAVING always uses SqlExpressionCondition to preserve aggregate function expressions
		// (ToWhereExpression would decompose AND/OR into AndCondition/OrCondition with string-based
		// comparison operands, losing the ability to evaluate aggregate functions like COUNT/SUM).
		var having = havingExpr != null ? new SqlExpressionCondition(havingExpr) : null;
		var rawHavingExpr = havingExpr;

		// Backward compat: first join, first order by
		var firstJoin = joins.Length > 0 ? joins[0] : null;
		OrderByClause legacyOrderBy = null;
		if (orderByFields is { Length: > 0 })
		{
			legacyOrderBy = new OrderByClause(orderByFields[0].Field, orderByFields[0].Ascending);
		}

		return new CosmosSqlQuery(
			SelectFields: fields,
			IsSelectAll: isSelectAll,
			TopCount: top,
			FromAlias: fromAlias,
			Join: firstJoin,
			Where: where,
			Offset: offset,
			Limit: limit,
			OrderBy: legacyOrderBy,
			IsDistinct: isDistinct,
			IsValueSelect: isValue,
			Joins: joins,
			OrderByFields: orderByFields,
			GroupByFields: groupBy,
			GroupByExpressions: groupByExpressions,
			Having: having,
			WhereExpr: whereExpr,
			HavingExpr: rawHavingExpr,
			FromSource: fromSource,
			RankExpression: rankExpr);
	}

	// Captures tokens inside balanced parentheses as a raw string
	private static TokenListParser<CosmosSqlToken, string> CaptureBalancedParens()
	{
		return input =>
		{
			var depth = 0;
			var startPos = input.Position;
			var current = input;
			var parts = new List<string>();

			while (!current.IsAtEnd)
			{
				var token = current.ConsumeToken();
				if (!token.HasValue)
				{
					break;
				}

				if (token.Value.Kind == CosmosSqlToken.OpenParen)
				{
					depth++;
					parts.Add("(");
					current = token.Remainder;
				}
				else if (token.Value.Kind == CosmosSqlToken.CloseParen)
				{
					if (depth == 0)
					{
						return TokenListParserResult.Value(string.Join(" ", parts), input, current);
					}
					depth--;
					parts.Add(")");
					current = token.Remainder;
				}
				else
				{
					parts.Add(token.Value.Span.ToStringValue());
					current = token.Remainder;
				}
			}

			return TokenListParserResult.Value(string.Join(" ", parts), input, current);
		};
	}
}
