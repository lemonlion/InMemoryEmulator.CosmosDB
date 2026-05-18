# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.0.18] - 2026-05-15

### Fixed
- `COUNT(expr)` with nested ternary expressions no longer miscounts documents (Issue #64). `ExprToString` in `CosmosSqlParser` did not parenthesise ternary/coalesce sub-expressions when they appeared as operands of higher-precedence binary operators. When the SDK's transformed query was round-tripped through `SimplifySdkQuery`, the missing parentheses caused re-parsing to produce a different AST — e.g. `(innerTernary > 0) ? 1 : undefined` became `innerTernary ? val : (otherVal > 0 ? 1 : undefined)` — making `COUNT` evaluate the wrong condition. The fix wraps ternary and coalesce expressions in parentheses whenever they appear inside binary, unary, BETWEEN, IN, or LIKE operators.

## [4.0.17] - 2026-05-14

### Fixed
- `COUNT(expr)` with a non-trivial inner expression no longer throws `Newtonsoft.Json.JsonException` (Issue #59). Previously the inner argument was stringified and handed to `JToken.SelectToken` as a JSONPath, which failed on anything beyond a trivial property path — including double-quoted bracket notation (`c.obj["value"]`, required for fields named with Cosmos SQL reserved words) when combined with comparison or ternary operators (e.g. `COUNT(c.amount["value"] > 0 ? 1 : undefined)`). `COUNT` now evaluates the inner expression per-item like `SUM`/`AVG` and counts rows whose result is defined and non-null, matching Cosmos DB semantics. The `undefined` keyword is also now distinguished from the string literal `'undefined'` so it correctly evaluates to the undefined sentinel.

## [4.0.16] - 2026-05-11

### Added
- `COUNTIF(<bool_expr>)` aggregate function is now supported in SQL queries. This undocumented Cosmos DB server-side aggregate (added in .NET SDK [PR #4738](https://github.com/Azure/azure-cosmos-dotnet-v3/pull/4738)) counts items where the boolean expression evaluates to true. Works in all contexts: standalone queries, GROUP BY, HAVING, mixed aggregates, subqueries, and SELECT VALUE projections.

## [4.0.15] - 2026-05-08

### Fixed
- `EmulatorTestFixture` now caches containers by name in the session and reuses them across tests instead of creating/deleting a uniquely-named container per test method (Issue #43). This prevents partition pool exhaustion on the Linux emulator when running test classes with many tests (e.g. PartitionKeyTests, LinqTests, CrudHardeningTests, BulkTests). Containers are cleaned (all documents deleted) between tests to maintain isolation, and only deleted at the end of the test run.
- Tagged `Linq_OrderBy_ThenBy` test as `InMemoryOnly` — real Cosmos DB requires a composite index for multi-field ORDER BY, which the in-memory emulator intentionally does not enforce (Issue #44 triage).
- Verified JsTriggers `queryDocuments` callback correctly passes `responseOptions` object (not null) — already fixed in current codebase (Issue #22). Added regression tests.
- Verified JsTriggers bulk delete stored procedure pattern (200 documents, recursive pagination) completes without stack overflow — already fixed (Issue #23). Added regression tests.

## [4.0.14] - 2026-05-07

### Fixed
- Container throughput specified via `CreateContainerAsync(props, throughput: N)` is now persisted and returned by `ReadThroughputAsync()` (Issue #26). Previously the throughput parameter was silently ignored and the default 400 RU/s was always returned. Also fixed for `CreateContainerIfNotExistsAsync`, `CreateContainerStreamAsync`, and `ThroughputProperties` overloads.
- `FakeCosmosHandler` now reads the `x-ms-offer-throughput` header during container creation and persists it. Also handles the `/offers` query endpoint so `ReadThroughputAsync` works through the SDK HTTP pipeline.
- `GetContainerQueryIterator<string>` with `SELECT VALUE(c.id)` queries no longer throws `InvalidCastException` (Issue #7). The method now detects `SELECT VALUE` id projections and returns container IDs as strings instead of always casting `ContainerProperties` to `T`.

## [4.0.13] - 2026-05-07

### Fixed
- Nested function calls in SQL queries (e.g. `CONTAINS(LOWER(c.name), @val)`) now evaluate correctly (Issue #11). Two root causes fixed: (1) the parser now routes expressions with nested function call arguments through the `SqlExpressionCondition` evaluation path instead of the legacy `FunctionCondition` path that stringifies arguments, and (2) parameter values backed by `JToken`/`JValue` (from HTTP body parsing) are now unwrapped to their primitive .NET types before function evaluation.

## [4.0.12] - 2026-05-07

### Fixed
- UniqueKeyPolicy with nested paths (e.g. `/address/zipCode`) now correctly enforces uniqueness constraints (Issues #10, #13). The path resolution was using slash-separated notation which `JObject.SelectToken` doesn't support — now converts to dot notation.
- `IContainerTestSetup` now exposes `UniqueKeyPolicy` property, allowing unique key policies to be configured through the `InMemoryCosmos.Create()` builder and test fixtures.

## [4.0.11] - 2026-05-07

### Fixed
- `CreateContainerAsync` no longer throws "Container already exists" when the container was only lazily created by `GetContainer()` (Issue #28). Lazily-created containers are now replaced by the explicitly-created one with the correct partition key path.
- `CreateContainerIfNotExistsAsync` now updates the partition key path and properties when it finds a lazily-created container instead of silently using the wrong default `/id` path.

## [4.0.10] - 2026-05-07

### Fixed
- CosmosSqlParser now accepts `$`-prefixed property names (e.g. `c.$type`) used by EF Core Cosmos as discriminator columns (Issue #35)

## [4.0.9] - 2026-05-07

### Fixed
- Fixed concurrent transactional batch race condition where a failing batch's rollback could destroy items committed by another concurrent batch (Issue #57). `TrackBatchWrite` is now called after the write succeeds, preventing rollback from reverting keys that were never actually modified by the failing batch.

## [4.0.8] - 2026-07-14

### Fixed
- FakeCosmosHandler now implements the change feed HTTP protocol (A-IM: Incremental feed), fixing `ArgumentException: continuationToken must be a non-empty string` when using `ChangeFeedProcessor` through the SDK pipeline

## [4.0.7] - 2026-07-14

### Fixed
- Removed known limitations that were only relevant to the direct `InMemoryContainer` approach (no longer the recommended usage pattern)
- Marked Known Limitation #18 (ChangeFeedProcessor Stream Handler) as FIXED

## [4.0.5] - 2026-04-23

### Fixed
- Document-level 404 (NotFound) exception messages now include "Resource Not Found" to match real Cosmos DB SDK behavior, enabling code that checks `e.Message.Contains("Resource Not Found")` to work correctly with the emulator
- Stream API 404 responses for document CRUD operations now include a JSON error body (`{"code":"NotFound","message":"Resource Not Found. ..."}`) matching the real Cosmos DB REST API format, so `CosmosException.Message` through FakeCosmosHandler contains the expected error text

## [2.0.189] - 2026-04-18

### Added
- NuGet package icon for all three packages

## [2.0.188] and earlier

See [GitHub Releases](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/releases) for previous changes.
