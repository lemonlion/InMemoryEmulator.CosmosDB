# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
