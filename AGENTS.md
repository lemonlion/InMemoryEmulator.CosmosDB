# Contribution Instructions

## TDD Workflow

- Always use Test-Driven Development (TDD): write integration tests first, then follow the red-green-refactor cycle.
- Write a failing test (red), implement the minimum code to make it pass (green), then refactor.
- Write additional failing integration tests to cover edge cases and error conditions, and repeat the cycle until you have comprehensive test coverage for the feature or bug fix you're working on.
- Only write unit tests when it's not possible to test with integration tests

## Bug Fixing

- Always fix all bugs you find along the way, even if they are outside the immediate scope of the current task.
- When fixing a bug, identify missing test coverage in and around the affected area and create that coverage — again following the TDD red-green-refactor cycle.
- Fix any additional bugs discovered during that expanded test coverage work.

## Reflection Policy

- **Do not use reflection as a first resort.** Explore all public API options before considering reflection.
- Reflection on internal/private members of external libraries (e.g., SDK backing fields) is fragile — it can break silently on library updates with no compile-time warning.
- If reflection is genuinely the only viable approach after exhausting alternatives, it may be used — but:
  - **The PR description must explicitly state in bold that reflection is used**, what it targets, and why no public API alternative exists.
  - Add a code comment at the reflection site explaining the dependency and what would break if the internal member is renamed or removed.
  - Prefer a graceful fallback (e.g., leave the value as null) over a hard failure if the reflected member is missing.

## Behavioral Source Requirements

Every piece of behavioral logic in the source code — status codes, validation rules, error conditions, side-effect semantics — **must** be backed by a verified source. This prevents accidental divergence from real Cosmos DB behavior.

### Rules

1. **Before implementing any behavioral logic**, find and verify the expected behavior from one of the approved sources listed below.
2. **Add a code comment** at the implementation site citing the source (a short URL or description is sufficient). Example:
   ```csharp
   // Ref: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-dotnet-create-item
   //   "The Container.ReplaceItemAsync<> method requires the provided string for the id
   //    parameter to match the unique identifier of the item parameter."
   ```
3. **If sources conflict** (e.g., the emulator behaves differently from the documentation), prefer the official documentation over observed emulator behavior. Document the discrepancy in a code comment and mark the relevant integration test with `[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]`.
4. **If no source can be found**, do not guess. Ask for guidance or raise a discussion in the PR.

### Approved Behavioral Sources (in priority order)

| Priority | Source | URL / Location |
|----------|--------|----------------|
| 1 | Azure Cosmos DB REST API reference | https://learn.microsoft.com/en-us/rest/api/cosmos-db/ |
| 2 | Azure Cosmos DB .NET SDK API reference | https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos |
| 3 | Azure Cosmos DB "How-to" guides | https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/ |
| 4 | Azure Cosmos DB conceptual docs (OCC, partitioning, indexing, etc.) | https://learn.microsoft.com/en-us/azure/cosmos-db/ |
| 5 | Azure Cosmos DB .NET SDK source code | https://github.com/Azure/azure-cosmos-dotnet-v3 |
| 6 | Observed behavior on the Windows Cosmos DB Emulator | (local testing) |

> **Note:** Source 6 (emulator observation) is the weakest evidence. The emulator has known bugs. Always cross-reference with sources 1–5 when possible.

## Versioning & Release

Version numbers are computed automatically from `nuget-version.yaml` at the repo root and the existing GitHub release history — **no manual version edits are needed for patch releases**.

- `nuget-version.yaml` contains only `major` and `minor`. The patch is auto-incremented from the latest stable GitHub release for that `major.minor` pair each time a release is triggered.
- For a **major or minor bump**, edit `nuget-version.yaml` and merge to `main` before triggering the next release.
- **Do not** edit `<Version>` in `src/Directory.Build.props` — it produces `0.0.0-local` for local builds, which is correct. The CI/release workflows always pass `-p:Version=X.Y.Z` at build time.
- After bug-fix sessions: **no version file changes are needed** — just commit and push your code changes.

## Test Classification Rules

Tests are split into two projects. When creating or moving tests, follow these rules:

### Tests.Integration
- Uses `TestFixtureFactory.Create(session)` / `ITestContainerFixture` to obtain a container, where `session` is an injected `EmulatorSession` (xUnit collection fixture — decorate the test class with `[Collection(IntegrationCollection.Name)]`)
- Goes through the real CosmosClient SDK HTTP pipeline via `FakeCosmosHandler`
- Must **not** use `new InMemoryCosmosClient()`, `new FaultInjector()`, `FaultInjection`, or any `internal` API
- Can run against in-memory, Linux emulator, or Windows emulator via `COSMOS_TEST_TARGET`

### Tests.Unit
- Uses `new InMemoryContainer()`, `new InMemoryCosmosClient()`, or any `internal` API directly
- Tests that use `FakeCosmosHandler` but also touch internal APIs (e.g. cache internals, `FaultInjection`) belong here
- Only runs in-memory — never against a real emulator

### Tests.Shared
- Class library (not a test project) — shared infrastructure, fixtures, traits, and models
- Referenced by both Unit and Integration projects

### Key constraint
The Integration project does **not** have `InternalsVisibleTo` access. If a test needs internal APIs, it belongs in Unit.

## Publishing & Releases

Releases are published manually via the **Actions → Release** workflow (`workflow_dispatch`). The full test suite runs before anything is published. Releases are serialised — if two releases are triggered simultaneously, the second queues behind the first.

### PR Version Preview

When a PR changes files in `src/`, `*.props`, or `nuget-version.yaml`, the CI `version-preview` job shows in the workflow step summary what the next beta and stable versions would be if published from that branch.

### Stable Release (from `main`)

1. Merge all desired changes to `main`. Update `CHANGELOG.md` if appropriate.
2. Go to **Actions → Release** and trigger the workflow from `main` with **Prerelease: unchecked** (the default).
3. The workflow auto-computes the next patch version, runs all tests, publishes to NuGet.org, and creates a GitHub Release with auto-generated notes.

### Prerelease / Beta (from any branch)

1. Push your branch.
2. Go to **Actions → Release**, select your branch from the branch dropdown, and trigger with **Prerelease: checked** (beta is also automatic when triggering from any non-`main` branch).
3. The computed version will be `X.Y.Z-beta.N.sanitised-branch-name`, where N auto-increments.
4. Consumers install with: `dotnet add package CosmosDB.InMemoryEmulator --version X.Y.Z-beta.N.branch-name`

### Version Conventions

- Patch auto-increments: the release workflow queries the highest existing `vX.Y.*` GitHub release tag and adds 1.
- Beta format: `X.Y.Z-beta.N.sanitised-branch-name`
- Stable format: `X.Y.Z`
- To bump major or minor: edit `nuget-version.yaml` (`major`/`minor` fields) and merge to `main` before the next release.

## Documentation

After any changes are made that might effect the public API or functionality, documentation must be updated to reflect those changes.  The documentation should be clear and comprehensive, covering all new features, changes to existing features, and any deprecations or removals.  This includes updating README file (if relevant), but mainly the wiki which can be found in a sister folder to the main repository - ../CosmosDB.InMemoryEmulator.wiki.