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

- After every session of bug fixes is complete and the full test suite has passed, increment the patch version in `src/Directory.Build.props` (the single `<Version>` property shared by all three packages).
- **On `main`:** Commit, create a git tag (`v{version}`), and push both the commit and the tag to origin.
- **On any other branch:** Commit and push the code changes and version bump only. Do not create or push a tag.

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

## Cross-Platform Testing (Dev Environment)

When investigating platform-specific test failures, use `scripts/dev-env.ps1` to reproduce issues locally on any platform + backend combination.

### Quick Reference

```powershell
# Check current state
./scripts/dev-env.ps1 status

# Reproduce a Linux CI failure
./scripts/dev-env.ps1 test -Platform linux -Target inmemory -Project integration -Filter "FullyQualifiedName~FailingTest"

# Reproduce a Linux + emulator CI failure
./scripts/dev-env.ps1 test -Platform linux -Target emulator-linux -Project integration -Filter "FullyQualifiedName~FailingTest"

# Run against Linux emulator from Windows host (tests emulator behavioral differences)
./scripts/dev-env.ps1 test -Platform windows -Target emulator-linux -Project integration

# Run arbitrary commands in the Linux container
./scripts/dev-env.ps1 exec -Cmd "dotnet build -c Release"

# Tear down when done
./scripts/dev-env.ps1 stop
```

### Investigation Workflow

When a test fails in CI on Linux but passes locally on Windows:

1. First try reproducing on Linux with in-memory: `./scripts/dev-env.ps1 test -Platform linux -Target inmemory -Filter "FullyQualifiedName~TestName"`
2. If it fails → it's a Linux .NET runtime difference (gateway fallback, string handling, etc.)
3. If it passes → try with the emulator: `./scripts/dev-env.ps1 test -Platform linux -Target emulator-linux -Filter "FullyQualifiedName~TestName"`
4. If that fails → it's a Linux + emulator interaction issue
5. Edit source files normally (they're mounted into the container), then re-run

### Supported Scenarios

| Platform | Target | Docker Required |
|----------|--------|----------------|
| windows | inmemory | No |
| windows | emulator-windows | No (needs Windows emulator installed) |
| windows | emulator-linux | Yes (emulator container) |
| linux | inmemory | Yes (dev container) |
| linux | emulator-linux | Yes (dev + emulator containers) |

## Documentation

After any changes are made that might effect the public API or functionality, documentation must be updated to reflect those changes.  The documentation should be clear and comprehensive, covering all new features, changes to existing features, and any deprecations or removals.  This includes updating README file (if relevant), but mainly the wiki which can be found in a sister folder to the main repository - ../CosmosDB.InMemoryEmulator.wiki.