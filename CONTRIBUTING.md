# Contributing to CosmosDB.InMemoryEmulator

Thank you for your interest in contributing!

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Create a feature branch: `git checkout -b my-feature`
4. Make your changes following the guidelines below
5. Push and open a Pull Request

## Development Requirements

- .NET 8.0 SDK (or later)
- PowerShell (for test scripts)
- Docker (for cross-platform testing — Docker Desktop or Rancher Desktop)

## Building

```bash
dotnet build CosmosDB.InMemoryEmulator.sln
```

## Running Tests

```powershell
# Unit tests only (Windows, in-memory)
dotnet test tests/CosmosDB.InMemoryEmulator.Tests.Unit

# Integration tests (Windows, in-memory)
dotnet test tests/CosmosDB.InMemoryEmulator.Tests.Integration
```

## Cross-Platform Testing

This project runs CI on both Windows and Linux. To reproduce platform-specific issues locally, use the dev environment script:

```powershell
# See what's running and available scenarios
./scripts/dev-env.ps1 status

# Run tests on Linux (catches gateway fallback / runtime differences)
./scripts/dev-env.ps1 test -Platform linux -Target inmemory -Project integration

# Run tests on Linux against the Linux Cosmos emulator
./scripts/dev-env.ps1 test -Platform linux -Target emulator-linux -Project integration

# Run tests on Windows against the Linux Cosmos emulator (emulator behavior differences)
./scripts/dev-env.ps1 test -Platform windows -Target emulator-linux -Project integration

# Run a specific failing test on Linux for investigation
./scripts/dev-env.ps1 test -Platform linux -Target inmemory -Filter "FullyQualifiedName~MyFailingTest"

# Start the Linux dev container for interactive exploration
./scripts/dev-env.ps1 start
./scripts/dev-env.ps1 exec -Cmd "dotnet build -c Release"

# Tear down all containers
./scripts/dev-env.ps1 stop
```

See [scripts/dev-env.ps1](scripts/dev-env.ps1) for full documentation of all commands and parameters.

## Guidelines

- **TDD**: Write a failing test first, then implement the minimum code to make it pass, then refactor.
- **Test Classification**: Unit tests go in `Tests.Unit`, integration tests in `Tests.Integration`. See [AGENTS.md](AGENTS.md) for classification rules.
- **No breaking changes** without discussion in an issue first.
- **Keep PRs focused** — one feature or fix per PR.

## Reporting Issues

- Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md) for bugs.
- Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md) for new ideas.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
