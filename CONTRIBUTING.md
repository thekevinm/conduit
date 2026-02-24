# Contributing to Conduit

Thank you for your interest in contributing to Conduit! This document provides guidelines for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/conduit.git`
3. Install dependencies:
   - Go 1.26+
   - Node.js 22+
   - [Task](https://taskfile.dev) (build runner)
4. Run tests: `task test`
5. Build: `task build:go`

## Development Workflow

1. Create a branch from `main`: `git checkout -b feature/my-feature`
2. Make your changes
3. Run tests: `task test`
4. Run linter: `task lint`
5. Commit with a clear message
6. Push and open a PR against `dev`

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Use `log/slog` for structured logging
- All exported functions need doc comments
- Write table-driven tests where appropriate
- Zero string interpolation in SQL — parameterized queries only

## Good First Issues

Look for issues labeled `good first issue` — these are specifically chosen for new contributors:

- Adding a new database type mapping
- Improving error messages
- Adding test coverage
- Documentation improvements
- CLI flag additions

## Adding a Database Connector

To add a new database connector:

1. Create `internal/connector/yourdb/` with three files:
   - `connector.go` — Implement the `Connector` interface
   - `introspect.go` — Schema discovery queries
   - `querybuilder.go` — Dialect-specific SQL generation
2. Self-register in `init()` via `connector.Register()`
3. Add type mapping (native types → simplified 7 types)
4. Add integration tests using testcontainers
5. Update the README with the new database

## Reporting Bugs

Please use the GitHub Issues template and include:
- Conduit version (`conduit version`)
- Database type and version
- Steps to reproduce
- Expected vs actual behavior

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Be kind.

## License

By contributing, you agree that your contributions will be licensed under Apache 2.0.
