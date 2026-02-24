# AGENTS.md — Conduit

## Identity

Conduit is an open-source auto-MCP generator. It connects SQL databases to AI agents via the Model Context Protocol.

## Instructions

- Read CLAUDE.md for project architecture and conventions
- Use `task test` to run tests, `task build:go` to build
- All SQL must use parameterized queries — never interpolate values
- Follow the two-tier tool strategy (core tools always present, per-table tools on demand)
- Use `log/slog` for logging, never `fmt.Println` in library code
- Database passwords must never appear in logs — use `connector.SanitizeDSN()`

## Key Files

| Path | Purpose |
|------|---------|
| `cmd/conduit/main.go` | Entry point |
| `internal/connector/interface.go` | Database connector interface |
| `internal/schema/model.go` | Schema types (TableSummary, TableDetail, etc.) |
| `internal/mcpgen/generator.go` | Schema → MCP tool generation |
| `internal/server/server.go` | MCP server setup |
| `internal/cli/root.go` | CLI commands |
