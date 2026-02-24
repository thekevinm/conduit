# CLAUDE.md — Conduit

## What Is This

Conduit is an open-source auto-MCP generator. Point it at any SQL database, get a production-ready MCP endpoint with typed, per-table tools in 30 seconds.

**Tagline:** "One binary. Every database. Your AI's data layer."

## Tech Stack

- **Language:** Go 1.26+
- **MCP SDK:** `modelcontextprotocol/go-sdk` v1.2.x+ (official)
- **CLI:** Cobra + Viper
- **Frontend:** Svelte 5, SvelteKit (adapter-static), Tailwind CSS v4, shadcn-svelte
- **Databases:** PostgreSQL (pgx), MySQL, SQL Server, SQLite (pure Go), Oracle, Snowflake
- **Build:** Taskfile.dev, GoReleaser, golangci-lint

## Project Structure

```
conduit/
├── cmd/conduit/          # Entry point
├── internal/
│   ├── app/              # Application orchestrator
│   ├── cli/              # Cobra commands
│   ├── connector/        # Database connector interface + implementations
│   │   ├── postgres/
│   │   ├── mysql/
│   │   ├── mssql/
│   │   ├── sqlite/
│   │   ├── oracle/
│   │   └── snowflake/
│   ├── schema/           # Schema models, cache, digest, PII detection
│   ├── query/            # Query engine, filter parser, sanitizer
│   ├── mcpgen/           # MCP tool/resource/prompt generation
│   ├── server/           # MCP server setup, auth, middleware
│   ├── access/           # RBAC engine
│   ├── audit/            # Audit logging
│   ├── web/              # Embedded UI handler
│   └── demo/             # Demo mode seed data
├── frontend/             # SvelteKit app (embedded via //go:embed)
└── examples/             # Docker compose, sample configs
```

## Key Commands

```bash
task build          # Build binary (includes frontend)
task test           # Run tests with race detector
task lint           # Run golangci-lint
task dev            # Dev mode with hot reload
task frontend:dev   # Frontend dev server
```

## Architecture Principles

1. **Single process** — no multi-process architecture
2. **Direct database access** — database/sql drivers, no intermediary
3. **Dynamic tool generation** — schema-derived tools, not hardcoded
4. **Token efficiency** — minimal schema representation for LLMs
5. **Read-only by default** — write access explicitly opted in
6. **Zero-config to full-config** — DSN alone works; YAML unlocks everything
7. **Official SDK** — use modelcontextprotocol/go-sdk, don't reinvent MCP

## Security Rules

- NEVER log raw DSNs (always sanitize passwords)
- ALWAYS use parameterized queries (zero string interpolation)
- NEVER expose password/token/secret columns
- All user-facing errors must suggest corrections (fuzzy matching)

## Two-Tier Tool Strategy

- **Tier 1 (always present):** list_tables, describe_table, query, list_procedures, call_procedure, enable_table_tools, refresh_schema
- **Tier 2 (on demand):** Per-table query_X, get_X_by_id, insert_X, update_X, delete_X — loaded via enable_table_tools meta-tool

## Testing

- Unit tests: `go test ./...`
- Integration tests: testcontainers-go (real databases in Docker)
- Frontend tests: Vitest + Playwright
