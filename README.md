# Conduit

**One binary. Every database. Your AI's data layer.**

Point Conduit at any SQL database and get a production-ready [MCP](https://modelcontextprotocol.io) endpoint with typed, per-table tools in 30 seconds. Your AI agent gets structured database access — no SQL knowledge required.

```bash
npx @conduitdb/conduit postgres://user:pass@localhost:5432/mydb
```

That's it. Claude, Cursor, VS Code, and ChatGPT can now query your database.

---

## Quick Start

### Try the Demo (No Database Needed)

```bash
npx @conduitdb/conduit demo
```

### Connect to Your Database

```bash
# Install
brew install conduitdb/tap/conduit
# or: go install github.com/conduitdb/conduit@latest

# Run
conduit postgres://user:pass@localhost:5432/mydb

# Add to Claude Code
claude mcp add mydb -- conduit postgres://user:pass@localhost:5432/mydb
```

### Supported Databases

| Database | Status | Driver |
|----------|--------|--------|
| **PostgreSQL** | GA | pgx/v5 |
| **MySQL** / MariaDB | GA | go-sql-driver/mysql |
| **SQL Server** | GA | go-mssqldb |
| **SQLite** | GA | modernc.org/sqlite |
| **Oracle** | GA | go-ora/v2 |
| **Snowflake** | GA | gosnowflake |

---

## What You Get

When you connect a database, Conduit auto-generates **typed MCP tools** from your schema:

### Core Tools (Always Available)

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables with row counts and relationships |
| `describe_table` | Get columns, types, PKs, FKs, indexes |
| `query` | Query any table with filters, sorting, pagination |
| `enable_table_tools` | Load per-table typed CRUD tools on demand |
| `refresh_schema` | Refresh cached schema after changes |

### Per-Table Tools (On Demand)

Call `enable_table_tools(["users", "orders"])` and get:

| Tool | Description |
|------|-------------|
| `query_users` | Typed query with column names in schema |
| `get_user_by_id` | Single record lookup by primary key |
| `insert_users` | Insert rows with typed fields |
| `update_users` | Update rows matching a filter |
| `delete_users` | Delete rows matching a filter |

Write tools require `--allow-writes`. Read-only by default.

---

## Why Conduit?

| | Conduit | DBHub | Google MCP Toolbox | Single-DB MCPs |
|---|---|---|---|---|
| **Setup** | Zero-config | Zero-config | YAML definitions | Varies |
| **Databases** | 6 | 5 | 42+ | 1 each |
| **Tool type** | Typed per-table | Raw SQL only | Manual YAML | Varies |
| **LLM guidance** | Column names in schema | None | None | Varies |
| **Security** | RBAC, PII masking | None | OAuth, IAM | Basic |
| **Binary** | Single binary | npm package | Go binary | Varies |
| **Web UI** | Built-in dashboard | Query tracing | None | None |

---

## Configuration

### CLI Flags

```bash
conduit postgres://... --allow-writes      # Enable write operations
conduit postgres://... --allow-raw-sql     # Enable raw SQL tool
conduit postgres://... --mask-pii          # Mask sensitive columns
conduit postgres://... --max-rows 500      # Limit results
conduit postgres://... --http --port 8090  # HTTP transport + dashboard
```

### Config File (Multi-Database)

```yaml
# conduit.yaml
sources:
  - name: "production"
    driver: "postgres"
    dsn: "postgres://user:pass@db.example.com/prod"
    read_only: true
    mask_pii: true

  - name: "analytics"
    driver: "snowflake"
    dsn: "snowflake://user:pass@account/db/schema"
    read_only: true
```

```bash
conduit --config conduit.yaml
```

### MCP Client Config

```bash
conduit config --client claude-code     # Generate Claude Code config
conduit config --client cursor          # Generate Cursor config
conduit config --client vscode          # Generate VS Code config
conduit config --client claude-desktop  # Generate Claude Desktop config
```

---

## Web Dashboard

Start with HTTP transport to get the built-in dashboard:

```bash
conduit postgres://... --http --port 8090
# Dashboard: http://localhost:8090/ui
```

Features:
- Setup wizard for first-time users
- Database source management
- Schema explorer
- Real-time activity feed
- Config snippet generator for every MCP client

---

## Security

- **Read-only by default** — Write operations require `--allow-writes`
- **Parameterized queries** — Zero SQL injection risk
- **PII masking** — Auto-detect and mask email, phone, SSN, credit cards
- **RBAC** — Role-based table/column access control
- **Audit logging** — Every query logged with user, timestamp, duration
- **DSN sanitization** — Passwords never appear in logs

---

## MCP Compliance

Built on the [official MCP Go SDK](https://github.com/modelcontextprotocol/go-sdk). Full [MCP 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25) spec compliance:

- Tool annotations (readOnlyHint, destructiveHint, idempotentHint)
- Resources (schema://, stats://)
- Prompts (explore_database, analyze_table)
- stdio + Streamable HTTP transports
- `.well-known/mcp.json` server card

---

## Building from Source

```bash
git clone https://github.com/conduitdb/conduit.git
cd conduit
task build    # Builds frontend + Go binary
task test     # Run tests
task lint     # Run linter
```

Requires: Go 1.26+, Node.js 22+, [Task](https://taskfile.dev)

---

## License

Apache 2.0 — see [LICENSE](LICENSE)
