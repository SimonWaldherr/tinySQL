# tinysql-mcp-server

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server for the
[tinySQL](https://github.com/SimonWaldherr/tinySQL) embedded database, written in Go.

---

## Purpose

`tinysql-mcp-server` lets any MCP-capable host (Claude Desktop, VS Code Copilot, Cursor, etc.)
interact with a tinySQL database through a standard set of tools, resources, and prompts.

An MCP host can:

- Execute read-only `SELECT` queries.
- Execute mutating SQL (`INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`).
- Inspect the database schema and tenant metadata.
- Record and retrieve analytical observations (insight memo).
- Run a guided interactive demo for any topic.

---

## Relationship to the SQLite MCP Server

This implementation was **functionally inspired** by the archived
[SQLite MCP server](https://github.com/modelcontextprotocol/servers-archived/tree/main/src/sqlite)
from the MCP project.  It is **not** a wrapper around SQLite, nor is it a
mechanical translation of that code.

Key differences:

| Feature | SQLite MCP server | tinysql-mcp-server |
|---|---|---|
| Backend | SQLite via Node.js | tinySQL (native Go) |
| Protocol layer | Python/TypeScript SDK | Official Go MCP SDK v1.6.0 |
| Schema metadata | `sqlite_master` | `sys.tables`, `sys.columns` |
| Multi-tenancy | No | Yes (via tenant parameter) |
| Agent context | No | Yes (via SQL fallback on sys.*) |
| Transport | stdio | stdio |

---

## Why tinySQL?

tinySQL is a lightweight, embeddable SQL engine written in Go that demonstrates
core database concepts (MVCC, WAL, multi-tenancy, vector search, full-text search, and more).
It is ideal for embedded use cases, AI agent tooling, and educational purposes.

> **Note:** tinySQL is a lightweight, educational database engine. It is not a
> drop-in replacement for mature production relational database systems such as
> PostgreSQL, MySQL, or SQLite. Evaluate it accordingly before using it in
> production workloads.

---

## Installation

### Build from source

```bash
# From the tinySQL repository root:
go build ./cmd/tinysql-mcp-server
```

This produces a `tinysql-mcp-server` binary in the current directory.

---

## Usage

### In-memory database (ephemeral)

```bash
tinysql-mcp-server --dsn "mem://?tenant=default"
```

### File-backed database (persistent)

```bash
tinysql-mcp-server --dsn "file:./data/tinysql.db?tenant=default&autosave=1"
```

### Using `--db-path` shorthand

```bash
tinysql-mcp-server --db-path ./data/tinysql.db --tenant default --autosave
```

> If both `--dsn` and `--db-path` are provided, `--dsn` takes precedence and
> `--db-path` is silently ignored (a warning is logged).

---

## Command-line flags

| Flag | Default | Description |
|---|---|---|
| `--dsn` | `""` | Full tinySQL DSN (`mem://` or `file:path?...`). Takes precedence over `--db-path`. |
| `--db-path` | `""` | Convenience shorthand for a file-backed database path. |
| `--tenant` | `"default"` | Tenant namespace (derived from DSN if omitted). |
| `--autosave` | `false` | Enable auto-save for file-backed databases. |
| `--readonly` | `false` | Block all mutating tools (`write_query`, `create_table`). |
| `--max-rows` | `0` | Maximum rows returned by `read_query` (0 = unlimited). |
| `--query-timeout` | `0` | Per-query timeout (e.g. `5s`; 0 = no timeout). |
| `--log-level` | `"info"` | Log verbosity: `debug`, `info`, `warn`, `error`. |

---

## MCP host configuration

### Generic MCP host (e.g. Claude Desktop)

Create or edit `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) or the equivalent for your platform:

```json
{
  "mcpServers": {
    "tinysql": {
      "command": "/absolute/path/to/tinysql-mcp-server",
      "args": [
        "--dsn",
        "file:/absolute/path/to/tinysql.db?tenant=default&autosave=1"
      ]
    }
  }
}
```

See [`examples/mcp_config.json`](examples/mcp_config.json) for a complete example
including an in-memory variant.

### VS Code (`.vscode/mcp.json`)

```json
{
  "servers": {
    "tinysql": {
      "type": "stdio",
      "command": "${workspaceFolder}/tinysql-mcp-server",
      "args": [
        "--dsn",
        "mem://?tenant=default"
      ]
    }
  }
}
```

---

## Available tools

| Tool | Description |
|---|---|
| `read_query` | Execute a read-only `SELECT` (or CTE) query. Mutating statements are rejected. |
| `write_query` | Execute a mutating statement (`INSERT`, `UPDATE`, `DELETE`). Blocked in `--readonly` mode. |
| `create_table` | Execute a `CREATE TABLE` statement. Other DDL is rejected. Blocked in `--readonly` mode. |
| `list_tables` | List all tables in the active tenant via `sys.tables`. |
| `describe_table` | Return column metadata for a table via `sys.columns`. |
| `append_insight` | Append an analytical observation to the in-memory insight memo. |
| `agent_context` | Return a compact, prompt-ready database profile (tables, columns, views, features). |
| `sample_table` | Return the first N rows of a table (identifier-quoted to prevent injection). |

---

## Available resources

| URI | MIME type | Description |
|---|---|---|
| `memo://insights` | `text/markdown` | Live insight memo updated by `append_insight`. |
| `tinysql://schema` | `application/json` | Current database schema (tables, columns, views, tenant). |
| `tinysql://agent-context` | `text/plain` | Compact agent profile derived from `sys.*` metadata. |

---

## Available prompts

| Prompt | Arguments | Description |
|---|---|---|
| `tinysql-demo` | `topic` (required) | Guides an MCP host through a full tinySQL analysis workflow for the given topic. |

---

## Security model

- **Statement classification:** All SQL is classified before execution using a
  conservative keyword-based classifier that strips comments and normalises
  whitespace.  Comment injection attacks (e.g. `/* SELECT */ DELETE FROM t`) are
  handled by stripping comments before examining the leading keyword.

- **Multi-statement rejection:** Inputs containing a semicolon before the trailing
  position are rejected to prevent statement smuggling.

- **Read-only mode:** `--readonly` blocks all mutating tools at the application
  layer.  Individual tools (`write_query`, `create_table`) check the flag
  independently.

- **Identifier validation:** `describe_table` and `sample_table` validate the
  supplied table name against `^[a-zA-Z_][a-zA-Z0-9_]*$` and quote it with
  double-quotes before interpolation.

- **Query timeout:** Pass `--query-timeout 5s` to bound every query execution.

- **Row limits:** Pass `--max-rows 1000` to cap result set sizes.

- **Logging:** DSN query parameters are not logged. The log output goes to
  stderr; MCP communication uses stdout exclusively.

- **No internal imports:** The MCP server uses only the public tinySQL API
  (`github.com/SimonWaldherr/tinySQL` and `github.com/SimonWaldherr/tinySQL/driver`).
  No `internal/*` packages are imported.

---

## Known limitations

- **`BuildAgentContext` unavailable via `database/sql`:** The `agent_context` tool
  and `tinysql://agent-context` resource use a SQL-based fallback that queries
  `sys.tables`, `sys.columns`, and `sys.views` instead of calling
  `tinysql.BuildAgentContext` directly.  The output is functionally equivalent for
  schema inspection purposes.

- **`affected_rows` availability:** tinySQL's `database/sql` driver may not always
  return a meaningful `RowsAffected` count.  When unavailable, `write_query`
  returns a truthful explanatory field instead of inventing a number.

- **CTE classification:** The CTE classifier is heuristic.  Deeply nested or
  atypical CTE structures may be classified as `kindUnknown` and rejected.

- **String literals with semicolons:** The multi-statement heuristic scans for
  semicolons without parsing string literals, so queries with a literal semicolon
  inside a string value may be incorrectly rejected.  Use parameterised queries
  (not yet supported via MCP tools) as a workaround.

- **File-backed persistence:** When using `file:` DSNs, the autosave behaviour
  is governed by the tinySQL internal driver.  Use `--autosave` or include
  `autosave=1` in the DSN.

- **tinySQL maturity:** tinySQL is an educational engine.  See the
  [tinySQL repository](https://github.com/SimonWaldherr/tinySQL) for supported
  features and known gaps.

---

## Running tests

```bash
cd cmd/tinysql-mcp-server
go test ./...
```

All tests run against an in-memory tinySQL database and do not require any external
services.
