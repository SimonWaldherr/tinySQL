# tinysql-mcp-server

Part of [TinySQL](../../README.md). Review the root limitations and this
command's security model before attaching it to an agent host.

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server for
the [tinySQL](https://github.com/SimonWaldherr/tinySQL) embedded database.

It lets any MCP-capable host (Claude Desktop, VS Code Copilot, Cursor, ...) run
`SELECT` queries and mutating SQL, inspect schema and tenant metadata, record
analytical observations, and run a guided demo — over stdio.

The design was functionally inspired by the archived
[SQLite MCP server](https://github.com/modelcontextprotocol/servers-archived/tree/main/src/sqlite),
but it is not a wrapper around SQLite. It uses tinySQL natively via the official
Go MCP SDK v1.6.1, reads schema from `sys.tables` / `sys.columns` / `sys.views`
instead of `sqlite_master`, and adds multi-tenancy (tenant parameter) and an
agent-context profile.

> tinySQL is a lightweight, educational engine. It is not a drop-in replacement
> for PostgreSQL, MySQL, or SQLite. Evaluate it accordingly before using it in
> production workloads.

## Build

```bash
# From the tinySQL repository root:
go build ./cmd/tinysql-mcp-server
```

## Usage

```bash
# In-memory (ephemeral)
tinysql-mcp-server --dsn "mem://?tenant=default"

# File-backed (persistent)
tinysql-mcp-server --dsn "file:./data/tinysql.db?tenant=default&autosave=1"

# --db-path shorthand
tinysql-mcp-server --db-path ./data/tinysql.db --tenant default --autosave
```

If both `--dsn` and `--db-path` are given, `--dsn` wins and `--db-path` is
ignored (a warning is logged).

### Flags

| Flag | Default | Description |
|---|---|---|
| `--dsn` | `""` | Full tinySQL DSN (`mem://` or `file:path?...`). Takes precedence over `--db-path`. |
| `--db-path` | `""` | Shorthand for a file-backed database path. |
| `--tenant` | `"default"` | Tenant namespace (derived from DSN if omitted). |
| `--autosave` | `false` | Enable auto-save for file-backed databases. |
| `--readonly` | `false` | Block all mutating tools (`write_query`, `create_table`). |
| `--max-rows` | `1000` | Maximum rows returned by `read_query` (0 = unlimited). |
| `--query-timeout` | `30s` | Per-query timeout (0 = no timeout). |
| `--log-level` | `"info"` | Log verbosity: `debug`, `info`, `warn`, `error`. |

## MCP host configuration

Claude Desktop (`~/Library/Application Support/Claude/claude_desktop_config.json`
on macOS, equivalent path elsewhere):

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

See [`examples/mcp_config.json`](examples/mcp_config.json) for a complete
example including an in-memory variant.

VS Code (`.vscode/mcp.json`):

```json
{
  "servers": {
    "tinysql": {
      "type": "stdio",
      "command": "${workspaceFolder}/tinysql-mcp-server",
      "args": ["--dsn", "mem://?tenant=default"]
    }
  }
}
```

## Tools

| Tool | Description |
|---|---|
| `read_query` | Read-only `SELECT` (or CTE). Mutating statements are rejected. Vector/FTS/RAG functions are usable here. |
| `write_query` | `INSERT`, `UPDATE`, `DELETE`. Blocked in `--readonly` mode. |
| `create_table` | `CREATE TABLE` only; other DDL is rejected. Blocked in `--readonly` mode. |
| `list_tables` | List tables of the active tenant via `sys.tables`. |
| `describe_table` | Column metadata for a table via `sys.columns`. |
| `append_insight` | Append an observation to the in-memory insight memo. |
| `agent_context` | Compact, prompt-ready database profile (tables, columns, views, features). |
| `sample_table` | First N rows of a table (identifier-quoted to prevent injection). |
| `rag_search` | Composed RAG retrieval: `VEC_SEARCH` k-NN, optional BM25 hybrid fusion via reciprocal rank fusion, optional neighbor-chunk context expansion. |

## Resources

| URI | MIME type | Description |
|---|---|---|
| `memo://insights` | `text/markdown` | Live insight memo updated by `append_insight`. |
| `tinysql://schema` | `application/json` | Current schema (tables, columns, views, tenant). |
| `tinysql://agent-context` | `text/plain` | Compact agent profile derived from `sys.*` metadata. |
| `tinysql://functions` | `text/plain` | Signatures of the vector, full-text, and RAG functions, including the `options_json` shape for `rag_search`. |

## Prompts

| Prompt | Arguments | Description |
|---|---|---|
| `tinysql-demo` | `topic` (required) | Guides a host through a full tinySQL analysis workflow for the topic. |

## Security model

- Statement classification: all SQL is classified before execution by a
  conservative keyword-based classifier that strips comments and normalises
  whitespace, so comment injection (e.g. `/* SELECT */ DELETE FROM t`) does not
  hide the leading keyword.
- Multi-statement rejection: input containing a semicolon before the trailing
  position is rejected to prevent statement smuggling.
- Read-only mode: `--readonly` blocks mutating tools at the application layer;
  `write_query` and `create_table` check the flag independently.
- Identifier validation: `describe_table` and `sample_table` validate the table
  name against `^[a-zA-Z_][a-zA-Z0-9_]*$` and double-quote it before
  interpolation.
- Bounds: `--query-timeout 5s` bounds every execution, `--max-rows 1000` caps
  result sets.
- Logging: DSN query parameters are never logged. Logs go to stderr; MCP
  communication uses stdout exclusively.
- No `internal/*` imports: only the public API
  (`github.com/SimonWaldherr/tinySQL` and
  `github.com/SimonWaldherr/tinySQL/driver`).

## Known limitations

- `BuildAgentContext` is unavailable via `database/sql`, so `agent_context` and
  `tinysql://agent-context` use a SQL fallback over `sys.tables`, `sys.columns`,
  and `sys.views`. Functionally equivalent for schema inspection.
- tinySQL's `database/sql` driver may not return a meaningful `RowsAffected`.
  When unavailable, `write_query` returns a truthful explanatory field instead
  of inventing a number.
- The CTE classifier is heuristic; deeply nested or atypical CTE structures may
  be classified as `kindUnknown` and rejected.
- The multi-statement heuristic scans for semicolons without parsing string
  literals, so a query with a literal semicolon inside a string value may be
  incorrectly rejected. Parameterised queries are not yet exposed via MCP
  tools.
- With `file:` DSNs, autosave is governed by the tinySQL internal driver: use
  `--autosave` or `autosave=1` in the DSN.

## Tests

```bash
cd cmd/tinysql-mcp-server
go test ./...
```

Tests run against an in-memory database and need no external services.
