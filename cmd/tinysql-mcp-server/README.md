# tinysql-mcp-server

An [MCP](https://modelcontextprotocol.io/) server for tinySQL over stdio. An
MCP host can query and modify a tinySQL database, inspect its schema, run RAG
retrieval, and keep an in-memory insight memo.

It uses tinySQL directly rather than wrapping SQLite. Review the
[root limitations](../../README.md#limitations) before relying on it for a
production workload.

## Build and run

```bash
go build ./cmd/tinysql-mcp-server

# ephemeral database
./tinysql-mcp-server --dsn "mem://?tenant=default"

# persistent database
./tinysql-mcp-server --db-path ./data/tinysql.db --tenant default --autosave
```

A supplied --dsn overrides --db-path.

| Flag | Default | Purpose |
| --- | --- | --- |
| --dsn | empty | Full mem:// or file: tinySQL DSN |
| --db-path | empty | File-backed database shorthand |
| --tenant | default | Tenant namespace |
| --autosave | false | Save a file-backed database automatically |
| --readonly | false | Disable write_query and create_table |
| --allow-sql-host-io | false | Enable SQL `FILE()` and `HTTP()` for trusted clients that need to read server files or fetch URLs |
| --max-rows | 1000 | Maximum rows returned to the MCP client; 0 removes the cap |
| --query-timeout | 30s | Execution deadline; 0 removes it |
| --log-level | info | debug, info, warn, or error |

## Configure a host

Use the built binary as a stdio command. For example, Claude Desktop uses an
MCP configuration like:

```json
{
  "mcpServers": {
    "tinysql": {
      "command": "/absolute/path/to/tinysql-mcp-server",
      "args": ["--db-path", "/absolute/path/to/tinysql.db", "--autosave"]
    }
  }
}
```

See [examples/mcp_config.json](examples/mcp_config.json) for the complete
in-memory configuration. VS Code uses the same command and arguments in
.vscode/mcp.json with type set to stdio.

## MCP surface

| Tool | Purpose |
| --- | --- |
| read_query | Read-only SELECT or CTE, including FTS, vector, and RAG functions |
| write_query | INSERT, UPDATE, or DELETE |
| create_table | CREATE TABLE only |
| list_tables / describe_table / sample_table | Schema and bounded table inspection |
| rag_search | Vector retrieval with optional BM25 fusion and chunk expansion |
| append_insight | Add a note to the in-memory insight memo |
| agent_context | Compact schema and feature profile |

| Resource or prompt | Purpose |
| --- | --- |
| memo://insights | Live Markdown insight memo |
| tinysql://schema | Schema, views, and tenant as JSON |
| tinysql://agent-context | Compact text schema profile |
| tinysql://functions | Vector, FTS, and RAG function signatures |
| tinysql-demo | Guided analysis prompt; requires a topic |

## Operate it safely

- Treat the MCP host and every connected agent as a database client. Use
  --readonly unless writes are required, and keep database filesystem
  permissions narrow.
- Keep both --max-rows and --query-timeout bounded. The row limit truncates
  client-visible output; it does not limit engine work.
- Each SQL input must contain one statement. Quoted strings may contain
  semicolons.
- The current startup log can include a short DSN prefix. Avoid credentialed
  DSNs in --dsn until that behavior is changed; use --db-path when possible
  and do not expose process logs.
- MCP protocol traffic uses stdout and logs use stderr. Do not add other output
  to stdout.

The server uses the tinySQL parser to route each statement. A CTE ending in
SELECT goes to read_query, and a CTE ending in INSERT goes to write_query.
The read tool requires an explicit SELECT or WITH query. Table-inspection
tools quote validated table identifiers. Agent context reads the schema
through sys tables and database/sql.

## Test

```bash
cd cmd/tinysql-mcp-server
go test ./...
```

The tests use an in-memory database and no external service.
