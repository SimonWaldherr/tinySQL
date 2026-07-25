# tinySQL Command Variants

Binaries under `cmd/`, each with its own README.

Conventions:

- In-memory DSN: `mem://?tenant=default`. File-backed:
  `file:/path/to/db.dat?tenant=<name>&autosave=1`.
- `repl` and `tinysql` rely on the internal `database/sql` driver registration
  (`_ ".../internal/driver"`).
- `server` uses `internal/storage` directly with the same DSN conventions.

## [demo](demo/README.md)

Creates tables, inserts sample data, runs example queries.

```bash
go build ./cmd/demo
./demo -dsn "mem://?tenant=default"
```

## [repl](repl/README.md)

Interactive SQL REPL on top of `database/sql`, with multiple output formats,
echo mode, and optional HTML output.

```bash
go build ./cmd/repl
./repl -dsn "mem://?tenant=default"
```

Flags: `-dsn`, `-echo`, `-format <table|csv|tsv|json|yaml|markdown>`,
`-beautiful`, `-html`, `-errors-only`

## [server](server/README.md)

HTTP JSON API and gRPC (JSON codec) server with optional federation across
peers.

```bash
cd cmd/server && go build .
./server -http :8080 -grpc :9090 -dsn "mem://?tenant=default" -peers "host1:9090,host2:9090"
```

Flags:

- Core: `-dsn`, `-http <addr>`, `-grpc <addr>`, `-auth <token>`,
  `-peers <addr,...>`, `-tenant <name>`, `-v`
- TLS: `-tls-min-version`, `-http-tls-cert`, `-http-tls-key`,
  `-grpc-tls-cert`, `-grpc-tls-key`, `-peer-tls`, `-peer-tls-ca`,
  `-peer-tls-server-name`, `-peer-tls-skip-verify`
- Limits: `-max-body-bytes`, `-max-sql-bytes`, `-grpc-max-recv-bytes`,
  `-grpc-max-send-bytes`
- Timeouts: `-request-timeout`, `-peer-timeout`, `-shutdown-timeout`
- HTTP hardening: `-trusted-proxies`, `-http-read-timeout`,
  `-http-read-header-timeout`, `-http-write-timeout`, `-http-idle-timeout`,
  `-http-max-header-bytes`

HTTP endpoints:

- `POST /api/exec` `{tenant, sql, timeout_ms?}`
- `POST /api/query` `{tenant, sql, timeout_ms?}`
- `POST /api/federated/query` `{tenant, sql, timeout_ms?, peer_timeout_ms?}`
- `GET /api/status`, `GET /api/cluster/status`
- `GET /healthz`, `GET /readyz`, `GET /metrics`

### [server/loadtest](server/loadtest/README.md)

HTTP load generator for `cmd/server`.

```bash
cd cmd/server && go build -o ../../bin/tinysql-loadtest ./loadtest
./bin/tinysql-loadtest -url http://127.0.0.1:8080/api/query -requests 10000 -concurrency 100
```

## [tinysqld](tinysqld/README.md)

Enterprise DBMS daemon: enterprise runtime profile with durable storage, job
scheduler, and a minimal HTTP API.

```bash
go build ./cmd/tinysqld
./tinysqld -data ./tinysqld-data -storage disk -tenant default -http 127.0.0.1:8088
./tinysqld -data ./tinysqld-data -storage disk -check   # validate configuration
```

Flags: `-data`, `-storage <disk|hybrid|index|wal|advanced_wal>`, `-tenant`,
`-http`, `-auth`, `-request-timeout`, `-check`

Endpoints: `GET /healthz`, `GET /readyz`, `GET /api/status`,
`POST /api/exec`, `POST /api/query`, `GET /api/catalog/tables`,
`GET /api/catalog/columns`, `GET /api/jobs`, `GET /api/job-history`,
`POST /api/jobs/run`

## [tinysql](tinysql/README.md)

SQLite-compatible CLI. Takes the database path as first argument
(`:memory:` for in-memory) and optional inline SQL as a positional argument.
Subcommands: `tables`, `schema`, `insert`, `query`, `export`.

```bash
go build ./cmd/tinysql
./tinysql mydb.dat
./tinysql mydb.dat "SELECT * FROM users"
```

Flags: `-tenant`, `-mode <column|list|csv|json|table>`, `-header`, `-echo`,
`-cmd <sql>`, `-batch`, `-output <file>`

## [sqltools](sqltools/README.md)

SQL utility toolkit.

```bash
go build ./cmd/sqltools
./sqltools beautify "select * from users where id=1"
```

Subcommands:

- `beautify [-upper=true] <sql>` — format a statement
- `validate <sql>` — check syntax
- `explain <sql>` — show a query execution plan
- `templates` — list built-in query templates
- `repl [-tenant=default]` — interactive shell with schema browsing and history

## [tinysqlpage](tinysqlpage/README.md)

HTTP server rendering SQL-driven pages: each URL path maps to a `.sql` file in
the pages directory, and results become HTML components in a template.

```bash
go build ./cmd/tinysqlpage
./tinysqlpage -addr :8080 -pages ./cmd/tinysqlpage/pages -seed ./cmd/tinysqlpage/sample_data.sql
```

Flags: `-addr <listen>`, `-pages <dir>`, `-seed <sql-file>`, `-css <file>`,
`-template <file>`. Health check: `GET /healthz`.

## [studio](studio/README.md)

Desktop GUI built with [Wails](https://wails.io/), requires the Wails CLI.

```bash
go install github.com/wailsapp/wails/v2/cmd/wails@latest
cd cmd/studio && wails build   # or: wails dev
```

## [wasm_browser](wasm_browser/README.md)

WebAssembly build for browsers; UI in `web/`.

```bash
cd cmd/wasm_browser && ./build.sh --build-only
cd cmd/wasm_browser && ./build.sh --serve   # then open http://localhost:8080

# manual
GOOS=js GOARCH=wasm go build -o web/tinySQL.wasm . && cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" web/
```

## [wasm_node](wasm_node/README.md)

WebAssembly build for Node.js, plus a Node runner.

```bash
cd cmd/wasm_node && ./build.sh --build-only
cd cmd/wasm_node && ./build.sh --run

# manual
GOOS=js GOARCH=wasm go build -o tinySQL.wasm . && cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./
node wasm_runner.js
node wasm_runner.js query "SELECT 1"
```

## [query_files](query_files/README.md)

Query CSV, JSON, and XML files with SQL, via web UI or CLI.

```bash
go build -o query_files ./cmd/query_files
./query_files -web -port 8080 -datadir ./data
./query_files -query "SELECT * FROM users" users.csv
```

## [query_files_wasm](query_files_wasm/README.md)

WASM build of `query_files`, and the source of the gh-pages playground at
https://simonwaldherr.github.io/tinySQL/. Includes an intro page, shareable
URL-hash demos, a mobile-optimized SQL editor, file imports, geodata recipes,
FTS/vector search examples, and result export.

```bash
cd cmd/query_files_wasm && ./build.sh --build-only
cd cmd/query_files_wasm && ./build.sh --serve
make build-gh-pages-demo   # static gh-pages artifacts
make update-gh-pages       # update and commit the gh-pages branch in a worktree
```

Opening `index.html` directly does not work; a local HTTP server is required
for the WASM MIME type.

## [catalog_demo](catalog_demo/README.md)

Demonstrates the catalog and job scheduler APIs by registering tables and
scheduling SQL jobs.

```bash
go build ./cmd/catalog_demo
./catalog_demo
```

## [debug](debug/README.md)

Parses and executes SQL against an in-memory database, printing results and
per-statement timing.

```bash
go build ./cmd/debug
./debug -sql "SELECT 1 + 1 AS result"
```

Flags: `-sql`, `-timing`, `-verbose`

## [fsql](fsql/README.md)

Treats the filesystem as a relational database: register named mounts and
query file metadata, text lines, CSV rows, and JSON rows with SQL.

```bash
cd cmd/fsql && go build -o fsql .
./fsql mount logs /var/log
./fsql --mount /var/log "SELECT path, size FROM files('root', true) WHERE ext = 'log'"
```

## [migrate](migrate/README.md)

Data pipeline CLI: import CSV/JSON into tinySQL, connect to external databases
(PostgreSQL, MySQL, SQLite, MS SQL), and transfer data between any combination
of sources and targets.

```bash
cd cmd/migrate && go build -o migrate .
migrate import-file -file users.csv -query "SELECT * FROM users"
migrate web
```
