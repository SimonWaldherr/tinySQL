# tinySQL Command Variants

This repo ships multiple binaries under `cmd/` for different use-cases. Each
tool has its own README with full documentation — click the links below.

| Tool | README |
|------|--------|
| demo | [README](demo/README.md) |
| repl | [README](repl/README.md) |
| server | [README](server/README.md) |
| tinysql | [README](tinysql/README.md) |
| sqltools | [README](sqltools/README.md) |
| tinysqlpage | [README](tinysqlpage/README.md) |
| studio | [README](studio/README.md) |
| wasm_browser | [README](wasm_browser/README.md) |
| wasm_node | [README](wasm_node/README.md) |
| query_files | [README](query_files/README.md) |
| query_files_wasm | [README](query_files_wasm/README.md) |
| catalog_demo | [README](catalog_demo/README.md) |
| debug | [README](debug/README.md) |
| fsql | [README](fsql/README.md) |
| migrate | [README](migrate/README.md) |

---

- demo
  - A simple demo that creates tables, inserts sample data, and runs example queries.
  - Build: `go build ./cmd/demo`
  - Run: `./demo -dsn "mem://?tenant=default"`

- repl
  - Interactive SQL REPL on top of database/sql. Supports multiple output formats (table, csv, tsv, json, yaml, markdown), echo mode, and optional HTML output.
  - Build: `go build ./cmd/repl`
  - Run: `./repl -dsn "mem://?tenant=default"`
  - Flags: `-dsn`, `-echo`, `-format <table|csv|tsv|json|yaml|markdown>`, `-beautiful`, `-html`, `-errors-only`

- server
  - HTTP JSON API and gRPC (JSON codec) server with optional federation across peers.
  - Build: `go build ./cmd/server`
  - Run: `./server -http :8080 -grpc :9090 -dsn "mem://?tenant=default" -peers "host1:9090,host2:9090"`
  - Flags:
    - Core: `-dsn`, `-http <addr>`, `-grpc <addr>`, `-auth <token>`, `-peers <addr,...>`, `-tenant <name>`, `-v`
    - TLS: `-tls-min-version`, `-http-tls-cert`, `-http-tls-key`, `-grpc-tls-cert`, `-grpc-tls-key`, `-peer-tls`, `-peer-tls-ca`, `-peer-tls-server-name`, `-peer-tls-skip-verify`
    - Limits: `-max-body-bytes`, `-max-sql-bytes`, `-grpc-max-recv-bytes`, `-grpc-max-send-bytes`
    - Timeouts: `-request-timeout`, `-peer-timeout`, `-shutdown-timeout`
    - HTTP hardening: `-trusted-proxies`, `-http-read-timeout`, `-http-read-header-timeout`, `-http-write-timeout`, `-http-idle-timeout`, `-http-max-header-bytes`
  - HTTP Endpoints:
    - POST /api/exec {tenant, sql}
    - POST /api/query {tenant, sql}
    - GET  /api/status
    - POST /api/federated/query {tenant, sql}
    - GET  /healthz
    - GET  /readyz
    - GET  /metrics

- tinysql
  - SQLite-compatible CLI with file-based and in-memory database support. Accepts a filename as the database path (`:memory:` for in-memory), optional inline SQL as a positional argument, and supports utility subcommands (`tables`, `schema`, `insert`, `query`, `export`).
  - Build: `go build ./cmd/tinysql`
  - Run interactive REPL: `./tinysql mydb.dat`
  - Run inline SQL: `./tinysql mydb.dat "SELECT * FROM users"`
  - Flags: `-tenant`, `-mode <column|list|csv|json|table>`, `-header`, `-echo`, `-cmd <sql>`, `-batch`, `-output <file>`

- sqltools
  - SQL utility toolkit: format (beautify), validate, explain, list templates, and an interactive REPL with schema browsing and query history.
  - Build: `go build ./cmd/sqltools`
  - Run: `./sqltools beautify "select * from users where id=1"`
  - Subcommands:
    - `beautify [-upper=true] <sql>` – format a SQL statement
    - `validate <sql>` – check SQL syntax
    - `explain <sql>` – show a query execution plan
    - `templates` – list built-in query templates
    - `repl [-tenant=default]` – interactive SQL tools shell

- tinysqlpage
  - HTTP server that renders SQL-driven web pages. Each URL path maps to a `.sql` file in the pages directory; query results are turned into HTML components and served via a template.
  - Build: `go build ./cmd/tinysqlpage`
  - Run: `./tinysqlpage -addr :8080 -pages ./cmd/tinysqlpage/pages -seed ./cmd/tinysqlpage/sample_data.sql`
  - Flags: `-addr <listen>`, `-pages <dir>`, `-seed <sql-file>`, `-css <file>`, `-template <file>`
  - Health check: GET /healthz

- studio
  - Desktop GUI application built with [Wails](https://wails.io/). Provides a native window for running SQL queries against tinySQL.
  - Requires Wails CLI: `go install github.com/wailsapp/wails/v2/cmd/wails@latest`
  - Build: `cd cmd/studio && wails build`
  - Dev mode: `cd cmd/studio && wails dev`

- wasm_browser
  - Builds tinySQL to WebAssembly for browsers. A modern UI is provided in `web/`.
  - Build only: `cd cmd/wasm_browser && ./build.sh --build-only`
  - Build & serve: `cd cmd/wasm_browser && ./build.sh --serve` then open http://localhost:8080
  - Manual build: `cd cmd/wasm_browser && GOOS=js GOARCH=wasm go build -o web/tinySQL.wasm . && cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" web/`

- wasm_node
  - Builds tinySQL to WebAssembly for Node.js and provides a Node runner.
  - Build only: `cd cmd/wasm_node && ./build.sh --build-only`
  - Build & run demo: `cd cmd/wasm_node && ./build.sh --run`
  - Manual build: `cd cmd/wasm_node && GOOS=js GOARCH=wasm go build -o tinySQL.wasm . && cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./`
  - Run: `node wasm_runner.js` or `node wasm_runner.js query "SELECT 1"`

- query_files
  - Query CSV, JSON, and XML files using SQL via a web UI or CLI. See [cmd/query_files/README.md](./query_files/README.md) for full documentation.
  - Build: `go build -o query_files ./cmd/query_files`
  - Web mode: `./query_files -web -port 8080 -datadir ./data`
  - CLI mode: `./query_files -query "SELECT * FROM users" users.csv`

- query_files_wasm
  - WebAssembly build of the query_files tool for use directly in the browser. Exposes `importFile`, `executeQuery`, `listTables`, and `exportResults` JavaScript functions.
  - Build: `cd cmd/query_files_wasm && ./build.sh --build-only`
  - Build & serve: `cd cmd/query_files_wasm && ./build.sh --serve`
  - Open `index.html` in a browser (requires a local HTTP server due to WASM MIME type).

- catalog_demo
  - Demonstrates the tinySQL catalog and job scheduler APIs by registering tables and scheduling SQL jobs.
  - Build: `go build ./cmd/catalog_demo`
  - Run: `./catalog_demo`

- debug
  - SQL diagnostic tool: parse and execute SQL statements against an in-memory database, print results and per-statement timing. See [cmd/debug/README.md](./debug/README.md).
  - Build: `go build ./cmd/debug`
  - Run: `./debug -sql "SELECT 1 + 1 AS result"`
  - Flags: `-sql`, `-timing`, `-verbose`

- fsql
  - Treat the filesystem as a relational database. Register named mounts and query file metadata, text lines, CSV rows, and JSON rows using SQL. See [cmd/fsql/README.md](./fsql/README.md).
  - Build: `cd cmd/fsql && go build -o fsql .`
  - Register mount: `./fsql mount logs /var/log`
  - Query: `./fsql --mount /var/log "SELECT path, size FROM files('root', true) WHERE ext = 'log'"`

- migrate
  - Data pipeline CLI: import CSV/JSON into tinySQL, connect to external databases (PostgreSQL, MySQL, SQLite, MS SQL), and transfer data between any combination of sources and targets. See [cmd/migrate/README.md](./migrate/README.md).
  - Build: `cd cmd/migrate && go build -o migrate .`
  - Import and query: `migrate import-file -file users.csv -query "SELECT * FROM users"`
  - Web UI: `migrate web`

- server/loadtest
  - Lightweight HTTP load generator for `cmd/server`.
  - Build: `go build -o bin/tinysql-loadtest ./cmd/server/loadtest`
  - Run: `./bin/tinysql-loadtest -url http://127.0.0.1:8080/api/query -requests 10000 -concurrency 100`

Notes:
- The in-memory DSN is `mem://?tenant=default`. Files can be persisted using `file:/path/to/db.dat?tenant=<name>&autosave=1`.
- `repl` and `tinysql` rely on the internal `database/sql` driver registration (`_ ".../internal/driver"`).
- `server` uses `internal/storage` directly with the same DSN conventions.
