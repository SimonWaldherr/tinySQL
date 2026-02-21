# TinySQL

[![CI](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/SimonWaldherr/tinySQL/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/1065449861.svg)](https://doi.org/10.5281/zenodo.17216339)
[![Go Report Card](https://goreportcard.com/badge/github.com/SimonWaldherr/tinySQL)](https://goreportcard.com/report/github.com/SimonWaldherr/tinySQL)
[![GoDoc](https://godoc.org/github.com/SimonWaldherr/tinySQL?status.svg)](https://godoc.org/github.com/SimonWaldherr/tinySQL)

## 🎥 Demo

[![Watch the video](https://img.youtube.com/vi/W28-aBk3BL0/hqdefault.jpg)](https://youtu.be/W28-aBk3BL0)

TinySQL is a lightweight, educational SQL database engine written in pure Go. It implements a comprehensive subset of SQL features using only Go's standard library, making it perfect for learning database internals and for applications that need a simple embedded SQL database.

## Quick start

### Install

```bash
go get github.com/SimonWaldherr/tinySQL@latest
```

### Use the engine directly

```go
package main

import (
    "context"
    "fmt"
    tsql "github.com/SimonWaldherr/tinySQL"
)

func main() {
    db := tsql.NewDB()

    p := tsql.NewParser(`CREATE TABLE users (id INT, name TEXT)`)
    st, _ := p.ParseStatement()
    tsql.Execute(context.Background(), db, "default", st)

    p = tsql.NewParser(`INSERT INTO users VALUES (1, 'Alice')`)
    st, _ = p.ParseStatement()
    tsql.Execute(context.Background(), db, "default", st)

    p = tsql.NewParser(`SELECT id, name FROM users`)
    st, _ = p.ParseStatement()
    rs, _ := tsql.Execute(context.Background(), db, "default", st)

    for _, row := range rs.Rows {
        fmt.Println(tsql.GetVal(row, "id"), tsql.GetVal(row, "name"))
    }
}
```

### Use with database/sql

```go
package main

import (
    "database/sql"
    "fmt"
    _ "github.com/SimonWaldherr/tinySQL/internal/driver"
)

func main() {
    db, _ := sql.Open("tinysql", "mem://?tenant=default")
    defer db.Close()

    db.Exec(`CREATE TABLE t (id INT, name TEXT)`)
    db.Exec(`INSERT INTO t VALUES (?, ?)`, 1, "Alice")

    row := db.QueryRow(`SELECT name FROM t WHERE id = ?`, 1)
    var name string
    _ = row.Scan(&name)
    fmt.Println(name)
}
```

## Run tests

```bash
# no cache
go test ./... -count=1

# with coverage output
go test -coverprofile=coverage.out ./...
```

## Available Tools (`cmd/`)

The `cmd/` directory contains ready-to-use binaries for common workflows. See [cmd/README.md](./cmd/README.md) for the full list and build instructions.

| Command | Description |
|---------|-------------|
| `demo` | Creates tables, inserts sample data, and runs example queries |
| `repl` | Interactive SQL REPL with multiple output formats |
| `server` | HTTP JSON API + gRPC server with optional peer federation |
| `tinysql` | SQLite-compatible CLI (file or in-memory databases) |
| `sqltools` | SQL formatter, validator, explain, and REPL |
| `tinysqlpage` | HTTP server that renders SQL-driven web pages |
| `studio` | Desktop GUI built with Wails |
| `wasm_browser` | tinySQL compiled to WebAssembly for browsers |
| `wasm_node` | tinySQL compiled to WebAssembly for Node.js |
| `query_files` | Query CSV / JSON / XML files with SQL (web UI + CLI) |
| `query_files_wasm` | WebAssembly build of query_files for the browser |
| `catalog_demo` | Demo of the catalog and job-scheduler APIs |
| `debug` | Development aid for testing BOOL column behavior |

## Goals (and non-goals)

- Lightweight, educational SQL engine in pure Go
- Useful for embeddings, demos, and learning database internals
- Not intended as a production-grade relational database

## Requirements

- Go 1.25+ (see go.mod)

## DSN (Data Source Name) Format

When using the database/sql driver:

- **In-memory database**: `mem://?tenant=<tenant_name>`
- **File-based database**: `file:/path/to/db.dat?tenant=<tenant_name>&autosave=1`

Parameters:
- `tenant` - Tenant name for multi-tenancy (required)
- `autosave` - Auto-save to file (optional, for file-based databases)

## Limitations

TinySQL is designed for educational purposes 

## Testing

Run the test suite:

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests multiple times to check consistency
go test -v -count=3 ./...
```

## Contributing

This is an educational project. Contributions that improve code clarity, add comprehensive examples, or enhance the learning experience are welcome.

## Educational Goals

TinySQL demonstrates:

- SQL parsing and AST construction
- Query execution and optimization basics
- Database storage concepts
- Go's database/sql driver interface
- 3-valued logic (NULL semantics)
- JSON data handling in SQL
- Multi-tenancy patterns

Perfect for computer science students, developers learning database internals, or anyone who wants to understand how SQL databases work under the hood.
