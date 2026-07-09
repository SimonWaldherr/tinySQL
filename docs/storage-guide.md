# Storage & Persistence Guide

TinySQL separates the SQL engine from how data is persisted. All storage
modes share the same engine and `*DB`/`database/sql` API, so switching modes
never changes application code — only the `StorageConfig`/DSN.

See also: [Developer Integration Guide](./developer-integration.md) for
`database/sql` connection pooling, timeouts, and config patterns beyond the
DSNs below.

## `database/sql` driver

```go
import (
    "context"
    "database/sql"

    _ "github.com/SimonWaldherr/tinySQL/driver"
)

func open() (*sql.DB, error) {
    return sql.Open("tinysql", "mem://?tenant=default")
}

func run(db *sql.DB) error {
    _, err := db.ExecContext(context.Background(), `CREATE TABLE t (id INT, name TEXT)`)
    return err
}
```

Common DSNs:

| DSN | Use |
|---|---|
| `mem://?tenant=default` | In-memory database |
| `file:/path/to/db.gob?tenant=default&autosave=1` | GOB snapshot file |
| `file:/path/to/dbdir?tenant=default&mode=json` | JSON table files |
| `file:/path/to/dbdir?tenant=default&mode=advanced_wal` | Row-level WAL mode |

External projects should import `github.com/SimonWaldherr/tinySQL/driver`, not
`internal/driver`.

## Storage modes

| Mode | String | Notes |
|---|---|---|
| `ModeMemory` | `memory` | Default; in-memory, optional GOB snapshot via `Path` |
| `ModeDisk` | `disk` | One GOB file per table |
| `ModeJSON` | `json` | One readable JSON file per table |
| `ModeWAL` | `wal` | Older WAL mode; manual logging |
| `ModeAdvancedWAL` | `advanced_wal` | Row-level WAL logged automatically on writes |
| `ModeIndex` | `index` | Schemas in memory, rows on disk |
| `ModeHybrid` | `hybrid` | LRU buffer pool with spill-to-disk behavior |

JSON mode example:

```go
db, err := tsql.OpenDB(tsql.StorageConfig{
    Mode: tsql.ModeJSON,
    Path: "./data/tinysql",
})
```

## Read-only serving

Load once (e.g. a nightly bulk import), then reopen the same snapshot
read-only for serving traffic:

```go
// Load phase: write a snapshot.
db, _ := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeMemory, Path: "./data/db.gob"})
// ... bulk INSERT/UPDATE via tsql.Execute ...
db.Close()

// Serve phase: reopen the same snapshot read-only.
serveDB, _ := tsql.OpenDB(tsql.StorageConfig{
    Mode:     tsql.ModeMemory,
    Path:     "./data/db.gob",
    ReadOnly: true,
})
defer serveDB.Close()

warmStmt, _ := tsql.ParseSQL(`SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`)
tsql.Execute(context.Background(), serveDB, "default", warmStmt)
```

`ReadOnly` rejects `INSERT`, `UPDATE`, `DELETE`, and DDL. `SELECT`, `EXPLAIN`,
and `PRAGMA` still run. This pattern pairs well with [RAG serving](./rag-guide.md#6-serving-and-performance-notes):
`VEC_WARM` prebuilds ANN indexes once at startup instead of on the first query.
