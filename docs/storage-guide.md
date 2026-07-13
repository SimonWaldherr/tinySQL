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

### Connection ownership and DSN validation

`tinysql` implements `database/sql/driver.DriverContext`. One call to
`sql.Open("tinysql", dsn)` creates one lazy Connector and therefore exactly one
shared tinySQL `storage.DB` for all physical connections in that `*sql.DB`.
Connection-local state (transactions, prepared statements and cursor state)
remains separate. A second `sql.Open`, including one with identical `mem://`
text, deliberately receives a separate in-memory database. Do not rely on a
global driver cache for sharing; pass one `*sql.DB` to the application
components which should share data.

`SetDefaultDB` and `OpenWithDB` remain available for embedding. They apply only
to the legacy empty DSN (`OpenWithDB` calls `Open("")`); a named `mem://` or
`file:` DSN never inherits that default database.

DSN query options are URL-decoded, must occur once, and reject unknown or
malformed values. Supported options are:

| Option | Accepted value | Meaning |
|---|---|---|
| `tenant` | non-empty string | Tenant/catalog namespace |
| `autosave` | `0/1`, `true/false`, `yes/no`, `on/off` | Legacy GOB snapshot persistence |
| `pool_readers`, `pool_writers` | non-negative integer | Driver admission limit (`0` = no driver limit) |
| `busy_timeout` | Go duration or integer milliseconds | Wait bound for the driver pool |
| `mode` | `memory`, `disk`, `json`, `index`, `hybrid`, `wal`, `advanced_wal` | Storage backend |
| `max_memory_bytes` | bytes, `KiB`/`MiB`/`GiB`, or decimal `KB`/`MB`/`GB` | Hybrid/Index buffer-pool budget |
| `read_only` | strict boolean | Reject mutations and persistence actions |
| `sync_on_mutate`, `compress_files` | strict boolean | Storage behaviour |
| `checkpoint_every` | unsigned integer | WAL checkpoint transaction count |
| `checkpoint_interval` | non-negative Go duration | WAL checkpoint interval |
| `checkpoint_max_bytes` | size, or `-1` to disable | WAL size trigger |

For a file-backed storage mode, all storage values are forwarded to
`storage.OpenDB(StorageConfig{...})`; they are not merely driver hints.

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

For `disk`, `json`, `index`, and `hybrid`, a read-only open requires an existing
directory and never creates a manifest, table file, checkpoint, or WAL file.
The disk backend independently rejects direct persistence calls too. `wal` and
`advanced_wal` are intentionally rejected in read-only mode at present because
their recovery code opens and can repair/truncate WAL sidecars; use a published
checkpointed artifact for serving instead.

### Bounded ModeIndex/Hybrid residency

On reopen, `ModeIndex` and `ModeHybrid` no longer put backend-loaded tables in
the DB tenant catalog. Their only long-lived owner is the bounded buffer pool;
an oversized table is returned for the current statement but is not admitted to
that pool. This prevents memory from growing with every *different* table
looked up and makes `max_memory_bytes` a hard pool-admission bound.

The current legacy table-file codec still decodes one complete table for a
cache miss. Consequently `max_memory_bytes` bounds retained cache residency,
not the temporary allocation for one oversized table. It is safe against the
former catalog leak, but it is not yet a page/record-level MBTiles serving
format. For multi-gigabyte `images` tables, use SQLite for production MBTiles
serving until the pager-native immutable index format is introduced.
