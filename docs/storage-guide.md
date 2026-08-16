# Storage & Persistence Guide

TinySQL separates the SQL engine from how data is persisted. All storage modes
share the same engine and `*DB`/`database/sql` API, so switching modes changes
only the `StorageConfig`/DSN, never application code.

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
Connection-local state (transactions, prepared statements, cursor state) stays
separate. A second `sql.Open`, including one with identical `mem://` text,
deliberately receives a separate in-memory database. Do not rely on a global
driver cache for sharing; pass one `*sql.DB` to the components that should
share data.

`SetDefaultDB` and `OpenWithDB` remain available for embedding. They apply only
to the legacy empty DSN (`OpenWithDB` calls `Open("")`); a named `mem://` or
`file:` DSN never inherits that default database.

DSN query options are URL-decoded, must occur once, and reject unknown or
malformed values:

| Option | Accepted value | Meaning |
|---|---|---|
| `tenant` | non-empty string | Tenant/catalog namespace |
| `autosave` | `0/1`, `true/false`, `yes/no`, `on/off` | Legacy GOB snapshot persistence |
| `pool_readers`, `pool_writers` | non-negative integer | Driver admission limit (`0` = no driver limit) |
| `busy_timeout` | Go duration or integer milliseconds | Wait bound for the driver pool |
| `mode` | `memory`, `disk`, `json`, `index`, `hybrid`, `wal`, `advanced_wal`, `paged_index`, `sqlite` | Storage backend |
| `max_memory_bytes` | bytes, `KiB`/`MiB`/`GiB`, or decimal `KB`/`MB`/`GB` | Hybrid/Index buffer-pool budget |
| `read_only` | strict boolean | Reject mutations and persistence actions |
| `sync_on_mutate`, `compress_files` | strict boolean | Storage behaviour |
| `checkpoint_every` | unsigned integer | WAL checkpoint cadence (transactions in `wal`; row-operation records in `advanced_wal`) |
| `checkpoint_interval` | non-negative Go duration | WAL checkpoint interval |
| `checkpoint_max_bytes` | size, or `-1` to disable | WAL size trigger |
| `wal_sync` | `full` (default) or `normal` | WAL commit flush strength; `normal` is ordinary fsync on every commit, not SQLite `synchronous=NORMAL` |

For a file-backed storage mode, all storage values are forwarded to
`storage.OpenDB(StorageConfig{...})`; they are not merely driver hints.

## Storage modes

| Mode | String | Notes |
|---|---|---|
| `ModeMemory` | `memory` | Default; in-memory, optional GOB snapshot via `Path` |
| `ModeDisk` | `disk` | One GOB file per table |
| `ModeJSON` | `json` | One readable JSON file per table |
| `ModeWAL` | `wal` | Older WAL mode; manual logging |
| `ModeAdvancedWAL` | `advanced_wal` | Row-level WAL logged automatically on writes; `compress_files` gzips the periodic checkpoint snapshot only, not the live log |
| `ModeIndex` | `index` | Schemas in memory, rows on disk |
| `ModeHybrid` | `hybrid` | LRU buffer pool with spill-to-disk behavior |
| `ModePagedIndex` | `paged_index` | Immutable page-oriented artifact for large read-mostly workloads |
| `ModeSQLite` | `sqlite` | Each table is a native table in a real `.sqlite` file, readable by any SQLite tool (requires the `sqliteimport` build tag) |

JSON mode example:

```go
db, err := tsql.OpenDB(tsql.StorageConfig{
    Mode: tsql.ModeJSON,
    Path: "./data/tinysql",
})
```

SQLite mode example (build with `-tags=sqliteimport`; `Path` is a file, not a
directory):

```go
db, err := tsql.OpenDB(tsql.StorageConfig{
    Mode: tsql.ModeSQLite,
    Path: "./data/tinysql.sqlite",
})
```

Or via a DSN: `file:./data/tinysql.sqlite?tenant=default&mode=sqlite`.

Columns whose values map cleanly onto SQLite's native storage classes
(integers, floats, strings, booleans, blobs) are stored as native
INTEGER/REAL/TEXT/BLOB columns, so `sqlite3`/DB Browser for SQLite/etc. can
query the file directly. Types with no native SQLite equivalent — Decimal,
UUID, time values, JSON, vectors, geometry — are stored as JSON-encoded text
in the same table, the same lossy-to-string convention `ModeJSON` already
uses for those types. Views, RBAC, and other catalog state are persisted to
a `<path>.catalog.gob` sidecar file next to the `.sqlite` file, the same
mechanism `ModeDisk`/`ModeJSON` use.

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
and `PRAGMA` still run. This pairs with
[RAG serving](./rag-guide.md#6-serving-and-performance-notes): `VEC_WARM`
prebuilds ANN indexes at startup instead of on the first query.

For `disk`, `json`, `index`, and `hybrid`, a read-only open requires an existing
directory and never creates a manifest, table file, checkpoint, or WAL file.
The disk backend independently rejects direct persistence calls too. `wal` and
`advanced_wal` are intentionally rejected in read-only mode at present because
their recovery code opens and can repair/truncate WAL sidecars; use a published
checkpointed artifact for serving instead.

### Bounded ModeIndex/Hybrid residency

On reopen, `ModeIndex` and `ModeHybrid` do not put backend-loaded tables in the
DB tenant catalog. Their only long-lived owner is the bounded buffer pool; an
oversized table is returned for the current statement but is not admitted to
that pool. This keeps memory from growing with every *different* table looked
up and makes `max_memory_bytes` a hard pool-admission bound.

The legacy table-file codec still decodes one complete table for a cache miss,
so `max_memory_bytes` bounds retained cache residency, not the temporary
allocation for one oversized table. It is safe against the former catalog leak,
but it is not yet a page/record-level MBTiles serving format.

### Serving MBTiles

For a tileset that fits in memory, tinySQL serves tiles at the same speed as
SQLite. The per-request query is a point lookup on
`(zoom_level, tile_column, tile_row)`; with a composite index on those columns it
is an index seek, measured at parity with SQLite's `:memory:` and roughly 4-5x
faster than a SQLite file — see
[BENCHMARKS.md](../BENCHMARKS.md#mbtiles-tile-serving-tinysql-vs-sqlite). Create
the index explicitly; a declared `PRIMARY KEY` does not create one:

```sql
CREATE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);
```

`cmd/tinysqld -tiles` then serves `/tiles/{tileset}/{z}/{x}/{y}.{ext}` plus
TileJSON, handling the XYZ-to-TMS row conversion.

For a tileset **larger than memory**, use `ModePagedIndex`. It is an immutable
page store, and a complete composite equality predicate — exactly a tile lookup —
resolves its B+Tree and materializes only the located row, so it never decodes the
whole table the way the `ModeIndex`/`ModeHybrid` GOB codec does:

```bash
# Build the artifact once (writable), then serve it read-only.
tinysqld -data /srv/tiles -storage paged_index -tiles
```

Measured on a 65,536-tile fixture with an 800-byte payload and a 32 MiB page
budget, a warm tile lookup is in the same range as a SQLite file — see
[BENCHMARKS.md](../BENCHMARKS.md). Two caveats worth knowing before relying on it:

- Each page-cache **miss** allocates a fresh page buffer, so a working set far
  larger than `max_memory_bytes` allocates ~11 KB per lookup. Size the page
  budget to the hot zoom levels rather than the whole tileset.
- Only *equality* predicates take the per-record path today. A range predicate on
  a paged table falls back to the full-table compatibility path, so the range
  seeks described above do not yet apply to `ModePagedIndex`.

`importer.OpenMBTiles` remains the option when you would rather query an existing
`.mbtiles` in place; its `Zooms` and `WithoutTileData` options read only the zoom
levels or only the tile index you need.

### B+Tree leaf/internal splits are byte-balanced, not count-balanced

A `ModePagedIndex` table with variable-size records — the `images` half of an
MBTiles projection is the case that surfaced it — used to split a full leaf
page at the entry-count midpoint (`len(merged) / 2`). With records of very
different encoded size, a count midpoint can leave one side of the split
holding several large records whose combined bytes exceed a fresh page, even
though the two sides together fit in two pages. A real regional tileset (158
MiB, 11,465 `images` rows, mixed BLOB sizes in the 1.4–2.5 KiB range) failed
importing into `ModePagedIndex` with exactly this shape of error:

```
insert row 10784: split right insert: btree page full: need 1569, have 1536 free
```

`internal/storage/pager/btree.go` now splits both leaf and internal pages by
*encoded byte footprint* (`leafSplitIndex`/`internalSplitIndex`): the split
point is chosen so both sides fit within one page's capacity, minimizing the
byte-size skew between them rather than the entry-count skew. The overflow
decision (inline value vs. a page-reference record) is a dedicated,
pageSize-only function, `leafEntryNeedsOverflow`, so it never depends on how
full any particular page happens to be at insert time — the same key/value
pair overflows (or doesn't) the same way regardless of insertion order. A
related gap in the same area is also fixed: replacing the *sole* record in a
single-entry leaf with a larger value used to be handled by the same
count-based split path and could fail outright (a one-entry set has no second
side to split into); an oversized replace or insert first tries a from-scratch
compaction of the leaf's live entries — which reclaims dead space earlier
in-place updates never free — and only allocates a sibling page if the live
content genuinely does not fit one page.

**File-format compatibility:** this is a write-path *algorithm* change, not a
page-layout change. Every on-disk structure — page header, slotted-page
layout, leaf/internal record encoding, overflow-page chains, the free list,
the superblock — is byte-for-byte identical to before; `CurrentFormatVersion`
(`internal/storage/pager/superblock.go`) is unchanged. A `paged_index`
artifact published by an older tinySQL build opens and reads correctly under
the fixed code, and an artifact written by the fixed code reads correctly
under older code too — the fix only changes *which* keys a writer places on
which page during a split, never how a page's bytes are interpreted. There is
nothing to migrate; rebuilding an artifact is only useful to get the more
balanced page layout itself (marginally better fill and fewer future splits),
not for correctness.

Regression coverage: `internal/storage/pager/btree_split_regression_test.go`
(exact boundary sizes from the report, all three key orders, replace/delete/
insert cycles checked for leaked overflow pages, and multi-level internal-split
invariants) and `internal/engine/paged_index_mbtiles_regression_test.go` (the
same failure reproduced and fixed at the SQL/engine layer, including a real
`UPDATE`/`DELETE`/`INSERT` sequence against overflow-sized BLOBs and a
durable close + read-only reopen). Read-path performance for the MBTiles
`map`→`tile_id`→`images` access shape, including size-class-isolated,
concurrent-reader and open/reopen benchmarks, lives in
[BENCHMARKS.md](../BENCHMARKS.md#mbtiles-import-a-tileset-larger-than-memory).
