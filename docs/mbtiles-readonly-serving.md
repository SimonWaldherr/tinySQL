# Read-only `database/sql` and MBTiles-like artifacts

This note describes the ownership and memory guarantees of the current
tinySQL driver/storage path for small read-only map artifacts. It does not
change the recommendation that SQLite remains the production default for
standard, large MBTiles files.

## Connection ownership

`tinysql` implements `database/sql/driver.DriverContext`. Each call to
`sql.Open("tinysql", dsn)` produces one Connector. The Connector lazy-opens one
`server` and one `storage.DB`; all physical connections created by that one
`*sql.DB` share it. Transactions, prepared statements and cursors are still
connection-local.

Named `mem://` databases are isolated per `sql.Open`, even when their DSN text
is identical. The legacy embedding API (`SetDefaultDB`, `CurrentDefaultDB`,
`OpenWithDB`) is preserved for the empty DSN only and does not leak into named
DSNs.

## Storage and read-only rules

All driver storage options are URL-query parameters and are validated before
opening storage. Relevant options include `mode`, `tenant`, `autosave`, pool
limits, `busy_timeout`, `max_memory_bytes`, `read_only`, `sync_on_mutate`,
`compress_files`, and all checkpoint controls. Size values accept forms such as
`64MiB` and `512MiB`.

For `disk`, `json`, `hybrid`, and `index`, a read-only open requires an existing
artifact directory. The backend itself rejects save/delete/sync persistence
operations, and driver connection close does not autosave. Read-only WAL modes
are rejected because their recovery implementation otherwise needs a writable
WAL sidecar.

## Residency behaviour

`ModeIndex` and `ModeHybrid` no longer retain a backend-loaded table in the
tenant catalog. The bounded BufferPool is the long-lived owner. Tables too
large for `max_memory_bytes` are returned only for the current caller and are
not admitted to the pool.

This fixes growth caused by looking up many different tables, but does **not**
turn the legacy GOB table file into a record store: one cache miss still decodes
the complete table. A future immutable page/record format must provide
on-disk secondary-index pages, row locators and out-of-line BLOB pages before
tinySQL can guarantee bounded peak heap for a multi-gigabyte `images` table.

## MBTiles-like SQL shape

The covered shape is:

```sql
CREATE UNIQUE INDEX idx_map_zxy
ON map(zoom_level, tile_column, tile_row);

SELECT tile_id FROM map
WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;

SELECT tile_data FROM images WHERE tile_id = ?;
```

The existing in-table materialized composite index yields `INDEX POINT SEEK` in
`EXPLAIN`; BLOB scans return defensive `[]byte` copies.

## Reproducible warm-read measurement

Run:

```bash
go test ./internal/driver -run '^$' \
  -bench '^BenchmarkReadOnlyMBTilesLikeTwoPointReads$' \
  -benchmem -benchtime=1s -count=3
```

On Darwin/arm64, Apple M2 Max, Go 1.26.5, commit `c4d4c9b` plus this working
change, the synthetic 10,000-tile, 1 KiB-payload benchmark measured 3.299 µs,
3.381 µs and 3.331 µs per warm two-point-read sequence (5,696 B/op,
60 allocs/op). These are warm-cache measurements, not a SQLite comparison and
not a p95/p99 result.
