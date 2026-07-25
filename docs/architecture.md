# Architecture

How tinySQL is put together, what each layer owns, and the invariants that are
easy to break. Read this before changing anything under `internal/`.

For where files live, see [repository-structure.md](./repository-structure.md).

## Layers

Each layer knows only about the one below it.

```
   your code                    tinysql.ParseSQL / Execute / builder
        |                       driver (database/sql)
        v
   internal/driver              connections, transactions, placeholder binding
        |                       owns: which *storage.DB is live, writer/reader gating
        v
   internal/engine              lexer -> parser -> Statement -> execute
        |                       owns: SQL semantics, planning, expression evaluation
        v
   internal/storage             DB, Table, catalog, WAL, backends
                                owns: what is in memory, what is on disk, durability
```

`internal/storage` never parses SQL, and the engine reaches it only through
exported methods on `*storage.DB`.

The engine touches the filesystem in exactly one place, and it is worth knowing
about: `io_functions.go` implements the SQL-callable `file()` and `http()`
functions. Those give any SQL statement the process's own filesystem and network
reach, which is fine for a trusted embedder and is not fine for a deployment that
runs queries from untrusted callers. Nothing else in the engine opens a file.

## Layer responsibilities

### internal/storage

| Concern | Where |
|---|---|
| The database: tenants, tables, per-database switches | `db.go` |
| One table's rows, schema, statistics, dirty tracking | `table.go` |
| Column types, SQLite affinities, constraints | `column_types.go` |
| Opening: mode-specific backend/WAL/checkpoint construction | `open.go` |
| Finding, adding, removing tables in a tenant | `tables.go` |
| Clones and snapshots handed to the driver and engine | `snapshot.go` |
| On-disk formats and catalog serialisation | `disk_format.go`, `disk_table.go` |
| Whole-database snapshots (GOB) | `checkpoint_file.go` |
| Write-ahead log for `ModeWAL` | `wal_manager.go`, `wal_changes.go` |
| Row-level write-ahead log for `ModeAdvancedWAL` | `wal_advanced.go` |
| Views, triggers, jobs, indexes, RBAC | `catalog.go`, `rbac.go` |
| Health, sync, close, evict | `lifecycle.go` |
| Disk/JSON/hybrid/index/paged backends | `backend_*.go`, `pager/` |

A `DB` is a map of tenant name to a map of table name to `*Table`. A `Table`
holds its rows as `[][]any` — no per-row struct, no page layout. That is the
central design choice: values stay native Go values from storage through
evaluation to the result, so reads need no unmarshalling.

### internal/engine

Statement execution is two stages. `executeStatement` (`exec_statement.go`) owns
everything a statement needs regardless of what it does: authorization, the
content lock, the rollback point, panic isolation, auditing, and write-ahead
logging. `execStmt` (`exec_dispatch.go`) then switches on the statement type and
calls a handler that only has to implement SQL semantics.

Expression evaluation exists twice, on purpose:

- `eval_expr.go` evaluates against a `Row` (a `map[string]any` carrying both
  `col` and `table.col` keys). General, handles everything.
- `exec_raw_*.go` evaluates against a stored `[]any` row, with the column
  positions resolved once up front. Much faster, handles a subset.

The `exec_fastpath_*.go` files compile a query into the second form when they
can. **A fast path must decline, never guess**: every builder returns
`(plan, ok)` or a nil filter, and `ok == false` falls back to the general path.
Returning a plan that is subtly wrong is the one failure mode that cannot be
caught by a test that does not already know the answer.

### internal/driver

One `sql.Open` creates one connector, which owns one `*storage.DB`. Every
physical connection from that `*sql.DB` shares it; transactions and prepared
statements stay per-connection.

Writes are serialised by a writer slot plus `server.mu`; reads take the read
side. An autocommit statement runs against the live database. An explicit
transaction runs against a private snapshot — a *shadow* — that is merged back
at `COMMIT`.

## The life of a statement

```
sql.DB.Exec("UPDATE t SET x = ? WHERE id = ?", ...)
  -> conn.ExecContext            bind placeholders into SQL text
  -> conn.execSQL                parse (cached for SELECT/EXPLAIN)
  -> conn.execStatement          classify read vs write, acquire the writer slot
  -> engine.Execute
       -> executeStatement       permission check
                                 content lock (write)
                                 metadata pre-image for WAL diffing
                                 rollback snapshot
       -> execStmt               dispatch
       -> executeUpdate          the fast path, or the general path
                                 report each changed row: Table.MarkRowUpdated
       -> maybeLogToWALManager   diff the pre-image, append, fsync
                                 (on failure: restore the snapshot, return error)
  -> server.persist              flush to the backend; failure fails the statement
```

Inside `BEGIN … COMMIT` the middle changes: statements run against the shadow
and are not logged as they go. `conn.commitTx` collects the whole transaction's
table changes, logs them once against the live database, applies them, and
publishes the shadow's catalog.

## Invariants

These are load-bearing. Each one was broken at some point and cost data.

**One write-ahead logging site.** Exactly one place appends a statement to
`WALManager`: `maybeLogToWALManager` in `wal_logging.go`. The driver logs only
whole transactions, at `COMMIT`. When both logged, every autocommit write went
to the log twice.

**A shadow never logs.** `DB.StatementWAL` returns nil for a shadow, so a
statement inside an uncommitted transaction cannot reach the log. Otherwise a
rolled-back statement stays on disk looking committed and recovery resurrects it.
`ModeAdvancedWAL` instead joins one ambient transaction per SQL transaction
(`BeginAmbientWALTx`), committed or aborted as a unit.

**A clone carries runtime state.** Every clone goes through
`DB.copyRuntimeState`. Hand-copying fields is how a promoted clone once lost its
write-ahead log — after which a fresh `ModeWAL` database never logged again — and
how a transaction shadow once lost the catalog, so triggers silently did not fire.

**Log before publish.** A change is appended and fsynced before it is
acknowledged. If the append fails, the statement's rollback point is restored, so
memory never holds a change the log does not.

**Durability failures are errors.** `server.persist` returns them and both
acknowledge points propagate. Never report success for a write that is not
durable.

**Checkpoints carry a watermark.** `WALManager.Checkpoint` writes the log
position the snapshot reflects into the snapshot, then truncates. Recovery skips
records at or below it. Without that, a crash between the two replayed
already-checkpointed deltas and duplicated rows. Consequently `Seq` keeps
increasing across checkpoints — resetting it would make later records compare
below an older watermark and vanish.

**Dirty tracking is fail-safe.** `Table.dirtyFrom` and `Table.dirtyRows` let the
log write a delta instead of a whole table. Every mutation that cannot describe
itself that way must call `MarkDirtyFrom`, which gives up the hint. The worst
outcome of a missed call is one oversized record; the worst outcome of a wrong
hint is lost data, so the fallback direction matters.

**The catalog revision drives commit.** `CatalogManager.revision` increments on
every mutation, through `lockWrite`/`unlockWrite`. `conn.commitTx` uses it as a
gate and then compares contents, because ordinary DML takes the catalog's write
lock without changing anything there.

**Lock order.** `DB.contentMu` (whole statement, taken by `executeStatement`),
then `DB.mu` (the tenant/table map), then `catalogMu`. Never the reverse.
`contentMu` is coarse on purpose: one choke point is auditable, per-table locking
was not.

## Storage modes

| Mode | Data | Durability |
|---|---|---|
| `ModeMemory` | RAM | explicit `SaveToFile`, or `Close` with a path |
| `ModeWAL` | RAM | log fsynced per committed statement, periodic full checkpoint |
| `ModeAdvancedWAL` | RAM | row-level log, transaction begin/commit/abort records |
| `ModeDisk` | one GOB file per table, loaded on demand | flushed on `Sync`/`Close` |
| `ModeJSON` | as `ModeDisk`, human-readable | flushed on `Sync`/`Close` |
| `ModeIndex` | schemas in RAM, rows on disk | flushed on `Sync`/`Close` |
| `ModeHybrid` | LRU buffer pool with a memory budget | flushed on `Sync`/`Close` |
| `ModePagedIndex` | rows and indexes in separate B+Trees | immutable artifact, read-mostly |

`ModeWAL` is the mode to compare against SQLite: an acknowledged write survives
a crash. See [BENCHMARKS.md](../BENCHMARKS.md) for what that costs.

## Where to look

| To change | Start at |
|---|---|
| SQL syntax | `internal/engine/lexer.go`, `parser.go` |
| What a statement does | `exec_dispatch.go`, then the `exec_*.go` for that statement |
| A scalar function | `builtin_registry.go` to register, `builtin_*.go` to implement |
| An aggregate | `eval_aggregate.go` |
| A window function | `eval_window.go` |
| Why a query is slow | `exec_plan.go` (planning, index choice), `exec_fastpath_*.go` |
| NULL / comparison / ordering rules | `value_semantics.go` |
| Type coercion and affinities | `coerce.go` |
| Durability | `internal/storage/wal_manager.go`, `wal_advanced.go` |
| The on-disk format | `internal/storage/disk_format.go` |
| Transactions and the pool | `internal/driver/driver.go` |
| Vector search | `internal/engine/vector_*.go` |
| Full-text search | `internal/engine/fts.go` |
