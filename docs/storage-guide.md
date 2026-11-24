# Storage & Persistence Guide

tinySQL separates the SQL engine from how data is persisted. All storage modes
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

In an explicit `advanced_wal` transaction, a statement that fails after
attempting a row-log write makes the whole transaction rollback-only. Roll
back and retry the transaction. A subsequent COMMIT discards the pending
changes and returns an error. A failure before row logging, such as an
unknown table, leaves the transaction usable.

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
| `skip_search_index_persistence` | strict boolean | Keep FTS/vector index images out of table files (`disk`/`json`/`hybrid`/`index`) |
| `row_block_files` | strict boolean | Write table files as independent CRC-checked row blocks (`disk`/`hybrid`/`index`, unencrypted) |
| `checkpoint_every` | unsigned integer | WAL checkpoint cadence (transactions in `wal`; row-operation records in `advanced_wal`) |
| `checkpoint_interval` | non-negative Go duration | WAL checkpoint interval |
| `checkpoint_max_bytes` | size, or `-1` to disable | WAL size trigger |
| `wal_sync` | `full` (default) or `normal` | WAL commit flush strength; `normal` is ordinary fsync on every commit, not SQLite `synchronous=NORMAL` |

For a file-backed storage mode, all storage values are forwarded to
`storage.OpenDB(StorageConfig{...})`; they are not merely driver hints.

## Storage modes

A "mode" here means exactly one thing: `StorageMode` picks which backend
manages where table data lives (RAM, disk, or a bounded mix) and how it's
persisted. Everything else that sounds like a mode — `read_only`, `wal_sync`,
checkpoint cadence, `compress_files`, encryption — is a separate, orthogonal
setting layered on top of whichever backend you pick; see
["Orthogonal settings"](#orthogonal-settings-not-modes) below. There are nine
`StorageMode` values, which is more than most projects need to know about at
once — the decision guide below exists so you don't have to read all nine
doc comments to pick one.

### Which mode should I use?

Work through these questions in order; stop at the first one that applies.

1. **Does the whole dataset comfortably fit in RAM, and would losing
   unsaved changes on a crash be acceptable** (a short-lived process, a test,
   an analytics job, a cache you can rebuild)? → **`ModeMemory`**. Fastest
   mode, and the right default unless a later question says otherwise. Add
   `Path` if you want an explicit snapshot on `Close`/`SaveToFile`, but that's
   not crash durability — see the next question.
2. **Same as above, but a crash must never lose a committed write?** →
   **`ModeAdvancedWAL`** (row-level WAL, full ACID durability, point-in-time
   recovery — the mode to reach for by default when durability matters).
   `ModeWAL` (whole-table-diff, periodic checkpoint) still exists for
   compatibility with older setups, but has no advantage over AdvancedWAL for
   new work.
3. **Is the dataset bigger than comfortably fits in RAM?**
   - Read-mostly, built or rebuilt as a batch (tile serving, a published
     search index, a nightly export)? → **`ModePagedIndex`**. An exact-match
     index seek loads only the B+Tree pages it touches instead of decoding a
     whole table file.
   - Write-heavy or an unpredictable access pattern, and you want tinySQL to
     manage a memory budget automatically? → **`ModeHybrid`** (LRU buffer
     pool; hot tables stay resident, cold ones spill to disk under
     `max_memory_bytes`).
   - You mainly want to avoid holding every table's *rows* in RAM at once,
     and can accept a table being decoded whole on each cold access? →
     **`ModeIndex`** (schema stays resident, rows load on demand) or
     **`ModeDisk`** (nothing stays resident; simplest of the disk-backed
     modes).
4. **Do the on-disk files need to be human-readable, diffable, or
   hand-editable** (debugging, version control, a non-Go tool reading them)?
   → **`ModeJSON`** instead of `ModeDisk` — same lazy-load behavior, JSON
   instead of GOB, at the cost of larger files and some types (Decimal, UUID)
   round-tripping as plain strings.
5. **Does the file need to open in *other* SQLite tools** (the `sqlite3`
   CLI, DB Browser for SQLite, a BI tool that speaks SQLite natively)? →
   **`ModeSQLite`** (requires the `sqliteimport` build tag).

Still unsure? Start with `ModeMemory` (or `ModeAdvancedWAL` if a crash must
not lose data), and only move to `Hybrid`/`Index`/`PagedIndex` once you've
actually measured that the dataset doesn't fit in memory — most applications
never need to.

### Pager crash recovery

The page-based pager replays the newest committed WAL image for each page,
chosen by its log sequence number (LSN). Aborted and uncommitted images and
images at or below the checkpoint watermark are ignored. Repeated updates to
one page therefore require only one recovery write and cannot overwrite the
latest committed version through unordered transaction iteration.

After reopening a checkpointed database, new WAL sequence numbers advance past
the persisted checkpoint even when the WAL is empty. This prevents later
recovery from skipping newly committed changes as if they were already
checkpointed. The file format is unchanged. This is the pager's WAL protocol;
the `wal` and `advanced_wal` storage modes have separate recovery paths.

Regression tests simulate commits followed by a crash without checkpointing,
including the sequence close, reopen, commit, crash, reopen:

```sh
go test ./internal/storage/pager -run TestRecovery -count=3
go test -race ./internal/storage/pager -run TestRecovery -count=3
```

### Online snapshots and backups

`SaveToFile`, `SaveToWriter`, and `SaveToBytes` capture tables and catalog as
one detached image. The database read lock is held only while copying that
image; GOB encoding, optional gzip compression, filesystem flushes, and a slow
`io.Writer` run after the lock is released. `SaveToFile` writes a unique
temporary sibling, fsyncs it, renames it over the destination, and syncs the
directory, so concurrent backup calls cannot share a partial temporary file.

For a database with an attached WAL, checkpoint through the WAL manager (or
`DB.Close`) when writing its own checkpoint path. This records the replay
watermark together with the snapshot. `SaveToFile` remains suitable for a
separate backup destination.

### Reference table

| Mode | String | RAM usage | Crash durability | Best for | Main tradeoff |
|---|---|---|---|---|---|
| `ModeMemory` | `memory` | Whole dataset | None, unless snapshotted | Default: analytics, tests, caches, short-lived processes | Fastest, but loses everything since the last snapshot on a crash |
| `ModeAdvancedWAL` | `advanced_wal` | Whole dataset | Full, row-level, point-in-time recovery | Production writes that must survive a crash | Per-statement WAL write cost (tunable via `wal_sync`) |
| `ModeWAL` | `wal` | Whole dataset | Whole-table-diff, periodic checkpoint | Legacy; kept for compatibility | Coarser durability granularity than AdvancedWAL |
| `ModeDisk` | `disk` | ~0 (loaded on demand) | One durable GOB file per table | Minimizing idle RAM, simplicity | Whole table decoded on every cold access |
| `ModeJSON` | `json` | ~0 (loaded on demand) | One durable JSON file per table | Same as Disk, plus files you need to read/diff/hand-edit | Larger files; some types serialize as plain strings |
| `ModeIndex` | `index` | Schema only | One durable file per table | Many tables, only a few of them "hot" | Rows still fully decoded per access, like Disk |
| `ModeHybrid` | `hybrid` | Bounded (`max_memory_bytes`) | One durable file per table | Mixed/unpredictable workloads bigger than you want fully resident | LRU eviction — a working set that thrashes the budget re-decodes often |
| `ModePagedIndex` | `paged_index` | Bounded (page cache) | Durable, immutable artifact | Read-mostly data far larger than RAM (tile serving, published datasets) | Built/rebuilt rather than incrementally heavy-written; only equality seeks skip a full decode today |
| `ModeSQLite` | `sqlite` | ~0 (delegates to SQLite) | Durable `.sqlite` file | Interop with other SQLite tooling | Requires the `sqliteimport` build tag; some tinySQL types serialize as JSON text |

### Orthogonal settings (not modes)

These apply *within* whichever mode you picked above; they are configuration
knobs, not alternative backends:

- **`read_only`** — reject all mutations; see
  ["Read-only serving"](#read-only-serving) below. Works with every mode
  except `wal`/`advanced_wal`, whose recovery code needs write access to the
  WAL sidecar files.
- **`wal_sync`: `full` (default) or `normal`** — how hard `ModeWAL`/
  `ModeAdvancedWAL` push each commit to disk. `full` asks the OS for its
  strongest available flush (`F_FULLFSYNC` on macOS); `normal` is an
  ordinary `fsync` — faster, but its power-loss guarantee then depends on
  the filesystem and hardware write cache, same distinction as SQLite's
  `synchronous` pragma (though the exact levels don't map 1:1 — see the
  driver's DSN option table above).
- **`checkpoint_every` / `checkpoint_interval` / `checkpoint_max_bytes`** —
  how often a WAL mode folds its log into a fresh snapshot. Tuning, not a
  mode choice.
- **`compress_files`** — gzip the on-disk files (`disk`/`hybrid`/`index`/
  `json`) or, for `advanced_wal`, only the periodic checkpoint snapshot
  (never the live log). Smaller files, more CPU per write.
- **`SkipSearchIndexPersistence`** (DSN `skip_search_index_persistence`) —
  table files of `disk`/`json`/`hybrid`/`index` carry no FTS or vector (ANN)
  index image, and a search that merely builds such an index no longer makes
  `Sync` rewrite the table. The indexes are rebuilt from the rows on first use
  after a reopen. On a large retrieval corpus those images are roughly a third
  of the file and dominate `Sync`'s memory and CPU. Ignored by `paged_index`,
  `sqlite` and the log/checkpoint modes. Secondary (`CREATE INDEX`) indexes are
  always persisted. Files stay readable by older versions.
- **`RowBlockFiles`** (DSN `row_block_files`) — a table file becomes a header
  (schema, secondary indexes, FTS/ANN images, statistics) followed by
  independent blocks of about 32 MB of rows, each framed with its length and a
  CRC-32C and, with `compress_files`, gzipped on its own. Blocks are encoded on
  up to `GOMAXPROCS` workers (the bytes do not depend on the worker count) and
  decoded in parallel. The classic layout is one GOB message per table, built
  completely in memory before the first byte is written, single-threaded, and
  `encoding/gob` refuses messages of 8 GiB or more, so a table of that size can
  neither be saved nor loaded; row blocks have no such limit and save with
  memory for a few blocks. The layout is recognised on read by its magic, so
  both layouts open whatever the setting and it can be switched off again.
  **A file written with it cannot be read by binaries that predate the option.**
  Not used with `EncryptionKey` (one AEAD blob, classic layout; opening with
  both set returns an error rather than silently keeping the 8 GiB limit),
  `json` mode (also an error) or the other backends (ignored). **The header is
  still one message**: schema, secondary indexes, statistics and, unless
  `SkipSearchIndexPersistence` is set, the FTS/ANN images (about a third of a
  retrieval table). The 8 GiB limit and the whole-message memory bound therefore
  apply to the header, so they are removed for the *rows* only; for a corpus whose
  images are large, combine the option with `SkipSearchIndexPersistence`. A table
  file that has already grown past 8 GiB in the classic layout can no longer be
  loaded at all, so switch the option on, and let a `Sync` rewrite the file,
  *before* the table gets there. Each block's checksum covers its position and the
  header's checksum, so reordered or foreign blocks are rejected as well as
  damaged ones.
- **`EncryptionKey`** — encrypts data at rest, independent of which backend
  manages it.

**`Sync` does not block statements** (`disk`/`json`/`hybrid`/`index`). It copies
each dirty table's row headers and schema under the engine's content read lock
(a few milliseconds even for a million rows; secondary indexes are lent, not
copied), then encodes and writes the copy with no engine lock held, replacing
each table file atomically (temporary file, fsync, rename). Statements and DML
on the table being written run meanwhile; what they change stays dirty for the
next `Sync`. A `Sync` that returns nil means the state each table had when it was
copied is on disk (every table that was dirty when `Sync` started is written; one
that first changed after the search-index images were copied is left for the
next `Sync`); a failed one leaves the previous files intact. `Sync` calls
are serialized, and `Sync` must not be called from a goroutine that holds the
content lock itself. Peak memory is still the encoded table (the GOB encoder
builds one message per table file).

Four things can still make a statement wait or cost more during a `Sync`, all
bounded and none a stop-the-world:

- With the FTS/ANN images persisted (the default), `Sync` deep-copies them
  *before* it takes the content lock, holding the table's derived-state lock,
  which an INSERT/UPDATE/DELETE that uses a PRIMARY KEY or UNIQUE constraint
  index, and every FTS or vector search that has to follow a change, on that
  table also take. Such a statement waits for the copy (`DB.LastSyncImageCopy`
  reports its length; it grows with the size of the images, seconds on a
  million-chunk corpus), and while it waits it holds the content write lock, so
  every other statement queues behind it. If a committed explicit transaction (or
  a statement issued while a direct SELECT stream is open, or a `Replace`) swaps
  the table's object during that copy, the copy belongs to the old object and is
  not used: that `Sync` still writes the table, but without FTS/ANN images (the
  file carries rows and secondary indexes only; the images are derived and are
  rebuilt on first use after a reopen), and flags the images so that the next
  `Sync` writes them. A table whose object is swapped during the image copy of
  every `Sync` therefore keeps its rows durable each time but its images out of
  the file for as long as that goes on. A table without a PRIMARY KEY or
  UNIQUE constraint (a RAG chunk table) pays nothing for INSERT, UPDATE and DELETE
  statements, failed ones included: they find the constraint cache empty and
  never touch the lock. Every statement on other tables is unaffected.
  `SkipSearchIndexPersistence` removes the copy and the wait (the engine still
  keeps its own in-memory copy of the HNSW topology that no file will carry; that
  is a few hundred MB at a million rows and is refreshed after each followed
  removal).
- The skip lists of the secondary indexes are lent to the snapshot, not copied,
  and stay lent until the snapshot's table file has been written (minutes for a
  large table). The first INSERT, UPDATE or DELETE that touches an index in that
  window deep-copies the list (hundreds of milliseconds for a million keys) under
  the content write lock, and the copy is held next to the lent list until the
  `Sync` is done. In-place renumbering by DELETE (see `insert-update-performance.md`)
  therefore saves nothing for the first statement after a `Sync` started; it does
  for the others. With `Sync` calls issued back to back, that is every cycle.
- Every `Sync` rewrites the whole file of every table that changed, however few
  rows changed: it is off the lock now, but still O(table) in I/O, index
  materialisation and memory. At a million 768-dimensional chunks that is 8 to
  10 GB per `Sync`, which is also where the classic file layout reaches its 8 GiB
  limit; use `RowBlockFiles` (and `SkipSearchIndexPersistence`) from there on, and
  do not call `Sync` more often than a table of that size can be written.
- A table that was saved by someone else (a pool eviction, `SyncTable`, a
  `Replace`) between the copy and the write is snapshotted again, up to five
  times; if it keeps being superseded `Sync` returns an error, leaves every
  table it did write intact and the others dirty, and the caller calls `Sync`
  again. It never falls back to writing under the content lock. A pool eviction
  does not wait for a snapshot write of the same table either: the table stays
  cached until the write is done.

The new options are available through `StorageConfig`, `driver.OpenConfig` and
the DSN only; the `tinysql`/`tinysqld` command line flags and the language
bindings do not expose them yet.

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
[RAG serving](./rag-guide.md#4-embedding-and-ingestion): `VEC_WARM`
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

### Telling a cold cache from a budget that can never fit

A low `CacheHitRate` has two very different causes, and picking the wrong one
wastes a tuning cycle:

- The working set is simply **colder or larger than the budget**. Raising
  `max_memory_bytes` helps proportionally.
- One table **exceeds the budget outright**. No eviction can make room for it,
  so it is refused by the pool and decoded from disk on *every* access, forever.
  Raising the budget helps only once it clears that one table's size.

`BackendStats` separates them. `AdmissionRejects` counts refusals, and
`LargestRejectedBytes` is the largest size refused — i.e. the budget floor that
would make the worst offender cacheable:

```go
st := db.BackendStats()
if st.AdmissionRejects > 0 {
    log.Printf("raise max_memory_bytes past %d bytes: %d cache admissions refused",
        st.LargestRejectedBytes, st.AdmissionRejects)
}
```

Both are zero for backends without a table cache. `tinysqld` exposes them on
`/status` as `admission_rejects` and `largest_rejected_bytes`, and the backend
also logs the first refusal per table with the table's estimated size.

Two caveats when reading the numbers. The size is `EstimateTableSize`, which
extrapolates from the table's first row, so a table with widely varying row
widths (long `TEXT` values in some rows only) can be mis-estimated in either
direction — treat `LargestRejectedBytes` as a starting point, then confirm
`AdmissionRejects` stops climbing. And admission is checked against the space
left *after* eviction, which cannot touch `PinnedTables`: a table comfortably
below the total budget can still be refused when pinned tables hold most of it.
The `EvictionThreshold` (0.85) only decides when eviction kicks in; with
eviction enabled, as `ModeHybrid`/`ModeIndex` always have it, the full
`MaxMemoryBytes` remains usable.

### Serving MBTiles

For a tileset that fits in memory, the per-request query is a point lookup on
`(zoom_level, tile_column, tile_row)`. With a composite index on those columns
it is an index seek. Create the index explicitly; a declared `PRIMARY KEY` does
not create one:

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

Two caveats matter before relying on this path:

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
durable close + read-only reopen). Measure the MBTiles `map`→`tile_id`→`images`
access shape with the target tile sizes, reader count, and page-cache budget.
