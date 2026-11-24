# Retrieval maintenance for continuously re-imported RAG tables

This page is the entry point for applications that keep one large chunk table in
tinySQL (10^5 to 10^6 rows, a `VECTOR` column of several hundred dimensions, a
text column behind `FTS_SEARCH`) while importers replace documents all day and
users search at the same time. It says what such a workload costs, what changed
to make it cheaper, which bounds remain, and what an application has to do to
benefit. The mechanisms are described in detail in
[RAG optimization roadmap](rag-optimization-roadmap.md) (derived structures),
[Insert/update performance](insert-update-performance.md) (the DELETE statement)
and [Storage guide](storage-guide.md) (`Sync` and the file layouts); the numbers
below were produced with [`cmd/ragmaint`](../cmd/ragmaint/README.md).

The measurements cover 100,000 rows of 768 dimensions and 1,000,000 rows of 16
dimensions on one laptop (section 6). Larger combinations are extrapolated from
the code and the package documents and are marked as such.

The numbers in sections 1 and 6 were measured on v0.75.0 and on the branch built on
it. The branch was later merged with v0.76.0 (exact L2 scan, HNSW delta-merge changes,
projected `VEC_SEARCH`); after that merge only the test suites, a differential test
of the merged paths and a coarse re-measurement were run (section 7).

## 1. What the workload costs without these changes

An importer replaces a document by inserting its new chunks and deleting the
chunks of the previous load:

```sql
INSERT INTO chunks (...) VALUES (...), (...), ...;            -- a few to a few hundred rows
DELETE FROM chunks WHERE source_id = 'x' AND load_id <> 'y';  -- the old load
UPDATE chunks SET loaded_at = ? WHERE source_id IN (...);     -- "seen" marker, thousands of rows
```

and the application calls `DB.Sync` to make the change durable. Every derived
structure of the table (the full-text index, the packed vector column used by the
flat scan, the HNSW graph) is validated against the table's versions on the next
search. Before these changes the answer to "what happened to the table?" was
mostly "something", so the next search rebuilt the structure over the whole
corpus:

Measured on v0.75.0 with 100,000 rows of 768 dimensions (flat scan; the first
`HYBRID_SEARCH` after the mutation, median of three runs; the same search takes about
28 ms, 26 to 30 ms, when nothing changed). A source has 3 to 9 chunks, so the row
counts below vary by a few rows between repetitions:

| Mutation before the search | What the search pays for | First search |
| --- | --- | ---: |
| `INSERT` of 6 rows | appends are followed | 105 ms |
| `INSERT` of 1,000 rows | appends are followed | 248 ms |
| `UPDATE` of a few rows | the full-text index is refreshed row by row | 188 ms |
| `UPDATE` of about 3,000 rows (a "seen" marker on 500 sources, one bookkeeping column) | the full-text index is refreshed row by row (slower than a rebuild) | 11,759 ms |
| `UPDATE` of about 6,000 rows | the update history overflows: the full-text index is rebuilt | 3,535 ms |
| `DELETE` of one source (6 to 9 rows) | the full-text index is rebuilt, the packed vector column is repacked | 3,889 ms |
| `DELETE` of 100 sources (about 550 to 590 rows) | the same | 3,826 ms |
| `DELETE` and `INSERT` of one source | the same | 5,769 ms |

With HNSW instead of the flat scan the `DELETE` of one source costs a rebuild of the
graph on the next search (about 0.29 ms per row): 6.3 s at 20,000 rows and 14.5 s at
50,000 rows. The costs grow with the corpus. At 1,000,000 rows of 16 dimensions the
full-text index alone takes 30.8 s to build, and the first search after the touch
`UPDATE` of about 3,000 rows took 110.6 s (37.7 s when only the full-text search ran
first). The run was stopped by its memory cap of 18 GiB during the first `DELETE`
step, when the process had grown to 18.7 GiB against a live heap of 9.6 GiB (10.3 GB).

Three effects follow from this and are what users see as slow search:

- **The first search after a write pays for it, and so does everyone behind it.**
  The rebuild runs inside the `SELECT` while it holds the database's content read
  lock (one lock for all tables). Go's `sync.RWMutex` blocks new readers as soon as
  a writer is waiting, so a queued importer statement turns one slow search into a
  stall of every search on every table.
- **`Sync` is O(table).** In v0.75.0 `DB.Sync` took no engine lock itself but wrote
  the live table, so the caller had to keep writers out for the whole write (an
  application that also kept readers out stalled every search for that long). It
  rewrote every changed table in full, with the search-index images, as one GOB
  message.
- **There is a size cliff.** `encoding/gob` refuses a single message of 8 GiB or
  more on a 64-bit system. A classic table file is one message, measured before
  compression. Once a table's encoding reaches that size, `Sync` fails with
  `gob: encoder: message too big` and nothing newer than the last good file can be
  made durable. An application that does not surface that error keeps reporting
  successful imports that are lost at the next restart.

## 2. What changed

Five work packages, developed and reviewed separately and then merged and checked
together (section 7).

**A. Column-aware UPDATE events.** An UPDATE event in the table's change log
records which columns the statement actually changed. A derived structure that
does not index any of them skips the event, so an UPDATE of a bookkeeping column
(`loaded_at`) no longer touches the full-text index, the vector cache or the
graph, however many rows it covers, as long as the updates between two searches
stay within the retained history (about two million row events when no DELETE
interleaves: at most 64 summaries of 32,768 events each, plus the 65,536 events kept
in the log; section 3). A
DELETE that matches no row no longer bumps `Table.Version`, and `CREATE INDEX` /
`DROP INDEX` no longer advance the table's structural version or reset its change
log (they still bump `Table.Version`). The history holds 65,536 events instead of
4,096; when it is full, its older half is folded: a run of UPDATEs becomes one
column summary and each DELETE is kept as a fold of its own, so an older structure
can still follow it.

**B. DELETE events, and a full-text index that follows them.** A DELETE that can
name the rows it removed is one event of the change log (internal:
`Table.MarkRowsRemoved`, `Table.ChangesSince`). The next full-text search compacts
the document directory, drops the dead documents from the posting lists and
renumbers the rest in place, without tokenizing the corpus again.

**C. Vector structures that follow DELETEs.** The packed vector column of the flat
scan shares its arrays with the previous cache entry and only rebuilds the small
per-row arrays; the floats of dead rows are counted as garbage until they exceed
10 percent. The HNSW graph drops the dead nodes and renumbers nodes and edges in
place under its write lock. Edges to dead nodes are dropped and neighbours are not
repaired, so a followed graph is approximate (section 3).
A graph with no node left that can start a search is not followed when rows were
updated in the same window (they would only become delta rows of an empty graph); it is
rebuilt. A search on a graph without an entry row, or one that finds no row at all
because its entry row cannot be scored, answers with the exact scan when delta rows
exist, instead of from the delta rows alone.

**D. A `Sync` that does not stop the engine.** In the disk, JSON, hybrid and index
modes, `DB.Sync` copies the row headers and schema of all dirty tables in one short
critical section under the content read lock (milliseconds), then encodes and
writes the copies with no engine lock held. The other table-file modes (paged index,
SQLite) still write the live table and need the caller to keep writers out. Two
opt-in options address the remaining costs: `SkipSearchIndexPersistence` keeps the
search-index images out of the table files, and `RowBlockFiles` writes the rows as
independent, checksummed blocks of about 32 MB, which removes the 8 GiB limit for
the rows (disk, hybrid and index modes only).

**E. A DELETE statement that scales with what it deletes.** A DELETE that reports
its removed rows renumbers each secondary index in place instead of rebuilding it
(not with triggers, `DELETE` without `WHERE`, or point deletes by `PRIMARY KEY`/`UNIQUE`
value). A DELETE whose predicate has a top-level condition `text_col = literal` or
`text_col IN (...)` with up to 4,096 literals on the leading column of a secondary
index reads its candidate rows from that index instead of scanning the table,
provided the rest of the predicate cannot raise an error and the table has no DELETE
trigger.

## 3. Bounds and reset conditions

Everything below leaves results correct for the full-text index and the flat scan.
A followed HNSW graph is approximate: it drops edges to dead nodes without repair.
Its recall against a fresh build was measured only at 32 dimensions and about 6,000
rows (recall@10 0.9925 to 0.9988 against 0.9988 to 1.0 on source-shaped data, within
4 points on isotropic vectors, the worst case, at 20 percent removed). What a bound
controls is whether the next search pays for a rebuild.

| What | Bound | What happens past it |
| --- | --- | --- |
| Change log | 65,536 events (each UPDATEd row is one, each reporting DELETE statement is one) | the older half is folded: a run of UPDATEs becomes one column summary, each DELETE becomes a fold of its own that keeps its positions; at most 64 summaries and at most 4,096 folds in all are kept, and beyond either bound the oldest folds are dropped, so structures stamped before them rebuild |
| Removed positions in log and folds | 2^20; composing removal events costs at most 2^28 element moves | the oldest folds, then the oldest log blocks are forgotten and only structures stamped before them rebuild; a single DELETE of more than 2^20 rows resets the log (every structure of the table rebuilds); a window whose removal events would cost more than 2^28 moves to compose makes the reader rebuild |
| Fold of UPDATEs | vouches only that no column the reader indexes changed | a structure that missed an UPDATE of its own column inside a fold rebuilds once |
| Full-text index | follows UPDATE and DELETE events | rebuilds when more than min(max(1024, rows/256), max(64, rows/4)) rows with an updated indexed column are reported, when at least 1,024 token-arena entries (or 512 term-arena entries) are obsolete and exceed a quarter of that arena, and, after a DELETE, when at least 1,024 posting lists are empty and exceed a quarter of the terms |
| Flat vector cache | follows DELETEs | rebuilds when more than 10 percent of the packed floats are garbage; keeps at most 24 segments (the smallest neighbours are merged beyond that); rebuilds when more than min(1024, max(32, rows/8)) rows have an updated vector |
| HNSW graph | follows DELETEs | rebuilds when the removals since the graph was last brought up to date (the statements between two searches, added together) exceed 5 percent of the nodes, when more than 10 percent were removed since the last build (the count is persisted), or when more than min(1024, max(32, rows/8)) rows have an updated vector |
| DELETE | by a non-key column reports its rows | `DELETE` without `WHERE`, swap-and-pop deletes by `PRIMARY KEY`/`UNIQUE` value (without `RETURNING`), a DELETE on a table with DELETE triggers, schema changes and a rollback that undoes a logged DELETE reset that table's log: every structure of the table rebuilds. A foreign-key cascade or `SET NULL` resets the log of the child table it changes |
| Failed statement | the statement is rolled back | the flat cache and the HNSW/IVF graphs of the restored table (of every table when the rollback point is the whole database) are dropped from the process caches: the flat cache is rebuilt in full, a graph is rebuilt unless its persisted topology is still valid, and the full-text cache is re-derived from the index stored on the table (a full re-tokenization if the statement ran a search or logged a DELETE, and, when the rollback point is a copy of the table or of the whole database, also if the statement had changed an existing row) |
| Table object | the structures are keyed by the live table object | a committed explicit write transaction installs its private copy of the table; an INSERT, UPDATE, DELETE or `CREATE`/`DROP INDEX` issued while a direct `SELECT` stream is open on the table installs a copy without search structures; both start the flat cache cold. The graph is rebuilt in full unless the copy still carries a persisted topology that matches (only after a transaction that neither updated nor deleted). The full-text index survives a committed transaction, but its deep copy costs O(index) per writing transaction |
| Cache entries | per process: 256 flat vector columns, 256 full-text document caches, 64 HNSW graphs, 64 IVF indexes | entries are evicted at random (Go map order), including ones a search is still using, which then rebuild at their next search |
| Hybrid and index mode | the pool admits a table whose estimated size fits `max_memory_bytes` | a table the pool refuses is decoded from disk for every statement, and every process cache (flat vector column, document cache, graph object) is cold each time; the persisted search-index images are decoded again with the table, or rebuilt when they are skipped (section 5) |

## 4. What still costs O(N)

Per replace cycle (INSERT a few rows, DELETE the old load, touch UPDATE, search) the
following remain proportional to the table, not to the change:

- the DELETE copies the row headers twice, 24 bytes per row for the rollback
  snapshot and 24 bytes per row for the new survivor slice (about 48 MB at a
  million rows), and walks every secondary index once (about 5 to 7 ms per index per
  400,000 keys);
- the first search after a DELETE shifts the posting lists behind the first removed
  row and visits every term of the vocabulary; the flat cache allocates 33 bytes of
  per-row arrays per row (33 MB per million rows); the graph pass costs about 90 ms
  per million nodes for a six-row removal (100 to 160 ms for 600 to 30,000 rows) and
  the deep copy that persists the graph another 61 ms and 321 MB of transient memory
  (figures of the synthetic benchmarks `BenchmarkVecHNSWRemovalPass` and
  `BenchmarkVecHNSWPersistClone` quoted from the roadmap, not re-run for this page and
  not measured on a corpus);
- the flat scan itself is O(rows x dimensions);
- `Sync` rewrites every table that changed, whatever the size of the change;
- a write that touches an index while a `Sync` is still writing its snapshot copies
  that index's skip list (the snapshot borrows it until its table file is written);
  with `Sync` calls back to back this is the first write of every cycle;
- the RAG context index rebuilds after any UPDATE or DELETE, IVF indexes after any
  change of the table (their cache is keyed on `Table.Version`), the row-filter caches
  and ACL-filtered graphs at least after a removal, and subscriptions after a DELETE
  (they follow UPDATEs within 4,096 versions); row twins only matter for explicit
  transactions on tables without `VECTOR`, `JSON` or `BLOB` columns, so not for a chunk
  table.

## 5. Adopting it in an application

### Rollout and rollback

1. Upgrade tinySQL and leave the application's own lock around `Sync` as it is.
   With v0.75.0 that lock is what keeps writers out of the write.
2. Change the importers to autocommit statements, a delete by a non-key column and
   one bookkeeping column for the "seen" marker (below).
3. Remove the application's lock around `Sync` (disk, JSON, hybrid and index modes).
4. Enable `SkipSearchIndexPersistence` if the image copy of a `Sync` or the file size
   is a problem.
5. Enable `RowBlockFiles` before the largest table's encoding approaches 8 GiB.

`SkipSearchIndexPersistence` is fully reversible. `RowBlockFiles` can be switched off
again: a table is written in the classic layout the next time `Sync` writes it (a table
that has not changed keeps its row-block file until then, and the classic layout must
stay below 8 GiB), but a binary that predates the option cannot read files written with
it, so before a rollback to an older library version every table has to be rewritten,
which a table that is not otherwise dirty will not do by itself. Compatibility was checked on 2026-11-24 with a 20,000-row table of 768 dimensions in disk
mode, each reader working on its own copy of the directory. v0.75.0 opens and searches a
directory written by the merged branch with default options (search-index images
persisted) and one written with `SkipSearchIndexPersistence` alone, and the merged branch
opens a directory written by v0.75.0; the readers returned the same row count, number of
distinct sources, sum of `chunk_idx` and `loaded_at` range, and every search returned 300
rows (vectors and text were not compared). v0.75.0 cannot read a directory written with
`RowBlockFiles` (tested together with `SkipSearchIndexPersistence`): opening succeeds, and
the first statement on the table fails with `load default/chunks: EOF`, because the file
starts with the bytes `00 'TSQLRB1'` instead of a GOB stream. Only v0.75.0 was tried as an
older reader. A v0.75.0 process with the default options that opened a directory written
without images and built a search index wrote the images into the file when it closed the
database (the file grew by about 62 MB); a process that sets `SkipSearchIndexPersistence`
itself does not.

### Statements

- Issue DELETE and UPDATE as autocommit statements, on a connection that holds no
  open stream on the table. The structures follow the live table object; a committed
  transaction or a stream-pinned write replaces it (section 3).
- Delete by a non-key column. A chunk table without `PRIMARY KEY`/`UNIQUE` has no
  swap-and-pop path.
- Keep the "seen" marker to one bookkeeping column. The full-text index, the flat
  vector cache and the HNSW graph skip such an UPDATE (it still costs one change-log
  event per updated row, and the structures listed at the end of section 4 still
  rebuild); an UPDATE of the text or the vector column makes them refresh or rebuild.
- A large batch is cheaper as one statement than as many: one `DELETE ... WHERE
  source_id IN (...)` is one change-log event however many rows it removes, up to
  2^20 rows (and its candidate rows come from an index up to 4,096 keys). More than 5
  percent of the graph's nodes removed between two searches forces a graph rebuild.

### Persistence

- `DB.Sync` no longer needs the caller to keep writers out in the disk, JSON, hybrid
  and index modes (the paged-index and SQLite modes still do). `Sync` calls are
  serialized by tinySQL; a goroutine that holds the engine's content lock must not
  call it.
- With the search-index images persisted (the default), a `Sync` deep-copies them
  before it takes the content lock, under the table's derived-state lock
  (`DB.LastSyncImageCopy` reports how long; it grows with the size of the images and
  was not measured at a million chunks: expect seconds by extrapolation). A search that
  has to follow a change waits for
  that copy; so does an INSERT, UPDATE or DELETE on a table that has a `PRIMARY KEY`,
  `UNIQUE` or `CHECK` constraint, and a statement that waits holds the content write
  lock, so every other statement queues behind it. A table with none of these, like
  a plain chunk table, pays nothing for its writes. If a committed explicit
  transaction, a write issued while a direct `SELECT` stream is open, or a `Replace`
  swaps the table object during the copy, that `Sync` still writes the table but
  without the images (the file carries rows and secondary indexes; the images are
  rebuilt on first use after a reopen) and the next `Sync` writes them; a table whose
  object is swapped during the image copy of every `Sync` keeps its rows durable each
  time and its images out of the file for as long as that goes on.
  `SkipSearchIndexPersistence` removes the copy.
- Every `Sync` still writes every table that changed in full. Do not call it more
  often than a table of that size can be written, and share one durable save
  between several statements.
- `SkipSearchIndexPersistence` (DSN `skip_search_index_persistence`; disk, JSON,
  hybrid and index modes, ignored by paged index, SQLite and the WAL modes): smaller
  files and a cheaper `Sync`, at the price of rebuilding the full-text index and the
  graph on the first search after every reopen. Plan for that start-up cost.
- `RowBlockFiles` (DSN `row_block_files`; disk, hybrid and index modes only; an error
  with `EncryptionKey` or in JSON mode): required for a table that approaches 8 GiB
  per file. The file header (schema, indexes, statistics, and the search-index images
  unless they are skipped) is still one GOB message, so combine it with
  `SkipSearchIndexPersistence` when the images are large. Switch it on before the
  table's encoding gets to 8 GiB: once it is past that size, `Sync` fails in the
  classic layout and nothing newer than the last good file can be made durable until
  the option is on.
- Check the size of the largest table file regularly (`<data dir>/<tenant>/<table>.tbl`
  with the table name in lower case, for the disk, hybrid and index modes). With
  compression (`.tbl.gz`) the 8 GiB limit applies to the uncompressed encoding, so the
  file size understates it by the compression ratio. A failing `Sync` is the first
  symptom of the limit.
- In hybrid and index mode the buffer pool keeps a table resident only if its
  estimated size fits `max_memory_bytes`; a table that does not fit is refused and
  decoded from disk for every statement, with every process cache cold each time. The
  estimate is taken from the first row of the table and counts a `VECTOR` cell as 64
  bytes, so for a table with wide vectors it is far below the real heap (about 22 KB
  per row measured at 768 dimensions with all structures): the budget neither tells
  whether such a table is admitted nor bounds its real memory. Watch
  `BackendStats()`: `AdmissionRejects`, `LargestRejectedBytes`, `LoadCount` and
  `CacheHitRate`. The disk mode keeps loaded tables resident without a budget.

### Sizing

At 768 dimensions the live heap was about 22 KB per row at 100,000 rows (2.25 GB:
0.95 GB the rows, 0.68 GB the full-text index of the generated text, 0.62 GB the
packed vector column), so about 22 GB at a million rows before any transient copy;
the vectors alone are 6.1 GB in the row cells and another 6.1 GB in the packed
column at that size. A `Sync` needs memory in addition to the table itself: the
encoded table (in the classic layout one message of the uncompressed encoding's size;
with row blocks at most 2 x `GOMAXPROCS` blocks of about 32 MB, but the header with
the images is still one message), a deep copy of the search-index images unless
`SkipSearchIndexPersistence` is set, and 24 bytes of row header per row. A cold
rebuild of the flat cache adds the packed column again while searches still hold the
old one. At a million 768-dimensional rows a `Sync` writes roughly 9 to 10 GB without
search-index images and 12 GB or more with them (extrapolated from 20,000-row files of
188 MB without and 250 MB with images), either way past the 8 GiB limit of the classic
layout: a corpus of that size needs `RowBlockFiles` (with `SkipSearchIndexPersistence`)
from the start. These large-corpus figures are extrapolations, not measurements. Measure scale behaviour with `cmd/ragmaint -dim 16`
(the vector part scales with the dimension, the text and index parts do not) and the
real dimension up to what the machine can hold.

### Monitoring

`DB.LastSyncSnapshotHold` (how long the last `Sync` held the content read lock to take
its snapshots; the image-copy wait and the skip-list copy of the first write are not
included), `DB.LastSyncImageCopy`, the duration of `Sync`, the size of the largest table file,
the process RSS, in hybrid and index mode `BackendStats()`, and at the application
level the latency of the first search after an import.

### Flat scan or HNSW

The flat scan is exact and costs O(rows x dimensions) per search. With the changes
above its maintenance after a replace cycle no longer includes copying the packed
vectors, but it still grows with the row count at a small constant (33 bytes per row
of per-row arrays, plus the full-text follow of section 4). A warm HNSW search is much
cheaper than the flat scan on a large corpus (measured here: 2.3 ms at 20,000 rows and
2.7 ms at 50,000 rows, against about 27 ms for the flat hybrid search at 100,000 rows;
the first search after a `DELETE` of one source took 9.6 ms at 20,000 and 23 ms at
50,000 rows), but a build costs about 0.27 to 0.31 ms per row on one core at those
sizes (at a million rows it is minutes: a figure of 380 to 600 seconds was supplied to
this work and not measured), it is rebuilt after 10 percent of its nodes were removed
since the last build, and the rebuild runs inside a search. For a corpus that is replaced
continuously the rebuild comes round again after about `0.1 x rows /
rows_removed_per_replacement` replacements (for example every 3,100 replacements of
32-chunk documents at 1,000,000 rows). Choose HNSW when the latency of the flat scan
is the problem, not to save maintenance, and plan for the rebuild.

## 6. Measured results

All numbers come from one laptop (Intel Core i7-10850H, 6 cores, Windows 11,
`GOMAXPROCS=4`, process priority below normal, other applications running) and from
`cmd/ragmaint`. "v0.75.0" is the unmodified release; "A+B", "A+B+C" and "A+B+E" are
intermediate states of the work (A+B: change-aware UPDATE events and the full-text
index that follows DELETEs; C adds the vector structures, E the DELETE statement);
"all five" is the merged branch. The first-search series are medians of three
repetitions; the convoy, `Sync`, HNSW and scale runs are single runs, so differences of
a few milliseconds or of 20 percent are within the noise of a shared machine. A source
has 3 to 9 chunks, so the row counts of a mutation vary a little between repetitions.
Process memory is the peak working set in MiB as the tool prints it; "live heap" is the
Go heap in use in decimal GB. The raw output of every run is not part of the
repository; `cmd/ragmaint` reproduces it.

### first search after a mutation (core, 100,000 x 768, flat)

| Mutation before the search | v0.75.0 | A+B | A+B+C | A+B+E | all five | warm |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| INSERT 6 rows | 105 ms | 32 ms | 30 ms | 36 ms | 32 ms | 32 ms |
| INSERT 1000 rows | 248 ms | 132 ms | 126 ms | 156 ms | 174 ms | 28 ms |
| UPDATE 6 rows | 188 ms | 27 ms | 27 ms | 27 ms | 26 ms | 29 ms |
| UPDATE ~3000 rows (500 src) | 11,759 ms | 29 ms | 29 ms | 27 ms | 30 ms | 29 ms |
| UPDATE ~6000 rows (1000 src) | 3,535 ms | 28 ms | 28 ms | 29 ms | 28 ms | 30 ms |
| DELETE 1 source | 3,889 ms | 140 ms | 51 ms | 156 ms | 37 ms | 30 ms |
| DELETE 100 sources IN | 3,826 ms | 217 ms | 129 ms | 201 ms | 177 ms | 32 ms |
| DELETE 1 + INSERT 1 (replace) | 5,769 ms | 120 ms | 35 ms | 125 ms | 42 ms | 28 ms |

The warm search takes 26 to 32 ms in every variant. What each package removes shows in
the intermediate columns: A+B removes the full-text cost (the touch `UPDATE` drops from
11.8 s to 29 ms and a `DELETE` from 3.9 s to 140 ms); what is left in A+B after a
`DELETE` is the repack of the packed vector column, which C removes (140 ms to 51 ms);
E speeds up the statement, not the search (next table). The remaining cost of a
100-source `DELETE` (about 130 to 180 ms) is the full-text follow, which shifts the
posting lists behind the first removed row. The first search after the `INSERT` of 1,000 rows pays for extending the structures over the
new rows; the four newer variants are about twice as fast as v0.75.0 there. The differences
among them are within the noise of the machine: the series above shows 126 to 174 ms for them,
and three alternating re-check rounds of five repetitions each gave 115 to 144 ms for them and
235 to 240 ms for v0.75.0.

### the DELETE statement itself (core, 100,000 x 768, one secondary index)

| Statement | v0.75.0 | A+B+E | all five |
| --- | ---: | ---: | ---: |
| DELETE 1 source | 61 ms, 30 MB | 5.0 ms, 5.0 MB | 5.6 ms, 5.0 MB |
| DELETE 100 sources IN | 67 ms, 30 MB | 6.0 ms, 5.1 MB | 7.6 ms, 5.1 MB |
| DELETE 1 + INSERT 1 (replace) | 77 ms, 33 MB | 5.0 ms, 5.0 MB | 4.7 ms, 5.0 MB |

The statement scales with what it deletes instead of with the table: the allocation
falls from 30 MB to 5 MB per statement at 100,000 rows.

### searches while an importer re-imports one source per second (in memory, 100,000 x 768, four searchers)

| | v0.75.0 | A+B | all five | no writer |
| --- | ---: | ---: | ---: | ---: |
| search p50 (ms) | 194 | 29 | 33 | 31 |
| search p90 (ms) | 2,447 | 139 | 43 | 41 |
| search p99 (ms) | 2,861 | 227 | 62 | 44 |
| search max (ms) | 2,912 | 323 | 83 | 55 |
| searches over 1 s | 164 of 329 | 0 of 960 | 0 of 959 | 0 |
| re-imports done (of 120) | 83 | 120 | 120 | - |
| INSERT of 6 rows p50 / p90 (ms) | 21 / 1,748 | 0.0 / 1.5 | 0.0 / 0.0 | - |
| DELETE p50 / p90 (ms) | 55 / 203 | 57 / 78 | 5.0 / 13 | - |
| peak RSS (MiB) | 4,738 | 5,465 | 3,076 | - |

In v0.75.0 a re-import stalls the searches behind it: about half of the searches of the
writer phase (164 of 329) took over a second, and the importer itself was delayed (83 of
120 re-imports completed, `INSERT` p90 1.7 s) because its statements queued behind a
rebuilding search. With A+B nothing waits any more; the merged branch also removes the
repack of the vector column, which shows in the tail (p99 62 ms against 227 ms for A+B)
and in the memory peak (3,076 MiB against 5,465 MiB for A+B and 4,738 MiB for v0.75.0).

### Sync while an importer re-imports (hybrid, 50,000 x 768, a Sync after every re-import, four searchers)

| | v0.75.0, exclusive lock around Sync | all five, no lock | all five, no lock, images skipped |
| --- | ---: | ---: | ---: |
| search p50 (ms) | 2,953 | 17 | 15 |
| search p90 (ms) | 3,151 | 42 | 27 |
| search p99 (ms) | 3,336 | 216 | 54 |
| search max (ms) | 3,406 | 280 | 88 |
| searches over 1 s | 104 of 205 | 0 of 719 | 0 of 720 |
| re-imports in 90 s | 26 | 31 | 61 |
| INSERT of 6 rows p50 (ms) | 1,209 | 0.0 | 0.0 |
| DELETE p50 (ms) | 93 | 2.4 | 2.1 |
| Sync p50 (ms) | 2,232 | 2,989 | 1,491 |
| search p50 without writer (ms) | 17 | 15 | 16 |

This is the pattern of an application that makes every re-import durable: one `Sync`
after every document. With the application's exclusive lock around `Sync` (what v0.75.0
needs) a search that overlaps a `Sync` waits for it and for the importer's statements
queued behind it: about half of the searches (104 of 205) took 3.0 to 3.4 s, against a
`Sync` of 2.2 s, and the others about 75 ms; the p50 of 2,953 ms sits on the boundary
between the two groups. The wait grows with the table: 50,000 rows already take about
2.2 s per `Sync`. Without that lock the median search is barely affected (p50 17 ms
against 15 ms without a writer) and the importer no longer waits, but the tail is
visible: p99 216 ms against 30 ms without a writer. With `SkipSearchIndexPersistence`
the `Sync` is twice as fast and the tail much closer (p99 54 ms, maximum 88 ms). A
`Sync` of the merged branch took 3.0 s beside the searches, against 2.2 s for v0.75.0
with the searches locked out; the runs do not separate the cost of competing for cores
from the cost of the image copy the merged `Sync` makes (an idle merged `Sync` took
2.6 s). The size of a `Sync` grows with the table, so these numbers are a lower bound for
a large corpus.

### HNSW (in memory, 768 dimensions): first hybrid search after a mutation, cold build, warm search

| | 20,000 rows v0.75.0 | 20,000 rows all five | 50,000 rows v0.75.0 | 50,000 rows all five |
| --- | ---: | ---: | ---: | ---: |
| cold graph build | 5,861 ms (0.2930 ms/row) | 6,107 ms (0.3053 ms/row) | 13,983 ms (0.2797 ms/row) | 13,705 ms (0.2741 ms/row) |
| warm hybrid search p50 | 2.3 ms | 2.3 ms | 4.4 ms | 2.7 ms |
| first search after INSERT 6 rows | 38 ms | 10 ms | 73 ms | 19 ms |
| first search after INSERT 1000 rows | 286 ms | 277 ms | 311 ms | 342 ms |
| first search after UPDATE 6 rows | 35 ms | 3.0 ms | 90 ms | 11 ms |
| first search after DELETE 1 source | 6,301 ms | 9.6 ms | 14,496 ms | 23 ms |

The warm HNSW search takes a few milliseconds at these sizes. The cold build costs 0.27
to 0.31 ms per row and is unchanged. In v0.75.0 every `DELETE` makes the next search
rebuild the graph (6.3 s at 20,000 rows, 14.5 s at 50,000); with the merged branch the
graph is renumbered in place (10 and 23 ms). A rebuild still happens once 10 percent of
the nodes were removed since the last build (section 3). The `INSERT` of 1,000 rows
costs the extension of the graph in both.

### scale check: 1,000,000 rows of 16 dimensions (in memory, one run each)

| | v0.75.0 | all five |
| --- | ---: | ---: |
| initial full-text index build | 30.8 s | 27.7 s |
| warm hybrid search p50 | 26 ms | 26 ms |
| first search after `UPDATE` of 3,019 rows | 110,625 ms | 13 ms |
| the same with the full-text search first (3,016 rows) | 37,685 ms | 22 ms |
| first search after `DELETE` of one source, hybrid / full-text first | not measured | 406 ms / 139 ms |
| first search after `DELETE` of 100 sources (587 rows) | not measured | 1,132 ms / 1,103 ms |
| first search after `DELETE` and `INSERT` of one source | not measured | 159 ms / 260 ms |
| the `DELETE` statement | not measured | 39 to 52 ms, 48.8 MB allocated |
| live heap after the structures were built | 10.3 GB | 10.3 GB |
| process memory | 18.7 GiB of Go-mapped memory when the cap of 18 GiB stopped the run | peak working set 10,612 MiB (10.4 GiB) |

The baseline process was stopped by the memory cap of 18 GiB during the first `DELETE`
step (the statement and the search after it); the log does not show which part, so there
is no measured `DELETE` figure for v0.75.0 at this size. A `DELETE` on v0.75.0 makes the
next search rebuild the full-text index, which took 30.8 s when built from scratch, so it
should cost at least that order of time; that was not measured. The process had grown to
18.7 GiB against a live heap of 9.6 GiB (10.3 GB), about 1.9 times, which shows how far
above its steady state such a step goes. Sixteen-dimensional vectors keep the table small
enough for a laptop. The vector part of a 768-dimensional table holds 48 times as many
floats, but the flat scan does not scale that way: it took about 8 ms at a million rows of
16 dimensions and about 24 ms at 100,000 rows of 768 dimensions, so roughly 240 ms at a
million rows of 768 dimensions (a linear extrapolation in the row count, not measured).
The full-text and index parts, which dominate the numbers above, do not depend on the
dimension.

### helper statements around a search (core, 100,000 x 768; three runs, ms)

| Statement | v0.75.0 | A+B | A+B+C | A+B+E | all five |
| --- | ---: | ---: | ---: | ---: | ---: |
| DISTINCT embed_model | 5.6 | 5.0 | 4.5 | 5.0 | 6.6 |
| COUNT(*) | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| DISTINCT source_kind | 9.6 | 8.5 | 8.0 | 9.5 | 11 |
| listSources GROUP BY 6 cols | 34 | 33 | 27 | 34 | 42 |

The helper statements an application runs around a search (distinct embedding models,
source kinds, source list) scan the table and are not targeted by the work packages; the
differences between the variants are within the noise of the machine (the three re-check
rounds show no consistent difference, although the merged-branch run of the first series
was 20 to 25 percent slower on the source list). On v0.75.0 they take, at 1,000,000 rows
of 16 dimensions, 47 ms (`DISTINCT embed_model`), 96 ms (`DISTINCT source_kind`) and 269 ms
(the source list: a `GROUP BY` over six columns with 166,623 groups); on the merged
branch 48, 104 and 291 ms (medians of three runs). They are small next to the rebuilds
above, but not free if they are refreshed after every write.

## 7. How this was verified

- Package tests of each work package (change log, FTS follow, flat cache follow,
  HNSW follow with an exact renumbering oracle, index renumbering, snapshot `Sync`,
  row blocks).
- Two differential tests. One compares the followed full-text index, flat vector
  cache and HNSW graph with a cold build for random statement sequences, including
  failing statements, `DELETE` without `WHERE`, table re-creation and a `SELECT`
  stream open during a write. The other compares every secondary index with a
  scan-only reference table across `DELETE`s inside transactions with savepoints,
  rollbacks, commits and reopens. Transactions with savepoints are checked against
  the search structures only by the stress test.
- `TestRetrievalMaintenanceStress` runs an importer workload (replace cycles, batched
  deletes, touch UPDATEs beyond the change-log bound, failing statements, explicit
  transactions with savepoints) against an oracle while two goroutines call `Sync` and
  three run searches. It checks the live database, databases reopened after `Sync` and
  `Close`, copies of the storage directory taken while the workload runs and copies
  taken at crash points, in `disk` and `hybrid` mode with three option sets: default,
  `SkipSearchIndexPersistence`, and `RowBlockFiles` combined with
  `SkipSearchIndexPersistence`. It uses 8-dimensional vectors and 90 sources (350 to
  650 rows at the checks, up to about 1,200 in the cap phases) and checks logic, not
  scale. In a manual mutation check of about ten deliberate changes to the
  implementation it caught nine (in 3 to 16 of its 16 runs, depending on the
  mutation); the tenth is an equivalent mutant. The mutations are not part of the
  repository.
- On 2026-11-24 the storage, engine and driver suites of the merged branch failed
  only in tests that also fail on the unmodified v0.75.0: `TestWALCloseReleasesFileAfterFlushError`,
  `TestAuditCloseReleasesFileAfterFlushError`, `TestSQLPreparedRawClockRefreshes`,
  `TestFunctionReferenceJSONColumnExamples`,
  `TestStoredProcedureOptionsValidateArgsAndExposeStats`, `TestExplainAnalyzeStages`,
  `TestExplainAnalyzeExecutionIsolation`, `TestExplainAnalyzeCachedSubqueryCalls` and
  `TestRawClockExecutionAPIs` (two more, `TestDelayedInsertFrozenValuesAndPersistence`
  and `TestQueryProfileConcurrentProcedureReadsKeepSummary`, fail intermittently).
- After main (v0.76.0) was merged into the branch, the module's suites were run again.
  The failures were those of v0.75.0, plus `TestBuildDemo` in `cmd/demo`, which fails the
  same way on unmodified v0.76.0 (it executes a binary without `.exe` on Windows);
  `TestDelayedInsertFrozenValuesAndPersistence` passed in that run. A review of the merged
  vector paths (exact L2 scan, IVF lists, HNSW delta merge, projected `VEC_SEARCH`, filtered
  scans) against the follow logic found no incompatibility. It did find HNSW edge cases
  that exist on both sides (an unscorable or missing entry row combined with delta rows:
  the search answered from the delta rows alone, or returned nothing) and one that only
  followed removals reach (all graph nodes removed while other rows were updated in the
  same window). These are handled now (section 2.C). A graph that finds some rows, but
  fewer than k, is still merged with the delta rows, as before.
  `vector_merge_differential_test.go` runs random INSERT, UPDATE and DELETE sequences
  against a brute-force oracle for the l2 and cosine metrics with flat, HNSW and IVF
  searches, projected and unprojected, and checks the cache and graph states it reaches.
  `vector_merge_edge_test.go` adds the prefix pruning of the exact scan on prefix-heavy
  vectors, delta rows across followed removals and the HNSW fallbacks. In a manual
  mutation check of ten deliberate changes to the merged code these tests and the
  existing ones caught all ten (the first round of the differential test alone missed
  two, which the second file closes). The mutations are not part of the repository.
- A coarse re-measurement of the merged build (core mode, 100,000 x 768) ran on a laptop
  that was not otherwise idle: the same binary varied by a factor of two between runs.
  At their quietest runs unmodified v0.75.0, unmodified v0.76.0 and the merged build were
  on par for the warm hybrid search (30.8, 34.7 and 31.6 ms p50 at limit 300), and the first
  search after a mutation stayed close to the warm search (touch UPDATE of about 3,000
  rows 45.9 ms against 43.8 ms warm, DELETE of one source 62.0 against 44.4 ms, replace
  59.0 against 41.1 ms, in the noisier run). These runs are not a benchmark and are not
  part of the tables above.

What is not verified: there is no race-detector run (it needs cgo), so the stress
test relies on its oracle and invariants to find concurrency defects; nothing was
measured end to end on a million-row, 768-dimensional corpus; the HNSW recall was
tested at 32 dimensions with up to about 6,000 rows (the stress test at 8 dimensions
with a few hundred rows), never at 768 dimensions and a million rows; the stress test
covers only `disk` and `hybrid`, the differential tests only the in-memory database and
`disk`, all without encryption, while `index`, `paged_index`, `sqlite`, the WAL modes
and encryption are not run through them. A
rolled-back explicit transaction in `hybrid` mode on a table that was loaded from
files was seen to leave its changes in the live table, on the unmodified v0.75.0 as
well; explicit transactions are not covered there.

## 8. Reproducing the numbers

The commands behind section 6 (`-memlimit` is a soft limit in GiB and must stay above
the live heap of the run):

```sh
go build -o ragmaint ./cmd/ragmaint
# first search after each mutation, helper statements, memory
./ragmaint -mode core -n 100000 -reps 3 -gc 50 -memlimit 10 -rsscap 11
# searches while an importer re-imports one source per second
./ragmaint -mode convoy -n 100000 -gc 30 -memlimit 9 -rsscap 11
# Sync after every re-import; -syncmode excl is the exclusive application lock of v0.75.0
./ragmaint -mode convoysync -n 50000 -storage hybrid -data ./scratch -syncmode none -gc 50 -memlimit 10 -rsscap 11
./ragmaint -mode convoysync -n 50000 -storage hybrid -data ./scratch2 -syncmode none -skipimages -gc 50 -memlimit 10 -rsscap 11
# HNSW
./ragmaint -mode hnsw -n 20000 -gc 50 -memlimit 10 -rsscap 11
./ragmaint -mode hnsw -n 50000 -gc 50 -memlimit 10 -rsscap 11
# scale check, a subset of the mutations
./ragmaint -mode core -n 1000000 -dim 16 -reps 1 -only "~3000,source,(replace)" -gc 50 -memlimit 14 -rsscap 18
```

The v0.75.0 columns of section 6 were measured with a build of the tool that does not set
the two storage options (`-skipimages`, `-rowblocks`) that only the merged branch has. Run
one process at a time on an otherwise idle machine and pass
`-rsscap`; the `-data` directory must be empty or not exist. The compatibility check of
section 5 used a small extra mode that is not part of the tool. See the
[tool's README](../cmd/ragmaint/README.md) for the modes and flags.
