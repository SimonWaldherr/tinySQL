# ragmaint

`ragmaint` measures what an application with a large, continuously re-imported
chunk table pays in tinySQL around a search: the first search after a mutation,
the effect of a mutation on concurrent searches, what `DB.Sync` costs and blocks,
and what an HNSW graph costs. It uses only the public `tinysql` API and the
statement shapes of an importer-fed RAG corpus: one `chunks` table of 14 columns
with a 768-dimensional (by default) `VECTOR` column, one secondary index on
`(source_id, chunk_idx)`, documents of 3 to 9 chunks (the re-import writers use 6)
that are replaced by `INSERT` of the new rows and
`DELETE ... WHERE source_id = ? AND load_id <> ?`, `UPDATE ... SET loaded_at` touch
batches, and `HYBRID_SEARCH` / `FTS_SEARCH` / `VEC_SEARCH`.

The numbers in [docs/retrieval-maintenance.md](../../docs/retrieval-maintenance.md)
were produced with this tool.

```
go build -o ragmaint ./cmd/ragmaint
./ragmaint -mode core -n 100000 -rsscap 11
```

## Modes

| `-mode` | what it does | main output lines |
| --- | --- | --- |
| `core` | in-memory database; warm queries, helper statements, then for every mutation the first search that follows it | `E1` load, `E2` warm latencies, `E10` helper statements, `E3`/`SUM3` first/second search after each mutation |
| `convoy` | in-memory; four searchers (one search per 500 ms each) while a writer re-imports one source per second | `E6` latency percentiles per phase and per 10 s window |
| `persist` | disk or hybrid storage; what each `Sync` writes and how long it takes after each mutation, close and reopen | `E7` |
| `hnsw` | cold HNSW build, warm queries, first search after `INSERT` / `UPDATE` / `DELETE` with `index: hnsw` | `E8` |
| `convoysync` | like `convoy` on a disk or hybrid database, with a `Sync` after every writer iteration; an application-level `RWMutex` is emulated so the lock an application puts around `Sync` shows up in the search latency | `E9` |
| `updprof` | how the first search after a touch `UPDATE` scales with the number of updated rows | `UP` |

## Flags that matter

| flag | meaning |
| --- | --- |
| `-n`, `-dim` | rows and vector dimensions (`-dim 16` keeps the rows of a 1 M-row table at about 3.5 GB and the table with its full-text index and packed vector column at about 10 GB, against more than 22 GB at 768 dimensions; the vector part scales with `-dim`, the text and index parts do not) |
| `-reps` | repetitions of every mutation in `core` (the `SUM3` lines are medians with `[min-max]` over them) |
| `-limit` | `candidate_k` / `k` of the searches |
| `-index flat\|hnsw` | index used by the `core` searches |
| `-storage disk\|hybrid`, `-data DIR` | persistence mode and directory for `persist` / `convoysync` (the directory must be empty or not exist; it is not removed) |
| `-skipimages`, `-rowblocks` | `StorageConfig.SkipSearchIndexPersistence` and `StorageConfig.RowBlockFiles` |
| `-syncmode excl\|rlock\|none`, `-syncevery N` | `convoysync`: lock held around `Sync` (`excl` is what an application needs when `Sync` cannot run beside writers; `none` is valid with the snapshot-based `Sync`) and a `Sync` after every N writer iterations |
| `-csdur`, `-csctrl`, `-convdur`, `-ctrldur` | seconds of the writer and control phases of `convoysync` and `convoy` |
| `-only`, `-skip` | comma lists of substrings of mutation names (`core`) |
| `-skipe3` | `core`: skip the mutation series E3/SUM3 (the profile step still runs when `-prof` is set) |
| `-memlimit GIB`, `-gc PCT`, `-rsscap GIB` | soft memory limit (keep it above the live heap of the run), GC percent, and a watchdog that aborts the process when Go's mapped memory exceeds the cap |
| `-prof DIR` | write profiles into the existing directory DIR: `core` writes heap profiles after the load and after the rebuild and a CPU profile of the first hybrid search after a DELETE of one source; `updprof` writes a CPU profile of the first hybrid search after the touch `UPDATE` of 500 sources |

## Memory

At 768 dimensions the live heap of a table was about 22 KB per row (measured at
100,000 rows: 0.95 GB after the load, 1.63 GB after the full-text index and 2.25 GB
after the packed vector column of the flat scan; the vectors exist twice, in the row
cells and in the packed column). A rebuild of the search structures adds transient
copies: a 100,000 x 768 run peaked at about 5.8 GiB of process memory (5,915 MiB) in
these measurements, so `-rsscap 11` leaves a margin of about two. One million
768-dimensional rows would need more than 22 GB before any transient copy: use `-dim 16`
for scale checks and always pass `-rsscap`. Run one process at a time and keep the
machine otherwise idle; the first searches after a mutation are sensitive to memory
pressure and to other load. The `RSS` values (MiB) in the output come from Windows
PowerShell and read `rss n/a` on other platforms; the `-rsscap` watchdog works
everywhere. The heap figures are decimal MB.

## Reading the results

`E3` runs every mutation twice: once probing with `HYBRID_SEARCH`
(`first` / `second` are the first and second search afterwards) and once
probing with `FTS_SEARCH`, then `VEC_SEARCH`, then `HYBRID_SEARCH`, which
shows which structure paid for the rebuild. `warm` is the median of three
searches before the mutation, `dml_*` are the statement times and allocation
of the mutation itself. `SUM3` lines are medians with `[min-max]` over `-reps`
repetitions.

A search that is slower than `warm` right after a mutation is the cost the
engine postponed to the search; that is the number that matters for an
application whose users search while importers write.
