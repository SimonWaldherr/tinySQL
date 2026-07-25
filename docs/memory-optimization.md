# Memory Optimization Guide

Where tinySQL spends memory, what has landed, and what is still open. Open
items list options with their trade-offs.

Measurements: `GO111MODULE=on go test ... -benchmem` and
`go tool pprof -alloc_space`, Go 1.26, Apple Silicon. Reproduce before and
after any change.

## Two kinds of memory consumption

- **Resident footprint** — memory a running database holds for a dataset (row
  storage, live transaction snapshots, caches). Lowering it lowers peak RSS.
- **Transient churn** — short-lived per-statement allocations the GC reclaims
  almost immediately (result rows, scratch buffers). Lowering it lowers GC
  pressure and improves throughput, but barely moves peak RSS.

Report them separately; they call for different fixes.

## Landed: single-copy transaction snapshots

`BeginTx` used to clone the whole database twice via `DeepClonePair`: a `base`
snapshot for conflict detection plus a mutable `shadow` for writes. The only
consumers of `base` (`CollectWALChanges` and the driver's `detectTxConflicts`)
read `Table.Version` and table existence — never rows.

`storage.SnapshotForTx` now copies rows once (into `shadow`) and gives `base`
only per-table identity plus `Version` via `cloneTableMeta`. Read-only
transactions skip `base`, take a single read snapshot, and commit without a
writer lock.

Result (8 tables × 500 rows):

| Transaction | Before | After | Reduction |
|---|---|---|---|
| Read-only | 920 KB / 9183 allocs | 558 KB / 5124 allocs | −39 % mem, −44 % allocs |
| Read-write | 728 KB / 8192 allocs | 370 KB / 4176 allocs | −49 % mem, −49 % allocs |

See `internal/storage/db.go` (`SnapshotForTx`, `cloneTableMeta`) and
`internal/driver/driver.go` (`BeginTx`, `commitTx`).

## Open item 1 — result rows as `map[string]any`

Results are `[]Row` with `type Row = map[string]any`. Each row is a separate
map holding every projected column and, for `SELECT *` / joins, both the
unqualified `col` and qualified `table.col` keys. Largest allocation source in
read paths.

```
# SELECT id, name FROM users WHERE active = true  (500 rows)
projectRawRow            93 % of alloc_space   ~354 B/row, ~2 allocs/row
# JOIN ... 500 result rows
projectJoinRawRow        53 % of alloc_space
executeSimpleJoinFastPath 41 % (hash map + key boxing)
```

```bash
GO111MODULE=on go test . -run=XXX -bench='BenchmarkExecute_ReadQueries/FilteredScan' \
  -benchmem -memprofile=fscan.prof
GO111MODULE=on go tool pprof -top -alloc_space fscan.prof
```

A Go map carries a header plus at least one 8-slot bucket regardless of column
count, so a 2-column row costs ~350 B — mostly overhead. Mostly transient
churn, but large result sets also inflate peak RSS while held.

**1a. Columnar / slice-backed rows** (biggest win, biggest change). Store
values positionally in a `[]any` (or typed column vector) with one shared
`map[string]int` name→index per result set. Removes ~1 map allocation per row;
per-row cost drops to one `len(cols)`-sized slice.

`type Row = map[string]any` is public (`tsql.Row`, `GetVal`, callers doing
`row["col"]` / `range row`), so changing it is breaking. Keep the map API and
add a parallel one:

- `type ResultSet2 struct { Cols []string; ColIndex map[string]int; Rows [][]any }`
  behind an opt-in call (e.g. `ExecuteColumnar` / `Rows.Columnar()`), leaving
  `Row`/`ResultSet` untouched; or
- make `Row` an interface with `Get(name) any` plus index access, backed
  internally by a slice implementation — requires auditing internal `row[...]`
  index/range sites in `internal/engine`.

**1b. Drop the duplicate qualified key on final projection.** For `SELECT *` /
joins each row stores both `col` and `alias.col`
(`buildSimpleSelectStarProjections` sets `altKey`); at ≥5 columns the second
key pushes the map from one bucket to two. Small and low-risk, but callers
looking up the qualified form on a top-level result would break — several tests
rely on `GetVal(row, "orders.id")` for `SELECT *`. Qualified keys must stay on
intermediate rows (joins/subqueries) and be dropped only on the outermost
projection, which needs extra plumbing to know "this is the final result".

**1c. Pool the row maps.** Not viable as-is: result rows escape to the caller
with unbounded lifetime, so they cannot go back to a `sync.Pool`. Only
workable with 1a, where the caller consumes a columnar set that can be recycled
after iteration.

Recommendation: 1a as an additive columnar API for large-result read paths
(exports, scans, RAG retrieval), map form staying the default. 1b only pays off
if 1a is deferred.

## Open item 2 — per-row copy in the UPDATE fast path

`executeSimpleUpdateFastPath` allocates
`nextRow := append([]any(nil), raw...)` per matched row — dominant allocation
in `BenchmarkExecute_Update` (~88 MB flat over the run). The old row image is
needed *after* the write by `wal.logUpdate` (WAL before-image) and
`patchConstraintIndexRow` (old index key, which for composite indexes can
involve unchanged columns), so mutating `raw` in place would destroy it. This
is transient churn, not resident memory.

**2a. Allocate the before-image only when a consumer needs it.** Mutate the
live row in place; copy old values only when WAL is active or a constraint
index covers the table. WAL inactive (pure in-memory `mem://`) *and* no
constraint index → zero allocations per row; either active → one copy, same
cost as today. Requires a cheap "does this table have a constraint index?"
check (peek `constraintIndexes` under `constraintIndexMu`, cached per
statement) and confirming `wal` is a real no-op when inactive. Correctness
hinges on the before-image only being read post-write by those two paths —
audit needed.

**2b. Copy only the changed columns' old values into a reused scratch.** Keep a
`[]struct{col int; old any}` scratch sized to `len(plan.sets)`, reused across
rows, and mutate `raw` in place; feed it to `logUpdate` /
`patchConstraintIndexRow` variants taking "old values for these columns".
Bounded allocation regardless of row width, but composite constraint indexes
need the full old row to recompute the old key, so this works only for
single-column index keys and needs a fallback to the full-copy path. New
signatures for both consumers.

**2c. Leave as-is.** The copy is correct and churns only transient memory.
Given the subtle before-image invariants this is the safe default unless
UPDATE-heavy in-memory workloads show GC pressure in profiles.

Recommendation: 2a for the common in-memory case, guarded so the full-copy path
still runs whenever WAL or a constraint index is present. Ship only with a
regression test that commits an UPDATE under an active WAL and under a
constraint index and verifies the before-image/rollback.

## Open item 3 — join hash table key boxing

`executeSimpleJoinFastPath` builds `rightByKey := map[any][][]any`, boxes each
join key through `comparableKeyPart` (~25 MB in the join profile), and
allocates one `[][]any` bucket slice per distinct key.

**3a. Type-specialized hash maps.** For a single `int64`/`string` join key (the
common case), build `map[int64][]int` / `map[string][]int` over row indices
instead of `map[any][][]any` over row copies: no key boxing, `int` indices
instead of re-referenced `[]any` rows, far less per-key overhead. Costs one
fast path per key type plus a generic `any` fallback for mixed/other types.

**3b. Size the map from a distinct-key estimate.**
`make(map[any][][]any, len(right.Rows))` over-allocates when many rows share a
key. Low effort, small win; mostly helps many-to-one joins.

Recommendation: 3a for int/string equi-joins, generic map as fallback.

## Guidance for future work

- Attach a before/after `-benchmem` table and state whether the win is resident
  or transient.
- Prefer additive/opt-in APIs over changing `Row`/`ResultSet`; the map form is
  public surface.
- Guard fast-path allocation elisions behind the exact precondition (WAL
  inactive, no constraint index, single-column key), keep the correct slow path
  as fallback, and add a regression test exercising the guarded branch.
