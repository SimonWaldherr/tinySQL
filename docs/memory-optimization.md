# Memory Optimization Guide

This document records where tinySQL spends memory at runtime, which
optimizations have already landed, and concrete proposals for the remaining
opportunities. It is meant as a working reference for future performance work —
each open item lists several implementation options with their trade-offs so a
change can be picked deliberately rather than guessed at.

Measurements below were taken with `GO111MODULE=on go test ... -benchmem` and
`go tool pprof -alloc_space` on Go 1.26, Apple Silicon. Reproduce with the
commands in each section before and after a change.

## Two kinds of "memory consumption"

Keep the distinction in mind when weighing a change:

- **Resident footprint** — memory a running database *holds* for a given
  dataset (row storage, live transaction snapshots, caches). Reducing this
  lowers peak RSS and lets tinySQL serve larger datasets in the same box.
- **Transient churn** — short-lived allocations produced *per query/statement*
  that the GC reclaims almost immediately (result rows, scratch buffers).
  Reducing this lowers GC pressure and improves throughput/latency, but does
  not lower peak RSS much.

Both matter, but they call for different fixes and should be reported
separately.

## Landed: single-copy transaction snapshots

**Problem.** Every `BeginTx` cloned the *entire* database twice via
`DeepClonePair` — a `base` snapshot for conflict detection plus a mutable
`shadow` that receives writes. But the only consumers of `base`
(`CollectWALChanges` and the driver's `detectTxConflicts`) read `Table.Version`
and table existence exclusively; they never inspect rows. Copying every row
into `base` wasted memory proportional to the whole database on every
transaction.

**Fix.** `storage.SnapshotForTx` copies rows once (into `shadow`) and gives
`base` only per-table identity + `Version` via `cloneTableMeta`. Read-only
transactions skip `base` entirely and take a single read snapshot; their commit
short-circuits with no writer lock.

**Result** (8 tables × 500 rows):

| Transaction | Before | After | Reduction |
|---|---|---|---|
| Read-only  | 920 KB / 9183 allocs | 558 KB / 5124 allocs | −39 % mem, −44 % allocs |
| Read-write | 728 KB / 8192 allocs | 370 KB / 4176 allocs | −49 % mem, −49 % allocs |

See `internal/storage/db.go` (`SnapshotForTx`, `cloneTableMeta`) and
`internal/driver/driver.go` (`BeginTx`, `commitTx`).

---

## Open item 1 — result rows as `map[string]any`

**What.** Query results are `[]Row` where `type Row = map[string]any`. Each
output row is a separate map that stores every projected column (and, for
`SELECT *` / joins, both the unqualified `col` and qualified `table.col` keys).
This is the single largest allocation source in read paths.

**Evidence.**

```
# SELECT id, name FROM users WHERE active = true  (500 rows)
projectRawRow            93 % of alloc_space   ~354 B/row, ~2 allocs/row
# JOIN ... 500 result rows
projectJoinRawRow        53 % of alloc_space
executeSimpleJoinFastPath 41 % (hash map + key boxing)
```

Reproduce:

```
GO111MODULE=on go test . -run=XXX -bench='BenchmarkExecute_ReadQueries/FilteredScan' \
  -benchmem -memprofile=fscan.prof
GO111MODULE=on go tool pprof -top -alloc_space fscan.prof
```

A Go map carries a header plus at least one bucket (8 slots) regardless of how
few columns the row has, so a 2-column result row costs ~350 B — most of it
overhead, not data. This is mostly **transient churn**, but for large result
sets it also inflates peak RSS while the set is held.

### Proposals

**1a. Columnar / slice-backed result rows (biggest win, biggest change).**
Introduce a result representation that stores values positionally in a `[]any`
(or a typed column vector) with a *single* shared `map[string]int`
name→index for the whole result set instead of one map per row.

- Pros: removes ~1 map allocation per row; per-row cost drops from a map+bucket
  to one `len(cols)`-sized slice; column-name lookups hit one shared map.
- Cons: `type Row = map[string]any` is public (`tsql.Row`, `GetVal`, and
  callers doing `row["col"]`/`range row`). Changing it is breaking.
- Mitigation — keep the map API, add a parallel one:
  - Add `type ResultSet2 struct { Cols []string; ColIndex map[string]int; Rows [][]any }`
    and expose it via a new opt-in call (e.g. `ExecuteColumnar` /
    `Rows.Columnar()`), leaving `Row`/`ResultSet` untouched.
  - Or make `Row` an interface with both `Get(name) any` and index access, and
    provide a slice-backed implementation used internally; the map form stays
    available for compatibility. Requires auditing internal `row[...]`
    index/range sites in `internal/engine`.

**1b. Drop the duplicate qualified key on final projection.**
For `SELECT *`/joins each row currently stores both `col` and `alias.col`
(`buildSimpleSelectStarProjections` sets `altKey`). At ≥5 columns the second
key pushes the map from one bucket to two.

- Pros: small, low-risk; halves map entries for star/join rows, occasionally
  saving a whole bucket.
- Cons: callers that look up the qualified form on a top-level result would
  break; several tests rely on `GetVal(row, "orders.id")` for `SELECT *`. Would
  need to keep qualified keys for intermediate rows (joins/subqueries) and drop
  them only on the outermost projection — extra plumbing to know "this is the
  final result".

**1c. Pool the row maps.** Not viable as-is: result rows escape to the caller
and their lifetime is unbounded, so they cannot be returned to a `sync.Pool`.
Only workable together with 1a (the caller consumes a columnar set that *can*
be recycled after iteration).

**Recommendation.** Pursue 1a as an *additive* columnar API (no breaking
change), aimed at large-result read paths (exports, scans, RAG retrieval).
Keep the map form as the default. 1b only pays off if 1a is deferred.

---

## Open item 2 — per-row copy in the UPDATE fast path

**What.** `executeSimpleUpdateFastPath` allocates a fresh `nextRow :=
append([]any(nil), raw...)` for every matched row (dominant allocation in
`BenchmarkExecute_Update`: ~88 MB flat over the run).

**Why it exists.** The old row image is needed *after* the write for two
consumers: `wal.logUpdate` (WAL before-image) and `patchConstraintIndexRow`
(old index key, which for composite indexes can involve unchanged columns).
Mutating `raw` in place would destroy that before-image.

This is **transient churn**, not resident memory.

### Proposals

**2a. Allocate the before-image only when a consumer needs it.**
Invert the copy: mutate the live row in place and copy the old values *only*
when WAL is active or a constraint index covers the table.

- When `wal` is inactive (pure in-memory `mem://`) *and* the table has no
  constraint index → no copy at all (zero allocations per row).
- When either is active → allocate one copy (same cost as today).
- Requires: a cheap "does this table have a constraint index?" check
  (peek `constraintIndexes` under `constraintIndexMu`, cached per statement)
  and confirming `wal` is a real no-op when inactive. Correctness hinges on the
  before-image only being read post-write by those two paths — audit needed.

**2b. Copy only the changed columns' old values into a reused scratch.**
Keep a `[]struct{col int; old any}` scratch sized to `len(plan.sets)`, reused
across rows, and mutate `raw` in place. Feed the scratch to a
`logUpdate`/`patchConstraintIndexRow` variant that takes "old values for these
columns" instead of a full old row.

- Pros: bounded, reusable allocation regardless of row width.
- Cons: composite constraint indexes need the *full* old row to recompute the
  old key, so this only works for single-column index keys; needs a fallback to
  the full-copy path otherwise. New signatures for the two consumers.

**2c. Leave as-is (documented default).** The copy is correct and only churns
transient memory that the GC reclaims. Given the subtle before-image
invariants, this is the safe default unless UPDATE-heavy in-memory workloads
show GC pressure in profiles.

**Recommendation.** 2a for the common in-memory case, guarded so the full-copy
path still runs whenever WAL or a constraint index is present. Ship only with a
regression test that commits an UPDATE under an active WAL and under a
constraint index and verifies the before-image/rollback.

---

## Open item 3 — join hash table key boxing

**What.** `executeSimpleJoinFastPath` builds `rightByKey := map[any][][]any` and
boxes each join key through `comparableKeyPart` (~25 MB in the join profile),
plus one `[][]any` bucket slice per distinct key.

### Proposals

**3a. Type-specialized hash maps.** When the join key column is a single
`int64`/`string` (the common case), build `map[int64][]int` / `map[string][]int`
over *row indices* instead of `map[any][][]any` over row copies.

- Pros: avoids boxing the key into `any`; stores `int` row indices rather than
  re-referencing `[]any` rows; far less per-key overhead.
- Cons: needs a fast path per key type with a fallback to the generic `any` map
  for mixed/other types; modest added code.

**3b. Size the map from distinct-key estimate, not row count.**
`make(map[any][][]any, len(right.Rows))` over-allocates when many rows share a
key. Low effort, small win; mostly helps many-to-one joins.

**Recommendation.** 3a for int/string equi-joins (covers the majority),
generic map as fallback.

---

## General guidance for future work

- Always attach a before/after `-benchmem` table and say whether the win is
  **resident** or **transient**.
- Prefer additive/opt-in APIs over changing `Row`/`ResultSet`; the map form is
  part of the public surface.
- Guard fast-path allocation elisions behind the exact precondition
  (WAL inactive, no constraint index, single-column key) and keep the correct
  slow path as the fallback, with a regression test that exercises the guarded
  branch.
