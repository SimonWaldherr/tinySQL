# Analytical table snapshots

The `tinystore` package stores compact column-oriented snapshots. Version one
uses the `TINY01` format identifier and the `.tsnap` extension.
The public operations are `Write`, `Read`, `ReadDir`, `Has`, and `FileName`.
See the executable [example](../tinystore/example_test.go) for writing tables,
loading them into an in-memory database, and querying the result.

The codec stores TEXT dictionaries (or plain text for high cardinality),
zigzag-varint INT values, FLOAT values and NULL bitmaps. Columns use bounded
parallelism and CRC32C checksums. Each write uses its own temporary file,
fsync and rename. Directory enumeration treats glob characters literally.
Writes reject nil tables; reads check dimensions against platform integer
capacity before allocating cells.

This is a data-only snapshot: column names/types and values are preserved;
constraints, defaults, declared SQL types, indexes and statistics are not.
Only TEXT, INT and FLOAT columns are accepted. INT loads as Go `int` and values
outside the host's range are rejected. Readers load whole tables into memory;
callers remain responsible for dataset memory budgets and snapshot lifecycle.
Do not mutate a table while writing it. Directory rename durability and
replacement atomicity depend on the platform; the directory is not fsynced.
Existing ModeDisk/WAL formats and recovery behavior are unchanged.

CAST now uses a shared conversion helper directly from the raw-row evaluator,
avoiding the generic function argument-wrapper pool and preserving its target
as syntax. Arity, NULL, conversion and error behavior are compared against the
general evaluator. A grouped SUM/AVG regression test verifies both raw-plan
admission and equality with the derived-table evaluation path.

## What belongs in tinySQL next

| Application optimization | Current tinySQL evidence and decision |
| --- | --- |
| Remove CAST from SUM/AVG | Already admitted by `isSimpleRawExpr` and `buildSimpleAggregateProjection`. Keep CAST semantics; the raw evaluator is optimized in this change. |
| Store day/week/month/quarter/year columns | `buildSimpleAggregatePlan` still requires a `VarRef` for each GROUP BY item. Extend it with evaluated grouping keys, then consider a bounded per-query cache for deterministic date expressions. Preserve NULLs, timezone semantics, mixed-type equality and expression errors. Do not automatically add five physical columns to every date field. |
| Rewrite ratios as an aggregate subquery plus outer division | `buildSimpleAggregateProjection` still requires a plain group column or aggregate call. The appropriate engine improvement is to accumulate aggregate leaves once and evaluate the enclosing expression per group. Test HAVING, aliases, empty input, zero denominators, decimals and nested aggregates before enabling it. A dataset-specific compiler rewrite is not a general SQL rewrite. |
| Build tables through the storage API | Useful for new, privately built analytical copies; the example uses `NewTable` and one `DB.Put`. Do not route general SQL INSERT through direct row assignment: that would bypass constraints, triggers and transaction contracts. Transaction snapshots already share immutable scalar rows (`SnapshotForTx`/`cloneRowsShared`); remeasure throughput before changing commit behavior. |
| Intern repeated strings | Included in dictionary snapshot decoding. Generic global interning would retain unrelated datasets and cost memory for high-cardinality values. |
| Warm copies before activation; count cells instead of rows | Sound application lifecycle and budget decisions. Keep scheduling, dataset policies, generation swaps and cleanup in the application. |

The next engine priorities are expression grouping and aggregate expressions
such as ratios. A transparent replacement of the database's durable table
format is a separate migration project: the snapshot codec intentionally lacks
much of the metadata and type coverage required by ModeDisk.

## Validation and measurements

Tests include round-trip, empty-table, NULL, floating-point edge, dictionary,
plain-text, atomic replacement, path and corruption-rejection tests from the
source package, plus a compiled SQL integration example. Engine and tinystore
tests pass; the root module's `go test ./...` passes. Focused race tests cover
the new evaluator and snapshot read/write paths. `go vet` covers both packages.
Nested modules under cmd/, bindings/ and other directories are not included by
root-module `./...`.

Local Apple M2 Max, Go 1.27.3, 100,000 rows × four columns, five benchmark
iterations, warm filesystem cache:

| Codec | Write (including fsync) | Read | File bytes | Read allocation bytes/op |
| --- | ---: | ---: | ---: | ---: |
| Column snapshot | 6.68 ms | 2.25 ms | 1,291,884 | 11,719,203 |
| gob table codec | 68.94 ms | 72.73 ms | 6,725,560 | 35,566,520 |

This is approximately 10× faster writing, 32× faster reading and 81% smaller
for this repeated-text fixture. It is a codec comparison, not a full ModeDisk
benchmark or a guarantee for other data. Earlier measurements while other
tests were running varied materially; do not compare these timings with
other million-row results as if they were the same workload.

Reproduce:

```sh
go test ./tinystore ./internal/engine
go test ./tinystore -run '^$' -bench BenchmarkSnapshot -benchtime=5x
go test ./internal/engine -run '^$' -bench BenchmarkRawCast
```

## Compatibility and decoder optimization

A frozen version-one fixture in `tinystore/testdata` verifies existing-file
reads and byte-for-byte writer compatibility for the current format. This complements round-trip tests, which alone cannot detect a writer and
reader drifting together. Existing database persistence remains
separate and unchanged.

The unsigned-varint decoder now handles one-byte values directly. Dictionary
references and short string lengths use this path; larger values retain the
standard decoder and its error checks. A local three-run comparison (20 reads
per run, the fixture above) measured 4.47–5.59 ms before and 2.47–2.95 ms after.
These short runs are indicative and sensitive to machine load, not a guaranteed
speedup. Allocation counts remain essentially unchanged.

## Snapshot write path

Dictionary references are encoded as varints during the first pass, avoiding a
four-byte index per row and a second encoding pass. Column checksums are
computed alongside parallel encoding. The frozen compatibility fixture checks the format bytes.

On the same Apple M2 Max and Go 1.27.3, three local runs of the 100,000-row
`BenchmarkSnapshot/columnar/write` (20 iterations each) measured 8.70–10.00 ms
and about 3.65 MB allocated per operation before this change, versus 6.61–6.69 ms
and about 3.06 MB afterward. These short filesystem measurements are sensitive
to system load and apply only to this fixture.
