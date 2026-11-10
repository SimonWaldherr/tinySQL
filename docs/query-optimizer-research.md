# Query optimizer research and large result pages

Research checked on 2026-11-10 against primary documentation and source code.

| Source | Finding | Application in tinySQL |
| --- | --- | --- |
| [PostgreSQL tuplesort](https://github.com/postgres/postgres/blob/master/src/backend/utils/sort/tuplesort.c) | A LIMIT is a hint, not a reason to always use a bounded heap. PostgreSQL considers input size, the bound and available memory when switching algorithms. | Select full sorting for validated raw scans whose LIMIT + OFFSET retains more than 90% of their input. Small pages retain Top-N. The conservative threshold limits extra candidate storage. |
| [PostgreSQL expression execution](https://github.com/postgres/postgres/blob/master/src/backend/executor/README) | Expression preparation resolves invariant work before execution; runtime state is separate from read-only plans. | Reuse `raw_value.go` for projections, predicates, updates and computed raw aggregate arguments. Argument bindings and sorting decisions use execution-local state. |
| [DuckDB execution format](https://duckdb.org/docs/2.0/internals/vector) | Type-homogeneous vectors and constant representations avoid repeated per-value work. DuckDB's default vector size is 2,048. | tinySQL already uses 256-row aggregate batches and validity masks. Changing that size needs its own measurements against row storage. Reuse the existing unboxed float sorter after verifying its input. |
| [SQLite deferred work](https://www.sqlite.org/optoverview.html#using_co_routines_to_defer_work_until_after_the_sorting) | Expensive projections can be moved after sorting and limiting. | Keep projecting only the final page, including when full sorting replaces the heap. Tests distinguish errors in visible and discarded projection rows. |
| [PostgreSQL index ordering](https://www.postgresql.org/docs/current/indexes-ordering.html), [SQLite partial sorting](https://www.sqlite.org/queryplanner.html#partial_sorting_using_an_index_a_k_a_block_sorting) | Matching index order can avoid a sort; matching a prefix can permit smaller block sorts. | Deferred: tinySQL currently preserves scan order in index candidate lists, and some existing key encodings do not implement SQL value order. Index iteration and comparison semantics must agree before adding this optimization. |

The implementation adapts the algorithm-selection idea to tinySQL's existing
in-memory sorter. tinySQL's materialized-row sorter already has a related
large-page decision; this extends it to raw scans and the unboxed float path.
PostgreSQL's memory manager, LLVM execution and DuckDB's
columnar storage are separate designs, not dependencies of this change.

## Compatibility and cost boundaries

The new path applies to at least 64 source rows, an unfiltered scan, direct
column sort keys, and a retained count above the 90% cutover or the entire
input. It checks the current values, accepting NULL plus one consistent
supported type per key: int, int64, float64, text, boolean or BLOB. Each query
can make a different decision after inserts, updates or changed LIMIT/OFFSET.

NaN and mixed types retain the previous algorithm. tinySQL's existing comparison
rules do not form a total order for those values, so switching sorting
algorithms could change results even with scan position as a tie breaker.
Decimals, computed ordering expressions, filtered scans and index candidate
sets also retain their existing paths. Validation checks cancellation and leaves
ragged-row errors to the existing evaluator.

Full sorting skips heap maintenance and then selects the page before allocating
output maps. The float variant also uses the existing scan-position tie breaker
with an ordinary sort after total-order validation. Small and excluded cases
keep the existing stable float heap path. Input rows are never reordered.

Keeping all candidates can require more storage than retaining only LIMIT +
OFFSET. The 90% cutover bounds that increase in candidate entries to roughly a
ninth; allocation rounding and key chunks affect actual bytes. This is a CPU
optimization with a measured memory tradeoff.

## Measurements

Apple M2 Max, Go 1.27.3, darwin/arm64, GOMAXPROCS=12. Fixtures contain
20,000 shuffled rows, including native integer, float and text keys. Both
binaries contain the same benchmark fixtures; SQL parsing and loading are
outside the timed region. Five 300 ms samples per fixture are collected in
alternating before/after process pairs, reversing the pair order on each round.

The baseline is the working tree at the start of this research, including the
preceding standard-query optimizations, rather than a clean released version.
Only `exec_fastpath_select.go` is overlaid for the before build; its frozen
snapshot has SHA-256
`91df67dfe2bce6b937af7b960a2e60a16376feb0dbc6e240bedf5eac3c37aed9`.

Small pages use LIMIT 20 OFFSET 0. Large pages use LIMIT 19000 OFFSET 0;
deep pages use LIMIT 20 OFFSET 19980. The figures are medians, not guarantees
for other data distributions or storage modes.

| Keys / page | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| Float / small | 0.094 ms | 0.093 ms | 8,441 → 8,443 | 47 → 47 |
| Float / large | 7.269 ms | 4.113 ms | 7,302,219 → 7,343,283 | 38,007 → 38,007 |
| Float / deep | 5.354 ms | 2.091 ms | 810,392 → 810,371 | 47 → 47 |
| Integer / small | 0.290 ms | 0.288 ms | 9,193 → 9,193 | 48 → 48 |
| Integer / large | 7.737 ms | 6.178 ms | 7,990,384 → 8,064,031 | 38,008 → 38,007 |
| Integer / deep | 5.234 ms | 4.299 ms | 1,531,323 → 1,531,249 | 48 → 47 |
| Three keys / small | 0.383 ms | 0.354 ms | 10,218 → 10,217 | 49 → 49 |
| Three keys / large | 11.235 ms | 9.561 ms | 8,973,442 → 9,047,094 | 38,013 → 38,012 |
| Three keys / deep | 10.008 ms | 7.585 ms | 2,514,388 → 2,514,308 | 53 → 52 |

Large/deep-page medians improve by about 15–61%. Small-page timing ranges
overlap, so no speedup is attributed to those controls. There is still visible
system noise: integer large-page samples range from 7.27–13.99 ms before and
6.12–7.88 ms after. Float deep-page ranges are 5.19–5.43 ms and 2.08–2.27 ms.
Earlier non-interleaved runs also fluctuated enough to reverse one multi-key
comparison; alternating processes avoids treating that one run as a result.

The large-page fixtures allocate about 41–74 KB more per query for the extra
candidate entries. Deep pages retain the entire candidate set in both versions
and do not have that increase. These measurements exclude persistence, network
transfer and peak live memory.

## Verification

New tests cover the cutover, stable ties, NULL placement, integer/float/text/
boolean/BLOB keys, NaN and mixed-type exclusions, cancellation, ragged inputs,
deep and empty pages, source ownership, and ordering keys spanning multiple
allocation chunks. Projection errors are checked after page selection.

```sh
go test ./internal/engine -run '^TestRawAdaptive' -count=1
go test ./internal/engine -run '^$' -bench '^BenchmarkRawOrderPages$' \
  -benchmem -benchtime=300ms -count=5
go test ./...
go test -race ./internal/engine ./internal/driver ./bindings/c \
  -run 'Test.*(RawAdaptive|Order|Sort|Limit|TopN|CompiledRawValue|CompiledUpdateAcross|Columnar|Stream|Prepare|Parameter)' -count=1
make test-sql-compat
bash scripts/check-wasm.sh
go vet ./...
```
