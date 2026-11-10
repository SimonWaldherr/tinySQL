# CASE and COALESCE execution

The raw expression compiler binds CASE, COALESCE, IFNULL and NVL operands to
column positions once per execution. They compose with arithmetic, comparisons,
static conversions and IS NULL/IS NOT NULL expressions. Eligible SELECT
projections, WHERE predicates, DISTINCT, streamed and columnar results, and raw
aggregate arguments share this path. UPDATE binds eligible SET expressions when
at least 64 candidate rows can amortize setup; smaller updates retain their
previous evaluator.

Binding prepares callbacks without evaluating operands. CASE evaluates its
operand once, checks WHEN expressions in order, and evaluates only the selected
THEN or ELSE expression. Simple CASE uses the existing comparison rules,
including NULL not matching NULL; searched CASE retains three-valued truth
conversion. COALESCE and its aliases stop at the first value that is not SQL
NULL, preserving its type. Zero, false, empty strings and typed nil Go values
remain non-NULL. Existing argument-count behavior is unchanged.

Every child must be supported for the enclosing conditional to use the bound
path. Otherwise the complete conditional retains its interpreter, preserving
lazy unknown-name errors and the statement clock. Unsupported scalar functions,
dynamic conversion declarations and Boolean AND/OR expressions remain examples
of this fallback. Parameters are read afresh during execution; bound evaluators
do not add mutable state to shared parsed statements. UPDATE assignments still
read the original row and use the existing rollback and index-maintenance paths.

This extends [standard SQL expression binding](standard-query-performance.md)
without changing SQL syntax or the public API.

## Measurements

Measured on 2026-11-10 with Go 1.27.3, darwin/arm64, Apple M2 Max and
GOMAXPROCS=12. The baseline is `200f73b`. A Go source overlay restores its
`raw_value.go`, `exec_fastpath_aggregate.go` and `exec_dml_update.go` while keeping
identical benchmark fixtures. Before and after executables run serially in five
alternating process pairs, reversing their order each pair, with 300 ms per
fixture. Tables are in memory and statements are parsed before timing. CASE and
NULL-function scans contain 50,000 native integer rows with 10% NULLs; the UPDATE
changes 20,000 rows using the existing float-valued performance fixture.

| Workload | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| SUM(CASE WHEN v > 50 THEN v ELSE 0 END) | 2.156 ms | 1.421 ms | 1,824 → 2,032 | 20 → 27 |
| SUM(CASE v WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 0 END) | 1.945 ms | 1.456 ms | 1,824 → 2,016 | 20 → 28 |
| COUNT WHERE CASE WHEN v IS NULL THEN FALSE ELSE v > 95 END | 2.018 ms | 0.864 ms | 2,456 → 2,080 | 27 → 29 |
| Grouped CASE SUM with HAVING | 4.386 ms | 3.424 ms | 3,355,311 → 3,355,516 | 6,063 → 6,070 |
| UPDATE SET val = CASE WHEN id > 10000 THEN val + 1 ELSE val - 1 END | 2.501 ms | 1.883 ms | 3,599,042 → 3,598,302 | 20,413 → 20,425 |
| SUM(COALESCE(v, 0)) | 1.672 ms | 0.991 ms | 2,080 → 2,176 | 22 → 26 |
| SUM(IFNULL(v, 0)) | 1.731 ms | 0.993 ms | 2,080 → 2,176 | 22 → 26 |
| SUM(COALESCE(v, LENGTH(LOWER(body)))), interpreter fallback | 6.402 ms | 6.416 ms | 1,402,359 → 1,402,400 | 15,022 → 15,024 |
| Grouped COUNT/SUM with HAVING, control | 2.180 ms | 2.171 ms | 3,722,087 → 3,722,118 | 6,069 → 6,069 |
| COUNT WHERE ISNULL(v), control | 0.202 ms | 0.206 ms | 2,104 → 2,104 | 23 → 23 |
| COUNT WHERE v IS NULL, control | 0.200 ms | 0.199 ms | 2,104 → 2,104 | 23 → 23 |
| COUNT WHERE NOT ISNULL(v), control | 0.200 ms | 0.200 ms | 2,104 → 2,104 | 23 → 23 |
| COUNT(NULLIF(v, 1)), control | 1.989 ms | 2.007 ms | 2,056 → 2,056 | 21 → 21 |
| SUM(IF(ISNULL(v), LENGTH(LOWER(body)), v)), control | 7.260 ms | 7.453 ms | 1,402,328 → 1,402,319 | 15,022 → 15,022 |

Values are medians of the five samples. These fixtures take roughly 41% less
time for COALESCE, 43% for IFNULL, 34% for searched CASE SUM, 25% for simple CASE
SUM, 57% for the CASE filter, 22% for the grouped CASE SUM and 25% for the CASE
UPDATE. COALESCE ranges are 1.663–1.716 ms before and 0.987–1.019 ms after;
CASE-filter ranges are 1.991–2.039 ms and 0.855–0.882 ms. Control medians move
by up to about 3%, with overlapping timing ranges. No gain is attributed to
those controls or the complex COALESCE fallback.

The speedup reduces per-row interpretation and column lookup. Binding adds
small per-execution allocations: four for the simple COALESCE/IFNULL aggregates,
seven for searched CASE SUM and eight for simple CASE SUM. An unsupported
COALESCE fallback also costs two additional setup allocations before retaining
the interpreter. Intermediate numeric boxing and UPDATE bookkeeping remain;
these measurements establish neither a general reduction in peak live memory
nor a speedup for arbitrary SQL expressions. They exclude parsing, persistence
and client transfer.

## Verification

Differential tests compare compiled values and errors against the interpreter
for NULL, integer limits, large integers, REAL, NaN/Inf, decimal, text, Boolean
and BLOB inputs. They cover argument counts, typed nils, short rows, missing
names, parameter changes, unsupported-child fallback and unselected failing
branches. Query tests exercise materialized, streamed, columnar and aggregate
results, CASE/COALESCE composition, and concurrent parsed-statement reuse.
Bulk UPDATE tests verify original-row assignment semantics, secondary indexes
and rollback after earlier rows changed. WASM smoke checks exercise both
conditionals and their composition in browser and Node modules, in full and
minimal builds.

```sh
go test ./internal/engine -run '^$' \
  -bench '^Benchmark(CaseHaving|NullFunctions|ConditionalUpdate)$' \
  -benchmem -benchtime=300ms -count=5
go test ./...
go test -race ./internal/engine ./internal/driver ./bindings/c \
  -run 'Test.*(Case|Coalesce|Null|Conditional|Compiled|Aggregate|Update|Parameter|Prepare|Columnar|Stream)' -count=1
make test-sql-compat
bash scripts/check-wasm.sh
go vet ./...
```
