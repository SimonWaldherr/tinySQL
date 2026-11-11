# Conditional, NULL and text expression execution

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

The follow-up below also binds ISNULL, NULLIF, IF/IIF, computed BETWEEN,
TRIM/LTRIM/RTRIM, LENGTH/LEN/CHAR_LENGTH and LOWER/UPPER. Static conversions can
consume these bound expressions. This lets nested COALESCE expressions retain
their short-circuit behavior while avoiding repeated interpretation of text
and NULL-function operands.

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

## NULL and text follow-up (2026-11-11)

The added bindings retain the original helper functions for NULLIF comparison,
IF truth conversion, BETWEEN results, trimming and case conversion. LENGTH's
NULL, BLOB-byte and Unicode-character behavior is shared with its interpreter.
TRIM still distinguishes an omitted cutset from an explicit empty cutset,
evaluates the input before the cutset, and skips the cutset for NULL inputs.
NULLIF evaluates both operands even when the left value is NULL. Computed
BETWEEN evaluates its comparand once, then both bounds in their original order.
Plain-column BETWEEN retains its existing specialized filters.

Binding requires every child to be supported and keeps predicates in their
existing cost tier, preserving AND/OR evaluation order. Invalid arities,
unknown names, dynamic conversion declarations and statement-clock expressions
retain the interpreter. Parameter values remain execution-time reads. Updates
with at least 64 candidates also bind these expressions; tests cover original
row values, late errors, index restoration and rollback.

Differential tests exposed a pre-existing panic comparing a decimal with NaN,
infinity or a nil rational. Unrepresentable decimal conversions now fail
normally, and NULLIF returns an error instead of dereferencing a nil rational.
NULLIF reports these comparison errors. Typed nil inputs retain their existing
non-NULL interface semantics for ISNULL and COALESCE.

The following comparison uses Go 1.27.3, darwin/arm64, Apple M2 Max and
GOMAXPROCS=12. The before build restores `raw_value.go`, `raw_conversion.go`,
`builtin_string.go`, `exec_fastpath_aggregate.go` and `exec_dml_update.go` from
`9dfb267` with a source overlay. Other changes, including storage and dependency
updates, remain identical in both builds. Five serialized before/after process
pairs reverse order each round, with 300 ms per fixture. SQL parsing and loading
are outside timing.

The new scalar fixture scans 50,000 integer/Unicode-text rows, with 10% NULL
numeric values. The existing NULL-function fixture has 50,000 rows and a longer
ASCII payload. BETWEEN controls scan 100,000 float-valued rows; edge-trimming
controls scan 10,000 long-text rows.

| Workload | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| COUNT(ISNULL(v)) | 1.669 ms | 0.876 ms | 2,081 → 2,125 | 21 → 23 |
| COUNT(NULLIF(v,1)), Unicode fixture | 2.068 ms | 1.135 ms | 2,082 → 2,149 | 21 → 24 |
| COALESCE with LENGTH/LOWER fallback, Unicode fixture | 3.041 ms | 1.711 ms | 242,375 → 242,358 | 10,024 → 10,028 |
| IF with ISNULL and LENGTH/LOWER, Unicode fixture | 4.087 ms | 1.802 ms | 242,321 → 242,389 | 10,022 → 10,029 |
| COUNT WHERE NULLIF(v,1) IS NULL | 1.597 ms | 0.618 ms | 2,792 → 2,219 | 29 → 27 |
| COUNT WHERE COALESCE(v,0) BETWEEN 40 AND 60 | 1.657 ms | 0.620 ms | 2,825 → 2,331 | 31 → 32 |
| COUNT WHERE COALESCE(v,0) NOT BETWEEN 40 AND 60 | 1.682 ms | 0.629 ms | 2,825 → 2,331 | 31 → 32 |
| TRIM(body) equality filter | 2.605 ms | 1.486 ms | 803,135 → 802,593 | 50,029 → 50,026 |
| TRIM(body,' x') equality filter | 3.209 ms | 2.041 ms | 803,147 → 802,613 | 50,029 → 50,027 |
| TRIM(body,cut) equality filter | 3.617 ms | 2.211 ms | 803,150 → 802,629 | 50,029 → 50,027 |
| SUM(LENGTH(TRIM(body,cut))) | 7.459 ms | 3.060 ms | 802,810 → 802,567 | 50,023 → 50,026 |
| LOWER(UPPER(body)) equality filter | 15.026 ms | 12.153 ms | 4,804,728 → 4,804,198 | 200,032 → 200,030 |
| SUM(TRY_CONVERT(INT,COALESCE(NULLIF(v,1),0))) | 4.292 ms | 1.802 ms | 2,121 → 2,305 | 22 → 29 |
| COUNT(NULLIF(v,1)), existing ASCII fixture | 2.074 ms | 1.107 ms | 2,056 → 2,128 | 21 → 24 |
| COALESCE with LENGTH/LOWER fallback, existing ASCII fixture | 6.465 ms | 5.132 ms | 1,402,514 → 1,402,450 | 15,024 → 15,028 |
| IF with ISNULL and LENGTH/LOWER, existing ASCII fixture | 7.606 ms | 5.439 ms | 1,402,352 → 1,402,410 | 15,022 → 15,029 |
| LTRIM long-text filter | 0.506 ms | 0.295 ms | 162,450 → 162,097 | 10,025 → 10,024 |
| RTRIM long-text filter | 0.534 ms | 0.298 ms | 162,448 → 162,097 | 10,025 → 10,024 |
| WHERE ISNULL(v), control | 0.205 ms | 0.204 ms | 2,104 → 2,104 | 23 → 23 |
| WHERE v IS NULL, control | 0.207 ms | 0.205 ms | 2,104 → 2,104 | 23 → 23 |
| WHERE NOT ISNULL(v), control | 0.201 ms | 0.209 ms | 2,104 → 2,104 | 23 → 23 |
| SUM(COALESCE(v,0)), control | 1.010 ms | 1.037 ms | 2,176 → 2,176 | 26 → 26 |
| SUM(IFNULL(v,0)), control | 1.028 ms | 1.018 ms | 2,176 → 2,176 | 26 → 26 |
| Plain BETWEEN, literal bounds, control | 0.583 ms | 0.612 ms | 2,656 → 2,656 | 24 → 24 |
| Plain BETWEEN, parameter bounds, control | 0.671 ms | 0.669 ms | 2,768 → 2,768 | 26 → 26 |

NULLIF uses about 45–47% less time, computed NULL checks 61%, computed BETWEEN
63%, trim filters 36–44%, and composed LENGTH/TRIM 59% in these fixtures.
Nested COALESCE uses about 21–44% less time depending on payload; nested IF
uses 29–56%. NULLIF's ASCII-fixture ranges are 1.998–2.196 ms before and
1.053–2.003 ms after. Computed BETWEEN ranges are 1.617–1.785 ms and
0.618–0.638 ms. Control medians move by about -1% to +5%, with overlapping
ranges and unchanged allocation counts; no speedup is attributed to them.

Binding adds small per-statement setup costs, such as three allocations for
NULLIF and seven for the composed IF or conversion. Returned strings and
numeric boxing still dominate allocation for many text workloads. This is
primarily a CPU improvement, not a general reduction in peak live memory or a
promise of faster arbitrary SQL expressions.

The complete combined working tree passes the normal and minimal Go suites,
the full race suite with twin verification enabled, JSON v2 storage/engine
tests, SQL semantics in UTC/Berlin/Kiritimati, all 13 nested Go modules, SQL
package compatibility, and browser/Node WASM in full and minimal builds.
Module tidy checks, checksum verification and go vet also pass. Networked
integration tests require permission to bind their local test listeners.

```sh
go test ./internal/engine -run '^$' \
  -bench '^Benchmark(ScalarPipeline|NullFunctions|TrimFilterScan|BetweenCountScan)$' \
  -benchmem -benchtime=300ms -count=5
go test ./...
go test -tags=tinysql_minimal ./...
TINYSQL_VERIFY_ROW_TWINS=1 go test -race ./...
GOEXPERIMENT=jsonv2 TINYSQL_VERIFY_ROW_TWINS=1 go test ./internal/storage ./internal/engine
make test-semantics test-sql-compat modules-tidy-check modules-verify
bash scripts/check-wasm.sh
go vet ./...
```
