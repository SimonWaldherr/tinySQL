# Numeric DISTINCT and general JOIN allocation

Single-column SELECT DISTINCT now deduplicates float64 values by numeric bit
keys. Homogeneous float64 COUNT(DISTINCT) inputs, including grouped aggregates,
use the same representation. This avoids formatting every input row and
allocating a string for each newly encountered float value.

The keys preserve the existing formatting equivalence: +0 and -0 remain
distinct, all NaN signs and payloads share one key, and finite values and
infinities retain their own keys. SELECT DISTINCT keeps its type distinctions,
the first original value and encounter order. It does not replace a returned
NaN with a canonical NaN. Multi-column DISTINCT retains its existing keys.

COUNT(DISTINCT) retains its different, text-based equivalence across types.
Float-first inputs keep their numeric keys after a type change and compare
canonical text only for new values. Thus a float value and its exact formatted
text still count together, including NaN and signed zero, without copying all
previous float keys on a late type change. Parsing alone is insufficient:
noncanonical text such as '1.00' or a rounded large integer must remain distinct
from nearby floats. Integer-first inputs retain their existing text migration
on a type change. NULL remains excluded from COUNT and included once in SELECT
DISTINCT.

General row-map equi-join candidate lookups also avoid unnecessary allocations.
Homogeneous int or int64 keys borrow the boxed input values; mixed int/int64
inputs normalize after validation. Candidate buckets retain their existing
storage and input order. Lookup admission, numeric precision, NULL exclusions,
fallback errors, cancellation and FULL JOIN unmatched-right bookkeeping retain
their existing rules. Raw cached inner-join indexes use their existing path.

These are internal execution changes with no new SQL syntax or public API.

## Measurements

Measured on 2026-11-11 with Go 1.27.3, darwin/arm64, Apple M2 Max and
GOMAXPROCS=12. The baseline is `0da0fc8`, using identical benchmark fixtures.
Source overlays restore the three changed executor files for the before build.
Both executables are built from an isolated copy of the baseline plus these
changes, excluding unrelated working-tree edits. Before and after processes run
serially in five alternating pairs, reversing their order each round, with
300 ms per fixture. Values below are medians, with parsing and loading outside
timing.

The numeric SQL fixtures scan 50,000 rows, with either 1,000 repeated values or
50,000 unique values. Float cells are native float64 values ending in .5;
integer controls are native ints. Grouped queries contain 100 groups. Mixed
fixtures replace one float with text matching the first value, either near the
start or on the last row. Replacing a unique value leaves 49,999 distinct
outputs in those mixed fixtures.

| SQL workload | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| Float COUNT(DISTINCT), 1,000 values | 2.964 ms | 1.226 ms | 116,202 → 76,404 | 1,043 → 42 |
| Float SELECT DISTINCT, 1,000 values | 2.682 ms | 0.938 ms | 470,945 → 428,633 | 3,035 → 2,037 |
| Float grouped COUNT(DISTINCT), 1,000 values | 3.504 ms | 1.695 ms | 159,217 → 135,216 | 2,053 → 953 |
| Float COUNT(DISTINCT), 50,000 values | 5.921 ms | 3.163 ms | 3,893,946 → 2,366,928 | 50,296 → 295 |
| Float SELECT DISTINCT, 50,000 values | 11.691 ms | 8.527 ms | 23,185,082 → 21,335,653 | 150,302 → 100,304 |
| Float grouped COUNT(DISTINCT), 50,000 values | 7.108 ms | 4.142 ms | 5,921,637 → 3,835,235 | 52,353 → 2,253 |
| Mixed COUNT(DISTINCT), repeated values, early type change | 2.954 ms | 1.251 ms | 116,136 → 76,635 | 1,043 → 45 |
| Mixed COUNT(DISTINCT), repeated values, late type change | 2.985 ms | 1.202 ms | 116,143 → 76,635 | 1,043 → 45 |
| Mixed COUNT(DISTINCT), unique fixture, early type change | 5.977 ms | 4.915 ms | 3,893,848 → 2,367,182 | 50,295 → 298 |
| Mixed COUNT(DISTINCT), unique fixture, late type change | 5.947 ms | 3.073 ms | 3,893,844 → 2,367,179 | 50,295 → 298 |
| Integer COUNT(DISTINCT), 1,000 values, control | 1.181 ms | 1.188 ms | 76,387 → 76,387 | 42 → 42 |
| Integer SELECT DISTINCT, 1,000 values, control | 1.174 ms | 1.207 ms | 463,180 → 463,179 | 2,037 → 2,037 |
| Integer grouped COUNT(DISTINCT), 1,000 values, control | 1.729 ms | 1.707 ms | 133,617 → 135,216 | 953 → 953 |
| Integer COUNT(DISTINCT), 50,000 values, control | 3.057 ms | 3.053 ms | 2,366,922 → 2,366,924 | 295 → 295 |

Float COUNT(DISTINCT) medians use about 47–59% less time, SELECT DISTINCT
27–65%, and grouped COUNT(DISTINCT) 42–52% in these fixtures. The mixed fixtures
use about 18–60% less time. Repeated float COUNT ranges are 2.829–3.015 ms before
and 1.144–1.311 ms after; repeated SELECT DISTINCT ranges are 2.598–2.765 ms
and 0.874–0.960 ms. Integer-control medians move by less than 3%, with
overlapping timing ranges and unchanged allocation counts. Grouped integer
plans allocate about 1.6 KB more across 100 groups for the larger execution
state; no speedup is attributed to the integer controls.

The direct JOIN fixture builds candidate lookups over two 400-row inputs with
one, 40 or 400 distinct keys. End-to-end joins use the existing 10-, 100- and
400-row fixtures; ORDER BY or an outer join forces the general row-map path.
Their key cells are float64 values, making them controls for integer key reuse.

| JOIN workload | Before | After | Bytes/op before → after | Allocs/op before → after |
| --- | ---: | ---: | ---: | ---: |
| Build lookup, 400 unique native-int keys | 33.788 µs | 31.438 µs | 40,952 → 38,648 | 694 → 406 |
| Build lookup, 40 native-int keys | 30.704 µs | 30.353 µs | 45,368 → 45,368 | 206 → 206 |
| Build lookup, 400 unique int64 keys, control | 31.298 µs | 31.851 µs | 38,648 → 38,648 | 406 → 406 |
| Build lookup, 400 unique text keys, control | 31.771 µs | 32.463 µs | 38,648 → 38,648 | 406 → 406 |
| Ordered inner join, 10 rows, control | 12.895 µs | 12.807 µs | 14,824 → 14,824 | 100 → 100 |
| Ordered inner join, 400 rows, control | 359.922 µs | 365.186 µs | 604,260 → 604,261 | 3,626 → 3,626 |
| Left join, 400 rows, control | 345.619 µs | 350.336 µs | 590,662 → 590,661 | 3,624 → 3,624 |
| Right join, 400 rows, control | 346.628 µs | 348.884 µs | 590,724 → 590,725 | 3,625 → 3,625 |
| Full join, 400 rows, control | 347.692 µs | 355.102 µs | 594,212 → 594,213 | 3,626 → 3,626 |

The unique native-int build saves 288 allocations and 2,304 bytes by reusing
input boxes. Its timing median is about 7% lower, but ranges overlap and the
after samples contain a 90 µs outlier (before range 33–34 µs, after 30–90 µs).
No general JOIN throughput gain is claimed. The other direct and end-to-end
JOIN control medians move by about 3% or less, and their allocation counts stay
unchanged. These are execution-allocation measurements, excluding persistence,
client transfer and peak live memory.

## Cost boundaries

COUNT(DISTINCT) adds one map field to its execution state. Float-first mixed
inputs can retain both numeric keys and text keys for the same value. New
non-float values also need a canonical-text check against the float set.
Integer-first mixtures keep the previous text path. The specialization does
not establish a general reduction in peak live memory or a speedup for every
mixed-type input.

JOIN key reuse avoids integer boxing for homogeneous native-int inputs.
Int64, text and float keys already reused their input values. Mixed int/int64
keys still require conversion and an extra checked pass; no speedup is promised
for those inputs. Candidate slices, large result maps and other executor
allocations remain.

## Verification

COUNT(DISTINCT) insertion tests compare every result to valueText-based
deduplication, rotating float-only and mixed sequences. They include random
float bit patterns, NaN payloads, signed zeros, infinities, subnormal values,
large integers, exact decimals, text, booleans and BLOBs. Query tests compare
single-column DISTINCT to the general evaluator, including original float
bits, type boundaries, filtering, ordering and pagination. Grouped and
ungrouped counts also run through both query paths and after input changes.

JOIN tests compare native ints, int64s, mixed representations and unsupported
domains against nested loops. Interleaved duplicates verify both candidate
order and complete FULL JOIN results after integer normalization.
Existing cancellation and row-limit tests remain in force. WASM smoke checks
cover float DISTINCT, grouped counts and interleaved JOIN matches in browser
and Node modules, each in full and minimal builds.

```sh
go test ./internal/engine -run '^$' \
  -bench '^Benchmark(DistinctNumericSQL|DistinctFloatMixedSQL|JoinCandidateBuild|SmallGeneralJoin)$' \
  -benchmem -benchtime=300ms -count=5
go test ./...
go test -race ./internal/engine ./internal/driver ./bindings/c \
  -run 'Test.*(Distinct|Join|Count|Aggregate|Columnar|Stream|Parameter|Prepare|Update|Snapshot|Transaction)' -count=1
make test-sql-compat
bash scripts/check-wasm.sh
go vet ./...
```

The final validation commands run in an isolated copy of the committed
repository plus this patch, including its original dependency definitions.
