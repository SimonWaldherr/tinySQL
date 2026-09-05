# LIKE search and view materialization

LIKE and ILIKE now share one compiled pattern implementation across raw filters,
ordinary expression evaluation and ROW_TO_TEXT predicates. Existing exact,
prefix, suffix and substring paths remain. Patterns such as `%needle%middle%tail`
use ordered literal searches instead of rune-by-rune wildcard backtracking.
The anchored suffix is reserved before searching intermediate parts, preventing
separate literal segments from overlapping.

Patterns containing `_`, backslash escapes, invalid UTF-8 or a literal replacement
character retain the general Unicode matcher. This also fixes the previous
shortcut interpreting an escaped `%` as a wildcard or ignoring escaped backslashes.
Raw LIKE filters now stringify non-NULL values like the general evaluator, rather
than silently rejecting every non-string value. Bound pattern parameters stay
dynamic. ILIKE retains its existing lowercase semantics; lowercasing text may
allocate. Explicit ESCAPE and GLOB matching keep their existing implementation.

Materialized-view refresh now binds column lookup keys once and fills rows in
blocks of roughly 4,096 cells, instead of separately allocating each row and
lowercasing every column name per row. Each row has a capped slice capacity;
appending cannot overwrite its neighbor. A complete fresh table is still published
with Replace, preserving old snapshots and failed-refresh behavior. Refresh is
still a full recomputation, not incremental view maintenance.

Ordinary view result conversion also prepares plain and qualified column keys
once. All these improvements avoid repeated work without adding a result cache.

## Measurements

Apple M2 Max, Go 1.27.1, darwin/arm64. Medians of three 500 ms benchmark runs:

| Benchmark | Before | After | Allocations before → after |
| --- | ---: | ---: | ---: |
| LIKE `%needle%middle%tail`, ~530-byte matching text | 2,406 ns | 208.1 ns | 0 → 0 |
| Full materialized-view refresh, 20,000 rows / 3 columns | 4.485 ms | 2.781 ms | 120,089 → 40,085 |
| Ordinary view result conversion, 20,000 rows / 3 columns | 9.078 ms | 2.773 ms | 280,001 → 40,012 |

The LIKE result is about 11.6× faster for this specific pattern and input. Full
refresh takes about 38% less time, and ordinary view row conversion about 69%
less time. These are scoped benchmarks, not claims about every query or pattern.
The LIKE benchmark reuses its compiled matcher; the refresh benchmark recomputes
the complete view on every iteration.

```sh
go test ./internal/engine -run '^$' -bench 'Benchmark(LikeLiteralSegments|MaterializedViewRefreshRows|ViewResultRows)$' -benchmem -count=3 -benchtime=500ms
```

Regression tests compare 10,000 generated input/pattern pairs in both LIKE and
ILIKE modes against the general matcher. Further cases cover non-overlap,
escaping, NULLs, numeric/blob conversion, prepared patterns, independent row
storage, old snapshot preservation and retaining the last good refresh after
an error. Existing concurrent materialized-refresh tests run with the race detector.
