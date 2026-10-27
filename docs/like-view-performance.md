# LIKE search and view materialization

LIKE and ILIKE now share one compiled pattern implementation across raw filters,
ordinary expression evaluation and ROW_TO_TEXT predicates. Existing exact,
prefix, suffix and substring paths remain. Patterns such as `%needle%middle%tail`
use ordered literal searches instead of rune-by-rune wildcard backtracking.
The anchored suffix is reserved before searching intermediate parts, preventing
separate literal segments from overlapping.

Escaped literals such as `log\_%` and `%\%%` use the same string-search paths.
Fixed-width `_` patterns decode their wildcard positions. Mixed `_`/`%` patterns
check their fixed-width prefix and suffix directly, then use the general Unicode
matcher for any remaining middle. Patterns with invalid UTF-8 or a literal
replacement character retain the general matcher. Escaped `%` and `_` match
literally, including when searching between wildcards, and a trailing backslash
remains literal.
Raw LIKE filters now stringify non-NULL values like the general evaluator, rather
than silently rejecting every non-string value. Bound pattern parameters stay
dynamic. ILIKE retains its existing Unicode lowercase semantics. Exact, prefix,
suffix and fixed-width patterns compare only the required code points without
allocating a lowercase copy of the source text. Mixed anchors do the same,
lowercasing only the remaining middle when needed. Other shapes may still allocate. Explicit
ESCAPE and GLOB matching keep their existing implementation.

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

## Mixed-pattern anchors (2026-10-27)

A pattern such as `%event_2026.json` has a fixed-width suffix: `_` consumes
exactly one Unicode code point. Checking that suffix from the end avoids
retrying it at every position in the text. Prefixes are consumed first, so
the two anchors cannot overlap. Escapes, invalid input bytes, newlines, NULL
handling, and ILIKE's lowercasing semantics retain their existing behavior.
Patterns without useful anchors still use the general matcher.

The query analyzer identified `SCAN/FILTER/AGGREGATE` as 98.8% of elapsed time
for a COUNT over 10,000 text rows with this predicate. The benchmark uses
527-byte values, half matching, and excludes parsing and fixture construction.

Go 1.27.3, darwin/arm64, Apple M2 Max, GOMAXPROCS=1; medians of three alternating
300 ms runs of separately compiled test binaries. The baseline is `31aa6c1`,
with the same benchmark sources supplied through a Go source overlay.

| Workload | Before | After |
| --- | ---: | ---: |
| COUNT with mixed suffix LIKE, 10,000 rows | 16.47 ms | 0.500 ms |
| Compiled mixed suffix, matching 4 KB text | 17.02 µs | 19.31 ns |
| Compiled mixed suffix, nonmatching 4 KB text | 14.43 µs | 12.51 ns |

These fixtures benefit from avoiding a full text scan. They do not represent
arbitrary LIKE patterns or general query performance. Machine load varied:
the COUNT samples ranged from 15.47–65.21 ms before and 0.281–0.677 ms after.
Unchanged interior-wildcard and parser controls also varied substantially;
their timing differences are not evidence of an improvement. Matcher calls
still allocate nothing. Compiling the two anchors adds two allocations per
COUNT execution (24 → 26, approximately 1.84 → 1.92 KB).

Regression coverage includes generated comparisons with the original rune
matcher, escaped anchors, multibyte characters, invalid UTF-8, and overlapping
anchors. A 15-second fuzz run exercised over 37,000 cases without a mismatch.

```sh
go test ./internal/engine -run TestLikeMixedAnchorQueryProfile -v
go test ./internal/engine -run '^$' -bench '^BenchmarkLikeMixedAnchor' -benchmem -benchtime=300ms -count=3
go test ./internal/engine -run '^$' -fuzz '^FuzzLikeAnchors$' -fuzztime=15s -parallel=2
```

## ILIKE input copies and cache ownership

ILIKE now compares literal and mixed-wildcard suffixes backwards, lowering
only the compared code points. It tracks offsets in the original string:
lowercasing `İ` changes its UTF-8 width, while `_` still consumes one original
code point. Prefix and fixed-width matching use the same approach. Comparisons
retain the existing lowercase semantics, including the distinction between
ordinary and final Greek sigma.

The following measurements compare against the mixed-LIKE implementation
above, using the same Go/runtime settings and three alternating 200 ms samples.
Parsing and fixture construction are excluded.

| Workload | Before | After | Bytes/op before → after |
| --- | ---: | ---: | ---: |
| ILIKE literal suffix, uppercase 4 KB text | 11.76 µs | 13.98 ns | 4,864 → 0 |
| ILIKE mixed suffix, uppercase 4 KB text | 12.09 µs | 31.04 ns | 4,864 → 0 |
| ILIKE Unicode suffix, uppercase 4 KB text | 12.28 µs | 60.33 ns | 4,864 → 0 |
| COUNT with ILIKE mixed suffix, 10,000 rows | 4.233 ms | 0.273 ms | 1,968 → 1,937 |

The COUNT fixture has lowercase input, so its old lowercase conversion usually
allocated nothing but still scanned every byte. Removing that scan is about
15.5× faster for this workload. Case-sensitive COUNT measured 185 → 190 µs
(about 3% slower); interior ILIKE and existing prefix controls were close to
their previous timings. These results are specific to the listed patterns.

Global LIKE and REGEXP caches now clone missed patterns before compilation.
A literal can be a substring of a large SQL statement; retaining that substring
in a map key or compiled matcher kept the complete statement alive. Both the
key and compiled pattern now own the small string. Existing cache hits still
allocate nothing. Ownership tests fail against the original cache sources and
pass with the fix.

```sh
go test ./internal/engine -run 'Test(ILikeAnchorsAvoidInputCopies|PatternCache)' -count=1
go test ./internal/engine -run '^$' -bench '^(BenchmarkILikeAnchors|BenchmarkILikeMixedAnchorScan|BenchmarkPatternCacheHit)$' -benchmem -benchtime=200ms -count=3
```
