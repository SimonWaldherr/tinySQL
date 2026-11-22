# REGEXP performance

`WHERE REGEXP_MATCH(column, 'constant pattern')` binds a compiled boolean matcher
to the raw-row filter. Each row accesses the column directly, avoiding generic
function dispatch, argument evaluation and a shared pattern-cache lookup. No
query-result cache is introduced.

The existing `REGEXP`, `RLIKE` and `SIMILAR TO` constant-pattern predicates
already bind their patterns this way. Bound parameters remain dynamic rather
than being captured as constants. Other expression shapes use the existing
general evaluator and share the same matcher. `REGEXP_MATCH`, `REGEXP_EXTRACT`
and `REGEXP_REPLACE` can also bind constant patterns in raw-row projections.
Extraction and replacement use string operations for plain literal patterns
where applicable. Capture substitutions and empty matches use Go regexp.
Replacement can return the original string if a required literal prefix is
absent.

Literal patterns such as `ERROR_`, `^ERROR_`, `timeout$` and `^ERROR_timeout$`
use direct string searches. Literal runs separated by `.*` use ordered searches
with non-overlapping segments. The optimization analyzes the parsed regexp,
including escaped literals and text anchors. Short, fixed-width ASCII classes
such as `number [0-9]{3}0` can use byte masks when the pattern has an anchor or
literal prefix. Case folding, multiline anchors and variable-width classes use
Go regexp.
The same fixed-width matcher supplies the first match for `REGEXP_EXTRACT`
and successive non-overlapping matches for `REGEXP_REPLACE` with literal
replacement text. Capture substitutions use Go regexp.
Dot-star matching respects newlines: `(?s)^ERROR_.*timeout$` permits them,
while `^ERROR_.*timeout$` does not.

Semantics remain those of Go regexp, including Unicode, inline flags and
zero-length matches. `REGEXP_MATCH(NULL, pattern)` returns false; the infix
predicate retains SQL NULL semantics. Invalid patterns fall back to evaluation
so NULL inputs still short-circuit errors as before.

## Reproduce

```sh
go test ./internal/engine -run 'TestRegexp'
go test ./internal/engine -run '^$' -bench '^BenchmarkRegexpMatchFilter$' -benchmem -count=3 -benchtime=700ms
go test ./internal/engine -run '^$' -bench 'BenchmarkRegexp(MatchFunction|Row)Scan$' -benchmem -count=3 -benchtime=500ms
go test ./internal/engine -run '^$' -bench '^BenchmarkRegexpProjection$' -benchmem -count=4 -benchtime=150ms
go test ./internal/engine -run '^$' -bench '^BenchmarkRegexpLikeSubstr$' -benchmem -count=3 -benchtime=500ms
go test ./internal/engine -run '^TestTextSearchAlternativeFilters$'
go test ./internal/engine -run '^$' -bench '^BenchmarkTextSearchAlternatives$' -benchmem -count=3 -benchtime=300ms
```

The isolated benchmark compares the generic and bound filters on the same
matching, nonmatching and NULL inputs. The scan benchmark executes a SELECT
against 20,000 rows, including result materialization. Neither benchmark caches
query results; both reuse compiled patterns. Scan timings varied substantially
on the development machine, so they should not be interpreted as a stable
end-to-end speedup percentage.

The REGEXP/LIKE/SUBSTR comparison uses equivalent prefix, suffix and combined
prefix/suffix filters on the same 50,000-row log fixture. It includes NULLs,
matching and nonmatching inputs, reuses parsed statements and compiled patterns,
and executes `COUNT(*)` to limit result-materialization overhead. Separate tests
check that the predicates select the same rows, including Unicode and newline
cases. SUBSTR is an extraction function: these comparisons include its argument
evaluation, substring construction and comparison, rather than measuring only
the byte comparison. Negative positions currently count source characters too.

Additional comparisons cover `CONTAINS`, `STARTS_WITH`, `ENDS_WITH`, `INSTR`,
`POSITION`, `LOCATE`, `LEFT`, `RIGHT` and `GLOB`. Each group uses equivalent
positive WHERE predicates over TEXT/NULL values and verifies identical counts
before timing. `CONTAINS_ANY`/`CONTAINS_ALL` ignore case, so their comparison uses
ILIKE OR/AND expressions. Plain CONTAINS, STARTS_WITH and ENDS_WITH are
case-sensitive literal searches: percent signs and underscores need no escaping.

These equivalences do not extend to every expression context. In particular,
CONTAINS/STARTS_WITH/ENDS_WITH return false for NULL arguments, whereas LIKE
returns SQL UNKNOWN; negation can therefore select different rows. GLOB uses
`*` and `?` as wildcards. FTS_MATCH tokenizes, stems and removes stop words and
supports word/phrase/boolean queries, so it is not a literal substring substitute
and is not included in the equivalent-filter timings. LEVENSHTEIN measures edit
distance and is useful for typo tolerance rather than wildcard matching.

On Apple M2 Max with Go 1.27.4 and `GOMAXPROCS=2`, four alternating runs of the
20,000-row scan and projection workloads recorded these medians. Projection
times include constructing the result rows.

| Workload | Before | After |
| --- | ---: | ---: |
| Fixed-width `REGEXP` filter | 1.92 ms | 0.68 ms |
| Literal `REGEXP_EXTRACT` | 6.11 ms | 2.37 ms |
| Literal `REGEXP_REPLACE` | 8.15 ms | 4.04 ms |
| `REGEXP_REPLACE` with absent prefix | 6.53 ms | 3.14 ms |

A separate comparison against the boolean-only optimization used the same
20,000-row fixture and the pattern `number [0-9]{3}0`. Reusing the matcher
for extraction reduced the median from 4.51 to 2.82 ms. Replacing matches
with `number #` took 3.06 ms, down from 6.05 ms, and used about 62,000 fewer
allocations per query.
