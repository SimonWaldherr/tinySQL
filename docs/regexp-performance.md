# REGEXP filter performance

`WHERE REGEXP_MATCH(column, 'constant pattern')` binds a compiled boolean matcher
to the raw-row filter. Each row accesses the column directly, avoiding generic
function dispatch, argument evaluation and a shared pattern-cache lookup. No
query-result cache is introduced.

The existing `REGEXP`, `RLIKE` and `SIMILAR TO` constant-pattern predicates
already bind their patterns this way. Bound parameters remain dynamic rather
than being captured as constants. Other expression shapes use the existing
general evaluator and share the same matcher. `REGEXP_EXTRACT` and
`REGEXP_REPLACE` keep the original compiled Go regexp and its capture/match
boundaries; they do not build the boolean optimization unless it is requested.

Literal patterns such as `ERROR_`, `^ERROR_`, `timeout$` and `^ERROR_timeout$`
use direct string searches. Literal runs separated by `.*` use ordered searches
with non-overlapping segments. The optimization analyzes the parsed regexp,
including escaped literals and text anchors. Case folding, multiline anchors,
character classes, alternatives and other unsupported shapes retain Go regexp.
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
go test ./internal/engine -run '^$' -bench '^BenchmarkRegexpLikeSubstr$' -benchmem -count=3 -benchtime=500ms
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

On Apple M2 Max, darwin/arm64, Go 1.27.1, the isolated benchmark's three-run
medians were 334.4 ns/op (generic) and 151.2 ns/op (bound), approximately 55%
less filter time. Both paths allocated 0 B/op. This measures the filter only,
not a general speedup for extraction, replacement or complete SQL queries.
