# REGEXP filter performance

`WHERE REGEXP_MATCH(column, 'constant pattern')` now binds the compiled Go
regular expression to the raw-row filter. Each row accesses the column directly
and calls `MatchString`, avoiding generic function dispatch, argument evaluation
and a shared pattern-cache lookup. No query-result cache is introduced.

The existing `REGEXP`, `RLIKE` and `SIMILAR TO` constant-pattern predicates
already bind their patterns this way. Bound parameters remain dynamic rather
than being captured as constants. Other expression shapes use the existing
general evaluator. `REGEXP_EXTRACT` and `REGEXP_REPLACE` are unchanged.

Semantics remain those of Go regexp, including Unicode, inline flags and
zero-length matches. `REGEXP_MATCH(NULL, pattern)` returns false; the infix
predicate retains SQL NULL semantics. Invalid patterns fall back to evaluation
so NULL inputs still short-circuit errors as before.

## Reproduce

```sh
go test ./internal/engine -run 'TestRegexp'
go test ./internal/engine -run '^$' -bench '^BenchmarkRegexpMatchFilter$' -benchmem -count=3 -benchtime=700ms
go test ./internal/engine -run '^$' -bench 'BenchmarkRegexp(MatchFunction|Row)Scan$' -benchmem -count=3 -benchtime=500ms
```

The isolated benchmark compares the generic and bound filters on the same
matching, nonmatching and NULL inputs. The scan benchmark executes a SELECT
against 20,000 rows, including result materialization. Neither benchmark caches
query results; both reuse compiled patterns. Scan timings varied substantially
on the development machine, so they should not be interpreted as a stable
end-to-end speedup percentage.

On Apple M2 Max, darwin/arm64, Go 1.27.1, the isolated benchmark's three-run
medians were 334.4 ns/op (generic) and 151.2 ns/op (bound), approximately 55%
less filter time. Both paths allocated 0 B/op. This measures the filter only,
not a general speedup for extraction, replacement or complete SQL queries.
