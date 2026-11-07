# Ordered CTE consumers

Direct-column SELECTs over materialized CTEs now bind ORDER BY to source keys,
select the requested page and only then allocate output maps. The baseline is
`9f522b7`. This extends the existing CTE scan optimization without changing the
producer's materialization or the public API.

## Research applied

| Primary source | Application |
| --- | --- |
| [DuckDB's TopN optimizer](https://duckdb.org/2024/11/14/optimizers) retains only the best candidates for ORDER BY with LIMIT. | Reuse tinySQL's existing bounded heaps and adaptive full-sort cutover, directly on the materialized source rows. Avoid qualifying and projecting every candidate first. |
| [SQLite's deferred-work example](https://www.sqlite.org/optoverview.html#using_co_routines_to_defer_work_until_after_the_sorting) moves expensive output work after sorting and pagination. | Defer allocating direct-column output maps. Expression projections retain their existing evaluation order, including errors and volatile calls in discarded rows. |
| [PostgreSQL's CTE materialization discussion](https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-CTE-MATERIALIZATION) explains reuse and one-time evaluation. | Keep the producer result immutable and materialized once. Consumer ordering cannot cross its LIMIT or change the source order observed by a sibling consumer. |

## Semantics and scope

Ordering binds case-insensitively to output aliases before source columns.
Duplicate aliases retain the last projected value. CTE-name and FROM-alias
qualifiers, positional ORDER BY, ASC/DESC, NULLS FIRST/LAST, stable ties and
OFFSET continue to use the existing rules. Hidden source ordering keys remain
in Row maps but outside ResultSet.Cols, preserving Go and positional-client
result shapes.

WHERE finishes before projection validation. Every matching row is checked for
missing projection values before LIMIT can discard it. Unsupported predicates,
ambiguous dotted source columns, expression/star projections, joins, DISTINCT,
aggregation, windows and compound queries retain their existing paths. LIMIT 0
also uses the general CTE path to preserve validation behavior. Parsed statements
and source maps are not modified; returned maps have independent ownership.

The source ResultSet remains fully materialized. Without WHERE, a bounded page
needs only the sorter's retained candidates and the selected output maps in
addition to that source. WHERE uses an additional slice of row references with
capacity equal to the source row count. Unlimited ordering copies source slice
headers before an in-place sort when no filtered slice already owns them.
Large pages can use the existing full-sort strategy; this is not a streaming
query executor. Allocation measurements below do not measure peak live memory.

## Measurements

Apple M2 Max, Go 1.27.3, darwin/arm64, GOMAXPROCS=12. Medians of three serialized
300 ms samples, parsed statements reused, warm in-memory data. End-to-end query
fixtures include producer materialization but exclude parsing, client transfer
and durable I/O. The consumer fixtures instead start with a precomputed source.

| Fixture, 20,000 source rows | Before | After | Allocated bytes/op before → after |
| --- | ---: | ---: | ---: |
| Query: single sort key, LIMIT 20 | 11.68 ms | 4.55 ms | 27,214,364 → 6,893,041 |
| Query: two sort keys, LIMIT 20 OFFSET 10 | 10.91 ms | 3.25 ms | 27,216,058 → 6,894,747 |
| Query: half the rows match WHERE, two keys, LIMIT 20 | 9.55 ms | 3.11 ms | 23,855,349 → 7,057,951 |
| Query: single sort key, LIMIT 19000 | 9.90 ms | 6.14 ms | 28,016,309 → 14,227,772 |
| Query: single sort key, no LIMIT | 10.02 ms | 5.94 ms | 27,860,664 → 14,580,149 |
| Consumer only: two keys, LIMIT 20 | 8.68 ms | 0.803 ms | 20,329,758 → 9,120 |
| Consumer only: two keys, LIMIT 19000 | 15.05 ms | 10.32 ms | 21,933,639 → 8,145,776 |

The two-key small-page query reduces allocation counts from 160,028 to 40,059;
producer materialization still accounts for most remaining objects. Its isolated
consumer reduces counts from 120,017 to 51. These are fixture-specific results;
the isolated consumer gain does not describe total query latency.

```sh
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkCTEOrdered(Page|Consumer)$' -benchmem -benchtime=300ms -count=3
go test ./...
go test -race ./internal/engine -run 'Test.*(CTE|Sort|Order|Profile)' -count=1
make test-sql-compat
bash scripts/check-wasm.sh
go vet ./...
```

Differential tests compare complete Row maps and column lists with the original
qualified-source/filter/project/sort pipeline. They cover NULLs and tied keys,
alias collisions, hidden keys, large pages, offsets, key-arena boundaries, empty
and sparse rows, matching error messages, cancellation and concurrent statement
reuse. Integration tests check producer LIMIT, sibling reuse and expression
errors outside the returned page. Both full/minimal Browser and Node WASM modules
exercise ordered CTE pagination and duplicate aliases through their public API.
