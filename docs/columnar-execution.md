# Columnar execution and compact results

tinySQL keeps tables and persistent formats row-oriented. This feature adds two
query-local optimizations: typed batches for eligible numeric aggregates and an
opt-in column-oriented result API. It does not add disk projection pushdown,
column segments, compression, or an Arrow API.

## Aggregate batching

Eligible direct-column `SUM` and `AVG` queries use batches once their selected
access path has at least 2,048 candidates. A batch contains at most 256 rows.
Filters still run once per candidate before aggregate values are extracted.
Repeated `SUM`, `AVG`, and `COUNT(column)` projections over one column share its
typed `float64` buffer, NULL bitmap, and running state; repeated `COUNT(*)`
shares its counter.

Grouped queries keep existing key semantics and first-seen group order. HAVING,
sorting, LIMIT/OFFSET, decimal promotion, and floating-point accumulation order
retain the ordinary executor's behavior. `EXPLAIN` identifies a batch aggregate
and the possibility of a scalar fallback.

Small candidate sets, COUNT-only queries, MIN/MAX, DISTINCT aggregates, and
expression arguments keep the scalar implementation. Nonnumeric or exact-decimal
runtime values move the current batch and the remainder of the query to that
path without reevaluating predicates. Scratch space is bounded by referenced
columns; group state and materialized output can still grow with their result.

## Compact result API

```go
result, err := tinysql.ExecSQLColumnar(ctx, db, "default",
    `SELECT id, amount FROM sales WHERE amount > 0`)
if err != nil {
    return err
}
for row := 0; row < result.RowCount; row++ {
    fmt.Println(result.Values[0][row], result.Values[1][row])
}
```

`ExecuteColumnar` accepts a parsed SELECT. `ColumnarResultSet` contains `Cols`,
`Values[column][row]`, and `RowCount`; each cell is `any` and SQL NULL is `nil`.
Simple unordered physical-table scans write directly to column slices. Complex
queries—including aggregates, joins, sorts, DISTINCT, CTEs, and alias
collisions—first use the ordinary executor and transpose its final result.

The column API has the same authorization, locking, panic recovery, and audit
behavior as ordinary execution. It rejects non-SELECT statements. Returned
slices belong to the caller and are not reused. Nested cell values follow the
existing row API's ownership rules. Results remain fully materialized; use
`ExecuteStream` when the entire output should not stay resident.

## Verify and measure locally

The focused tests compare columnar and row results, exercise error/permission
paths and concurrent calls, and check output ownership. Run the benchmark source
against a representative schema and result shape; its output is local evidence,
not a portability guarantee.

```sh
go test ./internal/engine -run '^TestColumnar' -count=1
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkColumnar|BenchmarkColumnarOutput|BenchmarkColumnarGrowth|BenchmarkAggregateKernelAblation)$' \
  -benchmem
go test -race ./internal/engine -run '^TestColumnar' -count=1
```
