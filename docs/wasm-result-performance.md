# WASM result transfer

The `cmd/wasm_browser` and `cmd/wasm_node` modules share their Go-to-JavaScript
result bridge. The worker-based `cmd/query_files_wasm` app has a separate,
bounded preview and paging implementation.

`tinySQL.query(sql)` returns the existing native object with `columns`, `rows`,
`count`, `elapsed_ms` and an optional `error`. Despite its historical name,
`elapsed_ms` remains **nanoseconds**, and excludes bridge transfer time. No
caller changes are needed.

For primitive results with at least 32 rows, the bridge uses JSON parsing to
avoid a boundary crossing for every cell. Generic JSON arrays also let the
encoder avoid reflection allocations for common cell values. Large results
are split into blocks of at most 1,024 rows. A conservative upper bound on
JSON-encoded row sizes caps
each block at 256 KiB, accounting for escaping. A single oversized row uses
direct conversion instead of allocating a large JSON buffer. Small responses
that fit the budget retain their single-payload path.

NaN, infinities, negative zero, float32, invalid UTF-8 and native JavaScript
values keep their direct-conversion semantics. Only the affected rows bypass
JSON in large results. Nil outer slices remain `null`; empty result slices and
nil inner rows retain their existing array representation. Integer conversion
still follows JavaScript Number semantics, including rounding outside its safe
integer range.

The row conversion stage allocates one contiguous cell buffer, with each row's
capacity capped to prevent append from overwriting its neighbor. This reduces
allocation count; it does not remove the engine's row maps or make query results
streaming. The full Go result and JavaScript result remain materialized, and
the synchronous call blocks its executing thread. SQL projections and `LIMIT`
remain useful for reducing total memory and latency.

## Reproduce measurements

On Apple M2 Max, Go 1.27.3 and Node 26.13.0, the median of three 500 ms
Go/WASM benchmark runs compared with commit `82f8166` was:

| Case | Before | After |
| --- | ---: | ---: |
| Transfer 200 primitive rows, four columns | 451 µs | 255 µs |
| Transfer 10,000 primitive rows, four columns | 23.0 ms | 14.0 ms |
| Transfer 10,000 rows with one negative-zero cell | 45.6 ms | 13.6 ms |
| Allocate row buffers for 10,000 × 4 cells | 10,001 allocations | 2 allocations |

Transfer measurements exclude SQL execution. They are throughput measurements,
not peak-memory measurements. Total allocated bytes for the large primitive
transfer increased slightly (about 1.66 MB to 1.78 MB per operation) because of
the generic row-array staging; the number of allocations fell from about
40,010 to 20,111. Individual row JSON blocks obey the byte budget above.

A separate end-to-end comparison of 200 queries over 10,000 rows, selecting
three columns with `ORDER BY id`, averaged 35.3 ms before and 29.1 ms after.
Constant queries stayed at approximately 44 µs. These local measurements
depend on the runtime and query shape; they are not performance guarantees.

Run bridge and row-buffer benchmarks in the actual Go/WASM runtime:

```sh
GOOS=js GOARCH=wasm go test ./internal/wasmbridge \
  -exec "$(go env GOROOT)/lib/wasm/go_js_wasm_exec" \
  -bench 'Benchmark(QueryResult|MakeRows)' -benchmem -count=3
```

For an end-to-end query benchmark, build either embedding module and use the
browser benchmark runner; both expose the same query API. The last arguments
are iterations per case and source table rows (default: 1000 and 200).

```sh
GOOS=js GOARCH=wasm go build -tags=tinysql_minimal -trimpath \
  -o /tmp/tinysql-bench.wasm ./cmd/wasm_node
WASM_EXEC_JS="$(go env GOROOT)/lib/wasm/wasm_exec.js" \
  node cmd/wasm_browser/wasm_benchmark.js /tmp/tinysql-bench.wasm 60 10000
```

Run `make wasm-smoke` to check Browser and Node APIs in both full and minimal
profiles, including 5,003-row transfers and the shared SQL semantics contracts.
