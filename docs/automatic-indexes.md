# Opt-in automatic index creation

`IndexAdvisor` observes successful queries routed through its `Execute` method.
It aggregates repeated equality-filter patterns and recommends single-column,
non-unique indexes. Automatic creation is **off by default**; normal
`tinysql.Execute`, SQL driver calls and streams are not instrumented.

```go
advisor, err := tinysql.NewIndexAdvisor(db, tinysql.IndexAdvisorOptions{
    MinExecutions:      20,
    MinTableRows:       1024,
    MaxTableRows:       100_000,
    MaxIndexesPerTable: 3,
    MaxCandidates:     256,
    AutoCreate:        false,
})
if err != nil {
    return err
}
stmt, err := tinysql.ParseSQL("SELECT * FROM events WHERE customer_id = 42")
if err != nil {
    return err
}
result, err := advisor.Execute(ctx, "default", stmt)
// Use result / err as with tinysql.Execute.
```

Repeated executions of that filter (including different bound/literal values)
contribute to the same table/column pattern. `advisor.Recommendations()` returns
copied records with tenant, table, column, generated index name, execution count,
total observed query duration, sampled distinct count, status and last error.
It does not record SQL text or bind values. Duration includes lock wait, and is
an observation, not the estimated benefit of an index; a query with several
eligible columns contributes its duration to each pattern.

To apply a recommendation explicitly:

```go
for _, r := range advisor.Recommendations() {
    if r.Status == "ready" {
        if err := advisor.Apply(ctx, r.Tenant, r.Name); err != nil {
            return err
        }
    }
}
```

To enable automatic application, set `AutoCreate: true` at construction or call
`advisor.SetAutoCreate(true)`. Enabling the mode does not immediately execute
pending DDL. The next eligible observed query can create at most one index.
`SetAutoCreate(false)` stops subsequent automatic builds and keeps observation.
There is no background goroutine, periodic job or automatic index deletion.

## Eligibility and limits

The first version handles `column = literal` (either operand order) in simple
single-table SELECTs and AND conjunctions. Aliases and bound `Literal` parameters
are supported. OR, range predicates, joins, CTEs, subqueries, expression indexes,
compound indexes, FTS and vector-index creation are outside this advisor's scope.
Failed queries are not observed. Queries outside the supported shapes execute
normally without generating recommendations.

A candidate must reach the configured successful-execution threshold. The
advisor then checks the table and up to 256 evenly spaced rows. At least 16
distinct non-NULL scalar values are required. Complex value types and sampled
strings longer than 1024 bytes are skipped. This is a conservative cardinality
heuristic, not a query optimizer's full cost model or an index-benefit guarantee.
It can miss skew or unsampled long values; it does not measure write frequency.

Existing secondary indexes whose leading column covers the filter, and PRIMARY
KEY/UNIQUE constraints, count as coverage. Index count limits include manually
created secondary indexes. Row limits bound which tables can be built; they are
not an exact byte budget. Additional index storage and DML maintenance still
cost memory and time. Check representative read/write workloads before enabling
large limits.

Zero-valued limits use the defaults in the example. Negative limits and an
inverted row range are rejected. At the candidate cap new patterns are ignored;
existing counters continue. Ready recommendations are not resampled on every
read. Skipped/covered candidates are reconsidered every `MinExecutions` reads.
Created records are historical and do not recreate an index automatically if
it is later dropped. A fresh advisor starts fresh observation counters.

## Execution, permissions and persistence

The advisor finishes the SELECT before initiating DDL, with the same context
and normal CREATE INDEX authorization. It rechecks schema, coverage, row limits,
index count and the cardinality sample **under the DDL content write lock**.
An already covering index makes `Apply` a successful no-op. Only known
recommendation names for the supplied tenant are accepted.

Creation is synchronous and extends the triggering call's latency. It holds the
normal database write lock and may wait for readers. A cancelled context is
checked before building once the lock is acquired; an already running index
build uses the existing non-interruptible CREATE INDEX implementation. Use
recommendation mode and call `Apply` during a maintenance window if that pause
is unsuitable for request handling.

Automatic build errors are recorded as `failed` with `LastError`; the successful
SELECT result remains successful. Failed candidates are not retried on every
query. Explicit `Apply` reports errors directly and can retry after the cause
is resolved. The caller still needs DDL permission; SELECT permission alone
never grants it. Read-only database rules continue to apply.

Normal CREATE INDEX rollback, auditing, notifications and WAL behavior apply.
Created indexes persist like manually created indexes in the selected storage
mode. Observation counters and configuration are process-local and are not
persisted. The advisor is scoped to one DB and separates candidate records by
tenant. It supports concurrent callers; its own bookkeeping/build requests are
serialized. No advisor state is shared globally between databases.

## Validation and performance

Tests cover advice-only operation, mode switching, repeat Apply, existing
coverage, cardinality/candidate/row limits, stale recommendations, rejected
DDL privileges, tenant separation, concurrency, cancellation, public API use,
result equivalence before/after and WAL close/reopen persistence.

`BenchmarkIndexAdvisor` compares ordinary execution, observation without an
index, and advised execution after index creation on the same deterministic
20,000-row equality lookup. Data generation, parsing and the initial
recommendation are outside timing. `BenchmarkIndexAdvisorBuild` measures the
actual in-memory DDL path plus its guard sample; source setup/observation and
DROP INDEX are excluded. It does not include durable WAL flushing. Build cost
must be amortized by later queries; indexed query latency alone is insufficient
for deciding whether automatic creation is worthwhile.

Local measurement: Apple M2 Max, Go 1.27.1 darwin/arm64, GOMAXPROCS=1,
GOGC=100. Eight 300 ms rounds alternate case order, with no concurrent builds
or tests. Medians:

| Operation | Time | Allocated bytes/op | Allocations/op |
|---|---:|---:|---:|
| Ordinary query before index | 107.05 µs | 1,448 | 7 |
| Advisor, recommendations only | 107.10 µs | 1,616 | 12 |
| Advisor query after index | 1.85 µs | 1,256 | 19 |
| Explicit advised index build | 4.003 ms | 2,437,336 | 80,790 |

Observation adds 168 B/op; its paired timing interval spans no change
(-0.5%, 95% interval [-2.5%, +1.2%]). Indexed execution is 98.3% faster in
this fixture (roughly 58×). At these medians the build amortizes after about
39 subsequent lookups, excluding future write maintenance and persistent I/O.
The default 20-observation threshold is a workload-frequency heuristic, not an
amortization calculation or guarantee that every created index pays off.
The indexed query figure includes the advisor's bookkeeping. Neither build
allocations nor query allocations measure retained index size.

Raw samples, hashes, environment and paired bootstrap intervals are in
[benchmarks/index-advisor](benchmarks/index-advisor). Reproduce with:

```sh
GOCACHE=/tmp/tinysql-columnar-go-cache go test ./internal/engine -run '^$' -c -o /tmp/tinysql-index-advisor.test
python3 -B docs/benchmarks/index-advisor/run.py /tmp/tinysql-index-advisor.test /tmp/index-advisor-results
python3 -B docs/benchmarks/index-advisor/compare.py /tmp/index-advisor-results/raw.txt
```

Validation passed: `go test ./...`, `go vet ./...`, and
`go test -race . ./internal/engine ./internal/storage ./internal/driver`.
The additional intervening-coverage/index-limit test also passes under `-race`.
