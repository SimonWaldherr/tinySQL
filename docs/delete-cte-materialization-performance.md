# DELETE, CTE and table materialization performance

These changes reduce intermediate objects in DELETE, WITH consumers,
INSERT INTO ... SELECT and CREATE TABLE AS SELECT, including TEMP tables.
The baseline is `82577b9`. Public result shapes, trigger/constraint execution,
WAL handling and existing temporary-table visibility/lifetime are preserved.

## Research applied

The following engineering decisions adapt guidance from primary sources to
tinySQL:

| Guidance | Application in tinySQL |
| --- | --- |
| [DuckDB binds column references before execution](https://duckdb.org/docs/2.0/internals/overview). | Qualified CTE references build one execution-local schema lookup, lazily. Repeated references reuse it instead of scanning/lowercasing every column again. Shared parsed statements remain immutable. |
| [SQLite describes early filtering and the restrictions on pushing predicates through LIMIT](https://www.sqlite.org/optoverview.html#the_predicate_push_down_optimization). | Qualified CTE filters run against the already materialized source before output maps are allocated. DELETE creates RETURNING maps only for matches. The CTE producer's LIMIT and evaluation boundary remain intact. |
| [PostgreSQL explains how CTE reuse protects one-time evaluation, including volatile functions](https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-CTE-MATERIALIZATION). | CTE producer results remain shared within one execution. Regression tests check inner LIMIT, nested scope and reuse of a UUID-producing CTE. |
| [DuckDB uses bounded data chunks and vectors](https://duckdb.org/docs/1.5/internals/vector). | CTAS reuses tinySQL's existing 64-row allocation blocks. INSERT SELECT reuses one row of expression slots. Both adapt the principle to tinySQL's row engine. |
| [PostgreSQL recommends efficient bulk paths, prepared statements and current statistics](https://www.postgresql.org/docs/current/populate.html), and [measuring actual plans](https://www.postgresql.org/docs/current/using-explain.html). | The selected-row adapter keeps the existing single-statement mutation path. The staged-load example below creates an index and refreshes statistics; benchmarks include correctness checks and separate allocation from latency results. |

## Execution and compatibility

DELETE with a simple predicate and no row triggers now uses the existing raw
filter even with RETURNING. Survivor order and the established DELETE version
semantics remain unchanged. A failing RETURNING projection rolls back the
deletion; regression tests reopen both WAL modes to check recovery.

Qualified CTE filters bind copied expression nodes to plain source keys.
Unknown qualifiers, dotted output-column ambiguities and unsupported shapes use
the general executor. LIMIT 0 retains its existing validation behavior. No
predicate is moved inside the CTE producer.

INSERT SELECT's adapter is now proportional to output width rather than rows
times width. Literal slots are consumed synchronously and overwritten for the
next selected row; stored rows, RETURNING maps and delayed-insert payloads retain
values, not those slots. The SELECT result itself remains materialized, keeping
self-inserts and multiple consumers stable. VALUES inserts still use their
original expression objects. The cursor retains the SELECT result until it is
consumed; lower total allocation does not establish lower peak live memory for
every destination width or default configuration.

CTAS packs nonoverlapping, capacity-capped row slices into blocks of up to 64
rows and checks cancellation while copying. A table is published after the copy
finishes. Cells retain their existing reference semantics. Fewer objects can
mean more bytes because of allocator size classes and partial blocks; a retained
row can keep its block alive. There is no cross-statement pooling.

The CTAS parser also accepts WITH directly after AS, using the same parser as
INSERT SELECT and views. This additive syntax works for ordinary and TEMP
tables, including IF NOT EXISTS. Existing table persistence rules are unchanged.

## Measurements

Apple M2 Max, Go 1.27.3, darwin/arm64, GOMAXPROCS=12. Three serialized 300 ms
samples, medians below. Queries reuse parsed statements and in-memory storage;
these measurements exclude parsing, client transfer, durable I/O and fsync.

| Fixture | Before | After | Allocated bytes/op before → after |
| --- | ---: | ---: | ---: |
| DELETE one of 20,000 rows, RETURNING id | 6.17 ms | 0.251 ms | 14,840,033 → 969,906 |
| Qualified CTE filter/page over 20,000 rows | 7.90 ms | 2.32 ms | 20,446,463 → 6,892,462 |
| INSERT SELECT 10,000 two-column rows | 1.91 ms | 1.73 ms | 5,234,040 → 4,128,358 |
| CTAS 10,000 two-column rows | 1.28 ms | 1.25 ms | 4,009,169 → 4,050,899 |
| TEMP CTAS, same fixture | 1.28 ms | 1.20 ms | 4,009,168 → 4,050,898 |
| Bind/project 128 qualified columns, 20 pre-materialized rows | 1.00 ms | 0.102 ms | 464,690 → 209,144 |

The wide-binding comparison isolates the additional research-driven binding
change; its baseline already includes the qualified-filter optimization.
It reduces allocations from 33,112 to 348. CTAS reduces allocations from 30,013
to 20,170 while increasing allocated bytes by about 1%; timing ranges in other
runs overlap, so its clearest benefit is object-count reduction. INSERT SELECT
also has overlapping timing ranges; its allocation reduction is more robust.
The two-column VALUES control retains 77,864 bytes and 1,023 allocations/op.
The CTAS fixture includes dropping the new table between iterations.

```sh
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkDeleteReturningScan|BenchmarkCTEQualifiedFilter|BenchmarkInsertSelectedRows|BenchmarkCreateSelectedRows|BenchmarkCTEWideBinding)$' \
  -benchmem -benchtime=300ms -count=3
go test ./internal/engine -run 'Test(DeleteReturning|CTE.*Qualified|SelectedInsertCursor|CreateSelectedRows)' -count=1
make wasm-smoke
```

## Staged-load example

This supported workflow is exercised by the regression suite:

```sql
CREATE TEMP TABLE research_stage AS
WITH source(id,label) AS (SELECT 1,'a' UNION ALL SELECT 2,'b')
SELECT s.id AS id,s.label AS label FROM source s WHERE s.id>0;
CREATE INDEX research_stage_id ON research_stage(id);
ANALYZE research_stage;
EXPLAIN ANALYZE SELECT label FROM research_stage WHERE id=2;
```

Keep index creation and ANALYZE decisions workload-specific. The example retains
uniqueness, foreign-key checks, statement rollback and durability.
