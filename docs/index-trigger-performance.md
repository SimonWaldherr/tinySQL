# Index creation and INSERT-trigger allocation

`CREATE INDEX` now builds only the requested secondary index. Previously it
inserted the definition into the table's index map and called the general
rebuild routine, rebuilding every existing index too. The new implementation
publishes the new index only after a successful build; a failed unique-index
build leaves existing index structures intact. Bulk builds resolve column
positions once, while preserving canonical key encoding and uniqueness checks.
The persisted index format is unchanged.

For multi-row INSERTs with triggers and without RETURNING, one statement-local
NEW-row map is filled again after each row's synchronous BEFORE/AFTER trigger
execution has finished. Nested INSERTs have their own map. INSERTs with RETURNING
continue to allocate independent row maps so later rows cannot overwrite earlier
results. Both all-column and explicit-column INSERT paths use this optimization.
UPDATE and DELETE behavior is unchanged.

Tests cover existing-index identity, failed unique-index creation, index lookup
and cloning, trigger batches, nested trigger execution, defaults, RETURNING and
correlated subqueries through the general expression evaluator. The latter uses
a CASE wrapper because the existing raw-expression fast path does not resolve
trigger bindings inside subqueries; that separate limitation is unchanged.

## Reproduce

```sh
go test ./internal/storage ./internal/engine -run '^$' \
  -bench '^(BenchmarkCreateAdditionalIndex|BenchmarkTriggerBoundedInsert)$' \
  -benchmem -benchtime=700ms -count=3
```

The index fixture has 10,000 rows, four existing indexes on distinct-valued IDs,
and creates/drops a fifth index on a 100-value category column. The trigger fixture
inserts 100 source rows per operation, each producing an audit INSERT; source and
audit rows are cleared between operations to keep the working set bounded. It
calls the INSERT handler directly, excluding top-level rollback snapshot cost.
Neither benchmark measures network latency or durable WAL commit throughput.

## Local measurements

Apple M2 Max, Go 1.27.1, darwin/arm64; medians of three 700 ms runs, compared
with the parent implementation using the same benchmark fixtures:

| Operation | Before | After |
| --- | ---: | ---: |
| Add a fifth secondary index | 15.778 ms | 1.945 ms |
| Index build allocated bytes | 5,057,793 | 221,137 |
| Index build allocations | 161,127 | 1,107 |
| 100-row INSERT with audit trigger | 104.752 µs | 71.110 µs |
| Trigger batch allocated bytes | 46,690 | 13,425 |
| Trigger batch allocations | 606 | 408 |

The measured median latency reductions are about 88% and 32%; allocation bytes
fall by about 96% and 71%. Timings varied between runs (optimized index build
1.51–2.03 ms, trigger batch 61–102 µs). Gains depend on the number and cardinality
of existing indexes and the number of rows per statement. Single-row triggers,
trigger bodies dominated by other work, and INSERT RETURNING will not get the
same map-reuse benefit. This change makes no claim about faster index seeks.
