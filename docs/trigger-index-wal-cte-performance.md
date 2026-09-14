# Trigger, range-index, WAL, and recursive CTE execution

This note records execution-path changes and their contracts. Measure them with
the current Go benchmark sources on the intended workload; no historical result
files are required to use or verify the features.

## Trigger execution

Trigger runners reuse their `NEW`/`OLD` binding object across rows in one
statement. Calls stay synchronous, nested DML receives its own runner, and row
maps plus `RETURNING` ownership keep their existing behavior. Parsed programs
and trigger caches are unchanged.

## Numeric range indexes

Secondary-index range seeks use bounded scratch for an equality prefix, numeric
bounds, and the combined seek key. Larger prefixes grow normally. Persisted
sorted arrays and mutable skip lists retain validation, table-order sorting, and
independently owned row-ID results. The change affects temporary keys, not index
format or search complexity.

## Advanced WAL checksums

AdvancedWAL writes the same CRC32C byte stream with contiguous header and
reusable scratch for headers, values, and column descriptors. A checksum writer
accepts strings directly. WAL format, locking, transaction ownership, flush
policy, and checkpoint behavior remain unchanged.

## Recursive CTEs

Recursive CTEs prepare their positional column mapping once per execution and
reuse it across frontiers. A changed output schema rebuilds that mapping. Each
result row remains independently owned with the same aliases, NULL handling,
and fallback lookup; the shared parsed AST gains no mutable execution state.

## Verify and measure locally

Focused tests cover nested triggers, rollback, CTE aliases and limits, range
boundaries/type safety, WAL corruption/recovery, and byte-identical checksums.
Use the direct benchmark sources to compare a change on the target hardware.

```sh
go test ./internal/engine ./internal/storage -run 'Test(Trigger|RangeIndex|RecursiveCTE|WAL)' -count=1
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkTrigger(Batch|Bounded)Insert|BenchmarkRecursiveCTEChain)$' -benchmem
go test ./internal/storage -run '^$' \
  -bench '^(BenchmarkRangeSeekScratch|BenchmarkAdvancedWAL(CalculateChecksum|CalculateChecksumBlob|LogInsert))$' \
  -benchmem
go test -race ./internal/engine ./internal/storage -run 'Test(Trigger|RangeIndex|RecursiveCTE|WAL)' -count=1
```
