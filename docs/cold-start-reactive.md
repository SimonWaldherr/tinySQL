# Cold point reads after reopening

Persisted secondary indexes keep sorted keys and row positions. On first access
after reopening, point, prefix, and numeric-range lookups binary-search those
entries instead of rebuilding the mutable runtime index. A later mutation can
still switch to the normal runtime path. Positive and negative lookups share the
same behavior, and concurrent readers do not initialize duplicate indexes.

`keyvalue` and `document` tables create a persisted unique key index. For an
older profile table, add an equivalent ordinary index, for example:

```sql
CREATE UNIQUE INDEX cache_key ON cache(key);
```

`ModePagedIndex` can then retrieve the matching row without decoding the whole
table. A resident compatibility table still wins so unflushed SQL writes remain
visible. `ModeDisk` continues to decode whole table files on a cold access.

## What the local benchmark measured

Apple M2 Max, Go 1.27.1, 20,000 keys, `GOMAXPROCS=12`; one warm-file-cache,
100 ms sample. Fixture construction and close are excluded, so this is process
reopen behavior rather than cold physical-media latency.

| Open plus first SELECT | Plain primary key | Persisted profile index |
| --- | ---: | ---: |
| Disk | 12.69 ms; 9.67 MB allocated | 12.47 ms; 12.28 MB allocated |
| Paged Index | 10.90 ms; 5.90 MB allocated | 5.11 ms; 0.208 MB allocated |

The profile index materially helps Paged Index (about 2.1× latency and 96%
fewer allocated bytes in this fixture); Disk does not gain a comparable latency
improvement. After index bytes are decoded, an isolated first lookup changed
from 3.14–3.29 ms and 2.41 MB to 65–70 ns and 96 B. Neither result is an
end-to-end request-latency claim.

## Reactive queries

Use [reactive SQL queries](subscribe-sql.md) for `SubscribeSQL` examples,
supported query shapes, output ownership, resource limits, monitoring, and
coalescing.
Simple physical-table filters and projections use incremental INSERT/UPDATE
maintenance; complex queries, deletes, expired update history, and replaced
table objects can require a full result recomputation. `ScannedRows` reports
candidate rows on the incremental path and is `-1` for general execution.

## Reproduce

```sh
go test ./internal/storage -run '^$' -bench '^BenchmarkColdIndexPoint$' -benchmem
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkKeyValueReopenFirstQuery|BenchmarkSubscriptionPointUpdate)$' -benchmem
go test -race . ./internal/engine ./internal/storage \
  -run 'Test(Subscription|ColdIndex|Specialized|UpdateHistory|ChangeListener)' -cpu=1,36
go test ./...
```

Regression coverage includes cold positive/negative lookups followed by
mutation, profile indexes, subscription rollback/commit behavior, immutable
output, update-history rollover, slow consumers, shutdown, and schema changes.
