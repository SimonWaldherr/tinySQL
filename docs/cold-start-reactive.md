# Cold point reads and reactive SELECTs

## First access after reopening

A persisted secondary index already contains sorted keys and row positions.
Point, prefix and numeric range lookups now binary-search those entries directly
until a mutation requires the runtime skip list. Range queries then walk only
the matching sorted entries. This removes its O(n) reconstruction
from the first point SELECT without changing the index format. Both positive
and negative lookups use this path; concurrent readers do not initialize a
second mutable index. Existing indexes benefit automatically.

New `keyvalue` and `document` profiles also create a persisted unique key index.
For older profile tables, add an ordinary unique index on the key column.
For example: `CREATE UNIQUE INDEX cache_key ON cache(key)`.

Paged Index can use that index to locate the requested row without decoding the
entire table. A resident compatibility table takes precedence over disk roots:
SQL UPDATEs that have not yet been flushed must remain visible to readers.
This optimization also covers prefix and numeric range access to persisted
secondary-index entries. The classic Disk backend still loads whole table files;
it does not become page-oriented through this optimization.

Apple M2 Max, Go 1.27.1, 20,000 keys, GOMAXPROCS=12, one 100 ms sample:

| OpenDB plus first SELECT | Plain primary key | Persisted profile index |
|---|---:|---:|
| Disk | 12.69 ms; 9.67 MB allocated | 12.47 ms; 12.28 MB allocated |
| Paged Index | 10.90 ms; 5.90 MB allocated | 5.11 ms; 0.208 MB allocated |

Fixture creation and Close are excluded. OS file caches are warm: this measures
process-level reopening, not cold physical media. Disk gains no substantial
latency improvement and uses more allocated memory for the additional stored
index. Paged Index improves roughly 2.1x with 96% fewer allocated bytes.

An isolated first index lookup, after index bytes have already been decoded,
changes from 3.14–3.29 ms and 2.41 MB (hydrate, then lookup) to 65–70 ns and 96 B
(binary search). These two 200 ms samples exclude all file I/O and query work;
they are not end-to-end database latency claims.

## Subscribe to a SELECT

```go
sub, err := tinysql.SubscribeSQL(ctx, db, "default",
    "SELECT id, value FROM metrics WHERE value > 0")
if err != nil {
    return err
}
defer sub.Close()

for change := range sub.Changes {
    if change.Initial {
        // Replace your entire local result with change.Added.
    } else {
        // Remove one occurrence of each change.Removed row,
        // then add every change.Added row.
    }
}
return sub.Err()
```

Simple physical-table filters and projections use incremental execution. Views,
joins, CTEs, GROUP BY/HAVING, aggregates, computed projections, window functions,
DISTINCT, set operations and ORDER BY with LIMIT/OFFSET use the general SELECT
executor and multiset result comparison. See [reactive SQL queries](subscribe-sql.md)
for examples, ordering semantics and execution costs.

Every subscription delivers an initial result, including an empty one. Later
messages are multiset deltas, so duplicate projected rows are preserved. There
is no ordering or stable physical row-ID contract. Output rows and their BLOB,
JSON and vector values are copied independently of stored data and subscriber
state. Unsupported custom mutable values terminate the subscription with an
error rather than exposing shared references.

INSERTs evaluate only appended candidates. UPDATEs evaluate only row positions
in the table's bounded update history; its lookup now skips older history and
keeps the most recent update at rollover. DELETEs, expired history or replaced
table objects require a complete rescan and result diff. Transaction commits
that merge a shadow table therefore generally take the rescan path. `ScannedRows`
reports the number of candidates evaluated for a delivered change on the simple
path; it is -1 for general SELECT execution, whose input scan count is unavailable.

Writers signal after releasing the content write lock. Subscribers recheck
permissions and inspect state under the read lock. Failed statements and
rolled-back driver transactions produce no result delta. This is a view of
visible database state, not an external-durability acknowledgement or a WAL feed.
Raw mutation of exported Table fields without the DB content lock/normal version
tracking is outside the subscription contract.

Each subscription has one worker and one buffered output message. Slow readers
never block writers; several commits may coalesce into one delta, so intermediate
states are not guaranteed. The implementation retains the current table reference
and owned matching results, plus bounded pending delivery state. It does not
provide a hard memory budget or a replayable exactly-once change log. After a
restart, create a new subscription and consume its initial snapshot.

Cancel the context or call Close to stop. Closing the DB stops its subscriptions,
including workers waiting on slow readers. Table removal, incompatible result
schema changes, permission errors and evaluation errors close Changes and are
reported through Err. Other tenants/tables may wake the worker, but unchanged
source versions avoid a rescan on the simple path. General SELECTs recompute
on notifications; unchanged results do not produce an output event.

## Incremental execution measurement

A maintained 20,000-row result with one indexed point UPDATE per iteration:

| Update plus result maintenance | Time | Bytes/op | Allocations/op |
|---|---:|---:|---:|
| Force complete rescan/diff | 14.43 ms | 16.84 MB | 202,847 |
| Incremental row update | 3.02 µs | 3,846 B | 56 |

One 200 ms sample on the host above. The benchmark calls result maintenance
directly and includes the SQL UPDATE; it excludes goroutine scheduling and channel
consumer latency. The incremental case verifies that exactly one candidate is
scanned per update, including after update-history rollover.

```sh
go test ./internal/storage -run '^$' -bench '^BenchmarkColdIndexPoint$' -benchmem
go test ./internal/engine -run '^$' \
  -bench '^(BenchmarkKeyValueReopenFirstQuery|BenchmarkSubscriptionPointUpdate)$' -benchmem
go test -race . ./internal/engine ./internal/storage \
  -run 'Test(Subscription|ColdIndex|Specialized|UpdateHistory|ChangeListener)' -cpu=1,36
go test ./...
```

GOMAXPROCS=36 oversubscribes the 12-core test host. Regression tests cover cold
positive/negative lookups followed by mutation, subscriptions with rollback and
commit, immutable output, incremental candidate counts, slow consumers, shutdown,
unsupported SQL, schema removal, and update-history rollover.
