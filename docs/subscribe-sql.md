# Reactive SQL queries

`SubscribeSQL(ctx, db, tenant, query)` emits an initial result followed by
multiset deltas whenever committed database changes affect that result.

```go
sub, err := tinysql.SubscribeSQL(ctx, db, "default", `
    SELECT category, COUNT(*) AS orders, SUM(amount) AS revenue
    FROM sales
    GROUP BY category
    HAVING SUM(amount) > 100
`)
if err != nil {
    return err
}
defer sub.Close()
for change := range sub.Changes {
    if change.Initial {
        // Replace the local result with change.Added, even when empty.
    } else {
        // Remove one occurrence of each Removed row, then add each Added row.
    }
}
return sub.Err()
```

Use `SubscribeSQLWithOptions` when a retained result needs an explicit cap:

```go
sub, err := tinysql.SubscribeSQLWithOptions(ctx, db, "default", query,
    tinysql.SubscriptionOptions{MaxResultRows: 10_000, MaxResultBytes: 8 << 20})
```

Both limits default to disabled. An oversized result closes the subscription
before publication. `MaxResultBytes` counts JSON-encoded row payloads, so it
caps retained output rather than every allocation made while executing a query.

## Supported queries

Subscriptions accept SELECT statements supported by the SQL executor, including:

- Views, including nested views and views containing aggregates.
- GROUP BY, HAVING, and aggregates such as COUNT, SUM, MIN, MAX, and AVG.
- Joins, DISTINCT, computed projections, and window functions.
- CTEs, derived tables, supported subqueries, and UNION/UNION ALL/EXCEPT/INTERSECT.
- ORDER BY, LIMIT, and OFFSET.

For example, subscribing to `SELECT * FROM sales_summary` tracks changes to the
underlying tables of that view. A top-ten query can use `ORDER BY revenue DESC,
category LIMIT 10`; changes to membership and projected values produce deltas.
Deltas carry no row positions or ordering guarantee. Clients needing an ordered
list must maintain and sort their local result; project the sort keys for that
purpose. A pure order change without a membership/value change produces no delta.

## Execution and ownership

Simple physical-table filters and direct projections retain the incremental
INSERT/UPDATE path. Deletions and expired update history require a full scan.
`ScannedRows` counts evaluated candidate rows on this path.

Simple aggregates over a physical table can maintain results after INSERT,
UPDATE and DELETE. This path supports direct GROUP BY columns and COUNT, SUM,
AVG, MIN and MAX over direct columns (and COUNT(*)), with deterministic,
row-local WHERE predicates. HAVING, ordering, pagination, indexed WHERE plans,
and unsupported types and expressions use the general path. The first refresh
scans the table; updates use the bounded row-update history and recompute
changed groups in source order. A delete compacts physical positions, so the
subscription checks the remaining rows against retained identities, then
recomputes affected groups. Lost update history uses the same reconciliation;
ambiguous identities, reordering, schema changes and table replacement fall
back to full recomputation. Unsupported values also fall back.

`ScannedRows` counts candidates checked plus retained group rows re-evaluated;
it is -1 for full recomputation. The subscription retains source row identities,
matching row values and per-group state in addition to the result. The result
limits do not cap this working memory.

Other SELECTs run through the general executor on each relevant coalesced change
notification. Known base-table dependencies are routed by tenant and table.
Unclassified writes and dynamic query dependencies retain global notifications. Their complete
results are compared as multisets; unchanged rows produce no event, and duplicate
rows retain their multiplicity. This path uses full recomputation; joins are not maintained incrementally. `ScannedRows` is **-1** on this path because the
executor does not expose the number of input candidates. Large joins or aggregate queries outside the incremental subset can therefore
cost substantially more than simple subscriptions.

Each evaluation gets fresh statement-scoped subquery caches. Values such as NOW()
or random functions are evaluated during refresh, not continuously or on a timer.
Their results may change on otherwise unrelated database notifications. Use
deterministic queries when notifications should reflect only stored data changes.

Output rows, BLOBs, JSON values, and vectors are copied independently of storage
and retained subscription state. Unsupported custom mutable values cause an error.
Incompatible result column changes, dropped sources, evaluation failures, and
permission errors terminate the subscription; inspect `Err()` after `Changes`
closes. Non-SELECT statements are rejected.

There is one worker and one buffered result per subscription. Slow readers never
block writers, and multiple commits can coalesce into one result change. A
subscription observes committed state, not every intermediate event, and is not
an audit log. Rollbacks do not publish uncommitted results. Cancel the context,
call `Close()`, or close the database to stop the worker.

`sub.Stats()` reports refreshes, full refreshes, delivered changes,
notifications, coalescing, cumulative refresh time, and delivery wait time.
Counters reset when the process restarts. `ResultStream.Stats()` additionally
reports buffer occupancy and blocking-send wait time.

See [asynchronous runtime](async-runtime.md) for bounded jobs and the separate
transactional event log with replay and acknowledgements.

## Aggregate append benchmark

On an Apple M2 Max with Go 1.27.3, `BenchmarkSubscriptionAggregateAppend`
starts with 20,000 rows and 50 groups, then appends one row per refresh. Three
50-iteration runs measured 442–461 µs per refresh before incremental aggregate
maintenance and 50–61 µs afterward. The measurement includes result-delta
construction, and its cost grows with the number of output groups.

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkSubscriptionAggregateAppend$' -benchmem -benchtime=50x -count=3
```

### Updates, deletions and filters

With 20,000 source rows and 50 groups, three 30-iteration runs on an Apple M2
Max with Go 1.27.3 measured the following refresh times. The benchmark excludes
the mutations themselves. The baseline uses the previous implementation, which
fully recomputes and rebuilds aggregate state after updates and deletes and
fully recomputes filtered aggregates.

| Change | Previous | Incremental |
| --- | ---: | ---: |
| Update one row | 1.20–1.54 ms | 87–93 µs |
| Delete one row and append one | 1.19–1.21 ms | 0.80–0.84 ms |
| Append one matching row with WHERE | 393–408 µs | 56 µs |

The deletion path still examines the remaining table and its benefit depends on
table size and affected group sizes. Re-aggregation preserves source order for
floating-point results.

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkSubscriptionAggregateMutation$' -benchmem -benchtime=30x -count=3
```
