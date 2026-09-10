# Bounded jobs, reactive queries, and a transactional event log

These APIs serve different contracts:

| API | Purpose | Delivery and recovery |
|---|---|---|
| ResultStream | Consume one query result with backpressure | A query snapshot; no replay cursor |
| SubscribeSQL | Maintain a current query result | Coalesced multiset deltas; start with a new snapshot after reconnect |
| Scheduler | Run scheduled or explicitly submitted SQL | Bounded in-memory work queue; configurable retries and catalog history |
| eventlog | Persist business events and consumer progress | Transactional outbox, replay until acknowledgement, explicit retention |

## Jobs

```go
if err := tinysql.StartJobScheduler(db, "default"); err != nil {
    return err
}
scheduler := db.JobScheduler()
scheduler.SetMaxConcurrentJobs(4)
scheduler.SetMaxQueuedJobs(128)

job, err := jobs.Build(jobs.Config{
    Name: "refresh-report",
    SQL: "REFRESH MATERIALIZED VIEW sales_report",
    ScheduleType: "INTERVAL",
    IntervalMs: 60_000,
    NoOverlap: true,
    MaxAttempts: 3,
    RetryDelayMs: 250,
})
if err != nil {
    return err
}
return scheduler.AddJob(job)
```

The default is eight worker slots and 256 waiting jobs. Workers are created only
when needed, reused across queued work, and exit when idle. Waiting jobs do not
get their own goroutine. Reducing the worker limit lets existing executions
finish; reducing queue capacity preserves already admitted jobs and rejects new
work until capacity becomes available. Zero queue capacity disables waiting.

`SubmitJob(job)` returns a RunID and an explicit error when stopped, overlapping
a NoOverlap job, or overloaded. Scheduled overload is counted and recorded as
REJECTED. Due ONCE jobs stay eligible if admission fails. NoOverlap covers queued
and executing runs. Overlapping runs have distinct IDs and are all canceled by
RemoveJob or Stop. Stop prevents new admission before waiting for workers.

Retry is opt-in: MaxAttempts defaults to one and is bounded at 100. Delay doubles
from RetryDelayMs (default 100 ms), is capped at 30 seconds, and receives jitter
between half and the full delay. The run timeout includes queue time, attempts,
and retry waits. A retry wait occupies its worker slot. Use idempotent SQL or
external operations when enabling retries; an error does not always prove that
no side effect occurred.

Custom executors can call `tinysql.JobRunFromContext(ctx)` to obtain RunID,
Attempt, and an IdempotencyKey that stays stable across automatic attempts.
`RetryJob(name)` manually starts a new logical run with a new key. Terminal
failures stay visible in catalog history; they are not silently requeued forever.
There is no separate RabbitMQ-style dead-letter exchange.

Inspect `scheduler.Stats()` for running/queued counts, limits, oldest queued age,
rejections, retries, completions, cumulative queue wait, and start delays. Query
`catalog.job_history` for per-run attempts, idempotency_key, queued_at,
queue_wait_ms, start_delay_ms, status, and error_message. Catalog job getters now
return owned copies; edit through RegisterJob/UpsertJob rather than mutating a
returned pointer.

Queue admission is not durable acceptance. Pending/running work is process-local;
scheduled definitions and terminal history use the catalog's persistence policy.
Use the event log below for work that must be discoverable after restart.
History retention remains application-managed.

## Reactive queries and pressure metrics

```go
sub, err := tinysql.SubscribeSQLWithOptions(ctx, db, "default", query,
    tinysql.SubscriptionOptions{
        MaxResultRows: 10_000,
        MaxResultBytes: 8 << 20,
    })
```

Limits default to disabled for compatibility. Exceeding one terminates the
subscription with an error before publishing the oversized result. The byte
limit counts JSON-encoded row payloads, not Go heap overhead. These limits cap
retained result size; they are not hard bounds on executor working memory.
A delta can contain both a removed and an added copy of a result, and initial
materialization of a complex query can allocate before a limit is checked.

Known table dependencies are indexed by tenant and table. Views are expanded,
and joins, CTEs, subqueries, and set operations contribute their base tables.
Unknown/dynamic functions and virtual catalog sources retain global wakeups.
Broad notifications rebuild dependencies, so a transactionally replaced view
can switch source tables. Normal scoped DML only wakes interested listeners.
The engine reuses proven rollback-snapshot write sets for this optimization;
DDL, complex trigger/foreign-key writes, transaction merges, and unclassified
writes conservatively broadcast. Failed writes may wake a listener, but unchanged
results still produce no delta.

`sub.Stats()` reports refreshes, full refreshes, delivered changes, notification
and coalescing counts, cumulative refresh time (including lock wait), and delivery
wait time. `ResultStream.Stats()` additionally reports current buffer occupancy,
entries into the blocking send path, and accumulated send wait. Cumulative wait
counters include waits once they finish. These runtime counters reset on restart.

## Transactional event log

The `eventlog` package uses three reserved `_tiny_event_*` SQL tables. Open it on
a correctly configured persistent database/sql connection to retain data across
restart. For example, `driver.Open("file:" + path + "?mode=disk")` selects a
durable backend; an in-memory database remains in memory. No stronger fsync or
replication guarantee is added beyond the database's Commit/durability policy.

```go
log, err := eventlog.Open(ctx, pool)
if err != nil {
    return err
}

tx, err := pool.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()
if _, err = tx.ExecContext(ctx, "INSERT INTO orders VALUES (?)", orderID); err != nil {
    return err
}
if _, err = eventlog.Append(ctx, tx, eventKey, "order.created", payload); err != nil {
    return err
}
return tx.Commit()
```

Append belongs to the caller's transaction. On any error, roll it back. If Commit
reports a transaction conflict, retry the whole business transaction. A sequence
returned by Append is provisional until Commit succeeds. `log.Publish` provides
a standalone transaction when no business change needs to be included.

Consumers explicitly register a persisted position and acknowledge after work:

```go
if err := log.RegisterConsumer(ctx, "invoice-worker", 0); err != nil {
    return err
}
batch, err := log.Read(ctx, "invoice-worker", 100)
if err != nil {
    return err
}
for _, event := range batch.Events {
    // Apply the effect idempotently using event.Key.
}
return batch.Ack(ctx)
```

Read is pull-based and bounded to 1–10,000 events per batch. A slow or disconnected
consumer retains a cursor, not its own in-memory event queue. Before Ack commits,
Read replays from the old cursor. Each consumer name has independent progress.
Multiple processes sharing a consumer name must coordinate work themselves:
Read does not acquire an exclusive processing lease. Concurrent Ack operations
use transaction conflict detection and expected-cursor checks.

Event keys are application-supplied and duplicate keys are allowed. External
effects must deduplicate by that key; crashing after an effect but before Ack can
repeat delivery. This is at-least-once processing, not an exactly-once promise.
The convenience methods retry only definite optimistic transaction conflicts,
up to four total attempts. They return ambiguous commit/I/O errors without
blindly publishing again. Large logs still incur the underlying SQL executor and
transaction snapshot costs; this is not a dedicated high-throughput broker.

`Prune(ctx, throughSequence)` atomically deletes old events and advances the
retention boundary. A lagging consumer receives ErrCursorExpired instead of
silently skipping data. After rebuilding its application state, register a new
consumer name at the corresponding valid cursor. `log.Stats(ctx, consumer)` exposes
the persisted cursor, latest sequence, retention boundary, lag, and expiration.
Applications choose when to prune; no background retention goroutine is created.

## GIS optimization

Planar line/line, line/polygon, and polygon-boundary intersection tests now reject
disjoint axis-aligned bounds before testing every segment pair. Segment tests
also reject disjoint bounds before evaluating orientations. Touching bounds are
included, and exact predicates remain authoritative. Path-level NaN/Inf bounds
disable the shortcut. Containment and polygon-hole checks are preserved.

For disjoint paths, boundary rejection is O(n+m); overlapping bounds can still
require O(n*m) exact tests. No geodesic, antimeridian, or coordinate-system
semantics were changed.

On an Apple M2 Max, Go 1.27.1, two 1,000-segment disjoint ring boundaries took
4.56–4.64 ms before and about 2.35 microseconds after (zero allocations in both).
This is a boundary-test microbenchmark, not an end-to-end spatial SQL speedup.
`BenchmarkDisjointRingBoundaries` reproduces the case. Randomized differential
tests compare the optimized predicate with the original orientation test, and
existing GIS relation tests cover crossings, containment, holes, and boundaries.
