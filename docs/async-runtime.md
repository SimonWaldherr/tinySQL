# Jobs and transactional event log

These APIs solve different delivery problems. For live query results, use the
separate [reactive SQL guide](subscribe-sql.md).

| API | Purpose | Recovery model |
| --- | --- | --- |
| `ResultStream` | Consume one query result with backpressure | Query snapshot; no replay cursor. |
| [`SubscribeSQL`](subscribe-sql.md) | Maintain a current query result | Coalesced multiset deltas; reconnect from a fresh snapshot. |
| Scheduler | Run scheduled or submitted SQL | Bounded process-local work queue; persisted definitions and history. |
| `eventlog` | Persist business events and consumer progress | Transactional outbox; replay until acknowledgement. |

## Jobs

```go
if err := tinysql.StartJobScheduler(db, "default"); err != nil {
    return err
}
scheduler := db.JobScheduler()
scheduler.SetMaxConcurrentJobs(4)
scheduler.SetMaxQueuedJobs(128)

job, err := jobs.Build(jobs.Config{
    Name: "refresh-report", SQL: "REFRESH MATERIALIZED VIEW sales_report",
    ScheduleType: "INTERVAL", IntervalMs: 60_000,
    NoOverlap: true, MaxAttempts: 3, RetryDelayMs: 250,
})
if err != nil {
    return err
}
return scheduler.AddJob(job)
```

The defaults are eight worker slots and 256 queued jobs. Workers are created on
demand and reused; queued work has no goroutine of its own. Reducing either
limit preserves work already admitted and rejects later work when necessary.
`SubmitJob` returns a RunID or an overload/stopped/overlap error. `NoOverlap`
covers queued and running instances; `RemoveJob` and `Stop` cancel both.
Rejected scheduled work is recorded, and a due one-time job stays eligible when
admission fails.

Retries are opt-in: `MaxAttempts` defaults to one and is capped at 100. Delays
start at 100 ms by default, double up to 30 seconds, and include jitter. Queue
time, attempts, and retry delays count toward the run timeout. Use idempotent
SQL or external effects: an error does not prove that a side effect was absent.
`JobRunFromContext` provides a stable idempotency key across automatic retries;
`RetryJob` starts a new logical run with a new key.

`scheduler.Stats()` reports queue pressure, waits, retries, and completions;
`catalog.job_history` records individual attempts. Scheduled definitions and
terminal history follow catalog persistence, but pending and running work is
process-local. Use the event log when unfinished work must be discoverable after
a restart. Retention of job history remains an application decision.

## Transactional event log

The `eventlog` package uses three reserved `_tiny_event_*` SQL tables. Open it
on a persistent `database/sql` database if it must survive restart; its
durability is exactly the database's commit policy.

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

Append belongs in the business transaction. Roll back on error; when a commit
reports a transaction conflict, retry the whole transaction. Its sequence is
provisional until commit succeeds. `Publish` is the standalone alternative when
there is no business write to include.

Consumers register a persistent cursor, read a bounded batch (1–10,000 events),
apply effects idempotently using the event key, and acknowledge only after the
effect succeeds:

```go
if err := log.RegisterConsumer(ctx, "invoice-worker", 0); err != nil {
    return err
}
batch, err := log.Read(ctx, "invoice-worker", 100)
if err != nil {
    return err
}
for _, event := range batch.Events {
    // Apply an idempotent external effect using event.Key.
}
return batch.Ack(ctx)
```

This is at-least-once delivery. A crash after an effect and before `Ack` can
replay an event; duplicate event keys are allowed. Consumers sharing a name must
coordinate their own work because `Read` provides no lease. The convenience
methods retry only definite optimistic conflicts, never ambiguous I/O/commit
results. A disconnected consumer retains only its cursor, and a read before
acknowledgement replays from that cursor; each consumer name has independent
progress.

`Prune(ctx, throughSequence)` atomically advances retention. A consumer behind
that boundary receives `ErrCursorExpired`, rebuilds its state, and registers a
new cursor. `log.Stats` reports cursor, latest sequence, retention boundary,
lag, and expiration. There is no automatic pruning or background retention
worker.
