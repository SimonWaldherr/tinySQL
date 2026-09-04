# Delayed inserts

Use `DELAY_INSERT` to evaluate INSERT values now and write them to the target
later. The pending values live in a normal, persistent internal table named
`__tiny_delayed_inserts`, scoped to the current tenant. It is hidden from
`sys.tables`, the ordinary table catalog and `sqlite_schema`; it is not a
replacement for access control. Do not modify its schema or rows directly.

```sql
CREATE TABLE documents (id INT PRIMARY KEY, body TEXT);
CALL DELAY_INSERT('10m', 'INSERT INTO documents VALUES (1, ''background text'')');
CALL DELAY_INSERT('2030-01-01T02:00:00Z',
                  'INSERT INTO documents VALUES (2, ''scheduled text'')');
CALL DELAYED_INSERTS();
CALL FLUSH_DELAYED_INSERTS(100);
```

Enqueue returns `id`, `due_at` and `row_count`. Times accept nonnegative Go
durations (`500ms`, `30s`, `2h`) or RFC3339 timestamps, including time zones.
A duration of `0s` makes a job eligible immediately; it still needs a flush.
Absolute dates also allow delays beyond the duration type's range.

```sql
CALL RESCHEDULE_DELAYED_INSERT('id-from-enqueue', '1h');
CALL CANCEL_DELAYED_INSERT('id-from-enqueue');
```

`INSERT ... SELECT ...` is supported: its result is captured at enqueue time,
so subsequent source-table edits do not change pending values. Explicit
expressions are also evaluated at enqueue time. Omitted defaults, coercion,
constraints, triggers and index maintenance run at delivery time through the
ordinary INSERT implementation. `RETURNING` is rejected because target rows
do not exist yet. Unsupported serialized value types fail during enqueue.
A changed target column schema prevents delivery rather than reinterpreting
old values. `ON CONFLICT DO NOTHING` retains its normal INSERT behavior.

## Background processing

Create a bounded recurring job and start the existing scheduler in the host:

```sql
CREATE JOB delayed_ingest SCHEDULE INTERVAL 1000
AS CALL FLUSH_DELAYED_INSERTS(100);
```

```go
if err := tinysql.StartJobScheduler(db, "default"); err != nil {
    return err
}
defer tinysql.StopJobScheduler(db)
```

The scheduler polls once per second. Delivery happens **no earlier than** the
stored timestamp; poll intervals, contention and backlog may make it later.
An inactive scheduler or closed process cannot deliver jobs. Restart the
scheduler after reopening the DB; overdue jobs become eligible again.
With RBAC enabled, provide an authenticated JobExecutor via the database's
`StartJobScheduler` method; the convenience SQL executor has no user identity.
Both the worker's and submitter's INSERT permissions are checked at delivery.
Queue listing/cancellation/rescheduling are restricted to the submitter under
RBAC. Without RBAC these operations are available to the embedding application.

## Resource limits, errors and persistence

- A flush processes at most 1–1000 jobs, earliest due first. Candidate selection
  scans queue metadata but retains at most one batch of job references.
- One job may contain multiple rows. Submit modest batches when ingestion must
  fit a short work window; the job limit is not a row or CPU-time limit.
- Each flush is atomic, including target changes and queue removal. A failed
  job rolls back the entire batch; all jobs remain available. The returned
  error identifies an insert/constraint failure. Cancel or reschedule the
  offending job before retrying. Scheduled failures appear in job history.
- Concurrent flushes are serialized by the database write lock. A successful
  job is removed in the same statement that inserts its values.
- The atomic procedure uses the existing rollback snapshot machinery. It can
  copy database state; AdvancedWAL procedure execution also checkpoints metadata.
  This first implementation shifts expensive target work to a chosen time;
  it does not promise cheaper individual writes. Tune batch size and interval.
- The internal table follows the selected storage mode's durability rules.
  Memory mode requires an explicit snapshot save; merely enqueueing does not
  make an in-memory database durable. Disk/WAL/AdvancedWAL reopen paths are tested.
- Pending payloads use a versioned Gob encoding and carry the target column
  schema. They are internal storage data, not a public serialization format.

The new reopen regression test also exposed and fixes basic WAL appends after
recovery: a fresh Gob encoder must start a fresh stream. Opening a nonempty
basic WAL now checkpoints the recovered state with its sequence watermark
before accepting more writes. This adds a checkpoint to that startup path.
