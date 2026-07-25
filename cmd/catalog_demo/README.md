# tinySQL Catalog & Scheduler Demo (`catalog_demo`)

Registers tables, views, and functions in the tinySQL catalog, then schedules
recurring and one-shot SQL jobs executed by the real tinySQL engine.

## What it shows

- Catalog registration: `RegisterTable`, `RegisterView`, `RegisterFunction`
- Catalog introspection: `GetTables`, `GetColumns`, `ListJobs`
- INTERVAL job: SQL re-run every N milliseconds
- ONCE job: SQL scheduled at an absolute point in time
- A custom `JobExecutor` that parses and executes SQL, printing results
- Scheduler lifecycle: `StartJobScheduler` / `StopJobScheduler`

## Build and run

```bash
go build -o catalog_demo ./cmd/catalog_demo
./catalog_demo
```

No flags. The demo runs for about 6 seconds, prints job output, reports job
status, then exits.

## Expected output (abridged)

```
=== tinySQL Catalog & Scheduler Demo ===

✓ Seeded events table with 20 rows

2. Tables registered in catalog:
   - main.events          (type: table, created: 12:00:01)
   - main.event_stats     (type: table, created: 12:00:01)

3. Columns for 'events':
   - id              INT    (position 0, nullable: true)
   ...

6. Creating scheduled jobs...
   - INTERVAL job "refresh_event_stats" every 2s: SELECT kind, COUNT(*) ...
   - ONCE job "integrity_check" at 12:00:02: SELECT COUNT(*) ...

8. Monitoring jobs for 6 seconds (watch log output)...
   job result (2 rows): kind=click, total=13

9. Job status:
   refresh_event_stats       enabled | last: 12:00:06 | next: 12:00:08
   integrity_check           enabled | last: 12:00:02 | next: n/a

=== Demo Complete ===
```

## Key APIs used

```go
catalog := tdb.Catalog()

catalog.RegisterTable("main", "events", []tinysql.Column{...})
catalog.RegisterView("main", "recent_events", "SELECT ...")
catalog.RegisterFunction(&tinysql.CatalogFunction{...})
catalog.RegisterJob(&tinysql.CatalogJob{
    ScheduleType: "INTERVAL",
    IntervalMs:   2000,
})

tdb.StartJobScheduler(executor)
tdb.StopJobScheduler()
```

See [catalog.go](../../internal/storage/catalog.go) and
[scheduler.go](../../internal/storage/scheduler.go) for the full API.
