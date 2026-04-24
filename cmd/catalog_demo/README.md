# tinySQL Catalog & Scheduler Demo (`catalog_demo`)

Demonstrates the tinySQL **catalog** and **job-scheduler** APIs. It registers
tables, views, and functions in the catalog, then schedules recurring and
one-shot SQL jobs backed by the real tinySQL execution engine.

## What it shows

| Concept | Description |
|---------|-------------|
| Catalog registration | `RegisterTable`, `RegisterView`, `RegisterFunction` |
| Catalog queries | `GetTables`, `GetColumns`, `GetTables` introspection |
| INTERVAL job | SQL query re-run every N milliseconds |
| ONCE job | SQL query scheduled at an absolute point in time |
| JobExecutor | Custom executor that parses and executes SQL, printing results |
| Scheduler lifecycle | `Start` / `Stop` a `storage.Scheduler` |

## Build

```bash
go build -o catalog_demo ./cmd/catalog_demo
```

## Run

```bash
./catalog_demo
```

No flags needed. The demo runs for about 6 seconds, prints job output, reports
job status, then exits.

## Expected output (abridged)

```
=== tinySQL Catalog & Scheduler Demo ===

✓ Seeded events table with 20 rows

1. Registering tables in catalog...

2. Tables registered in catalog:
   - main.events          (type: table, created: 12:00:01)
   - main.event_stats     (type: table, created: 12:00:01)

3. Columns for 'events':
   - id              INT    (position 0, nullable: true)
   - kind            TEXT   (position 1, nullable: true)
   ...

4. Registering views...
   - registered view: recent_events

5. Registering functions...
   - registered function: json_get

6. Creating scheduled jobs...
   - INTERVAL job "refresh_event_stats" every 2s: SELECT kind, COUNT(*) ...
   - ONCE job "integrity_check" at 12:00:02: SELECT COUNT(*) ...

7. Starting scheduler (jobs will execute real SQL)...

8. Monitoring jobs for 6 seconds (watch log output)...
   job result (2 rows): kind=click, total=13
   ...

9. Job status:
   refresh_event_stats       enabled | last: 12:00:06 | next: 12:00:08
   integrity_check           enabled | last: 12:00:02 | next: n/a

10. Stopping scheduler...

=== Demo Complete ===
```

## Key APIs used

```go
catalog := db.Catalog()

catalog.RegisterTable("main", "events", []storage.Column{...})
catalog.RegisterView("main", "recent_events", "SELECT ...")
catalog.RegisterFunction(&storage.CatalogFunction{...})
catalog.RegisterJob(&storage.CatalogJob{
    ScheduleType: "INTERVAL",
    IntervalMs:   2000,
})

scheduler := storage.NewScheduler(db, executor)
scheduler.Start()
scheduler.Stop()
```

See [catalog.go](../../internal/storage/catalog.go) and
[scheduler.go](../../internal/storage/scheduler.go) for the full API.
