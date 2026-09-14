# tinySQL Catalog and Scheduler Demo

This example registers tables, views, functions, and scheduled SQL jobs in the
tinySQL catalog. It has a short terminal mode and a browser dashboard for a
running scheduler.

## Run

```bash
# terminal demo; finishes after about six seconds
go run ./cmd/catalog_demo

# browser dashboard
go run ./cmd/catalog_demo -web -addr 127.0.0.1:8089
# http://localhost:8089
```

| Flag | Default | Purpose |
| --- | --- | --- |
| -web | false | Keep the scheduler running and serve the dashboard |
| -addr | 127.0.0.1:8089 | Dashboard listen address |

The terminal demo runs refresh_event_stats every two seconds and runs
integrity_check once. In web mode, refresh_event_stats runs every 15 seconds
and integrity_check runs once after one minute. The dashboard can manually run
only these registered jobs; it accepts no arbitrary SQL.

## What it demonstrates

- catalog registration and inspection for tables, views, functions, and jobs;
- interval and one-time job schedules;
- a JobExecutor that parses and executes catalog SQL;
- scheduler start, stop, status, and recent execution history.

The web mode starts with a fresh in-memory dataset. Tables, job history, and
scheduler state disappear on restart.

## HTTP API

| Method | Path | Purpose |
| --- | --- | --- |
| GET | /healthz | Liveness check |
| GET | /api/state | Catalog, job status, and recent runs |
| POST | /api/jobs/{name}/run | Run a registered job |

```bash
curl -X POST http://127.0.0.1:8089/api/jobs/refresh_event_stats/run
```

The dashboard has no login or TLS. Keep it on loopback, or add authentication
and HTTPS before making it reachable over a network.
