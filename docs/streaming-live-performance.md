# Streaming, scheduled jobs, and live-query performance

## Changes

Buffered result streams first try a nonblocking send before waiting for the
consumer. Cancellation is checked before accepting another row. A projected row
that cannot be sent is returned to the row pool, and EOF clears the stream's
reference to its last consumed row. Buffer capacity and backpressure remain
unchanged; there is no unbounded queue.

General SubscribeSQL refreshes reuse one temporary projection map when comparing
rows. Retained snapshots and emitted rows still receive independent copies.
This reduces allocation for large unchanged results, including refreshes caused
by changes to unrelated tables. Complex SELECTs still require full execution;
this change does not implement incremental joins or aggregates.

Interval jobs with CatchUp enabled skip missed ticks arithmetically while
preserving the existing cadence: select the first scheduled tick at or after
now, with at least one interval after LastRunAt. They do not execute every missed
tick. Calculating the next tick takes at most two arithmetic steps for elapsed
times within time.Duration's range, instead of iterating over every missed tick.
Intervals that would overflow time.Duration are rejected by this calculation.
The interval scheduler's existing one-second polling cadence is unchanged.

Cron registration now parses the expression once and registers that same schedule
with its configured timezone. Previously, the displayed next-run calculation
used the job timezone but registration reparsed the expression in UTC. Explicit
TZ/CRON_TZ prefixes in the expression take precedence over the job's Timezone.
Invalid configured timezones retain the UTC fallback. Tests cover summer/winter
offsets and explicit timezone overrides.

## Local measurements

Apple M2 Max, darwin/arm64, Go 1.27.1, GOMAXPROCS=12. Two one-second benchmark
samples per case, run before and after without parallel test workloads. These
are local microbenchmarks, not end-to-end service latency guarantees.

| Benchmark | Before | After |
|---|---:|---:|
| Stream all 20,000 rows | 5.99–6.00 ms | 4.97–5.06 ms |
| Unchanged general subscription, 20,000 rows | 13.35–13.48 ms | 11.27–11.37 ms |
| Allocated bytes per unchanged refresh | 16.96 MB | 10.24 MB |
| Allocations per unchanged refresh | 220,076 | 180,078 |
| Next interval after a day offline, 100-ms cadence | 9.95–10.19 ms | 76–77 ns |

The streaming benchmark uses the default 64-row buffer and consumes every row.
The subscription benchmark uses `SELECT id + 1 AS next_id FROM stream_rows` and
includes full query execution plus result comparison. The scheduler benchmark
measures deadline calculation only, excluding job execution and polling latency.

Reproduce with:

```sh
go test ./internal/engine ./internal/storage -run '^$' \
  -bench 'Benchmark(ExecuteStreamAllRows|SubscriptionGeneralUnchanged|SchedulerCatchUp)$' \
  -benchtime=1s -count=2
```
