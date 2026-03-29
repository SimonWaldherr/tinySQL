# tinySQL Server SLO Baseline

This repository exposes Prometheus metrics at `GET /metrics` on `cmd/server`.

## Core SLIs

- Availability: successful requests / total requests
- Latency: request duration distribution from histogram buckets
- Error rate: non-2xx requests / total requests

## Example PromQL

Availability (5m):

```promql
sum(rate(tinysql_requests_total{protocol="http",status=~"2.."}[5m]))
/
sum(rate(tinysql_requests_total{protocol="http"}[5m]))
```

HTTP error rate (5m):

```promql
sum(rate(tinysql_requests_total{protocol="http",status!~"2.."}[5m]))
/
sum(rate(tinysql_requests_total{protocol="http"}[5m]))
```

p95 latency (5m):

```promql
histogram_quantile(
  0.95,
  sum by (le, route) (rate(tinysql_request_duration_seconds_bucket{protocol="http"}[5m]))
)
```

## Initial Targets

- Availability: 99.9% monthly
- p95 latency: < 250ms for `/api/query`
- Error rate: < 0.5% rolling 5m

## Rollout Notes

- Enable auth for `/metrics` in public environments.
- Place service behind a reverse proxy/load balancer.
- Configure `-trusted-proxies` to avoid spoofed client IP headers.
