# tinySQL Server SLO Baseline

This repository exposes Prometheus metrics at `GET /metrics` on `cmd/server`.

## Core SLIs

- Availability: successful requests / total requests
- Latency: request duration distribution from histogram buckets
- Error rate: non-2xx requests / total requests

## Example PromQL

The examples below are split by `protocol` (`http` or `grpc`) because `cmd/server`
records both protocols under the same metric names, but their `status` label
values are not on the same scale: HTTP uses status codes like `200`/`404`/`500`,
while gRPC uses small integer status codes (`codes.OK` is `0`, everything else
is an error). A `status=~"2.."` match is meaningless for gRPC.

Availability (5m), HTTP:

```promql
sum(rate(tinysql_requests_total{protocol="http",status=~"2.."}[5m]))
/
sum(rate(tinysql_requests_total{protocol="http"}[5m]))
```

Availability (5m), gRPC:

```promql
sum(rate(tinysql_requests_total{protocol="grpc",status="0"}[5m]))
/
sum(rate(tinysql_requests_total{protocol="grpc"}[5m]))
```

Error rate (5m), HTTP:

```promql
sum(rate(tinysql_requests_total{protocol="http",status!~"2.."}[5m]))
/
sum(rate(tinysql_requests_total{protocol="http"}[5m]))
```

Error rate (5m), gRPC:

```promql
sum(rate(tinysql_requests_total{protocol="grpc",status!="0"}[5m]))
/
sum(rate(tinysql_requests_total{protocol="grpc"}[5m]))
```

p95 latency (5m), by protocol and route:

```promql
histogram_quantile(
  0.95,
  sum by (le, protocol, route) (rate(tinysql_request_duration_seconds_bucket[5m]))
)
```

> **gRPC-only deployments** (e.g. started with `-http= -grpc=:9090`): `tinysql_requests_total`
> and `tinysql_request_duration_seconds` are still populated, but only with
> `protocol="grpc"` series. Make sure your alerting rules and dashboards query
> `protocol="grpc"` (or aggregate across both `protocol` values, as above) —
> rules that hardcode `protocol="http"` will never fire in this configuration.
> See `deploy/observability/alerts.yml` for the paired HTTP/gRPC alert rules.

## Initial Targets

- Availability: 99.9% monthly
- p95 latency: < 250ms for `/api/query`
- Error rate: < 0.5% rolling 5m

## Rollout Notes

- Enable auth for `/metrics` in public environments.
- Place service behind a reverse proxy/load balancer.
- Configure `-trusted-proxies` to avoid spoofed client IP headers.
