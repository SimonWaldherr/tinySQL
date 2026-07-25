# tinySQL Server SLO Baseline

`cmd/server` exposes Prometheus metrics at `GET /metrics`.

## Core SLIs

- Availability: successful requests / total requests
- Latency: request duration distribution from histogram buckets
- Error rate: non-2xx requests / total requests

## Example PromQL

The examples are split by `protocol` (`http` or `grpc`) because `cmd/server`
records both protocols under the same metric names while their `status` label
values differ: HTTP uses status codes like `200`/`404`/`500`, gRPC uses small
integers (`codes.OK` is `0`, everything else is an error). A `status=~"2.."`
match is meaningless for gRPC.

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

> **gRPC-only deployments** (e.g. `-http= -grpc=:9090`): `tinysql_requests_total`
> and `tinysql_request_duration_seconds` are still populated, but only with
> `protocol="grpc"` series. Alerting rules and dashboards must query
> `protocol="grpc"` (or aggregate across both `protocol` values, as above) —
> rules that hardcode `protocol="http"` will never fire in this configuration.
> See [`alerts.yml`](./alerts.yml) for the paired HTTP/gRPC alert rules.

## Initial targets

- Availability: 99.9% monthly
- p95 latency: < 250ms for `/api/query`
- Error rate: < 0.5% rolling 5m

## Rollout notes

- Enable auth for `/metrics` in public environments.
- Place the service behind a reverse proxy/load balancer.
- Configure `-trusted-proxies` to avoid spoofed client IP headers.
