# Clusters and load balancing

`cmd/server` supports a single writable primary and multiple read-only replicas
over HTTP and gRPC. The primary uses `advanced_wal`; each replica bootstraps a
complete in-memory snapshot and applies committed WAL batches. A load balancer
can distribute independent reads across those replicas.

This is asynchronous replication. Replicas can briefly return older data.
There is no leader election, automatic promotion, multi-primary writing, sharding,
or distributed transaction protocol. Keep exactly one writer for a database.
Each process owns its storage: never open the same writable path from several
processes or mount a shared writable volume across nodes. Replicas re-bootstrap
on restart; their memory is not a durable failover copy.

## Start a local cluster

From the repository root, with Docker Compose available:

```bash
export TINYSQL_AUTH_TOKEN="$(openssl rand -hex 32)"
docker compose -f deploy/cluster/compose.yaml up --build -d
```

This starts one persistent primary, two in-memory replicas, and HAProxy at
`http://127.0.0.1:8080`. Only the load balancer publishes a host port. The same
token authenticates clients and replication; probes are public. Keep the token
for subsequent Compose commands and client requests.

```bash
curl --fail-with-body http://127.0.0.1:8080/api/exec \
  -H "Authorization: Bearer $TINYSQL_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"sql":"CREATE TABLE items (id INT PRIMARY KEY, name TEXT)"}'

curl --fail-with-body http://127.0.0.1:8080/api/exec \
  -H "Authorization: Bearer $TINYSQL_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"sql":"INSERT INTO items VALUES (1, '\''hello cluster'\'')"}'

curl --fail-with-body http://127.0.0.1:8080/api/query \
  -H "Authorization: Bearer $TINYSQL_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"sql":"SELECT * FROM items"}'
```

The example routes `/api/exec` and administrative requests to the primary, and
`/api/query` plus `/api/query/stream` to the replicas in round-robin order. Queries
fall back to the primary if all replicas are unready. Do not submit writes to
query routes: replicas reject mutations even through those routes, including
`EXPLAIN ANALYZE` of a write. The balancer does not parse SQL.

For a read that must observe a preceding successful write, force the primary:

```bash
curl --fail-with-body http://127.0.0.1:8080/api/query \
  -H "Authorization: Bearer $TINYSQL_AUTH_TOKEN" \
  -H 'X-tinySQL-Consistency: primary' \
  -H 'Content-Type: application/json' \
  --data '{"sql":"SELECT * FROM items"}'
```

`X-tinySQL-Consistency` is a routing convention implemented by the supplied
HAProxy configuration. A direct request to a replica cannot request primary
consistency. gRPC clients should use the primary address for writes and immediate
read-after-write queries, and a separate read pool for `Query` / `QueryStream`.

The example uses [HAProxy HTTP health checks](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/reliability/health-checks/)
to remove unready backends. It disables retries so SQL POST requests are not
automatically replayed. Clients must also avoid blindly retrying a write after
a timeout: a lost response does not prove that the write failed.

```bash
docker compose -f deploy/cluster/compose.yaml logs -f
python3 deploy/cluster/smoke.py
docker compose -f deploy/cluster/compose.yaml stop replica1
python3 deploy/cluster/smoke.py --replicas replica2
docker compose -f deploy/cluster/compose.yaml start replica1
docker compose -f deploy/cluster/compose.yaml down
```

`down` retains the primary volume. A replica restart is safe while the primary
remains available; requests already in flight may still fail during removal.
If the primary fails, writes become unavailable and replicas withdraw readiness
on a feed error or freshness timeout. Restore the primary from its durable
storage or a verified backup. Starting a new independent writer behind the
balancer would create a different database.

The smoke test creates and removes a uniquely named test table. It verifies
primary writes, forced-primary reads, and convergence on every expected replica
using HAProxy's `X-tinySQL-Backend` response header. The cluster CI workflow also
runs it after removing and restarting a replica.

## Start individual nodes

```bash
(cd cmd/server && go build -o ../../bin/tinysql-server .)

# Terminal 1: durable primary.
./bin/tinysql-server -http 127.0.0.1:8080 -grpc 127.0.0.1:9090 \
  -dsn 'file:./cluster-data/primary?mode=advanced_wal&tenant=default' \
  -auth "$TINYSQL_AUTH_TOKEN" -grpc-max-send-bytes 67108864

# Terminal 2: read replica (repeat with different ports for more replicas).
./bin/tinysql-server -http 127.0.0.1:8081 -grpc 127.0.0.1:9091 \
  -replica-of 127.0.0.1:9090 -auth "$TINYSQL_AUTH_TOKEN" \
  -grpc-max-recv-bytes 67108864
```

Replicas honor the server's HTTP/gRPC listeners, auth, TLS, body/response limits,
timeouts, concurrency limits, and graceful shutdown settings. `-dsn` is unused
on a replica. `-replica-transport stream` is the default; `poll` is available for
networks that cannot carry long-lived gRPC streams. On different hosts, configure
`-grpc-tls-cert` / `-grpc-tls-key` on the primary and `-peer-tls` with the relevant
CA/server name on replicas. Terminate public HTTPS at your load balancer or use
the server's HTTP TLS flags.

The complete snapshot and each WAL response must fit both the primary's
`-grpc-max-send-bytes` and the replica's `-grpc-max-recv-bytes`. Defaults are
4 MiB; these examples use 64 MiB. Increase both deliberately for larger datasets;
bootstrap is a whole-database transfer and each replica must fit it in memory.

## Readiness and recovery

| Endpoint | Meaning |
|---|---|
| `/healthz` | Process liveness; remains healthy while waiting for a primary |
| `/readyz`, `/readyz/read` | Healthy database, ready to serve; replicas require a fresh applied feed response |
| `/readyz/write` | Same readiness checks, plus a writable database; always 503 on replicas |
| `/api/status` | Authenticated status including read-only state, replication source, epoch, resume LSN, response age, and last error |

`-replica-ready-timeout` defaults to `5s` and must exceed the primary's `1s`
heartbeat interval. An idle primary sends heartbeat responses. The timeout bounds
time since a successfully applied response, not actual replication lag or a
read-after-write consistency guarantee. Upgrade primary and replicas together;
older primaries without heartbeats can make idle streaming replicas unready.

Before bootstrap, while re-bootstrapping, after a feed error, or after freshness
expires, new SQL requests fail with HTTP 503 / gRPC `Unavailable`. Bootstrap
retries automatically if the primary is initially unreachable. A checkpoint
that removes needed WAL records, a schema/catalog checkpoint, or a changed WAL
epoch triggers a full new snapshot. Existing queries finish against their
current snapshot before that database is closed. Replicas never execute a
second job scheduler or replay SQL triggers; they apply committed row changes.

`-peers` is federation: it concatenates results from distinct databases. It is
not replication or load balancing, and would duplicate rows across identical
replicas. The server rejects combining it with `-replica-of`.

## Immutable read farms

For published, unchanging data, several `tinysqld -read-only` processes can serve
independent copies of the same `disk`, `index`, or `paged_index` artifact behind
a load balancer checking `/readyz`. Replace artifacts through a rolling restart;
do not change their files in place while a process serves them. Use `cmd/server`
with `-replica-of` when the served data needs to follow a live writable primary.
