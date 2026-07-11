# TinyGo and embedded targets

tinySQL can run under TinyGo when the database lives in the same process as
your application. This is a good fit for local control planes, device-side
rules, compact dashboards, and WebAssembly applications that need a real SQL
execution engine without a separate database service.

For RP2350 boards, use the dedicated bare-metal example:

```bash
tinygo build -target=pico2 -o tinygo-rp2350.uf2 ./examples/tinygo-rp2350
```

It drives an in-memory telemetry loop, logs results over the TinyGo serial
port, and pulses the onboard LED after every SQL update/query cycle.

## Start with the smoke test

```bash
tinygo run -target=wasm ./examples/tinygo-smoke
```

The example creates an in-memory database, parses SQL, and executes a query.
It is the smallest supported embedded integration:

```go
db := tinysql.NewDB()
stmt, _ := tinysql.ParseSQL("SELECT 1 AS ready")
result, err := tinysql.Execute(context.Background(), db, "default", stmt)
```

## Target support

The complete tinySQL feature set was verified with TinyGo 0.41.1 for:

| Target | Result | Notes |
|---|---|---|
| `wasm` | Runs | Suitable for TinyGo WebAssembly deployments. |
| `pico2` | Builds | Good baseline target for RP2350-class boards. |
| `xiao-rp2350` | Builds | Suitable for Seeed XIAO RP2350-class boards. |
| `elecrow-rp2350` | Builds | Suitable for RP2350-class boards with more board-specific IO. |
| `teensy41` | Builds | Suitable for memory-rich embedded hardware. |
| `cortex-m-qemu` | Does not fit | The complete engine exceeds this target's flash and static-RAM limits. |

tinySQL is a feature-rich engine, so the full package is not aimed at tiny AVR
or small Cortex-M devices. For those targets, use the SQL parser/executor only
after budgeting memory for your schema, rows, and enabled storage features.

## RP2350 notes

RP2350 targets build without a tinySQL fork or per-board patches. The main
practical constraint is still RAM and flash budgeting for your own schema and
workload, not parser compatibility.

For board-facing integrations:

- Keep the database in memory unless your board-specific storage story is ready.
- Prefer narrow schemas and bounded row counts for control-loop workloads.
- Use `machine.Serial` or board-specific transports to expose query results.
- Treat tinySQL as the local decision engine, not as a remote multi-client DB.

## TinyGo-specific availability

TinyGo targets keep the core parser, execution engine, SQL functions, and
in-memory storage available. Features that need an operating-system or HTTP
runtime fail with a clear SQL/API error instead of preventing the application
from building:

- `HTTP()` is unavailable on TinyGo WASM and bare-metal targets.
- MBTiles import is unavailable on TinyGo WASM and bare-metal targets because
  it depends on an embedded SQLite reader.
- `sys.memory` retains its regular shape; runtime metrics TinyGo cannot expose
  are reported as `unavailable`.

The build uses TinyGo-target build constraints automatically, so consumers do
not need a custom tinySQL fork or a special import path.
