# TinyGo and embedded targets

tinySQL runs under TinyGo when the database lives in the same process as the
application: local control planes, device-side rules, compact dashboards, and
WebAssembly apps needing a real SQL engine without a separate service.

TinyGo build constraints are applied automatically — no fork, no special import
path.

## Smoke test

```bash
tinygo run -target=wasm ./examples/tinygo-smoke
```

## Browser and Node bundles

The browser and Node WASM build scripts can use TinyGo for substantially
smaller release artifacts while retaining the same JavaScript API. The default
compiler remains the standard Go toolchain.

```bash
cd cmd/wasm_browser
WASM_COMPILER=tinygo ./build.sh --build-only

cd ../wasm_node
WASM_COMPILER=tinygo ./build.sh --run --query "SELECT 1 AS ready"
```

The script copies the TinyGo-matched `wasm_exec.js`; do not mix it with the
standard Go runtime shim. Benchmark application queries before switching a
production deployment, since artifact size and query throughput can trade off.

The smallest supported embedded integration: create an in-memory database,
parse SQL, execute a query.

```go
db := tinysql.NewDB()
stmt, _ := tinysql.ParseSQL("SELECT 1 AS ready")
result, err := tinysql.Execute(context.Background(), db, "default", stmt)
```

## Target support

Verified with TinyGo 0.41.1 against the complete tinySQL feature set:

| Target | Result | Notes |
|---|---|---|
| `wasm` | Runs | TinyGo WebAssembly deployments. |
| `pico2` | Builds | Baseline RP2350-class boards. |
| `xiao-rp2350` | Builds | Seeed XIAO RP2350-class boards. |
| `elecrow-rp2350` | Builds | RP2350-class boards with more board-specific IO. |
| `teensy41` | Builds | Memory-rich embedded hardware. |
| `cortex-m-qemu` | Does not fit | The complete engine exceeds this target's flash and static-RAM limits. |

The full package is not aimed at tiny AVR or small Cortex-M devices. For those,
use the SQL parser/executor only after budgeting memory for your schema, rows,
and enabled storage features.

## RP2350

```bash
tinygo build -target=pico2 -o tinygo-rp2350.uf2 ./examples/tinygo-rp2350
```

The bare-metal example drives an in-memory telemetry loop, logs results over
the TinyGo serial port, and pulses the onboard LED after every SQL
update/query cycle.

RP2350 targets build without a fork or per-board patches; the practical
constraint is RAM and flash budgeting for your own schema and workload, not
parser compatibility. For board-facing integrations:

- Keep the database in memory unless your board-specific storage story is ready.
- Prefer narrow schemas and bounded row counts for control-loop workloads.
- Use `machine.Serial` or board-specific transports to expose query results.
- Treat tinySQL as the local decision engine, not a remote multi-client DB.

## Feature availability

TinyGo targets keep the core parser, execution engine, SQL functions, and
in-memory storage. Features needing an OS or HTTP runtime fail with a clear
SQL/API error rather than breaking the build:

- `HTTP()` is unavailable on TinyGo WASM and bare-metal targets.
- MBTiles import is unavailable on TinyGo WASM and bare-metal targets; it
  depends on an embedded SQLite reader.
- `sys.memory` keeps its regular shape; runtime metrics TinyGo cannot expose
  are reported as `unavailable`.
