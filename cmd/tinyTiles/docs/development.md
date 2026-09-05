# Development guide

## Prerequisites

- Go version declared in `go.mod`;
- GNU Make or BSD make;
- a modern browser for the optional WASM demo;
- a Karte.Bayern-compatible preprocessor only for real PBF integration runs.

The module uses a temporary local `replace` to the sibling tinySQL worktree.
That makes it runnable in this staging location. After moving into its own
repository, remove the `replace` and depend on a released tinySQL version.

## Daily loop

```bash
make fmt
make ci
make coverage
make bench
```

`make ci` is intentionally hermetic with small local fixtures: it does not
download PBF data, mutate downstream configuration or require a running tile
server. It includes `go vet`, race detection and a `GOOS=js GOARCH=wasm`
build. `make demo-check` checks browser demo assets and uses `node --check`
when Node is installed.

## Test strategy

- CLI lifecycle tests create local flat and normalized MBTiles fixtures and
  cover import, validate, inspect, lookup, PBF adapter and SQLite comparison.
- Offline tests cover cancellation, failed updates, cache reuse, pruning,
  checksum validation, atomic file persistence and concurrent access.
- `-race` exercises native cache/synchronizer readers and writers.
- WASM is compile-checked in CI. A real browser integration depends on the
  consuming application's origin, CORS and storage quota and is documented as
  a release-level test.

New changes should add a deterministic regression test at the public API
boundary. Do not rely on test ordering, local DACH files or network access.

## Releasing

Before tagging a release:

1. run `make ci`, `make coverage` and `make bench`;
2. run a regional artifact import/validate/benchmark and record the output;
3. run the full-DACH evaluation when resources permit;
4. update `CHANGELOG.md`, compatibility notes and protocol version if needed;
5. build native and WASM artifacts with `make build wasm-package`;
6. verify the license and security contact in the standalone repository.

`VERSION` defaults to `git describe`; override it for a reproducible release:

```bash
make build wasm VERSION=v0.1.0
```

The build uses `-trimpath` and `-buildvcs=false`, and embeds only the supplied
version string in the main binary.

`wasm-package` additionally emits `tinytiles.wasm.gz` using `gzip -n -9`, so
the compressed file does not receive a timestamp or source filename. Serve it
through normal HTTP content negotiation as `application/wasm` plus
`Content-Encoding: gzip`; do not apply this convention to sync tile payloads.
