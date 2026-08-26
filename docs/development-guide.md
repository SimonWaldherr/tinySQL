# Development Guide

## Tests

```bash
go test ./... -count=1
go test -coverprofile=coverage.out ./...
```

Go 1.26's `testing/synctest` provides virtual time for concurrency tests; keep
new timing-sensitive tests deterministic instead of adding real sleeps.

## Makefile

`make` or `make help` lists all documented targets. Common workflow:

```bash
make deps
make ci
make build-all
```

| Target | Purpose |
|---|---|
| `make build` | Build `cmd/tinysql` into `bin/tinysql`. |
| `make build-all` | Build the CLI plus the common command demos into `bin/`. |
| `make build-query-files-wasm` | Build the browser playground WASM artifacts. |
| `make build-wasm-browser` / `make build-wasm-node` | Build the browser or Node WASM API bundles. |
| `make build-gh-pages-demo` | Incrementally build and validate the static files for the GitHub Pages demo. |
| `make update-gh-pages` | Reuse a clean `gh-pages` worktree, sync only changed assets, and commit only if the generated demo changed. |
| `make push-gh-pages` | Run `update-gh-pages` and push `gh-pages` (only that branch). |
| `make test` / `make test-all` | Root tests plus the standalone query-file demo modules. |
| `make test-unit` | Short unit tests. |
| `make test-jsonv2` | Storage/engine persistence tests against Go's experimental JSON v2 — a compatibility gate, not a production default. |
| `make test-query-files` / `make test-fsql` | Standalone query-files and filesystem-query module tests. |
| `make test-query-files-wasm` | Tests inside `cmd/query_files_wasm`. |
| `make wasm-check` | Compile the standard-Go browser, Node, and query-files WebAssembly targets without retaining artifacts. |
| `make tinygo-wasm` | Build and execute the Node WASM smoke test using the pinned TinyGo Docker image. |
| `make ci` | The complete standard-Go GitHub Actions matrix: formatting, module verification, native/WASM builds, vet, tests, race detection, and coverage. |
| `cd odbc && make linux` | Build the c-shared ODBC driver from its nested Go module. |
| `make coverage` | Run tests and open an HTML coverage report. |
| `make bench` | Benchmarks with allocation output. |
| `make bench-stream` | Repeated time-to-first-row measurements for engine, `database/sql`, CLI, and server (plus the materialized engine baseline). |
| `make bench-stream-guard` | CI-safe first-row allocation/latency budget; limits are configurable with `STREAM_FIRST_ROW_MAX_B` and `STREAM_FIRST_ROW_MAX_NS`. |
| `make release-check` | Cross-compile the published CLI targets (Linux, macOS, Windows; amd64/arm64) into a temporary directory. |
| `make fmt` / `make fmt-check` | Format Go files / check formatting without modifying. |
| `make vet` | `go vet ./...`. |
| `make lint` | `golangci-lint`; must be installed locally. |
| `make verify` | Mutating local check: fmt, vet, lint, tests. Runs `make fmt`, so it may rewrite tracked Go files. |
| `make verify-ci` | Quick non-mutating CI-style check: fmt-check, vet, build-check, tests. |
| `make clean` | Remove binaries, WASM artifacts, coverage files, WAL leftovers. |
| `make run-repl` / `make run-server` / `make run-demo` | Build and start the corresponding demo. |
| `make info` | Print build version, Go version, configured paths. |

`verify-ci` builds all host Go packages plus the `query_files` CLI and its WASM
artifact. Run `make build-wasm-browser` or `make build-wasm-node` separately
when changing those targets. Push `main` separately after committing source
changes.

### Nested module dependencies

The ODBC driver has its own `go.mod` and `go.sum`. Keep them in sync with the
root module whenever the root SQLite dependency changes:

```bash
go mod tidy -diff                 # from the repository root
(cd odbc && go mod tidy)
CGO_ENABLED=1 make -C odbc linux
```

Running `go mod tidy` in the repository root does not update nested modules;
a stale module otherwise fails independently in CI. Use `make tidy-all` after
changing shared dependencies to tidy every tracked module. `make modules-verify`
checks checksums across that same tracked set without touching untracked work.

Variables are overridable:

```bash
make build BINARY_DIR=dist
make test GO_TEST_FLAGS="-run TestGeo -count=1"
make update-gh-pages GH_PAGES_COMMIT_MESSAGE="Update playground"
make update-gh-pages GH_PAGES_WORKTREE=/tmp/tinysql-gh-pages
```

## Releases

Push a signed `vX.Y.Z` tag after the normal verification suite. The release
workflow cross-builds the `tinysql` CLI for Linux, macOS, and Windows, embeds
the tag in `tinysql --version`, publishes GitHub release archives, and attaches
a combined `SHA256SUMS` file. A manually dispatched release workflow builds
artifacts for inspection but does not publish a release.

## Further reading

- [Repository structure](./repository-structure.md)
- [Developer integration](./developer-integration.md)
- [Benchmarks](../BENCHMARKS.md)
