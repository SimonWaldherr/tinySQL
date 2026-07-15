# Development Guide

## Running tests

```bash
go test ./... -count=1
go test -coverprofile=coverage.out ./...
```

## Makefile

The repository `Makefile` wraps the common build, test, demo, and release
tasks. Run `make` or `make help` to list all documented targets.

Common workflow:

```bash
make deps
make verify-ci
make build-all
```

Useful targets:

| Target | Purpose |
|---|---|
| `make help` | Show all documented targets. |
| `make build` | Build the main `cmd/tinysql` CLI into `bin/tinysql`. |
| `make build-all` | Build the main CLI and the common command demos into `bin/`. |
| `make build-query-files-wasm` | Build the browser playground WASM artifacts. |
| `make build-wasm-browser` / `make build-wasm-node` | Build the browser or Node WASM API bundles. |
| `make build-gh-pages-demo` | Build the static files used by the GitHub Pages demo. |
| `make update-gh-pages` | Build the demo, update the `gh-pages` worktree, and commit changes there. |
| `make push-gh-pages` | Run `update-gh-pages` and push the `gh-pages` branch. |
| `make test` / `make test-all` | Run root tests plus standalone module tests for query file demos. |
| `make test-unit` | Run short unit tests. |
| `make test-jsonv2` | Exercise storage and engine persistence tests with Go's experimental JSON v2 implementation; it is a compatibility gate, not a production default. |
| `make test-query-files-wasm` | Run tests inside `cmd/query_files_wasm`. |
| `make coverage` | Run tests and open an HTML coverage report. |
| `make bench` | Run Go benchmarks with allocation output. |
| `make fmt` / `make fmt-check` | Format Go files or check formatting without modifying files. |
| `make vet` | Run `go vet ./...`. |
| `make lint` | Run `golangci-lint`; requires it to be installed locally. |
| `make verify` | Run mutating local verification: format, vet, lint, and tests. |
| `make verify-ci` | Run non-mutating CI-style verification: format check, vet, build check, and tests. |
| `make clean` | Remove generated binaries, WASM artifacts, coverage files, and WAL leftovers. |
| `make run-repl` / `make run-server` / `make run-demo` | Build and start the corresponding demo tool. |
| `make info` | Print build version, Go version, and configured paths. |

The Makefile uses overridable variables. Examples:

```bash
make build BINARY_DIR=dist
make test GO_TEST_FLAGS="-run TestGeo -count=1"
make update-gh-pages GH_PAGES_COMMIT_MESSAGE="Update playground"
make update-gh-pages GH_PAGES_WORKTREE=/tmp/tinysql-gh-pages
```

Notes:

- `make verify-ci` is the safest pre-push check because it does not rewrite Go
  files.
- `make verify` runs `make fmt`, so it may modify tracked Go files.
- Go 1.26's `testing/synctest` is used for virtual-time concurrency tests;
  keep new timing-sensitive tests deterministic rather than adding real sleeps.
- CI compiles all browser and Node WASM targets in addition to the host build.
- `make update-gh-pages` creates or refreshes a local worktree for the
  `gh-pages` branch and commits only when the generated static demo changed.
- `make push-gh-pages` pushes only `gh-pages`; push `main` separately after
  committing source changes.

## Further reading

- [Repository structure](./repository-structure.md)
- [Developer integration](./developer-integration.md)
- [Benchmarks](../BENCHMARKS.md)
