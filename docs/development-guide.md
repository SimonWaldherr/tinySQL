# Development Guide

Go **1.27.3** remains the minimum supported version. The `toolchain` directive
in `go.mod` selects **Go 1.27.4**, the security patch used by CI and release
builds; their Go setup reads that directive.

## Tests

```bash
go test ./... -count=1
mkdir -p bin
go test -coverprofile=bin/coverage.out ./...
```

Use `testing/synctest` for virtual time in concurrency tests; keep new
timing-sensitive tests deterministic instead of adding real sleeps.

For SQL behavior changes, follow the [SQL compatibility contract](api-stability.md#sql-behavior)
and update [the executable contracts](../internal/engine/testdata/sql_semantics.json).
Use independent expected results, including errors; comparison between two
engine paths alone can preserve a bug shared by both. Keep the SQLite oracle
tests for overlapping standard behavior, and document deliberate differences.

Before adding an optimization, establish NULL, empty-input, Unicode, timezone,
overflow and discarded-argument behavior. Then compare the optimized execution
with those results. The existing `make ci` and `make wasm-smoke` gates check
the same contract fixtures; unsupported combinations need negative cases.

## Makefile

`make` or `make help` lists all documented targets. Common workflow:

```bash
make deps
make ci
make build-all
```

Pushes and pull requests use `.github/workflows/ci.yml`. The smaller
`Local CI (act)` workflow is manual so it does not duplicate the standard CI
matrix on GitHub. To run it with a local Docker daemon:

```bash
act workflow_dispatch -W .github/workflows/local-act.yml
```

Separate Actions jobs check Chromium, TinyGo, C/Python/Rust bindings, and
amd64/arm64 vector correctness. Vector benchmarks run only when the CI workflow
is dispatched manually; use focused local benchmarks when comparing a change.

| Target | Purpose |
|---|---|
| `make reference` | Regenerate HTML and browser JSON from `docs/reference/functions.sql`; freshness tests enforce both outputs. |
| `make docs-check` | Check local documentation links and generated function-reference freshness without rewriting files. |
| `make test-pages-tooling` | Check incremental builds, staging, dataset preservation and Git worktree safeguards in temporary directories. |
| `make clean` | Remove build output and legacy root artifacts; preserve databases, WALs and Git worktrees. |
| `make build` | Build `cmd/tinysql` into `bin/tinysql`. |
| `make build-all` | Build the CLI plus the common command demos into `bin/`. |
| `make build-query-files-wasm` | Build the browser playground WASM artifacts. |
| `make build-wasm-browser` / `make build-wasm-node` | Build the browser or Node WASM API bundles. |
| `make build-gh-pages-demo` | Incrementally build and validate the static files for a local GitHub Pages preview. |
| `make check-gh-pages-demo` | Validate the existing playground build and its asset manifest. |
| `make update-gh-pages` | Reuse a clean `gh-pages` worktree, sync changed assets, and commit changes for branch-based publication. |
| `make push-gh-pages` | Run `update-gh-pages` and push `gh-pages` for branch-based publication. |
| `make test` / `make test-all` | Root tests with race detection and coverage, plus the query-file, browser-playground, and server modules. |
| `make test-unit` | Short unit tests. |
| `make test-jsonv2` | Storage/engine persistence tests against Go's experimental JSON v2 — a compatibility gate, not a production default. |
| `make test-semantics` | Shared SQL contracts under UTC, Europe/Berlin and Pacific/Kiritimati; included in `make ci`. |
| `make test-query-files` / `make test-fsql` | Standalone query-files and filesystem-query module tests. |
| `make test-query-files-wasm` | Tests inside `cmd/query_files_wasm`. |
| `make test-query-files-browser` | Real Chromium import, snapshot recovery and workspace-switching tests; requires the browser test dependencies described in the module README. |
| `make wasm-check` | Compile the standard-Go browser, Node, and query-files WebAssembly targets without retaining artifacts. |
| `make tinygo-wasm` | Build and execute the Node WASM smoke test using the pinned TinyGo Docker image. |
| `make ci` | Standard-Go validation: formatting, documentation, module verification, native/WASM builds, vet, tests, race detection, and coverage. |
| `make build-c` | Build the C ABI (`libtinysql.a`, shared library, `tinysql.h`) into `bindings/c/build`. |
| `make test-c` / `make test-python` / `make test-rust` | Test the C ABI (race detector plus a C program), the Python package, or the Rust crate. |
| `make test-bindings` | All three binding suites. |
| `make build-apple` / `make test-swift` | Build the Swift XCFramework and run the Swift tests (macOS with Xcode). |
| `cd odbc && make linux` | Build the c-shared ODBC driver from its nested Go module. |
| `make coverage` | Run tests and open an HTML coverage report. |
| `make bench` | Benchmarks with allocation output. |
| `make bench-stream` | Repeated time-to-first-row measurements for engine, `database/sql`, CLI, and server (plus the materialized engine baseline). |
| `make bench-stream-guard` | CI-safe first-row allocation/latency budget; limits are configurable with `STREAM_FIRST_ROW_MAX_B` and `STREAM_FIRST_ROW_MAX_NS`. |
| `make release-check` | Cross-compile the published CLI targets (Linux, macOS, Windows; amd64/arm64) into a temporary directory. |
| `make fmt` / `make fmt-check` | Format Go files / check formatting without modifying. |
| `make hooks-install` | Enable the repository-managed pre-commit checks for this clone. |
| `make vet` | `go vet ./...`. |
| `make lint` | `golangci-lint`; must be installed locally. |
| `make verify` | Mutating local check: fmt, vet, lint, tests. Runs `make fmt`, so it may rewrite tracked Go files. |
| `make verify-ci` | Non-mutating local gate: formatting, vet, host builds, and `test-all` including root race detection and coverage. |
| `make run-repl` / `make run-server` / `make run-demo` | Build and start the corresponding demo. |
| `make info` | Print build version, Go version, configured paths. |

`verify-ci` builds all host Go packages plus the `query_files` CLI. `make ci`
adds the build-tag, SQL compatibility, experimental JSON v2, and nested-module
test matrix, and compiles all three standard-Go WASM targets. Run
`make wasm-smoke` to execute the shared SQL contracts under Node, and
`make test-query-files-browser` when changing browser behavior.

## Benchmarking

Use a focused benchmark and repeated samples when comparing changes:

```bash
go test ./benchmarks -run '^$' -bench '^Benchmark(Join|Aggregate)$' -benchmem -count=5 > before.txt
# Repeat on the changed revision into after.txt, then use benchstat if installed:
benchstat before.txt after.txt
```

Run both revisions on the same machine with the same Go version, power settings,
and background load. The query benchmarks populate each backend before timing;
they include query parsing/execution and result retrieval, but exclude setup and
cleanup. tinySQL materializes results while SQLite rows are scanned via
`database/sql`, and their query planners and storage settings differ, so these
numbers are workload comparisons rather than isolated engine measurements.
The `TestQueryBenchmarkFixtures` test checks expected result counts for each
backend. `make bench` runs the root module's benchmarks; nested modules need
separate runs. `make bench-hotpaths` and `make bench-stream` provide focused
alternatives. Benchmark result files should stay outside version control.

## Git hooks

Run `make hooks-install` once after cloning. The versioned pre-commit hook
checks the **staged** Go files with `gofmt` before Git creates a commit. It
never stages or overwrites unstaged work; when it reports a file, run
`make fmt`, review and stage the result, then retry the commit. CI keeps the
same `make fmt-check` gate as a backstop.

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
`make modules-tidy-check` is the non-mutating CI gate that rejects a stale
nested module before its build runs.

Variables are overridable:

```bash
make build BINARY_DIR=dist
make test GO_TEST_FLAGS="-run TestGeo -count=1"
make update-gh-pages GH_PAGES_COMMIT_MESSAGE="Update playground"
make update-gh-pages GH_PAGES_WORKTREE=/tmp/tinysql-gh-pages
```

## GitHub Pages

The [Pages workflow](../.github/workflows/pages.yml) builds and validates the
playground and SQL function reference on pushes to `main` and pull requests.
It publishes an artifact through GitHub's Pages actions on `main`, on manual
runs from the default branch, and after a successful Bavaria data refresh.
Pull requests run the build checks without deploying.

Enable this workflow once under **Settings → Pages → Source → GitHub Actions**;
see [GitHub's publishing-source instructions](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site).
This repository setting is separate from the workflow files.

The monthly [Bavaria workflow](../.github/workflows/bavaria-mbtiles.yml) keeps its
generated map snapshot on `gh-pages`. The Pages build includes that snapshot
when available, preserving same-origin map access. Its successful completion
starts a new Pages run explicitly: a branch push using `GITHUB_TOKEN` alone
does not trigger a Pages build.

Use `make build-gh-pages-demo` for a local build and `make check-gh-pages-demo`
to validate existing output. The compatible `make update-gh-pages` and
`make push-gh-pages` targets remain available for repositories configured to
publish from a branch. With the Actions source, commit source changes to `main`
to publish the current application.

## Releases

Push a signed `vX.Y.Z` tag after the normal verification suite. The release
workflow cross-builds the `tinysql` CLI for Linux, macOS, and Windows, embeds
the tag in `tinysql --version`, publishes GitHub release archives, and attaches
a combined `SHA256SUMS` file. A manually dispatched release workflow builds
artifacts for inspection but does not publish a release.

## Further reading

- [Repository structure](./repository-structure.md)
- [Developer integration](./developer-integration.md)
- [SQL parser, sorting and small-result performance](./sql-hotpath-performance.md)
- Run `make bench` for local regression measurements; compare results only with
  the same workload, machine state, and storage configuration.
