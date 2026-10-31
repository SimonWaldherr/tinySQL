# Go API stability

The public Go API is the root module
`github.com/SimonWaldherr/tinySQL` and its documented public subpackages such
as `driver`, `exporter`, `importer`, `resultutil`, and `tiles`. Code below an
`internal/` directory is not a supported integration surface.

## Compatibility contract

`tinysql.APIVersion` is currently `1`.

- A patch release fixes behavior without intentionally changing public API.
- A minor release can add exported functions, fields, SQL features, storage
  modes, and output formats while retaining source compatibility.
- An incompatible public Go API or persisted-format change requires a new
  major module version, release notes, and an updated compatibility plan.
- Application code should pin a released module version rather than a branch
  tip when persisted data or a production CLI is involved.

The CLI, HTTP API, and gRPC API are versioned at their transport boundaries.
New endpoints and optional response fields are additive. Existing output modes
remain compatible unless a major release explicitly says otherwise.

## SQL behavior

SQL results are part of the public contract, independently of Go source
compatibility. This includes NULL behavior, argument evaluation, numeric
precision, calendar definitions, timezone interpretation and ordering defaults.

- Preserve the documented meaning of a released operation. Prefer a new,
  explicit operation when a different meaning is useful.
- An intentional incompatible SQL change requires a breaking release, release
  notes with before/after results, and an executable migration example. A
  documentation edit alone does not make an existing query compatible.
- A correctness fix must include the failing query and expected result or
  error. If callers can observe a changed result, describe that impact in the
  release notes even when the previous result was a bug.
- The same explicit inputs must not acquire different meanings through
  connection settings, process locale, optimizer selection or integer width.
  Document ambient inputs such as the statement clock separately.
- Reject unsupported syntax or combinations with a useful error. Do not
  accept a spelling while ignoring an argument or approximating its meaning.

The current unreleased DATEDIFF change is an intentional SQL behavior break:
it counts crossed UTC boundaries. Duration queries must migrate to
TIMESTAMPDIFF. The [migration examples](datetime-performance.md#differences-boundaries-and-elapsed-units)
must accompany the release notes, including an audit of stored view/procedure
SQL that calls DATEDIFF. This does not change APIVersion or the storage format.

The [SQL contracts](../internal/engine/testdata/sql_semantics.json) are checked
through native row, streaming and columnar APIs and the full/minimal Browser
and Node WASM APIs. `make test-semantics` also runs them in three process
timezones. Add cases when extending a feature; change existing expectations
only with an explanation of compatibility and migration impact.

## Native C ABI

`bindings/c/include/tinysql.h` versions the native interface used by the
Rust, Python and Swift bindings. `TinySQLABIVersion()` returns the ABI
version (currently `2`). A new version only adds functions and optional JSON
response members; existing signatures, ownership rules and response shapes
stay valid, so a binding checks for a minimum version. Removing or changing a
function requires a major release. The legacy single-database functions of
the former Python bridge remain exported.

## Streams

`ExecuteStream`, `ExecSQLStream`, and `ExecuteCompiledStream` use a default
bounded producer buffer. The corresponding `*WithOptions` variants accept a
`StreamOptions{Buffer: n}` value; `Buffer: 0` gives strict consumer
backpressure.

Call `Close` whenever a consumer stops before EOF. `Next` and `Row` are a
single-consumer pair; `Stats` is safe to read from a monitoring goroutine.
Simple scans can produce rows before the entire query finishes. Exact global
operations (`ORDER BY`, aggregates, joins, `DISTINCT`, and set operations)
remain materialized so their SQL results stay correct; `Stats().Materialized`
identifies that path.

## Persistence and upgrades

Use `OpenDB` and `StorageConfig` for durable applications, and always call
`DB.Close` on shutdown. Back up a database before changing the tinySQL major
version. Storage compatibility, encryption scope, and optional build-tag
requirements are documented in the [storage guide](./storage-guide.md).
