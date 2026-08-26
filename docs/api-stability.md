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
