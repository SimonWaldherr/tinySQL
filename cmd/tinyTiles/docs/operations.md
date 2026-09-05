# Operations and artifact lifecycle

## Build inputs and storage planning

`tinytiles import` treats MBTiles data as untrusted input. Before creating a
destination it opens the source read-only, detects its flat or normalized
shape, counts source rows, measures the largest tile blob and computes a
resource estimate.

```bash
tinytiles import \
  --batch 2048 \
  --max-memory $((256 * 1024 * 1024)) \
  --min-free $((8 * 1024 * 1024 * 1024)) \
  source.mbtiles dataset.ttiles/
```

The preflight line reports the source byte size, tiles, estimated output/working
disk, configured working set, free disk and batch size. An import aborts before
the first destination write when:

- the estimated working set exceeds `--max-memory`;
- available disk is below estimated output plus `--min-free`;
- source schema or metadata is invalid;
- the destination already exists without `--replace`.

Set `--min-free` to a real operational reserve. `--min-free 0` is useful only
for small test fixtures.

For a host that only validates or serves an existing artifact, `make
build-reader-cli` produces `tinytiles-reader` without SQLite. It retains only
the `validate`, `inspect` and `tile` CLI commands; import/build/benchmark are
explicitly unavailable because they require an MBTiles SQLite source.

## Atomic artifact publication and rollback

Import writes to a temporary sibling directory, not the destination:

```text
parent/
  .tinysql-artifact-.../      ← import, page/index flush and full validation
  dataset.ttiles/             ← existing published artifact, if any
  dataset.ttiles.rollback/    ← temporary only during replacement
```

The sequence is:

1. stream bounded batches into the temporary paged-index database;
2. write index configuration, manifest and checksums;
3. validate tables, unique keys, index completeness, data digests, checksums
   and required directories;
4. write `COMPLETE`, validate again, fsync the tree;
5. rename the temporary artifact into place;
6. on replacement, restore the old artifact from `.rollback` if the final
   rename fails, then remove the rollback directory after success.

No reader should open a path without a valid `COMPLETE` marker. The reader
performs the same integrity checks on opening, so manually copied, partial or
corrupted directories fail closed.

## Reader process model

A `tiles.Reader` owns one read-only tinySQL pager handle behind the stable
public API. Share a pool of independent readers across request handlers rather
than one mutable handle. `tinytiles-server`'s `-readers` flag creates that
pool; choose it based on CPU, file descriptor and cache budget:

```bash
tinytiles-server \
  -artifact /srv/tiles/dach.ttiles \
  -dataset dach \
  -readers 8 -max-memory $((32 * 1024 * 1024)) \
  -public-base https://tiles.example
```

`-max-memory` is **per reader**, so eight readers at 32 MiB have a 256 MiB
cache-budget ceiling before process/runtime overhead. The server is a small
integration: put authentication, TLS termination, rate limits, request
timeouts, observability and deployment policy in the surrounding application
or proxy. `-public-base` is strongly recommended behind a reverse proxy.

For the long-term boundary, SQL-driver policy and the separate routing decision,
see [tinySQL integration API](tinysql-api.md).

## Offline cache operations

- Give each dataset a stable `dataset` identifier and each immutable build a
  new `revision`.
- Fetch the manifest with revalidation; cache tiles by revision indefinitely.
- Do not reuse a revision after changing any tile, media type or raw encoding.
- Start without `PrunePrevious` during rollouts. Enable it only after the new
  cache has been observed working and disk pressure warrants cleanup.
- Treat a `PruneError` as cleanup debt, not a failed update: the new manifest
  is already atomically active.
- Set `HTTPFetcher.MaxTileSize` and `FileStore.SetMaxTileSize` according to
  the largest expected tile, not an unbounded user input.

Native FileStore cache files are an implementation detail. Move the cache as a
whole or resynchronize it; do not edit filenames or tile record bytes.

## Recovery checklist

| Situation | Safe action |
|---|---|
| Import was interrupted | Delete only the generated temporary sibling directory if it remains; the destination remains valid. |
| `validate` fails | Do not serve the artifact. Rebuild into a fresh destination and compare source/digest information. |
| Replacement failed | Inspect `dataset.ttiles.rollback`; the importer attempts restoration automatically. Do not delete it until a known-good artifact is validated. |
| Offline sync failed | Keep using the active local revision. Retry the same manifest; valid inactive tiles are reused. |
| Browser storage quota failure | Surface the sync error, offer a smaller range or explicit old-revision cleanup, and retry. |
| Server revision changed | Resynchronize selected ranges; never rewrite bytes in an active local revision. |

## Observability

The CLI emits stable phase-oriented lines: `phase=generate`, `phase=import`,
`preflight`, `published` and elapsed time. Capture these together with input
hashes, artifact manifest, tinySQL version, generator version and resource
configuration. The sync library returns tile totals, downloads, cache reuses,
revision and optional prune error; record them in application metrics.

Useful service-level signals are reader-pool wait time, tile hit/miss/error
count, HTTP status by endpoint, sync duration, sync bytes, active revision,
cache quota errors and validation failures. Do not log raw tile payloads or
unbounded request URLs.
