# Contributing to tinyTiles

## Local checks

Run the complete native quality gate before opening a change:

```bash
make ci
```

Use `make coverage` for a function-level report and `make bench` for package
benchmarks. Keep tests deterministic: fixtures must be small, local and must
not depend on a live tile server or a full DACH extract.

## Design rules

- Preserve TMS coordinates at every public boundary.
- Do not make browser code depend on SQLite, a local filesystem, or a full
  server-side `.ttiles` page file.
- Keep sync operations bounded: stream ranges into workers instead of building
  unbounded tile slices.
- Treat source PBFs, MBTiles files, artifacts and HTTP responses as untrusted
  input. Fail closed before publishing a new revision.
- Keep map-style semantics in explicit generators such as Karte.Bayern; the
  generic tinyTiles artifact/cache layer must not quietly reinterpret OSM data.

## Changes to the sync protocol

`offline.ProtocolVersion` is a compatibility boundary. Any incompatible JSON,
cache-record or coordinate change requires a new version, migration notes and
tests for old revisions remaining readable or safely rejected.
