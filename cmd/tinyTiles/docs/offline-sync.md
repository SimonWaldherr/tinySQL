# Offline synchronization protocol

The offline protocol is intentionally small and independent of `.ttiles`
pager files. It transfers a bounded selection of **TMS** tile payloads from a
server to a native `FileStore` or browser `IndexedDBStore`.

## Why it is separate from `.ttiles`

`.ttiles` is a server-side, read-only tinySQL artifact. Its pages and indexes
are efficient for a native process but are not a browser storage or transport
format. In particular, shipping it to a browser would couple offline clients
to server paging details and force a full artifact download for a small map
area.

The offline cache stores only requested tile responses. A server may use a
`.ttiles` reader, SQLite, object storage or another immutable tile source
behind the same protocol.

## Manifest

Clients first `GET` a JSON manifest. `offline.ProtocolVersion` is currently
`1`.

```json
{
  "format_version": 1,
  "dataset": "dach",
  "revision": "4c8e...",
  "coordinate_system": "TMS",
  "tile_url_template": "https://tiles.example/sync/tiles/{revision}/{z}/{x}/{y}",
  "content_type": "application/vnd.mapbox-vector-tile",
  "content_encoding": "gzip",
  "created_at": "2026-08-05T12:00:00Z"
}
```

Required rules:

- `format_version` must equal the client's supported protocol version.
- `dataset` is a stable local-cache key.
- `revision` identifies an immutable set of tile responses. It **must change**
  whenever any tile served by the template changes.
- `coordinate_system` is exactly TMS (case-insensitive on input). The client
  does not convert XYZ row numbers.
- `tile_url_template` contains `{z}`, `{x}` and `{y}`. `{revision}` is
  optional for backwards compatibility but strongly recommended.
- `content_encoding` describes the raw stored tile payload, not HTTP transfer
  compression.

Manifest responses should use `Cache-Control: no-cache` plus an ETag. Tile
responses for a revisioned URL can use `Cache-Control: public, max-age=31536000,
immutable`.

## Tile response

For `GET /sync/tiles/{revision}/{z}/{x}/{y}`, the sync server sends the exact raw
tile bytes to cache. Recommended headers are:

| Header | Meaning |
|---|---|
| `Content-Type` | tile media type |
| `X-TinyTiles-SHA256` | SHA-256 hexadecimal digest of the exact raw response body |
| `X-TinyTiles-Content-Encoding` | optional encoding of the raw tile payload, e.g. `gzip` |
| `ETag` | optional entity tag; a revision-specific value is acceptable |

`X-TinyTiles-Content-Encoding` deliberately differs from HTTP
`Content-Encoding`. Browsers can transparently decode HTTP compression before
WebAssembly reads the body, which would make a wire-byte checksum and cached
payload ambiguous. A sync endpoint should transfer raw tile bytes and put any
MVT/gzip payload metadata in the `X-TinyTiles-*` header. The client still
accepts HTTP `Content-Encoding` as a legacy native fallback, but new browser
endpoints must not rely on it.

This rule is specific to tile payloads. It does not prohibit normal HTTP
compression for unrelated static assets such as `tinytiles.wasm`.

When a browser calls a different origin, CORS must expose custom response
headers, for example:

```text
Access-Control-Allow-Origin: https://app.example
Access-Control-Expose-Headers: X-TinyTiles-SHA256, X-TinyTiles-Content-Encoding, ETag
```

The supplied standalone server emits these headers when `-cors` is configured.

## Atomic client update

`Synchronizer.Sync` accepts explicit keys and/or non-overlapping inclusive
TMS ranges. Ranges are visited directly into a fixed-size jobs channel and a
bounded worker pool (default 4, maximum 32); a large region is never expanded
into a full key slice.

```text
fetch and validate manifest
          │
          ▼
stream requested TMS keys → bounded workers → checksum-validate → cache under revision N
          │                                                                │
          └── failure/cancel ──────────────────────────────────────────────┘
                                       old active manifest remains unchanged
          │
          ▼ (all requested tiles present)
atomically publish active manifest = revision N
          │
          ▼
optionally prune the old immutable revision
```

An interrupted attempt can leave valid tiles under an inactive new revision.
That is safe: they are invisible to normal `get` calls until the manifest is
published, and the next attempt reuses them after checksum validation. A prune
failure is returned as a warning after a successful switch; it does not roll
back the new revision.

## Native API

```go
store, err := offline.NewFileStore("/var/lib/my-app/tiles")
if err != nil { /* handle */ }

syncer := &offline.Synchronizer{
    Store: store,
    Fetcher: &offline.HTTPFetcher{
        ManifestURL: "https://tiles.example/sync/manifest.json",
        MaxTileSize: 32 << 20,
    },
}

result, err := syncer.Sync(ctx, offline.SyncRequest{
    Dataset: "dach",
    Ranges: []offline.TileRange{{Z: 9, XMin: 271, XMax: 274, YMin: 334, YMax: 337}},
    Concurrency: 4,
    PrunePrevious: true,
})
```

The `FileStore` uses SHA-256-derived path components and per-file atomic
rename publication. It rejects the filesystem root as a cache destination and
validates cache records, size limits and checksums when reading.

Do not call a `Progress` callback that mutates shared state without its own
synchronization: worker callbacks may run concurrently. A single
`Synchronizer` serializes active-manifest publication; share one instance for
a given Store when multiple components may update the same dataset.

## WASM API

After loading `tinytiles.wasm` and `wasm_exec.js`, the runtime registers
`window.tinyTiles`:

| Function | Result |
|---|---|
| `open(name?)` | `Promise<{opened, name, version}>` |
| `close()` | `Promise<{closed}>` after closing the IndexedDB handle |
| `status()` | `{opened, version}` |
| `sync(manifestURL, request)` | `Promise<SyncResult>` |
| `get(dataset, z, x, y)` | `Promise<{found, revision?, data?, ...}>` |

Call `open` before `sync` or `get`. `data` in a successful `get` response is a
`Uint8Array`; `contentEncoding` describes those raw bytes. The runtime applies
a 30-minute sync timeout and a 30-second open/read timeout. Application code
should surface failed promises and decide when to retry.

The IndexedDB implementation uses separate `tiles` and `manifests` object
stores and transactional writes. Browser runtime compilation is part of `make
ci`; real-browser integration remains a deployment-level test because CORS,
quotas and service-worker policy belong to the consuming application.

## Protocol evolution

Do not change the interpretation of an existing manifest field in place.
Increment `offline.ProtocolVersion`, document migration behavior and retain a
safe rejection path for unsupported records. Revisioned namespaces mean old
caches can remain readable until an application explicitly prunes them.
