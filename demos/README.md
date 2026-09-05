# Focused demos

Run commands from the repository root unless stated otherwise.

| Demo | Start | Prerequisite |
| --- | --- | --- |
| [OSM routing](../cmd/routingdemo/README.md) | `go run ./cmd/routingdemo` | None; embedded synthetic road network |
| [RAG evaluation](../cmd/ragdemo/README.md) | `go run ./cmd/ragdemo -verbose` | LM Studio embedding server |
| [Tiles and offline sync](../cmd/tinyTiles/examples/README.md) | Follow the nested module's build/serve commands | A `.ttiles` artifact; WASM build for browser use |
| [SQL feature tour](../cmd/demo/README.md) | `go run ./cmd/demo -timer` | None |

Routing computes fresh answers. RAG batches ingestion and embedding requests;
it does not store embedding or answer caches. Tile offline sync deliberately
persists tiles in IndexedDB, independently of the tile server's payload cache.
