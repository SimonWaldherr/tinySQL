# Focused demos

Run commands from the repository root unless stated otherwise.

| Demo | Start | Prerequisite |
| --- | --- | --- |
| [OSM routing](../cmd/routingdemo/README.md) | `go run ./cmd/routingdemo` | None; embedded synthetic road network |
| [Offline POI explorer](../cmd/offline_demo/README.md) | `go run ./cmd/offline_demo -near 11.5756,48.1372 -radius-m 1000` | None; a local sample snapshot |
| [RAG retrieval](../cmd/ragdemo/README.md) | `go run ./cmd/ragdemo -offline` | None; deterministic demonstration features |
| [Tiles and offline sync](../cmd/tinyTiles/examples/README.md) | Follow the nested module's build/serve commands | A `.ttiles` artifact; WASM build for browser use |
| [SQL feature tour](../cmd/demo/README.md) | `go run ./cmd/demo -timer` | None |
| [Stored procedures](../cmd/procedure_demo/README.md) | `go run ./cmd/procedure_demo` | None |
| [Catalog and scheduler](../cmd/catalog_demo/README.md) | `go run ./cmd/catalog_demo -web` | None; in-memory sample |
| [tinyORM places](../cmd/tinyorm_demo/README.md) | `go run ./cmd/tinyorm_demo -country CH -include-inactive` | None |
| [Game of Life](../cmd/gameoflife/README.md) | `go run ./cmd/gameoflife -pattern glider -plain` | None |
| [Support desk](../cmd/supportdesk/README.md) | `go run ./cmd/supportdesk -web` | Local persistent data in web mode |
| [Geofence service](../cmd/geofence-service/README.md) | `go run ./cmd/geofence-service` | Local data and GeoJSON zones |
| [Document search](../cmd/docsearch/README.md) | `go run ./cmd/docsearch` | Local documents |
| [CSV import validation](../cmd/import-validate/README.md) | `go run ./cmd/import-validate` | A CSV file for import |
| [Worklog](../cmd/worklog/README.md) | `go run ./cmd/worklog` | Local persistent data |

Routing computes fresh answers. RAG batches ingestion and embedding requests;
it does not store embedding or answer caches. Tile offline sync deliberately
persists tiles in IndexedDB, independently of the tile server's payload cache.
The RAG demo also supports LM Studio for model-based embeddings and evaluation;
the offline features are a simple retrieval illustration, not a semantic model.
