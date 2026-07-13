# Offline POI demo

This demo shows a deliberately small embedded use case: create or reopen a
local POI snapshot, put it into read-only mode, and search it without a server,
network access, or SQLite dependency.

```bash
# In-memory dataset
go run ./cmd/offline_demo

# Build a reusable local snapshot, then reopen it on the next run
go run ./cmd/offline_demo -snapshot /tmp/tinysql-poi.snapshot -query museum

# Stable output for scripts
go run ./cmd/offline_demo -snapshot /tmp/tinysql-poi.snapshot -json
```

The demo intentionally uses a tiny dataset. It illustrates the lifecycle and
read-only behavior for embedded applications; it is not a substitute for the
dedicated POI-index or standard MBTiles paths used with larger map datasets.
