# Offline POI demo

Create or reopen a local POI snapshot, put it into read-only mode, and search it
without a server, network access, or SQLite dependency.

```bash
# In-memory dataset
go run ./cmd/offline_demo

# Build a reusable local snapshot, then reopen it on the next run
go run ./cmd/offline_demo -snapshot /tmp/tinysql-poi.snapshot -query museum

# Stable output for scripts
go run ./cmd/offline_demo -snapshot /tmp/tinysql-poi.snapshot -json
```

Further flags: `-rebuild` ignores an existing snapshot, `-read-only` (default
true) rejects writes after the dataset is loaded or created.

The tiny dataset only illustrates the lifecycle and read-only behavior; larger
map datasets use the dedicated POI-index or MBTiles paths.
