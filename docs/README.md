# Documentation map

Use this page to choose one guide for the task at hand. The guides describe
current behavior and keep performance claims scoped to the workload they measure.

## Start here

- [Developer integration](developer-integration.md) — embed tinySQL from Go,
  `database/sql`, or WebAssembly.
- [Storage and persistence](storage-guide.md) — select a storage mode, configure
  a DSN, back up data, and serve read-only artifacts.
- [CLI guide](cli-guide.md) — find the command-line tools and browser playground.
- [Go API stability](api-stability.md) — public-surface and streaming guarantees.

## Build and change tinySQL

- [Development guide](development-guide.md) — tests, Make targets, hooks, and
  releases.
- [Repository structure](repository-structure.md) — where packages and commands
  live.
- [Architecture](architecture.md) and [architecture diagrams](architecture-diagrams.md)
  — layer ownership, statement flow, transactions, and persistence invariants.
- [SQL feature gaps](sql-feature-gaps.md) — supported additions and known limits.

## Use the runtime

- [Specialized tables](specialized-tables.md) — `keyvalue`, `document`, and
  `timeseries` table profiles.
- [Automatic indexes](automatic-indexes.md) — opt-in observation and index
  creation.
- [Delayed inserts](delayed-inserts.md) — durable scheduled INSERT work.
- [Asynchronous runtime](async-runtime.md) — jobs and the transactional event log.
- [Reactive SQL queries](subscribe-sql.md) — `SubscribeSQL`, limits, and delivery
  semantics.
- [Cold point reads](cold-start-reactive.md) — persisted-index behavior after
  reopening.
- [Columnar execution](columnar-execution.md) — aggregate batches and compact
  result sets.
- [Clusters and load balancing](cluster.md) — primary/replica deployment.
- [TinyGo and embedded targets](tinygo-guide.md) — supported targets and build
  flow.

## Search, RAG, and GIS

- [RAG guide](rag-guide.md) — corpus design, ingestion, hybrid retrieval,
  filtering, evaluation, and operations.
- [RAG optimization roadmap](rag-optimization-roadmap.md) — implemented baseline
  and planned retrieval work.
- [Retrieval performance](retrieval-performance.md) — search-path measurements
  and links to focused RAG/GIS notes.
- [Exact RAG postings](rag-postings-performance.md) and
  [filtered RAG postings](rag-filtered-postings-performance.md) — detailed
  lexical-retrieval measurements and correctness constraints.
- [Geospatial standards](geospatial-standards.md) — interoperable formats, CRS,
  GeoPackage, and map-service profiles.
- [Geospatial performance](geospatial-performance.md) — geometry and spatial-grid
  execution notes.
- [Tile serving and routing](tiles-routing-serving.md) and [OSM routing](osm-routing.md)
  — serving, route graphs, and OSM import.

## Design and performance notes

- [Memory optimization](memory-optimization.md) — resident-memory versus
  allocation-churn tradeoffs.
- [Driver and engine performance](driver-engine-performance.md) — shared
  allocation and execution-path improvements; it links to focused records below.
- [CTE alignment](cte-performance.md), [CTE and indexed SELECT](cte-select-performance.md),
  [ORDER BY and aggregates](order-aggregate-performance.md), and
  [windows, DISTINCT, and LIMIT](window-distinct-limit-performance.md).
- [INSERT and UPDATE](insert-update-performance.md),
  [index and trigger allocation](index-trigger-performance.md), and
  [trigger, range-index, WAL, and CTE follow-up](trigger-index-wal-cte-performance.md).
- [CPU and storage modes](cpu-storage-mode-performance.md),
  [query generation](query-generation-performance.md), and
  [streaming and live queries](streaming-live-performance.md).
- [Text predicates](predicate-string-performance.md), [LIKE and views](like-view-performance.md),
  and [REGEXP](regexp-performance.md).
