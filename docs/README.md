# Documentation map

Use this page to choose one guide for the task at hand. The guides describe
current behavior and keep performance claims scoped to the workload they measure.

## Start here

- [Developer integration](developer-integration.md) — embed tinySQL from Go,
  `database/sql`, or WebAssembly.
- [Language bindings](language-bindings.md) — embed tinySQL in Rust, Python,
  Swift, or any C-compatible host through the shared C ABI.
- [Storage and persistence](storage-guide.md) — select a storage mode, configure
  a DSN, back up data, and serve read-only artifacts.
- [CLI guide](cli-guide.md) — find the command-line tools and browser playground.
- [Go API stability](api-stability.md) — public-surface and streaming guarantees.
- [SQL function reference](reference/functions.html) — built-ins and runnable
  examples, generated from the canonical SQL source.

## Build and change tinySQL

- [Development guide](development-guide.md) — tests, Make targets, documentation
  checks, hooks, and releases.
- [Repository structure](repository-structure.md) — where packages and commands
  live.
- [Architecture](architecture.md) and [architecture diagrams](architecture-diagrams.md)
  — layer ownership, statement flow, transactions, and persistence invariants.
- [SQL feature gaps](sql-feature-gaps.md) — supported additions and known limits.
- [SQL semantics](sql-semantics.md) — date differences, NULLs, trimming, filtered
  aggregates, window frames and dialect compatibility.

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
- [Analytical snapshots](analytical-snapshots.md) — column-oriented table exports
  and their limitations.
- [Clusters and load balancing](cluster.md) — primary/replica deployment.
- [TinyGo and embedded targets](tinygo-guide.md) — supported targets and build
  flow.

## Search, RAG, and GIS

- [RAG guide](rag-guide.md) — corpus design, ingestion, hybrid retrieval,
  filtering, evaluation, and operations.
- [RAG optimization roadmap](rag-optimization-roadmap.md) — implemented baseline
  and planned retrieval work.
- [IVF training](ivf-training-performance.md) — e-means research, reproducible
  initialization, exact convergence and retrieval-quality measurements.
- [Exact vector pruning and selection](vector-pruning-selection.md) —
  SuperKMeans-inspired bounds and batched top-k initialization.
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

- [Loading and replica lifecycle](loading-replication-performance.md) — restore
  ownership, streamed bootstrap, shutdown, allocation and process-memory measurements.
- [SELECT, conversions, and vector ranking](select-conversion-vector-performance.md)
  — bind conversion declarations, preserve BETWEEN semantics and prepare query vectors.
- [Ordered CTE consumers](cte-order-performance.md) — bind sort keys, select
  pages before allocating output maps, and preserve materialization boundaries.
- [DELETE and materialization](delete-cte-materialization-performance.md) —
  research sources, CTE binding, INTO/TEMP construction and measured tradeoffs.
- [JSON and set performance](json-and-set-performance.md) — Go JSON compatibility,
  streaming export, normalization, and set representation.
- [RAG, vector, and GROUP BY performance](rag-group-performance.md) — context
  expansion, HNSW traversal, integer grouping, and aggregate pagination.
- [Query and index performance](query-index-performance.md) — text ordering,
  UNION chains, keyword parsing, and persistence snapshots.
- [Memory optimization](memory-optimization.md) — resident-memory versus
  allocation-churn tradeoffs.
- [Driver and engine performance](driver-engine-performance.md) — shared
  allocation and execution-path improvements; it links to focused records below.
- [Standard SQL execution](standard-query-performance.md) — bound SELECT, WHERE
  and UPDATE expressions, plus raw ordering without copying sort candidates.
- [Conditional, NULL and text execution](conditional-expression-performance.md)
  — CASE, ISNULL, NULLIF, COALESCE, computed BETWEEN, TRIM and measured tradeoffs.
- [Numeric DISTINCT and general JOINs](distinct-join-performance.md) — float
  deduplication, mixed-type counts and reuse of integer join keys.
- [Query optimizer research](query-optimizer-research.md) — PostgreSQL, DuckDB
  and SQLite techniques assessed against tinySQL, with adaptive large-page sorts.
- [CTE alignment](cte-performance.md), [CTE and indexed SELECT](cte-select-performance.md),
  [ORDER BY and aggregates](order-aggregate-performance.md), and
  [windows, DISTINCT, and LIMIT](window-distinct-limit-performance.md).
- [Transaction twins](transaction-twin-performance.md) — a transaction's first
  write reuses the row array and index lists of the table the last commit
  replaced instead of copying the table.
- [Transaction snapshots](transaction-snapshot-performance.md) — `BEGIN` shares
  live tables and copies on first write instead of copying the database.
- [INSERT and UPDATE](insert-update-performance.md),
  [index and trigger allocation](index-trigger-performance.md), and
  [trigger, range-index, WAL, and CTE follow-up](trigger-index-wal-cte-performance.md).
- [CPU and storage modes](cpu-storage-mode-performance.md),
  [query generation](query-generation-performance.md), and
  [streaming and live queries](streaming-live-performance.md).
- [Text predicates](predicate-string-performance.md), [LIKE and views](like-view-performance.md),
  and [REGEXP](regexp-performance.md).
