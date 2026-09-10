# Specialized SQL table profiles

TinySQL provides three workload-specific schemas through its existing virtual
TABLE syntax and a Go API. They use ordinary physical SQL tables, constraints,
indexes and persistence. A profile is independent of the DB's storage mode;
it is not a separate storage engine or a new transaction implementation.

| Module | Default columns | Access path |
|---|---|---|
| `keyvalue` | `key TEXT PRIMARY KEY NOT NULL`, `value BLOB` | Persisted unique point index |
| `document` | `id TEXT PRIMARY KEY NOT NULL`, `document JSON NOT NULL` | Persisted unique point index; strict JSON validation |
| `timeseries` | `series TEXT NOT NULL`, `time INT NOT NULL`, `value FLOAT NOT NULL` | Internal composite index on `(series, time)` |

```sql
CREATE VIRTUAL TABLE cache USING keyvalue;
INSERT INTO cache VALUES ('greeting', X'68656c6c6f');
SELECT value FROM cache WHERE key = 'greeting';
UPDATE cache SET value = X'627965' WHERE key = 'greeting';
DELETE FROM cache WHERE key = 'greeting';

CREATE VIRTUAL TABLE documents USING document;
INSERT INTO documents VALUES ('user:1', '{"name":"Ada","active":true}');
SELECT JSON_GET(document, 'name') AS name FROM documents WHERE id = 'user:1';

CREATE VIRTUAL TABLE metrics USING timeseries;
INSERT INTO metrics VALUES ('cpu', 1000, 0.25), ('cpu', 1010, 0.75);
SELECT time, value FROM metrics
WHERE series = 'cpu' AND time >= 1000 AND time < 2000
ORDER BY time;
```

Supply all column names to rename them, preserving their order and types:

```sql
CREATE VIRTUAL TABLE objects USING keyvalue(object_key, payload);
CREATE VIRTUAL TABLE IF NOT EXISTS events USING document(event_id, body);
CREATE VIRTUAL TABLE measurements USING timeseries(sensor, tick, reading);
```

Unknown modules and duplicate/wrong column lists return errors. `IF NOT EXISTS`
leaves an existing table unchanged; it does not validate or convert its schema.
The `fts` module continues to work as before.

## Go API

```go
err := tinysql.CreateSpecializedTable(ctx, db, "default", "cache", tinysql.KeyValueTable)
// Other kinds: tinysql.DocumentTable and tinysql.TimeSeriesTable.
// Optional trailing strings rename every profile column in order.
```

Use normal SQL, the builder or database/sql prepared statements for operations.
Creation through the Go API builds an AST directly without parsing generated SQL.
Keys are case-sensitive TEXT values. BLOB values can be NULL; duplicate keys fail.
INSERT and UPDATE remain distinct operations: no atomic upsert/Set API is added.
DELETE uses the existing SQL row-removal path and is not promised O(1).

Document writes validate JSON and require a top-level object or array and decode it into owned maps/arrays.
Invalid JSON is rejected on INSERT and UPDATE, including after reopening a DB.
Ordinary JSON columns retain their previous permissive conversion semantics.
SQL NULL, JSON null and other top-level scalar values are rejected. Nested
strings, booleans, nulls and numbers are supported; JSON numbers use float64,
so encode precision-sensitive identifiers as nested strings. JSON_GET uses
TinySQL's `name.nested[0]` paths, not a `$`-prefixed JSONPath syntax.

Time is an integer tick chosen by the application; use one consistent unit per
table (for example Unix milliseconds on a 64-bit host). This profile does not
convert time zones. Equal series/time pairs are allowed. The internal index
`__timeseries_series_time` is table-local and persisted with the table; it is
not a separately registered user-created catalog index. There is no automatic
retention, compaction, downsampling or append-only restriction.

## Storage and compatibility

Round-trip tests cover Memory, Disk, Hybrid, Index, JSON and Paged Index, including
BLOB bytes, structured JSON, strict validation metadata and the time-series index.
AdvancedWAL replay tests cover inserts, updates and index reconstruction.
The profile identity is expressed by its schema/index configuration rather than
a separate persistent table-kind flag. You may use normal SQL DDL to modify it.

Supporting these profiles also fixes native BLOB decoding and integer cell
restoration in JSON storage, and preserves NOT NULL/strict-JSON metadata in Paged
Index. The pager adds row tag `0x09` for JSON maps/arrays with a bounded uint32
payload length; full and single-column readers understand it. New code reads
old files, but older binaries cannot read rows containing the new JSON tag.
GOB/JSON schemas gain an optional `StrictJSON` field (false for old tables).
Keep the updated binary when reopening new document tables; older releases do
not enforce the new strict-JSON metadata.

## Cold access and verification

New key-value and document tables include the internal unique index
`__profile_key`. It serves point SELECTs immediately from the persisted index
and permits Paged Index to fetch individual rows without loading the full table.
The primary-key constraint retains its existing hash cache for enforcement.
Existing tables are not automatically migrated: add a normal unique index on
the key to enable the same persisted access path.

See [cold-start measurements and SELECT subscriptions](cold-start-reactive.md)
for current measurements, including the cost of reopening the database.

```sh
go test ./internal/engine -run '^TestSpecialized' -count=1
go test ./internal/engine -run '^$' -bench '^BenchmarkKeyValueProfile$' -benchmem
go test -race ./internal/engine ./internal/storage ./internal/storage/pager \
  -run 'Test(Specialized|RowCodec|Disk|JSON)' -cpu=1,36 -count=1
go test ./...
```
