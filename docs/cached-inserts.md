# Cached inserts

Use `INSERT CACHED` to stage rows and `FLUSH INSERTS` to apply them to the
destination in one atomic batch.

```sql
CREATE TABLE documents (id INT PRIMARY KEY, body TEXT);
INSERT CACHED INTO documents VALUES (1, 'first'), (2, 'second');
INSERT CACHED INTO documents VALUES (3, 'third');
SELECT COUNT(*) FROM documents; -- 0
FLUSH INSERTS INTO documents;
SELECT COUNT(*) FROM documents; -- 3
```

tinySQL creates an internal staging table with the destination's columns. It
evaluates `VALUES` expressions and `INSERT ... SELECT` when staging the rows,
converts values to the destination types and fills defaults for omitted
columns. An explicit `NULL` stays `NULL`.

The flush applies destination constraints, triggers and indexes. A constraint,
trigger or WAL error while applying the batch rolls back the destination
changes and preserves the pending rows. A schema change or a destination
that cannot fit in the configured table cache can also prevent a flush and
leave the batch pending. As with ordinary inserts, a `database/sql` disk save
can fail after the batch has already been applied in memory.

Use `DISCARD INSERTS FROM documents` to clear the staged rows. Both control
commands return zero when there are no staged rows. `INTO` and `FROM` are
optional in the corresponding commands.

Pending rows belong to the database instance, tenant, destination table and
acting user. Connections in the same tenant using the same identity share the
batch. Pending rows live in memory and are lost on close or restart. RAM
usage grows with the staged values. Each `VECTOR` coordinate uses eight
bytes, plus row and value overhead. An empty destination without triggers or
`CHECK` constraints can reuse the staged row headers during the flush. For
durable scheduled delivery, see [Delayed inserts](delayed-inserts.md).

Run cached insert commands outside explicit transactions, procedure and
trigger bodies, and `EXPLAIN ANALYZE`. `INSERT CACHED` supports named columns,
multi-row `VALUES` and `SELECT` sources. `RETURNING` and `ON CONFLICT` are
unavailable for this form. In `database/sql`, `Exec` reports how many rows were
staged, inserted or discarded via `RowsAffected`.
