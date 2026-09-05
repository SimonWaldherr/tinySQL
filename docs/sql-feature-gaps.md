# SQL feature gaps and additions

This inventory is based on tinySQL's parser, executor and regression tests. It
is not a claim of complete SQL-standard or SQLite/PostgreSQL compatibility.
Some older README limitations described features that have since been added:
for example, target-less `ON CONFLICT DO NOTHING` already works.

## Added in this change

| Feature | Behavior |
| --- | --- |
| `%` remainder operator | Same precedence as `*` and `/`, evaluated left to right; works in ordinary expressions and raw execution paths. |
| Leading `WITH` for INSERT | `WITH ... INSERT INTO ... SELECT ...`, including recursive CTEs, `RETURNING` and `ON CONFLICT DO NOTHING`. |
| Nested CTE declarations | A CTE body may itself start with `WITH`; inner names shadow outer names within that body. |
| Views on the right of JOIN | Ordinary and materialized views supply rows and column metadata, including empty-view LEFT JOIN null extension. |

```sql
SELECT 17 % 5 AS remainder; -- 2
SELECT -17 % 5 AS remainder; -- -2
SELECT 5.5 % 2 AS remainder; -- 1.5
SELECT NULL % 5 AS remainder; -- NULL

CREATE TABLE numbers (id INT PRIMARY KEY);
WITH RECURSIVE seq(n) AS (
  SELECT 1
  UNION ALL SELECT n + 1 FROM seq WHERE n < 10
)
INSERT INTO numbers SELECT n FROM seq RETURNING id;

WITH outer_cte AS (
  WITH inner_cte AS (SELECT 7 AS n)
  SELECT n FROM inner_cte
)
SELECT n FROM outer_cte;

CREATE VIEW even_numbers AS SELECT id FROM numbers WHERE id % 2 = 0;
SELECT n.id, e.id AS even_id
FROM numbers n LEFT JOIN even_numbers e ON n.id = e.id;
```

`%` uses tinySQL's existing arithmetic numeric model: ordinary numbers are
converted to floating point, while two decimal rational operands stay exact.
It is a remainder with truncation toward zero, not a nonnegative mathematical
modulo. Division by zero raises an error. NULL propagates. This is not SQLite's
integer-only `%` coercion rule; do not rely on floating-point arithmetic to
preserve integers larger than 2^53.

Both CTE INSERT spellings are supported:
`WITH ... INSERT INTO ... SELECT ...` and `INSERT INTO ... WITH ... SELECT ...`.
The new leading-WITH form requires an INSERT SELECT source; it does not add
WITH-prefixed UPDATE, DELETE or INSERT VALUES. Data-modifying CTE bodies are
also outside this implementation.

Views use their existing expansion/depth-limit and materialized-refresh
behavior. A CTE of the same name takes precedence. This does not add lateral
or correlated JOIN sources. The Game of Life demo now exercises `%` and a
direct join against a view: `go run ./cmd/gameoflife -steps 100`.

## Important remaining gaps

| Missing operation | Current boundary | Main implementation work |
| --- | --- | --- |
| `CHECK` constraints | Explicitly rejected by column/table constraint parsing | Persist expressions and enforce SQL three-valued constraint semantics on INSERT, UPDATE and schema changes. |
| Composite PRIMARY/UNIQUE/FOREIGN KEY constraints | Constraint metadata is column-oriented | Composite metadata, conflict indexes, foreign-key matching and cascades. |
| `ON CONFLICT (...) DO UPDATE` / full UPSERT | Only target-less `DO NOTHING` exists | Conflict-target resolution, `excluded` row binding, UPDATE constraints and triggers. |
| `SAVEPOINT`, `ROLLBACK TO`, `RELEASE` | Driver deliberately rejects nested transaction spellings | Nested rollback boundaries integrated with storage and WAL. |
| Partial indexes and generated columns | Not supported by the schema/parser | Persist predicates/expressions and maintain them on every mutation path. |
| Aggregate `FILTER (WHERE ...)` | Function parser handles calls and OVER, without FILTER | Per-aggregate predicates across generic, grouped and optimized aggregate paths. |
| WITH-prefixed UPDATE/DELETE and modifying CTE bodies | SELECT-based CTEs only | DML scope, execution order, RETURNING relations and atomic rollback. |

These larger changes remain explicit gaps; accepting their syntax without
implementing constraints, transaction semantics or index maintenance would
silently produce incorrect results. They are not represented as implemented
by the additions above.

## Verification

```sh
go test ./internal/engine -run 'TestModuloOperator|TestLeadingWith|TestViewsAsJoin'
go test ./internal/driver -run TestPreparedLeadingWithInsertAndModulo
go test ./cmd/gameoflife
```

The regressions cover operator precedence, NULL/zero/negative/decimal operands,
raw scans and joins, nested CTE scope, recursive INSERT, conflicts and rollback,
delayed delivery, prepared parameters, empty joined views and fresh view data.
