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
SELECT 17 % 5 AS remainder; -- 2 (INT)
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

`%` follows the arithmetic model below: two integers give an integer, a
REAL operand gives a REAL, and two decimal rational operands stay exact. It is
a remainder with truncation toward zero, not a nonnegative mathematical
modulo. Division by zero raises an error. NULL propagates.

## Integer arithmetic

Arithmetic keeps integers exact, as SQLite does. Every evaluation path — the
generic evaluator, raw projections and filters, joins, HAVING, columnar
results, index-seek planning and all aggregate kernels — shares one
implementation (`internal/engine/arith.go`), and a differential test compares
their values and Go types.

| Expression | Result |
| --- | --- |
| `+`, `-`, `*`, `%` on two integers | Integer (`SELECT 1 + 41` → `42`, `SELECT -5` → `-5`) |
| Integer overflow of `+`, `-`, `*` or unary `-` | REAL with the nearest value (`9223372036854775807 + 1` → `9.223372036854776e+18`) |
| Any REAL operand | REAL (`1.5 + 1` → `2.5`) |
| `/` | Always REAL (`7 / 2` → `3.5`, `84 / 2` → `42.0`) |
| DECIMAL/MONEY operands | Exact rational |
| `SUM` over integers | Integer; REAL if the total overflows 64 bits |
| `SUM` with any REAL input, `AVG` | REAL |
| `MOVING_SUM` | Like `SUM` |

Division deliberately differs from SQLite, whose `7 / 2` truncates to `3`:
tinySQL has always returned the exact quotient, and silently truncating
existing queries would be a far more dangerous change than keeping it. Use
`CAST(7 / 2 AS INT)` or `FLOOR` where truncation is intended. Unlike SQLite,
an overflowing integer `SUM` continues as REAL instead of raising
"integer overflow".

Before this change every arithmetic result was a float64: `SELECT 1 + 41`
returned `42.0`, integers above 2^53 lost precision, and `WHERE id = 2^53 + 1`
style comparisons could match neighbouring keys. Applications that relied on
float results from integer arithmetic (for example a Go type assertion to
`float64` on `SUM(int_column)`) now receive `int` values; database/sql
drivers see `int64`.

Both CTE INSERT spellings are supported:
`WITH ... INSERT INTO ... SELECT ...` and `INSERT INTO ... WITH ... SELECT ...`.
`CREATE [TEMP] TABLE name AS WITH ... SELECT ...` is also supported, including
`IF NOT EXISTS`, through the same SELECT/CTE parser.
The new leading-WITH form requires an INSERT SELECT source; it does not add
WITH-prefixed UPDATE, DELETE or INSERT VALUES. Data-modifying CTE bodies are
also outside this implementation.

Views use their existing expansion/depth-limit and materialized-refresh
behavior. A CTE of the same name takes precedence. This does not add lateral
or correlated JOIN sources. The Game of Life demo now exercises `%` and a
direct join against a view: `go run ./cmd/gameoflife -steps 100`.

## Subquery membership and pagination

`IN (SELECT ...)` and `NOT IN (SELECT ...)` compare against every row of a
one-column result. Duplicate candidates do not change membership. A matching
value takes precedence over NULL candidates; without a match, a NULL candidate
makes the result unknown. An empty subquery yields FALSE for IN and TRUE for
NOT IN, including a NULL left operand. Direct `IN (WITH ... SELECT ...)` also
works. Results are cached only within an execution when the existing subquery
cache proves it safe.

AND/OR filters with a subquery fall back to the general evaluator. A simple
sibling comparison no longer sends an unsupported IN, scalar or EXISTS
subquery into the raw-row evaluator.

Scalar subqueries still allow at most one row and now require exactly one
column, including when they return no rows. An additional pair of parentheses,
as in `IN ((SELECT ...))`, keeps the subquery scalar and retains its cardinality
check.

Pagination accepts `OFFSET n ROW` as well as `ROWS`, and a FETCH clause without
a count defaults to one row:

```sql
SELECT id FROM numbers ORDER BY id OFFSET 2 ROW FETCH NEXT ROW ONLY;
```

These forms follow the [SQL:2008 pagination syntax documented by PostgreSQL](https://www.postgresql.org/docs/17/sql-select.html#SQL-LIMIT).
`LIMIT ALL` remains supported; `OFFSET ALL`, `FETCH FIRST ALL ROWS ONLY` and
combining LIMIT with FETCH are rejected. Integer bounds remain exact above
2^53, up to the platform's maximum `int`; out-of-range bounds are rejected
before conversion. Both materialized and raw execution paths handle large
valid limits and offsets without allocating a buffer of that size.

## Date and time functions

Date/time evaluation now preserves a statement-wide clock, calendar-aware
truncation and month-end offsets. STRFTIME handles escaped percent signs and
literal text directly, supports SQLite's documented substitutions, and returns
NULL for unsupported codes. Date/time functions propagate NULL inputs; numeric
timestamp conversion preserves native integer seconds and rejects invalid
ranges. See [date/time execution](datetime-performance.md) for compatibility
boundaries, parser and formatter measurements, and regression commands.

MAKE_DATE/DATEFROMPARTS, LAST_DAY, DAYNAME, MONTHNAME and DATEPART provide
calendar construction and component helpers. CONVERT/TRY_CONVERT support
type-first conversion and a documented subset of explicit date styles.
Ordinary dates remain proleptic Gregorian. TO_CALENDAR, FROM_CALENDAR and
CALENDAR_CONVERT provide explicit Julian, Discordian and Islamic civil
conversion, plus ISO week and ordinal date formats. See
[calendar conversion](calendar-conversion.md) for syntax and supported ranges.
JULIANDAY remains an astronomical day number. SQL Server session settings and
observational calendar variants remain unsupported.

ISODOW and DATEPART(ISO_WEEKDAY, ...) provide an explicit Monday-first weekday.
DATEDIFF now counts crossed UTC boundaries; TIMESTAMPDIFF provides complete
elapsed units. This replaces DATEDIFF's previous fixed-duration approximations
for months and years. See the [migration note](datetime-performance.md#differences-boundaries-and-elapsed-units).

## Filtered aggregates and explicit ordering

Aggregate FILTER (WHERE ...) is supported for grouped, ungrouped and window
aggregates, including HAVING. It evaluates the predicate before the aggregate
argument, excludes FALSE/NULL and retains groups whose selected input is empty.
COUNT(DISTINCT ...), SUM/AVG and the other ordinary aggregates can also use OVER.

ORDER BY accepts NULLS FIRST/LAST, including in window definitions. ROWS frames
count positions; numeric RANGE offsets count values and require one ORDER BY
column. FIRST_VALUE/LAST_VALUE share aggregate frame bounds, including empty
frames. STRING_SPLIT accepts multi-character delimiters and an optional ordinal
column. [SQL semantics](sql-semantics.md) describes the contracts, execution
fallbacks and remaining window limitations.

## Constraints, upserts and savepoints

| Feature | Behavior |
| --- | --- |
| `CHECK` constraints | Column-level or table-level, optionally named with `CONSTRAINT name`. Accepted by CREATE TABLE and by ALTER TABLE ... ADD COLUMN, which validates the existing rows first. Enforced on INSERT, UPDATE and upsert with SQL three-valued logic: FALSE fails, while TRUE and NULL pass. Subqueries, aggregates, window functions and parameters are rejected when the table is defined. |
| `ON CONFLICT (target) DO UPDATE` | The target is a PRIMARY KEY or UNIQUE column, or a unique index whose columns match the target exactly. `excluded.column` is the proposed row. The existing row is read by column name or qualified by the table name. An optional `WHERE` gates the update. `DO NOTHING` also accepts a target. The conflict update fires AFTER UPDATE triggers, with the existing row as `OLD`. |
| `SAVEPOINT`, `RELEASE`, `ROLLBACK TO` | Accepted by the database/sql driver inside an explicit transaction. Savepoints nest, and `ROLLBACK TO` keeps the savepoint. Outside a transaction they fail. |

```sql
CREATE TABLE inventory (
  id  INT PRIMARY KEY,
  sku TEXT UNIQUE,
  qty INT NOT NULL CHECK (qty >= 0)
);

INSERT INTO inventory VALUES (1, 'A1', 5)
  ON CONFLICT (id) DO UPDATE SET qty = inventory.qty + excluded.qty
  WHERE inventory.qty < 1000
  RETURNING id, qty;

BEGIN;
INSERT INTO inventory VALUES (2, 'B2', 3);
SAVEPOINT before_restock;
UPDATE inventory SET qty = qty * 2;
ROLLBACK TO SAVEPOINT before_restock; -- the doubling is undone, the insert stays
COMMIT;
```

- The proposed row is checked against CHECK constraints before the conflict
  lookup. A malformed row is rejected even when `DO NOTHING` would skip it, in
  the same way NOT NULL is enforced first.
- `DELAY_INSERT` queues only a target-less `ON CONFLICT DO NOTHING`. A conflict
  target or `DO UPDATE` is rejected when the row is queued, because the queued
  form keeps no more than that flag.
- A savepoint copies the transaction's tables when it is created. A long
  transaction that opens many savepoints over large tables pays for each copy.
- On `mode=advancedwal`, `ROLLBACK TO SAVEPOINT` logs compensating row
  operations, so a restart replays the transaction to the savepoint's state. It
  cannot undo a CREATE, DROP or ALTER TABLE made after the savepoint: that
  rollback fails and the transaction continues unchanged. Other modes have no
  such restriction.
- `RowsAffected` for an INSERT still counts the VALUES rows, so a skipped or
  unchanged conflict is included in the count.

## Important remaining gaps

| Missing operation | Current boundary | Main implementation work |
| --- | --- | --- |
| Composite PRIMARY/UNIQUE/FOREIGN KEY constraints | Constraint metadata is column-oriented | Composite metadata, conflict indexes, foreign-key matching and cascades. |
| Partial indexes and generated columns | Not supported by the schema/parser | Persist predicates/expressions and maintain them on every mutation path. |
| WITH-prefixed UPDATE/DELETE and modifying CTE bodies | SELECT-based CTEs only | DML scope, execution order, RETURNING relations and atomic rollback. |

These larger changes remain explicit gaps; accepting their syntax without
implementing constraints, transaction semantics or index maintenance would
silently produce incorrect results. They are not represented as implemented
by the additions above.

## Verification

```sh
go test ./internal/engine -run 'TestModuloOperator|TestLeadingWith|TestViewsAsJoin|TestArithmeticPathsAgree|TestBatchSumMatchesScalarAccumulator|TestIntegerArithmeticSemantics'
go test ./internal/driver -run TestPreparedLeadingWithInsertAndModulo
go test ./cmd/gameoflife
go test ./internal/engine -run 'TestInSubquery|TestInScalarSubquery|TestLogicalSubqueriesDeclineRawFilter|TestSubqueryRequiresSingleColumn|TestSubqueryCache|TestFetchDefaultCountAndSingularOffset|TestPagination'
go test ./internal/driver -run TestSQLPreparedInSubqueryBindingsAndFetch
go test ./cmd/wasm_browser
go test ./internal/engine -run 'TestCheckConstraints|TestOnConflict|TestDelayedInsertRejectsConflictTargets'
go test ./internal/driver -run 'TestSavepoint|TestDriverSavepoints|TestSQLiteTransactionSpellings|TestUpsertAndCheckConstraintsSurviveRestart'
go test . -run TestBuilderInsertToSQLKeepsOnConflictClause
```

The regressions cover operator precedence, NULL/zero/negative/decimal operands,
raw scans and joins, nested CTE scope, recursive INSERT, conflicts and rollback,
delayed delivery, prepared parameters, empty joined views and fresh view data.
The constraint, upsert and savepoint regressions also reopen the database: a
copy taken while it is open exercises WAL replay, and a clean restart exercises
the checkpoint.
