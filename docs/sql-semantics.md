# SQL semantics and compatibility

tinySQL supports selected spellings from several SQL dialects. A shared
function name does not imply complete compatibility with that database.
These contracts apply equally to ordinary results, streaming and columnar
results; unsupported optimization shapes use the general evaluator.

[Executable examples](../internal/engine/testdata/sql_semantics.json) lock these
contracts across native results, streams, columns and WASM. The
[compatibility policy](api-stability.md#sql-behavior) applies to changes in their
meaning as well as to changes in SQL syntax.

SQL integer arithmetic retains 64-bit precision. JavaScript's JSON/Number
representation can round integers above 2^53; use CAST(value AS TEXT) when an
exact large integer must cross that boundary. The contracts include the signed
nanosecond limits transported as text.

## Dates and clocks

Timezone-free timestamps parse as UTC. Explicit offsets and native time.Time
locations are retained by component extraction and calendar arithmetic.
There is no session language, session timezone or SET DATEFIRST setting.
NOW and related functions use one timestamp per statement execution.
Clock functions currently require parentheses in SELECT expressions. The
statement clock retains the host's local timezone; use an explicit timestamp
with an offset when a reproducible calendar date is required.

`ISODOW(date)`, `DATEPART(ISO_WEEKDAY, date)` and
`EXTRACT('ISODOW', date)` return Monday=1 through Sunday=7 in the input's
location. `DAYOFWEEK` and DATEPART(WEEKDAY, ...) retain Sunday=1 through
Saturday=7; EXTRACT('DOW', ...) retains Sunday=0 through Saturday=6.

```sql
SELECT id FROM events WHERE ISODOW(occurred_at) = 5;
```

`DATEDIFF` counts crossed boundaries; `TIMESTAMPDIFF` counts complete elapsed
units. Both compare in UTC. Months and years use calendar fields, never a
30-day or 365-day approximation. See [date/time execution](datetime-performance.md)
for examples, supported units, error behavior and the DATEDIFF migration note.

## NULL and strings

| Operation | tinySQL contract |
| --- | --- |
| `x IS NULL`, `x IS NOT NULL` | Boolean predicates; empty strings, zero and FALSE are non-NULL. |
| `ISNULL(x)` | One-argument Boolean NULL test. It does not implement T-SQL's two-argument replacement function. |
| `COALESCE(x, ...)`, `IFNULL(x, y)`, `NVL(x, y)` | First non-NULL value; discarded expressions are not evaluated. Prefer COALESCE for portable SQL. |
| `TRIM`, `LTRIM`, `RTRIM` | One argument removes Unicode whitespace; an optional second argument specifies a set of Unicode characters, not a substring. |
| Explicit empty trim set | Removes nothing. It differs from omitting the argument. |
| NULL trim input or trim set | Returns NULL. Non-NULL input values are converted to text; a non-NULL trim set must be text. |
| `ORDER BY x` without NULL placement | NULLs last for both ASC and DESC, preserving tinySQL's previous default. |
| `ORDER BY x [ASC\|DESC] NULLS FIRST\|LAST` | Explicit placement independent of direction; also supported inside OVER. |

Empty strings and NULL remain distinct. SQL predicates retain three-valued
logic: only TRUE passes WHERE, HAVING or an aggregate FILTER. In particular,
NULL inside a NOT IN list can make its result unknown.

Explicit NULL placement applies to every sort key separately, including
DISTINCT, grouped results, compound queries and LIMIT/OFFSET. Equal complete
sort keys retain input order, including when a bounded heap selects a page.
For a portable application-level tie breaker, specify an additional unique
ORDER BY key.

## Filtered and window aggregates

```sql
SELECT category,
       COUNT(*) FILTER (WHERE enabled) AS enabled_rows,
       SUM(amount) FILTER (WHERE enabled) AS enabled_total
FROM entries
GROUP BY category;

SELECT id,
       COUNT(DISTINCT category) FILTER (WHERE enabled) OVER () AS categories,
       SUM(amount) OVER (
           ORDER BY day_number
           RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS seven_day_total
FROM entries;
```

FILTER applies to one aggregate, after the query's WHERE and before argument
evaluation and DISTINCT. FALSE and NULL exclude the row. Excluded rows still
exist for grouping and for other aggregates. COUNT returns zero over no selected
rows; SUM/AVG/MIN/MAX return NULL. FILTER is supported in SELECT and HAVING and
before OVER. Scalar and ranking functions reject FILTER; its predicate cannot
contain aggregate or window calls.

The raw single-table aggregate path compiles per-aggregate predicates. It does
not use whole-table COUNT/MAX shortcuts or shared batch sample counts for
filtered projections. Join and materialized aggregate shapes that cannot
apply a per-aggregate predicate use the general aggregate evaluator.

COUNT, SUM, AVG, MIN, MAX and MEDIAN support DISTINCT, also with OVER. Aggregate
windows use the same value and NULL rules as grouped aggregates. With ORDER BY,
an omitted frame includes the partition start through the current row's peers;
without ORDER BY, it includes the whole partition.

ROWS offsets count positions. RANGE offsets count values and require exactly
one numeric ORDER BY column. Integer and decimal comparisons retain exact
precision. NULL keys form a peer group; nonnumeric or nonfinite offset keys
raise an error. PRECEDING/FOLLOWING follow the ordering direction, including
DESC. FIRST_VALUE and LAST_VALUE use these same frame bounds; empty frames
return NULL, while COUNT over an empty frame returns zero.

RANGE offsets currently accept nonnegative integers. There is no datetime
INTERVAL syntax, fractional offset or frame EXCLUDE clause. For day ranges,
order by an integer day number rather than a timestamp or a row index. Window
aggregation currently evaluates each frame with the general accumulator;
support does not imply a specialized sliding-window performance optimization.

Window functions and grouped aggregation cannot share a SELECT scope yet.
Nested aggregate/window arguments and window calls inside FILTER are rejected,
including when hidden within IN, BETWEEN or pattern predicates. Aggregate in a
subquery first, then apply windows to its result:

```sql
SELECT category, total, ROW_NUMBER() OVER (ORDER BY total DESC) AS position
FROM (SELECT category, SUM(amount) AS total FROM entries GROUP BY category) AS grouped;
```

Unknown window sort keys raise an error instead of being treated as NULL keys.
HAVING requires GROUP BY or an aggregate; it is never silently discarded on
an ordinary row query.

## Splitting strings into rows

```sql
SELECT value, ordinal
FROM STRING_SPLIT('alpha::beta::::', '::', 1)
ORDER BY ordinal;
```

STRING_SPLIT accepts a nonempty literal delimiter of any length, including
Unicode. Consecutive and trailing separators retain empty fields. Empty or
NULL input, or a NULL delimiter, yields zero rows while retaining output
column metadata. An empty delimiter raises an error. This is delimiter
splitting, without CSV quoting rules.

The third argument is optional integer 0 or 1. With 1, the result includes
one-based `ordinal`; otherwise it contains only `value`. Ordinals follow input
order, including empty fields. Use ORDER BY ordinal when that order is part of
the query contract. Multi-character delimiters and a parameterized ordinal flag
are tinySQL extensions to T-SQL's STRING_SPLIT behavior.

For fields across columns rather than rows, use TEXT_TO_COLUMNS, which already
supports multi-character delimiters and permits an empty delimiter to split
Unicode characters.
