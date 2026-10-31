# Date and time execution

## Parsing and formatting

`parseDateTime` dispatches legacy `MM/DD/YYYY` and `DD-Mon-YYYY` dates to their
matching layout before trying ISO layouts. `parseTimeValue` also reads common
fixed-width clocks and minute-precision SQL timestamps directly. The parsers
retain their previous accepted layouts, timezone offsets and error behavior.
In particular, this does not extend the SQLite-named date functions to accept
bare clock strings. Native `time.Time` inputs retain their location.

`STRFTIME` scans substitutions directly instead of translating the format to
a Go time layout. Literal text such as `2006`, `Jan` and `15:04` therefore
stays literal, and `%%Y` produces `%Y`. The formatter supports the substitutions
listed in the [SQLite date/time documentation](https://sqlite.org/lang_datefunc.html),
including ISO week years, Monday/Sunday week numbers and fractional seconds.
Unknown substitutions and a trailing `%` return NULL. `%f` rounds to
milliseconds, capped at `59.999`; `%J` uses the engine's existing JULIANDAY
calculation, so submillisecond results need not match SQLite byte for byte.

The existing timezone policy remains: timezone-free timestamps parse as UTC,
explicit offsets are retained, and implicit or explicit `'now'` uses the local
statement clock. SQLite's modifiers such as `'utc'` and `'+1 day'` remain
unsupported and raise an error; use DATE_ADD/DATE_SUB for arithmetic.

## Calendar and value semantics

Clock functions, date/time functions with omitted values or `'now'`, and
IN_PERIOD use the same timestamp throughout one statement. This applies to
ordinary, raw and streamed evaluation. Prepared statements receive a fresh
clock on each execution. This is a statement clock, not a transaction clock.

DATE_TRUNC uses local calendar boundaries rather than absolute 24-hour
intervals for DAY and WEEK. HOUR and MINUTE also respect the input's local
clock and preserve the occurrence of repeated DST hours where that boundary
exists. WEEK begins on Monday. This follows the calendar interpretation of
[date_trunc](https://www.postgresql.org/docs/17/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC)
while retaining tinySQL's input location rather than a session timezone setting.

EOMONTH applies its offset to the calendar month, so
`EOMONTH('2024-01-31', 1)` returns February 29. Component extractors, the
SQLite-named date/time family, DATEDIFF, DATE_ADD/SUB, EXTRACT, DATE_TRUNC,
EOMONTH and ADD_MONTHS propagate NULL arguments. IN_PERIOD retains its existing
FALSE result for a NULL date. Invalid non-NULL dates still raise errors.

TIMESTAMP and FROM_TIMESTAMP preserve native integer seconds above 2^53.
Nonfinite or out-of-range numeric seconds raise errors before conversion.
FROM_TIMESTAMP also checks the representable range of Go's time.Time, including
nanosecond rounding carry. Numeric strings retain floating-point coercion.

DATE_ADD/SUB reject nonfinite intervals, integer conversion overflow and clock
spans that exceed time.Duration, including overflow in combined ISO-duration
components. Extreme calendar shifts also check the normalized date against
time.Time's range before returning it. Ordinary calendar shifts still use
AddDate and clock shifts use fixed durations. Existing truncation of fractional
unit counts is retained.

## Differences: boundaries and elapsed units

`DATEDIFF(unit, start, end)` counts crossed UTC calendar or clock boundaries.
`TIMESTAMPDIFF(unit, start, end)` counts complete elapsed units, truncated toward
zero. Both accept quoted units or bare unit names, including singular/plural
names and the DATEPART abbreviations. An unrecognized bare identifier remains a column reference. If a unit column
has the same name as a supported unit, qualify it or use an expression such as
`COALESCE(unit_column, 'DAY')` to disambiguate.

Both inputs are normalized to UTC before comparison. Equivalent instants with
different offsets produce zero. Component extraction, DATE_TRUNC and calendar
shifts continue to use the input location. A calendar DAY shift across spring
DST can therefore span 23 hours; TIMESTAMPDIFF(HOUR, ...) reports 23.

| Expression | Result |
| --- | --- |
| `DATEDIFF(MONTH, '2024-01-31', '2024-02-29')` | 1 month boundary |
| `TIMESTAMPDIFF(MONTH, '2024-01-31', '2024-02-29')` | 0 complete months |
| `DATEDIFF(DAY, '2024-02-28T23:59:59Z', '2024-02-29T00:00:00Z')` | 1 day boundary |
| `TIMESTAMPDIFF(DAY, '2024-02-28T23:59:59Z', '2024-02-29T00:00:00Z')` | 0 complete 24-hour days |

Supported units are YEAR, QUARTER, MONTH, WEEK, ISO_WEEK, DAY, HOUR, MINUTE,
SECOND, MILLISECOND, MICROSECOND and NANOSECOND. DATEDIFF weeks begin on Sunday;
ISO_WEEK begins on Monday. TIMESTAMPDIFF weeks are complete seven-day durations.
A complete month requires reaching the original day and time of day; no
month-end clamping is applied. Years and quarters use complete months divided
by twelve and three. Swapping operands negates the result.

Migration: DATEDIFF formerly divided elapsed time by fixed durations, including
approximations of 30 days per month and 365 days per year. Those approximations
are removed. Use TIMESTAMPDIFF for the former complete-duration interpretation
of seconds through weeks. Months and years now have explicit calendar meaning.
Integer arithmetic preserves subsecond precision and supports spans beyond
Go's time.Duration range; an unrepresentable integer result raises an error.
NULL propagation and argument error order are shared by raw and ordinary calls.

## Calendar helpers

Dates use the proleptic Gregorian calendar: its leap-year rules apply even to
historical dates, without a Julian/Gregorian cutover. ISO week dates are
available through STRFTIME, WEEKOFYEAR and DATEPART. JULIANDAY produces an
astronomical day number; it does not switch to the Julian calendar.
Explicit [calendar conversion functions](calendar-conversion.md) support the
Julian, Discordian and Islamic civil calendars, ISO week dates and ordinal
dates without changing the interpretation of ordinary date functions.

| Function | Result |
| --- | --- |
| `MAKE_DATE(year, month, day)` / `DATEFROMPARTS(...)` | Native time.Time at UTC midnight; integer components, years 1–9999; rejects invalid dates instead of normalizing them. |
| `LAST_DAY(date)` | Native time.Time at local midnight on the last day of the input month, preserving its location. EOMONTH also accepts a month offset. |
| `DAYNAME(date)` | English weekday name, such as `Thursday`, from the input's local date. |
| `MONTHNAME(date)` | English month name, such as `February`, from the input's local date. |
| `DATEPART(part, date)` | Integer component, with bare or quoted part names. |
| `ISODOW(date)` | ISO weekday: Monday=1 through Sunday=7, using the input location. |

DATEPART supports YEAR, QUARTER, MONTH, DAYOFYEAR, DAY, WEEK, WEEKDAY, HOUR,
MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, TZOFFSET, ISO_WEEK and
ISO_YEAR, ISO_WEEKDAY (also ISODOW), plus the common [T-SQL abbreviations](https://learn.microsoft.com/en-us/sql/t-sql/functions/datepart-transact-sql).
WEEK uses Sunday-first weeks with January 1 in week 1; WEEKDAY is Sunday=1
through Saturday=7. ISO_WEEKDAY and EXTRACT('ISODOW', date) use Monday=1
through Sunday=7, so a Friday filter is `WHERE ISODOW(date_column) = 5`. ISO_WEEK and ISO_YEAR use ISO week numbering. There is no
SET DATEFIRST setting. Nanoseconds retain Go's full nine-digit precision.
All of these helpers propagate NULL and share validation between raw and
ordinary evaluation, including argument evaluation order.

## CONVERT and TRY_CONVERT

`CONVERT(type, value [, style])` and `TRY_CONVERT(type, value [, style])` use
type-first syntax. TRY_CONVERT returns NULL for an invalid value; expression
errors, invalid target declarations and unsupported styles still raise errors.
Neither function suppresses a division-by-zero error in its value expression.
A NULL input or explicitly NULL style returns NULL.

Supported targets are INT/INTEGER/BIGINT, FLOAT/REAL/DOUBLE/NUMERIC,
BOOL/BOOLEAN, TEXT/STRING/CHAR/VARCHAR/NCHAR/NVARCHAR, GEOMETRY/GEOM,
DATE, TIME, DATETIME/DATETIME2/TIMESTAMP. NUMERIC follows the existing floating
CAST semantics. Native integers preserve their exact value; integer overflow
and nonfinite numeric conversions are rejected. Invalid integer text in
TRY_CONVERT is checked without constructing and discarding an error object.
Boolean text accepts true/false, t/f, yes/no and 1/0, ignoring case and outer
whitespace; other text is an invalid boolean.

DATE returns time.Time at midnight in the input's location; DATETIME returns
time.Time with its clock fields; TIME returns `HH:mm:ss` with any fractional
seconds. Text input without a style uses the existing date parser. Character
targets may specify a length of 1–8000 Unicode characters; excess characters
are truncated without splitting UTF-8, and CHAR/NCHAR pad shorter values.
Without a length, text is returned without truncation or padding.

These [CONVERT date style numbers](https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql)
select a single parser for text-to-date conversion, or a formatter for native
time.Time-to-text conversion:

| Style | Format |
| --- | --- |
| 23 | `yyyy-mm-dd` |
| 101, 102, 103, 104, 105 | `mm/dd/yyyy`, `yyyy.mm.dd`, `dd/mm/yyyy`, `dd.mm.yyyy`, `dd-mm-yyyy` |
| 110, 111, 112 | `mm-dd-yyyy`, `yyyy/mm/dd`, `yyyymmdd` |
| 8 / 24 / 108 | `HH:mm:ss` |
| 20 / 120 | `yyyy-mm-dd HH:mm:ss` |
| 21 / 25 / 121 | SQL timestamp; output has three fractional digits. |
| 126 | ISO timestamp with `T`; output has up to three fractional digits. |
| 127 | RFC3339 timestamp with offset and up to nine fractional digits. |

This is an explicit subset, not full SQL Server conversion compatibility:
two-digit years, alternate calendars, locale/session settings, binary styles,
decimal precision declarations and other style numbers are unsupported.
Numeric targets do not accept date styles. The existing CAST behavior remains
unchanged.

```sql
SELECT DATEPART(iso_year, '2021-01-01');              -- 2020
SELECT DATE(MAKE_DATE(2024, 2, 29));                 -- 2024-02-29
SELECT DATE(TRY_CONVERT(DATE, '29.02.2024', 104));    -- 2024-02-29
SELECT TRY_CONVERT(INT, 'not a number');             -- NULL
SELECT CONVERT(VARCHAR(8), MAKE_DATE(2024, 2, 29), 112); -- 20240229
```

## Measurements

Local medians on Apple M2 Max, Go 1.27.2, darwin/arm64, three 300 ms samples:

| Kernel | Before → after | Bytes/op before → after | Allocations/op before → after |
| --- | ---: | ---: | ---: |
| Parse `09/08/2026` | 282 → 57 ns | 448 → 0 | 12 → 0 |
| Parse `08-Sep-2026` | 327 → 68 ns | 560 → 0 | 15 → 0 |
| Parse `12:34:56.123456789` | 329 → 21 ns | 640 → 0 | 15 → 0 |
| Parse `2026-09-08 12:34` | 82 → 39 ns | 0 → 0 | 0 → 0 |
| Format `%Y-%m-%d` | 205 → 66 ns | 80 → 16 | 5 → 1 |
| Format `%Y-%m-%d %H:%M:%S` | 409 → 96 ns | 144 → 24 | 5 → 1 |
| Format `%Y-%j-%W-%w` | 418 → 79 ns | 144 → 16 | 10 → 1 |

Parser baselines used the kernels from `84ad3a4` through a Go source overlay,
retaining the same tests and other working-tree changes. Formatter benchmarks
retain the previous implementation as a same-process control. Inputs are
prepared outside the timed section. These measure individual kernels, not
full-query speedups; unchanged ISO timestamp paths are excluded from the claims.
Host load affects timings, including comparisons between separate runs.
The SQL benchmark below also exercises filters and grouping over 10,000 rows.

The new raw TRY_CONVERT integer benchmark measured 0 B/op and 0 allocations/op
for a small valid integer, malformed text and an overflowing integer. Local
samples were roughly 45–79 ns/op. This measures expression evaluation, not a
complete query; there is no earlier CONVERT implementation to compare against.

```sh
go test ./internal/engine ./internal/driver -run 'Test(Date|Time|FromTime|EOMonthCalendar|Strftime|RawClock|RawDateTime|SQLPreparedRawClock|ApplyDate|ISODuration|ParseISO)' -count=1
go test -race ./internal/engine ./internal/driver -run 'Test(Date|Time|FromTime|EOMonthCalendar|Strftime|RawClock|RawDateTime|SQLPreparedRawClock|ApplyDate|ISODuration|ParseISO)' -count=1
go test ./internal/engine -run '^$' -bench '^(BenchmarkDateTimeParserLayouts|BenchmarkTimeValueParserLayouts|BenchmarkStrftimeFormat|BenchmarkDateTimeQueries|BenchmarkTimestampNumericConversion)$' -benchmem -benchtime=300ms -count=3
go test ./internal/engine -run '^$' -fuzz '^FuzzDateTimeParserLayouts$' -fuzztime=10s -parallel=2
go test ./internal/engine -run 'Test(CalendarFunctions|DatePart|Convert|ConversionInteger)' -count=1
go test ./internal/engine -run '^$' -bench '^BenchmarkTryConvertInteger$' -benchmem
go test ./internal/engine -run '^$' -fuzz '^FuzzConversionInteger$' -fuzztime=10s -parallel=2
```
