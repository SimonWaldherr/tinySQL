# Calendar conversion

Calendar conversions are explicit, date-only scalar functions. Ordinary DATE,
DATE_ADD, DATEPART and timestamp storage retain their Gregorian semantics.

| Function | Result |
| --- | --- |
| `TO_CALENDAR(date, calendar)` | Calendar date as text, using the Gregorian input's local year/month/day. |
| `FROM_CALENDAR(text, calendar)` | Gregorian native `time.Time` at UTC midnight. |
| `CALENDAR_CONVERT(text, source, target)` | Text converted directly between two calendar representations. |

TO_CALENDAR accepts the same Gregorian strings and native time values as the
existing date parser. It ignores clock fields without first shifting the date
to UTC. FROM_CALENDAR and CALENDAR_CONVERT accept the strict representations
below, allowing outer whitespace. Calendar identifiers and named Discordian
seasons are case-insensitive. Invalid dates and unsupported names raise errors;
NULL arguments propagate in both ordinary and raw evaluation. Arguments are
evaluated from left to right, stopping at NULL; arity is checked first.

| Calendar identifier | Canonical text | Example |
| --- | --- | --- |
| `GREGORIAN` | `yyyy-mm-dd` | `2024-02-29` |
| `JULIAN` | `yyyy-mm-dd` | `2024-02-16` |
| `DISCORDIAN` | `YOLD-Season-dd` or `YOLD-StTibs` | `3190-StTibs` |
| `ISLAMIC_CIVIL` / `ISLAMIC-CIVIL` | `yyyy-mm-dd` | `1445-08-19` |
| `ISO_WEEK` / `ISO_WEEK_DATE` | `yyyy-Www-d` | `2020-W53-5` |
| `ORDINAL` / `ISO_ORDINAL` | `yyyy-ddd` | `2024-060` |

Every result must correspond to a Gregorian date from 0001-01-01 through
9999-12-31. There are no BCE dates or year zero. Gregorian, Julian and Islamic
civil input years have exactly four digits; Discordian years have four or five.
Conversions across the supported Gregorian endpoints raise an error rather
than wrapping or producing a date that cannot be converted back. Islamic civil
output additionally requires a date on or after its epoch.

## Calendar rules

The Gregorian and Julian calendars are proleptic: their rules apply throughout
the supported range, without country-specific historical adoption gaps.
Julian years divisible by four are leap years, including 1900 and 2100; the
Gregorian century exceptions still apply to Gregorian input. For example,
Julian 1900-02-29 is Gregorian 1900-03-13. The
[US Naval Observatory](https://aa.usno.navy.mil/faq/leap_years) explains the
different leap rules. Conversion uses integer Julian day numbers internally;
these are day labels, not fractional astronomical JULIANDAY timestamps.

Discordian years equal Gregorian years plus 1166. The seasons, in order, are
Chaos, Discord, Confusion, Bureaucracy and Aftermath, each with 73 ordinary
days. `The Aftermath` is also accepted as input. St. Tib's Day is represented
separately as `YOLD-StTibs`; it consumes neither a season day nor an ordinary
weekday. This implementation places it on Gregorian February 29, including
Gregorian century rules, so 2000 has one and 1900 does not. This makes the
correspondence with Gregorian dates explicit even where variants differ.
The season and intercalary-day structure comes from the
[Principia Discordia](https://www.cs.cmu.edu/~tilt/principia/body.html);
the year offset is also used by the original
[ddate implementation](https://kernel.googlesource.com/pub/scm/utils/util-linux/util-linux/+/v2.18-rc2/misc-utils/ddate.c).
Canonical output uses English season names; weekday names and festival labels
are not included in the date representation.

Islamic civil is the arithmetic 30-year calendar with alternating 30/29-day
months, an additional day in month 12 of leap years, and a Friday epoch at
Julian day number 1948440 (Gregorian 0622-07-19). Leap years in each cycle are
2, 5, 7, 10, 13, 16, 18, 21, 24, 26 and 29. This is the
[ICU Islamic civil variant](https://unicode-org.github.io/icu-docs/apidoc/released/icu4j/com/ibm/icu/util/IslamicCalendar.CalculationType.html),
with cycle rules described by
[NASA's calendar reference](https://eclipse.gsfc.nasa.gov/SEhelp/calendars.html)
and epoch documented in the
[Fourmilab calendar converter](https://www.fourmilab.ch/documents/calendar/).
It does not model crescent observations, Umm al-Qura, or sunset boundaries.
The identifier `ISLAMIC` is deliberately rejected to avoid choosing a variant
implicitly.

ISO week dates use Monday=1 through Sunday=7, with week 1 containing January 4.
The week year can differ from the Gregorian year: `2020-W53-5` is 2021-01-01.
A nonexistent week 53 is rejected. Ordinal dates use the Gregorian year and a
day-of-year of 001–365 or 366. These are the week and ordinal representations
described by [ISO 8601](https://www.iso.org/standard/40874.html); ordinal dates
are distinct from Julian calendar dates and astronomical Julian day numbers.

## SQL examples

```sql
SELECT TO_CALENDAR('2024-02-29', 'JULIAN');
-- 2024-02-16

SELECT DATE(FROM_CALENDAR('1900-02-29', 'JULIAN'));
-- 1900-03-13

SELECT CALENDAR_CONVERT('3190-StTibs', 'DISCORDIAN', 'GREGORIAN');
-- 2024-02-29

SELECT CALENDAR_CONVERT('1445-09-01', 'ISLAMIC_CIVIL', 'ISO_WEEK');
-- 2024-W11-1

SELECT TO_CALENDAR('2024-02-29', 'ORDINAL');
-- 2024-060
```

## Validation

Tests include known Julian dates and century boundaries, every Discordian
season and St. Tib's Day, the complete 30-year Islamic leap cycle, and all
146,097 dates of a Gregorian 400-year cycle in every representation. They also
cover range endpoints, invalid inputs, NULL/arity/error parity between the two
evaluators, mixed-calendar SQL grouping, and repeated prepared statements.

```sh
go test ./internal/engine ./internal/driver -run 'Test.*(CalendarConversion|IslamicCivil)' -count=1
go test -race ./internal/engine ./internal/driver -run 'Test.*(CalendarConversion|IslamicCivil)' -count=1
go test ./internal/engine -run '^$' -fuzz '^FuzzCalendarDateParsing$' -fuzztime=10s -parallel=2
```
