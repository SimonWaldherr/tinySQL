# Predicate and string performance

The following paths avoid repeated per-row work without introducing a result
cache:

- `LIKE` and `NOT LIKE`: patterns containing `_` but no `%` or escapes compare
  literal segments directly and decode only wildcard positions. Wildcards still
  consume one Unicode rune, including a decoding error for an invalid byte.
- `IN` and `NOT IN`: constant lists use separate exact integer, floating-point,
  and string sets. Integer columns can probe floating SQL literals directly.
  Integer-to-integer equality retains all 64 bits; floating comparisons preserve
  the existing conversion semantics. Other values retain raw equality checks.
  Set construction costs time and memory proportional to the list length and is
  outside the lookup measurements below. Bound parameters remain dynamic.
- `BETWEEN` and `NOT BETWEEN`: homogeneous integers and non-NaN floats use direct
  comparisons. NULLs, mixed types, and NaNs retain the general comparison path.
- `LEFT` and `RIGHT`: decode only the requested edge, avoiding a full rune slice.
  Small results from strings larger than 4 KiB are copied when shorter than one
  quarter of the source so they do not retain large backing buffers. Partial
  invalid UTF-8 keeps the former rune-slice normalization behavior.
- `UPPER` and `LOWER`: raw evaluation avoids the function-call scratch pool and
  argument wrappers. Unicode conversion still uses the standard library.

## Correctness change

A nonmatching `IN` list containing NULL now produces UNKNOWN, as does its
`NOT IN` counterpart. For example, `2 NOT IN (1, NULL)` produces NULL rather
than TRUE. A matching element still wins: `1 IN (1, NULL)` is TRUE. This applies
to general, raw, and join expression evaluation; WHERE excludes UNKNOWN.

## Measurements

Apple M2 Max, darwin/arm64, Go 1.27.1. Medians of three isolated 300 ms runs,
comparing the parent implementation with this change. These are focused engine
microbenchmarks, not end-to-end query throughput claims.

| Benchmark | Before | After | Speedup |
| --- | ---: | ---: | ---: |
| LEFT, 4 of 30,000 Unicode runes | 220,544 ns | 70.08 ns | 3,147× |
| RIGHT, 4 of 30,000 Unicode runes | 146,833 ns | 82.54 ns | 1,779× |
| IN, integer miss in 1,000 float literals | 2,459 ns | 12.91 ns | 190× |
| NOT IN, same list | 2,460 ns | 12.94 ns | 190× |
| BETWEEN, homogeneous float values | 17.73 ns | 7.272 ns | 2.44× |
| UPPER, raw column expression | 103.1 ns | 95.30 ns | 1.08× |
| LIKE, `document____.json` | 51.92 ns | 18.95 ns | 2.74× |

LEFT/RIGHT allocations fall from 122,912 B / 3 allocations to 32 B /
2 allocations. IN, BETWEEN, and the LIKE matcher allocate nothing per lookup.
UPPER remains at 40 B / 2 allocations when the text changes case.

```sh
go test ./internal/engine -run '^$' \
  -bench 'Benchmark(ShortStringEdges|IntegerInFloatLiterals|BetweenNumeric|RawUpper|LikeFixedWidth)$' \
  -benchmem -count=3 -benchtime=300ms
```

Regression tests cover Unicode and invalid UTF-8, NULL truth tables across
execution paths, large integer precision, NaN, mixed list types, prepared IN
parameters, and SQL-level LIKE/NOT LIKE/IN/NOT IN/BETWEEN behavior.
