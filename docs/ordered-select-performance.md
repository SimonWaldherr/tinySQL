# Ordered SELECT performance

Single-table `ORDER BY` over stored columns has two unboxed paths. Both return
the same rows, in the same order, with the same errors as the generic ordered
path. Any input outside their rules still takes that generic path.

## One column of one stored type

`ORDER BY col [DESC] [NULLS FIRST | NULLS LAST] [LIMIT n [OFFSET m]]` qualifies
when the non-NULL values in the candidate set all share one of these Go types:
`int`, `int64`, `float64` without NaN, or `string`. Candidates come from a table
scan or a secondary-index seek, and `WHERE` filters them as before.

- A bounded page keeps a heap of the best `LIMIT + OFFSET` candidates. Each
  heap entry holds the typed key and the scan position. A new candidate that
  sorts first replaces the root with a bottom-up sift: the hole follows the
  worse child with one comparison per level, then the new entry rises from the
  leaf it reaches. A new best candidate usually stays at that leaf, so this
  takes about half the comparisons of a standard sift-down. The "newest N rows"
  shape, `ORDER BY id DESC LIMIT 20` over ascending ids, is the worst case for
  the heap because every row displaces the root.
- An unbounded page, or one that keeps nearly every candidate, sorts the typed
  candidates once.
- NULL placement is unchanged: NULLs come first only under `NULLS FIRST`, and
  last in both directions otherwise. Ties keep scan order whatever the
  direction.
- Mixed Go types in one column, NaN values and any other type decline to the
  generic path (`typedOrderKeyKind`).

## Several columns, unbounded or nearly complete pages

`ORDER BY a, b DESC, ...` over direct column references encodes each candidate
into one byte string whose lexicographic order equals the generic comparison:

- A NULL key writes a marker: `00` under `NULLS FIRST`, `FF` otherwise. Markers
  ignore the direction.
- A value writes marker `80` followed by an order-preserving payload: eight
  bytes for an integer (sign bit flipped), eight bytes for a float (IEEE bits
  with the sign handled, and negative zero normalized to positive zero because
  the generic comparison treats them as equal), or a self-terminating string in
  which each zero byte is escaped as `00 FF` and the string ends with `00 01`.
- `DESC` inverts the payload bytes, which reverses their order.
- The scan position closes the key, so ties keep scan order.

Sorting then compares bytes. Every column must hold one value family: integers,
floats or strings. NaN, mixed families and other types decline.

"Bounded" here means a page that keeps at most 90% of the candidates. Those
pages keep the generic heap. Encoding every candidate costs more than the heap
comparisons it saves when only a few candidates are retained. Encoding all rows
for `LIMIT 20` made that query about 1.7 times slower during development, so
those pages decline to the generic path.

## Measurements

Apple M2 Max, darwin/arm64, Go 1.27.4, 12 CPUs. The baseline is `f481ccc`,
built from a clean worktree. Each row is the median of six 300 ms samples from
two interleaved passes of `go test -bench ... -benchmem -count=3`, on the
20,000-row fixtures in the engine's benchmark files. Run-to-run noise on this
host is about ±15%.

| Workload | Benchmark | Before | After | Ratio |
| --- | --- | ---: | ---: | ---: |
| `ORDER BY id DESC LIMIT 20`, ids ascending | `OrderIntKeyLatestLimit` | 1.211 ms | 0.489 ms | 0.40 |
| `ORDER BY age` full sort, INT | `OrderIntKeyFullSort` | 4.959 ms | 3.931 ms | 0.79 |
| `ORDER BY name LIMIT 20`, TEXT | `OrderTextKeyLimit` | 0.268 ms | 0.231 ms | 0.86 |
| `ORDER BY name` full sort, TEXT | `OrderTextKeyFullSort` | 5.85 ms | 5.37 ms | 0.92 * |
| `ORDER BY age, name DESC` full sort | `OrderMultiKeyFullSort` | 6.856 ms | 4.750 ms | 0.69 |
| `ORDER BY grp, sub, val DESC` full sort | `OrderByMultiColumnNoLimit` | 8.963 ms | 5.245 ms | 0.59 |
| Multi-column page, `LIMIT 19000` | `RawOrderPages/Multi/Large` | 8.573 ms | 4.754 ms | 0.55 |
| Multi-column page, `OFFSET 19980` | `RawOrderPages/Multi/Deep` | 6.773 ms | 3.004 ms | 0.44 |
| INT page, `LIMIT 20` | `RawOrderPages/Integer/Small` | 0.225 ms | 0.162 ms | 0.72 |
| INT page, `LIMIT 19000` | `RawOrderPages/Integer/Large` | 4.578 ms | 3.546 ms | 0.77 |
| INT page, `OFFSET 19980` | `RawOrderPages/Integer/Deep` | 2.862 ms | 1.852 ms | 0.65 |
| Multi-column top-N, `LIMIT 20` | `RawOrderPages/Multi/Small` | 0.363 ms | 0.390 ms | 1.07 † |
| Multi-column top-N, `LIMIT 20 OFFSET 5` | `OrderByMultiColumnLimit` | 0.370 ms | 0.397 ms | 1.07 † |
| `ORDER BY val` float pages (unchanged path) | `RawOrderPages/Float/*` | | | 1.00–1.01 |
| Computed ORDER BY keys (generic path, unchanged) | `RawOrderValues/*` | | | 0.99–1.01 |
| `SELECT *` full scan, no ordering | `SelectStarFullScan` | 5.329 ms | 5.355 ms | 1.00 |

\* In the two-pass run, one pass for the new binary was about 17% slower than
its other pass, and the baseline was stable in both. An interleaved rerun with
four samples each measured 5.80–5.90 ms before and 5.31–5.44 ms after, so the
row shows that rerun.

† These bounded multi-column pages decline to the generic path, so they run the
same comparisons as before. They still measure about 7% slower, consistently in
both passes. A variant in which the new multi-column function returns at once
keeps the slowdown. That points to the extra code's placement in the binary
rather than work done per row, but this is an inference; I did not confirm it
further and did not find a cheap fix.

## Validation

- Differential tests compare each typed ordering with the generic path. The
  reference orders by a select alias over arithmetic or `LOWER`, which forces
  the generic route. The cases cover ascending and descending order, every NULL
  placement, `WHERE` filters, secondary-index seeks, and page bounds on both
  sides of the bounded and full-sort cutover.
- A byte-order test compares the encoded key order with the generic comparator
  on random key tuples, including signed zeros, infinities, embedded zero bytes
  and NULLs under all placements.
- A heap test compares bounded selection with a reference built from
  `compareOrderedValueNulls`.
- Mutation checks: inverting the direction rule, or removing the negative-zero
  normalization, makes these tests fail.
- The ordering tests also run under `go test -race`.

## Reproduce

```sh
go test ./internal/engine -run 'TypedOrder|TypedMultiOrder' -count=1
```

```sh
go test ./internal/engine -run '^$' \
  -bench '^Benchmark(OrderInt|OrderText|OrderMultiKey|OrderByMultiColumn|OrderBy|RawOrderPages)' \
  -benchmem -benchtime=300ms -count=3
```

## Limits

- Computed ORDER BY terms (arithmetic, `CASE`, `COALESCE`, or an alias for an
  expression), BOOL, BLOB and date or time keys still use the generic path.
- Bounded multi-column pages (keeping at most 90% of the candidates) still use
  the generic heap, and measure about 7% slower here; see the note under the
  table.
- Results that return many rows still pay for building one `Row` map per result
  row. This work does not change that cost.
