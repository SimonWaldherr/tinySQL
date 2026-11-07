# SELECT, conversion filters and exact vector ranking

Baseline: `e831509`. These changes bind constant conversion declarations outside
the row loop and prepare constant JSON vector arguments once per execution.
They optimize existing SELECT/WHERE/BETWEEN behavior without adding SQL syntax
or changing the public API.

## Research applied

| Area and primary source | Application in tinySQL |
| --- | --- |
| SELECT/WHERE: [DuckDB's expression rewriter](https://duckdb.org/2024/11/14/optimizers#expression-rewriter) specializes work that would otherwise repeat during execution. | Bind column positions and conversion declarations in immutable raw-plan evaluators. Use them for ordinary, streaming and DISTINCT projections, comparison filters and converted BETWEEN operands. |
| CAST/CONVERT: [DuckDB's casting documentation](https://duckdb.org/docs/1.5/sql/expressions/cast) distinguishes conversion failures, source/target rules and numeric rounding. | Share tinySQL's existing conversion implementations and diagnostics. Bind only static declarations; dynamic/invalid declarations, style parameters, statement-clock expressions and unsupported input expressions retain their prior evaluator. |
| BETWEEN: [SQLite's range optimization](https://www.sqlite.org/optoverview.html#the_between_optimization) separates planning constraints from single evaluation; [PostgreSQL documents inclusive endpoints and NULL comparisons](https://www.postgresql.org/docs/current/functions-comparison.html). | Keep a converted comparand as one BetweenExpr, evaluate it once and call the existing three-valued comparison helper. Do not rewrite it into two independently evaluated conversions. |
| Vector search: [pgvector explains selective filtering, exact search and ANN recall tradeoffs](https://github.com/pgvector/pgvector#filtering). | Retain metadata filtering/index candidates before exact distance scoring. Extend the unboxed Top-N scorer to JSON-string/array query literals and metric aliases, preparing the query once per execution after parameter binding. |
| Index use: [PostgreSQL's expression-index documentation](https://www.postgresql.org/docs/current/indexes-expressional.html) shows that transformed-column predicates need matching index semantics. | Do not strip CAST/CONVERT from indexed-column predicates: text ordering and numeric conversion can disagree. This change accelerates evaluation; it does not introduce expression indexes or new conversion-based seeks. |

## Compatibility boundaries

Conversions compile when their input is a column, literal, signed numeric
constant or another supported conversion. Targets and optional date styles must
be static; invalid styles retain runtime validation. Value/bound parameters are
read afresh, never frozen into a cached filter. Cached evaluators perform no
lazy writes. CAST and CONVERT continue to differ where they previously did;
for example, their accepted boolean strings and integer conversions are not
merged into one policy. TRY_CONVERT suppresses value-conversion failures while
retaining declaration and input-expression errors.

Compiled conversion predicates remain in the expression tier of WHERE. The
existing cheap-conjunct-first rule must still discard excluded invalid input
before evaluating a conversion. BETWEEN/NOT BETWEEN keep inclusive endpoints,
reversed-bound behavior, NULL/UNKNOWN, NaN and mixed-type rules. Bounds are
evaluated in the original order, even when the converted value is NULL.

Vector preparation supports literal native vectors, JSON strings and numeric
arrays. SQL's literal VEC_FROM_JSON constructor was already constant-folded.
The scorer also accepts the existing dot/inner-product, Euclidean and L1 aliases.
It uses the same distance kernels and stable tie rules. Incorrect arity falls
back to normal argument validation, fixing a case where the existing native
vector scorer could ignore extra arguments. Invalid query vectors do not fail
during preparation: row-time argument ordering and queries with no surviving
rows retain their behavior. Changing a bound query vector prepares a new scorer.

This work concerns exact ORDER BY distance LIMIT searches. It changes neither
HNSW/IVF search nor their recall settings. An outer WHERE on VEC_SEARCH's
already-ranked top-k result remains a post-filter; it is not silently moved
inside that table function. A scalar exact query with metadata WHERE evaluates
distance only on surviving rows, as shown below.

```sql
SELECT id, VEC_COSINE_DISTANCE(embedding, '[1,0]') AS distance
FROM ranked_vectors
WHERE id BETWEEN 1 AND 3
ORDER BY distance
LIMIT 2;
```

## Measurements

Apple M2 Max, Go 1.27.3, darwin/arm64, GOMAXPROCS=12. Medians of three serialized
300 ms samples using parsed statements and warm in-memory data. Measurements
exclude parsing, client transfer and durable I/O. Conversion fixtures have
20,000 rows including NULLs; TRY_CONVERT also sees malformed strings. Vector
fixtures contain 5,000 32-dimensional vectors, filter to 1,001 rows and return 20.

| Fixture | Before | After | Allocated bytes/op before → after |
| --- | ---: | ---: | ---: |
| SELECT CAST(input AS INT) | 3.18 ms | 2.95 ms | 7,033,160 → 7,033,158 |
| SELECT CONVERT(VARCHAR(4),input) | 3.76 ms | 3.20 ms | 7,223,317 → 7,223,316 |
| COUNT WHERE CAST(input AS INT) BETWEEN bounds | 1.16 ms | 0.526 ms | 151,614 → 151,075 |
| COUNT WHERE TRY_CONVERT(INT,dirty) BETWEEN bounds | 1.43 ms | 0.708 ms | 140,168 → 139,663 |
| COUNT WHERE CONVERT(INT,input) >= bound | 1.46 ms | 0.676 ms | 151,616 → 151,089 |
| Plain column BETWEEN control | 0.098 ms | 0.099 ms | 2,297 → 2,297 |
| Exact cosine ranking, JSON query literal | 1.69 ms | 0.075 ms | 278,760 → 14,088 |
| Exact Euclidean alias ranking, JSON query literal | 1.73 ms | 0.069 ms | 278,759 → 14,088 |

CAST projection timing ranges overlap; its samples establish no reliable latency
gain. Conversion projection allocation is essentially unchanged: output maps and
boxed values still dominate. Vector allocation counts fall from 2,091 to 90.
The already-folded VEC_FROM_JSON/L2 control retains 8,712 bytes and 69 allocations
per operation; its timing also varied between runs, so no additional improvement
is attributed to query preparation there. Results do not establish a general
speedup for arbitrary predicates or embedding dimensions, or peak live memory.

```sh
go test ./internal/engine -run '^$' \
  -bench '^BenchmarkSelect(Conversions|ConstantVector)$' -benchmem -benchtime=300ms -count=3
go test ./...
go test -race ./internal/engine ./internal/driver \
  -run 'Test.*(Compiled|ConstantVector|Convert|Cast|Between|RawFilter|Vector|Prepare|Parameter|Stream)' -count=1
make test-sql-compat
bash scripts/check-wasm.sh
go vet ./...
```

Differential coverage compares bound evaluators against the existing evaluator
for conversion types, diagnostics, overflow/precision, NaN/Inf, NULLs, Unicode
lengths, dates and dynamic bounds. Query tests cover DISTINCT, pooled streaming,
concurrent statement reuse, short-circuiting, stable vector ties, pagination,
metadata indexes, parameter changes and extra arguments. Full/minimal Browser
and Node WASM modules exercise conversion filters and vector ranking through
their public API.
