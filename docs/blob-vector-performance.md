# BLOB and vector execution

## BLOBs

BLOB_LENGTH, BLOB_HEX, BLOB_SUBSTR, BLOB_CONCAT and BLOB_EQUAL use shared value
kernels in both expression evaluators. Raw queries pass values directly,
avoiding pooled Literal wrappers. Native BLOBs remain `[]byte`; constructors,
substrings and concatenations return independently owned bytes. NULL and empty
BLOBs remain distinct. Existing BLOB_CONCAT NULL-as-empty and BLOB_EQUAL
NULL/NULL equality behavior is retained.

Legacy string inputs keep their existing interpretation: surrounding whitespace
is trimmed, valid even-length HEX is decoded, and other text means UTF-8 bytes.
Read-only source views let LENGTH avoid decoding and SUBSTR copy only the
requested bytes. CONCAT classifies strings before allocating its final buffer,
including malformed HEX-like text. Small HEX results do not retain large
whitespace-padded inputs. Native equality uses bytes.Equal. URL-safe base64
decoding selects its alphabet before allocating the output.

SUBSTR checks integer conversion and negative lengths and clamps before adding
the length to its start. Large lengths cannot wrap into an invalid slice. The
same addition fix applies to VEC_SLICE; its output remains independent of the
input vector.

## Vectors

Flat numeric JSON arrays are parsed into one sized float64 slice without a
temporary byte copy or repeated slice growth. Other shapes retain encoding/json
validation and the former conversion diagnostics. JSON `null`, empty arrays and
null array elements retain their previous behavior. VEC_TO_JSON still uses
encoding/json.

VEC_TO_BYTES continues to return lowercase HEX text containing little-endian
float32 values. It writes that text directly. VEC_FROM_BYTES decodes HEX directly
into its float64 result; native binary input also yields an owned vector.
Float32 rounding, signed zero, NaN/Inf and malformed-input errors are covered
by compatibility tests. Native float64 slices are borrowed for reading, and
functions that transform vectors allocate their own results.

Norm, normalization and cosine use the existing SIMD kernels for ordinary
embeddings, with scaled fallback calculations for extreme finite magnitudes.
This avoids overflowed squared norms and rounded subnormal denominators.
Cached search norms use the same rules in flat, IVF and HNSW searches. Scalar
cosine uses one square root and normalization multiplies by its reciprocal;
their ordinary results may differ from the prior arithmetic by a few ULPs.
Nonfinite inputs retain NaN/Inf behavior, and NaN search distances are excluded.
Quantization also handles finite opposite endpoints whose difference overflows.
Centroid accumulation no longer retains an intermediate slice of input vectors.

VEC_SEARCH now reads its metric with either five or six arguments. Previously,
an explicit index argument caused the fifth argument to be ignored and cosine
to be used even when another metric was requested. Parallel top-k merge storage
is bounded by the source row count, so a very large k does not reserve k entries
over a small table. Metric aliases use the same raw kernels as their main names.

## Measurements

Local kernel measurements on Apple M2 Max, Go 1.27.2, darwin/arm64. Fixtures are
outside the timed section. Benchmarks retain the previous implementations as
same-process controls; these are development measurements, not clean-release
or full-query comparisons. Timing varied with concurrent host load.

| Workload | Bytes/op before → after | Allocations/op before → after |
| --- | ---: | ---: |
| Parse JSON vector, 768 dimensions | ~31,394 → 6,144 | 13 → 1 |
| Encode HEX vector, 768 dimensions | ~9,275 → 6,144 | 3 → 1 |
| Decode HEX vector, 768 dimensions | 9,216 → 6,144 | 2 → 1 |
| Centroid, 64 vectors × 64 dimensions | 2,328 → 536 | 3 → 2 |
| BLOB_LENGTH, 32-KiB payload as HEX | 32,776 → 8 | 2 → 1 |
| BLOB_HEX, lowercase HEX for 32-KiB payload | 163,856 → 16 | 4 → 1 |
| BLOB_SUBSTR, 64 bytes from 32-KiB HEX | 32,856 → 88 | 3 → 2 |
| BLOB_CONCAT, two 32-KiB payloads as HEX | 131,096 → 65,560 | 4 → 2 |
| Decode URL-safe base64, 32-KiB payload | 81,944 → 40,984 | 3 → 2 |

Native 32-KiB BLOB equality measured roughly 9.90 → 0.628 µs with no allocations.
JSON parsing at 768 dimensions measured roughly 88 → 65 µs. Stability checks
add about 1–2 ns to small norm/cosine kernels; no general cosine or normalization
speedup is claimed.

HEX-text concatenation trades extra validation for bounded output allocation:
the final measured sample took roughly 58.0 → 72.0 µs while halving allocated
bytes. Native BLOB concatenation kept its allocation profile and measured
roughly 2.65 → 2.66 µs. Malformed HEX-like text is classified before allocation,
so it cannot cause a large output buffer to be discarded and rebuilt.

```sh
go test ./internal/engine ./internal/driver ./internal/engine/search -run 'Test.*(Blob|BLOB|Vec|Vector)' -count=1
go test -race ./internal/engine ./internal/driver ./internal/engine/search -run 'Test.*(Blob|BLOB|Vec|Vector)' -count=1
go test ./internal/engine -run '^$' -bench '^(BenchmarkBlobKernels|BenchmarkRawBlobFunctions|BenchmarkVectorCodecs|BenchmarkVectorCentroidAccumulation)$' -benchmem -benchtime=300ms -count=3
go test ./internal/engine/search -run '^$' -bench '^BenchmarkVectorNumeric$' -benchmem -benchtime=300ms -count=3
go test ./internal/engine -run '^$' -fuzz '^FuzzVectorJSONCompatibility$' -fuzztime=10s -parallel=2
```
