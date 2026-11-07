# JSON and set performance

Go 1.27 uses the new implementation behind `encoding/json`, retaining the v1
API's compatibility semantics. `encoding/json/v2` and `encoding/json/jsontext`
provide streaming APIs with different default semantics. In particular, v2's
defaults differ for invalid UTF-8, duplicate object names, nil maps and slices,
field matching and `omitempty`. See the [Go 1.27 release notes](https://go.dev/doc/go1.27)
and the [JSON migration guide](https://go.dev/doc/jsonv2-migration).

The accepted [generic set proposal](https://github.com/golang/go/issues/69230)
uses a `map[E]struct{}` representation and is initially intended for
`golang.org/x/exp/collection/set`. Its API improves expressiveness, but does
not by itself establish a performance benefit over the existing map-backed
sets. tinySQL's `COUNT(DISTINCT)` additionally has type-specific and historic
cross-type equality rules, so its implementation remains specialized.

## Changes and compatibility

`storage.JSONMarshal` and the JSON disk encoder normalize common maps and
slices without copying containers that contain no changed values. They still
convert `big.Rat` and UUID values to strings and preserve the established
empty-container handling for typed nil maps and slices. For unknown value types,
the original eager normalization is used: user-defined JSON marshaling methods
may mutate input while encoding, and the eager path retains the previous
container isolation. Callers must not concurrently mutate the values being
encoded.

Disk JSON writes classify the complete row set before encoding. If a custom
marshaler occurs in a row, or a column default is a structured or unknown value,
all row maps and generic slices use eager normalization. Structured defaults
are left to the JSON encoder, including its cycle detection. This preserves
isolation when one marshaler
mutates a different row. Ordinary scalar rows use a typed slice path without
boxing or copying the rows; conversions and typed nil containers still allocate
only where needed. Both compressed and encrypted writes use the same rules.

JSON result export uses `jsontext.Encoder` and `json/v2.MarshalEncode` for
simple rows, with `encoding/json.DefaultOptionsV1` to retain existing output
semantics, including deterministic map order and HTML escaping. Other rows are
marshaled independently before being written to the stream: a custom
`MarshalJSONTo` can observe the encoder's stack and must see the same context
as before. The `GOEXPERIMENT=nojsonv2` build retains the previous encoder.
The output stream may contain partial JSON when writing or marshaling fails.
Writer errors retain their original identity, including sentinel comparisons.
NDJSON uses its existing encoder.

## Measurements

Local measurements on an Apple M2 Max with Go 1.27.3, three 300 ms runs;
these are workload-specific comparisons using the same process and fixtures:

| Workload | Previous | Updated | Allocated bytes/op |
| --- | ---: | ---: | ---: |
| Normalize and marshal 1,000 ordinary nested records | 849–885 µs | 567–580 µs | 967–975 KB → 104–106 KB |
| Same, with rational values in 10% of records | 869–924 µs | 597–612 µs | 968–975 KB → 168–170 KB |
| Same, with time values in 10% of records (eager fallback) | 860–875 µs | 857–881 µs | 970–975 KB → 970–978 KB |
| Normalize 1,000 disk rows, three scalar columns | 59–65 µs | 15.1–15.3 µs | 121 KB → 0 bytes |
| Export 1,000 rows, compact JSON | 898–1,005 µs | 873–906 µs | 217 KB → 161 KB |
| Export 1,000 rows, indented JSON | 1,157–1,168 µs | 941–1,047 µs | 326 KB → 161 KB |

The compact timing ranges overlap; the allocation reduction is clearer. The
export benchmark writes to `io.Discard` and does not measure network or disk
latency. The disk-row normalization measurement excludes encoding, compression,
encryption and file I/O. Its allocation count falls from 3,001 to zero for that
scalar fixture; rows requiring conversions or custom marshalers still allocate.

```sh
go test ./internal/storage -run '^$' -bench '^BenchmarkJSONMarshalNormalization$' -benchmem -benchtime=300ms -count=3
go test ./internal/storage -run '^$' -bench '^BenchmarkJSONDiskRowNormalization$' -benchmem -benchtime=300ms -count=3
go test ./internal/exporter -run '^$' -bench '^BenchmarkJSONExportEncoding$' -benchmem -benchtime=300ms -count=3
GOEXPERIMENT=nojsonv2 go test ./internal/storage ./internal/exporter
```
