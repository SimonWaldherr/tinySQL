| Benchmark | Before µs | After µs | Time change (95% paired interval) | B/op before → after | Allocs/op before → after |
|---|---:|---:|---:|---:|---:|
| BenchmarkRangeSeekScratch/warmfalse/rows1 | 0.23 | 0.13 | -42.8% [-44.7, -40.7] | 216 → 128 | 8 → 1 |
| BenchmarkRangeSeekScratch/warmfalse/rows10 | 0.34 | 0.25 | -27.4% [-27.9, -26.8] | 216 → 128 | 8 → 1 |
| BenchmarkRangeSeekScratch/warmfalse/rows1000 | 18.67 | 18.58 | -0.2% [-0.9, +0.8] | 25176 → 25088 | 15 → 8 |
| BenchmarkRangeSeekScratch/warmtrue/rows1 | 0.31 | 0.19 | -36.2% [-39.2, -32.9] | 216 → 128 | 8 → 1 |
| BenchmarkRangeSeekScratch/warmtrue/rows10 | 0.41 | 0.31 | -25.7% [-28.5, -22.7] | 216 → 128 | 8 → 1 |
| BenchmarkRangeSeekScratch/warmtrue/rows1000 | 21.65 | 21.49 | -0.4% [-4.3, +4.2] | 25176 → 25088 | 15 → 8 |
| BenchmarkAdvancedWALCalculateChecksum | 0.32 | 0.22 | -29.9% [-31.8, -27.9] | 112 → 68 | 8 → 2 |
| BenchmarkAdvancedWALCalculateChecksumBlob | 5.75 | 5.61 | -3.6% [-5.1, -2.3] | 112 → 68 | 8 → 2 |
| BenchmarkAdvancedWALLogInsert | 4334.24 | 4434.91 | +7.1% [-3.1, +25.5] | 1208 → 1144 | 35 → 21 |
