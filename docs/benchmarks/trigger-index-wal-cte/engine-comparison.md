| Benchmark | Before µs | After µs | Time change (95% paired interval) | B/op before → after | Allocs/op before → after |
|---|---:|---:|---:|---:|---:|
| BenchmarkCTESingleReference | 562.25 | 560.19 | +0.2% [-1.1, +1.4] | 80272 → 80272 | 808 → 808 |
| BenchmarkCTEFilteredProjection | 3811.65 | 3759.53 | -1.7% [-3.1, -0.4] | 7000611 → 7000612 | 40215 → 40215 |
| BenchmarkRecursiveCTEChain | 518.05 | 500.67 | -5.2% [-7.7, -3.1] | 1006345 → 1002521 | 10522 → 10029 |
| BenchmarkTriggerBoundedInsert | 42.49 | 41.51 | -2.5% [-3.2, -1.8] | 14833 → 13249 | 310 → 210 |
| BenchmarkWALUpdateIndexed | 3.02 | 3.00 | +0.1% [-1.7, +2.7] | 3672 → 3672 | 54 → 54 |
