| Benchmark | Before µs | After µs | Time change (95% paired interval) | B/op before → after | Allocs/op before → after |
|---|---:|---:|---:|---:|---:|
| BenchmarkIndexAdvisor/observe | 107.05 | 107.10 | -0.5% [-2.5, +1.2] | 1448 → 1616 | 7 → 12 |
| BenchmarkIndexAdvisor/indexed | 107.05 | 1.85 | -98.3% [-98.3, -98.2] | 1448 → 1256 | 7 → 19 |

Index build median: 4.003 ms, 2437336 B/op, 80790 allocs/op.
