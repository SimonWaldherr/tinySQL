These five workloads have changed SUM semantics (0 → NULL); their timings are not equivalent-result comparisons.

| Benchmark | Before µs | After µs | Time change (95% paired interval) | B/op before → after | Allocs/op before → after |
|---|---:|---:|---:|---:|---:|
| width4/filter0 | 36325.08 | 545.50 | -98.5% [-98.5, -98.5] | 45614056 → 4608 | 262166 → 33 |
| width4/group_many | 69573.86 | 71905.99 | +2.8% [-0.5, +5.9] | 105552998 → 107652830 | 1304796 → 1304797 |
| width32/filter0 | 276609.47 | 1311.79 | -99.5% [-99.6, -99.5] | 650646360 → 8992 | 524343 → 89 |
| width32/group_many | 76462.33 | 75948.07 | +1.2% [-1.7, +4.8] | 105559088 → 107657072 | 1304851 → 1304851 |
| small0 | 1.28 | 1.79 | +41.4% [+37.4, +45.8] | 952 → 2365 | 21 → 33 |
