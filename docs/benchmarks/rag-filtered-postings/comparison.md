| Benchmark | Before µs | After µs | Time change (95% paired interval) | B/op before → after | Allocs/op before → after |
|---|---:|---:|---:|---:|---:|
| BenchmarkRAGFTSFilteredBranch | 518.57 | 44.73 | -91.3% [-91.5, -91.0] | 59120 → 60576 | 323 → 338 |
| BenchmarkRAGFilteredPostingSelectivity/stride1/term0 | 468.60 | 73.12 | -84.3% [-84.5, -84.1] | 568 → 408 | 5 → 4 |
| BenchmarkRAGFilteredPostingSelectivity/stride1/term7_OR_term23_OR_term180_OR_needle42 | 1874.72 | 28.35 | -98.5% [-98.5, -98.5] | 568 → 2024 | 5 → 20 |
| BenchmarkRAGFilteredPostingSelectivity/stride4/term0 | 126.74 | 53.85 | -57.3% [-57.9, -56.8] | 568 → 408 | 5 → 4 |
| BenchmarkRAGFilteredPostingSelectivity/stride4/term7_OR_term23_OR_term180_OR_needle42 | 472.92 | 10.71 | -97.7% [-97.8, -97.7] | 568 → 2024 | 5 → 20 |
| BenchmarkRAGFilteredPostingSelectivity/stride100/term0 | 4.44 | 4.43 | -0.4% [-1.3, +0.7] | 568 → 568 | 5 → 5 |
| BenchmarkRAGFilteredPostingSelectivity/stride100/term7_OR_term23_OR_term180_OR_needle42 | 9.03 | 8.87 | -1.5% [-2.7, -0.5] | 568 → 568 | 5 → 5 |
| BenchmarkRAGFilteredPostingSelectivity/stride10000/term0 | 0.36 | 0.38 | +5.9% [+3.5, +8.7] | 568 → 568 | 5 → 5 |
| BenchmarkRAGFilteredPostingSelectivity/stride10000/term7_OR_term23_OR_term180_OR_needle42 | 0.39 | 0.39 | -0.3% [-1.4, +0.7] | 568 → 568 | 5 → 5 |
| BenchmarkRAGHybridSearch | 563.57 | 558.23 | +6.5% [-2.0, +22.9] | 41240 → 41240 | 184 → 184 |
| BenchmarkRAGHybridSearchPreFilterSelective | 693.61 | 193.57 | -72.0% [-72.5, -71.5] | 40624 → 42080 | 184 → 199 |
| BenchmarkRAGHybridSearchPreFilterWithExpansion | 688.34 | 197.90 | -74.2% [-79.8, -70.5] | 31240 → 32696 | 183 → 198 |
| BenchmarkRAGVectorOnly | 540.45 | 532.91 | -2.0% [-5.5, +0.9] | 21440 → 21440 | 129 → 129 |
| BenchmarkRAGFTSSelectiveTerm | 5.70 | 4.75 | -10.9% [-16.5, -4.0] | 8952 → 8952 | 62 → 62 |
| BenchmarkRAGFTSCommonTerm | 39.87 | 39.58 | +0.0% [-1.5, +1.1] | 24856 → 24856 | 139 → 139 |
| BenchmarkRAGFTSPhrase | 2853.14 | 2828.00 | -0.4% [-2.4, +1.7] | 24856 → 24856 | 139 → 139 |
