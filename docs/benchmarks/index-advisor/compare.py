#!/usr/bin/env python3
"""Compare observation/indexed queries against plain scans, using paired rounds.
Usage: python3 compare.py RAW_FILE
"""
import importlib.util
import statistics
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("paired_compare", Path(__file__).resolve().parents[1]/"columnar"/"compare.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
samples = module.read(sys.argv[1])
plain = samples["BenchmarkIndexAdvisor/plain"]
names = ["BenchmarkIndexAdvisor/observe", "BenchmarkIndexAdvisor/indexed"]
module.compare({n:plain for n in names}, {n:samples[n] for n in names})
build = samples["BenchmarkIndexAdvisorBuild"]
if len(build) != len(plain) or len(build) < 6:
    raise ValueError("incomplete build samples")
med = [statistics.median(v[i] for v in build) for i in range(3)]
print(f"\nIndex build median: {med[0]/1e6:.3f} ms, {med[1]:.0f} B/op, {med[2]:.0f} allocs/op.")
