#!/usr/bin/env python3
"""Compare Go benchmark samples with paired bootstrap intervals (stdlib only).

Usage: python3 compare.py paired-before.txt after.txt > comparison.md
Inputs must contain one sample per benchmark per paired run, in run order.
Intervals describe timing variability on this machine, not other workloads.
"""
import math
import random
import re
import statistics
import sys
from pathlib import Path


def read(path):
    samples = {}
    for line in Path(path).read_text().splitlines():
        match = re.match(r"(Benchmark\S+)\s+\d+\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op", line)
        if match:
            name, *values = match.groups()
            samples.setdefault(name, []).append(tuple(map(float, values)))
    return samples


def compare(before, after):
    if before.keys() != after.keys():
        raise ValueError("benchmark sets differ")
    rng = random.Random(20260912)
    print("| Benchmark | Before µs | After µs | Time change (95% paired interval) | B/op before → after | Allocs/op before → after |")
    print("|---|---:|---:|---:|---:|---:|")
    for name, old in before.items():
        new = after[name]
        if len(old) != len(new) or len(old) < 6:
            raise ValueError(f"need >=6 paired samples for {name}")
        logs = [math.log(b[0]/a[0]) for a, b in zip(old, new)]
        boots = sorted(math.expm1(statistics.mean(rng.choices(logs, k=len(logs))))*100 for _ in range(10000))
        delta = math.expm1(statistics.mean(logs))*100
        a = [statistics.median(v[i] for v in old) for i in range(3)]
        b = [statistics.median(v[i] for v in new) for i in range(3)]
        print(f"| {name.removeprefix('BenchmarkColumnar/')} | {a[0]/1000:.2f} | {b[0]/1000:.2f} | {delta:+.1f}% [{boots[249]:+.1f}, {boots[9749]:+.1f}] | {a[1]:.0f} → {b[1]:.0f} | {a[2]:.0f} → {b[2]:.0f} |")


if __name__ == "__main__":
    compare(read(sys.argv[1]), read(sys.argv[2]))
