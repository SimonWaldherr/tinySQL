#!/usr/bin/env python3
"""Compare stage-1 and stage-2 binaries with the same extended benchmark suite.

python3 run.py BEFORE_TEST_BINARY AFTER_TEST_BINARY OUTPUT_DIR
Run only after builds and correctness tests finish; no concurrent profiling.
"""
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

before, after = (str(Path(p).resolve()) for p in sys.argv[1:3])
out = Path(sys.argv[3])
out.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, GOMAXPROCS="1", GOGC="100")
pattern = "^(BenchmarkColumnar|BenchmarkColumnarOutput|BenchmarkColumnarGrowth|BenchmarkUpdateByPrimaryKey)$"
metadata = {
    "utc": datetime.now(timezone.utc).isoformat(),
    "platform": platform.platform(),
    "go": subprocess.check_output(["go", "version"], text=True).strip(),
    "baseline": "Stage-1 implementation; reconstruct using restore-stage1.patch",
    "repository_parent": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "GOMAXPROCS": 1, "GOGC": 100, "benchtime": "300ms", "paired_rounds": 8,
    "benchmark_pattern": pattern,
    "binary_sha256": {k: hashlib.sha256(Path(p).read_bytes()).hexdigest() for k, p in [("before", before), ("after", after)]},
    "harness_sha256": {p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in ["internal/engine/columnar_benchmark_test.go", "internal/engine/columnar_test.go"]},
}
(out / "environment.json").write_text(json.dumps(metadata, indent=2)+"\n")
for name in ["before.txt", "after.txt"]:
    (out / name).write_text("")
for iteration in range(8):
    pair = [(before, "before.txt"), (after, "after.txt")]
    if iteration % 2:
        pair.reverse()
    for binary, name in pair:
        with (out / name).open("a") as log:
            subprocess.run([binary, "-test.run=^$", f"-test.bench={pattern}", "-test.benchmem", "-test.benchtime=300ms", "-test.count=1"], env=env, stdout=log, check=True)
    print(f"stage-2 paired round {iteration+1}/8 complete", flush=True)
