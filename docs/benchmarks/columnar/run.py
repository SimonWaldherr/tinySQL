#!/usr/bin/env python3
"""Run precompiled before/after test binaries sequentially, alternating order.

python3 run.py /tmp/tinysql-columnar-before.test /tmp/tinysql-columnar-after.test OUTPUT_DIR
No builds or other test suites should run concurrently with measurements.
"""
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

before, after = map(lambda p: str(Path(p).resolve()), sys.argv[1:3])
out = Path(sys.argv[3])
out.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, GOMAXPROCS="1", GOGC="100")
metadata = {
    "utc": datetime.now(timezone.utc).isoformat(),
    "platform": platform.platform(),
    "go": subprocess.check_output(["go", "version"], text=True).strip(),
    "baseline_commit": "7f5bf487157fedab77f313595b8a1e1e5cf10796",
    "GOMAXPROCS": 1, "GOGC": 100, "benchtime": "300ms", "paired_rounds": 8,
    "batch_size": 256,
    "binary_sha256": {k: hashlib.sha256(Path(p).read_bytes()).hexdigest() for k, p in [("before", before), ("after", after)]},
    "harness_sha256": hashlib.sha256(Path("internal/engine/columnar_benchmark_test.go").read_bytes()).hexdigest(),
}
(out / "environment.json").write_text(json.dumps(metadata, indent=2)+"\n")
for name in ["paired-before.txt", "after.txt"]:
    (out / name).write_text("")
for iteration in range(8):
    pair = [(before, "paired-before.txt"), (after, "after.txt")]
    if iteration % 2:
        pair.reverse()
    for binary, name in pair:
        with (out / name).open("a") as log:
            subprocess.run([binary, "-test.run=^$", "-test.bench=^(BenchmarkColumnar|BenchmarkUpdateByPrimaryKey)$", "-test.benchmem", "-test.benchtime=300ms", "-test.count=1"], env=env, stdout=log, check=True)
    print(f"paired round {iteration+1}/8 complete", flush=True)
