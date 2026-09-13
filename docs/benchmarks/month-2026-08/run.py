#!/usr/bin/env python3
"""Run precompiled before/after test binaries sequentially, alternating order.

python3 run.py BEFORE_BINARY AFTER_BINARY OUTPUT_DIR
No builds or other test suites should run concurrently with measurements.
"""
import hashlib
import json
import os
import re
import platform
import subprocess
import sys
import tempfile
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
    "baseline_commit": "185c8533d754d4337daa0ba90f2344918c62b988",
    "GOMAXPROCS": 1, "GOGC": 100, "benchtime": "300ms", "paired_rounds": 8,
    "current_commit": subprocess.check_output(["git", "rev-parse", "1ae111c"], text=True).strip(),
    "binary_sha256": {k: hashlib.sha256(Path(p).read_bytes()).hexdigest() for k, p in [("before", before), ("after", after)]},
    "harness_sha256": hashlib.sha256(Path(__file__).with_name("columnar_benchmark_test.go.txt").read_bytes()).hexdigest(),
}
(out / "environment.json").write_text(json.dumps(metadata, indent=2)+"\n")
# Compare complete result multisets before timing; sorting ignores unordered SQL output.
checks = []
result_sets = []
for binary, name in [(before, "before"), (after, "after")]:
    result_dir = tempfile.mkdtemp(prefix="tinysql-month-check-")
    result = subprocess.check_output([binary, "-test.run=^$", "-test.bench=^BenchmarkColumnar$", "-test.benchtime=1x"], env=dict(env, TINYSQL_MONTH_CHECK="1", TINYSQL_MONTH_RESULTS=result_dir), text=True)
    (out / (name + "-correctness.txt")).write_text(result)
    result_sets.append({p.stem: json.loads(p.read_text()) for p in Path(result_dir).glob("*.json")})
    checks.append(dict(re.findall(r"RESULT (\S+) ([0-9a-f]+)", result)))
expected = {"BenchmarkColumnar/width4/filter0": "col_0", "BenchmarkColumnar/width32/filter0": "col_0", "BenchmarkColumnar/small0": "col_0", "BenchmarkColumnar/width4/group_many": "col_1", "BenchmarkColumnar/width32/group_many": "col_1"}
different = {k for k in checks[0] if checks[0][k] != checks[1].get(k)}
if len(checks[0]) != 24 or checks[0].keys() != checks[1].keys() or different != expected.keys():
    raise RuntimeError("Unexpected query differences or incomplete coverage")
changes = {}
for name, col in expected.items():
    old, new = [results[name.replace("/", "_")] for results in result_sets]
    converted = []
    count = 0
    for raw in new["Rows"]:
        row = json.loads(raw)
        if row[col] is None:
            row[col] = 0
            count += 1
        converted.append(json.dumps(row, sort_keys=True, separators=(",", ":")))
    original = [json.dumps(json.loads(raw), sort_keys=True, separators=(",", ":")) for raw in old["Rows"]]
    if old["Cols"] != new["Cols"] or sorted(original) != sorted(converted):
        raise RuntimeError("Difference exceeds documented SUM(NULL/empty) change")
    changes[name] = {"column": col, "changed_rows": count, "before": 0, "after": None}
(out / "semantic-differences.json").write_text(json.dumps(changes, indent=2)+"\n")
print("19 exact result matches; 5 cases differ only in empty/all-NULL SUM (0 -> NULL)", flush=True)
for name in ["paired-before.txt", "after.txt"]:
    (out / name).write_text("")
for iteration in range(8):
    pair = [(before, "paired-before.txt"), (after, "after.txt")]
    if iteration % 2:
        pair.reverse()
    for binary, name in pair:
        with (out / name).open("a") as log:
            subprocess.run([binary, "-test.run=^$", "-test.bench=^BenchmarkColumnar$", "-test.benchmem", "-test.benchtime=300ms", "-test.count=1"], env=env, stdout=log, check=True)
    print(f"paired round {iteration+1}/8 complete", flush=True)
