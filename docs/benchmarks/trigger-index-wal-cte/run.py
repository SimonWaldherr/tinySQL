#!/usr/bin/env python3
"""Run prebuilt engine/storage binaries, without concurrent builds or tests.
Usage: python3 run.py BINARY_DIRECTORY OUTPUT_DIRECTORY
Binaries: engine-before.test, engine-after.test, storage-before.test, storage-after.test.
"""
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

binary_dir, out = map(Path, sys.argv[1:3])
out.mkdir(parents=True, exist_ok=True)
specs = {
 "engine": "BenchmarkTriggerBoundedInsert|BenchmarkRecursiveCTEChain|BenchmarkCTEFilteredProjection|BenchmarkCTESingleReference|BenchmarkWALUpdateIndexed",
 "storage": "BenchmarkRangeSeekScratch|BenchmarkAdvancedWALCalculateChecksum|BenchmarkAdvancedWALCalculateChecksumBlob|BenchmarkAdvancedWALLogInsert",
}
env = dict(os.environ, GOMAXPROCS="1", GOGC="100")
files = ["internal/engine/triggers.go", "internal/engine/exec_cte.go", "internal/storage/secondary_index_range.go", "internal/storage/wal_advanced.go", "internal/storage/range_seek_benchmark_test.go", "internal/storage/wal_advanced_benchmark_test.go", "internal/engine/trigger_insert_reuse_test.go", "internal/engine/cte_benchmark_test.go", "internal/engine/wal_dml_benchmark_test.go"]
metadata = {
 "utc": datetime.now(timezone.utc).isoformat(),
 "platform": platform.platform(),
 "go": subprocess.check_output(["go","version"],text=True).strip(),
 "baseline_commit": "1ae111c84628ec6e15cc3b66054a8d80ac21ff8f",
 "GOMAXPROCS": 1, "GOGC": 100, "benchtime": "300ms", "paired_rounds": 8,
 "source_sha256": {f:hashlib.sha256(Path(f).read_bytes()).hexdigest() for f in files},
 "binary_sha256": {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in binary_dir.glob("*-*.test")},
}
(out/"environment.json").write_text(json.dumps(metadata,indent=2)+"\n")
for package in specs:
 for version in ["before","after"]:
  (out/(package+"-"+version+".txt")).write_text("")
for i in range(8):
 for package, bench in specs.items():
  for version in (["before","after"] if i%2==0 else ["after","before"]):
   with (out/(package+"-"+version+".txt")).open("a") as log:
    subprocess.run([str((binary_dir/(package+"-"+version+".test")).resolve()),"-test.run=^$","-test.bench=^("+bench+")$","-test.benchmem","-test.benchtime=300ms","-test.count=1"],env=env,stdout=log,check=True)
 print(f"paired round {i+1}/8 complete",flush=True)
