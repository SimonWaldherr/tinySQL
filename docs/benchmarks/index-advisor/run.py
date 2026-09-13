#!/usr/bin/env python3
"""Run advisor cases in alternating order, with setup outside query timing.
Usage: python3 run.py TEST_BINARY OUTPUT_DIRECTORY
"""
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

binary = str(Path(sys.argv[1]).resolve())
out = Path(sys.argv[2]); out.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, GOMAXPROCS="1", GOGC="100")
files = ["internal/engine/index_advisor.go", "internal/engine/index_advisor_test.go", "internal/engine/exec_ddl_table.go", "internal/engine/ast.go", "tinysql.go"]
meta = {
 "utc":datetime.now(timezone.utc).isoformat(),
 "platform":platform.platform(),
 "go":subprocess.check_output(["go","version"],text=True).strip(),
 "parent_commit":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
 "GOMAXPROCS":1,"GOGC":100,"benchtime":"300ms","paired_rounds":8,
 "binary_sha256":hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
 "source_sha256":{p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in files},
}
(out/"environment.json").write_text(json.dumps(meta,indent=2)+"\n")
(out/"raw.txt").write_text("")
cases = ["^BenchmarkIndexAdvisor$/^plain$", "^BenchmarkIndexAdvisor$/^observe$", "^BenchmarkIndexAdvisor$/^indexed$", "^BenchmarkIndexAdvisorBuild$"]
for i in range(8):
 for case in (cases if i%2==0 else list(reversed(cases))):
  with (out/"raw.txt").open("a") as log:
   subprocess.run([binary,"-test.run=^$","-test.bench="+case,"-test.benchmem","-test.benchtime=300ms","-test.count=1"],env=env,stdout=log,check=True)
 print(f"paired round {i+1}/8 complete",flush=True)
