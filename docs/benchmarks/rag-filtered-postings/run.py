#!/usr/bin/env python3
"""Alternate precompiled RAG baseline/current binaries; no concurrent builds.
Usage: python3 run.py BEFORE AFTER OUTPUT_DIRECTORY
"""
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

before, after = [str(Path(p).resolve()) for p in sys.argv[1:3]]
out = Path(sys.argv[3]); out.mkdir(parents=True, exist_ok=True)
bench = "BenchmarkRAGHybridSearch|BenchmarkRAGHybridSearchPreFilterSelective|BenchmarkRAGHybridSearchPreFilterWithExpansion|BenchmarkRAGFTSFilteredBranch|BenchmarkRAGFTSSelectiveTerm|BenchmarkRAGFTSCommonTerm|BenchmarkRAGFTSPhrase|BenchmarkRAGVectorOnly|BenchmarkRAGFilteredPostingSelectivity"
env = dict(os.environ,GOMAXPROCS="1",GOGC="100")
source = ["internal/engine/fts_index.go","internal/engine/rag_prefilter.go","internal/engine/rag_workload_benchmark_test.go","internal/engine/rag_fts_plan_test.go","internal/engine/rag_posting_intersection_benchmark_test.go","internal/engine/triggers.go","internal/engine/exec_cte.go","internal/storage/secondary_index_range.go","internal/storage/wal_advanced.go"]
metadata = {
 "utc":datetime.now(timezone.utc).isoformat(), "platform":platform.platform(),
 "go":subprocess.check_output(["go","version"],text=True).strip(),
 "baseline":"1ae111c84628ec6e15cc3b66054a8d80ac21ff8f plus prior-optimizations.patch",
 "GOMAXPROCS":1,"GOGC":100,"benchtime":"300ms","paired_rounds":8,
 "binary_sha256":{n:hashlib.sha256(Path(p).read_bytes()).hexdigest() for n,p in [("before",before),("after",after)]},
 "source_sha256":{p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in source},
 "prior_patch_sha256":hashlib.sha256(Path(__file__).with_name("prior-optimizations.patch").read_bytes()).hexdigest(),
}
(out/"environment.json").write_text(json.dumps(metadata,indent=2)+"\n")
for n in ["before","after"]: (out/(n+".txt")).write_text("")
for i in range(8):
 pair=[("before",before),("after",after)]
 if i%2: pair.reverse()
 for name,binary in pair:
  with (out/(name+".txt")).open("a") as log:
   subprocess.run([binary,"-test.run=^$","-test.bench=^("+bench+")$","-test.benchmem","-test.benchtime=300ms","-test.count=1"],env=env,stdout=log,check=True)
 print(f"paired round {i+1}/8 complete",flush=True)
