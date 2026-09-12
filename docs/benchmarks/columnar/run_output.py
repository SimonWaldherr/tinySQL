#!/usr/bin/env python3
"""Compare result APIs in fresh processes, alternating format order per round.

python3 run_output.py /tmp/tinysql-columnar-after.test OUTPUT_DIR
"""
import os
import subprocess
import sys
from pathlib import Path

binary = str(Path(sys.argv[1]).resolve())
out = Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, GOMAXPROCS="1", GOGC="100")
with (out / "output.txt").open("w") as log:
    for repeat in range(8):
        formats = ("maps", "columns") if repeat % 2 == 0 else ("columns", "maps")
        for query in ("all", "half", "sparse", "aggregate_fallback"):
            for format in formats:
                subprocess.run([binary, "-test.run=^$", f"-test.bench=^BenchmarkColumnarOutput$/^{query}$/^{format}$", "-test.benchmem", "-test.benchtime=300ms", "-test.count=1"], env=env, stdout=log, check=True)
        print(f"output round {repeat+1}/8 complete", flush=True)
