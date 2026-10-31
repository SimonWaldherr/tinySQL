"""Measure Python-side batch assembly; excludes native decoding and SQL execution.

Run from any directory: python3 bindings/python/benchmarks/batch_memory.py
"""
import gc
import pathlib
import sys
import tracemalloc

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import tinysql


class Sink:
    def __init__(self):
        self.calls = 0
        self.max_payload = 0

    def call(self, name, handle, sql, payload):
        self.calls += 1
        self.max_payload = max(self.max_payload, len(payload.encode("utf-8")))
        return {"rowsAffected": 0, "inTransaction": False}


def measure(batch_size):
    sink = Sink()
    connection = tinysql.Connection(sink, 1, True)
    gc.collect()
    tracemalloc.start()
    connection.executemany("INSERT INTO events VALUES (?, ?)",
                           ((i, "x" * 128) for i in range(20000)), batch_size=batch_size)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    connection._handle = 0  # Sink owns no native handle.
    return peak, sink.calls, sink.max_payload


if __name__ == "__main__":
    print("20,000 rows; integer + 128 ASCII characters; Python heap only")
    for size in (None, 1000):
        samples = [measure(size) for _ in range(3)]
        print(f"batch_size={size}: median peak={sorted(s[0] for s in samples)[1]:,} bytes; "
              f"calls={samples[0][1]}; largest JSON payload={samples[0][2]:,} bytes")
