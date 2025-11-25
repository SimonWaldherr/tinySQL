#!/usr/bin/env python3
"""Minimal ctypes demo for the tinySQL cgo bridge."""

from __future__ import annotations

import ctypes
import json
import pathlib
import sys
from typing import Any, Dict

LIB_PATH = pathlib.Path(__file__).with_name("libtinysql.so")
if not LIB_PATH.exists():
    sys.stderr.write(
        "libtinysql.so not found next to example.py. Build it with:\n"
        "    go build -buildmode=c-shared -o bindings/python/libtinysql.so ./bindings/python\n"
    )
    sys.exit(1)

lib = ctypes.CDLL(str(LIB_PATH))
lib.TinySQLExec.argtypes = [ctypes.c_char_p]
lib.TinySQLExec.restype = ctypes.c_void_p
lib.TinySQLFree.argtypes = [ctypes.c_void_p]
lib.TinySQLReset.argtypes = []
lib.TinySQLReset.restype = None

def exec_sql(sql: str) -> Dict[str, Any]:
    """Execute SQL via the shared library and return the decoded JSON payload."""
    ptr = lib.TinySQLExec(sql.encode("utf-8"))
    if not ptr:
        raise RuntimeError("TinySQLExec returned NULL pointer")
    try:
        payload = ctypes.string_at(ptr).decode("utf-8")
        result = json.loads(payload)
        if result.get("status") != "ok":
            raise RuntimeError(result.get("error", "unknown tinySQL error"))
        return result
    finally:
        lib.TinySQLFree(ptr)


def main() -> None:
    lib.TinySQLReset()
    exec_sql("CREATE TABLE users (id INT, name TEXT);")
    exec_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');")
    exec_sql("UPDATE users SET name = 'Charlie' WHERE id = 3;")
    exec_sql("INSERT INTO users VALUES (4, 'Dave');")

    rows = exec_sql("SELECT * FROM users ORDER BY id;")
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()
