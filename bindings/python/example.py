#!/usr/bin/env python3
"""
Improved Python wrapper for tinySQL.
"""

from __future__ import annotations

import ctypes
import json
import pathlib
import sys
from typing import Any, Dict, List, Optional


class TinySQL:
    def __init__(self, lib_path: Optional[str] = None):
        if lib_path is None:
            lib_path = str(pathlib.Path(__file__).parent / "libtinysql.so")
        
        if not pathlib.Path(lib_path).exists():
            raise FileNotFoundError(f"libtinysql.so not found at {lib_path}. Please run 'make build' in the python bindings directory.")

        self.lib = ctypes.CDLL(lib_path)
        
        # Define argument and return types
        self.lib.TinySQLVersion.restype = ctypes.c_char_p
        
        self.lib.TinySQLExec.argtypes = [ctypes.c_char_p]
        self.lib.TinySQLExec.restype = ctypes.c_void_p
        
        self.lib.TinySQLSave.argtypes = [ctypes.c_char_p]
        self.lib.TinySQLSave.restype = ctypes.c_void_p
        
        self.lib.TinySQLLoad.argtypes = [ctypes.c_char_p]
        self.lib.TinySQLLoad.restype = ctypes.c_void_p
        
        self.lib.TinySQLFree.argtypes = [ctypes.c_void_p]
        
        self.lib.TinySQLReset.argtypes = []
        self.lib.TinySQLReset.restype = None

    def version(self) -> str:
        return self.lib.TinySQLVersion().decode("utf-8")

    def reset(self) -> None:
        self.lib.TinySQLReset()

    def _handle_response(self, ptr: int) -> Dict[str, Any]:
        if not ptr:
            raise RuntimeError("tinySQL returned a NULL pointer")
        try:
            payload = ctypes.string_at(ptr).decode("utf-8")
            result = json.loads(payload)
            if result.get("status") != "ok":
                raise RuntimeError(result.get("error", "unknown tinySQL error"))
            return result
        finally:
            self.lib.TinySQLFree(ptr)

    def execute(self, sql: str) -> Dict[str, Any]:
        ptr = self.lib.TinySQLExec(sql.encode("utf-8"))
        return self._handle_response(ptr)

    def save(self, path: str) -> None:
        ptr = self.lib.TinySQLSave(path.encode("utf-8"))
        self._handle_response(ptr)

    def load(self, path: str) -> None:
        ptr = self.lib.TinySQLLoad(path.encode("utf-8"))
        self._handle_response(ptr)


def main():
    try:
        db = TinySQL()
        print(f"tinySQL Version: {db.version()}")
        
        db.reset()
        
        print("Creating table and inserting data...")
        db.execute("CREATE TABLE users (id INT, name TEXT);")
        db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');")
        
        print("Querying data...")
        result = db.execute("SELECT * FROM users ORDER BY id;")
        for row in result.get("rows", []):
            print(f"  {row}")
            
        print("Saving database to 'test.db'...")
        db.save("test.db")
        
        print("Resetting database...")
        db.reset()
        
        print("Loading database from 'test.db'...")
        db.load("test.db")
        
        print("Querying data again...")
        result = db.execute("SELECT * FROM users ORDER BY id;")
        for row in result.get("rows", []):
            print(f"  {row}")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
