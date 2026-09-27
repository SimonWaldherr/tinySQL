#!/usr/bin/env python3
"""tinySQL quickstart: parameters, batches, transactions and persistence."""

import pathlib
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import tinysql  # noqa: E402

print("tinySQL", tinysql.engine_version())

with tempfile.TemporaryDirectory() as directory:
    data = pathlib.Path(directory) / "shop"

    # A durable WAL database: every acknowledged write survives a restart.
    with tinysql.connect(data, mode="wal") as conn:
        conn.executescript(
            """
            CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price FLOAT, image BLOB);
            CREATE INDEX products_name ON products (name);
            """
        )
        conn.executemany(
            "INSERT INTO products VALUES (?, ?, ?, ?)",
            [(1, "Coffee", 4.5, None), (2, "Tea", 3.0, b"\x89PNG"), (3, "Cocoa", 3.8, None)],
        )
        with conn.transaction():
            conn.execute("UPDATE products SET price = price * ? WHERE name <> ?", (1.1, "Tea"))

    with tinysql.connect(data, mode="wal") as conn:
        conn.row_factory = tinysql.dict_row
        for product in conn.execute("SELECT id, name, price FROM products WHERE price > ? ORDER BY id", (3.5,)):
            print(product)
