"""Integration tests for the tinySQL Python package against the real library."""

from __future__ import annotations

import datetime
import decimal
import doctest
import gzip
import json
import math
import os
import pathlib
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import tinysql  # noqa: E402


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(tinysql))
    return tests


class ConnectionTests(unittest.TestCase):
    def setUp(self):
        self.conn = tinysql.connect()
        self.addCleanup(self.conn.close)

    def test_module_attributes(self):
        self.assertEqual(tinysql.apilevel, "2.0")
        self.assertEqual(tinysql.paramstyle, "qmark")
        self.assertIn(tinysql.threadsafety, (0, 1, 2, 3))
        self.assertTrue(tinysql.engine_version())
        self.assertTrue(issubclass(tinysql.IntegrityError, tinysql.DatabaseError))
        self.assertTrue(issubclass(tinysql.DatabaseError, tinysql.Error))
        self.assertEqual("INTEGER", tinysql.NUMBER)

    def test_types_round_trip(self):
        self.conn.execute(
            "CREATE TABLE v (i INT, f FLOAT, whole FLOAT, s TEXT, b BOOL, raw BLOB, empty BLOB, n TEXT)"
        )
        values = (2 ** 63 - 1, 0.1, 3.0, "Grüße ' 🦊 \x00 ;", True, b"\x00\xff\x10", b"", None)
        self.conn.execute("INSERT INTO v VALUES (?, ?, ?, ?, ?, ?, ?, ?)", values)
        row = self.conn.execute("SELECT * FROM v").fetchone()
        self.assertEqual(tuple(row), values)
        self.assertIsInstance(row[2], float)
        self.assertIsInstance(row[0], int)
        self.assertIsInstance(row[5], bytes)

    def test_parameter_styles_and_injection_safety(self):
        self.conn.execute("CREATE TABLE users (id INT, name TEXT)")
        evil = "x'); DROP TABLE users; --"
        self.conn.execute("INSERT INTO users VALUES ($1, $2)", (1, evil))
        self.conn.execute("INSERT INTO users VALUES (:1, :2)", [2, "b"])
        rows = self.conn.execute("SELECT name FROM users WHERE id <= ? ORDER BY id -- why?", (2,)).fetchall()
        self.assertEqual([r[0] for r in rows], [evil, "b"])
        with self.assertRaises(tinysql.ProgrammingError):
            self.conn.execute("SELECT ?", {"a": 1})
        with self.assertRaises(tinysql.ProgrammingError):
            self.conn.execute("SELECT ?, ?", (1,))
        with self.assertRaises(tinysql.ProgrammingError):
            self.conn.execute("SELECT ?", "not a sequence")
        with self.assertRaises(tinysql.DataError):
            self.conn.execute("SELECT ?", (2 ** 63,))
        with self.assertRaises(tinysql.DataError):
            self.conn.execute("SELECT ?", (math.inf,))
        with self.assertRaises(tinysql.ProgrammingError):
            self.conn.execute("SELECT ?", (object(),))

    def test_nul_sql_is_rejected_before_native_side_effects(self):
        self.conn.execute("CREATE TABLE nul_rows (id INT)")
        for operation in [
            lambda: self.conn.execute("INSERT INTO nul_rows VALUES (1)\x00ignored"),
            lambda: self.conn.executemany("INSERT INTO nul_rows VALUES (?)\x00ignored", [(1,)]),
            lambda: self.conn.executescript("INSERT INTO nul_rows VALUES (1);\x00ignored"),
        ]:
            with self.assertRaises(tinysql.ProgrammingError):
                operation()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM nul_rows").fetchone()[0], 0)
        self.conn.autocommit = False
        self.conn.execute("INSERT INTO nul_rows VALUES (2)")
        with self.assertRaises(tinysql.ProgrammingError):
            self.conn.executescript("SELECT 1\x00ignored")
        self.assertTrue(self.conn.in_transaction)
        self.conn.rollback()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM nul_rows").fetchone()[0], 0)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "snapshot")
            with self.assertRaises(tinysql.ProgrammingError):
                self.conn.save(path + "\x00ignored")
            self.assertFalse(os.path.exists(path))

    def test_commit_failure_rolls_back_transaction_scope(self):
        from unittest.mock import patch
        self.conn.execute("CREATE TABLE commit_rows (id INT)")
        error = tinysql.OperationalError("injected commit failure")
        with patch.object(self.conn, "commit", side_effect=error):
            with self.assertRaises(tinysql.OperationalError) as failure:
                with self.conn.transaction():
                    self.conn.execute("INSERT INTO commit_rows VALUES (1)")
        self.assertIs(failure.exception, error)
        self.assertFalse(self.conn.in_transaction)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM commit_rows").fetchone()[0], 0)
        with self.conn.transaction():
            self.conn.execute("INSERT INTO commit_rows VALUES (2)")
        self.assertEqual(self.conn.execute("SELECT id FROM commit_rows").fetchone()[0], 2)

    def test_transaction_cleanup_preserves_application_error(self):
        from unittest.mock import patch
        original = ValueError("application failure")
        with patch.object(self.conn, "rollback", side_effect=tinysql.OperationalError("cleanup failed")):
            with self.assertRaises(ValueError) as failure:
                with self.conn.transaction():
                    raise original
        self.assertIs(failure.exception, original)
        # Cleanup was deliberately made to fail; callers can still retry it.
        self.assertTrue(self.conn.in_transaction)
        self.conn.rollback()
        self.assertFalse(self.conn.in_transaction)

    def test_chunked_batches_preserve_order_types_and_counts(self):
        self.conn.execute("CREATE TABLE chunks (id INT PRIMARY KEY, text TEXT, raw BLOB)")
        consumed = []
        def rows():
            for i in range(7):
                consumed.append(i)
                yield (i, "text\x00" if i % 2 else None, b"\x00\xff")
        from unittest.mock import patch
        native_call = self.conn._library.call
        sizes = []
        def observe(name, *args):
            if name == "TinySQLDatabaseExecuteBatch":
                sizes.append(len(json.loads(args[-1])))
                self.assertEqual(len(consumed), sum(sizes))
            return native_call(name, *args)
        with patch.object(self.conn._library, "call", side_effect=observe):
            cur = self.conn.executemany("INSERT INTO chunks VALUES (?, ?, ?)", rows(), batch_size=3)
        self.assertEqual(sizes, [3, 3, 1])
        self.assertEqual(cur.rowcount, 7)
        self.assertEqual(consumed, list(range(7)))
        self.assertEqual([tuple(r) for r in self.conn.execute("SELECT * FROM chunks ORDER BY id")],
                         [(i, "text\x00" if i % 2 else None, b"\x00\xff") for i in range(7)])
        self.assertEqual(cur.executemany("INSERT INTO chunks VALUES (?, ?, ?)", iter(()), batch_size=2).rowcount, 0)
        for size in [0, -1, True, 1.5]:
            with self.assertRaises(tinysql.ProgrammingError):
                cur.executemany("INSERT INTO chunks VALUES (?, ?, ?)", rows(), batch_size=size)
        self.assertEqual(consumed, list(range(7)))

    def test_chunked_failures_and_default_validation_remain_distinct(self):
        self.conn.execute("CREATE TABLE chunks (id INT PRIMARY KEY)")
        with self.assertRaises(tinysql.IntegrityError):
            self.conn.executemany("INSERT INTO chunks VALUES (?)", [(1,), (2,), (1,)], batch_size=2)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0], 2)
        self.conn.execute("DELETE FROM chunks")
        with self.assertRaises(tinysql.IntegrityError):
            with self.conn.transaction():
                self.conn.executemany("INSERT INTO chunks VALUES (?)", [(1,), (2,), (1,)], batch_size=2)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0], 0)
        # Legacy mode validates the whole parameter iterable before any write.
        with self.assertRaises(tinysql.ProgrammingError):
            self.conn.executemany("INSERT INTO chunks VALUES (?)", [(1,), (object(),)])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0], 0)
        def broken():
            yield (3,)
            yield (4,)
            raise ValueError("source failed")
        with self.assertRaises(ValueError):
            with self.conn.transaction():
                self.conn.executemany("INSERT INTO chunks VALUES (?)", broken(), batch_size=2)
        self.assertFalse(self.conn.in_transaction)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0], 0)

    def test_large_chunked_generator(self):
        self.conn.execute("CREATE TABLE large_chunks (id INT PRIMARY KEY)")
        with self.conn.transaction():
            result = self.conn.executemany("INSERT INTO large_chunks VALUES (?)",
                                           ((i,) for i in range(5003)), batch_size=1024)
        self.assertEqual(result.rowcount, 5003)
        self.assertEqual(tuple(self.conn.execute("SELECT COUNT(*),SUM(id) FROM large_chunks").fetchone()),
                         (5003, 5003 * 5002 // 2))

    def test_default_executemany_preserves_legacy_cursor_override(self):
        from unittest.mock import patch
        self.conn.execute("CREATE TABLE legacy_cursor (id INT)")
        cursor = self.conn.cursor()
        class LegacyCursor:
            def executemany(self, sql, parameters):
                return cursor.executemany(sql, parameters)
        with patch.object(self.conn, "cursor", return_value=LegacyCursor()):
            result = self.conn.executemany("INSERT INTO legacy_cursor VALUES (?)", [(1,), (2,)])
        self.assertEqual(result.rowcount, 2)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM legacy_cursor").fetchone()[0], 2)

    def test_other_parameter_types(self):
        self.conn.execute("CREATE TABLE t (d TEXT, ts TEXT, dec TEXT, mv BLOB)")
        aware = datetime.datetime(2026, 9, 27, 14, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        self.conn.execute(
            "INSERT INTO t VALUES (?, ?, ?, ?)",
            (datetime.date(2026, 9, 27), aware, decimal.Decimal("1.10"), memoryview(b"ab")),
        )
        self.assertEqual(
            tuple(self.conn.execute("SELECT * FROM t").fetchone()),
            ("2026-09-27", "2026-09-27T12:00:00Z", "1.10", b"ab"),
        )

    def test_cursor_protocol(self):
        cur = self.conn.cursor()
        self.assertIsNone(cur.description)
        cur.execute("CREATE TABLE n (id INT)")
        self.assertIsNone(cur.description)
        cur.executemany("INSERT INTO n VALUES (?)", [(i,) for i in range(10)])
        self.assertEqual(cur.rowcount, 10)
        cur.execute("UPDATE n SET id = id + 100 WHERE id < ?", (3,))
        self.assertEqual(cur.rowcount, 3)
        cur.execute("SELECT id FROM n ORDER BY id")
        self.assertEqual(cur.description[0][0], "id")
        self.assertEqual(cur.rowcount, -1)
        self.assertEqual(cur.fetchone(), (3,))
        cur.arraysize = 2
        self.assertEqual(cur.fetchmany(), [(4,), (5,)])
        self.assertEqual(cur.fetchmany(3), [(6,), (7,), (8,)])
        self.assertEqual([r[0] for r in cur], [9, 100, 101, 102])
        self.assertIsNone(cur.fetchone())
        self.assertEqual(cur.fetchall(), [])
        cur.execute("DELETE FROM n WHERE id >= 100 RETURNING id")
        self.assertEqual(sorted(r[0] for r in cur.fetchall()), [100, 101, 102])
        self.assertEqual(self.conn.executemany("INSERT INTO n VALUES (?)", []).rowcount, 0)
        cur.close()
        with self.assertRaises(tinysql.InterfaceError):
            cur.execute("SELECT 1")

    def test_row_access_and_factories(self):
        self.conn.execute("CREATE TABLE p (id INT, Name TEXT)")
        self.conn.execute("INSERT INTO p VALUES (1, 'a')")
        row = self.conn.execute("SELECT id, Name FROM p").fetchone()
        self.assertEqual((row["id"], row["name"], row["NAME"], row[1]), (1, "a", "a", "a"))
        self.assertEqual(row.keys(), ["id", "Name"])
        self.assertEqual(row.asdict(), {"id": 1, "Name": "a"})
        with self.assertRaises(IndexError):
            row["missing"]
        self.conn.row_factory = tinysql.dict_row
        self.assertEqual(self.conn.execute("SELECT id FROM p").fetchall(), [{"id": 1}])

    def test_executescript_with_trigger(self):
        cur = self.conn.executescript(
            """
            CREATE TABLE src (id INT);
            CREATE TABLE audit (id INT, note TEXT);
            CREATE TRIGGER copy AFTER INSERT ON src BEGIN
                INSERT INTO audit VALUES (NEW.id, 'semi;colon');
            END;
            INSERT INTO src VALUES (1), (2);
            """
        )
        self.assertEqual(cur.rowcount, 2)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0], 2)
        with self.assertRaisesRegex(tinysql.ProgrammingError, "statement 2"):
            self.conn.executescript("INSERT INTO src VALUES (3); INSERT INTO nope VALUES (1)")

    def test_explicit_transactions(self):
        self.conn.execute("CREATE TABLE t (id INT)")
        with self.conn.transaction():
            self.conn.execute("INSERT INTO t VALUES (1)")
            self.assertTrue(self.conn.in_transaction)
        self.assertFalse(self.conn.in_transaction)
        with self.assertRaises(RuntimeError):
            with self.conn.transaction():
                self.conn.execute("INSERT INTO t VALUES (2)")
                raise RuntimeError("abort")
        self.assertEqual(self.conn.execute("SELECT id FROM t").fetchall(), [(1,)])
        self.conn.execute("BEGIN")
        self.assertTrue(self.conn.in_transaction)
        self.conn.execute("INSERT INTO t VALUES (3)")
        self.conn.rollback()
        self.assertFalse(self.conn.in_transaction)
        self.conn.executescript("BEGIN; INSERT INTO t VALUES (4);")
        self.assertTrue(self.conn.in_transaction)
        self.conn.commit()
        self.assertEqual(sorted(r[0] for r in self.conn.execute("SELECT id FROM t")), [1, 4])
        self.conn.commit()  # no-op without a transaction
        self.conn.rollback()

    def test_pep249_autocommit_off(self):
        conn = tinysql.connect(autocommit=False)
        self.addCleanup(conn.close)
        conn.execute("CREATE TABLE t (id INT)")
        self.assertFalse(conn.in_transaction)
        conn.execute("INSERT INTO t VALUES (1)")
        self.assertTrue(conn.in_transaction)
        conn.rollback()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0], 0)
        conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
        conn.commit()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0], 2)

    def test_errors_are_translated(self):
        with self.assertRaises(tinysql.ProgrammingError):
            self.conn.execute("SELEC 1")
        with self.assertRaises(tinysql.ProgrammingError):
            self.conn.execute("SELECT * FROM missing")
        self.conn.execute("CREATE TABLE u (id INT PRIMARY KEY)")
        self.conn.execute("INSERT INTO u VALUES (1)")
        with self.assertRaises(tinysql.IntegrityError):
            self.conn.execute("INSERT INTO u VALUES (1)")

    def test_close_is_idempotent(self):
        conn = tinysql.connect()
        conn.close()
        conn.close()
        self.assertTrue(conn.closed)
        with self.assertRaises(tinysql.InterfaceError):
            conn.execute("SELECT 1")

    def test_connections_are_isolated_and_parallel(self):
        other = tinysql.connect()
        self.addCleanup(other.close)
        self.conn.execute("CREATE TABLE only_here (id INT)")
        with self.assertRaises(tinysql.ProgrammingError):
            other.execute("SELECT * FROM only_here")
        errors = []

        def work(n):
            try:
                with tinysql.connect() as conn:
                    conn.execute("CREATE TABLE t (id INT)")
                    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(200)])
                    total = conn.execute("SELECT SUM(id) FROM t").fetchone()[0]
                    assert total == sum(range(200)), total
            except Exception as error:  # pragma: no cover - reported below
                errors.append(error)

        threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = pathlib.Path(self.directory.name)

    def test_snapshot_save_and_load(self):
        for name in ("db.tinysql", "db.tinysql.gz"):
            snapshot = self.path / name
            with tinysql.connect() as conn:
                conn.executescript("CREATE TABLE t (id INT, s TEXT); INSERT INTO t VALUES (1, 'Grüße')")
                conn.save(snapshot)
            with tinysql.connect(snapshot) as conn:
                self.assertEqual(conn.execute("SELECT s FROM t").fetchone()[0], "Grüße")
                conn.execute("INSERT INTO t VALUES (2, 'unsaved')")
            with tinysql.connect(str(snapshot), read_only=True) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0], 1)
                with self.assertRaises(tinysql.DatabaseError):
                    conn.execute("INSERT INTO t VALUES (3, 'x')")
        with gzip.open(self.path / "db.tinysql.gz", "rb") as compressed:
            self.assertTrue(compressed.read(1))
        with self.assertRaises(tinysql.DatabaseError):
            tinysql.connect(self.path / "missing.tinysql")

    def test_durable_modes(self):
        for mode in ("wal", "advanced_wal", "disk", "json"):
            with self.subTest(mode=mode):
                directory = self.path / mode
                with tinysql.connect(directory, mode=mode, wal_sync="normal") as conn:
                    conn.executescript("CREATE TABLE t (id INT, raw BLOB)")
                    conn.execute("INSERT INTO t VALUES (?, ?)", (9007199254740993, b"\x00\x01"))
                    conn.sync()
                with tinysql.connect(directory, mode=mode) as conn:
                    self.assertEqual(tuple(conn.execute("SELECT id, raw FROM t").fetchone()), (9007199254740993, b"\x00\x01"))

    def test_encrypted_storage(self):
        key = os.urandom(32)
        directory = self.path / "secret"
        with tinysql.connect(directory, mode="json", encryption_key=key) as conn:
            conn.executescript("CREATE TABLE s (v TEXT); INSERT INTO s VALUES ('hidden value')")
        contents = b"".join(p.read_bytes() for p in directory.rglob("*") if p.is_file())
        self.assertNotIn(b"hidden value", contents)
        with tinysql.connect(directory, mode="json", encryption_key=key) as conn:
            self.assertEqual(conn.execute("SELECT v FROM s").fetchone()[0], "hidden value")
        with self.assertRaises(tinysql.ProgrammingError):
            tinysql.connect(directory, mode="json", encryption_key=b"short")

    def test_invalid_options(self):
        with self.assertRaises(TypeError):
            tinysql.connect(unknown_option=True)
        with self.assertRaises(tinysql.DatabaseError):
            tinysql.connect(self.path, mode="bogus")
        with self.assertRaises(tinysql.DatabaseError):
            tinysql.connect(mode="disk")


if __name__ == "__main__":
    unittest.main()
