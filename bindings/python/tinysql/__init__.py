"""tinySQL for Python: an embedded SQL engine with a DB-API 2.0 interface.

>>> import tinysql
>>> with tinysql.connect() as conn:
...     _ = conn.execute("CREATE TABLE users (id INT, name TEXT)")
...     _ = conn.executemany("INSERT INTO users VALUES (?, ?)", [(1, "Ada"), (2, "Grace")])
...     row = conn.execute("SELECT name FROM users WHERE id = ?", (2,)).fetchone()
>>> row, row["name"]
(('Grace',), 'Grace')

The module follows PEP 249 (``paramstyle = "qmark"``; ``$1`` and ``:1``
placeholders work too). Unlike PEP 249's default, connections autocommit
unless opened with ``autocommit=False``; use :meth:`Connection.transaction`
for explicit transactions.
"""

from __future__ import annotations

import base64
import contextlib
import datetime
import decimal
import json
import math
import re
import uuid
from typing import Any, Callable, Iterable, Iterator, List, Optional, Sequence, Tuple

from ._native import Library, NativeError, load_library

__all__ = [
    "connect", "Connection", "Cursor", "Row", "dict_row", "engine_version", "load_library",
    "apilevel", "threadsafety", "paramstyle",
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError", "OperationalError",
    "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError",
    "Binary", "Date", "Time", "Timestamp", "DateFromTicks", "TimeFromTicks", "TimestampFromTicks",
    "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID",
]

__version__ = "0.2.0"

apilevel = "2.0"
# Connections serialize their own native calls, but cursors and transaction
# state are not shared safely between threads: share the module, not
# connections. Separate connections run queries in parallel (ctypes releases
# the GIL during native calls).
threadsafety = 1
paramstyle = "qmark"


# PEP 249 exception hierarchy ------------------------------------------------

class Warning(Exception):  # noqa: A001 - name required by PEP 249
    """Important warnings, such as data truncation."""


class Error(Exception):
    """Base class of all tinySQL errors."""


class InterfaceError(Error):
    """Errors of the binding itself, such as using a closed connection."""


class DatabaseError(Error):
    """Errors reported by the database engine."""


class DataError(DatabaseError):
    """Problems with processed data, such as out-of-range values."""


class OperationalError(DatabaseError):
    """Database operation errors, such as I/O, conflicts or read-only storage."""


class IntegrityError(DatabaseError):
    """Constraint violations such as UNIQUE, PRIMARY KEY or FOREIGN KEY."""


class InternalError(DatabaseError):
    """Internal engine errors."""


class ProgrammingError(DatabaseError):
    """SQL syntax errors, missing tables or wrong parameter counts."""


class NotSupportedError(DatabaseError):
    """A feature the engine does not support."""


_ERROR_PATTERNS: Sequence[Tuple["re.Pattern[str]", type]] = (
    (re.compile(r"unique|primary key|foreign key|not null|constraint|duplicate", re.I), IntegrityError),
    (re.compile(r"conflict|read-only|read only|closed|locked|busy|timeout|i/o|no such file|permission", re.I), OperationalError),
    (re.compile(r"parse error|syntax|no such (table|column|view|index|function)|unknown (column|table|function)|"
                r"placeholder|args for|parameter|expected|ambiguous", re.I), ProgrammingError),
    (re.compile(r"not supported|unsupported", re.I), NotSupportedError),
    (re.compile(r"overflow|out of range|invalid .*value|cannot convert|division by zero", re.I), DataError),
    (re.compile(r"panic", re.I), InternalError),
)


def _translate(error: NativeError) -> DatabaseError:
    message = str(error)
    for pattern, cls in _ERROR_PATTERNS:
        if pattern.search(message):
            return cls(message)
    return DatabaseError(message)


# PEP 249 type objects and constructors --------------------------------------

Binary = bytes
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def DateFromTicks(ticks: float) -> datetime.date:
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: float) -> datetime.time:
    return datetime.datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ticks)


class _TypeObject:
    def __init__(self, *names: str):
        self.names = frozenset(names)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, str) and other.upper() in self.names

    def __hash__(self) -> int:
        return hash(self.names)


STRING = _TypeObject("TEXT", "STRING", "VARCHAR", "CHAR")
BINARY = _TypeObject("BLOB", "BINARY", "BYTES")
NUMBER = _TypeObject("INT", "INTEGER", "FLOAT", "REAL", "DOUBLE", "NUMERIC", "DECIMAL")
DATETIME = _TypeObject("DATE", "TIME", "DATETIME", "TIMESTAMP")
ROWID = _TypeObject("ROWID")

_INT64_MIN, _INT64_MAX = -(2 ** 63), 2 ** 63 - 1


def _encode_parameter(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        if not _INT64_MIN <= value <= _INT64_MAX:
            raise DataError(f"integer {value} does not fit into a signed 64-bit INT")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DataError("SQL REAL values must be finite")
        return {"real": value}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"blob": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc)
            return value.replace(tzinfo=None).isoformat() + "Z"
        return value.isoformat()
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (decimal.Decimal, uuid.UUID)):
        return str(value)
    raise ProgrammingError(f"unsupported parameter type {type(value).__name__}")


def _encode_list(parameters: Any) -> List[Any]:
    if parameters is None:
        return []
    if isinstance(parameters, dict):
        raise ProgrammingError("named parameters are not supported; use ?, $1 or :1 placeholders")
    if isinstance(parameters, (str, bytes)) or not isinstance(parameters, Sequence):
        raise ProgrammingError("parameters must be a sequence such as a tuple or list")
    return [_encode_parameter(v) for v in parameters]


def _dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


_FIRST_WORD = re.compile(r"^\s*(?:(?:--[^\n]*(?:\n|$)|/\*.*?\*/)\s*)*([A-Za-z]+)", re.S)
_DML_WORDS = frozenset({"INSERT", "UPDATE", "DELETE", "REPLACE", "UPSERT"})


def _first_word(sql: str) -> str:
    match = _FIRST_WORD.match(sql)
    return match.group(1).upper() if match else ""


class Row(tuple):
    """A result row: a tuple that also supports access by column name."""

    __slots__ = ()
    _columns: Tuple[str, ...] = ()

    def __getitem__(self, key: Any) -> Any:  # type: ignore[override]
        if isinstance(key, str):
            try:
                return tuple.__getitem__(self, self._index[key.lower()])
            except KeyError:
                raise IndexError(f"no column named {key!r}") from None
        return tuple.__getitem__(self, key)

    def keys(self) -> List[str]:
        return list(self._columns)

    def asdict(self) -> dict:
        return dict(zip(self._columns, self))


_ROW_CLASSES: dict = {}


def _row_class(columns: Tuple[str, ...]) -> type:
    # Result shapes repeat; reuse one Row subclass per column list.
    cls = _ROW_CLASSES.get(columns)
    if cls is None:
        index: dict = {}
        for i, name in enumerate(columns):
            index.setdefault(name.lower(), i)
        cls = type("Row", (Row,), {"__slots__": (), "_columns": columns, "_index": index})
        if len(_ROW_CLASSES) >= 256:
            _ROW_CLASSES.clear()
        _ROW_CLASSES[columns] = cls
    return cls


def dict_row(cursor: "Cursor", row: tuple) -> dict:
    """A ``row_factory`` returning ``{column: value}`` dictionaries."""
    return {d[0]: v for d, v in zip(cursor.description or (), row)}


class Cursor:
    """A PEP 249 cursor. Results are materialized when a statement runs."""

    arraysize = 1

    def __init__(self, connection: "Connection"):
        self.connection = connection
        self.description: Optional[Tuple[Tuple[Any, ...], ...]] = None
        self.rowcount = -1
        self.lastrowid = None
        self.row_factory: Optional[Callable[["Cursor", tuple], Any]] = connection.row_factory
        self._rows: List[tuple] = []
        self._position = 0
        self._closed = False

    # Execution ---------------------------------------------------------------

    def execute(self, sql: str, parameters: Any = None) -> "Cursor":
        """Run one statement. Row-producing statements fill the result set."""
        self._check()
        values = _encode_list(parameters)
        self.connection._before_statement(sql)
        result = self.connection._call("TinySQLDatabaseRun", sql, _dumps(values) if values else "")
        self._set_result(result)
        return self

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> "Cursor":
        """Prepare *sql* once and run it for every parameter sequence."""
        self._check()
        sets = [_encode_list(p) for p in seq_of_parameters]
        self._reset()
        if not sets:
            self.rowcount = 0
            return self
        self.connection._before_statement(sql)
        result = self.connection._call("TinySQLDatabaseExecuteBatch", sql, _dumps(sets))
        self.rowcount = result.get("rowsAffected", 0)
        return self

    def executescript(self, script: str) -> "Cursor":
        """Run a multi-statement script; a pending implicit transaction commits first."""
        self._check()
        self.connection._commit_implicit()
        result = self.connection._call("TinySQLDatabaseExecuteScript", script)
        self._reset()
        self.rowcount = result.get("rowsAffected", 0)
        return self

    def _set_result(self, result: dict) -> None:
        self._reset()
        columns = result.get("columns")
        if columns:
            names = tuple(columns)
            self.description = tuple((name, None, None, None, None, None, None) for name in names)
            factory = self.row_factory
            if factory is None:
                row_type = _row_class(names)
                self._rows = [row_type(row) for row in result.get("rows", ())]
            else:
                self._rows = [factory(self, tuple(row)) for row in result.get("rows", ())]
        else:
            self.rowcount = result.get("rowsAffected", 0)

    def _reset(self) -> None:
        self.description = None
        self.rowcount = -1
        self._rows = []
        self._position = 0

    # Fetching ------------------------------------------------------------------

    def fetchone(self) -> Any:
        self._check()
        if self._position >= len(self._rows):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        self._check()
        size = self.arraysize if size is None else size
        rows = self._rows[self._position:self._position + size]
        self._position += len(rows)
        return rows

    def fetchall(self) -> List[Any]:
        self._check()
        rows = self._rows[self._position:]
        self._position = len(self._rows)
        return rows

    def __iter__(self) -> Iterator[Any]:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    # PEP 249 housekeeping ------------------------------------------------------

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        pass

    def close(self) -> None:
        self._closed = True
        self._rows = []

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _check(self) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")
        self.connection._check()


class Connection:
    """A tinySQL database handle.

    Each connection owns an independent database and pins one engine
    connection, so ``BEGIN``/``COMMIT`` span calls.
    """

    # PEP 249 optional extension: exceptions as connection attributes.
    Warning, Error, InterfaceError, DatabaseError = Warning, Error, InterfaceError, DatabaseError
    DataError, OperationalError, IntegrityError = DataError, OperationalError, IntegrityError
    InternalError, ProgrammingError, NotSupportedError = InternalError, ProgrammingError, NotSupportedError

    def __init__(self, library: Library, handle: int, autocommit: bool):
        self._library = library
        self._handle = handle
        self.autocommit = autocommit
        self.in_transaction = False
        self.row_factory: Optional[Callable[[Cursor, tuple], Any]] = None

    # Statements ----------------------------------------------------------------

    def cursor(self) -> Cursor:
        self._check()
        return Cursor(self)

    def execute(self, sql: str, parameters: Any = None) -> Cursor:
        """Run one statement and return a cursor with its result."""
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> Cursor:
        """Prepare *sql* once and run it for every parameter sequence."""
        return self.cursor().executemany(sql, seq_of_parameters)

    def executescript(self, script: str) -> Cursor:
        """Run a multi-statement script. A pending implicit transaction is committed first."""
        return self.cursor().executescript(script)

    # Transactions --------------------------------------------------------------

    def begin(self) -> None:
        """Start an explicit transaction."""
        if self.in_transaction:
            raise ProgrammingError("a transaction is already active")
        self._call("TinySQLDatabaseExecute", "BEGIN", "")

    def commit(self) -> None:
        """Commit the active transaction; a no-op without one."""
        self._check()
        if self.in_transaction:
            self._call("TinySQLDatabaseExecute", "COMMIT", "")

    def rollback(self) -> None:
        """Roll back the active transaction; a no-op without one."""
        self._check()
        if self.in_transaction:
            self._call("TinySQLDatabaseExecute", "ROLLBACK", "")

    @contextlib.contextmanager
    def transaction(self) -> Iterator["Connection"]:
        """``with conn.transaction():`` commits on success and rolls back on error."""
        self.begin()
        try:
            yield self
        except BaseException:
            if self.in_transaction:
                self.rollback()
            raise
        else:
            self.commit()

    def _before_statement(self, sql: str) -> None:
        # PEP 249 mode: open a transaction implicitly before data changes.
        if not self.autocommit and not self.in_transaction and _first_word(sql) in _DML_WORDS:
            self.begin()

    def _commit_implicit(self) -> None:
        if not self.autocommit and self.in_transaction:
            self.commit()

    # Persistence and lifecycle -------------------------------------------------

    def save(self, path: str) -> None:
        """Write committed state to a snapshot file (gzip when it ends in ``.gz``)."""
        self._call("TinySQLDatabaseSave", str(path))

    def sync(self) -> None:
        """Flush durable storage modes (disk, json, index, hybrid) to disk."""
        self._call("TinySQLDatabaseSync")

    def close(self) -> None:
        """Close the database. An unfinished transaction is discarded. Idempotent."""
        if self._handle:
            handle, self._handle = self._handle, 0
            self.in_transaction = False
            try:
                self._library.call("TinySQLDatabaseClose", handle)
            except NativeError as error:
                raise _translate(error) from None

    @property
    def closed(self) -> bool:
        return not self._handle

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit (or roll back on error) a pending transaction, then close."""
        try:
            if self._handle and self.in_transaction:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _check(self) -> None:
        if not self._handle:
            raise InterfaceError("connection is closed")

    def _call(self, name: str, *args: Any) -> dict:
        self._check()
        statement = name in _STATEMENT_FUNCTIONS
        try:
            result = self._library.call(name, self._handle, *args)
        except NativeError as error:
            if statement:
                self.in_transaction = error.in_transaction
            raise _translate(error) from None
        # Statement responses report the engine's transaction state, which
        # also covers BEGIN/COMMIT issued as SQL text or inside scripts.
        if statement:
            self.in_transaction = bool(result.get("inTransaction", False))
        return result


_STATEMENT_FUNCTIONS = frozenset({
    "TinySQLDatabaseExecute", "TinySQLDatabaseQuery", "TinySQLDatabaseRun",
    "TinySQLDatabaseExecuteBatch", "TinySQLDatabaseExecuteScript",
})

_OPTION_NAMES = {
    "max_memory_bytes": "maxMemoryBytes",
    "sync_on_mutate": "syncOnMutate",
    "compress_files": "compressFiles",
    "checkpoint_every": "checkpointEvery",
    "checkpoint_interval_ms": "checkpointIntervalMs",
    "checkpoint_max_bytes": "checkpointMaxBytes",
    "wal_sync": "walSync",
}


def connect(
    path: Optional[str] = None,
    *,
    mode: Optional[str] = None,
    read_only: bool = False,
    autocommit: bool = True,
    encryption_key: Optional[bytes] = None,
    library: Optional[str] = None,
    **storage_options: Any,
) -> Connection:
    """Open a tinySQL database.

    * ``connect()`` or ``connect(":memory:")``: a new in-memory database.
    * ``connect("app.tinysql")``: load a snapshot written by
      :meth:`Connection.save`; changes are saved only when you call ``save``.
    * ``connect("./data", mode="wal")``: a durable database. Modes are
      ``wal``, ``advanced_wal``, ``disk``, ``json``, ``index``, ``hybrid``
      and ``paged_index``; every acknowledged write is persisted.

    Storage tuning keywords: ``max_memory_bytes``, ``sync_on_mutate``,
    ``compress_files``, ``checkpoint_every``, ``checkpoint_interval_ms``,
    ``checkpoint_max_bytes`` and ``wal_sync`` (``"full"`` or ``"normal"``).
    ``encryption_key`` (32 bytes) encrypts table files of the disk, json,
    index and hybrid modes.

    With ``autocommit=False`` a transaction starts implicitly before
    INSERT/UPDATE/DELETE and lasts until :meth:`Connection.commit`, as
    PEP 249 describes.
    """
    lib = load_library(library)
    options: dict = {}
    if path is not None and str(path) != ":memory:":
        options["path"] = str(path)
    if mode is not None:
        options["mode"] = mode
    if read_only:
        options["readOnly"] = True
    if encryption_key is not None:
        if len(encryption_key) != 32:
            raise ProgrammingError("encryption_key must be exactly 32 bytes")
        options["encryptionKey"] = base64.b64encode(bytes(encryption_key)).decode("ascii")
    for name, value in storage_options.items():
        if name not in _OPTION_NAMES:
            raise TypeError(f"connect() got an unexpected keyword argument {name!r}")
        options[_OPTION_NAMES[name]] = value
    try:
        result = lib.call("TinySQLDatabaseOpenWithOptions", json.dumps(options))
    except NativeError as error:
        raise _translate(error) from None
    return Connection(lib, int(result["handle"]), autocommit)


def engine_version(library: Optional[str] = None) -> str:
    """Return the version of the loaded tinySQL engine."""
    return str(load_library(library).call("TinySQLInfo")["version"])
