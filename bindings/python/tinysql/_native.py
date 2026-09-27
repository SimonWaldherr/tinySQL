"""ctypes access to the tinySQL C ABI (bindings/c/include/tinysql.h)."""

from __future__ import annotations

import base64
import ctypes
import ctypes.util
import json
import os
import pathlib
import sys
import threading
from typing import Any, Optional

REQUIRED_ABI = 2

_LIBRARY_NAMES = {
    "darwin": ("libtinysql.dylib",),
    "win32": ("tinysql.dll", "libtinysql.dll"),
}.get(sys.platform, ("libtinysql.so",))

_lock = threading.Lock()
_library: Optional["Library"] = None


class NativeError(Exception):
    """An error reported by the native library."""

    def __init__(self, message: str, in_transaction: bool = False):
        super().__init__(message)
        self.in_transaction = in_transaction


def _decode_value(obj: dict) -> Any:
    # Result cells use tagged objects for doubles and BLOBs. Text is always a
    # JSON string, so a one-key object is never user data.
    if len(obj) == 1:
        if "real" in obj:
            return float(obj["real"])
        if "blob" in obj:
            return base64.b64decode(obj["blob"])
    return obj


class Library:
    """A loaded libtinysql shared library."""

    def __init__(self, path: str):
        lib = ctypes.CDLL(path)
        self.path = path
        lib.TinySQLABIVersion.argtypes = []
        lib.TinySQLABIVersion.restype = ctypes.c_int32
        abi = lib.TinySQLABIVersion()
        if abi < REQUIRED_ABI:
            raise NativeError(
                f"{path} implements tinySQL ABI {abi}; version {REQUIRED_ABI} or newer is required"
            )
        # Returned buffers are declared as void* so ctypes does not copy and
        # discard the pointer that must be passed back to TinySQLDatabaseFree.
        text, handle, ptr = ctypes.c_char_p, ctypes.c_uint64, ctypes.c_void_p
        signatures = {
            "TinySQLInfo": [],
            "TinySQLDatabaseOpen": [text],
            "TinySQLDatabaseOpenWithOptions": [text],
            "TinySQLDatabaseExecute": [handle, text, text],
            "TinySQLDatabaseQuery": [handle, text, text],
            "TinySQLDatabaseRun": [handle, text, text],
            "TinySQLDatabaseExecuteBatch": [handle, text, text],
            "TinySQLDatabaseExecuteScript": [handle, text],
            "TinySQLDatabaseSave": [handle, text],
            "TinySQLDatabaseSync": [handle],
            "TinySQLDatabaseClose": [handle],
        }
        for name, argtypes in signatures.items():
            function = getattr(lib, name)
            function.argtypes = argtypes
            function.restype = ptr
        lib.TinySQLDatabaseFree.argtypes = [ptr]
        lib.TinySQLDatabaseFree.restype = None
        self._lib = lib

    def call(self, name: str, *args: Any) -> dict:
        """Call a response-returning function and decode its JSON object."""
        encoded = [a.encode("utf-8") if isinstance(a, str) else a for a in args]
        pointer = getattr(self._lib, name)(*encoded)
        if not pointer:
            raise NativeError(f"{name} returned NULL")
        try:
            payload = ctypes.string_at(pointer)
        finally:
            self._lib.TinySQLDatabaseFree(pointer)
        result = json.loads(payload, object_hook=_decode_value)
        error = result.get("error")
        if error:
            raise NativeError(error, bool(result.get("inTransaction", False)))
        return result


def _candidates(explicit: Optional[str]):
    if explicit:
        yield explicit
        return
    env = os.environ.get("TINYSQL_LIBRARY")
    if env:
        yield env
        return
    here = pathlib.Path(__file__).resolve().parent
    for name in _LIBRARY_NAMES:
        yield str(here / name)
    found = ctypes.util.find_library("tinysql")
    if found:
        yield found


def load_library(path: Optional[str] = None) -> Library:
    """Load (once) and return the native library.

    Search order: *path*, the ``TINYSQL_LIBRARY`` environment variable, the
    package directory, then the system library path.
    """
    global _library
    with _lock:
        if _library is not None and (path is None or _library.path == path):
            return _library
        tried = []
        for candidate in _candidates(path):
            tried.append(candidate)
            if os.sep in candidate and not os.path.exists(candidate):
                continue
            try:
                library = Library(candidate)
            except OSError:
                continue
            if path is None:
                _library = library
            return library
        raise NativeError(
            "libtinysql was not found (tried: "
            + ", ".join(tried)
            + "). Build it with `make -C bindings/python build` or `go build "
            "-buildmode=c-shared -o bindings/python/tinysql/"
            + _LIBRARY_NAMES[0]
            + " ./bindings/c`, or set TINYSQL_LIBRARY."
        )
