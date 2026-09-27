"""Build hook: compile the Go shared library into the package when needed.

`pip install ./bindings/python` from a repository checkout runs
`go build -buildmode=c-shared ./bindings/c` unless the library is already in
tinysql/ or TINYSQL_SKIP_GO_BUILD is set. The wheel is marked as
platform-specific because it bundles native code.
"""

import os
import pathlib
import shutil
import subprocess
import sys

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.dist import Distribution

HERE = pathlib.Path(__file__).resolve().parent
LIBRARY = {"darwin": "libtinysql.dylib", "win32": "tinysql.dll"}.get(sys.platform, "libtinysql.so")


class BuildWithGo(build_py):
    def run(self):
        target = HERE / "tinysql" / LIBRARY
        root = HERE.parent.parent
        if not target.exists() and not os.environ.get("TINYSQL_SKIP_GO_BUILD"):
            if not (root / "go.mod").exists() or shutil.which("go") is None:
                raise SystemExit(
                    f"{target} is missing. Build it with `make -C bindings/python build` "
                    "(requires Go and a C toolchain) or set TINYSQL_SKIP_GO_BUILD=1."
                )
            subprocess.check_call(
                ["go", "build", "-trimpath", "-buildmode=c-shared", "-o", str(target), "./bindings/c"],
                cwd=root,
            )
            header = target.with_suffix(".h")
            if header.exists():
                header.unlink()
        super().run()


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


setup(cmdclass={"build_py": BuildWithGo}, distclass=BinaryDistribution)
