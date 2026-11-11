#!/usr/bin/env python3
"""Regression checks for the standalone Pages artifact."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("pages", Path(__file__).with_name("stage-pages-reference.py"))
pages = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pages)


class PagesReference(unittest.TestCase):
    def test_stage_rewrites_links_without_changing_repository_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "repository" / "docs" / "reference"
            source.mkdir(parents=True)
            html = ('<a href="functions.sql">SQL</a>'
                    '<a href="../sql-semantics.md">Semantics</a>'
                    '<a href="../../cmd/wasm_browser/web/index.html">Playground</a>')
            (source / "functions.html").write_text(html)
            (source / "functions.sql").write_bytes(b"SELECT 1;\n")
            site = root / "site"
            site.mkdir()
            (site / "index.html").write_text('<script src="app.js"></script>')
            (site / "app.js").touch()
            pages.stage_reference(site, root / "repository")
            self.assertEqual(pages.validate_links(site), [])
            self.assertEqual((source / "functions.html").read_text(), html)
            self.assertEqual((site / "docs/reference/functions.sql").read_bytes(), b"SELECT 1;\n")
            staged = (site / "docs/reference/functions.html").read_text()
            self.assertIn('href="../../index.html"', staged)
            self.assertIn('href="https://github.com/SimonWaldherr/tinySQL/blob/main/docs/sql-semantics.md"', staged)

    def test_missing_files_and_links_outside_the_site_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            site = root / "site"
            site.mkdir()
            (root / "outside.js").touch()
            (site / "space name.js").touch()
            (site / "index.html").write_text(
                '<a href="missing.html#id">Missing</a><script src="../outside.js"></script>'
                '<script src="space%20name.js"></script><a href="#id">Anchor</a>'
                '<a href="https://example.invalid/">External</a><img src="data:image/png;base64,AA==">')
            self.assertEqual(pages.validate_links(site), [
                "index.html: missing site link target: missing.html#id",
                "index.html: missing site link target: ../outside.js",
            ])


if __name__ == "__main__":
    unittest.main()
