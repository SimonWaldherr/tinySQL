#!/usr/bin/env python3
"""Regression cases for offline documentation checks."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("doclinks", Path(__file__).with_name("check-doc-links.py"))
doclinks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(doclinks)


class LinkChecks(unittest.TestCase):
    def test_links_code_and_references(self):
        source = """[inline](target.md#heading) ![image](<image name.svg>)
[ref]: /docs/file.md "Title"
```markdown
[example](missing.md)
```
~~~
``` does not close a tilde fence
[example](missing-too.md)
~~~
`[code](skip.md)` [nested](a(b).md)
"""
        self.assertEqual(list(doclinks.destinations(source)), [
            (1, "target.md#heading"), (1, "image name.svg"),
            (2, "/docs/file.md"), (10, "a(b).md"),
        ])

    def test_resolution_and_missing_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "docs").mkdir()
            (root / "space name.md").touch()
            source = root / "docs" / "guide.md"
            source.write_text("[space](../space%20name.md) [root](/space%20name.md) "
                              "[web](https://example.invalid) [anchor](#id)\n[bad](missing.md)")
            self.assertEqual(doclinks.check_file(source, root), [
                "docs/guide.md:2: missing link target: missing.md"
            ])


if __name__ == "__main__":
    unittest.main()
