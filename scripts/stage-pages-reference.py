#!/usr/bin/env python3
"""Stage the SQL reference and check that the published site's local links work."""

import shutil
import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.targets = []

    def handle_starttag(self, tag, attributes):
        self.targets.extend(value for name, value in attributes
                            if name in {"href", "src"} and value)


def stage_reference(site, repository):
    source = repository / "docs" / "reference"
    destination = site / "docs" / "reference"
    destination.mkdir(parents=True, exist_ok=True)
    html = (source / "functions.html").read_text(encoding="utf-8")
    html = html.replace('href="../sql-semantics.md"',
                        'href="https://github.com/SimonWaldherr/tinySQL/blob/main/docs/sql-semantics.md"')
    html = html.replace('href="../../cmd/wasm_browser/web/index.html"',
                        'href="../../index.html"')
    (destination / "functions.html").write_text(html, encoding="utf-8")
    shutil.copyfile(source / "functions.sql", destination / "functions.sql")


def validate_links(site):
    site = site.resolve()
    errors = []
    for html in sorted(site.rglob("*.html")):
        parser = Links()
        parser.feed(html.read_text(encoding="utf-8"))
        for target in parser.targets:
            url = urlsplit(target)
            if url.scheme or url.netloc or not url.path:
                continue
            path = unquote(url.path)
            resolved = (site / path.lstrip("/") if path.startswith("/")
                        else html.parent / path).resolve()
            if not resolved.is_relative_to(site) or not resolved.exists():
                errors.append(f"{html.relative_to(site)}: missing site link target: {target}")
    return errors


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} site-directory", file=sys.stderr)
        return 2
    site = Path(sys.argv[1])
    repository = Path(__file__).resolve().parent.parent
    stage_reference(site, repository)
    errors = validate_links(site)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("Staged the SQL reference and checked all local HTML links.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
