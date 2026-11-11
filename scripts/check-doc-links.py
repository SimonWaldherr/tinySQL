#!/usr/bin/env python3
"""Check repository-local Markdown link destinations without network access.

Inline links, images and reference definitions are checked. Fragments and
external URLs are left to their renderers; fenced and inline code is ignored.
"""

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


INLINE_LINK = re.compile(r"!?\[[^\]\n]*\]\(\s*(<[^>\n]+>|(?:\\.|[^\s()\n]|\([^()\n]*\))+)\s*(?:[\"'][^\n]*[\"']\s*)?\)")
REFERENCE = re.compile(r"^\s{0,3}\[[^\]\n]+\]:\s*(<[^>\n]+>|\S+)")
FENCE = re.compile(r"^\s{0,3}(`{3,}|~{3,})(.*)$")


def destinations(source):
    """Yield (line number, destination), retaining line numbers for failures."""
    fence = None
    for number, line in enumerate(source.splitlines(), 1):
        marker = FENCE.match(line)
        if marker:
            run, suffix = marker.groups()
            if fence is None:
                fence = run
            elif run[0] == fence[0] and len(run) >= len(fence) and not suffix.strip():
                fence = None
            continue
        if fence is not None:
            continue
        # Match delimiters of equal length, so code containing a single
        # backtick can still be quoted with a double-backtick span.
        line = re.sub(r"(`+).*?(?<!`)\1(?!`)", "", line)
        definition = REFERENCE.match(line)
        matches = [definition] if definition else INLINE_LINK.finditer(line)
        for match in matches:
            yield number, match.group(1).strip("<>")


def check_file(path, root):
    errors = []
    for number, destination in destinations(path.read_text(encoding="utf-8")):
        destination = re.sub(r"\\([\\() ])", r"\1", destination)
        url = urlsplit(destination)
        if url.scheme or url.netloc or not url.path:
            continue
        target = unquote(url.path)
        resolved = root / target.lstrip("/") if target.startswith("/") else path.parent / target
        if not resolved.exists():
            errors.append(f"{path.relative_to(root)}:{number}: missing link target: {destination}")
    return errors


def main():
    root = Path(__file__).resolve().parent.parent
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "*.md"], cwd=root
    ).decode().split("\0")
    paths = [root / name for name in names if name]
    errors = [error for path in paths for error in check_file(path, root)]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Checked local links in {len(paths)} Markdown files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
