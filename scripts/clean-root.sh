#!/usr/bin/env bash
# Remove known, ignored build output left by older build/profile commands.
# Database files and WALs are persistent data and are never cleanup targets.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null
shopt -s nullglob

for artifact in .DS_Store .gocache *.test *.test.exe *.prof *.pprof *.trace \
    coverage*.out coverage*.html coverage.txt coverage-func.txt \
    cpu.out mem.out block.out mutex.out mbtilesdemo mbtilesregion; do
    [[ -e "$artifact" || -L "$artifact" ]] || continue
    # Refuse tracked paths even if someone later adds an overlapping rule.
    if [[ -n "$(git ls-files -- "$artifact")" ]]; then
        echo "Refusing to remove tracked path: $artifact" >&2
        exit 1
    fi
    if git check-ignore -q -- "$artifact"; then
        rm -rf -- "$artifact"
    fi
done
