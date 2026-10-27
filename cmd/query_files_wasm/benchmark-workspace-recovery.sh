#!/usr/bin/env bash
# Compare a committed baseline to the current workspace in a real browser.
# Usage: ./benchmark-workspace-recovery.sh [git-ref]
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_dir="$(cd "$script_dir/../.." && pwd -P)"
baseline_ref="${1:-HEAD}"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/tinysql-workspace-bench.XXXXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
git -C "$repo_dir" show "$baseline_ref:cmd/query_files_wasm/workspace-storage.js" > "$work_dir/baseline.js"
cp "$script_dir/workspace-storage.js" "$work_dir/"
cp "$script_dir/workspace-recovery-bench.html" "$work_dir/index.html"
printf 'Open http://127.0.0.1:%s/ to run the test; press Ctrl-C to stop.\n' "${PORT:-18974}"
python3 -m http.server "${PORT:-18974}" --bind 127.0.0.1 --directory "$work_dir"
