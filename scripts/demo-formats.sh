#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." >/dev/null

if ! command -v go >/dev/null 2>&1; then
    echo "go toolchain not found" >&2
    exit 1
fi

demo_tmp="$(mktemp -d "${TMPDIR:-/tmp}/tinysql-demo-formats.XXXXXX")"
trap 'rm -rf "$demo_tmp"' EXIT
go build -o "$demo_tmp/repl" ./cmd/repl

run_format() {
    local title="$1"
    local format="$2"
    echo "==================== ${title} ===================="
    "$demo_tmp/repl" --format="$format" <<'SQL'
SELECT 'Alice' AS name, 30 AS age, 'Engineer' AS job;
SQL
    echo ""
}

run_format "TABLE (default)" table
run_format JSON json
run_format YAML yaml
run_format CSV csv
run_format TSV tsv
run_format MARKDOWN markdown
