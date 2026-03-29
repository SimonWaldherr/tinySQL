#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v go >/dev/null 2>&1; then
    echo "go toolchain not found" >&2
    exit 1
fi

SQL_FILE="$(mktemp "${TMPDIR:-/tmp}/tinysql-demo-formats.XXXXXX.sql")"
trap 'rm -f "$SQL_FILE"' EXIT

cat >"$SQL_FILE" <<'EOF'
SELECT 'Alice' as name, 30 as age, 'Engineer' as job;
EOF

run_format() {
    local title="$1"
    local format="$2"
    local lines="$3"

    echo "==================== ${title} ===================="
    if [[ -n "$format" ]]; then
        go run ./cmd/repl --format="$format" --echo <"$SQL_FILE" 2>&1 | tail -n "$lines"
    else
        go run ./cmd/repl --echo <"$SQL_FILE" 2>&1 | tail -n "$lines"
    fi
    echo ""
}

run_format "TABLE (default)" "" 8
run_format "JSON" "json" 6
run_format "YAML" "yaml" 7
run_format "CSV" "csv" 5
run_format "TSV" "tsv" 5
run_format "MARKDOWN" "markdown" 7
