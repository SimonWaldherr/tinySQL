#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v go >/dev/null 2>&1; then
    echo "go toolchain not found" >&2
    exit 1
fi

BIN_PATH="$(mktemp "${TMPDIR:-/tmp}/tinysql-query-files-demo.XXXXXX")"
trap 'rm -f "$BIN_PATH"' EXIT

go build -trimpath -o "$BIN_PATH" .

run_case() {
    local title="$1"
    shift
    echo "$title"
    "$BIN_PATH" "$@"
    echo ""
}

echo "=== TinySQL File Query Demo (Optimized with Fuzzy Import) ==="
echo ""

run_case "1. Simple CSV query (select all):" \
    -query "SELECT name, age, city FROM sample" sample.csv

run_case "2. Filter by number (age > 25):" \
    -query "SELECT name, age FROM sample WHERE age > 25" sample.csv

run_case "3. Filter by text column (city = 'Paris'):" \
    -query "SELECT name, age FROM sample WHERE city = 'Paris'" sample.csv

run_case "4. JSON query with JSON output (ordered by name):" \
    -query "SELECT name, city FROM sample ORDER BY name" -output json sample.json

run_case "5. CSV query with pattern matching (city contains 'on'):" \
    -query "SELECT name, age, city FROM sample WHERE city LIKE '%on%'" sample.csv

run_case "6. Range query (age between 22 and 28):" \
    -query "SELECT name, age FROM sample WHERE age >= 22 AND age <= 28 ORDER BY age" sample.csv

run_case "7. Aggregation query (count by city):" \
    -query "SELECT city, COUNT(*) as count FROM sample GROUP BY city" sample.csv

run_case "8. CSV output format:" \
    -query "SELECT name, city FROM sample ORDER BY name" -output csv sample.csv

run_case "9. Semicolon-delimited CSV (auto-detected delimiter):" \
    -query "SELECT name, amount, status FROM semicolon WHERE status = 'active'" semicolon.csv

run_case "10. Numeric operations:" \
    -query "SELECT name, amount FROM semicolon WHERE amount > 600 ORDER BY amount DESC" semicolon.csv
