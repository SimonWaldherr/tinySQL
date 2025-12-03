#!/bin/bash
echo "SELECT 'Alice' as name, 30 as age, 'Engineer' as job;" > /tmp/demo.sql

echo "==================== TABLE (default) ===================="
go run cmd/repl/main.go --echo < /tmp/demo.sql 2>&1 | tail -8

echo ""
echo "==================== JSON ===================="
go run cmd/repl/main.go --format=json --echo < /tmp/demo.sql 2>&1 | tail -6

echo ""
echo "==================== YAML ===================="
go run cmd/repl/main.go --format=yaml --echo < /tmp/demo.sql 2>&1 | tail -7

echo ""
echo "==================== CSV ===================="
go run cmd/repl/main.go --format=csv --echo < /tmp/demo.sql 2>&1 | tail -5

echo ""
echo "==================== TSV ===================="
go run cmd/repl/main.go --format=tsv --echo < /tmp/demo.sql 2>&1 | tail -5

echo ""
echo "==================== MARKDOWN ===================="
go run cmd/repl/main.go --format=markdown --echo < /tmp/demo.sql 2>&1 | tail -7
