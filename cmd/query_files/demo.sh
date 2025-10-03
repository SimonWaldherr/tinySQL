#!/bin/bash

echo "=== TinySQL File Query Demo (Optimized with Fuzzy Import) ==="
echo ""

echo "1. Simple CSV query (select all):"
go run main.go -query "SELECT name, age, city FROM sample" sample.csv
echo ""

echo "2. Filter by number (age > 25):"
go run main.go -query "SELECT name, age FROM sample WHERE age > 25" sample.csv
echo ""

echo "3. Filter by text column (city = 'Paris'):"
go run main.go -query "SELECT name, age FROM sample WHERE city = 'Paris'" sample.csv
echo ""

echo "4. JSON query with JSON output (ordered by name):"
go run main.go -query "SELECT name, city FROM sample ORDER BY name" -output json sample.json
echo ""

echo "5. CSV query with pattern matching (city contains 'on'):"
go run main.go -query "SELECT name, age, city FROM sample WHERE city LIKE '%on%'" sample.csv
echo ""

echo "6. Range query (age between 22 and 28):"
go run main.go -query "SELECT name, age FROM sample WHERE age >= 22 AND age <= 28 ORDER BY age" sample.csv
echo ""

echo "7. Aggregation query (count by city):"
go run main.go -query "SELECT city, COUNT(*) as count FROM sample GROUP BY city" sample.csv
echo ""

echo "8. CSV output format:"
go run main.go -query "SELECT name, city FROM sample ORDER BY name" -output csv sample.csv
echo ""

echo "9. Semicolon-delimited CSV (auto-detected delimiter):"
go run main.go -query "SELECT name, amount, status FROM semicolon WHERE status = 'active'" semicolon.csv
echo ""

echo "10. Numeric operations:"
go run main.go -query "SELECT name, amount FROM semicolon WHERE amount > 600 ORDER BY amount DESC" semicolon.csv
echo ""
