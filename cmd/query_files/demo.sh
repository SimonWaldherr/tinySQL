#!/bin/bash

echo "=== TinySQL File Query Demo ==="
echo ""

echo "1. Simple CSV query (age > 25):"
go run main.go -query "SELECT name, age FROM sample WHERE age > 25" sample.csv
echo ""

echo "2. JSON query with JSON output (age < 30, ordered by age):"
go run main.go -query "SELECT name, city FROM sample WHERE age < 30 ORDER BY age" -output json sample.json
echo ""

echo "3. XML query with pattern matching (city contains 'on'):"
go run main.go -query "SELECT name, age FROM sample WHERE city LIKE '%on%'" sample.xml
echo ""

echo "4. CSV query with range and CSV output (age between 22 and 28):"
go run main.go -query "SELECT name, age, city FROM sample WHERE age BETWEEN 22 AND 28 ORDER BY age" -output csv sample.csv
echo ""

echo "5. Aggregation query (count by city):"
go run main.go -query "SELECT city, COUNT(*) as count FROM sample GROUP BY city" sample.csv
echo ""

echo "=== Demo completed ==="

