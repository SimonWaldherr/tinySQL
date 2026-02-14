// Tests for the importer package. These exercises cover common CSV and
// JSON import scenarios used by the ImportCSV and ImportJSON helpers.
// The tests are intentionally simple and focus on behavioral guarantees
// (delimiter detection, header handling, type inference and null handling)
// rather than full end-to-end CLI flows.
package importer

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestImportCSV_Basic verifies a simple CSV with header is imported and
// rows/columns are recorded as expected.
func TestImportCSV_Basic(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	csvData := `id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35`

	result, err := ImportCSV(ctx, db, "default", "users",
		strings.NewReader(csvData), &ImportOptions{
			CreateTable:   true,
			TypeInference: true,
			HeaderMode:    "present",
		})

	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if result.RowsInserted != 3 {
		t.Errorf("Expected 3 rows inserted, got %d", result.RowsInserted)
	}

	if len(result.ColumnNames) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.ColumnNames))
	}

	if result.Delimiter != ',' {
		t.Errorf("Expected comma delimiter, got %c", result.Delimiter)
	}

	// Verify data was inserted
	tbl, err := db.Get("default", "users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if len(tbl.Rows) != 3 {
		t.Errorf("Expected 3 rows in table, got %d", len(tbl.Rows))
	}
}

// TestImportCSV_NoHeader verifies behavior when the CSV has no header
// row: the importer should synthesize column names (col_1, col_2, ...).
func TestImportCSV_NoHeader(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	csvData := `1,Alice,30
2,Bob,25
3,Charlie,35`

	result, err := ImportCSV(ctx, db, "default", "users",
		strings.NewReader(csvData), &ImportOptions{
			CreateTable: true,
			HeaderMode:  "absent",
		})

	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if result.RowsInserted != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowsInserted)
	}

	// Should generate column names
	if len(result.ColumnNames) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.ColumnNames))
	}

	expectedNames := []string{"col_1", "col_2", "col_3"}
	for i, name := range expectedNames {
		if result.ColumnNames[i] != name {
			t.Errorf("Expected column %s, got %s", name, result.ColumnNames[i])
		}
	}
}

// TestImportCSV_TSV ensures the importer can detect a tab delimiter and
// import TSV data correctly.
func TestImportCSV_TSV(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	tsvData := "id\tname\tage\n1\tAlice\t30\n2\tBob\t25"

	result, err := ImportCSV(ctx, db, "default", "users",
		strings.NewReader(tsvData), &ImportOptions{
			CreateTable:         true,
			DelimiterCandidates: []rune{'\t'},
		})

	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if result.Delimiter != '\t' {
		t.Errorf("Expected tab delimiter, got %c", result.Delimiter)
	}

	if result.RowsInserted != 2 {
		t.Errorf("Expected 2 rows, got %d", result.RowsInserted)
	}
}

// TestImportCSV_TypeInference checks that basic type inference classifies
// integer, text, float and boolean sample columns correctly.
func TestImportCSV_TypeInference(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	csvData := `id,name,price,active,created
1,Widget,19.99,true,2024-01-01
2,Gadget,29.99,false,2024-01-02
3,Doohickey,39.99,true,2024-01-03`

	result, err := ImportCSV(ctx, db, "default", "products",
		strings.NewReader(csvData), &ImportOptions{
			CreateTable:   true,
			TypeInference: true,
			HeaderMode:    "present", // Explicitly state header is present
		})

	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	// Verify column names were detected
	expectedNames := []string{"id", "name", "price", "active", "created"}
	for i, name := range expectedNames {
		if result.ColumnNames[i] != name {
			t.Errorf("Column %d: expected name %s, got %s", i, name, result.ColumnNames[i])
		}
	}

	// Check inferred types
	if result.ColumnTypes[0] != storage.IntType {
		t.Errorf("Column %s: expected INT type, got %v",
			result.ColumnNames[0], result.ColumnTypes[0])
	}

	if result.ColumnTypes[1] != storage.TextType {
		t.Errorf("Column %s: expected TEXT type, got %v",
			result.ColumnNames[1], result.ColumnTypes[1])
	}

	if result.ColumnTypes[2] != storage.Float64Type {
		t.Errorf("Column %s: expected FLOAT type, got %v",
			result.ColumnNames[2], result.ColumnTypes[2])
	}

	if result.ColumnTypes[3] != storage.BoolType {
		t.Errorf("Column %s: expected BOOL type, got %v",
			result.ColumnNames[3], result.ColumnTypes[3])
	}
}

// TestImportCSV_NullHandling verifies that configured null literal
// strings are interpreted as SQL NULL values on import.
func TestImportCSV_NullHandling(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	csvData := `id,name,age
1,Alice,30
2,,25
3,Charlie,`

	result, err := ImportCSV(ctx, db, "default", "users",
		strings.NewReader(csvData), &ImportOptions{
			CreateTable:   true,
			TypeInference: false, // Use TEXT for simplicity
			NullLiterals:  []string{"", "N/A", "null"},
		})

	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if result.RowsInserted != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowsInserted)
	}

	// Verify nulls were inserted
	tbl, _ := db.Get("default", "users")

	// Row 2 (index 1) should have NULL name
	if tbl.Rows[1][1] != nil {
		t.Errorf("Expected NULL for row 2 name, got %v", tbl.Rows[1][1])
	}

	// Row 3 (index 2) should have NULL age
	if tbl.Rows[2][2] != nil {
		t.Errorf("Expected NULL for row 3 age, got %v", tbl.Rows[2][2])
	}
}

// TestImportJSON_ArrayOfObjects validates importing a JSON array of
// objects produces the expected rows in the target table.
func TestImportJSON_ArrayOfObjects(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	jsonData := `[
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "age": 25},
		{"id": 3, "name": "Charlie", "age": 35}
	]`

	result, err := ImportJSON(ctx, db, "default", "users",
		strings.NewReader(jsonData), &ImportOptions{
			CreateTable:   true,
			TypeInference: true,
		})

	if err != nil {
		t.Fatalf("ImportJSON failed: %v", err)
	}

	if result.RowsInserted != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowsInserted)
	}

	// Verify data
	tbl, _ := db.Get("default", "users")
	if len(tbl.Rows) != 3 {
		t.Errorf("Expected 3 rows in table, got %d", len(tbl.Rows))
	}
}

// TestTypeInference_Integer asserts that columns with integer-like
// samples are inferred as INT.
func TestTypeInference_Integer(t *testing.T) {
	samples := [][]string{
		{"1", "2", "3"},
		{"10", "20", "30"},
		{"100", "200", "300"},
	}

	types := inferColumnTypes(samples, 3, &ImportOptions{
		TypeInference: true,
		NullLiterals:  []string{""},
	})

	if types[0] != storage.IntType {
		t.Errorf("Expected INT type, got %v", types[0])
	}
}

// TestTypeInference_Float asserts that floating-point-like samples
// are inferred as FLOAT.
func TestTypeInference_Float(t *testing.T) {
	samples := [][]string{
		{"1.5", "2.7", "3.9"},
		{"10.1", "20.2", "30.3"},
	}

	types := inferColumnTypes(samples, 3, &ImportOptions{
		TypeInference: true,
		NullLiterals:  []string{""},
	})

	if types[0] != storage.Float64Type {
		t.Errorf("Expected FLOAT type, got %v", types[0])
	}
}

// TestTypeInference_Boolean asserts that boolean-like samples are
// inferred as BOOL.
func TestTypeInference_Boolean(t *testing.T) {
	samples := [][]string{
		{"true", "false", "true"},
		{"false", "true", "false"},
		{"true", "true", "false"},
	}

	types := inferColumnTypes(samples, 3, &ImportOptions{
		TypeInference: true,
		NullLiterals:  []string{""},
	})

	for i := 0; i < 3; i++ {
		if types[i] != storage.BoolType {
			t.Errorf("Column %d: expected BOOL type, got %v", i, types[i])
		}
	}
}

// TestTypeInference_MixedDefaultsToText ensures mixed-type columns
// fall back to TEXT to preserve data.
func TestTypeInference_MixedDefaultsToText(t *testing.T) {
	samples := [][]string{
		{"1", "text", "true"},
		{"2", "more text", "3.14"},
		{"three", "100", "false"},
	}

	types := inferColumnTypes(samples, 3, &ImportOptions{
		TypeInference: true,
		NullLiterals:  []string{""},
	})

	// All columns should default to TEXT due to mixed types
	for i := 0; i < 3; i++ {
		if types[i] != storage.TextType {
			t.Errorf("Column %d: expected TEXT type, got %v", i, types[i])
		}
	}
}

// TestDelimiterDetection checks that common delimiters are detected
// correctly from example lines.
func TestDelimiterDetection(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		expected rune
	}{
		{"comma", "a,b,c\n1,2,3", ','},
		{"semicolon", "a;b;c\n1;2;3", ';'},
		{"tab", "a\tb\tc\n1\t2\t3", '\t'},
		{"pipe", "a|b|c\n1|2|3", '|'},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines := splitUniversal(tt.data)
			delim := detectDelimiter(lines, []rune{',', ';', '\t', '|'})
			if delim != tt.expected {
				t.Errorf("Expected delimiter %c, got %c", tt.expected, delim)
			}
		})
	}
}

// TestSanitizeColumnNames ensures invalid characters are replaced and
// empty names are turned into synthetic column names.
func TestSanitizeColumnNames(t *testing.T) {
	input := []string{"Name", "User-ID", "email.address", "", "age/years"}
	expected := []string{"Name", "User_ID", "email_address", "col_4", "age_years"}

	result := sanitizeColumnNames(input)

	for i, exp := range expected {
		if result[i] != exp {
			t.Errorf("Column %d: expected %s, got %s", i, exp, result[i])
		}
	}
}

// TestImportCSV_QuotedFields verifies CSV parsing with quoted fields containing
// delimiters and escaped quotes.
func TestImportCSV_QuotedFields(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	csvData := `id,desc
1,"Hello, world", 
2,"He said ""Hi""",`

	result, err := ImportCSV(ctx, db, "default", "quotes",
		strings.NewReader(csvData), &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}
	if result.RowsInserted != 2 {
		t.Fatalf("expected 2 rows inserted, got %d", result.RowsInserted)
	}
	tbl, _ := db.Get("default", "quotes")
	// Check that commas inside quotes and escaped quotes are preserved
	if tbl.Rows[0][1] != "Hello, world" {
		t.Fatalf("expected first desc to be Hello, world, got %v", tbl.Rows[0][1])
	}
	if tbl.Rows[1][1] != `He said "Hi"` {
		t.Fatalf("expected second desc to contain escaped quotes, got %v", tbl.Rows[1][1])
	}
}

// TestImportCSV_CRLF ensures CRLF line endings are handled correctly.
func TestImportCSV_CRLF(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	csvData := "id,name\r\n1,Alice\r\n2,Bob\r\n"

	result, err := ImportCSV(ctx, db, "default", "crlf",
		strings.NewReader(csvData), &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}
	if result.RowsInserted != 2 {
		t.Fatalf("expected 2 rows inserted, got %d", result.RowsInserted)
	}
}

// TestImportJSON_NDJSON verifies line-delimited JSON (NDJSON) imports correctly.
func TestImportJSON_NDJSON(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	ndjson := `{"id":1, "name":"Alice"}
{"id":2, "name":"Bob"}
{"id":3, "name":"Charlie"}`

	result, err := ImportJSON(ctx, db, "default", "ndjson",
		strings.NewReader(ndjson), &ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		t.Fatalf("ImportJSON NDJSON failed: %v", err)
	}
	if result.RowsInserted != 3 {
		t.Fatalf("expected 3 rows inserted, got %d", result.RowsInserted)
	}
	tbl, _ := db.Get("default", "ndjson")
	if len(tbl.Rows) != 3 {
		t.Fatalf("expected 3 rows in table, got %d", len(tbl.Rows))
	}
}
