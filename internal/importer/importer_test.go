package importer

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

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
