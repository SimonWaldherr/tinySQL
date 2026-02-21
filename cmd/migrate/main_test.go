package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestBuildMigrate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_migrate_bin")
	defer os.Remove(out)

	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}

	info, err := os.Stat(out)
	if err != nil {
		t.Fatalf("stat binary: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("binary is empty")
	}
	t.Logf("migrate binary size: %d bytes", info.Size())
}

func TestParseDSN(t *testing.T) {
	tests := []struct {
		dsn        string
		wantDriver string
		wantConn   string
	}{
		{"postgres://user:pass@localhost/db?sslmode=disable", "postgres", "postgres://user:pass@localhost/db?sslmode=disable"},
		{"postgresql://user:pass@localhost/db", "postgres", "postgresql://user:pass@localhost/db"},
		{"mysql://user:pass@tcp(localhost:3306)/db", "mysql", "user:pass@tcp(localhost:3306)/db"},
		{"sqlite://test.db", "sqlite", "test.db"},
		{"mssql://user:pass@localhost:1433?database=db", "sqlserver", "sqlserver://user:pass@localhost:1433?database=db"},
		{"sqlserver://user:pass@localhost:1433?database=db", "sqlserver", "sqlserver://user:pass@localhost:1433?database=db"},
		{"test.db", "sqlite", "test.db"},
		{"test.sqlite", "sqlite", "test.sqlite"},
		{"user:pass@tcp(localhost:3306)/db", "mysql", "user:pass@tcp(localhost:3306)/db"},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			driver, conn := parseDSN(tt.dsn)
			if driver != tt.wantDriver {
				t.Errorf("parseDSN(%q) driver = %q, want %q", tt.dsn, driver, tt.wantDriver)
			}
			if conn != tt.wantConn {
				t.Errorf("parseDSN(%q) conn = %q, want %q", tt.dsn, conn, tt.wantConn)
			}
		})
	}
}

func TestTableNameFromFile(t *testing.T) {
	tests := []struct {
		filename string
		want     string
	}{
		{"users.csv", "users"},
		{"data/sales.json", "sales"},
		{"my-data.csv", "my_data"},
		{"file with spaces.csv", "file_with_spaces"},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got := tableNameFromFile(tt.filename)
			if got != tt.want {
				t.Errorf("tableNameFromFile(%q) = %q, want %q", tt.filename, got, tt.want)
			}
		})
	}
}

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"users", "users"},
		{"my-table", "my_table"},
		{"table.name", "table_name"},
		{"table name", "table_name"},
		{"123abc", "123abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeTableName(tt.name)
			if got != tt.want {
				t.Errorf("sanitizeTableName(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"nil", nil, "NULL"},
		{"int", int64(42), "42"},
		{"float", 3.14, "3.14"},
		{"string", "hello", "'hello'"},
		{"string with quote", "it's", "'it''s'"},
		{"bool true", true, "TRUE"},
		{"bool false", false, "FALSE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatValue(tt.val)
			if got != tt.want {
				t.Errorf("formatValue(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		driver string
		name   string
		want   string
	}{
		{"mysql", "users", "`users`"},
		{"postgres", "users", `"users"`},
		{"sqlserver", "users", "[users]"},
		{"sqlite", "users", `"users"`},
	}

	for _, tt := range tests {
		t.Run(tt.driver+"/"+tt.name, func(t *testing.T) {
			got := quoteIdentifier(tt.driver, tt.name)
			if got != tt.want {
				t.Errorf("quoteIdentifier(%q, %q) = %q, want %q", tt.driver, tt.name, got, tt.want)
			}
		})
	}
}

func TestMaskDSN(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{"postgres://user:secret@localhost/db", "postgres://user:***@localhost/db"},
		{"test.db", "test.db"},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			got := maskDSN(tt.dsn)
			if got != tt.want {
				t.Errorf("maskDSN(%q) = %q, want %q", tt.dsn, got, tt.want)
			}
		})
	}
}

func TestSplitQuotedFields(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{`import pg users`, []string{"import", "pg", "users"}},
		{`import pg "SELECT * FROM users" AS local`, []string{"import", "pg", "SELECT * FROM users", "AS", "local"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := splitQuotedFields(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("splitQuotedFields(%q) = %v (len %d), want %v (len %d)",
					tt.input, got, len(got), tt.want, len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitQuotedFields(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestImportFileAndQuery(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test_users.csv")
	err := os.WriteFile(csvFile, []byte("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	db := tinysql.NewDB()
	tenant := "default"

	err = importFileToTinySQL(db, ctx, tenant, csvFile, "users", true, false)
	if err != nil {
		t.Fatalf("importFileToTinySQL failed: %v", err)
	}

	stmt, err := tinysql.ParseSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("ParseSQL failed: %v", err)
	}

	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestImportJSONFile(t *testing.T) {
	tmpDir := t.TempDir()
	jsonFile := filepath.Join(tmpDir, "data.json")
	err := os.WriteFile(jsonFile, []byte(`[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	db := tinysql.NewDB()

	err = importFileToTinySQL(db, ctx, "default", jsonFile, "data", true, false)
	if err != nil {
		t.Fatalf("importFileToTinySQL (JSON) failed: %v", err)
	}

	stmt, err := tinysql.ParseSQL("SELECT * FROM data")
	if err != nil {
		t.Fatalf("ParseSQL failed: %v", err)
	}

	result, err := tinysql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}
