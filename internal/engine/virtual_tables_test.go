package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// mustParseSys is a small helper to parse SQL (reuses the existing mustParse helper).
func mustParseSys(sql string) Statement {
	return mustParse(sql)
}

// setupTestDB creates a DB with some tables for testing sys.* virtual tables.
func setupTestDB() *storage.DB {
	db := storage.NewDB()

	// Create table: users
	tUsers := storage.NewTable("users", []storage.Column{
		{Name: "id", Type: storage.IntType, Constraint: storage.PrimaryKey},
		{Name: "name", Type: storage.StringType},
		{Name: "email", Type: storage.TextType},
	}, false)
	tUsers.Rows = [][]any{
		{1, "Alice", "alice@example.com"},
		{2, "Bob", "bob@example.com"},
		{3, "Carol", "carol@example.com"},
	}
	db.Put("main", tUsers)

	// Create table: orders
	tOrders := storage.NewTable("orders", []storage.Column{
		{Name: "id", Type: storage.IntType, Constraint: storage.PrimaryKey},
		{Name: "user_id", Type: storage.IntType, Constraint: storage.ForeignKey, ForeignKey: &storage.ForeignKeyRef{Table: "users", Column: "id"}},
		{Name: "amount", Type: storage.Float64Type},
	}, false)
	tOrders.Rows = [][]any{
		{101, 1, 99.99},
		{102, 2, 149.50},
	}
	db.Put("main", tOrders)

	// Create a temp table
	tTemp := storage.NewTable("scratch", []storage.Column{
		{Name: "val", Type: storage.StringType},
	}, true)
	db.Put("main", tTemp)

	// Register a view in catalog
	db.Catalog().RegisterView("main", "active_users", "SELECT * FROM users WHERE active = true")

	return db
}

// TestSysTables verifies sys.tables returns real tables.
func TestSysTables(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.tables"))
	if err != nil {
		t.Fatalf("SELECT sys.tables failed: %v", err)
	}
	if len(rs.Rows) < 3 {
		t.Fatalf("expected at least 3 tables, got %d", len(rs.Rows))
	}

	// Check that users table is present with correct row count
	found := false
	for _, r := range rs.Rows {
		if v, ok := r["name"]; ok && v == "users" {
			found = true
			if rows, ok := r["rows"]; ok {
				if rowCount, ok := rows.(int); ok && rowCount != 3 {
					t.Errorf("expected 3 rows for users, got %d", rowCount)
				}
			}
			if cols, ok := r["columns"]; ok {
				if colCount, ok := cols.(int); ok && colCount != 3 {
					t.Errorf("expected 3 columns for users, got %d", colCount)
				}
			}
		}
	}
	if !found {
		t.Fatal("users table not found in sys.tables")
	}
}

// TestSysTablesWithWhere tests filtering sys.tables with WHERE clause.
func TestSysTablesWithWhere(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT name, rows FROM sys.tables WHERE name = 'users'"))
	if err != nil {
		t.Fatalf("SELECT sys.tables WHERE failed: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["name"] != "users" {
		t.Errorf("expected 'users', got %v", rs.Rows[0]["name"])
	}
}

// TestSysColumns verifies sys.columns returns real column metadata.
func TestSysColumns(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.columns WHERE table_name = 'users'"))
	if err != nil {
		t.Fatalf("SELECT sys.columns failed: %v", err)
	}
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 columns for users, got %d", len(rs.Rows))
	}

	// Check id column is PRIMARY KEY
	for _, r := range rs.Rows {
		if r["name"] == "id" {
			if r["constraint"] != "PRIMARY KEY" {
				t.Errorf("expected PRIMARY KEY constraint for id, got %v", r["constraint"])
			}
		}
	}
}

// TestSysConstraints verifies sys.constraints returns only constrained columns.
func TestSysConstraints(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.constraints WHERE table_name = 'orders'"))
	if err != nil {
		t.Fatalf("SELECT sys.constraints failed: %v", err)
	}
	if len(rs.Rows) < 2 {
		t.Fatalf("expected at least 2 constraints for orders (PK + FK), got %d", len(rs.Rows))
	}

	// Check FK on user_id
	foundFK := false
	for _, r := range rs.Rows {
		if r["column_name"] == "user_id" && r["constraint_type"] == "FOREIGN KEY" {
			foundFK = true
			if r["fk_table"] != "users" {
				t.Errorf("expected FK table 'users', got %v", r["fk_table"])
			}
			if r["fk_column"] != "id" {
				t.Errorf("expected FK column 'id', got %v", r["fk_column"])
			}
		}
	}
	if !foundFK {
		t.Fatal("FK constraint on orders.user_id not found")
	}
}

// TestSysIndexes verifies sys.indexes returns empty (CREATE INDEX is a no-op).
func TestSysIndexes(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.indexes"))
	if err != nil {
		t.Fatalf("SELECT sys.indexes failed: %v", err)
	}
	if len(rs.Rows) != 0 {
		t.Fatalf("expected 0 indexes, got %d", len(rs.Rows))
	}
}

// TestSysViews verifies sys.views returns registered views.
func TestSysViews(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.views"))
	if err != nil {
		t.Fatalf("SELECT sys.views failed: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 view, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["name"] != "active_users" {
		t.Errorf("expected view 'active_users', got %v", rs.Rows[0]["name"])
	}
}

// TestSysFunctions verifies sys.functions returns builtin functions.
func TestSysFunctions(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.functions"))
	if err != nil {
		t.Fatalf("SELECT sys.functions failed: %v", err)
	}

	// Should have a lot of functions (builtin + extended + vector).
	if len(rs.Rows) < 50 {
		t.Fatalf("expected at least 50 functions, got %d", len(rs.Rows))
	}

	// Check aggregate functions are correctly classified.
	for _, r := range rs.Rows {
		name, _ := r["name"].(string)
		fnType, _ := r["function_type"].(string)
		switch name {
		case "COUNT", "SUM", "AVG":
			if fnType != "AGGREGATE" {
				t.Errorf("expected AGGREGATE for %s, got %s", name, fnType)
			}
		case "ROW_NUMBER", "LAG", "LEAD":
			if fnType != "WINDOW" {
				t.Errorf("expected WINDOW for %s, got %s", name, fnType)
			}
		}
	}
}

// TestSysFunctionsWithWhere verifies filtering sys.functions.
func TestSysFunctionsWithWhere(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT name FROM sys.functions WHERE function_type = 'AGGREGATE'"))
	if err != nil {
		t.Fatalf("SELECT sys.functions WHERE failed: %v", err)
	}
	if len(rs.Rows) < 3 {
		t.Fatalf("expected at least 3 aggregate functions, got %d", len(rs.Rows))
	}
	for _, r := range rs.Rows {
		if r["function_type"] != nil && r["function_type"] != "AGGREGATE" {
			t.Errorf("expected only AGGREGATE functions, got %v", r["function_type"])
		}
	}
}

// TestSysVariables verifies sys.variables returns key/value pairs.
func TestSysVariables(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.variables"))
	if err != nil {
		t.Fatalf("SELECT sys.variables failed: %v", err)
	}
	if len(rs.Rows) < 5 {
		t.Fatalf("expected at least 5 variables, got %d", len(rs.Rows))
	}

	// Check essential variables exist.
	wantKeys := map[string]bool{
		"version": false, "go_version": false, "os": false,
		"arch": false, "storage_mode": false, "pid": false,
	}
	for _, r := range rs.Rows {
		if key, ok := r["name"].(string); ok {
			if _, want := wantKeys[key]; want {
				wantKeys[key] = true
			}
		}
	}
	for key, found := range wantKeys {
		if !found {
			t.Errorf("missing variable: %s", key)
		}
	}
}

// TestSysStatus verifies sys.status returns runtime info.
func TestSysStatus(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.status"))
	if err != nil {
		t.Fatalf("SELECT sys.status failed: %v", err)
	}
	if len(rs.Rows) < 8 {
		t.Fatalf("expected at least 8 status rows, got %d", len(rs.Rows))
	}

	// Check some mandatory keys.
	wantKeys := map[string]bool{
		"uptime": false, "goroutines": false, "go_version": false,
		"gc_runs": false, "pid": false,
	}
	for _, r := range rs.Rows {
		if key, ok := r["key"].(string); ok {
			if _, want := wantKeys[key]; want {
				wantKeys[key] = true
			}
		}
	}
	for key, found := range wantKeys {
		if !found {
			t.Errorf("missing status key: %s", key)
		}
	}
}

// TestSysMemory verifies sys.memory returns memory statistics.
func TestSysMemory(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.memory"))
	if err != nil {
		t.Fatalf("SELECT sys.memory failed: %v", err)
	}
	if len(rs.Rows) < 10 {
		t.Fatalf("expected at least 10 memory stats, got %d", len(rs.Rows))
	}

	// Check essential memory keys.
	wantKeys := map[string]bool{
		"alloc_bytes": false, "heap_alloc_bytes": false,
		"gc_runs": false, "num_goroutine": false,
	}
	for _, r := range rs.Rows {
		if key, ok := r["key"].(string); ok {
			if _, want := wantKeys[key]; want {
				wantKeys[key] = true
			}
		}
	}
	for key, found := range wantKeys {
		if !found {
			t.Errorf("missing memory key: %s", key)
		}
	}
}

// TestSysStorage verifies sys.storage returns backend statistics.
func TestSysStorage(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.storage"))
	if err != nil {
		t.Fatalf("SELECT sys.storage failed: %v", err)
	}
	if len(rs.Rows) < 5 {
		t.Fatalf("expected at least 5 storage stats, got %d", len(rs.Rows))
	}

	// Mode should be present.
	found := false
	for _, r := range rs.Rows {
		if r["key"] == "mode" {
			found = true
			if v, ok := r["value"].(string); ok && v == "" {
				t.Error("mode value should not be empty")
			}
		}
	}
	if !found {
		t.Error("missing storage key: mode")
	}
}

// TestSysConfig verifies sys.config returns configuration.
func TestSysConfig(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.config"))
	if err != nil {
		t.Fatalf("SELECT sys.config failed: %v", err)
	}
	if len(rs.Rows) < 1 {
		t.Fatalf("expected at least 1 config row, got %d", len(rs.Rows))
	}

	found := false
	for _, r := range rs.Rows {
		if r["key"] == "storage_mode" {
			found = true
		}
	}
	if !found {
		t.Error("missing config key: storage_mode")
	}
}

// TestSysConnections verifies sys.connections returns tenant info.
func TestSysConnections(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.connections"))
	if err != nil {
		t.Fatalf("SELECT sys.connections failed: %v", err)
	}
	if len(rs.Rows) < 1 {
		t.Fatalf("expected at least 1 connection (tenant), got %d", len(rs.Rows))
	}

	found := false
	for _, r := range rs.Rows {
		if r["tenant"] == "main" {
			found = true
			if tc, ok := r["table_count"].(int); ok && tc < 3 {
				t.Errorf("expected at least 3 tables in main tenant, got %d", tc)
			}
		}
	}
	if !found {
		t.Error("main tenant not found in sys.connections")
	}
}

// TestSysUnknownTable verifies that unknown sys.* tables produce an error.
func TestSysUnknownTable(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	_, err := Execute(ctx, db, "main", mustParseSys("SELECT * FROM sys.nonexistent"))
	if err == nil {
		t.Fatal("expected error for unknown sys table")
	}
	if !strings.Contains(err.Error(), "unknown sys table") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestSysWithAlias verifies that sys.* tables support table aliases.
func TestSysWithAlias(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT s.name FROM sys.tables s WHERE s.name = 'users'"))
	if err != nil {
		t.Fatalf("SELECT sys.tables with alias failed: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
}

// TestCatalogTablesAutoPopulate verifies catalog.tables shows real tables.
func TestCatalogTablesAutoPopulate(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	// Don't register anything in catalog — it should auto-populate from real tables.
	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT name, rows FROM catalog.tables"))
	if err != nil {
		t.Fatalf("SELECT catalog.tables failed: %v", err)
	}
	if len(rs.Rows) < 3 {
		t.Fatalf("expected at least 3 tables (auto-populated), got %d", len(rs.Rows))
	}

	found := false
	for _, r := range rs.Rows {
		if r["name"] == "users" {
			found = true
		}
	}
	if !found {
		t.Fatal("users not found in catalog.tables auto-populated results")
	}
}

// TestCatalogFunctionsAutoPopulate verifies catalog.functions shows builtin functions.
func TestCatalogFunctionsAutoPopulate(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT name, function_type FROM catalog.functions"))
	if err != nil {
		t.Fatalf("SELECT catalog.functions failed: %v", err)
	}
	if len(rs.Rows) < 50 {
		t.Fatalf("expected at least 50 functions (auto-populated), got %d", len(rs.Rows))
	}

	// UPPER should be here.
	found := false
	for _, r := range rs.Rows {
		if r["name"] == "UPPER" {
			found = true
		}
	}
	if !found {
		t.Fatal("UPPER not found in catalog.functions auto-populated results")
	}
}

// TestSysMemoryFilter verifies filtering sys.memory with WHERE.
func TestSysMemoryFilter(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT value FROM sys.memory WHERE key = 'alloc_mb'"))
	if err != nil {
		t.Fatalf("SELECT sys.memory WHERE failed: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	val, ok := rs.Rows[0]["value"].(string)
	if !ok || val == "" {
		t.Error("alloc_mb value should be a non-empty string")
	}
}

// TestSysStatusFilter verifies filtering sys.status with WHERE.
func TestSysStatusFilter(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()

	rs, err := Execute(ctx, db, "main", mustParseSys("SELECT value FROM sys.status WHERE key = 'goroutines'"))
	if err != nil {
		t.Fatalf("SELECT sys.status WHERE failed: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	val, ok := rs.Rows[0]["value"].(string)
	if !ok || val == "" || val == "0" {
		t.Error("goroutines should be a positive number")
	}
}
