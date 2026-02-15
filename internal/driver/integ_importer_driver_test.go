package driver

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/importer"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestImporterDriverEndToEnd wires importer -> storage.DB -> database/sql driver
// to verify data imported by the importer is queryable via the registered
// `tinysql` database/sql driver when `SetDefaultDB` is used.
func TestImporterDriverEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	csv := `id,name
1,Alice
2,Bob
3,Charlie`
	if _, err := importer.ImportCSV(ctx, db, "default", "people", strings.NewReader(csv), &importer.ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}
	// Verify import result directly from storage
	tbl, err := db.Get("default", "people")
	if err != nil {
		t.Fatalf("db.Get failed: %v", err)
	}
	if len(tbl.Rows) != 3 {
		t.Fatalf("expected 3 rows in storage.DB after import, got %d", len(tbl.Rows))
	}

	// Make driver use this DB instance
	SetDefaultDB(db)

	sdb, err := sql.Open("tinysql", "")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer sdb.Close()

	// Query count via database/sql
	var cnt int
	if err := sdb.QueryRow("SELECT COUNT(*) as c FROM people").Scan(&cnt); err != nil {
		t.Fatalf("QueryRow failed: %v", err)
	}
	if cnt != 3 {
		t.Fatalf("expected 3 rows via database/sql, got %d", cnt)
	}
}
