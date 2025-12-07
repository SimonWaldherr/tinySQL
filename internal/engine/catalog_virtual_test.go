package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCatalogVirtualTables(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	cat := db.Catalog()

	cols := []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "email", Type: storage.TextType}}
	if err := cat.RegisterTable("main", "users", cols); err != nil {
		t.Fatalf("RegisterTable failed: %v", err)
	}
	if err := cat.RegisterView("main", "active_users", "SELECT * FROM users WHERE active = true"); err != nil {
		t.Fatalf("RegisterView failed: %v", err)
	}
	if err := cat.RegisterFunction(&storage.CatalogFunction{Schema: "main", Name: "fn_custom", FunctionType: "SCALAR", ReturnType: "INT", Language: "BUILTIN", Description: "desc"}); err != nil {
		t.Fatalf("RegisterFunction failed: %v", err)
	}
	if err := cat.RegisterJob(&storage.CatalogJob{Name: "job1", SQLText: "SELECT 1", ScheduleType: "ONCE", Enabled: true}); err != nil {
		t.Fatalf("RegisterJob failed: %v", err)
	}

	// Query catalog.tables
	rs, err := Execute(ctx, db, "main", mustParse("SELECT name FROM catalog.tables"))
	if err != nil {
		t.Fatalf("SELECT catalog.tables failed: %v", err)
	}
	if len(rs.Rows) == 0 {
		t.Fatalf("expected at least one table in catalog.tables")
	}

	// Query catalog.functions
	rsf, err := Execute(ctx, db, "main", mustParse("SELECT name, function_type FROM catalog.functions"))
	if err != nil {
		t.Fatalf("SELECT catalog.functions failed: %v", err)
	}
	found := false
	for _, r := range rsf.Rows {
		if v, ok := r["name"]; ok && v == "fn_custom" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected fn_custom in catalog.functions")
	}

	// Query catalog.columns for users
	rsc, err := Execute(ctx, db, "main", mustParse("SELECT table_name, name FROM catalog.columns WHERE table_name = 'users'"))
	if err != nil {
		t.Fatalf("SELECT catalog.columns failed: %v", err)
	}
	if len(rsc.Rows) == 0 {
		t.Fatalf("expected columns for users in catalog.columns")
	}

	// Query catalog.jobs
	rsj, err := Execute(ctx, db, "main", mustParse("SELECT name, enabled FROM catalog.jobs WHERE enabled = true"))
	if err != nil {
		t.Fatalf("SELECT catalog.jobs failed: %v", err)
	}
	if len(rsj.Rows) == 0 {
		t.Fatalf("expected enabled jobs in catalog.jobs")
	}

	// Query catalog.views
	rsv, err := Execute(ctx, db, "main", mustParse("SELECT name, sql_text FROM catalog.views"))
	if err != nil {
		t.Fatalf("SELECT catalog.views failed: %v", err)
	}
	if len(rsv.Rows) == 0 {
		t.Fatalf("expected views in catalog.views")
	}
}
