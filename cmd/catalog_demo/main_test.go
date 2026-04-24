package main

import (
	"context"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCatalogOperations(t *testing.T) {
	db := tinysql.NewDB()
	catalog := db.Catalog()

	tableCols := []storage.Column{{Name: "id"}, {Name: "name"}}
	catalog.RegisterTable("main", "users", tableCols)

	tables := catalog.GetTables()
	found := false
	for _, tbl := range tables {
		if tbl.Name == "users" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("registered table 'users' not found in catalog")
	}

	// Register a view and a function and verify no panic
	catalog.RegisterView("main", "v_users", "SELECT * FROM users")
	catalog.RegisterFunction(&storage.CatalogFunction{Name: "dummy", Schema: "main", FunctionType: "SCALAR"})

	// Register a job and verify it appears in list
	job := &storage.CatalogJob{Name: "job1", SQLText: "SELECT 1", Enabled: true}
	catalog.RegisterJob(job)
	jobs := catalog.ListJobs()
	if len(jobs) == 0 {
		t.Fatalf("expected at least one job after RegisterJob")
	}
}

func TestTinySQLExecutor(t *testing.T) {
	db := tinysql.NewDB()
	tenant := "default"
	ctx := context.Background()

	// Seed a table so the executor can query it.
	for _, sql := range []string{
		`CREATE TABLE exec_test (id INT, val TEXT)`,
		`INSERT INTO exec_test VALUES (1, 'a')`,
		`INSERT INTO exec_test VALUES (2, 'b')`,
	} {
		stmt, err := tinysql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := tinysql.Execute(ctx, db, tenant, stmt); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec := &TinySQLExecutor{db: db, tenant: tenant}

	// SELECT should succeed and return results.
	rs, err := exec.ExecuteSQL(ctx, `SELECT * FROM exec_test ORDER BY id`)
	if err != nil {
		t.Fatalf("ExecuteSQL SELECT: %v", err)
	}
	if rs == nil {
		t.Fatal("expected non-nil result for SELECT")
	}

	// DDL / DML should also succeed without error.
	if _, err := exec.ExecuteSQL(ctx, `INSERT INTO exec_test VALUES (3, 'c')`); err != nil {
		t.Fatalf("ExecuteSQL INSERT: %v", err)
	}
}

func TestFormatFirstRow(t *testing.T) {
	rs := &tinysql.ResultSet{
		Cols: []string{"cnt"},
		Rows: []tinysql.Row{{"cnt": 42}},
	}
	got := formatFirstRow(rs)
	if got == "" {
		t.Fatal("formatFirstRow returned empty string")
	}
}
