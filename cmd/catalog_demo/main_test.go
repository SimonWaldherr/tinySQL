package main

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCatalogOperations(t *testing.T) {
	db := storage.NewDB()
	catalog := db.Catalog()

	tableCols := []storage.Column{{Name: "id"}, {Name: "name"}}
	catalog.RegisterTable("main", "users", tableCols)

	tables := catalog.GetTables()
	found := false
	for _, t := range tables {
		if t.Name == "users" {
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
