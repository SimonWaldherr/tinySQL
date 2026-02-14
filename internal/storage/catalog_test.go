package storage

import (
	"testing"
	"time"
)

func TestCatalogTableAndColumns(t *testing.T) {
	cm := NewCatalogManager()

	cols := []Column{{Name: "id", Type: IntType}, {Name: "name", Type: TextType}}
	if err := cm.RegisterTable("default", "users", cols); err != nil {
		t.Fatalf("RegisterTable failed: %v", err)
	}

	tables := cm.GetTables()
	found := false
	for _, tt := range tables {
		if tt.Name == "users" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("registered table not found in GetTables")
	}

	gotCols := cm.GetColumns("default", "users")
	if len(gotCols) != 2 || gotCols[0].Name != "id" || gotCols[1].Name != "name" {
		t.Fatalf("unexpected columns: %#v", gotCols)
	}
}

func TestCatalogJobsLifecycle(t *testing.T) {
	cm := NewCatalogManager()

	job := &CatalogJob{Name: "job1", SQLText: "SELECT 1", ScheduleType: "ONCE", Enabled: true}
	if err := cm.RegisterJob(job); err != nil {
		t.Fatalf("RegisterJob failed: %v", err)
	}

	j, err := cm.GetJob("job1")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if j.Name != "job1" {
		t.Fatalf("unexpected job name: %s", j.Name)
	}

	jobs := cm.ListJobs()
	if len(jobs) == 0 {
		t.Fatalf("expected at least one job")
	}

	// Update runtime
	now := time.Now()
	if err := cm.UpdateJobRuntime("job1", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("UpdateJobRuntime failed: %v", err)
	}

	// Delete
	if err := cm.DeleteJob("job1"); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}
	if _, err := cm.GetJob("job1"); err == nil {
		t.Fatalf("expected GetJob to fail after delete")
	}
}
