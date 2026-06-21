package storage

import (
	"path/filepath"
	"testing"
)

func TestDBHealthCheckAndIdempotentCloseWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.gob")
	db, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: path})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}

	health := db.HealthCheck()
	if !health.OK {
		t.Fatalf("health OK = false, error=%q", health.Error)
	}
	if !health.WALActive || health.Mode != ModeWAL || health.ModeName != "wal" {
		t.Fatalf("unexpected WAL health: %#v", health)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}

	health = db.HealthCheck()
	if health.OK || !health.Closed || health.LastCloseAt.IsZero() {
		t.Fatalf("closed health mismatch: %#v", health)
	}
}

func TestWALRecoveryStatusAfterOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.gob")
	db, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: path})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}

	before := NewDB()
	table := makeTestTable("events", 2)
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	changes := CollectWALChanges(before, db)
	if len(changes) == 0 {
		t.Fatal("expected WAL changes")
	}
	if _, err := db.WAL().LogTransaction(changes); err != nil {
		t.Fatalf("LogTransaction failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	recovered, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: path})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer recovered.Close()

	got, err := recovered.Get("default", "events")
	if err != nil {
		t.Fatalf("Get recovered table failed: %v", err)
	}
	if len(got.Rows) != 2 {
		t.Fatalf("recovered rows = %d, want 2", len(got.Rows))
	}
	health := recovered.HealthCheck()
	if health.Recovery.RecoveredTransactions != 1 {
		t.Fatalf("recovered transactions = %d, want 1", health.Recovery.RecoveredTransactions)
	}
	if health.Recovery.Path == "" || health.Recovery.RecoveredAt.IsZero() {
		t.Fatalf("missing recovery metadata: %#v", health.Recovery)
	}
}

func TestAdvancedWALCloseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advanced.wal")
	db, err := OpenDB(StorageConfig{Mode: ModeAdvancedWAL, Path: path})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	if !db.HealthCheck().AdvancedWALActive {
		t.Fatal("expected advanced WAL to be active")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func TestRestartJobScheduler(t *testing.T) {
	db := NewDB()
	exec := &schedulerTestExecutor{}
	if err := db.StartJobScheduler(exec); err != nil {
		t.Fatalf("StartJobScheduler failed: %v", err)
	}
	first := db.JobScheduler()
	if first == nil {
		t.Fatal("expected scheduler")
	}

	if err := db.RestartJobScheduler(exec); err != nil {
		t.Fatalf("RestartJobScheduler failed: %v", err)
	}
	second := db.JobScheduler()
	if second == nil {
		t.Fatal("expected scheduler after restart")
	}
	if second == first {
		t.Fatal("expected restart to replace scheduler instance")
	}
	db.StopJobScheduler()
}
