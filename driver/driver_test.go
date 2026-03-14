package driver

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestOpenInMemory(t *testing.T) {
	db, err := OpenInMemory("")
	if err != nil {
		t.Fatalf("OpenInMemory error: %v", err)
	}
	defer db.Close()

	// Perform a simple ping via database/sql
	if err := db.Ping(); err != nil {
		t.Fatalf("db.Ping failed: %v", err)
	}

	// Also verify OpenWithDB accepts an existing storage.DB
	sdb := storage.NewDB()
	sqlDB, err := OpenWithDB(sdb)
	if err != nil {
		t.Fatalf("OpenWithDB error: %v", err)
	}
	sqlDB.Close()
}
