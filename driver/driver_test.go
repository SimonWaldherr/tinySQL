package driver

import (
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
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

	// Also verify OpenWithDB accepts a public tinySQL DB without an external
	// application importing the internal/storage implementation package.
	sdb := tinysql.NewDB()
	sqlDB, err := OpenWithDB(sdb)
	if err != nil {
		t.Fatalf("OpenWithDB error: %v", err)
	}
	sqlDB.Close()
}
