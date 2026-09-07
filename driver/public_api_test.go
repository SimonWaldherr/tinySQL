package driver_test

import (
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/driver"
)

// This test is deliberately in driver_test: it proves an external module can
// embed the public tinySQL DB without naming an internal/storage type.
func TestOpenWithPublicTinySQLDB(t *testing.T) {
	native := tinysql.NewDB()
	defer native.Close()
	db, err := driver.OpenWithDB(native)
	if err != nil {
		t.Fatalf("OpenWithDB: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
