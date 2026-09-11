package main

import (
	"context"
	"database/sql"
	tinysql "github.com/SimonWaldherr/tinySQL"
	"path/filepath"
	"testing"
)

func BenchmarkExternalImport(b *testing.B) {
	src, err := sql.Open("sqlite", filepath.Join(b.TempDir(), "source.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer src.Close()
	if _, err = src.Exec(`CREATE TABLE source (id INTEGER, name TEXT, amount REAL)`); err != nil {
		b.Fatal(err)
	}
	if _, err = src.Exec(`WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<5000) INSERT INTO source SELECT n,'customer',12.5 FROM seq`); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		db := tinysql.NewDB()
		stats, err := importFromExternal(db, context.Background(), "default", src, `SELECT * FROM source`, "target")
		db.Close()
		if err != nil || stats.Imported != 5000 || stats.Skipped != 0 {
			b.Fatalf("%+v %v", stats, err)
		}
	}
}
