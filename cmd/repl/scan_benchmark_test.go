package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
)

func replBenchmarkDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec("CREATE TABLE repl_scan (id INT, label TEXT, payload BLOB)"); err != nil {
		b.Fatal(err)
	}
	values := make([]string, 1024)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,'row%d',X'000102')", i, i)
	}
	if _, err := db.Exec("INSERT INTO repl_scan VALUES " + strings.Join(values, ",")); err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkREPLReadRows(b *testing.B) {
	db := replBenchmarkDB(b)
	b.ReportAllocs()
	for b.Loop() {
		rows, err := db.Query("SELECT id,label,payload FROM repl_scan")
		if err != nil {
			b.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			b.Fatal(err)
		}
		out, err := rowsToSlice(rows, cols)
		rows.Close()
		if err != nil || len(out) != 1024 {
			b.Fatalf("rows: %d / %v", len(out), err)
		}
	}
}

func BenchmarkREPLRenderTable(b *testing.B) {
	db := replBenchmarkDB(b)
	device, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = device.Close() })
	for _, format := range []string{"table", "markdown"} {
		b.Run(format, func(b *testing.B) {
			previous := os.Stdout
			os.Stdout = device
			b.Cleanup(func() { os.Stdout = previous })
			b.ReportAllocs()
			for b.Loop() {
				rows, err := db.Query("SELECT id,label,payload FROM repl_scan")
				if err != nil {
					b.Fatal(err)
				}
				cols, err := rows.Columns()
				if err != nil {
					_ = rows.Close()
					b.Fatal(err)
				}
				printRows(rows, cols, format)
				if err := rows.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
