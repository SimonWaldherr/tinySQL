package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

func newBenchmarkApp(b *testing.B, rows int) *App {
	b.Helper()

	nativeDB := tinysql.NewDB()
	tsqldriver.SetDefaultDB(nativeDB)
	tenant := fmt.Sprintf("bench_%s_%d", strings.ReplaceAll(b.Name(), "/", "_"), rows)

	sqlDB, err := sql.Open("tinysql", "mem://?tenant="+tenant)
	if err != nil {
		b.Fatalf("open sql db: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(0)
	b.Cleanup(func() { sqlDB.Close() })

	tpl, err := parseTemplates()
	if err != nil {
		b.Fatalf("parse templates: %v", err)
	}
	app := newApp(nativeDB, sqlDB, tenant, tpl)

	if _, err := app.sqlDB.Exec("CREATE TABLE people (id INT, name TEXT, score FLOAT)"); err != nil {
		b.Fatalf("create table: %v", err)
	}
	for i := 1; i <= rows; i++ {
		if _, err := app.sqlDB.Exec(
			"INSERT INTO people (id, name, score) VALUES (?, ?, ?)",
			i, fmt.Sprintf("user_%04d", i), float64(i%100)+0.5,
		); err != nil {
			b.Fatalf("insert row %d: %v", i, err)
		}
	}

	return app
}

func BenchmarkAccessWebTableExport(b *testing.B) {
	for _, format := range []string{"csv", "json"} {
		b.Run(format, func(b *testing.B) {
			app := newBenchmarkApp(b, 1000)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest(http.MethodGet, "/t/people/export?format="+format, nil)
				req.SetPathValue("table", "people")
				w := httptest.NewRecorder()
				app.exportTableHandler(w, req)
				if w.Code != http.StatusOK {
					b.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
				}
			}
		})
	}
}

func BenchmarkAccessWebQueryExport(b *testing.B) {
	for _, format := range []string{"csv", "json"} {
		b.Run(format, func(b *testing.B) {
			app := newBenchmarkApp(b, 1000)
			body := fmt.Sprintf(`{"sql":"SELECT * FROM people WHERE id > 500","format":%q}`, format)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest(http.MethodPost, "/api/export", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				app.apiExportHandler(w, req)
				if w.Code != http.StatusOK {
					b.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
				}
			}
		})
	}
}
