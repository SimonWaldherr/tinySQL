package exporter

import (
	"bytes"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestExportCSVAndTSV(t *testing.T) {
	rs := &tinysql.ResultSet{
		Cols: []string{"id", "name"},
		Rows: []tinysql.Row{
			{"id": 1, "name": "Ada"},
		},
	}

	var csv bytes.Buffer
	if err := ExportCSV(&csv, rs, Options{}); err != nil {
		t.Fatalf("ExportCSV: %v", err)
	}
	if !strings.Contains(csv.String(), "id,name") || !strings.Contains(csv.String(), "1,Ada") {
		t.Fatalf("unexpected CSV: %q", csv.String())
	}

	var tsv bytes.Buffer
	if err := ExportTSV(&tsv, rs, Options{}); err != nil {
		t.Fatalf("ExportTSV: %v", err)
	}
	if !strings.Contains(tsv.String(), "id\tname") || !strings.Contains(tsv.String(), "1\tAda") {
		t.Fatalf("unexpected TSV: %q", tsv.String())
	}
}

func TestExportSQL(t *testing.T) {
	rs := &tinysql.ResultSet{
		Cols: []string{"id", "name"},
		Rows: []tinysql.Row{
			{"id": 1, "name": "O'Hara"},
		},
	}
	var sql bytes.Buffer
	if err := ExportSQL(&sql, rs, "people"); err != nil {
		t.Fatalf("ExportSQL: %v", err)
	}
	if got := sql.String(); !strings.Contains(got, "INSERT INTO people (id, name) VALUES (1, 'O''Hara');") {
		t.Fatalf("unexpected SQL: %q", got)
	}
}
