package exporter_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/exporter"
)

func facadeResultSet() *tinysql.ResultSet {
	return &tinysql.ResultSet{
		Cols: []string{"id", "name"},
		Rows: []tinysql.Row{{"id": 1, "name": "Ada"}, {"id": 2, "name": "Linus"}},
	}
}

func TestPublicExportFacadeFormats(t *testing.T) {
	rs := facadeResultSet()

	t.Run("JSON", func(t *testing.T) {
		var out bytes.Buffer
		if err := exporter.ExportJSON(&out, rs, exporter.Options{}); err != nil {
			t.Fatal(err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
			t.Fatal(err)
		}
		if len(rows) != 2 || rows[0]["name"] != "Ada" {
			t.Fatalf("JSON rows = %#v", rows)
		}
	})

	t.Run("NDJSON", func(t *testing.T) {
		var out bytes.Buffer
		if err := exporter.ExportNDJSON(&out, rs, exporter.Options{}); err != nil {
			t.Fatal(err)
		}
		if lines := strings.Split(strings.TrimSpace(out.String()), "\n"); len(lines) != 2 {
			t.Fatalf("NDJSON lines = %q", out.String())
		}
	})

	t.Run("XML", func(t *testing.T) {
		var out bytes.Buffer
		if err := exporter.ExportXML(&out, rs); err != nil {
			t.Fatal(err)
		}
		var document struct {
			Rows []struct{} `xml:"row"`
		}
		if err := xml.Unmarshal(out.Bytes(), &document); err != nil {
			t.Fatal(err)
		}
		if len(document.Rows) != 2 {
			t.Fatalf("XML rows = %d", len(document.Rows))
		}
	})

	t.Run("GOB", func(t *testing.T) {
		var out bytes.Buffer
		if err := exporter.ExportGOB(&out, rs); err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			Cols []string
			Rows []map[string]any
		}
		if err := gob.NewDecoder(&out).Decode(&decoded); err != nil {
			t.Fatal(err)
		}
		if len(decoded.Rows) != 2 || decoded.Cols[1] != "name" {
			t.Fatalf("GOB payload = %#v", decoded)
		}
	})
}

func TestPublicExportTableManifest(t *testing.T) {
	db := tinysql.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE people (id INT, name TEXT)",
		"INSERT INTO people VALUES (1, 'Ada')",
	} {
		stmt, err := tinysql.ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}

	var out bytes.Buffer
	if err := exporter.ExportTableManifest(&out, db, "default", "people"); err != nil {
		t.Fatal(err)
	}
	var manifest exporter.TableManifest
	if err := json.Unmarshal(out.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Table != "people" || manifest.RowCount != 1 || len(manifest.Columns) != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}
}
