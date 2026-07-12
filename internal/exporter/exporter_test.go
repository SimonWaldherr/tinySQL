package exporter

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func makeSample() *engine.ResultSet {
	now := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	return &engine.ResultSet{
		Cols: []string{"id", "name", "active", "created_at"},
		Rows: []engine.Row{
			{"id": 1, "name": "alice", "active": true, "created_at": now},
			{"id": 2, "name": "bob", "active": false, "created_at": now},
		},
	}
}

func TestExportCSV(t *testing.T) {
	rs := makeSample()
	var buf bytes.Buffer
	if err := ExportCSV(&buf, rs, Options{}); err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}
	out := buf.String()
	if out == "" {
		t.Fatalf("CSV output empty")
	}
	if !bytes.Contains(buf.Bytes(), []byte("id,name,active")) {
		t.Fatalf("CSV missing header: %s", out)
	}
}

func TestExportJSON(t *testing.T) {
	rs := makeSample()
	var buf bytes.Buffer
	if err := ExportJSON(&buf, rs, Options{PrettyJSON: false}); err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}
	var arr []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &arr); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(arr))
	}
}

func TestExportNDJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := ExportNDJSON(&buf, makeSample(), Options{}); err != nil {
		t.Fatalf("ExportNDJSON failed: %v", err)
	}
	dec := json.NewDecoder(&buf)
	count := 0
	for dec.More() {
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			t.Fatalf("decode NDJSON row: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 NDJSON rows, got %d", count)
	}
}

func TestExportSQL(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"id", "name", "created_at", "payload"},
		Rows: []engine.Row{
			{
				"id":         1,
				"name":       "O'Hara",
				"created_at": time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
				"payload":    []byte("a'b"),
			},
			{"id": 2, "name": nil},
		},
	}
	var buf bytes.Buffer
	if err := ExportSQL(&buf, rs, "people"); err != nil {
		t.Fatalf("ExportSQL failed: %v", err)
	}
	out := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("INSERT INTO people (id, name, created_at, payload) VALUES (1, 'O''Hara', '2020-01-02T03:04:05Z', X'612762');")) {
		t.Fatalf("SQL missing escaped row: %s", out)
	}
	if !bytes.Contains(buf.Bytes(), []byte("VALUES (2, NULL")) {
		t.Fatalf("SQL missing NULL row: %s", out)
	}
}

func TestExportBinaryValuesAreUnambiguous(t *testing.T) {
	rs := &engine.ResultSet{Cols: []string{"payload"}, Rows: []engine.Row{{"payload": []byte{0x00, 0xff}}}}

	var csvBuf bytes.Buffer
	if err := ExportCSV(&csvBuf, rs, Options{}); err != nil {
		t.Fatalf("ExportCSV: %v", err)
	}
	if !bytes.Contains(csvBuf.Bytes(), []byte("base64:AP8=")) {
		t.Fatalf("CSV BLOB = %q", csvBuf.String())
	}

	var jsonBuf bytes.Buffer
	if err := ExportJSON(&jsonBuf, rs, Options{}); err != nil {
		t.Fatalf("ExportJSON: %v", err)
	}
	if !bytes.Contains(jsonBuf.Bytes(), []byte(`"$tinysql":"blob"`)) || !bytes.Contains(jsonBuf.Bytes(), []byte(`"base64":"AP8="`)) {
		t.Fatalf("JSON BLOB envelope = %q", jsonBuf.String())
	}
}

func TestExportTableManifest(t *testing.T) {
	db := storage.NewDB()
	table := storage.NewTable("places", []storage.Column{
		{Name: "name", Type: storage.TextType, DeclaredType: "VARCHAR(80)", Affinity: storage.AffinityText, NotNull: true},
		{Name: "payload", Type: storage.BlobType},
	}, false)
	table.Rows = append(table.Rows, []any{"Bayern", []byte{0x01, 0x02}})
	if err := db.Put("default", table); err != nil {
		t.Fatalf("put: %v", err)
	}
	var buf bytes.Buffer
	if err := ExportTableManifest(&buf, db, "default", "places"); err != nil {
		t.Fatalf("ExportTableManifest: %v", err)
	}
	var manifest TableManifest
	if err := json.Unmarshal(buf.Bytes(), &manifest); err != nil {
		t.Fatalf("manifest JSON: %v", err)
	}
	if manifest.FormatVersion != 1 || manifest.TextEncoding != "utf-8" || manifest.RowCount != 1 || manifest.DataSHA256 == "" {
		t.Fatalf("manifest = %#v", manifest)
	}
	if manifest.Columns[0].DeclaredType != "VARCHAR(80)" || !manifest.Columns[0].NotNull {
		t.Fatalf("schema manifest = %#v", manifest.Columns)
	}
}

func TestExportXML(t *testing.T) {
	rs := makeSample()
	var buf bytes.Buffer
	if err := ExportXML(&buf, rs); err != nil {
		t.Fatalf("ExportXML failed: %v", err)
	}
	var xr struct {
		Rows []struct{} `xml:"row"`
	}
	if err := xml.Unmarshal(buf.Bytes(), &xr); err != nil {
		t.Fatalf("XML unmarshal failed: %v", err)
	}
	if len(xr.Rows) != 2 {
		t.Fatalf("expected 2 xml rows, got %d", len(xr.Rows))
	}
}

func TestExportGOB(t *testing.T) {
	rs := makeSample()
	var buf bytes.Buffer
	if err := ExportGOB(&buf, rs); err != nil {
		t.Fatalf("ExportGOB failed: %v", err)
	}
	dec := gob.NewDecoder(&buf)
	var got struct {
		Cols []string
		Rows []map[string]any
	}
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("gob decode failed: %v", err)
	}
	if len(got.Rows) != 2 {
		t.Fatalf("expected 2 gob rows, got %d", len(got.Rows))
	}
}
