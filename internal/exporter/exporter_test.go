package exporter

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
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
