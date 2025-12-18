package importer

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSanitizeTableName(t *testing.T) {
	if sanitizeTableName("123-foo.bar") != "_foo_bar" {
		t.Fatalf("sanitizeTableName failed: got %q", sanitizeTableName("123-foo.bar"))
	}
	if sanitizeTableName("!!!") != "___" {
		t.Fatalf("sanitizeTableName unexpected result: %q", sanitizeTableName("!!!"))
	}
}

func TestImportFile_JSONAndOpenFile(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	// Create temp JSON file (array of objects)
	dir := t.TempDir()
	fn := filepath.Join(dir, "data.json")
	f, err := os.Create(fn)
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	_, _ = f.WriteString("[{\"id\":1,\"name\":\"A\"},{\"id\":2,\"name\":\"B\"}]")
	f.Close()

	res, err := ImportFile(ctx, db, "default", "", fn, &ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		t.Fatalf("ImportFile JSON failed: %v", err)
	}
	if res.RowsInserted != 2 {
		t.Fatalf("expected 2 rows inserted, got %d", res.RowsInserted)
	}

	// Test OpenFile convenience
	db2, tbl, err := OpenFile(ctx, fn, &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if tbl == "" || db2 == nil {
		t.Fatalf("OpenFile returned invalid results")
	}
}

func TestImportByContent_CSVDetection(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	dir := t.TempDir()
	fn := filepath.Join(dir, "data.auto")
	_ = os.WriteFile(fn, []byte("a,b\n1,2\n"), 0644)

	f, err := os.Open(fn)
	if err != nil {
		t.Fatalf("open temp: %v", err)
	}
	defer f.Close()

	res, err := importByContent(ctx, db, "default", "auto_table", f, &ImportOptions{CreateTable: true, HeaderMode: "present"})
	if err != nil {
		t.Fatalf("importByContent failed: %v", err)
	}
	if res.RowsInserted == 0 {
		t.Fatalf("expected rows inserted, got 0")
	}
}

func TestImportFile_XMLError(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	dir := t.TempDir()
	fn := filepath.Join(dir, "data.xml")
	_ = os.WriteFile(fn, []byte("<root></root>"), 0644)

	if _, err := ImportFile(ctx, db, "default", "", fn, &ImportOptions{}); err == nil {
		t.Fatalf("expected ImportFile to return error for XML import (not implemented)")
	}
}

func TestImportCSV_Gzip(t *testing.T) {
	// create gzipped CSV content
	var raw bytes.Buffer
	raw.WriteString("id,name\n1,A\n")
	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	_, _ = gw.Write(raw.Bytes())
	gw.Close()

	ctx := context.Background()
	db := storage.NewDB()
	res, err := ImportCSV(ctx, db, "default", "gtable", bytes.NewReader(gz.Bytes()), &ImportOptions{CreateTable: true, HeaderMode: "present"})
	if err != nil {
		t.Fatalf("ImportCSV gzip failed: %v", err)
	}
	if res.RowsInserted == 0 {
		t.Fatalf("expected rows inserted from gzip CSV")
	}
}
