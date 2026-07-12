//go:build shapefile

package importer

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	shp "github.com/jonas-p/go-shp"
)

func TestImportShapefileZip(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	body, err := os.ReadFile(createTestShapefileZip(t))
	if err != nil {
		t.Fatalf("read shapefile zip: %v", err)
	}
	res, err := ImportShapefileZip(ctx, db, "default", "shape_zip", bytes.NewReader(body), &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportShapefileZip: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("RowsInserted = %d, want 1", res.RowsInserted)
	}
}

func createTestShapefileZip(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	base := filepath.Join(dir, "places")
	writer, err := shp.Create(base+".shp", shp.POINT)
	if err != nil {
		t.Fatalf("create shapefile: %v", err)
	}
	if err := writer.SetFields([]shp.Field{shp.StringField("name", 32)}); err != nil {
		t.Fatalf("set fields: %v", err)
	}
	row := writer.Write(&shp.Point{X: 11.5761, Y: 48.1372})
	if err := writer.WriteAttribute(int(row), 0, "Munich"); err != nil {
		t.Fatalf("write attribute: %v", err)
	}
	writer.Close()
	if _, err := os.Stat(base + ".dbf"); err != nil {
		if renameErr := os.Rename(base+"dbf", base+".dbf"); renameErr != nil {
			t.Fatalf("finalize dbf: stat=%v rename=%v", err, renameErr)
		}
	}
	zipPath := filepath.Join(dir, "places.zip")
	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("create zip: %v", err)
	}
	zw := zip.NewWriter(zipFile)
	for _, ext := range []string{".shp", ".shx", ".dbf"} {
		body, err := os.ReadFile(base + ext)
		if err != nil {
			t.Fatalf("read %s: %v", ext, err)
		}
		w, err := zw.Create("places" + ext)
		if err != nil {
			t.Fatalf("zip create %s: %v", ext, err)
		}
		if _, err := w.Write(body); err != nil {
			t.Fatalf("zip write %s: %v", ext, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	if err := zipFile.Close(); err != nil {
		t.Fatalf("close zip file: %v", err)
	}
	return zipPath
}
