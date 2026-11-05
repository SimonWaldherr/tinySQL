package tinystore

import (
	"bytes"
	tsql "github.com/SimonWaldherr/tinySQL"
	"os"
	"path/filepath"
	"testing"
)

// Frozen version-one fixture checks both reading and byte-for-byte writing,
// independently of round trips.
func TestVersionOneCompatibility(t *testing.T) {
	if Ext != ".tsnap" || magic != "TINY01" {
		t.Fatal("unexpected snapshot format identifier")
	}
	path := filepath.Join("testdata", FileName("legacy"))
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	want := table("legacy", []tsql.Column{{Name: "text", Type: tsql.TextType}, {Name: "int", Type: tsql.IntType}, {Name: "float", Type: tsql.FloatType}}, [][]any{{"same", -123, 1.5}, {nil, nil, nil}, {"same", 256, -2.25}, {"other", 0, 0.0}})
	sameTable(t, want, got)
	dir := t.TempDir()
	if _, err := Write(dir, want); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(filepath.Join(dir, FileName("legacy")))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, current) {
		t.Fatal("version-one encoding changed")
	}
}
