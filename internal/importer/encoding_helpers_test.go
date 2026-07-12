package importer

import (
	"bytes"
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDetectEncodingAndDecodeUTF16(t *testing.T) {
	// UTF-8 BOM
	b := []byte{0xEF, 0xBB, 0xBF, 'a', 'b'}
	enc, has := detectEncoding(b)
	if enc != "utf-8-bom" || !has {
		t.Fatalf("detectEncoding utf8-bom failed: %v %v", enc, has)
	}

	// UTF-16LE BOM
	b2 := []byte{0xFF, 0xFE, 0x61, 0x00, 0x62, 0x00}
	enc2, has2 := detectEncoding(b2)
	if enc2 != "utf-16le" || has2 {
		t.Fatalf("detectEncoding utf16le failed: %v %v", enc2, has2)
	}

	// decodeUTF16All
	utf16le := []byte{0xFF, 0xFE, 0x61, 0x00, 0x62, 0x00}
	out, err := decodeUTF16All(utf16le, false)
	if err != nil {
		t.Fatalf("decodeUTF16All failed: %v", err)
	}
	if string(out) != "ab" {
		t.Fatalf("decodeUTF16All result unexpected: %q", string(out))
	}
}

func TestImportCSVExplicitLegacyEncoding(t *testing.T) {
	db := storage.NewDB()
	data := []byte("name\nM\xfcnchen\n")
	result, err := ImportCSV(context.Background(), db, "default", "cities", bytes.NewReader(data), &ImportOptions{
		CreateTable: true,
		HeaderMode:  "present",
		Encoding:    "iso-8859-1",
	})
	if err != nil {
		t.Fatalf("ImportCSV ISO-8859-1: %v", err)
	}
	if result.Encoding != "iso-8859-1" {
		t.Fatalf("encoding = %q", result.Encoding)
	}
	table, err := db.Get("default", "cities")
	if err != nil || table.Rows[0][0] != "München" {
		t.Fatalf("decoded row = %#v, err=%v", table.Rows, err)
	}
}

func TestImportCSVRejectsInvalidUTF8(t *testing.T) {
	db := storage.NewDB()
	_, err := ImportCSV(context.Background(), db, "default", "bad", bytes.NewReader([]byte("name\n\xff\n")), &ImportOptions{
		CreateTable: true,
		HeaderMode:  "present",
	})
	if err == nil {
		t.Fatal("invalid UTF-8 import unexpectedly succeeded")
	}
}

func TestCandidateDelims(t *testing.T) {
	if got := candidateDelims([]rune{',', 0, ';'}); len(got) == 0 {
		t.Fatalf("candidateDelims returned empty")
	}
}
