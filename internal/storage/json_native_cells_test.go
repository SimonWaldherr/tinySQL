package storage

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
)

func TestJSONNumberNormalizationRejectsOverflow(t *testing.T) {
	for _, value := range []any{json.Number("1e999"), []any{json.Number("1e999")}, map[string]any{"n": json.Number("1e999")}} {
		if _, err := normalizeJSONNumbers(value); err == nil {
			t.Fatal("overflow accepted")
		}
	}
}

func TestJSONNativeIntegerAndBlobRoundTrip(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires 64-bit INT")
	}
	large, err := strconv.Atoi("9007199254740993")
	if err != nil {
		t.Fatal(err)
	}
	backend, err := NewJSONBackend(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	table := NewTable("native", []Column{{Name: "tick", Type: IntType}, {Name: "payload", Type: BlobType}}, false)
	table.Rows = [][]any{{large, []byte{0, 255, 128}}, {1, []byte{}}, {2, nil}}
	if err := backend.SaveTable("default", table); err != nil {
		t.Fatal(err)
	}
	loaded, err := backend.LoadTable("default", "native")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Rows[0][0] != large || !bytes.Equal(loaded.Rows[0][1].([]byte), []byte{0, 255, 128}) {
		t.Fatal(loaded.Rows)
	}
	if loaded.Rows[1][1] == nil || len(loaded.Rows[1][1].([]byte)) != 0 || loaded.Rows[2][1] != nil {
		t.Fatal("empty BLOB/NULL changed")
	}
}
