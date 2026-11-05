package tinystore

import "testing"

func TestWriteNilTable(t *testing.T) {
	if _, err := Write(t.TempDir(), nil); err == nil {
		t.Fatal("expected nil-table error")
	}
}
