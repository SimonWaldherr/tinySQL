package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestParsePointerReferencesConstraint(t *testing.T) {
	stmt := "CREATE TABLE pointer_test (id INT, ptr POINTER REFERENCES parent)"
	p := NewParser(stmt)
	parsed, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("failed to parse pointer REFERENCES: %v", err)
	}
	create, ok := parsed.(*CreateTable)
	if !ok {
		t.Fatalf("expected CreateTable, got %T", parsed)
	}
	if len(create.Cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(create.Cols))
	}
	ptrCol := create.Cols[1]
	if ptrCol.Type != storage.PointerType {
		t.Fatalf("expected POINTER type, got %v", ptrCol.Type)
	}
	if ptrCol.PointerTable != "parent" {
		t.Fatalf("expected pointer table 'parent', got %q", ptrCol.PointerTable)
	}
}
