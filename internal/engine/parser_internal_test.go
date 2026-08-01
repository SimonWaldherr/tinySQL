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

func TestParseInsertMultipleRows(t *testing.T) {
	stmt := "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
	p := NewParser(stmt)
	parsed, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("failed to parse multi-row INSERT: %v", err)
	}
	ins, ok := parsed.(*Insert)
	if !ok {
		t.Fatalf("expected Insert statement, got %T", parsed)
	}
	if len(ins.Rows) != 2 {
		t.Fatalf("expected 2 value rows, got %d", len(ins.Rows))
	}
	if len(ins.Rows[0]) != 2 || len(ins.Rows[1]) != 2 {
		t.Fatalf("expected both value rows to have 2 expressions, first=%d second=%d", len(ins.Rows[0]), len(ins.Rows[1]))
	}
}

func TestParseScientificNumber(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1.25e-3",
		"SELECT 6E+2",
		"SELECT -4e-7",
	} {
		if _, err := NewParser(sql).ParseStatement(); err != nil {
			t.Fatalf("failed to parse scientific literal %q: %v", sql, err)
		}
	}
}
