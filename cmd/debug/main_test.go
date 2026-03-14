package main

import (
	"context"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestDebugSQLFlow(t *testing.T) {
	db := tsql.NewDB()

	// Create table
	p := tsql.NewParser(`CREATE TABLE test_bool (id INT, flag BOOL)`)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if _, err := tsql.Execute(context.Background(), db, "default", st); err != nil {
		t.Fatalf("execute create error: %v", err)
	}

	// Insert rows
	p = tsql.NewParser(`INSERT INTO test_bool VALUES (1, true)`)
	st, _ = p.ParseStatement()
	if _, err := tsql.Execute(context.Background(), db, "default", st); err != nil {
		t.Fatalf("insert1 error: %v", err)
	}

	p = tsql.NewParser(`INSERT INTO test_bool VALUES (2, false)`)
	st, _ = p.ParseStatement()
	if _, err := tsql.Execute(context.Background(), db, "default", st); err != nil {
		t.Fatalf("insert2 error: %v", err)
	}

	// Select and verify
	p = tsql.NewParser(`SELECT * FROM test_bool ORDER BY id`)
	st, _ = p.ParseStatement()
	rs, err := tsql.Execute(context.Background(), db, "default", st)
	if err != nil {
		t.Fatalf("select err: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
}
