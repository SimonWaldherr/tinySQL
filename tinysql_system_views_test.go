package tinysql

import (
	"context"
	"testing"
)

func TestQuotedQualifiedSystemViews(t *testing.T) {
	ctx := context.Background()
	db := NewDB()

	create, err := ParseSQL("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("parse create: %v", err)
	}
	if _, err := Execute(ctx, db, "default", create); err != nil {
		t.Fatalf("execute create: %v", err)
	}

	queries := []string{
		`SELECT * FROM "sys"."constraints" LIMIT 10`,
		`SELECT * FROM "catalog"."columns" LIMIT 10`,
	}
	for _, query := range queries {
		stmt, err := ParseSQL(query)
		if err != nil {
			t.Fatalf("parse %s: %v", query, err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %s: %v", query, err)
		}
	}
}
