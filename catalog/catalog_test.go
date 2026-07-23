package catalog

import (
	"context"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestCatalogWrappers(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	for _, sql := range []string{
		"CREATE TABLE people (id INT, name TEXT)",
		"CREATE VIEW people_view AS SELECT id, name FROM people",
	} {
		stmt, err := tinysql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := tinysql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	objects, err := ListObjects(ctx, db, "default")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	var foundView bool
	for _, obj := range objects {
		if obj.Name == "people_view" && strings.Contains(obj.Type, "VIEW") {
			foundView = true
		}
	}
	if !foundView {
		t.Fatalf("expected people_view in objects: %#v", objects)
	}

	columns, err := ListColumns(ctx, db, "default")
	if err != nil {
		t.Fatalf("ListColumns: %v", err)
	}
	if len(columns) != 2 || columns[0].TableName != "people" || columns[0].Name != "id" || columns[1].Name != "name" {
		t.Fatalf("unexpected columns: %#v", columns)
	}

	profile, err := BuildAgentContext(ctx, db, "default", DefaultAgentContextConfig())
	if err != nil {
		t.Fatalf("BuildAgentContext: %v", err)
	}
	if !strings.Contains(profile, "people_view") {
		t.Fatalf("expected view in agent context: %s", profile)
	}
}
