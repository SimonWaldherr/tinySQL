package tinysql

import (
	"context"
	"strings"
	"testing"
)

func TestBuildAgentContext(t *testing.T) {
	ctx := context.Background()
	db := NewDB()

	for _, sql := range []string{
		"CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)",
		"CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT FOREIGN KEY REFERENCES departments(id))",
		"INSERT INTO departments VALUES (1, 'Engineering')",
		"INSERT INTO employees VALUES (1, 'Ada', 1)",
	} {
		stmt, err := ParseSQL(sql)
		if err != nil {
			t.Fatalf("ParseSQL(%q): %v", sql, err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("Execute(%q): %v", sql, err)
		}
	}

	profile, err := BuildAgentContext(ctx, db, "default", AgentContextConfig{
		MaxTables:          2,
		MaxColumnsPerTable: 2,
		MaxRelations:       2,
		MaxFunctions:       3,
		MaxViews:           2,
		MaxTriggers:        2,
		MaxJobs:            2,
		MaxConnections:     2,
		MaxChars:           5000,
	})
	if err != nil {
		t.Fatalf("BuildAgentContext: %v", err)
	}

	for _, want := range []string{
		"tinySQL agent profile",
		"version:",
		"tables(",
		"departments",
		"employees",
		"relations(",
		"employees.dept_id -> departments.id",
		"connections(",
		"features:",
		"gaps:",
	} {
		if !strings.Contains(profile, want) {
			t.Fatalf("expected profile to contain %q, got:\n%s", want, profile)
		}
	}

	if len(profile) > 5000 {
		t.Fatalf("expected bounded profile, got %d chars", len(profile))
	}
}

func TestDefaultAgentContextConfig(t *testing.T) {
	cfg := DefaultAgentContextConfig().normalized()
	if cfg.MaxTables <= 0 || cfg.MaxChars <= 0 {
		t.Fatalf("expected positive defaults, got %+v", cfg)
	}
}
