package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestBuildSqltools(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_sqltools_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	_ = os.Remove(out)
}

// ---- Beautifier tests -------------------------------------------------------

func TestBeautify_UppercasesKeywords(t *testing.T) {
	b := NewSQLBeautifier(DefaultBeautifyOptions())
	got := b.Beautify("select * from users where id = 1")
	if !strings.Contains(got, "SELECT") {
		t.Errorf("expected SELECT keyword uppercased, got: %s", got)
	}
	if strings.Contains(got, "select") {
		t.Errorf("unexpected lowercase 'select' in output: %s", got)
	}
}

func TestBeautify_PreservesStringLiterals(t *testing.T) {
	b := NewSQLBeautifier(DefaultBeautifyOptions())
	got := b.Beautify("select * from t where name = 'hello world'")
	if !strings.Contains(got, "'hello world'") {
		t.Errorf("string literal was mangled: %s", got)
	}
}

func TestBeautify_LowercaseMode(t *testing.T) {
	opts := DefaultBeautifyOptions()
	opts.Uppercase = false
	b := NewSQLBeautifier(opts)
	got := b.Beautify("SELECT * FROM t")
	// With Uppercase=false keywords stay as-is (no forced lower).
	// At minimum the output must not be empty.
	if strings.TrimSpace(got) == "" {
		t.Error("expected non-empty output")
	}
}

func TestBeautify_Newlines(t *testing.T) {
	b := NewSQLBeautifier(DefaultBeautifyOptions())
	got := b.Beautify("SELECT a, b FROM t WHERE x=1 ORDER BY a")
	// Major keywords should each start on their own line.
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) < 2 {
		t.Errorf("expected multiple lines, got: %s", got)
	}
}

func TestBeautify_Comments(t *testing.T) {
	b := NewSQLBeautifier(DefaultBeautifyOptions())
	got := b.Beautify("-- my comment\nSELECT 1")
	if !strings.Contains(got, "-- my comment") {
		t.Errorf("comment was dropped: %s", got)
	}
}

// ---- Validator tests --------------------------------------------------------

func TestValidateSQL_ValidSelect(t *testing.T) {
	res := ValidateSQL("SELECT id, name FROM users WHERE id = 1")
	if !res.Valid {
		t.Fatalf("expected valid, got error: %s", res.Error)
	}
	if res.SQLType != "SELECT" {
		t.Errorf("expected SQLType=SELECT, got %s", res.SQLType)
	}
}

func TestValidateSQL_WarnSelectStar(t *testing.T) {
	res := ValidateSQL("SELECT * FROM users")
	if !res.Valid {
		t.Fatalf("expected valid, got error: %s", res.Error)
	}
	found := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "SELECT *") || strings.Contains(w, "columns") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected SELECT * warning, got warnings: %v", res.Warnings)
	}
}

func TestValidateSQL_WarnDeleteWithoutWhere(t *testing.T) {
	res := ValidateSQL("DELETE FROM users")
	if !res.Valid {
		t.Fatalf("expected valid parse, got: %s", res.Error)
	}
	found := false
	for _, w := range res.Warnings {
		if strings.Contains(strings.ToLower(w), "where") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected WHERE warning for DELETE, got: %v", res.Warnings)
	}
}

func TestValidateSQL_InvalidSQL(t *testing.T) {
	res := ValidateSQL("THIS IS NOT SQL AT ALL !!!!")
	if res.Valid {
		t.Error("expected invalid result for garbage input")
	}
	if res.Error == "" {
		t.Error("expected non-empty error message")
	}
}

func TestValidateSQL_Insert(t *testing.T) {
	res := ValidateSQL("INSERT INTO t (x) VALUES (1)")
	if !res.Valid {
		t.Fatalf("expected valid INSERT, got: %s", res.Error)
	}
	if res.SQLType != "INSERT" {
		t.Errorf("expected SQLType=INSERT, got %s", res.SQLType)
	}
}

func TestValidateSQL_Update(t *testing.T) {
	res := ValidateSQL("UPDATE t SET x = 1 WHERE id = 2")
	if !res.Valid {
		t.Fatalf("expected valid UPDATE, got: %s", res.Error)
	}
	if res.SQLType != "UPDATE" {
		t.Errorf("expected SQLType=UPDATE, got %s", res.SQLType)
	}
}

// ---- ExplainQuery tests -----------------------------------------------------

func TestExplainQuery_Select(t *testing.T) {
	plan, err := ExplainQuery("SELECT id FROM users WHERE id = 1 ORDER BY id LIMIT 10")
	if err != nil {
		t.Fatalf("ExplainQuery: %v", err)
	}
	if len(plan.Steps) == 0 {
		t.Fatal("expected at least one plan step")
	}

	ops := make(map[string]bool)
	for _, s := range plan.Steps {
		ops[s.Operation] = true
	}
	if !ops["TABLE SCAN"] {
		t.Error("expected TABLE SCAN step")
	}
}

func TestExplainQuery_Join(t *testing.T) {
	plan, err := ExplainQuery("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		t.Fatalf("ExplainQuery: %v", err)
	}
	ops := make(map[string]bool)
	for _, s := range plan.Steps {
		ops[s.Operation] = true
	}
	if !ops["NESTED LOOP JOIN"] {
		t.Errorf("expected NESTED LOOP JOIN step, got steps: %v", plan.Steps)
	}
}

func TestExplainQuery_Insert(t *testing.T) {
	plan, err := ExplainQuery("INSERT INTO t (x) VALUES (1)")
	if err != nil {
		t.Fatalf("ExplainQuery: %v", err)
	}
	if len(plan.Steps) == 0 {
		t.Fatal("expected at least one step")
	}
	if plan.Steps[0].Operation != "INSERT" {
		t.Errorf("expected INSERT step, got %s", plan.Steps[0].Operation)
	}
}

func TestExplainQuery_InvalidSQL(t *testing.T) {
	_, err := ExplainQuery("NOT VALID SQL")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

// ---- SchemaBrowser tests ----------------------------------------------------

func TestSchemaBrowser_ListTables(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	p := tsql.NewParser("CREATE TABLE sb_users (id INT, name TEXT)")
	st, _ := p.ParseStatement()
	if _, err := tsql.Execute(ctx, db, "default", st); err != nil {
		t.Fatalf("create: %v", err)
	}

	browser := NewSchemaBrowser(db)
	tables := browser.ListTables("default")

	found := false
	for _, tbl := range tables {
		if tbl == "sb_users" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected sb_users in list, got: %v", tables)
	}
}

func TestSchemaBrowser_DescribeTable(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	p := tsql.NewParser("CREATE TABLE sb_orders (id INT, amount FLOAT)")
	st, _ := p.ParseStatement()
	if _, err := tsql.Execute(ctx, db, "default", st); err != nil {
		t.Fatalf("create: %v", err)
	}

	browser := NewSchemaBrowser(db)
	info, err := browser.DescribeTable("default", "sb_orders")
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if info.Name != "sb_orders" {
		t.Errorf("expected table name sb_orders, got %s", info.Name)
	}
	if len(info.Columns) < 2 {
		t.Errorf("expected at least 2 columns, got %d", len(info.Columns))
	}
}

// ---- QueryHistory tests -----------------------------------------------------

func TestQueryHistory_AddAndRetrieve(t *testing.T) {
	h := NewQueryHistory(5)
	h.Add("SELECT 1", 2*time.Millisecond, 1, nil)
	h.Add("SELECT 2", 3*time.Millisecond, 2, nil)

	last := h.Last(10)
	if len(last) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(last))
	}
	if last[0].SQL != "SELECT 1" {
		t.Errorf("expected first entry to be SELECT 1, got %s", last[0].SQL)
	}
}

func TestQueryHistory_MaxSizeEviction(t *testing.T) {
	h := NewQueryHistory(3)
	for i := range 5 {
		h.Add(strings.Repeat("x", i+1), time.Millisecond, 0, nil)
	}
	last := h.Last(10)
	if len(last) > 3 {
		t.Errorf("expected at most 3 entries due to max size, got %d", len(last))
	}
}

func TestQueryHistory_Search(t *testing.T) {
	h := NewQueryHistory(10)
	h.Add("SELECT * FROM users", time.Millisecond, 5, nil)
	h.Add("INSERT INTO orders VALUES (1)", time.Millisecond, 1, nil)

	results := h.Search("users")
	if len(results) != 1 {
		t.Fatalf("expected 1 match, got %d", len(results))
	}
	if !strings.Contains(results[0].SQL, "users") {
		t.Errorf("unexpected result: %s", results[0].SQL)
	}
}

// ---- Templates tests --------------------------------------------------------

func TestCommonTemplates_NotEmpty(t *testing.T) {
	templates := CommonTemplates()
	if len(templates) == 0 {
		t.Fatal("expected at least one query template")
	}
	for _, tmpl := range templates {
		if tmpl.Name == "" {
			t.Error("template has empty name")
		}
		if tmpl.SQL == "" {
			t.Errorf("template %q has empty SQL", tmpl.Name)
		}
	}
}

func TestApplyTemplate(t *testing.T) {
	tmpl := QueryTemplate{
		Name: "test",
		SQL:  "SELECT * FROM {table} LIMIT {limit}",
		Parameters: []string{"table", "limit"},
	}
	params := map[string]string{"table": "users", "limit": "10"}
	result := ApplyTemplate(tmpl, params)
	if !strings.Contains(result, "users") {
		t.Errorf("expected 'users' in result, got: %s", result)
	}
	if !strings.Contains(result, "10") {
		t.Errorf("expected '10' in result, got: %s", result)
	}
}
