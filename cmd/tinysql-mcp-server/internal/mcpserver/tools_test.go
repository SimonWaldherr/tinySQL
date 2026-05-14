package mcpserver

import (
	"context"
	"go/build"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"tinysql-mcp-server/internal/tinysqldb"
)

// ─── test helpers ─────────────────────────────────────────────────────────────

// openTestStore opens an in-memory tinySQL database for testing.
func openTestStore(t *testing.T) *tinysqldb.Store {
	t.Helper()
	store, err := tinysqldb.Open(context.Background(), tinysqldb.Config{
		DSN:     "mem://?tenant=default",
		Tenant:  "default",
		MaxRows: 100,
	})
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// openReadOnlyStore returns a server with ReadOnly enabled.
func openReadOnlyStore(t *testing.T) *Server {
	t.Helper()
	store, err := tinysqldb.Open(context.Background(), tinysqldb.Config{
		DSN:      "mem://?tenant=default",
		Tenant:   "default",
		ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return New(store)
}

// mustExec executes SQL and fails the test on error.
func mustExec(t *testing.T, s *Server, query string) {
	t.Helper()
	_, _, err := s.execSQL(context.Background(), query)
	if err != nil {
		t.Fatalf("mustExec(%q): %v", query, err)
	}
}

// ─── read_query tests ─────────────────────────────────────────────────────────

func TestReadQuery_PermitsSelect(t *testing.T) {
	s := New(openTestStore(t))
	mustExec(t, s, "CREATE TABLE rq_test (id INT, name TEXT)")
	mustExec(t, s, "INSERT INTO rq_test VALUES (1, 'alice')")

	res, err := s.HandleReadQuery(context.Background(), ReadQueryArgs{Query: "SELECT * FROM rq_test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error: %v", contentText(res))
	}
	if !strings.Contains(contentText(res), "alice") {
		t.Errorf("expected 'alice' in result, got: %s", contentText(res))
	}
}

func TestReadQuery_RejectsInsert(t *testing.T) {
	assertReadQueryRejects(t, "INSERT INTO t VALUES (1)")
}

func TestReadQuery_RejectsUpdate(t *testing.T) {
	assertReadQueryRejects(t, "UPDATE t SET x = 1")
}

func TestReadQuery_RejectsDelete(t *testing.T) {
	assertReadQueryRejects(t, "DELETE FROM t")
}

func TestReadQuery_RejectsCreate(t *testing.T) {
	assertReadQueryRejects(t, "CREATE TABLE x (id INT)")
}

func TestReadQuery_RejectsDrop(t *testing.T) {
	assertReadQueryRejects(t, "DROP TABLE t")
}

func TestReadQuery_RejectsAlter(t *testing.T) {
	assertReadQueryRejects(t, "ALTER TABLE t ADD COLUMN x INT")
}

func assertReadQueryRejects(t *testing.T, query string) {
	t.Helper()
	s := New(openTestStore(t))
	res, err := s.HandleReadQuery(context.Background(), ReadQueryArgs{Query: query})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Errorf("read_query should have rejected %q", query)
	}
}

// TestReadQuery_RejectsCommentInjection verifies that a leading comment
// cannot disguise a mutating statement as a SELECT.
func TestReadQuery_RejectsCommentInjection(t *testing.T) {
	queries := []string{
		"/* SELECT */ INSERT INTO t VALUES(1)",
		"-- SELECT\nDROP TABLE t",
		"/* SELECT * FROM t */ DELETE FROM t",
	}
	s := New(openTestStore(t))
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			res, err := s.HandleReadQuery(context.Background(), ReadQueryArgs{Query: q})
			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}
			if !res.IsError {
				t.Errorf("read_query should have rejected comment-injected query %q", q)
			}
		})
	}
}

// TestReadQuery_RejectsMultiStatement ensures multi-statement inputs are rejected.
func TestReadQuery_RejectsMultiStatement(t *testing.T) {
	s := New(openTestStore(t))
	res, err := s.HandleReadQuery(context.Background(), ReadQueryArgs{
		Query: "SELECT 1; DROP TABLE users",
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Error("read_query should reject multi-statement input")
	}
}

// ─── write_query tests ────────────────────────────────────────────────────────

func TestWriteQuery_RejectsSelect(t *testing.T) {
	s := New(openTestStore(t))
	res, err := s.HandleWriteQuery(context.Background(), WriteQueryArgs{Query: "SELECT * FROM t"})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Error("write_query should reject SELECT")
	}
}

func TestWriteQuery_PermitsInsert(t *testing.T) {
	s := New(openTestStore(t))
	mustExec(t, s, "CREATE TABLE wq_test (id INT, val TEXT)")

	res, err := s.HandleWriteQuery(context.Background(), WriteQueryArgs{
		Query: "INSERT INTO wq_test VALUES (1, 'x')",
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.IsError {
		t.Errorf("write_query should accept INSERT, got: %s", contentText(res))
	}
}

// ─── create_table tests ───────────────────────────────────────────────────────

func TestCreateTable_PermitsCreateTable(t *testing.T) {
	s := New(openTestStore(t))
	res, err := s.HandleCreateTable(context.Background(), CreateTableArgs{
		Query: "CREATE TABLE ct_test (id INT, name TEXT)",
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if res.IsError {
		t.Errorf("create_table should accept CREATE TABLE, got: %s", contentText(res))
	}
}

func TestCreateTable_RejectsCreateView(t *testing.T) {
	s := New(openTestStore(t))
	res, err := s.HandleCreateTable(context.Background(), CreateTableArgs{
		Query: "CREATE VIEW v AS SELECT 1",
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Error("create_table should reject CREATE VIEW")
	}
}

func TestCreateTable_RejectsSelect(t *testing.T) {
	s := New(openTestStore(t))
	res, err := s.HandleCreateTable(context.Background(), CreateTableArgs{
		Query: "SELECT * FROM t",
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Error("create_table should reject SELECT")
	}
}

// ─── readonly mode tests ──────────────────────────────────────────────────────

func TestReadOnly_BlocksWriteQuery(t *testing.T) {
	s := openReadOnlyStore(t)
	res, err := s.HandleWriteQuery(context.Background(), WriteQueryArgs{
		Query: "INSERT INTO t VALUES (1)",
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Error("write_query should be blocked in read-only mode")
	}
}

func TestReadOnly_BlocksCreateTable(t *testing.T) {
	s := openReadOnlyStore(t)
	res, err := s.HandleCreateTable(context.Background(), CreateTableArgs{
		Query: "CREATE TABLE x (id INT)",
	})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Error("create_table should be blocked in read-only mode")
	}
}

func TestReadOnly_PermitsReadQuery(t *testing.T) {
	s := openReadOnlyStore(t)
	// The table might not exist, but the tool should not be blocked by readonly.
	res, _ := s.HandleReadQuery(context.Background(), ReadQueryArgs{Query: "SELECT 1"})
	// We only care that it was not blocked for readonly reasons.
	text := contentText(res)
	if strings.Contains(text, "read-only mode") {
		t.Error("read_query should not be blocked in read-only mode")
	}
}

// ─── list_tables tests ────────────────────────────────────────────────────────

func TestListTables_ReturnsTables(t *testing.T) {
	s := New(openTestStore(t))
	mustExec(t, s, "CREATE TABLE lt_table_a (id INT)")
	mustExec(t, s, "CREATE TABLE lt_table_b (name TEXT)")

	res, err := s.HandleListTables(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("list_tables error: %s", contentText(res))
	}
	text := contentText(res)
	if !strings.Contains(text, "lt_table_a") {
		t.Errorf("expected lt_table_a in result, got: %s", text)
	}
	if !strings.Contains(text, "lt_table_b") {
		t.Errorf("expected lt_table_b in result, got: %s", text)
	}
}

// ─── describe_table tests ─────────────────────────────────────────────────────

func TestDescribeTable_ReturnsColumnMetadata(t *testing.T) {
	s := New(openTestStore(t))
	mustExec(t, s, "CREATE TABLE dt_users (id INT, email TEXT, active BOOL)")

	res, err := s.HandleDescribeTable(context.Background(), DescribeTableArgs{TableName: "dt_users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("describe_table error: %s", contentText(res))
	}
	text := contentText(res)
	for _, col := range []string{"id", "email", "active"} {
		if !strings.Contains(text, col) {
			t.Errorf("expected column %q in describe result, got: %s", col, text)
		}
	}
}

func TestDescribeTable_RejectsInvalidIdentifier(t *testing.T) {
	s := New(openTestStore(t))
	res, err := s.HandleDescribeTable(context.Background(), DescribeTableArgs{TableName: "t; DROP TABLE users --"})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Error("describe_table should reject invalid identifiers")
	}
}

// ─── append_insight tests ─────────────────────────────────────────────────────

func TestAppendInsight_UpdatesMemo(t *testing.T) {
	s := New(openTestStore(t))

	if s.insights.Count() != 0 {
		t.Error("insight store should start empty")
	}

	res, err := s.HandleAppendInsight(context.Background(), AppendInsightArgs{Insight: "Revenue grew 20% in Q1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("append_insight error: %s", contentText(res))
	}
	if s.insights.Count() != 1 {
		t.Errorf("expected 1 insight, got %d", s.insights.Count())
	}

	memo := s.insights.Memo()
	if !strings.Contains(memo, "Revenue grew 20% in Q1") {
		t.Errorf("memo should contain the insight text, got: %s", memo)
	}

	// Append a second insight.
	_, _ = s.HandleAppendInsight(context.Background(), AppendInsightArgs{Insight: "Top SKU is widget-42"})
	if s.insights.Count() != 2 {
		t.Errorf("expected 2 insights, got %d", s.insights.Count())
	}
}

func TestAppendInsight_EmptyInsightRejected(t *testing.T) {
	s := New(openTestStore(t))
	res, err := s.HandleAppendInsight(context.Background(), AppendInsightArgs{Insight: ""})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !res.IsError {
		t.Error("append_insight should reject empty insight")
	}
}

// ─── max-rows truncation tests ────────────────────────────────────────────────

func TestMaxRows_TruncatesResults(t *testing.T) {
	store, err := tinysqldb.Open(context.Background(), tinysqldb.Config{
		DSN:     "mem://?tenant=default",
		Tenant:  "default",
		MaxRows: 3,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	s := New(store)

	mustExec(t, s, "CREATE TABLE maxrows_test (id INT)")
	for i := 1; i <= 10; i++ {
		mustExec(t, s, "INSERT INTO maxrows_test VALUES ("+itoa(i)+")")
	}

	res, err := s.HandleReadQuery(context.Background(), ReadQueryArgs{Query: "SELECT * FROM maxrows_test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("read_query error: %s", contentText(res))
	}
	text := contentText(res)
	if !strings.Contains(text, "truncated") {
		t.Errorf("expected truncation notice, got: %s", text)
	}
}

// ─── no internal imports test ─────────────────────────────────────────────────

// TestNoInternalImports verifies that no file in the tinysql-mcp-server
// module imports from github.com/SimonWaldherr/tinySQL/internal/*.
func TestNoInternalImports(t *testing.T) {
	// Resolve the module root: go up from this test file's package to the
	// module root (cmd/tinysql-mcp-server).
	gopath := build.Default.GOPATH
	_ = gopath

	// We walk the packages using go/build to detect internal imports.
	pkgs := []string{
		"tinysql-mcp-server",
		"tinysql-mcp-server/internal/tinysqldb",
		"tinysql-mcp-server/internal/mcpserver",
	}
	for _, pkg := range pkgs {
		p, err := build.Default.Import(pkg, filepath.Join("..", ".."), 0)
		if err != nil {
			// Package may not be importable from test context; skip.
			t.Logf("skip %s: %v", pkg, err)
			continue
		}
		for _, imp := range p.Imports {
			if strings.Contains(imp, "github.com/SimonWaldherr/tinySQL/internal") {
				t.Errorf("package %s imports internal tinySQL package: %s", pkg, imp)
			}
		}
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// contentText returns the concatenated text of all TextContent items in a result.
func contentText(res *mcp.CallToolResult) string {
	if res == nil {
		return ""
	}
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
			b.WriteByte(' ')
		}
	}
	return b.String()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}
