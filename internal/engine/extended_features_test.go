// Extended SQL features test file
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestMedianAggregate(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create test table
	Execute(ctx, db, "default", mustParse("CREATE TABLE nums (id INT, val INT)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO nums VALUES (1, 10)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO nums VALUES (2, 20)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO nums VALUES (3, 30)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO nums VALUES (4, 40)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO nums VALUES (5, 50)"))

	rs, err := Execute(ctx, db, "default", mustParse("SELECT MEDIAN(val) as med FROM nums"))
	if err != nil {
		t.Fatalf("MEDIAN query failed: %v", err)
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rs.Rows))
	}

	// Median of [10, 20, 30, 40, 50] should be 30
	med := rs.Rows[0]["med"]
	if med != float64(30) && med != 30 {
		t.Errorf("Expected median 30, got %v", med)
	}
}

func TestCountDistinct(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse("CREATE TABLE items (id INT, category TEXT)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO items VALUES (1, 'A')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO items VALUES (2, 'B')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO items VALUES (3, 'A')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO items VALUES (4, 'B')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO items VALUES (5, 'C')"))

	rs, err := Execute(ctx, db, "default", mustParse("SELECT COUNT(DISTINCT category) as cnt FROM items"))
	if err != nil {
		t.Fatalf("COUNT(DISTINCT) query failed: %v", err)
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rs.Rows))
	}

	cnt := rs.Rows[0]["cnt"]
	if cnt != 3 && cnt != int64(3) {
		t.Errorf("Expected count 3, got %v", cnt)
	}
}

func TestInOperator(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse("CREATE TABLE products (id INT, name TEXT)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO products VALUES (1, 'Apple')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO products VALUES (2, 'Banana')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO products VALUES (3, 'Cherry')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO products VALUES (4, 'Date')"))

	rs, err := Execute(ctx, db, "default", mustParse("SELECT name FROM products WHERE id IN (1, 3, 5) ORDER BY id"))
	if err != nil {
		t.Fatalf("IN operator query failed: %v", err)
	}

	if len(rs.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rs.Rows))
	}

	if rs.Rows[0]["name"] != "Apple" {
		t.Errorf("Expected 'Apple', got %v", rs.Rows[0]["name"])
	}
	if rs.Rows[1]["name"] != "Cherry" {
		t.Errorf("Expected 'Cherry', got %v", rs.Rows[1]["name"])
	}
}

func TestLikeOperator(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse("CREATE TABLE users (id INT, email TEXT)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO users VALUES (1, 'alice@example.com')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO users VALUES (2, 'bob@test.com')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO users VALUES (3, 'charlie@example.com')"))

	rs, err := Execute(ctx, db, "default", mustParse("SELECT email FROM users WHERE email LIKE '%@example.com' ORDER BY id"))
	if err != nil {
		t.Fatalf("LIKE operator query failed: %v", err)
	}

	if len(rs.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rs.Rows))
	}

	if rs.Rows[0]["email"] != "alice@example.com" {
		t.Errorf("Expected alice@example.com, got %v", rs.Rows[0]["email"])
	}
}

func TestBase64Functions(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create a dummy table for testing since SELECT without FROM is not supported
	Execute(ctx, db, "default", mustParse("CREATE TABLE dual (dummy INT)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO dual VALUES (1)"))

	// Test BASE64 encoding
	rs, err := Execute(ctx, db, "default", mustParse("SELECT BASE64('Hello World') as encoded FROM dual"))
	if err != nil {
		t.Fatalf("BASE64 query failed: %v", err)
	}

	encoded := rs.Rows[0]["encoded"].(string)
	expected := "SGVsbG8gV29ybGQ="
	if encoded != expected {
		t.Errorf("Expected %s, got %s", expected, encoded)
	}

	// Test BASE64 decoding
	rs, err = Execute(ctx, db, "default", mustParse("SELECT BASE64_DECODE('SGVsbG8gV29ybGQ=') as decoded FROM dual"))
	if err != nil {
		t.Fatalf("BASE64_DECODE query failed: %v", err)
	}

	decoded := rs.Rows[0]["decoded"].(string)
	if decoded != "Hello World" {
		t.Errorf("Expected 'Hello World', got %s", decoded)
	}
}

func TestIfExistsCreateTable(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create table
	_, err := Execute(ctx, db, "default", mustParse("CREATE TABLE test_table (id INT)"))
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Try to create again without IF NOT EXISTS - should fail
	_, err = Execute(ctx, db, "default", mustParse("CREATE TABLE test_table (id INT)"))
	if err == nil {
		t.Fatal("Expected error when creating duplicate table")
	}

	// Create with IF NOT EXISTS - should succeed (no-op)
	_, err = Execute(ctx, db, "default", mustParse("CREATE TABLE IF NOT EXISTS test_table (id INT)"))
	if err != nil {
		t.Fatalf("CREATE TABLE IF NOT EXISTS failed: %v", err)
	}
}

func TestIfExistsDropTable(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Drop non-existent table without IF EXISTS - should fail
	_, err := Execute(ctx, db, "default", mustParse("DROP TABLE nonexistent"))
	if err == nil {
		t.Fatal("Expected error when dropping non-existent table")
	}

	// Drop with IF EXISTS - should succeed (no-op)
	_, err = Execute(ctx, db, "default", mustParse("DROP TABLE IF EXISTS nonexistent"))
	if err != nil {
		t.Fatalf("DROP TABLE IF EXISTS failed: %v", err)
	}
}

func mustParse(sql string) Statement {
	p := NewParser(sql)
	stmt, err := p.ParseStatement()
	if err != nil {
		panic(err)
	}
	return stmt
}
