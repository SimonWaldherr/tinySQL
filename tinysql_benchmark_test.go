package tinysql

import (
	"context"
	"fmt"
	"testing"
)

var benchmarkRows int

func mustBenchmarkExec(b *testing.B, ctx context.Context, db *DB, sql string) *ResultSet {
	b.Helper()
	stmt, err := ParseSQL(sql)
	if err != nil {
		b.Fatalf("parse %q: %v", sql, err)
	}
	rs, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		b.Fatalf("execute %q: %v", sql, err)
	}
	return rs
}

func newBenchmarkDB(b *testing.B, rows int) (*DB, context.Context) {
	b.Helper()
	ctx := context.Background()
	db := NewDB()
	b.Cleanup(func() { _ = db.Close() })

	mustBenchmarkExec(b, ctx, db, "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL, score FLOAT)")
	for i := 0; i < rows; i++ {
		mustBenchmarkExec(b, ctx, db, fmt.Sprintf(
			"INSERT INTO users VALUES (%d, 'user_%d', %d, %t, %.2f)",
			i, i, 18+i%50, i%2 == 0, float64(i)*1.25,
		))
	}
	return db, ctx
}

func BenchmarkParseSQL_BasicStatements(b *testing.B) {
	queries := []string{
		"CREATE TABLE users (id INT, name TEXT, active BOOL)",
		"INSERT INTO users VALUES (1, 'Alice', true)",
		"SELECT id, name FROM users WHERE active = true ORDER BY id LIMIT 10",
		"UPDATE users SET name = 'Bob' WHERE id = 1",
		"DELETE FROM users WHERE active = false",
	}

	for _, query := range queries {
		b.Run(query, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := ParseSQL(query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkQueryCache_CompileHit(b *testing.B) {
	cache := NewQueryCache(16)
	query := "SELECT id, name FROM users WHERE active = true ORDER BY id LIMIT 10"
	if _, err := Compile(cache, query); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Compile(cache, query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecute_CreateTable(b *testing.B) {
	ctx := context.Background()
	stmt, err := ParseSQL("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db := NewDB()
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
		_ = db.Close()
	}
}

func BenchmarkExecute_Insert(b *testing.B) {
	db, ctx := newBenchmarkDB(b, 0)
	stmt, err := ParseSQL("INSERT INTO users VALUES (1, 'Alice', 30, true, 42.5)")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecute_ReadQueries(b *testing.B) {
	db, ctx := newBenchmarkDB(b, 1000)

	queries := []struct {
		name string
		sql  string
		want int
	}{
		{"PointLookup", "SELECT id, name FROM users WHERE id = 500", 1},
		{"FilteredScan", "SELECT id, name FROM users WHERE active = true", 500},
		{"OrderByLimit", "SELECT id, score FROM users ORDER BY score DESC LIMIT 10", 10},
		{"Aggregate", "SELECT active, COUNT(*) as count FROM users GROUP BY active", 2},
	}

	for _, tc := range queries {
		stmt, err := ParseSQL(tc.sql)
		if err != nil {
			b.Fatalf("parse %s: %v", tc.name, err)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rs, err := Execute(ctx, db, "default", stmt)
				if err != nil {
					b.Fatal(err)
				}
				if len(rs.Rows) != tc.want {
					b.Fatalf("expected %d rows, got %d", tc.want, len(rs.Rows))
				}
				benchmarkRows = len(rs.Rows)
			}
		})
	}
}

func BenchmarkExecute_Join(b *testing.B) {
	db, ctx := newBenchmarkDB(b, 1000)
	mustBenchmarkExec(b, ctx, db, "CREATE TABLE orders (id INT, user_id INT, amount FLOAT)")
	for i := 0; i < 1000; i++ {
		mustBenchmarkExec(b, ctx, db, fmt.Sprintf(
			"INSERT INTO orders VALUES (%d, %d, %.2f)",
			i, i%1000, float64(i%100)*10.0,
		))
	}
	stmt, err := ParseSQL("SELECT users.id, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.active = true")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(ctx, db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 500 {
			b.Fatalf("expected 500 rows, got %d", len(rs.Rows))
		}
		benchmarkRows = len(rs.Rows)
	}
}

func BenchmarkExecute_Update(b *testing.B) {
	db, ctx := newBenchmarkDB(b, 1000)
	stmt, err := ParseSQL("UPDATE users SET score = 99.5 WHERE active = true")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecute_Delete(b *testing.B) {
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db := NewDB()
		mustBenchmarkExec(b, ctx, db, "CREATE TABLE deletions (id INT, active BOOL)")
		mustBenchmarkExec(b, ctx, db, "INSERT INTO deletions VALUES (1, false)")
		stmt, err := ParseSQL("DELETE FROM deletions WHERE active = false")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
		_ = db.Close()
	}
}

func BenchmarkExecuteCompiled_Select(b *testing.B) {
	db, ctx := newBenchmarkDB(b, 1000)
	cache := NewQueryCache(16)
	query, err := Compile(cache, "SELECT id, name FROM users WHERE active = true ORDER BY id LIMIT 10")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := ExecuteCompiled(ctx, db, "default", query)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 10 {
			b.Fatalf("expected 10 rows, got %d", len(rs.Rows))
		}
		benchmarkRows = len(rs.Rows)
	}
}
