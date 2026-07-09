package driver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// BenchmarkRepeatedSelectViaDriver measures the database/sql round-trip cost
// of re-issuing the same SELECT text — the shape the parsed-statement cache
// (parseSQLCached) accelerates. Run with the cache disabled by raising the
// statement above parsedStmtCacheMaxSQLLen if a no-cache baseline is needed.
func BenchmarkRepeatedSelectViaDriver(b *testing.B) {
	db, err := sql.Open("tinysql", "mem://?tenant=default")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `CREATE TABLE bench_docs (id INT, score FLOAT, title TEXT)`); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO bench_docs VALUES (%d, %d.5, 'document title %d')`, i, i%10, i)); err != nil {
			b.Fatal(err)
		}
	}

	const q = `SELECT id, title FROM bench_docs WHERE score > 3.0 AND title LIKE 'document%' ORDER BY id DESC LIMIT 10`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
		if n != 10 {
			b.Fatalf("expected 10 rows, got %d", n)
		}
	}
}
