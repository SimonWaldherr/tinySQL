package engine

// Deliberately self-contained (only Execute/mustParse/storage.NewDB -- no
// symbols introduced by the subquery-cache change) so this exact file can be
// dropped into the pre-change tree unmodified for an A/B benchmark
// comparison. See eval_subquery_cache.go for what it is measuring.

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// BenchmarkUncorrelatedInSubqueryManyOuterRows quantifies the win: without
// caching, "WHERE x IN (SELECT y FROM small_table)" re-executes the inner
// SELECT once per outer row; with it, once per statement execution.
func BenchmarkUncorrelatedInSubqueryManyOuterRows(b *testing.B) {
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse("CREATE TABLE outer_t (x INT)")); err != nil {
		b.Fatal(err)
	}
	if _, err := Execute(ctx, db, "default", mustParse("CREATE TABLE small_table (y INT)")); err != nil {
		b.Fatal(err)
	}
	if _, err := Execute(ctx, db, "default", mustParse("INSERT INTO small_table VALUES (4)")); err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= 2000; i++ {
		if _, err := Execute(ctx, db, "default", mustParse(fmt.Sprintf("INSERT INTO outer_t VALUES (%d)", i))); err != nil {
			b.Fatal(err)
		}
	}
	stmt := mustParse("SELECT x FROM outer_t WHERE x IN (SELECT y FROM small_table)")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUncorrelatedExistsManyOuterRows is the EXISTS-form counterpart:
// "WHERE EXISTS (SELECT 1 FROM small_table WHERE flag = 1)" over many outer
// rows, none of which correlate to the outer row at all.
func BenchmarkUncorrelatedExistsManyOuterRows(b *testing.B) {
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse("CREATE TABLE outer_t (x INT)")); err != nil {
		b.Fatal(err)
	}
	if _, err := Execute(ctx, db, "default", mustParse("CREATE TABLE small_table (y INT, flag INT)")); err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= 50; i++ {
		flag := 0
		if i == 25 {
			flag = 1
		}
		if _, err := Execute(ctx, db, "default", mustParse(fmt.Sprintf("INSERT INTO small_table VALUES (%d, %d)", i, flag))); err != nil {
			b.Fatal(err)
		}
	}
	for i := 1; i <= 2000; i++ {
		if _, err := Execute(ctx, db, "default", mustParse(fmt.Sprintf("INSERT INTO outer_t VALUES (%d)", i))); err != nil {
			b.Fatal(err)
		}
	}
	stmt := mustParse("SELECT x FROM outer_t WHERE EXISTS (SELECT 1 FROM small_table WHERE flag = 1)")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}
