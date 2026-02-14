package engine

import (
    "context"
    "testing"

    "github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestGroupByHaving exercises GROUP BY with HAVING to ensure aggregation
// and post-aggregation filtering work as expected.
func TestGroupByHaving(t *testing.T) {
    db := storage.NewDB()
    ctx := context.Background()

    // create table and insert
    p := NewParser(`CREATE TABLE sales (region TEXT, amount INT)`)
    st, err := p.ParseStatement()
    if err != nil {
        t.Fatalf("parse create: %v", err)
    }
    if _, err := Execute(ctx, db, "default", st); err != nil {
        t.Fatalf("exec create: %v", err)
    }

    inserts := []string{
        `INSERT INTO sales VALUES ('north', 10)`,
        `INSERT INTO sales VALUES ('north', 20)`,
        `INSERT INTO sales VALUES ('south', 5)`,
    }
    for _, q := range inserts {
        p = NewParser(q)
        st, err := p.ParseStatement()
        if err != nil {
            t.Fatalf("parse insert: %v", err)
        }
        if _, err := Execute(ctx, db, "default", st); err != nil {
            t.Fatalf("exec insert: %v", err)
        }
    }

    // GROUP BY region HAVING SUM(amount) > 15 should return only 'north'
    p = NewParser(`SELECT region, SUM(amount) as total FROM sales GROUP BY region HAVING SUM(amount) > 15`)
    st, err = p.ParseStatement()
    if err != nil {
        t.Fatalf("parse group by: %v", err)
    }
    rs, err := Execute(ctx, db, "default", st)
    if err != nil {
        t.Fatalf("execute group by: %v", err)
    }
    if len(rs.Rows) != 1 {
        t.Fatalf("expected 1 group passing HAVING, got %d", len(rs.Rows))
    }
}

// TestParseErrorPath ensures invalid SQL produces a parse error.
func TestParseErrorPath(t *testing.T) {
    p := NewParser(`THIS IS NOT SQL`)
    if _, err := p.ParseStatement(); err == nil {
        t.Fatalf("expected parse error for invalid input")
    }
}

// TestAggregatorEdgeCases checks aggregator behavior with NULLs and empty sets.
func TestAggregatorEdgeCases(t *testing.T) {
    db := storage.NewDB()
    ctx := context.Background()

    p := NewParser(`CREATE TABLE nums (v INT)`)
    st, err := p.ParseStatement()
    if err != nil {
        t.Fatalf("parse create: %v", err)
    }
    if _, err := Execute(ctx, db, "default", st); err != nil {
        t.Fatalf("exec create: %v", err)
    }

    // No rows inserted: SUM should return NULL or 0 depending on implementation; ensure it doesn't crash
    p = NewParser(`SELECT SUM(v) as s FROM nums`)
    st, err = p.ParseStatement()
    if err != nil {
        t.Fatalf("parse sum: %v", err)
    }
    if _, err := Execute(ctx, db, "default", st); err != nil {
        t.Fatalf("execute sum on empty table: %v", err)
    }

    // Insert NULL and numeric values
    p = NewParser(`INSERT INTO nums VALUES (NULL)`)
    st, _ = p.ParseStatement()
    Execute(ctx, db, "default", st)
    p = NewParser(`INSERT INTO nums VALUES (10)`)
    st, _ = p.ParseStatement()
    Execute(ctx, db, "default", st)

    p = NewParser(`SELECT AVG(v) as a FROM nums`)
    st, err = p.ParseStatement()
    if err != nil {
        t.Fatalf("parse avg: %v", err)
    }
    if _, err := Execute(ctx, db, "default", st); err != nil {
        t.Fatalf("execute avg with NULLs: %v", err)
    }
}
