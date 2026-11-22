package engine

import "testing"

func TestParseApplySources(t *testing.T) {
	const sql = `SELECT a.x FROM items AS t
		CROSS /* lateral */ aPpLy (SELECT t.x AS x) a
		OUTER APPLY explode(a.x) AS e
		CROSS APPLY metadata.lookup
		LEFT OUTER JOIN related AS r ON r.x = a.x`
	statement, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	joins := statement.(*Select).Joins
	if len(joins) != 4 {
		t.Fatalf("got %d joins, want 4", len(joins))
	}
	if j := joins[0]; j.Type != JoinCrossApply || j.On != nil || j.Right.Subquery == nil || j.Right.Alias != "a" {
		t.Fatalf("invalid CROSS APPLY subquery: %+v", j)
	}
	if j := joins[1]; j.Type != JoinOuterApply || j.On != nil || j.Right.TableFunc == nil || j.Right.Alias != "e" || j.Right.TableFunc.Alias != "e" {
		t.Fatalf("invalid OUTER APPLY table function: %+v", j)
	}
	if j := joins[2]; j.Type != JoinCrossApply || j.Right.Table != "metadata.lookup" || j.Right.Alias != "metadata.lookup" {
		t.Fatalf("invalid CROSS APPLY table: %+v", j)
	}
	if j := joins[3]; j.Type != JoinLeft || j.On == nil {
		t.Fatalf("invalid LEFT JOIN following APPLY: %+v", j)
	}
}

func TestParseApplySyntaxErrors(t *testing.T) {
	for _, sql := range []string{
		`SELECT * FROM t OUTER JOIN r ON 1=1`,
		`SELECT * FROM t LEFT APPLY r`,
		`SELECT * FROM t CROSS "apply" r`,
		`SELECT * FROM t CROSS APPLY`,
		`SELECT * FROM t CROSS APPLY (SELECT 1)`,
		`SELECT * FROM t OUTER APPLY (SELECT 1 AS x) s ON s.x = 1`,
	} {
		if _, err := NewParser(sql).ParseStatement(); err == nil {
			t.Errorf("parsed invalid APPLY syntax: %q", sql)
		}
	}
}

func TestParseApplyRemainsAnIdentifier(t *testing.T) {
	const sql = `SELECT apply.id FROM items apply JOIN apply AS other ON apply.id = other.id`
	statement, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	s := statement.(*Select)
	if s.From.Alias != "apply" || len(s.Joins) != 1 || s.Joins[0].Right.Table != "apply" {
		t.Fatalf("APPLY identifier was not preserved: %+v", s)
	}
}

func TestParseCrossJoinAdditionalSources(t *testing.T) {
	const sql = `SELECT * FROM t CROSS JOIN (SELECT 1 AS n) s CROSS JOIN source(1) AS f`
	statement, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatal(err)
	}
	joins := statement.(*Select).Joins
	if len(joins) != 2 || joins[0].Type != JoinCross || joins[0].Right.Subquery == nil || joins[0].On != nil ||
		joins[1].Type != JoinCross || joins[1].Right.TableFunc == nil || joins[1].On != nil {
		t.Fatalf("invalid CROSS JOIN sources: %+v", joins)
	}
}
