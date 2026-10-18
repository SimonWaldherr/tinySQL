package engine

import (
	"fmt"
	"math"
	"strconv"
	"testing"
)

func TestFetchDefaultCountAndSingularOffset(t *testing.T) {
	db := setupLimitTable(t)
	for _, tc := range []struct {
		tail string
		id   int
	}{
		{"FETCH FIRST ROW ONLY", 1},
		{"FETCH NEXT ROWS ONLY", 1},
		{"OFFSET 1 ROW FETCH FIRST ROW ONLY", 2},
		{"OFFSET 2 ROWS FETCH NEXT ROWS ONLY", 3},
		{"offset 3 row fetch next row only", 4},
	} {
		t.Run(tc.tail, func(t *testing.T) {
			rs := execSQL(t, db, "SELECT id FROM t ORDER BY id "+tc.tail)
			if len(rs.Rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rs.Rows))
			}
			expectInt(t, rs.Rows[0]["id"], tc.id, "fetched row")
		})
	}
	rs := execSQL(t, db, "SELECT id FROM t ORDER BY id OFFSET 1 ROW")
	if len(rs.Rows) != 9 {
		t.Fatalf("singular OFFSET: got %d rows, want 9", len(rs.Rows))
	}
}

func TestPaginationPreservesIntegerPrecision(t *testing.T) {
	values := []string{strconv.Itoa(math.MaxInt), strconv.Itoa(math.MaxInt - 1)}
	if strconv.IntSize == 64 {
		values = append(values, "9007199254740993", "9007199254740993 + 2")
	}
	for _, clause := range []string{"LIMIT", "OFFSET", "FETCH FIRST"} {
		for _, value := range values {
			t.Run(clause+" "+value, func(t *testing.T) {
				query := "SELECT 1 " + clause + " " + value
				if clause == "FETCH FIRST" {
					query += " ROWS ONLY"
				}
				stmt, err := NewParser(query).ParseStatement()
				if err != nil {
					t.Fatal(err)
				}
				sel := stmt.(*Select)
				got := sel.Limit
				if clause == "OFFSET" {
					got = sel.Offset
				}
				expr := value
				var want int
				if value == "9007199254740993 + 2" {
					expr = "9007199254740995"
				}
				want, err = strconv.Atoi(expr)
				if err != nil {
					t.Fatal(err)
				}
				if got == nil || *got != want {
					t.Fatalf("pagination did not retain exact value %d: %#v", want, sel)
				}
			})
		}
	}
}

func TestPaginationLargeBoundsExecution(t *testing.T) {
	db := setupLimitTable(t)
	for _, source := range []string{"t", "(SELECT id FROM t) AS src"} {
		for _, tc := range []struct {
			tail  string
			count int
		}{
			{fmt.Sprintf("LIMIT %d", math.MaxInt), 10},
			{fmt.Sprintf("LIMIT %d OFFSET 1", math.MaxInt), 9},
			{fmt.Sprintf("OFFSET %d ROWS", math.MaxInt), 0},
			{fmt.Sprintf("LIMIT 1 OFFSET %d", math.MaxInt), 0},
			{fmt.Sprintf("OFFSET %d ROWS FETCH FIRST ROW ONLY", math.MaxInt), 0},
		} {
			for _, order := range []string{"", " ORDER BY id"} {
				query := "SELECT id FROM " + source + order + " " + tc.tail
				t.Run(query, func(t *testing.T) {
					rs := execSQL(t, db, query)
					if len(rs.Rows) != tc.count {
						t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.count)
					}
				})
			}
		}
	}
}

func TestPaginationRejectsInvalidBoundsAndDuplicateLimit(t *testing.T) {
	for _, tail := range []string{
		"OFFSET ALL", "FETCH FIRST ALL ROWS ONLY",
		"LIMIT 2 FETCH FIRST 1 ROW ONLY", "LIMIT ALL FETCH NEXT ROW ONLY",
		"FETCH FIRST ONLY", "FETCH FIRST 1 ONLY", "FETCH FIRST ROW",
		"LIMIT 1e100", "OFFSET 1e100", "FETCH FIRST 1e100 ROWS ONLY",
		fmt.Sprintf("LIMIT %d + 1", math.MaxInt),
		fmt.Sprintf("OFFSET %d + 1", math.MaxInt),
	} {
		t.Run(tail, func(t *testing.T) {
			if _, err := NewParser("SELECT 1 " + tail).ParseStatement(); err == nil {
				t.Fatal("expected invalid pagination to fail")
			}
		})
	}
}
