package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestUnionDistinctChainsPreserveSetOperationBoundaries(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []any
	}{
		{
			name: "union-all-retains-duplicates",
			sql:  `SELECT 1 AS value UNION SELECT 2 AS b UNION SELECT 1 AS c UNION ALL SELECT 1 AS d`,
			want: []any{1, 2, 1},
		},
		{
			name: "union-after-union-all-deduplicates-all-rows",
			sql:  `SELECT 1 AS value UNION SELECT 2 AS b UNION ALL SELECT 1 AS c UNION SELECT 3 AS d UNION SELECT 2 AS e`,
			want: []any{1, 2, 3},
		},
		{
			name: "union-after-except-restores-removed-value",
			sql:  `SELECT 1 AS value UNION SELECT 2 AS b UNION SELECT 3 AS c EXCEPT SELECT 1 AS d UNION SELECT 1 AS e UNION SELECT 2 AS f`,
			want: []any{2, 3, 1},
		},
		{
			name: "union-after-intersect-restores-removed-value",
			sql:  `SELECT 1 AS value UNION SELECT 2 AS b UNION SELECT 3 AS c INTERSECT SELECT 2 AS d UNION SELECT 1 AS e UNION SELECT 3 AS f`,
			want: []any{2, 1, 3},
		},
		{
			name: "compound-order-and-page-use-later-alias",
			sql:  `SELECT 3 AS value UNION SELECT 1 AS b UNION SELECT 2 AS c UNION SELECT 3 AS d ORDER BY c LIMIT 2 OFFSET 1`,
			want: []any{2, 3},
		},
		{
			name: "first-numeric-representative-survives",
			sql:  `SELECT 1.0 AS value UNION SELECT 1 AS b UNION SELECT 2 AS c UNION SELECT 2.0 AS d`,
			want: []any{float64(1), 2},
		},
		{
			name: "null-and-type-boundaries",
			sql:  `SELECT NULL AS value UNION SELECT 1 AS b UNION SELECT 1.0 AS c UNION SELECT NULL AS d UNION SELECT '1' AS e UNION SELECT X'31' AS f UNION SELECT TRUE AS g UNION SELECT X'31' AS h`,
			want: []any{nil, 1, "1", []byte("1"), true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			stmt := mustParse(tt.sql)
			// Cached/prepared ASTs must never retain a previous execution's set.
			for run := 0; run < 2; run++ {
				rs, err := Execute(t.Context(), db, "default", stmt)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(rs.Cols, []string{"value"}) {
					t.Fatalf("columns = %#v, want [value]", rs.Cols)
				}
				if got := setOpsColumnValues(t, rs, "value"); !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("values = %#v, want %#v", got, tt.want)
				}
			}
		})
	}
}

func TestUnionDistinctLongChainKeepsFirstOccurrenceOrder(t *testing.T) {
	terms := []string{"SELECT 7 AS value"}
	want := []any{7}
	for i := 0; i < 64; i++ {
		terms = append(terms, fmt.Sprintf("SELECT %d AS alias_%d", i%17, i))
		if i < 17 && i != 7 {
			want = append(want, i)
		}
	}
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	rs := executeSetOpsSQL(t, db, strings.Join(terms, " UNION "))
	if got := setOpsColumnValues(t, rs, "value"); !reflect.DeepEqual(got, want) {
		t.Fatalf("values = %#v, want %#v", got, want)
	}
}

func TestUnionDistinctChainAlignsMultipleColumns(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	rs := executeSetOpsSQL(t, db, `
		SELECT 1 AS id, 'first' AS label
		UNION SELECT 2 AS other_id, 'second' AS other_label
		UNION SELECT 1.0 AS x, 'first' AS y
		UNION SELECT NULL AS a, 'third' AS b
		UNION SELECT NULL AS c, 'third' AS d`)
	want := []Row{
		{"id": 1, "label": "first"},
		{"id": 2, "label": "second"},
		{"id": nil, "label": "third"},
	}
	if !reflect.DeepEqual(rs.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", rs.Rows, want)
	}
}

func TestUnionDistinctChainStillEvaluatesLaterTerms(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for _, query := range []string{
		`SELECT 1 AS value UNION SELECT 1 AS b UNION SELECT 1 / 0 AS c LIMIT 1`,
		`SELECT 1 AS value UNION SELECT 1 AS b UNION SELECT 1 AS c, 2 AS d`,
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err == nil {
			t.Fatalf("query %q succeeded, want error from later term", query)
		}
	}
}

func TestUnionDistinctAccumulatorHonorsCancellationBetweenTerms(t *testing.T) {
	accumulator := newSetOperationDistinctAccumulator([]string{"value"}, 2)
	if err := accumulator.appendRows(t.Context(), []Row{{"value": 1}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// A duplicate term must still check cancellation, even though no result
	// allocation or membership insertion is needed.
	if err := accumulator.appendRows(ctx, []Row{{"value": 1}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("appendRows error = %v, want context.Canceled", err)
	}
}
