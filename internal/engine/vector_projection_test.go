package engine

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVecSearchProjectedSQLMatchesGeneralPath(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("vec_project", []storage.Column{
		{Name: "ID", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
		{Name: "tag", Type: storage.TextType},
		{Name: "_vec_rank", Type: storage.IntType},
		{Name: "notes", Type: storage.TextType},
	}, false)
	table.Rows = [][]any{
		{1, []float64{1, 0}, "z", 700, "long source text"},
		{2, []float64{0, 1}, "y", 701, nil},
		{3, []float64{-1, 0}, "x", 702},
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	const call = "('vec_project', 'embedding', '[1,0]', 3, 'cosine') AS hits"
	for _, tc := range []struct {
		projection, source, tail string
	}{
		{"hits.ID AS item, _vec_distance", "VEC_SEARCH" + call, "ORDER BY tag DESC LIMIT 2 OFFSET 1"},
		{"id, hits._vec_rank, hits._vec_similarity", "VEC_TOP_K" + call, "ORDER BY hits._vec_distance DESC"},
		{"id, _vec_rank AS tag", "VEC_SEARCH" + call, "ORDER BY tag DESC"},
		{"id AS item", "VEC_SEARCH" + call, "ORDER BY item DESC"},
		{"DISTINCT id, hits.ID AS duplicate", "VEC_SEARCH" + call, ""},
		{"id, notes", "VEC_SEARCH" + call, ""}, // The last row lacks a stored cell.
		{"_vec_rnak", "VEC_SEARCH" + call, ""},
		{"wrong.id", "VEC_SEARCH" + call, ""},
		{"missing", "VEC_SEARCH('vec_project', 'embedding', '[0,0]', 3)", ""},
	} {
		prefix := fmt.Sprintf("SELECT %s FROM %s", tc.projection, tc.source)
		query := prefix + " " + tc.tail
		got, err := Execute(t.Context(), db, "default", mustParse(query))
		want, refErr := Execute(t.Context(), db, "default", mustParse(prefix+" WHERE 1 = 1 "+tc.tail))
		if fmt.Sprint(err) != fmt.Sprint(refErr) || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got %v/%v, want %v/%v", query, got, err, want, refErr)
		}
	}

	stmt := mustParse("SELECT hits.ID AS item, _vec_distance FROM VEC_SEARCH" + call + " ORDER BY item, tag DESC").(*Select)
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	rows, err := resolveTableFunc(env, stmt)
	if err != nil || len(rows) != 3 || !reflect.DeepEqual(rows[0], Row{"hits.id": 1, "_vec_distance": float64(0), "tag": "z"}) {
		t.Fatalf("projected source returned %v/%v", rows, err)
	}
}

func BenchmarkVecSearchSQLProjection(b *testing.B) {
	for _, tc := range []struct{ width, k int }{{4, 20}, {36, 200}} {
		b.Run(fmt.Sprintf("columns=%d/k=%d", tc.width, tc.k), func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			cols := []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "v", Type: storage.VectorType}}
			for i := len(cols); i < tc.width; i++ {
				cols = append(cols, storage.Column{Name: fmt.Sprintf("attribute_%d", i), Type: storage.TextType})
			}
			table := storage.NewTable("vec_projection", cols, false)
			b.Cleanup(func() { purgeVectorCachesFor("default", table.Name) })
			for i := range 2048 {
				row := make([]any, len(cols))
				row[0], row[1] = i, []float64{1, float64(i)}
				for col := 2; col < len(cols); col++ {
					row[col] = "document metadata"
				}
				table.Rows = append(table.Rows, row)
			}
			if err := db.Put("default", table); err != nil {
				b.Fatal(err)
			}
			stmt := mustParse(fmt.Sprintf("SELECT hits.id, _vec_distance FROM VEC_SEARCH('vec_projection', 'v', '[1,0]', %d) hits ORDER BY _vec_rank", tc.k))
			if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				rs, err := Execute(b.Context(), db, "default", stmt)
				if err != nil || len(rs.Rows) != tc.k {
					b.Fatalf("unexpected result: %v", err)
				}
			}
		})
	}
}
