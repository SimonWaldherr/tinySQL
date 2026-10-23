package engine

import (
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRAGContextFromHitProjectionAndRanks(t *testing.T) {
	for _, tableHits := range []bool{false, true} {
		t.Run(map[bool]string{false: "cte", true: "table"}[tableHits], func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			chunks := storage.NewTable("chunks", []storage.Column{{Name: "doc", Type: storage.TextType}, {Name: "chunk", Type: storage.IntType}}, false)
			for _, doc := range []string{"a", "b", "c", "d", "e"} {
				chunks.Rows = append(chunks.Rows, []any{doc, 0})
			}
			if err := db.Put("default", chunks); err != nil {
				t.Fatal(err)
			}
			hits := storage.NewTable("hits", []storage.Column{
				{Name: "DOC", Type: storage.TextType}, {Name: "CHUNK", Type: storage.IntType},
				{Name: "_RRF_RANK", Type: storage.TextType}, {Name: "_VEC_RANK", Type: storage.IntType},
				{Name: "_HIT_RANK", Type: storage.IntType}, {Name: "RANK", Type: storage.IntType},
			}, false)
			hits.Rows = [][]any{
				{"a", 0, 4, 1, 1, 1},
				{"b", 0, "invalid", 3, 1, 1},
				{"c", 0, nil, nil, 2, 1},
				{"d", 0, nil, nil, nil, 1},
				{"e", 0}, // Declared missing cells are NULL, then rank falls back.
			}
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			if tableHits {
				if err := db.Put("default", hits); err != nil {
					t.Fatal(err)
				}
			} else {
				hitSource := ragSourceFromTable("default", hits)
				rows := make([]Row, len(hits.Rows))
				for i, raw := range hits.Rows {
					rows[i] = make(Row)
					for j, value := range raw {
						// Exercise CTE Row keys that require case-folded lookup.
						rows[i][hits.Cols[j].Name] = value
					}
				}
				env.ctes = map[string]*ResultSet{"hits": {Cols: hitSource.cols, Rows: rows}}
			}
			result, err := (&RAGContextFromTableFunc{}).Execute(t.Context(), ragContextHitTestArgs(), env, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i, wantDoc := range []string{"d", "c", "b", "a", "e"} {
				if row := result.Rows[i]; row["doc"] != wantDoc || row["_hit_rank"] != i+1 {
					t.Fatalf("rank %d row = %#v, want document %s", i+1, row, wantDoc)
				}
			}
		})
	}
}

func TestRAGContextFromIgnoresUnprojectedHitKeys(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	chunks := storage.NewTable("chunks", []storage.Column{{Name: "doc", Type: storage.TextType}, {Name: "chunk", Type: storage.IntType}}, false)
	chunks.Rows = [][]any{{"a", 0}, {"b", 0}, {nil, 0}}
	if err := db.Put("default", chunks); err != nil {
		t.Fatal(err)
	}
	hits := &ResultSet{Cols: []string{"doc", "chunk", "rank"}, Rows: []Row{
		{"doc": "a", "chunk": 0, "rank": 2, "_rrf_rank": 0},
		{"doc": "b", "chunk": 0, "rank": 1},
		{"chunk": 0, "rank": 3}, // Missing declared doc is a projected NULL.
	}}
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default", ctes: map[string]*ResultSet{"hits": hits}}
	got, err := (&RAGContextFromTableFunc{}).Execute(t.Context(), ragContextHitTestArgs(), env, nil)
	if err != nil {
		t.Fatal(err)
	}
	var docs []any
	for _, row := range got.Rows {
		docs = append(docs, row["doc"])
	}
	if !reflect.DeepEqual(docs, []any{"b", "a", nil}) {
		t.Fatalf("documents = %#v, want b/a/NULL", docs)
	}
	if hits.Rows[0]["_rrf_rank"] != 0 || len(hits.Rows[2]) != 2 {
		t.Fatal("context expansion changed input hit rows")
	}
}

func TestRAGContextFromHitValidationOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		hits *ResultSet
		want string
	}{
		{"missing-document", &ResultSet{Cols: []string{"chunk"}, Rows: []Row{{"doc": "a", "chunk": "invalid"}}}, `hit column "doc" not found`},
		{"missing-chunk", &ResultSet{Cols: []string{"doc"}, Rows: []Row{{"doc": "a", "chunk": 0}}}, `hit column "chunk" not found`},
		{"null-chunk", &ResultSet{Cols: []string{"doc", "chunk"}, Rows: []Row{{"doc": "a"}}}, "RAG_CONTEXT_FROM chunk:"},
		{"invalid-later-hit", &ResultSet{Cols: []string{"doc", "chunk"}, Rows: []Row{{"doc": "a", "chunk": 0}, {"doc": "a", "chunk": "invalid"}}}, "RAG_CONTEXT_FROM chunk:"},
		{"empty-hits", &ResultSet{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			chunks := storage.NewTable("chunks", []storage.Column{{Name: "doc", Type: storage.TextType}, {Name: "chunk", Type: storage.IntType}}, false)
			chunks.Rows = [][]any{{"a", 0}}
			if err := db.Put("default", chunks); err != nil {
				t.Fatal(err)
			}
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default", ctes: map[string]*ResultSet{"hits": tc.hits}}
			_, err := (&RAGContextFromTableFunc{}).Execute(t.Context(), ragContextHitTestArgs(), env, nil)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func ragContextHitTestArgs() []Expr {
	return []Expr{&Literal{Val: "chunks"}, &Literal{Val: "doc"}, &Literal{Val: "chunk"}, &Literal{Val: "hits"}, &Literal{Val: "doc"}, &Literal{Val: "chunk"}, &Literal{Val: 0}}
}

func TestRAGContextFromUnicodeHitColumnWithDuplicateProjections(t *testing.T) {
	for _, tableHits := range []bool{false, true} {
		t.Run(map[bool]string{false: "cte", true: "table"}[tableHits], func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			chunks := storage.NewTable("chunks", []storage.Column{
				{Name: "doc", Type: storage.TextType}, {Name: "chunk", Type: storage.IntType},
			}, false)
			chunks.Rows = [][]any{{"a", 0}, {"b", 0}}
			if err := db.Put("default", chunks); err != nil {
				t.Fatal(err)
			}
			hits := storage.NewTable("hits", []storage.Column{
				{Name: "s", Type: storage.TextType}, {Name: "S", Type: storage.TextType},
				{Name: "chunk", Type: storage.IntType},
			}, false)
			hits.Rows = [][]any{{"a", "b", 0}}
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			source := ragSourceFromTable("default", hits)
			if tableHits {
				if err := db.Put("default", hits); err != nil {
					t.Fatal(err)
				}
			} else {
				// CTE projection reads preserve the exact lowercase Row key's
				// precedence, even when an uppercase alternative also exists.
				rs := &ResultSet{Cols: source.cols, Rows: []Row{{"s": "a", "S": "b", "chunk": 0}}}
				env.ctes = map[string]*ResultSet{"hits": rs}
				source = ragSource{cols: rs.Cols, rows: rs.Rows}
			}
			for _, name := range []string{"s", "S", "ſ"} {
				want, ok := ragValue(source.outputRow(0), name)
				if !ok {
					t.Fatal("projected hit document not found")
				}
				args := ragContextHitTestArgs()
				args[4] = &Literal{Val: name}
				got, err := (&RAGContextFromTableFunc{}).Execute(t.Context(), args, env, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(got.Rows) != 1 || got.Rows[0]["doc"] != want {
					t.Fatalf("hit column %q: got %#v, want document %v", name, got.Rows, want)
				}
			}
		})
	}
}
