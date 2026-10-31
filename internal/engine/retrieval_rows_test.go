package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type sharedRetrievalRows struct{ result *ResultSet }

func (*sharedRetrievalRows) Name() string              { return "VEC_SEARCH" }
func (*sharedRetrievalRows) ValidateArgs([]Expr) error { return nil }
func (f *sharedRetrievalRows) Execute(context.Context, []Expr, ExecEnv, Row) (*ResultSet, error) {
	return f.result, nil
}

func TestRetrievalRowsDoNotMutateExtensionResults(t *testing.T) {
	shared := &ResultSet{Cols: []string{"id"}, Rows: []Row{{"id": 1}}}
	got := aliasRetrievalRows(&sharedRetrievalRows{result: shared}, shared, "r")
	got[0]["id"] = 99
	if !reflect.DeepEqual(shared.Rows, []Row{{"id": 1}}) {
		t.Fatalf("extension result mutated: %v", shared.Rows)
	}
}

func TestRetrievalRowsMatchCopyingQualification(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE retrieval_owned (id INT PRIMARY KEY, doc TEXT, chunk INT, body TEXT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO retrieval_owned VALUES (1,'a',0,'alpha opening','[1,0]'),(2,'a',1,'alpha middle','[0.8,0.2]'),(3,'a',2,'alpha end','[0,1]')`)
	execSQL(t, db, `CREATE TABLE retrieval_hits (doc TEXT, chunk INT)`)
	execSQL(t, db, `INSERT INTO retrieval_hits VALUES ('a',1)`)
	for _, call := range []string{
		`VEC_SEARCH('retrieval_owned','embedding','[1,0]',3)`,
		`VEC_SEARCH_FILTERED('retrieval_owned','embedding','[1,0]',3,'{"pre_filter":{"equals":{"doc":"a"}}}')`,
		`FTS_SEARCH('retrieval_owned','alpha',3,'body')`,
		`FTS_SEARCH_FILTERED('retrieval_owned','alpha',3,'{"pre_filter":{"equals":{"doc":"a"}}}','body')`,
		`RAG_SEARCH('retrieval_owned','embedding','[1,0]',3)`,
		`HYBRID_SEARCH('retrieval_owned','embedding','body','alpha','[1,0]',3)`,
		`RAG_CONTEXT('retrieval_owned','doc','chunk','a',1,1,1)`,
		`RAG_CONTEXT_FROM('retrieval_owned','doc','chunk','retrieval_hits','doc','chunk',1,1)`,
		`RAG_SEARCH('retrieval_owned','embedding','[1,0]',3,'{"text_columns":["body"],"text_query":"alpha","key_columns":["id"],"expand_before":1,"expand_after":1,"doc_id_column":"doc","chunk_index_column":"chunk"}')`,
	} {
		for _, alias := range []string{"", "r", "Über"} {
			s := mustParse("SELECT * FROM " + call).(*Select)
			fn, _ := GetTableFunc(s.From.TableFunc.Name)
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			result, err := fn.Execute(t.Context(), s.From.TableFunc.Args, env, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := aliasRows(result.Rows, alias)
			got := aliasRetrievalRows(fn, result, alias)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s alias %q: got %v, want %v", call, alias, got, want)
			}
			got[0]["id"] = -1
			fresh, err := fn.Execute(t.Context(), s.From.TableFunc.Args, env, nil)
			if err != nil || fresh.Rows[0]["id"] == -1 {
				t.Fatalf("result escaped into a cache or source: %v, %v", fresh, err)
			}
		}
	}
	for _, name := range []string{"ID", "r.id", "a.b"} {
		result := &ResultSet{Cols: []string{name}, Rows: []Row{{name: 1}}}
		before := cloneRow(result.Rows[0])
		got := aliasRetrievalRows(&VecSearchTableFunc{}, result, "r")
		if !reflect.DeepEqual(got, aliasRows([]Row{before}, "r")) || !reflect.DeepEqual(result.Rows[0], before) {
			t.Fatalf("fallback %s mutated rows", name)
		}
	}
}

func BenchmarkRetrievalRowQualification(b *testing.B) {
	for _, width := range []int{8, 32} {
		table := storage.NewTable("qualification", nil, false)
		for col := 0; col < width; col++ {
			table.Cols = append(table.Cols, storage.Column{Name: fmt.Sprintf("column_%d", col), Type: storage.TextType})
		}
		for i := 0; i < 6; i++ {
			row := make([]any, width)
			for j := range row {
				row[j] = "document metadata"
			}
			table.Rows = append(table.Rows, row)
		}
		hits := []vecScoredRow{{rowIdx: 0}, {rowIdx: 1}, {rowIdx: 2}, {rowIdx: 3}, {rowIdx: 4}, {rowIdx: 5}}
		for _, owned := range []bool{false, true} {
			b.Run(fmt.Sprintf("width=%d/owned=%v", width, owned), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					result := materializeVecCandidates(table, hits, "cosine")
					var rows []Row
					if owned {
						rows = aliasRetrievalRows(&VecSearchTableFunc{}, result, "r")
					} else {
						rows = aliasRows(result.Rows, "r")
					}
					if len(rows) != 6 || rows[0]["r.column_0"] != "document metadata" {
						b.Fatal("lost hit metadata")
					}
				}
			})
		}
	}
}
