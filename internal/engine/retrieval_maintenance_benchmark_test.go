package engine

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The importer maintenance scenario: a retrieval table with a TEXT column behind
// FTS_SEARCH, a VECTOR column behind VEC_SEARCH, and an INT bookkeeping column
// that a "touch" rewrites for thousands of rows at a time. The timed section is
// the first search after the mutation; the mutation itself is not timed.
//
//	go test ./internal/engine/ -run '^$' -bench RetrievalMaintenance -benchtime=5x

const (
	retrievalBenchDims  = 64
	retrievalBenchWords = 150
	retrievalBenchVocab = 8000
)

type retrievalBenchEnv struct {
	db     *storage.DB
	rng    *rand.Rand
	zipf   *rand.Zipf
	nextID int
}

func (e *retrievalBenchEnv) text() string {
	var sb strings.Builder
	for i := 0; i < retrievalBenchWords; i++ {
		sb.WriteByte('w')
		sb.WriteString(strconv.FormatUint(e.zipf.Uint64(), 10))
		sb.WriteByte(' ')
	}
	return sb.String()
}

func (e *retrievalBenchEnv) vec() []float64 {
	v := make([]float64, retrievalBenchDims)
	for i := range v {
		v[i] = e.rng.Float64()*2 - 1
	}
	return v
}

func newRetrievalBenchEnv(b *testing.B, docs int) *retrievalBenchEnv {
	b.Helper()
	rng := rand.New(rand.NewSource(7))
	e := &retrievalBenchEnv{db: storage.NewDB(), rng: rng, zipf: rand.NewZipf(rng, 1.15, 4, retrievalBenchVocab-1)}
	table := storage.NewTable("bench", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "loaded_at", Type: storage.IntType},
		{Name: "content", Type: storage.TextType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	table.Rows = make([][]any, docs)
	for i := range table.Rows {
		table.Rows[i] = []any{i, 1, e.text(), e.vec()}
	}
	table.Version = 1
	if err := e.db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	e.nextID = docs
	b.Cleanup(func() {
		purgeFTSCachesFor("default", "bench")
		purgeVectorCachesFor("default", "bench")
	})
	return e
}

func (e *retrievalBenchEnv) exec(b *testing.B, sql string) {
	b.Helper()
	if _, err := Execute(context.Background(), e.db, "default", mustParse(sql)); err != nil {
		b.Fatalf("%s: %v", sql, err)
	}
}

func (e *retrievalBenchEnv) search(b *testing.B, kind string) {
	b.Helper()
	switch kind {
	case "fts":
		e.exec(b, `SELECT id FROM FTS_SEARCH('bench', 'w12 OR w34 OR w101', 10, 'content')`)
	default:
		e.exec(b, `SELECT id FROM VEC_SEARCH('bench', 'embedding', '`+retrievalVecLiteral(e.queryVec())+`', 10, 'cosine')`)
	}
}

func (e *retrievalBenchEnv) queryVec() []float64 {
	v := make([]float64, retrievalBenchDims)
	for i := range v {
		v[i] = float64(i%7) - 3
	}
	return v
}

func (e *retrievalBenchEnv) insert(b *testing.B, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		e.exec(b, fmt.Sprintf(`INSERT INTO bench VALUES (%d, 1, '%s', '%s')`, e.nextID, e.text(), retrievalVecLiteral(e.vec())))
		e.nextID++
	}
}

func BenchmarkRetrievalMaintenance(b *testing.B) {
	docs := 30000
	if testing.Short() {
		docs = 3000
	}
	type scenario struct {
		name   string
		mutate func(b *testing.B, e *retrievalBenchEnv, iter int)
	}
	touch := func(rows int) func(*testing.B, *retrievalBenchEnv, int) {
		stride := docs / rows
		return func(b *testing.B, e *retrievalBenchEnv, iter int) {
			e.exec(b, fmt.Sprintf(`UPDATE bench SET loaded_at = %d WHERE id %% %d = %d`, 100+iter, stride, iter%stride))
		}
	}
	rewrite := func(column string, rows int) func(*testing.B, *retrievalBenchEnv, int) {
		stride := docs / rows
		return func(b *testing.B, e *retrievalBenchEnv, iter int) {
			value := "'" + e.text() + "'"
			if column == "embedding" {
				value = "'" + retrievalVecLiteral(e.vec()) + "'"
			}
			e.exec(b, fmt.Sprintf(`UPDATE bench SET %s = %s WHERE id %% %d = %d`, column, value, stride, iter%stride))
		}
	}
	scenarios := []scenario{
		{"touch900", touch(docs * 3 / 100)},
		{"touch1800", touch(docs * 6 / 100)},
		{"touch3000", touch(docs / 10)},
		{"touch6000", touch(docs / 5)},
		{"content300", rewrite("content", 300)},
		{"content1500", rewrite("content", 1500)},
		{"embedding1000", rewrite("embedding", 1000)},
		{"insert6", func(b *testing.B, e *retrievalBenchEnv, iter int) { e.insert(b, 6) }},
		{"insert1000", func(b *testing.B, e *retrievalBenchEnv, iter int) { e.insert(b, 1000) }},
		{"delete0", func(b *testing.B, e *retrievalBenchEnv, iter int) { e.exec(b, `DELETE FROM bench WHERE id < 0`) }},
	}
	for _, sc := range scenarios {
		for _, kind := range []string{"fts", "vec"} {
			b.Run(sc.name+"/"+kind, func(b *testing.B) {
				e := newRetrievalBenchEnv(b, docs)
				e.search(b, kind) // cold build, not measured
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					sc.mutate(b, e, i)
					b.StartTimer()
					e.search(b, kind)
				}
			})
		}
	}
}
