package engine

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The re-import scenario of a RAG corpus: every document is a source of a few
// chunks, a source is replaced by DELETE-ing its old chunks and INSERT-ing the
// new ones, and the next search has to see the result. Each iteration deletes
// some sources and then runs the first FTS_SEARCH; the re-inserted rows are added
// with the timer stopped (appending is cheap for the index either way).

const benchChunksPerSource = 6

type benchCorpus struct {
	db      *storage.DB
	table   *storage.Table
	rng     *rand.Rand
	live    []int // sources currently in the table
	nextSrc int
	nextID  int
	// hapax is the share of tokens that occur exactly once in the whole corpus
	// (part numbers, hashes, dates); it is what makes the vocabulary large.
	hapax     float64
	nextHapax int
	dim       int // vector dimensions
}

func (c *benchCorpus) chunkText(tokens int) string {
	var sb strings.Builder
	sb.Grow(tokens * 7)
	for i := 0; i < tokens; i++ {
		if c.hapax > 0 && c.rng.Float64() < c.hapax {
			fmt.Fprintf(&sb, "h%d ", c.nextHapax)
			c.nextHapax++
			continue
		}
		x := c.rng.Float64()
		fmt.Fprintf(&sb, "w%d ", int(20000*x*x*x))
	}
	return sb.String()
}

func benchVectorLiteral(dim int) string {
	parts := make([]string, dim)
	for i := range parts {
		parts[i] = fmt.Sprintf("%.3f", float64(i%17)/17+0.01)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func newBenchCorpus(b *testing.B, docs int, hapax float64, dim int) *benchCorpus {
	b.Helper()
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	c := &benchCorpus{db: db, rng: rand.New(rand.NewSource(7)), hapax: hapax, dim: dim}
	benchExec(b, db, `CREATE TABLE bench_docs (id INT, src INT, body TEXT, embedding VECTOR)`)
	benchExec(b, db, `INSERT INTO bench_docs VALUES (0, -1, 'seed row', '`+benchVectorLiteral(dim)+`')`)
	table, err := db.Get("default", "bench_docs")
	if err != nil {
		b.Fatal(err)
	}
	c.table = table
	vector := table.Rows[0][3]
	table.Rows = table.Rows[:0]
	for len(table.Rows) < docs {
		src := c.nextSrc
		c.nextSrc++
		c.live = append(c.live, src)
		for k := 0; k < benchChunksPerSource && len(table.Rows) < docs; k++ {
			table.Rows = append(table.Rows, []any{c.nextID, src, c.chunkText(150), vector})
			c.nextID++
		}
	}
	table.Version++
	table.MarkDirtyFrom(-1)
	return c
}

func benchExec(b *testing.B, db *storage.DB, sql string) {
	b.Helper()
	if _, err := Execute(b.Context(), db, "default", mustParse(sql)); err != nil {
		b.Fatalf("%.200s: %v", sql, err)
	}
}

// reimport appends the chunks of n new sources (as the importer does after the
// old ones were deleted).
func (c *benchCorpus) reimport(b *testing.B, n int) {
	b.Helper()
	vector := c.table.Rows[0][3]
	for i := 0; i < n; i++ {
		src := c.nextSrc
		c.nextSrc++
		c.live = append(c.live, src)
		for k := 0; k < benchChunksPerSource; k++ {
			c.table.Rows = append(c.table.Rows, []any{c.nextID, src, c.chunkText(150), vector})
			c.nextID++
		}
	}
	c.table.Version++
	c.table.MarkDirtyFrom(len(c.table.Rows) - n*benchChunksPerSource)
}

const benchFTSSearch = `SELECT id FROM FTS_SEARCH('bench_docs', 'w7 w19 w250', 10, 'body')`

func benchDeleteSources(b *testing.B, docs, sources int) {
	benchDeleteSourcesWith(b, newBenchCorpus(b, docs, 0, 8), sources, benchFTSSearch)
}

func benchDeleteSourcesWith(b *testing.B, c *benchCorpus, sources int, search string) {
	benchExec(b, c.db, search) // the cold build is not part of the measurement
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var ids []string
		for k := 0; k < sources; k++ {
			at := c.rng.Intn(len(c.live))
			ids = append(ids, fmt.Sprint(c.live[at]))
			c.live = append(c.live[:at], c.live[at+1:]...)
		}
		b.StartTimer()
		benchExec(b, c.db, `DELETE FROM bench_docs WHERE src IN (`+strings.Join(ids, ",")+`)`)
		benchExec(b, c.db, search)
		b.StopTimer()
		c.reimport(b, sources)
		benchExec(b, c.db, search) // bring the index up to date outside the timer
		b.StartTimer()
	}
}

// DELETE of one source (6 chunks) then the first FTS_SEARCH.
func BenchmarkFTSDeleteSourceThenSearch(b *testing.B) {
	for _, docs := range []int{50_000, 100_000} {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) { benchDeleteSources(b, docs, 1) })
	}
}

// DELETE of 100 sources in one statement then the first FTS_SEARCH.
func BenchmarkFTSDelete100SourcesThenSearch(b *testing.B) {
	for _, docs := range []int{50_000, 100_000} {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) { benchDeleteSources(b, docs, 100) })
	}
}

// The same with a corpus whose vocabulary is as large as a real one: one token in
// ten occurs in a single chunk only (part numbers, hashes, dates), so the
// vocabulary grows with the corpus (about 750k terms at 50k chunks) instead of
// staying at the 20k of the default text. Every pass over the term dictionary
// shows up here.
func BenchmarkFTSDeleteSourceThenSearchLargeVocabulary(b *testing.B) {
	for _, docs := range []int{50_000, 100_000} {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) {
			benchDeleteSourcesWith(b, newBenchCorpus(b, docs, 0.1, 8), 1, benchFTSSearch)
		})
	}
}

// What the application runs is HYBRID_SEARCH over 768-dimensional vectors. After a
// DELETE the full-text index is followed, but the flat vector column cache is
// rebuilt, so this is the end-to-end figure.
func BenchmarkHybridDeleteSourceThenSearch768(b *testing.B) {
	for _, docs := range []int{50_000} {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) {
			search := `SELECT id FROM HYBRID_SEARCH('bench_docs', 'embedding', 'body', 'w7 w19 w250', '` + benchVectorLiteral(768) +
				`', 10, '{"candidate_k":50,"rrf_k":60,"key_columns":["id"]}')`
			benchDeleteSourcesWith(b, newBenchCorpus(b, docs, 0, 768), 1, search)
		})
	}
}
