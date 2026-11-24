package engine

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The statement that dominates an importer's write path: a source is replaced by
// INSERT-ing its new chunks and DELETE-ing the old ones. The table has the shape of
// a RAG chunk table (14 columns, a vector, no PRIMARY KEY) with up to three
// secondary indexes. Only the DELETE is timed; the re-insert, the garbage
// collection and the choice of the next source happen with the timer stopped.

const removeBenchChunksPerSource = 6

type removeBenchCorpus struct {
	db      *storage.DB
	table   *storage.Table
	rows    int
	indexes int
	live    []int // sources currently in the table
	nextID  int
	nextGen int
	seq     uint64
}

var removeBenchCache struct {
	key string
	c   *removeBenchCorpus
}

const removeBenchTable = `CREATE TABLE chunks (id INT, source_id TEXT, source_kind TEXT, source_name TEXT, load_id TEXT,
	loaded_at INT, content_hash TEXT, doc_date INT, chunk_idx INT, content TEXT, embedding VECTOR,
	embed_model TEXT, chunk_hash TEXT, embedding_key TEXT)`

var removeBenchIndexStatements = []string{
	`CREATE INDEX idx_chunks_source_chunk ON chunks(source_id, chunk_idx)`,
	`CREATE UNIQUE INDEX uq_chunks_id ON chunks(id)`,
	`CREATE INDEX idx_chunks_load ON chunks(load_id)`,
}

func removeBenchRowSQL(id int, src int, gen string, chunk int, vector string) string {
	return fmt.Sprintf(`(%d, 's%d', 'doc', 'name %d', '%s', %d, 'h%d', %d, %d, 'chunk text %d of %d', '%s', 'model', 'c%d', 'k%d')`,
		id, src, src, gen, 1700000000+id, id, 1700000000, chunk, chunk, src, vector, id, id)
}

func newRemoveBenchCorpus(b *testing.B, rows, indexes int) *removeBenchCorpus {
	b.Helper()
	return newRemoveBenchCorpusWith(b, rows, indexes, removeBenchIndexStatements[:indexes])
}

// newRemoveBenchCorpusWith builds the corpus with exactly the given index
// statements; indexes only names the variant for the cache.
func newRemoveBenchCorpusWith(b *testing.B, rows, indexes int, statements []string) *removeBenchCorpus {
	b.Helper()
	key := fmt.Sprintf("%d/%d/%s", rows, indexes, strings.Join(statements, ";"))
	if removeBenchCache.key == key && removeBenchCache.c != nil {
		return removeBenchCache.c
	}
	if removeBenchCache.c != nil {
		_ = removeBenchCache.c.db.Close()
		removeBenchCache.c = nil
		removeBenchCache.key = ""
		runtime.GC()
	}
	db := storage.NewDB()
	c := &removeBenchCorpus{db: db, rows: rows, indexes: indexes}
	benchExec(b, db, removeBenchTable)
	benchExec(b, db, `INSERT INTO chunks VALUES `+removeBenchRowSQL(0, 0, "L0", 0, benchVectorLiteral(8)))
	table, err := db.Get("default", "chunks")
	if err != nil {
		b.Fatal(err)
	}
	c.table = table
	seed := table.Rows[0]
	table.Rows = table.Rows[:0]
	// Cells keep the types the engine itself stores.
	num := func(like any, n int) any {
		if _, ok := like.(int64); ok {
			return int64(n)
		}
		return n
	}
	for len(table.Rows) < rows {
		src := len(c.live)
		chunks := 0
		for k := 0; k < removeBenchChunksPerSource && len(table.Rows) < rows; k++ {
			chunks++
			id := c.nextID
			c.nextID++
			row := make([]any, len(seed))
			copy(row, seed)
			row[0] = num(seed[0], id)
			row[1] = fmt.Sprintf("s%d", src)
			row[3] = fmt.Sprintf("name %d", src)
			row[4] = "L0"
			row[5] = num(seed[5], 1700000000+id)
			row[6] = fmt.Sprintf("h%d", id)
			row[8] = num(seed[8], k)
			row[9] = fmt.Sprintf("chunk text %d of %d", k, src)
			row[12] = fmt.Sprintf("c%d", id)
			row[13] = fmt.Sprintf("k%d", id)
			table.Rows = append(table.Rows, row)
		}
		// A last source with fewer chunks than the others is never picked, so
		// every timed statement removes the same number of rows.
		if chunks == removeBenchChunksPerSource {
			c.live = append(c.live, src)
		}
	}
	table.Version++
	table.MarkDirtyFrom(-1)
	for _, stmt := range statements {
		benchExec(b, db, stmt)
	}
	removeBenchCache.key = key
	removeBenchCache.c = c
	return c
}

// pick returns n distinct live sources chosen pseudo-randomly (deterministic).
func (c *removeBenchCorpus) pick(n int) []int {
	out := make([]int, 0, n)
	for len(out) < n {
		c.seq = c.seq*6364136223846793005 + 1442695040888963407
		at := int(c.seq>>33) % len(c.live)
		dup := false
		for _, s := range out {
			if s == c.live[at] {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, c.live[at])
		}
	}
	return out
}

func (c *removeBenchCorpus) insertSources(b *testing.B, sources []int, gen string) {
	b.Helper()
	vector := benchVectorLiteral(8)
	var sb strings.Builder
	sb.WriteString(`INSERT INTO chunks VALUES `)
	first := true
	for _, src := range sources {
		for k := 0; k < removeBenchChunksPerSource; k++ {
			if !first {
				sb.WriteString(", ")
			}
			first = false
			sb.WriteString(removeBenchRowSQL(c.nextID, src, gen, k, vector))
			c.nextID++
		}
	}
	benchExec(b, c.db, sb.String())
}

// benchRemove runs the timed statement. build returns the DELETE for the sources it
// is given; replace says whether the statement keeps one generation of every source
// (DELETE ... AND load_id <> new) or removes the sources completely.
func benchRemove(b *testing.B, rows, indexes, sources int, keepNewest bool) {
	c := newRemoveBenchCorpus(b, rows, indexes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		picked := c.pick(sources)
		c.nextGen++
		gen := fmt.Sprintf("G%d", c.nextGen)
		var stmt string
		if keepNewest {
			c.insertSources(b, picked, gen)
			stmt = fmt.Sprintf(`DELETE FROM chunks WHERE source_id = 's%d' AND load_id <> '%s'`, picked[0], gen)
		} else {
			parts := make([]string, len(picked))
			for k, s := range picked {
				parts[k] = fmt.Sprintf("'s%d'", s)
			}
			stmt = `DELETE FROM chunks WHERE source_id IN (` + strings.Join(parts, ",") + `)`
		}
		parsed := mustParse(stmt)
		runtime.GC()
		b.StartTimer()
		rs, err := Execute(b.Context(), c.db, "default", parsed)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		want := removeBenchChunksPerSource * sources
		if keepNewest {
			want = removeBenchChunksPerSource
		}
		if got := rs.Rows[0]["deleted"]; got != want {
			b.Fatalf("deleted %v rows, want %d", got, want)
		}
		if !keepNewest {
			c.insertSources(b, picked, gen)
		}
		b.StartTimer()
	}
}

// DELETE FROM chunks WHERE source_id = 'x' AND load_id <> 'y' (6 of N rows).
func BenchmarkDeleteReplacedSource(b *testing.B) {
	for _, rows := range removeBenchRows() {
		for _, indexes := range []int{0, 1, 3} {
			b.Run(fmt.Sprintf("rows=%d/indexes=%d", rows, indexes), func(b *testing.B) { benchRemove(b, rows, indexes, 1, true) })
		}
	}
}

// DELETE FROM chunks WHERE source_id IN (100 ids) (600 of N rows).
func BenchmarkDelete100Sources(b *testing.B) {
	for _, rows := range removeBenchRows() {
		for _, indexes := range []int{0, 1, 3} {
			b.Run(fmt.Sprintf("rows=%d/indexes=%d", rows, indexes), func(b *testing.B) { benchRemove(b, rows, indexes, 100, false) })
		}
	}
}

func (c *removeBenchCorpus) insertSourcesSQL(sources []int, gen string) string {
	vector := benchVectorLiteral(8)
	var sb strings.Builder
	sb.WriteString(`INSERT INTO chunks VALUES `)
	first := true
	for _, src := range sources {
		for k := 0; k < removeBenchChunksPerSource; k++ {
			if !first {
				sb.WriteString(", ")
			}
			first = false
			sb.WriteString(removeBenchRowSQL(c.nextID, src, gen, k, vector))
			c.nextID++
		}
	}
	return sb.String()
}

// BenchmarkReplaceSourceCycle times what the importer holds the write lock for when
// it replaces one source: the INSERT of its six new chunks and the DELETE of the six
// old ones. DELETE leaves the row slice with no spare capacity unless it reserves
// some, and the INSERT after it pays for that, so the two are timed together.
func BenchmarkReplaceSourceCycle(b *testing.B) {
	for _, rows := range removeBenchRows() {
		for _, indexes := range []int{0, 1, 3} {
			b.Run(fmt.Sprintf("rows=%d/indexes=%d", rows, indexes), func(b *testing.B) {
				c := newRemoveBenchCorpus(b, rows, indexes)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					picked := c.pick(1)
					c.nextGen++
					gen := fmt.Sprintf("G%d", c.nextGen)
					insert := mustParse(c.insertSourcesSQL(picked, gen))
					del := mustParse(fmt.Sprintf(`DELETE FROM chunks WHERE source_id = 's%d' AND load_id <> '%s'`, picked[0], gen))
					runtime.GC()
					b.StartTimer()
					_, err := Execute(b.Context(), c.db, "default", insert)
					if err == nil {
						var rs *ResultSet
						if rs, err = Execute(b.Context(), c.db, "default", del); err == nil && rs.Rows[0]["deleted"] != removeBenchChunksPerSource {
							b.Fatalf("deleted %v rows", rs.Rows[0]["deleted"])
						}
					}
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			})
		}
	}
}

// BenchmarkDeleteLowSelectivity is the case index candidates are worst at: every
// row has the same source_kind, so an index on it names all N rows and the
// predicate (which then matches none) is evaluated on all of them, after the index
// has been read and the list sorted. "scan" has no index and is the reference.
func BenchmarkDeleteLowSelectivity(b *testing.B) {
	for _, rows := range removeBenchRows() {
		for _, variant := range []struct {
			name       string
			statements []string
		}{
			{"scan", nil},
			{"index", []string{`CREATE INDEX idx_chunks_kind ON chunks(source_kind)`}},
		} {
			b.Run(fmt.Sprintf("rows=%d/%s", rows, variant.name), func(b *testing.B) {
				c := newRemoveBenchCorpusWith(b, rows, len(variant.statements)+100, variant.statements)
				stmt := mustParse(`DELETE FROM chunks WHERE source_kind = 'doc' AND load_id = 'nomatch'`)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					runtime.GC()
					b.StartTimer()
					rs, err := Execute(b.Context(), c.db, "default", stmt)
					if err != nil {
						b.Fatal(err)
					}
					if rs.Rows[0]["deleted"] != 0 {
						b.Fatalf("deleted %v rows", rs.Rows[0]["deleted"])
					}
				}
			})
		}
	}
}

// removeBenchRows are the table sizes of the benchmarks above. A larger one (the
// 1M-row table of a real corpus needs about 1 GB) is opt-in:
// TINYSQL_BENCH_ROWS=100000,400000,1000000.
func removeBenchRows() []int {
	rows := []int{100_000, 400_000}
	if env := os.Getenv("TINYSQL_BENCH_ROWS"); env != "" {
		rows = rows[:0]
		for _, field := range strings.Split(env, ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(field)); err == nil && n > 0 {
				rows = append(rows, n)
			}
		}
	}
	return rows
}
