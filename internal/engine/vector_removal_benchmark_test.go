package engine

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The re-import scenario of a RAG corpus over real-looking embeddings: clustered
// random vectors (the vector of every row differs, unlike the shared one of
// benchCorpus), a source of a few chunks is replaced by DELETE-ing its old chunks and
// INSERT-ing the new ones, and the next search has to see the result.
//
// The timed section is the DELETE plus the first search after it. The re-inserted
// rows are appended with the timer stopped and searched once more, so every
// iteration starts from structures that are current.

type vecBenchCorpus struct {
	*benchCorpus
	centers [][]float64
}

func (c *vecBenchCorpus) vector() []float64 {
	center := c.centers[c.rng.Intn(len(c.centers))]
	v := make([]float64, c.dim)
	for i := range v {
		v[i] = center[i] + 0.35*c.rng.NormFloat64()
	}
	return v
}

func newVecBenchCorpus(b *testing.B, docs, dim int) *vecBenchCorpus {
	b.Helper()
	c := &vecBenchCorpus{benchCorpus: newBenchCorpus(b, docs, 0, dim)}
	c.centers = make([][]float64, 48)
	for i := range c.centers {
		c.centers[i] = make([]float64, dim)
		for j := range c.centers[i] {
			c.centers[i][j] = c.rng.NormFloat64()
		}
	}
	for i := range c.table.Rows {
		c.table.Rows[i][3] = c.vector()
	}
	c.table.Version++
	return c
}

// reimportVectors appends the chunks of n new sources, each with its own vectors.
func (c *vecBenchCorpus) reimportVectors(n int) {
	first := len(c.table.Rows)
	for i := 0; i < n; i++ {
		src := c.nextSrc
		c.nextSrc++
		c.live = append(c.live, src)
		for k := 0; k < benchChunksPerSource; k++ {
			c.table.Rows = append(c.table.Rows, []any{c.nextID, src, c.chunkText(150), c.vector()})
			c.nextID++
		}
	}
	c.table.Version++
	c.table.MarkDirtyFrom(first)
}

func (c *vecBenchCorpus) pickSources(n int) []string {
	var ids []string
	for k := 0; k < n; k++ {
		at := c.rng.Intn(len(c.live))
		ids = append(ids, fmt.Sprint(c.live[at]))
		c.live = append(c.live[:at], c.live[at+1:]...)
	}
	return ids
}

func vecBenchHybridSQL(dim int, index string, query []float64) string {
	parts := make([]string, len(query))
	for i, x := range query {
		parts[i] = fmt.Sprintf("%.4f", x)
	}
	opts := `{"candidate_k":50,"rrf_k":60,"key_columns":["id"]`
	if index != "" {
		opts += `,"index":"` + index + `"`
	}
	opts += `}`
	return `SELECT id FROM HYBRID_SEARCH('bench_docs', 'embedding', 'body', 'w7 w19 w250', '[` + strings.Join(parts, ",") +
		`]', 10, '` + opts + `')`
}

// benchVecDeleteSearch times DELETE of `sources` sources plus the first HYBRID_SEARCH.
func benchVecDeleteSearch(b *testing.B, docs, dim, sources int, index string) {
	b.Helper()
	c := newVecBenchCorpus(b, docs, dim)
	search := vecBenchHybridSQL(dim, index, c.vector())
	benchExec(b, c.db, search) // cold builds are not part of the measurement
	b.ReportAllocs()
	var deleteTime time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ids := c.pickSources(sources)
		runtime.GC()
		stopProfile := benchStartProfile(b, i)
		b.StartTimer()
		t0 := time.Now()
		benchExec(b, c.db, `DELETE FROM bench_docs WHERE src IN (`+strings.Join(ids, ",")+`)`)
		deleteTime += time.Since(t0)
		benchExec(b, c.db, search)
		b.StopTimer()
		stopProfile()
		c.reimportVectors(sources)
		benchExec(b, c.db, search) // bring every structure up to date outside the timer
		b.StartTimer()
	}
	b.ReportMetric(float64(deleteTime.Milliseconds())/float64(b.N), "delete-ms/op")
}

// DELETE of one source (6 chunks) then the first HYBRID_SEARCH, flat scan.
func BenchmarkVecHybridDeleteSourceThenSearchFlat(b *testing.B) {
	for _, docs := range []int{50_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d/dim=768", docs), func(b *testing.B) { benchVecDeleteSearch(b, docs, 768, 1, "") })
	}
}

// The same with 100 sources removed by one statement.
func BenchmarkVecHybridDelete100SourcesThenSearchFlat(b *testing.B) {
	for _, docs := range []int{50_000} {
		b.Run(fmt.Sprintf("rows=%d/dim=768", docs), func(b *testing.B) { benchVecDeleteSearch(b, docs, 768, 100, "") })
	}
}

// The HNSW mode: before the removal pass the whole graph was rebuilt after a
// DELETE (0.3 ms per row), so the sizes are far smaller than the flat ones.
func BenchmarkVecHybridDeleteSourceThenSearchHNSW(b *testing.B) {
	for _, docs := range []int{10_000, 20_000} {
		b.Run(fmt.Sprintf("rows=%d/dim=768", docs), func(b *testing.B) { benchVecDeleteSearch(b, docs, 768, 1, "hnsw") })
	}
}

// benchVecFollowOnly times only getVecColumnCache (flat) or getVecHNSWIndex (hnsw)
// after the DELETE, i.e. the cost of bringing the structure up to date.
func benchVecFollowOnly(b *testing.B, docs, dim, sources int, hnsw bool) {
	b.Helper()
	c := newVecBenchCorpus(b, docs, dim)
	ctx := context.Background()
	cache := getVecColumnCache("default", c.table, 3, true)
	if hnsw {
		if _, err := getVecHNSWIndex(ctx, "default", c.table, 3, "cosine", dim, cache); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ids := c.pickSources(sources)
		benchExec(b, c.db, `DELETE FROM bench_docs WHERE src IN (`+strings.Join(ids, ",")+`)`)
		runtime.GC()
		b.StartTimer()
		cache = getVecColumnCache("default", c.table, 3, true)
		if hnsw {
			if _, err := getVecHNSWIndex(ctx, "default", c.table, 3, "cosine", dim, cache); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		c.reimportVectors(sources)
		cache = getVecColumnCache("default", c.table, 3, true)
		if hnsw {
			if _, err := getVecHNSWIndex(ctx, "default", c.table, 3, "cosine", dim, cache); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}
}

func BenchmarkVecColumnCacheAfterDelete(b *testing.B) {
	for _, docs := range []int{50_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d/dim=768", docs), func(b *testing.B) { benchVecFollowOnly(b, docs, 768, 1, false) })
	}
}

func BenchmarkVecColumnCacheAfterDelete100Sources(b *testing.B) {
	b.Run("rows=50000/dim=768", func(b *testing.B) { benchVecFollowOnly(b, 50_000, 768, 100, false) })
}

func BenchmarkVecHNSWAfterDelete(b *testing.B) {
	for _, docs := range []int{10_000, 20_000} {
		b.Run(fmt.Sprintf("rows=%d/dim=768", docs), func(b *testing.B) { benchVecFollowOnly(b, docs, 768, 1, true) })
	}
}

// benchStartProfile writes a CPU profile of one timed iteration to
// $VEC_BENCH_PROFILE_DIR/iter-<i>.prof when that variable is set (merge the files
// with `go tool pprof -top *.prof`); it keeps the corpus build and the untimed
// re-import out of the profile. It returns the function that stops the profile.
func benchStartProfile(b *testing.B, i int) func() {
	dir := os.Getenv("VEC_BENCH_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	f, err := os.Create(filepath.Join(dir, fmt.Sprintf("iter-%d.prof", i)))
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		b.Fatal(err)
	}
	return func() { pprof.StopCPUProfile(); _ = f.Close() }
}

// ---------------------------------------------------------------------------
// The removal passes alone, at the size of the production table (a million rows).
// The passes do not read vectors (the flat cache shares the packed data and the
// graph only renumbers), so a small dimension keeps the memory down and the figure
// is the same.

// BenchmarkVecColumnCacheFollowPass times followVecColumnCache for one removal of
// `dead` rows from a cache of a million rows (16 dimensions).
func BenchmarkVecColumnCacheFollowPass(b *testing.B) {
	const rows, dims = 1_000_000, 16
	table := storage.NewTable("pass", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = make([][]any, rows)
	backing := make([]float64, rows*dims)
	for i := range table.Rows {
		v := backing[i*dims : (i+1)*dims : (i+1)*dims]
		for d := range v {
			v[d] = float64((i+d)%17) + 1
		}
		table.Rows[i] = []any{v}
	}
	table.Version = 1
	cache := buildVecColumnCache(table, 0, true)
	b.Run("rows=1M/cold-build", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = buildVecColumnCache(table, 0, true)
		}
	})
	for _, dead := range []int{6, 600, 30_000} {
		b.Run(fmt.Sprintf("rows=1M/dead=%d", dead), func(b *testing.B) {
			list := make([]int, dead)
			for i := range list {
				list[i] = 400_000 + i*(100_000/dead+1)
			}
			shrunk := storage.NewTable("pass", table.Cols, false)
			shrunk.Rows = table.Rows[:rows-dead]
			shrunk.Version = 2
			changes := storage.TableChanges{Dead: list, Survivors: rows - dead}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := followVecColumnCache(cache, shrunk, 0, true, changes); !ok {
					b.Fatal("not followed")
				}
			}
		})
	}
}

// BenchmarkVecHNSWRemovalPass times vecHNSWApplyRemovals on a synthetic graph of a
// million nodes with the degree of a real one (32 links on layer 0, 1/16 of the
// nodes on layer 1 with 16), for one removal of `dead` rows.
func BenchmarkVecHNSWRemovalPass(b *testing.B) {
	const nodes = 1_000_000
	levels, neighbors := syntheticHNSWGraph(nodes)
	for _, dead := range []int{6, 600, 30_000} {
		b.Run(fmt.Sprintf("nodes=1M/dead=%d", dead), func(b *testing.B) {
			list := make([]int, dead)
			for i := range list {
				list[i] = 400_001 + i*(100_000/dead+1)
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				idx := &vecHNSWIndex{metric: "cosine", dims: 16, entry: 0, maxLevel: 1,
					levels: slices.Clone(levels), neighbors: make([][][]int, nodes)}
				// The pass edits the neighbour lists in place: give it its own copy.
				for n, layers := range neighbors {
					cp := make([][]int, len(layers))
					for l, links := range layers {
						cp[l] = slices.Clone(links)
					}
					idx.neighbors[n] = cp
				}
				runtime.GC()
				b.StartTimer()
				if _, ok := vecHNSWApplyRemovals(idx, vecSearchColumnCacheEntry{}, list); !ok {
					b.Fatal("removal refused")
				}
			}
		})
	}
}

// BenchmarkVecHNSWPersistClone times the deep copy persistVecHNSWIndex makes of a
// graph of a million nodes, which every followed removal pays to keep the stored
// topology current.
func BenchmarkVecHNSWPersistClone(b *testing.B) {
	const nodes = 1_000_000
	levels, neighbors := syntheticHNSWGraph(nodes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = storage.CloneVectorTopology(levels, neighbors)
	}
}

// syntheticHNSWGraph builds the levels and neighbour lists of a graph with the
// degree of a real one: 32 links on layer 0, and 1/16 of the nodes on layer 1 with
// 16 links.
func syntheticHNSWGraph(nodes int) ([]int, [][][]int) {
	rng := rand.New(rand.NewSource(3))
	levels := make([]int, nodes)
	neighbors := make([][][]int, nodes)
	for i := range neighbors {
		level := 0
		if i%16 == 0 {
			level = 1
		}
		levels[i] = level
		layers := make([][]int, level+1)
		for l := range layers {
			degree := 32
			if l > 0 {
				degree = 16
			}
			links := make([]int, degree, degree+1)
			for k := range links {
				links[k] = rng.Intn(nodes)
				if links[k] == i {
					links[k] = (i + 1) % nodes
				}
			}
			layers[l] = links
		}
		neighbors[i] = layers
	}
	return levels, neighbors
}
