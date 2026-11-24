package engine

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// hfSourceCorpus draws documents the way an import loop sees them: a source is a
// run of chunks that all come from one topic, so removing a source removes a whole
// neighbourhood of the embedding space, and a re-import brings a new run of chunks
// (of any topic) back. The previous recall tests removed rows of unrelated topics.
type hfSourceCorpus struct {
	rng     *rand.Rand
	proj    [][]float64
	centres [][]float64
}

func newHFSourceCorpus(seed int64) *hfSourceCorpus {
	const rank, topics = 12, 40
	rng := rand.New(rand.NewSource(seed))
	c := &hfSourceCorpus{rng: rng}
	c.proj = make([][]float64, hfDims)
	for d := range c.proj {
		c.proj[d] = make([]float64, rank)
		for r := range c.proj[d] {
			c.proj[d][r] = rng.NormFloat64()
		}
	}
	c.centres = make([][]float64, topics)
	for t := range c.centres {
		c.centres[t] = make([]float64, rank)
		for r := range c.centres[t] {
			c.centres[t][r] = 2 * rng.NormFloat64()
		}
	}
	return c
}

// chunk draws one vector of the topic.
func (c *hfSourceCorpus) chunk(topic int) []float64 {
	z := make([]float64, len(c.centres[topic]))
	for r := range z {
		z[r] = c.centres[topic][r] + c.rng.NormFloat64()
	}
	vec := make([]float64, hfDims)
	for d := range vec {
		for r, w := range c.proj[d] {
			vec[d] += w * z[r]
		}
		vec[d] += 0.1 * c.rng.NormFloat64()
	}
	return vec
}

// The re-import loop on source-shaped data: delete the chunks of one source,
// insert the chunks of a new one, search; up to the bound that makes a rebuild.
// Recall is measured at several points, at k=10 and at the larger k a hybrid search
// asks its vector half for, against the flat scan, and compared with a graph built
// from scratch over the same rows.
func TestVecHNSWFollowRecallOnReimportedSources(t *testing.T) {
	const chunks = 32 // chunks per source
	sources := 190    // 6080 rows
	cycles := 17      // 17 of 190 sources removed in all: just under the 10 percent bound
	if testing.Short() {
		sources, cycles = 100, 8
	}
	for _, metric := range []string{"cosine", "l2"} {
		t.Run(metric, func(t *testing.T) {
			corpus := newHFSourceCorpus(7)
			source := storage.NewTable("hf_source", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "embedding", Type: storage.VectorType}}, false)
			topicOf := map[int]int{}
			for s := 0; s < sources; s++ {
				topicOf[s] = corpus.rng.Intn(len(corpus.centres))
				for c := 0; c < chunks; c++ {
					source.Rows = append(source.Rows, []any{s*chunks + c, corpus.chunk(topicOf[s])})
				}
			}
			queries := make([][]float64, 80)
			for i := range queries {
				queries[i] = corpus.chunk(corpus.rng.Intn(len(corpus.centres)))
			}
			f := newHFFrom(t, source, queries, len(source.Rows), metric)
			// newHFFrom numbers sources by hfPerSource; renumber to this test's size.
			for i, row := range f.table.Rows {
				row[1] = i / chunks
			}
			f.live = f.live[:0]
			for s := 0; s < sources; s++ {
				f.live = append(f.live, s)
			}
			f.warm()
			follows := vecHNSWFollowCount.Load()
			nextSource := sources
			check := map[int]bool{cycles / 4: true, cycles / 2: true, cycles: true}
			for cycle := 1; cycle <= cycles; cycle++ {
				victim := f.live[f.rng.Intn(len(f.live))]
				f.live = removeInt(f.live, victim)
				for _, row := range f.table.Rows {
					if row[1].(int) == victim {
						f.deleted[row[0].(int)] = true
					}
				}
				f.exec(fmt.Sprintf(`DELETE FROM hf WHERE src = %d`, victim))
				topic := corpus.rng.Intn(len(corpus.centres))
				for c := 0; c < chunks; c++ {
					f.exec(fmt.Sprintf(`INSERT INTO hf VALUES (%d, %d, '%s')`, f.nextID, nextSource, hfLiteral(corpus.chunk(topic))))
					f.nextID++
				}
				f.live = append(f.live, nextSource)
				nextSource++
				idx, cache := f.warm()
				if !check[cycle] {
					continue
				}
				hnswCheckInvariants(t, idx, f.table, cache, fmt.Sprintf("cycle %d", cycle))
				fresh, freshCache := f.freshGraph()
				for _, k := range []int{10, 50} {
					got, want := f.recall(idx, cache, k), f.recall(fresh, freshCache, k)
					t.Logf("%s, cycle %d (%d of %d sources replaced): recall@%d followed %.4f, fresh build %.4f", metric, cycle, cycle, sources, k, got, want)
					if got < want-0.05 || got < 0.9 {
						t.Errorf("cycle %d: recall@%d of the followed graph is %.4f against %.4f for a fresh build", cycle, k, got, want)
					}
				}
				reach, nodes := hnswReachable(idx)
				t.Logf("%s, cycle %d: %d of %d nodes reachable from the entry on layer 0", metric, cycle, reach, nodes)
				if float64(reach) < 0.99*float64(nodes) {
					t.Errorf("cycle %d: only %d of %d nodes are reachable", cycle, reach, nodes)
				}
			}
			if got := vecHNSWFollowCount.Load() - follows; got != int64(cycles) {
				t.Errorf("%d of %d removals were followed: the rest rebuilt the graph", got, cycles)
			}
		})
	}
}

func removeInt(list []int, v int) []int {
	for i, x := range list {
		if x == v {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}
