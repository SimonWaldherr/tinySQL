package engine

import (
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"testing"
)

// The dead-rank lookup agrees with a plain count, at the block boundaries too.
func TestVecDeadRankMatchesACount(t *testing.T) {
	nodes := 5000
	lists := [][]int{
		{0}, {nodes - 1}, {255, 256, 257}, {256}, {255}, {511, 512}, {0, 1, 2, 3},
		{1 << vecDeadRankShift, 2 << vecDeadRankShift, 3<<vecDeadRankShift - 1},
	}
	rng := rand.New(rand.NewSource(3))
	for k := 0; k < 20; k++ {
		set := map[int]bool{}
		for i := 0; i < 1+rng.Intn(400); i++ {
			set[rng.Intn(nodes)] = true
		}
		var list []int
		for d := range set {
			list = append(list, d)
		}
		slices.Sort(list)
		lists = append(lists, list)
	}
	for _, dead := range lists {
		rank := newVecDeadRank(dead, nodes)
		below := 0
		for id := 0; id < nodes; id++ {
			isDead := below < len(dead) && dead[below] == id
			got, gotDead := rank.lookup(id)
			if got != below || gotDead != isDead {
				t.Fatalf("dead %v: lookup(%d) = %d, %v; want %d, %v", dead, id, got, gotDead, below, isDead)
			}
			if isDead {
				below++
			}
		}
	}
}

// topologyByID is the graph keyed by the id column instead of the row position:
// node id -> layer -> set of neighbour ids, plus the entry and the delta rows.
type topologyByID struct {
	nodes map[int][]map[int]bool
	entry int
	delta map[int]bool
}

func (f *hfFixture) topology(idx *vecHNSWIndex) topologyByID {
	idOf := func(row int) int { return f.table.Rows[row][0].(int) }
	topo := topologyByID{nodes: map[int][]map[int]bool{}, entry: -1, delta: map[int]bool{}}
	for row, layers := range idx.neighbors {
		if len(layers) == 0 {
			continue
		}
		sets := make([]map[int]bool, len(layers))
		for layer, links := range layers {
			sets[layer] = map[int]bool{}
			for _, nb := range links {
				sets[layer][idOf(nb)] = true
			}
		}
		topo.nodes[idOf(row)] = sets
	}
	if idx.entry >= 0 {
		topo.entry = idOf(idx.entry)
	}
	for _, row := range idx.deltaRows {
		topo.delta[idOf(row)] = true
	}
	return topo
}

// The renumbering is exact: after a removal every surviving node keeps precisely
// the neighbours it had, minus the dead ones, expressed as the same ids (and the
// entry point and the delta rows are the same rows). Invariants and recall cannot
// see an edge that now points at the row next door.
func TestVecHNSWFollowRenumberingIsExact(t *testing.T) {
	cases := []struct {
		name  string
		rows  int
		chunk int // 0 keeps the default
		ids   string
	}{
		{"serial block boundaries", 1600, 0, "0,1,255,256,257,511,512,513,1023,1024,1598,1599"},
		{"parallel block boundaries", 3000, 64, "0,255,256,257,511,512,1000,1001,1002,2047,2048,2998,2999"},
		{"parallel many", 3000, 50, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			previous := runtime.GOMAXPROCS(4)
			t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
			if tc.chunk != 0 {
				saved := vecHNSWFollowChunk
				vecHNSWFollowChunk = tc.chunk
				t.Cleanup(func() { vecHNSWFollowChunk = saved })
			}
			f := newHF(t, tc.rows, "cosine")
			idx, _ := f.warm()
			// Two rows get a changed vector so the delta rows are in play too.
			for _, id := range []int{700, 2500} {
				if id < tc.rows {
					v := slices.Clone(f.table.Rows[id][hfEmbedding].([]float64))
					for i := range v {
						v[i] = 0.25 - v[i]
					}
					f.exec(fmt.Sprintf(`UPDATE hf SET embedding = '%s' WHERE id = %d`, hfLiteral(v), id))
				}
			}
			idx, _ = f.warm()
			before := f.topology(idx)
			wantEntry := before.entry
			if tc.ids != "" {
				f.exec(`DELETE FROM hf WHERE id IN (` + tc.ids + `)`)
			} else {
				f.deleteSources(40)
			}
			survivor := map[int]bool{}
			for _, row := range f.table.Rows {
				survivor[row[0].(int)] = true
			}
			for id := range before.nodes {
				if !survivor[id] {
					f.deleted[id] = true
				}
			}
			idx, cache := f.follow()
			hnswCheckInvariants(t, idx, f.table, cache, tc.name)
			after := f.topology(idx)
			if len(after.nodes) == 0 {
				t.Fatal("no nodes left")
			}
			for id, layers := range before.nodes {
				got, kept := after.nodes[id]
				if kept != survivor[id] {
					t.Fatalf("id %d: survives %v, node kept %v", id, survivor[id], kept)
				}
				if !kept {
					continue
				}
				if len(got) != len(layers) {
					t.Fatalf("id %d: %d layers, had %d", id, len(got), len(layers))
				}
				for layer, set := range layers {
					want := map[int]bool{}
					for nb := range set {
						if survivor[nb] {
							want[nb] = true
						}
					}
					if len(want) != len(got[layer]) {
						t.Fatalf("id %d layer %d: neighbours %v, want %v", id, layer, got[layer], want)
					}
					for nb := range want {
						if !got[layer][nb] {
							t.Fatalf("id %d layer %d: neighbour %d is missing (%v, want %v)", id, layer, nb, got[layer], want)
						}
					}
				}
			}
			if survivor[wantEntry] && after.entry != wantEntry {
				t.Fatalf("the entry point moved from id %d to %d although it survived", wantEntry, after.entry)
			}
			for id := range before.delta {
				if survivor[id] != after.delta[id] {
					t.Fatalf("delta id %d: survives %v, still a delta row %v", id, survivor[id], after.delta[id])
				}
			}
			if len(after.delta) > len(before.delta) {
				t.Fatalf("delta rows grew: %v -> %v", before.delta, after.delta)
			}
			f.recall(idx, cache, 10) // answers are valid rows, dead ones never
		})
	}
}

// The parallel pass over a bigger graph with a followed removal answers like the
// serial pass over the same one: same topology, whatever the worker count.
func TestVecHNSWFollowParallelEqualsSerial(t *testing.T) {
	run := func(procs, chunk int) topologyByID {
		previous := runtime.GOMAXPROCS(procs)
		saved := vecHNSWFollowChunk
		vecHNSWFollowChunk = chunk
		defer func() { runtime.GOMAXPROCS(previous); vecHNSWFollowChunk = saved }()
		f := newHF(t, 3000, "cosine")
		f.warm()
		f.deleteSources(30)
		idx, _ := f.follow()
		return f.topology(idx)
	}
	serial := run(1, 1<<20)
	parallel := run(4, 37)
	if len(serial.nodes) != len(parallel.nodes) || serial.entry != parallel.entry {
		t.Fatalf("serial: %d nodes, entry %d; parallel: %d nodes, entry %d", len(serial.nodes), serial.entry, len(parallel.nodes), parallel.entry)
	}
	for id, layers := range serial.nodes {
		other := parallel.nodes[id]
		if len(other) != len(layers) {
			t.Fatalf("id %d: %d layers vs %d", id, len(layers), len(other))
		}
		for layer := range layers {
			if len(layers[layer]) != len(other[layer]) {
				t.Fatalf("id %d layer %d differs", id, layer)
			}
			for nb := range layers[layer] {
				if !other[layer][nb] {
					t.Fatalf("id %d layer %d: neighbour %d missing in the parallel pass", id, layer, nb)
				}
			}
		}
	}
}
