package storage

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
)

// benchmarkSkipListInsertRandom builds n random keys once (outside the timed
// portion) and times inserting them into a fresh skip list, one b.N loop's
// worth of full builds at a time. Per-insert cost (b.Elapsed / (b.N * n)) is
// what should scale as log(n), not n or n^2 -- run with -benchtime and
// compare N=1000 vs N=10000 vs N=100000 (see the three benchmarks below) to
// see the growth rate directly. A per-op cost that merely creeps up slightly
// from N=1000 to N=100000 (as expected of O(log n)) rather than scaling
// roughly 100x (O(n)) or 10000x (O(n^2)) is the proof this stage is after.
func benchmarkSkipListInsertRandom(b *testing.B, n int) {
	rng := rand.New(rand.NewSource(1))
	keys := make([][]byte, n)
	perm := rng.Perm(n)
	for i, k := range perm {
		keys[i] = intKey(k)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewSkipList()
		for _, k := range keys {
			s.Insert(k, 0)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(n), "keys")
}

func BenchmarkSkipListInsert1e3(b *testing.B) { benchmarkSkipListInsertRandom(b, 1_000) }
func BenchmarkSkipListInsert1e4(b *testing.B) { benchmarkSkipListInsertRandom(b, 10_000) }
func BenchmarkSkipListInsert1e5(b *testing.B) { benchmarkSkipListInsertRandom(b, 100_000) }

// BenchmarkSkipListGetRandom measures point-lookup cost on a fixed-size
// skip list, which should likewise scale as log(n) rather than n.
func benchmarkSkipListGetRandom(b *testing.B, n int) {
	rng := rand.New(rand.NewSource(2))
	s := NewSkipList()
	for _, k := range rng.Perm(n) {
		s.Insert(intKey(k), k)
	}
	lookups := make([][]byte, 4096)
	for i := range lookups {
		lookups[i] = intKey(rng.Intn(n))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(lookups[i%len(lookups)])
	}
}

func BenchmarkSkipListGet1e3(b *testing.B) { benchmarkSkipListGetRandom(b, 1_000) }
func BenchmarkSkipListGet1e4(b *testing.B) { benchmarkSkipListGetRandom(b, 10_000) }
func BenchmarkSkipListGet1e5(b *testing.B) { benchmarkSkipListGetRandom(b, 100_000) }

// legacySliceInsertSecondaryIndexRowID reproduces, verbatim, the O(n)
// sorted-insert-with-dedup algorithm secondary_index.go's
// insertSecondaryIndexRowID used before stage 2 rewired InsertSecondaryIndexRow
// onto the skip list. Production code no longer has any code path shaped
// like this; it is kept here, test-only, purely so
// BenchmarkSecondaryIndexSliceInsert* can keep comparing the skip list
// against the exact old algorithm it replaced.
func legacySliceInsertSecondaryIndexRowID(index *SecondaryIndex, key []byte, rowID int) {
	pos := sort.Search(len(index.Entries), func(i int) bool {
		return bytes.Compare(index.Entries[i].Key, key) >= 0
	})
	if pos == len(index.Entries) || !bytes.Equal(index.Entries[pos].Key, key) {
		index.Entries = append(index.Entries, IndexEntry{})
		copy(index.Entries[pos+1:], index.Entries[pos:])
		index.Entries[pos] = IndexEntry{Key: append([]byte(nil), key...)}
	}
	rowIDs := index.Entries[pos].RowIDs
	rowPos := sort.SearchInts(rowIDs, rowID)
	if rowPos < len(rowIDs) && rowIDs[rowPos] == rowID {
		return
	}
	rowIDs = append(rowIDs, 0)
	copy(rowIDs[rowPos+1:], rowIDs[rowPos:])
	rowIDs[rowPos] = rowID
	index.Entries[pos].RowIDs = rowIDs
}

// BenchmarkSecondaryIndexSliceInsertRandom benchmarks the *old* O(n)
// sorted-slice insert (legacySliceInsertSecondaryIndexRowID, see above) side
// by side with the new skip list, on the same random key sequence and the
// same N values, so a diff of this benchmark's own before/after run is the
// most direct side-by-side evidence available: it proves the replacement
// data structure alone is the O(log n) improvement stage 2/3 deliver.
func benchmarkSecondaryIndexSliceInsertRandom(b *testing.B, n int) {
	rng := rand.New(rand.NewSource(1))
	keys := make([][]byte, n)
	perm := rng.Perm(n)
	for i, k := range perm {
		keys[i] = intKey(k)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := &SecondaryIndex{Name: "bench"}
		for _, k := range keys {
			legacySliceInsertSecondaryIndexRowID(idx, k, 0)
		}
	}
}

func BenchmarkSecondaryIndexSliceInsert1e3(b *testing.B) {
	benchmarkSecondaryIndexSliceInsertRandom(b, 1_000)
}
func BenchmarkSecondaryIndexSliceInsert1e4(b *testing.B) {
	benchmarkSecondaryIndexSliceInsertRandom(b, 10_000)
}
func BenchmarkSecondaryIndexSliceInsert1e5(b *testing.B) {
	benchmarkSecondaryIndexSliceInsertRandom(b, 100_000)
}
