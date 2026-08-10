// BenchmarkSecondaryIndexInsertRandom* measures Table.InsertSecondaryIndexRow
// throughput on an already-populated non-unique secondary index, at
// existingRows = 1,000 / 10,000 / 100,000 pre-existing rows, inserting new
// keys in random order across the whole existing key range rather than
// ascending. This is the benchmark stage 3 of the secondary-index skip-list
// work exists to add: proof the fix changes the *scaling* behavior of
// one-at-a-time inserts (flat/O(log n) per insert instead of growing
// linearly with table size), not just a constant-factor improvement.
//
// This deliberately calls Table.InsertSecondaryIndexRow directly rather than
// going through engine.Execute's SQL INSERT path: any statement that
// mutates a table with a secondary index takes the general
// SnapshotForTableStatement rollback path (see
// internal/engine/exec_statement.go's appendOnlySnapshotTarget /
// tableScopedSnapshotTarget choice), which clones every index on the table
// once per statement (cloneSecondaryIndexes) for MVCC rollback -- an O(n)
// cost by design, present identically before and after this fix (the old
// sorted-slice Entries clone was already O(n); the new structural
// SkipList.Clone is also deliberately O(n), see SecondaryIndex.clone's doc
// comment). That per-statement clone would swamp -- and hide -- the O(log n)
// per-insert improvement this benchmark exists to demonstrate, exactly the
// concern BenchmarkVecHNSWIncrementalInsert's doc comment
// (internal/engine/vector_hnsw_incremental_benchmark_test.go) raises about
// calling getVecHNSWIndex directly instead of going through SQL. Calling
// InsertSecondaryIndexRow directly isolates the one mechanism stage 1/2
// actually changed. (Confirmed empirically: an earlier draft of this
// benchmark went through engine.Execute's SQL INSERT and showed B/op and
// ns/op scaling linearly with existingRows even on the post-fix skip-list
// binary -- the per-statement index clone, not the index insert itself.)
package storage

import (
	"math/rand"
	"testing"
)

// secondaryIndexInsertScalingStride spaces seed keys far enough apart that
// every one-at-a-time insert performed by
// benchmarkSecondaryIndexInsertRandom lands in a distinct gap between two
// existing keys (bucket = i*stride for the i'th seed row) instead of at the
// tail of the key range. Landing at the tail is the cheap case even for the
// pre-fix O(n) sorted-slice algorithm (the shift-copy after the insertion
// point is empty, since pos == len); this benchmark exists specifically to
// measure the expensive case a random key order produces: a new key whose
// sorted position sits somewhere in the middle of however many keys already
// exist.
const secondaryIndexInsertScalingStride = 1 << 20

// setupSecondaryIndexInsertScalingTable builds a table with one non-unique
// secondary index (idx_ins_bucket on column "bucket", matching the
// idx_updates_bucket naming convention used by
// internal/engine/perf_benchmark_test.go's BenchmarkUpdateByPrimaryKey) and
// seeds it with existingRows rows whose bucket values are existingRows
// distinct keys evenly spaced by secondaryIndexInsertScalingStride.
//
// Rows are assigned directly and the index is built with one
// CreateSecondaryIndex call rather than existingRows one-at-a-time inserts:
// with the pre-fix O(n) sorted-slice insert, seeding e.g. 100,000 rows via
// one-at-a-time inserts would itself cost O(n^2) and make the "before"
// comparison binary's setup alone impractically slow. Setup cost is excluded
// from the timed portion regardless (b.ResetTimer runs only after this
// returns).
func setupSecondaryIndexInsertScalingTable(b *testing.B, existingRows int) (*Table, []string) {
	b.Helper()
	table := NewTable("ins_bench", []Column{
		{Name: "id", Type: IntType},
		{Name: "bucket", Type: IntType},
	}, false)
	table.Rows = make([][]any, existingRows)
	for i := 0; i < existingRows; i++ {
		table.Rows[i] = []any{float64(i), float64(i * secondaryIndexInsertScalingStride)}
	}
	if err := table.CreateSecondaryIndex("idx_ins_bucket", []string{"bucket"}, false); err != nil {
		b.Fatal(err)
	}
	return table, table.SortedIndexNames()
}

// benchmarkSecondaryIndexInsertRandom times b.N calls to
// InsertSecondaryIndexRow, one per newly-appended row, each carrying a new,
// previously-absent bucket value. The sequence of gaps used (gapOrder, a
// random permutation of all existingRows gaps, cycled and disambiguated by
// roundOf when b.N exceeds existingRows) makes every new key's sorted
// position effectively uniform across the whole existing key range -- not
// ascending, not always at the tail -- exactly the "random key order" case
// that was O(n) per insert / O(n^2) total for the whole benchmark before
// this fix. Sequential/ascending keys were already fast before this change
// (every insert lands at the tail, the cheap case for the old sorted-slice
// algorithm too), so this benchmark deliberately does not cover that case.
//
// Comparing this benchmark's ns/op and B/op across existingRows =
// 1,000/10,000/100,000 is the scaling evidence this stage exists to
// produce: on a "before" binary (predating the skip list), cost should grow
// with existingRows; on the "after" (skip-list-backed) binary it should stay
// roughly flat (O(log n) per insert).
func benchmarkSecondaryIndexInsertRandom(b *testing.B, existingRows int) {
	table, names := setupSecondaryIndexInsertScalingTable(b, existingRows)

	rng := rand.New(rand.NewSource(7))
	gapOrder := rng.Perm(existingRows)
	roundOf := make([]int, existingRows)

	rows := make([][]any, b.N)
	for i := 0; i < b.N; i++ {
		gap := gapOrder[i%existingRows]
		round := roundOf[gap]
		roundOf[gap]++
		key := gap*secondaryIndexInsertScalingStride + 1 + round
		rows[i] = []any{float64(existingRows + i), float64(key)}
	}

	// Pre-grow table.Rows' capacity to existingRows+b.N so every append
	// inside the timed loop below is a guaranteed no-realloc append.
	// setupSecondaryIndexInsertScalingTable leaves table.Rows at
	// len == cap == existingRows, so without this the very first timed
	// append would trigger a one-time O(existingRows) grow-and-copy of the
	// entire existing Rows backing array -- a real cost, but one that
	// belongs to slice growth, not to the InsertSecondaryIndexRow call this
	// benchmark measures. Amortized over only b.N iterations (not
	// existingRows+b.N), that one-time copy swamps ns/op and B/op at
	// exactly the small -benchtime=Nx iteration counts this session's A/B
	// comparison methodology relies on for low-noise before/after
	// comparisons.
	grown := make([][]any, existingRows, existingRows+b.N)
	copy(grown, table.Rows)
	table.Rows = grown

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowID := len(table.Rows)
		table.Rows = append(table.Rows, rows[i])
		if err := table.InsertSecondaryIndexRow(rowID, rows[i], names); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSecondaryIndexInsertRandom1e3Existing(b *testing.B) {
	benchmarkSecondaryIndexInsertRandom(b, 1_000)
}

func BenchmarkSecondaryIndexInsertRandom1e4Existing(b *testing.B) {
	benchmarkSecondaryIndexInsertRandom(b, 10_000)
}

func BenchmarkSecondaryIndexInsertRandom1e5Existing(b *testing.B) {
	benchmarkSecondaryIndexInsertRandom(b, 100_000)
}
