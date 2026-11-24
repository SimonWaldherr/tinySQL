package engine

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func ftsMaintenanceRows(rng *rand.Rand, n, vocab int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		length := 1 + rng.Intn(12)
		text := ""
		for j := 0; j < length; j++ {
			text += fmt.Sprintf("w%d ", rng.Intn(vocab))
		}
		if i%23 == 5 {
			text = "" // a document without tokens
		}
		rows[i] = []any{text}
	}
	return rows
}

// ftsIndexesEqual compares everything a query or a persisted copy can observe.
func ftsIndexesEqual(t *testing.T, got, want *storage.FTSIndex, label string) {
	t.Helper()
	type view struct {
		Format, BuiltRows, NumDocs int
		Docs                       []storage.FTSDocument
		AvgDocLen, TotalDocLen     float64
		Postings, PostingCounts    map[string][]int32
		PostingBlocks              map[string][]storage.FTSPostingBlock
		TermIDs                    map[string]int32
		DocTermIDs, DocTermCounts  []int32
		DocTokenIDs                []int32
	}
	mk := func(i *storage.FTSIndex) view {
		return view{i.Format, i.BuiltRows, i.NumDocs, i.Docs, i.AvgDocLen, i.TotalDocLen, i.Postings, i.PostingCounts, i.PostingBlocks,
			i.TermIDs, i.DocTermIDs, i.DocTermCounts, i.DocTokenIDs}
	}
	if !reflect.DeepEqual(mk(got), mk(want)) {
		t.Fatalf("%s: index differs from a cold build (docs %d vs %d, terms %d vs %d, tokens %d vs %d)", label,
			len(got.Docs), len(want.Docs), len(got.TermIDs), len(want.TermIDs), len(got.DocTokenIDs), len(want.DocTokenIDs))
	}
}

func requireTermNamesInverse(t *testing.T, index *storage.FTSIndex, label string) {
	t.Helper()
	names := index.TermNames()
	if len(names) != len(index.TermIDs) {
		t.Fatalf("%s: %d term names for %d terms", label, len(names), len(index.TermIDs))
	}
	for term, id := range index.TermIDs {
		if names[id] != term {
			t.Fatalf("%s: TermNames()[%d] = %q, want %q", label, id, names[id], term)
		}
	}
}

// Extending an index in steps of every size (map-based counting for a few
// rows, dense counting and per-term batches for many, the packed cold build
// from nothing) yields exactly the index a single cold build does, keeps its
// term-name table in step and stays valid by the full structural check.
func TestFTSExtendStepsMatchColdBuild(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		rng := rand.New(rand.NewSource(seed))
		rows := ftsMaintenanceRows(rng, 520, 40+int(seed)*30)
		cols := []int{0}

		cold := storage.NewTable("fts_cold", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
		cold.Rows = rows
		coldIndex := &storage.FTSIndex{}
		ftsExtendPersistent(cold, cols, coldIndex)

		live := storage.NewTable("fts_live", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
		liveIndex := &storage.FTSIndex{}
		steps := []int{0, 3, 1, 1, 70, 2, 64, 5, 100, 1, 63, 1, 120}
		for n, step := range steps {
			next := len(live.Rows) + step
			if n == len(steps)-1 || next > len(rows) {
				next = len(rows)
			}
			live.Rows = append(live.Rows, rows[len(live.Rows):next]...)
			live.Version++
			ftsExtendPersistent(live, cols, liveIndex)
			label := fmt.Sprintf("seed %d step %d (%d rows)", seed, n, len(live.Rows))
			if !ftsValidateStructure(liveIndex) {
				t.Fatalf("%s: structure invalid", label)
			}
			requireTermNamesInverse(t, liveIndex, label)
			if !liveIndex.Verified() {
				t.Fatalf("%s: a built index must be verified", label)
			}
			// The same rows built cold.
			ref := storage.NewTable("fts_ref", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
			ref.Rows = live.Rows
			refIndex := &storage.FTSIndex{}
			ftsExtendPersistent(ref, cols, refIndex)
			ftsIndexesEqual(t, liveIndex, refIndex, label)
		}
		ftsIndexesEqual(t, liveIndex, coldIndex, fmt.Sprintf("seed %d final", seed))
	}
}

// A refresh keeps the verified state and the term-name table valid, including
// when new terms appear, and agrees with a cold build over the final rows.
func TestFTSRefreshKeepsMaintenanceState(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	table := storage.NewTable("fts_refresh_state", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	table.Rows = ftsMaintenanceRows(rng, 200, 60)
	table.Version = 1
	cols := []int{0}
	index := &storage.FTSIndex{}
	ftsExtendPersistent(table, cols, index)
	if !index.Verified() {
		t.Fatal("cold build not verified")
	}
	for round := 0; round < 30; round++ {
		var touched []int
		for k := 0; k < 1+rng.Intn(8); k++ {
			ri := rng.Intn(len(table.Rows))
			switch rng.Intn(4) {
			case 0:
				table.Rows[ri] = []any{fmt.Sprintf("fresh%d term%d w%d", round, k, rng.Intn(60))} // new terms
			case 1:
				table.Rows[ri] = []any{""}
			case 2:
				table.Rows[ri] = ftsMaintenanceRows(rng, 1, 60)[0]
			default: // identical text
			}
			touched = append(touched, ri)
		}
		table.Version++
		ftsRefreshUpdatedRows(table, cols, index, touched)
		label := fmt.Sprintf("round %d", round)
		if !index.Verified() || !ftsValidateStructure(index) {
			t.Fatalf("%s: verified=%v valid=%v", label, index.Verified(), ftsValidateStructure(index))
		}
		requireTermNamesInverse(t, index, label)
		cold := &storage.FTSIndex{}
		coldTable := storage.NewTable("fts_refresh_cold", table.Cols, false)
		coldTable.Rows = table.Rows
		ftsExtendPersistent(coldTable, cols, cold)
		got, want := ftsCacheFromPersistent(table, index), ftsCacheFromPersistent(coldTable, cold)
		if got.numDocs != want.numDocs || got.avgDocLen != want.avgDocLen {
			t.Fatalf("%s: numDocs/avg %d/%v vs %d/%v", label, got.numDocs, got.avgDocLen, want.numDocs, want.avgDocLen)
		}
		// Live postings may keep empty lists for terms no document holds any
		// more; compare the non-empty ones.
		for term, rows := range want.postings {
			if !reflect.DeepEqual(got.postings[term], rows) || !reflect.DeepEqual(got.postingCounts[term], want.postingCounts[term]) {
				t.Fatalf("%s: postings of %q differ: %v vs %v", label, term, got.postings[term], rows)
			}
		}
		for term, rows := range got.postings {
			if len(rows) > 0 && len(want.postings[term]) == 0 {
				t.Fatalf("%s: stale postings for %q: %v", label, term, rows)
			}
		}
	}
}

// The full structural validation runs once per index object. A structure that
// was decoded is not trusted, a damaged one is rejected, a deep copy of a
// verified index is verified (its first search after a transaction commit must
// not pay the whole check again), and the flag is set by the first successful check.
func TestFTSPersistentUsableVerifiesOnce(t *testing.T) {
	table := storage.NewTable("fts_verify", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	rng := rand.New(rand.NewSource(5))
	table.Rows = ftsMaintenanceRows(rng, 100, 30)
	table.Version = 1
	_ = getFTSDocCache("fts-verify", table, []int{0})
	index := table.FTSIndexes["0"]
	if !index.Verified() || !ftsPersistentUsable(index, table) {
		t.Fatal("a freshly built index must be verified and usable")
	}

	// A deep copy of a verified index is verified and keeps its term-name table
	// without aliasing it; an unverified one (decoded, or touched by a mutation
	// that did not finish) is checked before it is used.
	var term string
	for term = range index.PostingCounts {
		break
	}
	_ = index.TermNames()
	clone := index.Clone()
	if !clone.Verified() {
		t.Fatal("a clone of a verified index lost the verified flag")
	}
	if len(clone.TermNames()) != len(index.TermIDs) {
		t.Fatal("clone lost its term-name table")
	}
	clone.SetTermNames(append(clone.TermNames(), "extra")) // grows the clone's table only
	if len(index.TermNames()) != len(index.TermIDs) {
		t.Fatal("clone's term-name table aliases the original")
	}
	if !ftsPersistentUsable(clone, table) {
		t.Fatal("a clone of a verified index was rejected")
	}
	unverified := index.Clone()
	unverified.SetVerified(false)
	if !ftsPersistentUsable(unverified, table) || !unverified.Verified() {
		t.Fatal("an intact unverified clone must pass the full check and become verified")
	}
	cold := &storage.FTSIndex{}
	if cold.Clone().Verified() {
		t.Fatal("a clone of a never-validated index is verified")
	}

	// Damage on an unverified index is rejected by the full check.
	damaged := index.Clone()
	damaged.SetVerified(false)
	damaged.PostingCounts[term] = damaged.PostingCounts[term][:len(damaged.PostingCounts[term])-1]
	if ftsPersistentUsable(damaged, table) {
		t.Fatal("a posting list without its counts was accepted")
	}
	damaged = index.Clone()
	damaged.SetVerified(false)
	damaged.Docs[3].TokenStart = int32(len(damaged.DocTokenIDs) + 10)
	if ftsPersistentUsable(damaged, table) {
		t.Fatal("a document pointing outside the token arena was accepted")
	}
	damaged = index.Clone()
	damaged.SetVerified(false)
	damaged.TermIDs[term] = int32(len(damaged.TermIDs) + 5)
	if ftsPersistentUsable(damaged, table) {
		t.Fatal("an out-of-range term id was accepted")
	}

	// The O(1) shape checks still run for a verified index.
	if ftsPersistentUsable(index, storage.NewTable("short", table.Cols, false)) {
		t.Fatal("an index built over more rows than the table has was accepted")
	}
}
