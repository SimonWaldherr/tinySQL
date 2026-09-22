package engine

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func ftsParallelBuildCorpus(rows int) *storage.Table {
	table := storage.NewTable("fts_parallel", nil, false)
	words := []string{"Alpha", "beta", "gamma", "running", "runner", "the", "and", "Vector", "search", "café", "漢字", "x"}
	for i := 0; i < rows; i++ {
		var sb strings.Builder
		for j := 0; j < i%17; j++ {
			sb.WriteString(words[(i*7+j*3)%len(words)])
			sb.WriteByte(' ')
		}
		// Terms that first appear late, and only in some chunks.
		if i%97 == 0 {
			fmt.Fprintf(&sb, "rare%d ", i/97)
		}
		var second any = fmt.Sprintf("Doc%d", i%500)
		switch i % 11 {
		case 3:
			second = nil
		case 5:
			second = i
		case 7:
			second = ""
		}
		table.Rows = append(table.Rows, []any{sb.String(), second})
	}
	return table
}

// ftsFreshIndexFor mirrors ftsExtendPersistent's setup for a fresh index.
func ftsFreshIndexFor(table *storage.Table) *storage.FTSIndex {
	return &storage.FTSIndex{
		PostingBlocks: make(map[string][]storage.FTSPostingBlock),
		PostingCounts: make(map[string][]int32),
		Postings:      make(map[string][]int32),
		TermIDs:       make(map[string]int32),
		Docs:          make([]storage.FTSDocument, len(table.Rows)),
	}
}

func ftsSequentialBuild(table *storage.Table, cols []int) *storage.FTSIndex {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	index := &storage.FTSIndex{}
	ftsExtendPersistent(table, cols, index)
	return index
}

func TestFTSParallelBuildMatchesSequential(t *testing.T) {
	// Sizes from the packed-build minimum (ftsDensePostingMinRows) up; the
	// parallel path only ever replaces that branch.
	for _, rows := range []int{ftsDensePostingMinRows, 65, 3000, 10007} {
		table := ftsParallelBuildCorpus(rows)
		for _, cols := range [][]int{{0}, {0, 1}, {1, 0, 1}} {
			want := ftsSequentialBuild(table, cols)
			for _, workers := range []int{2, 3, 7, 16} {
				got := ftsFreshIndexFor(table)
				ftsBuildFreshParallel(table, cols, got, workers)
				got.Format = want.Format
				got.Version, got.StructVersion, got.BuiltRows = want.Version, want.StructVersion, want.BuiltRows
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("rows=%d cols=%v workers=%d: parallel index differs from sequential build", rows, cols, workers)
				}
			}
		}
	}
}

// The parallel build is what ftsExtendPersistent now produces for large fresh
// indexes; extending it append-only must match a sequential fresh build too.
func TestFTSParallelBuildExtendsLikeSequential(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("needs GOMAXPROCS >= 2 to take the parallel path")
	}
	table := ftsParallelBuildCorpus(6000)
	cols := []int{0, 1}
	full := ftsParallelBuildCorpus(6100)
	extended := &storage.FTSIndex{}
	ftsExtendPersistent(table, cols, extended)
	table.Rows = full.Rows
	table.Version++
	ftsExtendPersistent(table, cols, extended)
	want := ftsSequentialBuild(table, cols)
	if !reflect.DeepEqual(extended.Docs, want.Docs) || !reflect.DeepEqual(extended.Postings, want.Postings) ||
		!reflect.DeepEqual(extended.PostingCounts, want.PostingCounts) || !reflect.DeepEqual(extended.TermIDs, want.TermIDs) ||
		!reflect.DeepEqual(extended.DocTokenIDs, want.DocTokenIDs) || !reflect.DeepEqual(extended.DocTermIDs, want.DocTermIDs) ||
		extended.NumDocs != want.NumDocs || extended.TotalDocLen != want.TotalDocLen {
		t.Fatal("append-only extension of a parallel build differs from a sequential fresh build")
	}
}
