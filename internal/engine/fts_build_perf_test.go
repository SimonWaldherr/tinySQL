package engine

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkFTSPersistentBuild(b *testing.B) {
	for _, columns := range []int{1, 3} {
		b.Run(fmt.Sprintf("columns=%d", columns), func(b *testing.B) {
			table := storage.NewTable("build_bench", nil, false)
			cols := make([]int, columns)
			for i := range cols {
				cols[i] = i
			}
			for i := 0; i < 1000; i++ {
				row := make([]any, columns)
				for j := range row {
					row[j] = strings.Repeat("document search index query retrieval vector context ", 8) + fmt.Sprintf("record%d", i)
				}
				table.Rows = append(table.Rows, row)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				index := &storage.FTSIndex{}
				ftsExtendPersistent(table, cols, index)
				if index.NumDocs != len(table.Rows) {
					b.Fatal(index.NumDocs)
				}
			}
		})
	}
}

func TestFTSColumnStreamingMatchesJoinedText(t *testing.T) {
	rows := [][]any{
		{"running", "runner", "runs"},
		{"alpha", "beta", "gamma"},
		{"", nil, "alpha"},
		{nil, "", nil},
		{"the AND", "or", ""},
		{"MiXeD_CASE", "漢字 café", "invalid\xffbytes"},
		{42, int64(-7), true},
		{1.25, []byte{1, 2}, "omega"},
		{},
	}
	for _, cols := range [][]int{{0}, {0, 1, 2}, {2, 0, 1, 0}, {3, 1}} {
		separate := storage.NewTable("separate", nil, false)
		joined := storage.NewTable("joined", nil, false)
		for _, row := range rows {
			var sb strings.Builder
			for _, ci := range cols {
				if ci < len(row) && row[ci] != nil {
					if sb.Len() > 0 {
						sb.WriteByte(' ')
					}
					ftsWriteValue(&sb, row[ci])
				}
			}
			var got, want []string
			ftsVisitRowTokens(row, cols, func(token string) { got = append(got, token) })
			ftsForEachToken(sb.String(), func(token string) bool { want = append(want, token); return true })
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("cols=%v row=%v: got %v, want %v", cols, row, got, want)
			}
			separate.Rows = append(separate.Rows, row)
			joined.Rows = append(joined.Rows, []any{sb.String()})
		}
		actual, expected := &storage.FTSIndex{}, &storage.FTSIndex{}
		ftsExtendPersistent(separate, cols, actual)
		ftsExtendPersistent(joined, []int{0}, expected)
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("cols=%v: index metadata differs from joined text", cols)
		}
	}
}

func BenchmarkFTSPersistentBatchUpdate(b *testing.B) {
	table := storage.NewTable("update_bench", nil, false)
	rows := make([]int, 1000)
	for i := range rows {
		rows[i] = i
		table.Rows = append(table.Rows, []any{strings.Repeat("alpha beta gamma delta epsilon zeta eta theta ", 4)})
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		index := &storage.FTSIndex{}
		ftsExtendPersistent(table, []int{0}, index)
		b.StartTimer()
		ftsRefreshUpdatedRows(table, []int{0}, index, rows)
		if index.NumDocs != len(rows) {
			b.Fatal(index.NumDocs)
		}
	}
}

func TestFTSAppendBatchesMatchFullBuild(t *testing.T) {
	table := storage.NewTable("batch_append", nil, false)
	incremental := &storage.FTSIndex{}
	for _, end := range []int{0, 1, 127, 128, 129, 255, 256, 300} {
		for len(table.Rows) < end {
			i := len(table.Rows)
			var text any = fmt.Sprintf("alpha %s unique%d", strings.Repeat("beta ", i%5+1), i)
			if i%17 == 0 {
				text = nil
			}
			table.Rows = append(table.Rows, []any{text})
		}
		table.Version++
		ftsExtendPersistent(table, []int{0}, incremental)
		fresh := &storage.FTSIndex{}
		ftsExtendPersistent(table, []int{0}, fresh)
		if !reflect.DeepEqual(incremental, fresh) {
			t.Fatalf("%d rows: incremental index differs from full build", end)
		}
		checkPostingMetadata(t, ftsCacheFromPersistent(table, incremental))
	}
}

func TestFTSBatchUpdateScratchIsolation(t *testing.T) {
	table := storage.NewTable("batch_update", nil, false)
	for i := 0; i < 140; i++ {
		table.Rows = append(table.Rows, []any{"alpha beta beta"})
	}
	index := &storage.FTSIndex{}
	ftsExtendPersistent(table, []int{0}, index)
	rows := []int{-1, 0, 1, 2, 3, 4, 127, 128, 139, 140}
	table.Rows[0][0] = "gamma gamma delta"
	table.Rows[1][0] = nil
	table.Rows[2][0] = ""
	table.Rows[3][0] = "the and or"
	table.Rows[4][0] = "epsilon"
	table.Rows[127][0] = "beta gamma"
	table.Rows[128][0] = "delta delta delta"
	table.Rows[139][0] = "omega"
	ftsRefreshUpdatedRows(table, []int{0}, index, rows)
	checkPostingMetadata(t, ftsCacheFromPersistent(table, index))
	names := make([]string, len(index.TermIDs))
	for name, id := range index.TermIDs {
		names[id] = name
	}
	for i, doc := range index.Docs {
		var want []string
		if table.Rows[i][0] != nil {
			want = ftsTokenize(table.Rows[i][0].(string))
		}
		var got []string
		if doc.Valid {
			for _, id := range index.DocTokenIDs[doc.TokenStart : doc.TokenStart+doc.TokenCount] {
				got = append(got, names[id])
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("row %d: tokens %v, want %v", i, got, want)
		}
	}
}

// Bulk and single-row construction must produce the same persisted format,
// including term-ID assignment, phrase positions, counts, and block bounds.
func TestFTSDensePostingBuildMatchesRowwiseExtension(t *testing.T) {
	for _, n := range []int{63, 64, 65, 300} {
		t.Run(fmt.Sprintf("rows=%d", n), func(t *testing.T) {
			bulkTable := storage.NewTable("fts_dense_compare", nil, false)
			for i := 0; i < n; i++ {
				row := []any{fmt.Sprintf("alpha %s café 漢字 unique%d", strings.Repeat("beta ", i%5), i%41), i % 7}
				if i%19 == 0 {
					row = []any{nil, "the and or"}
				}
				if i%23 == 0 {
					row = nil
				}
				bulkTable.Rows = append(bulkTable.Rows, row)
			}
			cols := []int{0, 1, 0} // Duplicate columns must still count twice.
			bulkTable.Version = n
			bulk := &storage.FTSIndex{}
			ftsExtendPersistent(bulkTable, cols, bulk)
			rowwiseTable := storage.NewTable(bulkTable.Name, nil, false)
			rowwise := &storage.FTSIndex{}
			for _, row := range bulkTable.Rows {
				rowwiseTable.Rows = append(rowwiseTable.Rows, row)
				rowwiseTable.Version++
				ftsExtendPersistent(rowwiseTable, cols, rowwise)
			}
			if !reflect.DeepEqual(bulk, rowwise) {
				t.Fatal("bulk index differs from rowwise index")
			}
			// Leave empty old postings and introduce a new term before a large append.
			for _, table := range []*storage.Table{bulkTable, rowwiseTable} {
				table.Rows[1] = []any{"replacement replacement", nil}
				table.Version++
				table.MarkRowUpdated(1)
			}
			ftsRefreshUpdatedRows(bulkTable, cols, bulk, []int{1})
			ftsRefreshUpdatedRows(rowwiseTable, cols, rowwise, []int{1})
			for i := 0; i < 130; i++ {
				row := []any{fmt.Sprintf("replacement alpha appended%d", i%3), nil}
				bulkTable.Rows = append(bulkTable.Rows, row)
				bulkTable.Version++
				rowwiseTable.Rows = append(rowwiseTable.Rows, row)
				rowwiseTable.Version++
				ftsExtendPersistent(rowwiseTable, cols, rowwise)
			}
			ftsExtendPersistent(bulkTable, cols, bulk)
			if !reflect.DeepEqual(bulk, rowwise) {
				t.Fatal("bulk append after update differs from rowwise extension")
			}
			checkPostingMetadata(t, ftsCacheFromPersistent(bulkTable, bulk))
		})
	}
}

func BenchmarkFTSSingleRowAppend(b *testing.B) {
	table := storage.NewTable("fts_single_append", nil, false)
	for i := 0; i < 1000; i++ {
		table.Rows = append(table.Rows, []any{fmt.Sprintf("alpha beta word%d", i)})
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		table.Rows = table.Rows[:1000]
		index := &storage.FTSIndex{}
		ftsExtendPersistent(table, []int{0}, index)
		table.Rows = append(table.Rows, []any{"alpha beta newword"})
		table.Version++
		b.StartTimer()
		ftsExtendPersistent(table, []int{0}, index)
		if index.BuiltRows != 1001 {
			b.Fatal(index.BuiltRows)
		}
	}
}

func TestFTSPackedPostingTermAppendIsolation(t *testing.T) {
	table := storage.NewTable("fts_packed_append", nil, false)
	for range 128 {
		table.Rows = append(table.Rows, []any{"alpha alpha beta gamma"})
	}
	index := &storage.FTSIndex{}
	ftsExtendPersistent(table, []int{0}, index)
	betaRows := index.Postings["beta"]
	betaCounts := index.PostingCounts["beta"]
	wantRows := slices.Clone(betaRows)
	wantCounts := slices.Clone(betaCounts)
	// Only alpha grows. Its extra row/count must not overwrite the following
	// term in the shared arena, including references held before the append.
	table.Rows = append(table.Rows, []any{"alpha alpha alpha"})
	table.Version++
	ftsExtendPersistent(table, []int{0}, index)
	if !slices.Equal(betaRows, wantRows) || !slices.Equal(betaCounts, wantCounts) {
		t.Fatal("growing alpha overwrote beta's backing storage")
	}
	if !slices.Equal(index.Postings["beta"], wantRows) || !slices.Equal(index.PostingCounts["beta"], wantCounts) {
		t.Fatal("growing alpha changed beta's postings")
	}
	rebuilt := &storage.FTSIndex{}
	ftsExtendPersistent(table, []int{0}, rebuilt)
	if !reflect.DeepEqual(index, rebuilt) {
		t.Fatal("incremental packed index differs from rebuild")
	}
	checkPostingMetadata(t, ftsCacheFromPersistent(table, index))
}
