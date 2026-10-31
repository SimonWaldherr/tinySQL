package engine

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRAGContextExpansionPreservesExactBigintDocuments(t *testing.T) {
	for _, pair := range [][2]int64{
		{1 << 53, 1<<53 + 1},
		{-1 << 53, -1<<53 - 1},
		{math.MaxInt64 - 1, math.MaxInt64},
		{math.MinInt64, math.MinInt64 + 1},
	} {
		t.Run(fmt.Sprint(pair), func(t *testing.T) {
			table := storage.NewTable("exact_context_ids", []storage.Column{
				{Name: "doc", Type: storage.IntType}, {Name: "chunk", Type: storage.IntType}, {Name: "body", Type: storage.TextType},
			}, false)
			for doc, id := range pair {
				for chunk := range 3 {
					table.Rows = append(table.Rows, []any{id, chunk, fmt.Sprintf("document-%d/chunk-%d", doc, chunk)})
				}
			}
			source := ragSourceFromTable("default", table)
			hits := ragSource{cols: []string{"doc", "chunk"}, rows: []Row{
				{"doc": pair[0], "chunk": 1}, {"doc": pair[1], "chunk": 1},
			}}
			result := ragExpandContextFrom(source, hits, "doc", "chunk", "doc", "chunk", 1, 1)
			if len(result.Rows) != 6 {
				t.Fatalf("two documents with three neighbors each produced %d rows: %v", len(result.Rows), result.Rows)
			}
			for i, row := range result.Rows {
				if row["doc"] != pair[i/3] || row["chunk"] != i%3 || row["_context_hits"] != 1 {
					t.Errorf("context %d mixed document identity or provenance: %v", i, row)
				}
			}
		})
	}
}

func TestRAGContextNumericIdentityColdAndWarm(t *testing.T) {
	const bigID int64 = 1 << 53
	table := storage.NewTable("numeric_context_ids", []storage.Column{
		{Name: "doc", Type: storage.IntType}, {Name: "chunk", Type: storage.IntType},
	}, false)
	table.Rows = [][]any{
		{int64(7), 0}, {int(7), 1}, {float64(7), 2}, {float64(7.5), 1},
		{bigID, 0}, {float64(bigID), 1}, {bigID + 1, 2},
		{int64(math.MaxInt64), 0}, {float64(0x1p63), 1},
		{int64(math.MinInt64), 0}, {float64(-0x1p63), 1},
		{int(0), 0}, {math.Copysign(0, -1), 1}, {float64(0), 2},
	}
	source := ragSourceFromTable("default", table)
	t.Cleanup(func() { purgeRAGContextCachesFor("default", table.Name) })
	for _, tc := range []struct {
		id   any
		rows []int
	}{
		{7, []int{0, 1, 2}}, {int64(7), []int{0, 1, 2}}, {float64(7), []int{0, 1, 2}},
		{7.5, []int{3}}, {bigID, []int{4, 5}}, {float64(bigID), []int{4, 5}},
		{bigID + 1, []int{6}}, {int64(math.MaxInt64), []int{7}}, {float64(0x1p63), []int{8}},
		{int64(math.MinInt64), []int{9, 10}}, {math.Copysign(0, -1), []int{11, 12, 13}},
	} {
		purgeRAGContextCachesFor("default", table.Name)
		cold := ragFindContextRows(source, "doc", "chunk", tc.id, 1, 1, 1)
		getRAGContextIndex(source, "doc", "chunk")
		warm := ragFindContextRows(source, "doc", "chunk", tc.id, 1, 1, 1)
		for _, matches := range [][]ragContextRow{cold, warm} {
			actual := make([]int, len(matches))
			for i, match := range matches {
				actual[i] = match.sourceRow
			}
			if !reflect.DeepEqual(actual, tc.rows) {
				t.Errorf("document %T(%v): matched rows %v, want %v", tc.id, tc.id, actual, tc.rows)
			}
		}
	}
}

func TestRAGContextNumericIdentityOrdering(t *testing.T) {
	ordered := []any{
		math.Inf(-1), int64(math.MinInt64), -7.5, int64(-7), 0, 7, 7.5,
		int64(1 << 53), int64(1<<53 + 1), int64(math.MaxInt64), float64(0x1p63), math.Inf(1),
	}
	for i, left := range ordered {
		for j, right := range ordered {
			got := ragContextKeyLess(ragContextIdentity(left, 1), ragContextIdentity(right, 1))
			if got != (i < j) {
				t.Errorf("%T(%v) < %T(%v) = %v, want %v", left, left, right, right, got, i < j)
			}
		}
	}
	for _, pair := range [][2]any{{7, int64(7)}, {int64(7), float64(7)}, {0, math.Copysign(0, -1)}, {int64(math.MinInt64), float64(-0x1p63)}} {
		if !ragContextDocumentsEqual(pair[0], pair[1]) || ragContextIdentity(pair[0], 1) != ragContextIdentity(pair[1], 1) {
			t.Errorf("equivalent IDs have different identities: %v", pair)
		}
	}
}
