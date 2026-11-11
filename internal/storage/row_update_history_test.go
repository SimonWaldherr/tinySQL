package storage

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
	"testing"
)

func makeHistoryTestData(count int) (rowUpdateHistory, []rowUpdateDelta) {
	var history rowUpdateHistory
	model := make([]rowUpdateDelta, count)
	for i := range model {
		model[i] = rowUpdateDelta{structVersion: 100 + 2*i, row: i % 17}
		history.add(model[i])
	}
	return history, model
}

func checkHistoryTestData(t *testing.T, history *rowUpdateHistory, model []rowUpdateDelta) {
	t.Helper()
	if history.len() != len(model) {
		t.Fatalf("history length = %d, want %d", history.len(), len(model))
	}
	for i, want := range model {
		if got := history.at(i); got != want {
			t.Fatalf("delta %d = %v, want %v", i, got, want)
		}
	}
	versions := []int{-1, 0, 99, 100, 101, 102, 225, 226, 227, 228, 229, 10000}
	if len(model) != 0 {
		last := model[len(model)-1].structVersion
		versions = append(versions, last-1, last, last+1)
	}
	for _, version := range versions {
		want := sort.Search(len(model), func(i int) bool { return model[i].structVersion >= version })
		if got := history.search(version); got != want {
			t.Fatalf("search(%d) = %d, want %d", version, got, want)
		}
	}
}

func TestRowUpdateHistoryCloneBranchesStayIndependent(t *testing.T) {
	for _, count := range []int{0, 1, 63, 64, 65, 4095, 4096, 4097} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			original, model := makeHistoryTestData(count)
			first, second, frozen := original.clone(), original.clone(), original.clone()
			branches := []*rowUpdateHistory{&original, &first, &second}
			models := [][]rowUpdateDelta{slices.Clone(model), slices.Clone(model), slices.Clone(model)}
			var wg sync.WaitGroup
			for branch, history := range branches {
				wg.Go(func() {
					for i := range 2*rowUpdateChunk + 3 {
						if branch == 1 && i == rowUpdateChunk/2 {
							history.reset()
							models[branch] = nil
						}
						delta := rowUpdateDelta{structVersion: 20000 + branch*1000 + i, row: branch*100 + i}
						history.add(delta)
						models[branch] = append(models[branch], delta)
					}
				})
			}
			wg.Wait()
			checkHistoryTestData(t, &frozen, model)
			for branch, history := range branches {
				checkHistoryTestData(t, history, models[branch])
			}
		})
	}
}

func TestUpdatedRowsSinceHistoryBoundaries(t *testing.T) {
	for _, count := range []int{0, 1, 63, 64, 65, 4095, 4096, 4097} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			table := NewTable("history", nil, false)
			for i := range count {
				table.noteRowUpdated(i % 7)
			}
			oldest := 0
			if count > 4096 {
				oldest = 4096
			}
			for _, version := range []int{-1, 0, 1, 62, 63, 64, 65, 4095, 4096, count - 1, count, count + 1} {
				wantOK := version >= oldest && version <= count
				var want []int
				if wantOK {
					seen := map[int]bool{}
					for i := version; i < count; i++ {
						row := i % 7
						if !seen[row] {
							seen[row] = true
							want = append(want, row)
						}
					}
				}
				got, ok := table.UpdatedRowsSince(version)
				if ok != wantOK || (ok && !reflect.DeepEqual(got, want)) {
					t.Fatalf("since %d: got %v/%v, want %v/%v", version, got, ok, want, wantOK)
				}
			}
			// A structural mutation invalidates old row positions. Subsequent
			// UPDATEs start a fresh history at the new structural version.
			before := table.StructVersion()
			table.noteStructuralInvalidation()
			if _, ok := table.UpdatedRowsSince(before); ok {
				t.Fatal("structural invalidation retained stale row positions")
			}
			restart := table.StructVersion()
			table.noteRowUpdated(2)
			if got, ok := table.UpdatedRowsSince(restart); !ok || !reflect.DeepEqual(got, []int{2}) {
				t.Fatalf("history after invalidation: %v/%v", got, ok)
			}
		})
	}
}

func historyTestTable(count int) *Table {
	table := NewTable("history", []Column{{Name: "id", Type: IntType}, {Name: "value", Type: IntType}}, false)
	for i := range 17 {
		table.Rows = append(table.Rows, []any{i, 0})
	}
	for i := range count {
		row := i % len(table.Rows)
		table.Rows[row] = []any{row, i + 1}
		table.noteRowUpdated(row)
	}
	return table
}

func TestRowUpdateHistoryTableClonesKeepOwnTrail(t *testing.T) {
	for _, count := range []int{63, 64, 65, 4096} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			table := historyTestTable(count)
			metadata, full := cloneTableMeta(table), cloneTable(table)
			version := table.StructVersion()
			table.noteStructuralInvalidation()
			metadata.noteRowUpdated(3)
			full.noteRowUpdated(5)
			for _, tc := range []struct {
				table *Table
				row   int
			}{{metadata, 3}, {full, 5}} {
				if got, ok := tc.table.UpdatedRowsSince(version); !ok || !reflect.DeepEqual(got, []int{tc.row}) {
					t.Fatalf("clone trail: %v/%v, want [%d]", got, ok, tc.row)
				}
			}
			if _, ok := table.UpdatedRowsSince(version); ok {
				t.Fatal("clone updates changed the source's invalidated history")
			}
		})
	}
}

func TestRowTwinCatchUpAcrossHistoryBlocks(t *testing.T) {
	for _, count := range []int{0, 1, 63, 64, 65, 4095, 4096, 4097} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			table := historyTestTable(count)
			before := slices.Clone(table.Rows)
			version := table.StructVersion()
			branch := cloneTable(table)
			for i := range 2*rowUpdateChunk + 3 {
				row := i % len(branch.Rows)
				branch.Rows[row] = []any{row, 10000 + i}
				branch.noteRowUpdated(row)
			}
			// Append-only growth does not change physical positions. A later
			// UPDATE to a new row must be picked up through the appended tail.
			newRow := len(branch.Rows)
			branch.Rows = append(branch.Rows, []any{newRow, "appended"})
			branch.noteRowUpdated(newRow)
			buffer := slices.Clone(before)
			got, ok := branch.catchUpRows(buffer, version)
			wantOK := count != 4095 // its next delta is evicted on rollover
			if ok != wantOK || (ok && !reflect.DeepEqual(got, branch.Rows)) {
				t.Fatalf("catch-up: ok=%v, want %v; got %v, want %v", ok, wantOK, got, branch.Rows)
			}
			if !reflect.DeepEqual(table.Rows, before) {
				t.Fatal("catch-up or branch updates mutated source rows")
			}
			if _, ok := branch.catchUpRows(slices.Clone(before), branch.StructVersion()+1); ok {
				t.Fatal("future twin version accepted")
			}
			branch.noteStructuralInvalidation()
			if _, ok := branch.catchUpRows(slices.Clone(before), version); ok {
				t.Fatal("invalidated twin positions accepted")
			}
		})
	}
}
