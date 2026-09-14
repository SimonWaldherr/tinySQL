package engine

import (
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"reflect"
	"testing"
)

func TestPrimaryCacheMaintenanceAndChurn(t *testing.T) {
	table := storage.NewTable("cache_changes", []storage.Column{{Name: "id", Type: storage.IntType, Constraint: storage.PrimaryKey}}, false)
	table.Rows = [][]any{{int(-1)}, {int64(2)}, {nil}, {float64(3)}}
	getConstraintIndex(table, 0)
	check := func() {
		t.Helper()
		index := getConstraintIndex(table, 0)
		want := map[any][]int{}
		other := 0
		for i, row := range table.Rows {
			if row[0] == nil {
				continue
			}
			switch row[0].(type) {
			case int, int64:
			default:
				other++
			}
			key := comparableKeyPart(row[0])
			want[key] = append(want[key], i)
		}
		if index.nonIntegerRows != other || !reflect.DeepEqual(index.rows, want) {
			t.Fatalf("cache=%v count=%d; want %v count=%d", index.rows, index.nonIntegerRows, want, other)
		}
	}
	change := func(row int, value any) {
		next := []any{value}
		patchConstraintIndexRow(table, row, table.Rows[row], next)
		table.Rows[row] = next
	}
	check()
	for _, value := range []any{float64(-1), int64(-1), int(-1)} {
		change(0, value)
		check()
	}
	change(2, "text")
	check()
	// An appended row outside the cached prefix is indexed only once, using its
	// latest representation when the cache catches up.
	table.Rows = append(table.Rows, []any{float64(4)})
	change(4, int(4))
	check()
	table.DerivedLock()
	clone := table.Derived().(*constraintIndexSet).CloneDerived().(*constraintIndexSet)
	table.DerivedUnlock()
	change(3, int(3))
	check()
	if clone.cols[0].nonIntegerRows != 2 || len(clone.cols[0].rows[float64(3)]) != 1 {
		t.Fatal("clone shared cache mutations")
	}
	last := len(table.Rows) - 1
	patchConstraintIndexSwapRemove(table, 2, table.Rows[2], last, table.Rows[last], len(table.Rows))
	table.Rows[2] = table.Rows[last]
	table.Rows = table.Rows[:last]
	check()
	for i := 0; i < 10000; i++ {
		change(0, 100000+i)
	}
	check() // Exactly the live keys remain; no empty historical buckets.
}
