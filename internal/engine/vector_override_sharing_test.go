package engine

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func overrideSharingFixture(rows, overrides, dims int, norms bool) (*storage.Table, vecSearchColumnCacheEntry) {
	table := storage.NewTable("override_sharing", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = make([][]any, rows)
	for row := range table.Rows {
		vector := make([]float64, dims)
		for d := range vector {
			vector[d] = float64((row+d)%17 + 1)
		}
		table.Rows[row] = []any{vector}
	}
	table.Version = 1
	cache := buildVecColumnCache(table, 0, norms)
	cache.overrides = make(map[int]vecColumnOverride, overrides)
	for row := range overrides {
		vector := append([]float64(nil), table.Rows[row][0].([]float64)...)
		value := vecColumnOverride{vector: vector, valid: true}
		if norms {
			value.norm = vectorL2Norm(vector)
		}
		cache.overrides[row] = value
	}
	return table, cache
}

func TestVecAppendOverridesRemainImmutableAcrossRefresh(t *testing.T) {
	for _, overrides := range []int{0, 16} {
		for _, norms := range []bool{false, true} {
			for _, includeNorms := range []bool{false, true} {
				t.Run(fmt.Sprintf("overrides=%d/cachedNorms=%t/requestNorms=%t", overrides, norms, includeNorms), func(t *testing.T) {
					table, base := overrideSharingFixture(512, overrides, 2, norms)
					wantOld := append([]float64(nil), base.vector(0)...)
					oldNorm := vectorL2Norm(wantOld)
					table.Rows = append(table.Rows, []any{[]float64{5, 12}})
					table.Version++
					extended := extendVecColumnCache(base, table, 0, includeNorms)
					ready := norms || includeNorms
					if extended.rows != 513 || extended.normsReady != ready || !reflect.DeepEqual(extended.vector(512), []float64{5, 12}) {
						t.Fatal("append lost the new vector or norm state")
					}
					if ready && extended.normAt(512) != 13 {
						t.Fatal("appended vector norm changed")
					}
					if overrides != 0 && !norms && includeNorms && base.overrides[0].norm != 0 {
						t.Fatal("norm upgrade wrote into the published override map")
					}
					table.Rows[0] = []any{[]float64{0, 12}}
					table.Rows[100] = []any{[]float64{8, 15}}
					table.Version++
					table.MarkRowUpdated(0)
					table.MarkRowUpdated(100)
					updated := refreshVecColumnCache(extended, table, 0, includeNorms, []int{0, 100})
					if !reflect.DeepEqual(updated.vector(0), []float64{0, 12}) || !reflect.DeepEqual(updated.vector(100), []float64{8, 15}) {
						t.Fatal("UPDATE did not replace and insert row overrides")
					}
					if ready && (updated.normAt(0) != 12 || updated.normAt(100) != 17) {
						t.Fatal("UPDATE lost cached norms")
					}
					if len(base.overrides) != overrides || len(extended.overrides) != overrides || len(updated.overrides) != max(overrides, 1)+1 {
						t.Fatal("UPDATE changed an earlier view's override membership")
					}
					for _, before := range []vecSearchColumnCacheEntry{base, extended} {
						if !reflect.DeepEqual(before.vector(0), wantOld) || (before.normsReady && before.normAt(0) != oldNorm) {
							t.Fatal("UPDATE changed an earlier view's vector or norm")
						}
					}
				})
			}
		}
	}
}

func TestVecColumnOverrideRefreshUpgradesNormsPrivately(t *testing.T) {
	table, base := overrideSharingFixture(512, 16, 2, false)
	oldVector := append([]float64(nil), base.vector(1)...)
	table.Rows[0] = []any{[]float64{0, 12}}
	table.Version++
	table.MarkRowUpdated(0)
	updated := refreshVecColumnCache(base, table, 0, true, []int{0})
	if !updated.normsReady || updated.normAt(0) != 12 || updated.normAt(1) != vectorL2Norm(oldVector) {
		t.Fatal("norm upgrade or updated-row norm was lost")
	}
	for _, value := range base.overrides {
		if value.norm != 0 {
			t.Fatal("norm upgrade changed the original override map")
		}
	}
	if !reflect.DeepEqual(base.vector(0), []float64{1, 2}) {
		t.Fatal("refresh replaced a published vector while upgrading norms")
	}
}

func TestVecAppendOverridesConcurrentReadersAndRefresh(t *testing.T) {
	table, base := overrideSharingFixture(512, 16, 2, true)
	table.Rows = append(table.Rows, []any{[]float64{5, 12}})
	table.Version++
	extended := extendVecColumnCache(base, table, 0, true)
	want := append([]float64(nil), base.vector(0)...)
	table.Rows[0] = []any{[]float64{0, 12}}
	table.Version++
	table.MarkRowUpdated(0)
	start := make(chan struct{})
	errs := make(chan string, 8)
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			<-start
			for range 1000 {
				for _, view := range []vecSearchColumnCacheEntry{base, extended} {
					vector, norm, valid := view.resolveRow(0, true)
					if !valid || !reflect.DeepEqual(vector, want) || norm != vectorL2Norm(want) || len(view.overrides) != 16 {
						errs <- "published view changed during refresh"
						return
					}
				}
			}
		})
	}
	close(start)
	for range 64 {
		updated := refreshVecColumnCache(extended, table, 0, true, []int{0})
		if updated.normAt(0) != 12 || !reflect.DeepEqual(updated.vector(0), []float64{0, 12}) {
			t.Fatal("private refreshed view did not receive the updated vector")
		}
	}
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
