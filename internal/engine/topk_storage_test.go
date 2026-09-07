package engine

import (
	"reflect"
	"testing"
)

func TestTopKTransfersStorageWithoutAliasingReuse(t *testing.T) {
	for _, k := range []int{1, 3, 9} {
		v := vecScoredHeap{}
		f := ftsScoredHeap{}
		for _, id := range []int{2, 0, 1} {
			vecScoredHeapPush(&v, vecScoredRow{rowIdx: id, distance: 1})
			ftsScoredHeapPush(&f, ftsScored{rowIdx: id, score: 1})
		}
		vr := topKFromHeap(&v, k)
		fr := ftsTopKFromHeap(&f, k)
		n := min(k, 3)
		for i := range n {
			if vr[i].rowIdx != 3-n+i || fr[i].rowIdx != 3-n+i {
				t.Fatalf("k=%d: vector=%v fts=%v", k, vr, fr)
			}
		}
		vc := append([]vecScoredRow(nil), vr...)
		fc := append([]ftsScored(nil), fr...)
		for i := range 10 {
			vecScoredHeapPush(&v, vecScoredRow{rowIdx: 100 + i})
			ftsScoredHeapPush(&f, ftsScored{rowIdx: 100 + i})
		}
		if !reflect.DeepEqual(vr, vc) || !reflect.DeepEqual(fr, fc) {
			t.Fatal("heap reuse changed returned rows")
		}
	}
}

func BenchmarkTopKStorage(b *testing.B) {
	b.Run("Vector", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			h := make(vecScoredHeap, 0, 100)
			for i := 0; i < 100; i++ {
				vecScoredHeapPush(&h, vecScoredRow{rowIdx: i, distance: float64(100 - i)})
			}
			if len(topKFromHeap(&h, 100)) != 100 {
				b.Fatal("missing rows")
			}
		}
	})
	b.Run("FTS", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			h := make(ftsScoredHeap, 0, 100)
			for i := 0; i < 100; i++ {
				ftsScoredHeapPush(&h, ftsScored{rowIdx: i, score: float64(i)})
			}
			if len(ftsTopKFromHeap(&h, 100)) != 100 {
				b.Fatal("missing rows")
			}
		}
	})
}
