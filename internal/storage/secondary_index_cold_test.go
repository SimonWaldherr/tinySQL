package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"testing"
)

func coldIndex(t testing.TB, n int) *SecondaryIndex {
	t.Helper()
	src := &SecondaryIndex{Name: "cold", Columns: []string{"key"}}
	for i := 0; i < n; i++ {
		src.hydrate().Insert(CanonicalIndexKey([]any{fmt.Sprintf("k%08d", i)}), i)
	}
	src.materialize()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		t.Fatal(err)
	}
	var restored SecondaryIndex
	if err := gob.NewDecoder(&buf).Decode(&restored); err != nil {
		t.Fatal(err)
	}
	return &restored
}
func TestColdIndexReadsDoNotHydrate(t *testing.T) {
	idx := coldIndex(t, 1000)
	for _, key := range []string{"k00000000", "k00000500", "missing"} {
		cold := append([]int(nil), idx.lookup(CanonicalIndexKey([]any{key}))...)
		if idx.fast != nil {
			t.Fatal("point read hydrated index")
		}
		reference := idx.clone()
		warm, _ := reference.hydrate().Get(CanonicalIndexKey([]any{key}))
		if !reflect.DeepEqual(cold, warm) {
			t.Fatalf("%s: %v != %v", key, cold, warm)
		}
	}
	idx.hydrate().Insert(CanonicalIndexKey([]any{"new"}), 1000)
	if got := idx.lookup(CanonicalIndexKey([]any{"new"})); len(got) != 1 || got[0] != 1000 {
		t.Fatal(got)
	}
}
func BenchmarkColdIndexPoint(b *testing.B) {
	persisted := coldIndex(b, 20000)
	key := CanonicalIndexKey([]any{"k00010000"})
	for _, hydrate := range []bool{true, false} {
		b.Run(fmt.Sprint(hydrate), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				idx := &SecondaryIndex{Entries: persisted.Entries}
				var rows []int
				if hydrate {
					rows, _ = idx.hydrate().Get(key)
				} else {
					rows = idx.lookup(key)
				}
				if len(rows) != 1 || rows[0] != 10000 {
					b.Fatal(rows)
				}
			}
		})
	}
}
