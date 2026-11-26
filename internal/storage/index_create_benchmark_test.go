package storage

import (
	"fmt"
	"testing"
)

func BenchmarkCreateAdditionalIndex(b *testing.B) {
	t := NewTable("build_index", []Column{{Name: "id", Type: IntType}, {Name: "category", Type: IntType}}, false)
	for i := 0; i < 10000; i++ {
		t.Rows = append(t.Rows, []any{i, i % 100})
	}
	for i := 0; i < 4; i++ {
		if err := t.CreateSecondaryIndex(fmt.Sprintf("existing%d", i), []string{"id"}, false); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := t.CreateSecondaryIndex("added", []string{"category"}, false); err != nil {
			b.Fatal(err)
		}
		t.DropSecondaryIndex("added")
	}
}

func BenchmarkCreateUniqueIndex(b *testing.B) {
	const rows = 1 << 14
	for _, input := range []struct {
		name   string
		stride int
	}{{"ascending", 1}, {"shuffled", 4051}} {
		b.Run(input.name, func(b *testing.B) {
			table := NewTable("unique_build", []Column{{Name: "id", Type: IntType}}, false)
			table.Rows = make([][]any, rows)
			for i := range table.Rows {
				// Every odd stride permutes a power-of-two row count.
				table.Rows[i] = []any{i * input.stride % rows}
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := table.CreateSecondaryIndex("unique_id", []string{"id"}, true); err != nil {
					b.Fatal(err)
				}
				table.DropSecondaryIndex("unique_id")
			}
		})
	}
}

func TestCreateIndexPreservesExistingStructures(t *testing.T) {
	table := NewTable("independent_indexes", []Column{{Name: "id", Type: IntType}, {Name: "category", Type: IntType}}, false)
	table.Rows = [][]any{{1, 7}, {2, 7}, {3, 9}}
	if err := table.CreateSecondaryIndex("original", []string{"id"}, true); err != nil {
		t.Fatal(err)
	}
	original := table.Indexes["original"]
	fast := original.hydrate()
	if err := table.CreateSecondaryIndex("category", []string{"category"}, false); err != nil {
		t.Fatal(err)
	}
	if original.hydrate() != fast {
		t.Fatal("existing index rebuilt")
	}
	if err := table.CreateSecondaryIndex("invalid", []string{"category"}, true); err == nil {
		t.Fatal("accepted duplicate unique key")
	}
	if table.Indexes["invalid"] != nil || original.hydrate() != fast {
		t.Fatal("failed build mutated index set")
	}
	rows := original.lookup(CanonicalIndexKey([]any{2}))
	if len(rows) != 1 || rows[0] != 1 {
		t.Fatalf("existing lookup changed: %v", rows)
	}
	clone := original.clone()
	if got := clone.lookup(CanonicalIndexKey([]any{3})); len(got) != 1 || got[0] != 2 {
		t.Fatal("snapshot lost index")
	}
}
