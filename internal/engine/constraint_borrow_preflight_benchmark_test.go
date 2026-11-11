package engine

import "testing"

func BenchmarkConstraintBorrowRefusal(b *testing.B) {
	for _, name := range []string{"MultiOnly", "Mixed", "MixedThenWrite"} {
		b.Run(name, func(b *testing.B) {
			set := &constraintIndexSet{cols: make(map[int]*constraintIndexEntry)}
			multi := newConstraintIndexEntry(0)
			multi.add("duplicate", 0)
			multi.add("duplicate", 1)
			set.cols[0] = multi
			var unique []*constraintIndexEntry
			if name != "MultiOnly" {
				for col := 1; col <= 8; col++ {
					entry := newConstraintIndexEntry(4096)
					for key := range 4096 {
						entry.add(key, key)
					}
					set.cols[col] = entry
					unique = append(unique, entry)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				if value, ok := set.BorrowDerived(); ok || value != nil {
					b.Fatal("mixed constraint set was borrowed")
				}
				if name == "MixedThenWrite" {
					for _, entry := range unique {
						entry.add("temporary", 4096)
						entry.remove("temporary", 4096)
					}
				}
			}
		})
	}
}
