package engine

import "testing"

func BenchmarkRecursiveCTEAlignment(b *testing.B) {
	anchor := &ResultSet{Cols: []string{"A", "B", "C"}}
	next := &ResultSet{Cols: []string{"x", "y", "z"}, Rows: make([]Row, 20000)}
	for i := range next.Rows {
		next.Rows[i] = Row{"x": i, "y": i + 1, "z": i + 2}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(alignRecursiveCTERows(anchor, next, "series")) != len(next.Rows) {
			b.Fatal("lost rows")
		}
	}
}

func TestRecursiveCTEAlignmentPreservesAliases(t *testing.T) {
	anchor := &ResultSet{Cols: []string{"First", "Second"}}
	next := &ResultSet{Cols: []string{"X", "Y"}, Rows: []Row{{"x": nil, "X": 99, "Y": 2}, {"x": 3, "y": 4}}}
	got := alignRecursiveCTERows(anchor, next, "Mixed")
	if got[0]["first"] != nil || got[0]["mixed.first"] != nil || got[0]["second"] != 2 || got[1]["mixed.second"] != 4 {
		t.Fatalf("bad aliases: %#v", got)
	}
	got[1]["first"] = 100
	if next.Rows[1]["x"] != 3 {
		t.Fatal("source rows mutated")
	}
}
