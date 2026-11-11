package engine

import (
	"fmt"
	"testing"
)

func BenchmarkJoinCandidateBuild(b *testing.B) {
	for _, domain := range []string{"Integer", "Int64", "Text"} {
		for _, distinct := range []int{1, 40, 400} {
			b.Run(fmt.Sprintf("%s/Distinct%d", domain, distinct), func(b *testing.B) {
				outer, inner := make([]Row, 400), make([]Row, 400)
				for i := range outer {
					var key any = i % distinct
					if domain == "Int64" {
						key = int64(i % distinct)
					} else if domain == "Text" {
						key = fmt.Sprintf("key-%d", i%distinct)
					}
					outer[i], inner[i] = Row{"l.id": key}, Row{"r.id": key}
				}
				env := ExecEnv{ctx: b.Context()}
				equality := &joinEquality{leftColumn: "l.id", rightColumn: "r.id"}
				b.ReportAllocs()
				for b.Loop() {
					lookup, err := buildJoinCandidateLookup(env, equality, outer, inner)
					if err != nil || lookup == nil {
						b.Fatalf("lookup not built: %v", err)
					}
					_, count := lookup.candidates(0, len(inner))
					if count != len(inner)/distinct {
						b.Fatal("unexpected candidate count", count)
					}
				}
			})
		}
	}
}
