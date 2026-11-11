package engine

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkUnionAlignmentBoundKeys(b *testing.B) {
	for _, mixedCase := range []bool{false, true} {
		b.Run(fmt.Sprintf("mixedCase=%v", mixedCase), func(b *testing.B) {
			source := []string{"SourceID", "SourceLabel", "SourceValue", "SourceNote"}
			target := []string{"ResultID", "ResultLabel", "ResultValue", "ResultNote"}
			if !mixedCase {
				for i := range source {
					source[i], target[i] = strings.ToLower(source[i]), strings.ToLower(target[i])
				}
			}
			rows := make([]Row, 20000)
			for i := range rows {
				rows[i] = Row{"sourceid": i, "sourcelabel": "α🙂", "sourcevalue": i % 17, "sourcenote": "note"}
			}
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				got, err := alignSetOperationRowsWithContext(ctx, rows, source, target)
				if err != nil || len(got) != len(rows) || got[len(got)-1]["resultid"] != len(rows)-1 {
					b.Fatalf("alignment: rows=%d / %v", len(got), err)
				}
			}
		})
	}
}

func BenchmarkUnionAliasedQuery(b *testing.B) {
	db := setupPerfTable(b, 20000)
	b.Cleanup(func() { _ = db.Close() })
	for _, operator := range []string{"UNION", "UNION ALL"} {
		b.Run(operator, func(b *testing.B) {
			runBench(b, db, "SELECT id AS ResultID,grp AS ResultGroup,val AS ResultValue,note AS ResultNote FROM t "+operator+
				" SELECT id AS SourceID,grp AS SourceGroup,val AS SourceValue,note AS SourceNote FROM t")
		})
	}
}

func BenchmarkIntersectMembershipSubset(b *testing.B) {
	left := make([]Row, 20000)
	for i := range left {
		left[i] = Row{"value": i % 1000}
	}
	for _, count := range []int{10, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			right := make([]Row, count)
			for i := range right {
				right[i] = Row{"value": i}
			}
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				got, err := intersectRowsWithContext(ctx, left, right, []string{"value"})
				if err != nil || len(got) != count {
					b.Fatalf("intersection: rows=%d / %v, want %d", len(got), err, count)
				}
			}
		})
	}
}
