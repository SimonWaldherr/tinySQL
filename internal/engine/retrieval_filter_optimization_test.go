package engine

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoCandidateMergeOwnershipDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(7301))
	for trial := 0; trial < 200; trial++ {
		groups := make([][]int32, trial%5)
		original := make([][]int32, len(groups))
		set := map[int32]bool{}
		for i := range groups {
			for j := 0; j < trial; j++ {
				row := int32(rng.Intn(1000))
				if trial%3 == 1 {
					row *= 1000000 // Sparse IDs must not allocate a huge bitmap.
				} else if trial%3 == 2 {
					row += -1 << 31 // Exercise signed range arithmetic.
				}
				groups[i] = append(groups[i], row)
				set[row] = true
			}
			original[i] = slices.Clone(groups[i])
		}
		got := geoMergeCandidateRows(groups...)
		if len(got) != len(set) {
			t.Fatalf("trial %d: got %d rows, want %d", trial, len(got), len(set))
		}
		for _, row := range got {
			if !set[row] {
				t.Fatalf("unexpected or duplicate row %d", row)
			}
			delete(set, row)
		}
		if len(got) > 0 {
			got[0] = -999
		}
		for i := range groups {
			if !slices.Equal(groups[i], original[i]) {
				t.Fatal("merge modified or aliased input")
			}
		}
	}
}

func TestRAGCombinedEqualityDifferential(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprint(indexed), func(t *testing.T) {
			db := storage.NewDB()
			execSQL(t, db, `CREATE TABLE filter_followup (id TEXT, tenant TEXT, quality INT)`)
			for i := 0; i < 256; i++ {
				execSQL(t, db, fmt.Sprintf(`INSERT INTO filter_followup VALUES ('id-%d', 'tenant-%d', %d)`, i, i%4, i%3))
			}
			if indexed {
				execSQL(t, db, `CREATE INDEX filter_followup_id ON filter_followup(id)`)
				execSQL(t, db, `CREATE INDEX filter_followup_tenant ON filter_followup(tenant)`)
			}
			table, _ := db.Get("default", "filter_followup")
			for _, n := range []int{0, 1, 8, 16, 17, 256} {
				ids := make([]any, 0, n+1)
				for i := n - 1; i >= 0; i-- {
					ids = append(ids, fmt.Sprintf("id-%d", i))
				}
				ids = append(ids, "missing")
				for _, equals := range []map[string]any{{"tenant": "tenant-0"}, {"tenant": "tenant-0", "quality": 0}, {"tenant": "missing"}} {
					opts := &ragPreFilterOptions{IDColumn: "id", AllowedRowIDs: ids, Equals: equals}
					got, err := ragBuildRowFilter(table, opts)
					if err != nil {
						t.Fatal(err)
					}
					predicates, err := ragNormalizeEqualityPredicates(table, equals)
					if err != nil {
						t.Fatal(err)
					}
					var want []int
					for i, row := range table.Rows {
						if i < n && ragRowMatchesEqualities(row, predicates) {
							want = append(want, i)
						}
					}
					if !slices.Equal(got.rows, want) {
						t.Fatalf("n=%d equals=%v: got %v want %v", n, equals, got.rows, want)
					}
				}
			}
			// An empty ACL must still validate the rest of the filter.
			if _, err := ragBuildRowFilter(table, &ragPreFilterOptions{IDColumn: "id", AllowedRowIDs: []any{}, Equals: map[string]any{"missing_column": 1}}); err == nil {
				t.Fatal("empty ACL suppressed an invalid equality")
			}
		})
	}
}

func BenchmarkRetrievalFilterFollowup(b *testing.B) {
	for _, n := range []int{32, 20000} {
		rng := rand.New(rand.NewSource(7301))
		a, c := make([]int32, n), make([]int32, n)
		rows := rng.Perm(n)
		for i := range a {
			a[i], c[i] = int32(rows[i]), int32(rows[i]+n/2)
		}
		b.Run(fmt.Sprintf("geo_merge/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if len(geoMergeCandidateRows(a, c)) != n+n/2 {
					b.Fatal("wrong union")
				}
			}
		})
		sparseA, sparseC := slices.Clone(a), slices.Clone(c)
		for i := range sparseA {
			sparseA[i] *= 50000
			sparseC[i] *= 50000
		}
		b.Run(fmt.Sprintf("geo_merge_sparse/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if len(geoMergeCandidateRows(sparseA, sparseC)) != n+n/2 {
					b.Fatal("wrong sparse union")
				}
			}
		})
		b.Run(fmt.Sprintf("normalize/%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if len(ragNormalizeRowIDs(n, rows)) != n {
					b.Fatal("wrong normalization")
				}
			}
		})
	}
	const n = 20000
	table := storage.NewTable("filter_followup", []storage.Column{{Name: "id", Type: storage.TextType}, {Name: "tenant", Type: storage.TextType}}, false)
	for i := 0; i < n; i++ {
		table.Rows = append(table.Rows, []any{fmt.Sprintf("id-%d", i), fmt.Sprintf("tenant-%d", i%4)})
	}
	predicates := []ragEqualityPredicate{{column: "tenant", pos: 1, value: "tenant-0"}}
	b.Run("equality_scan", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, err := ragRowsForEqualities(table, predicates)
			if err != nil || len(rows) != n/4 {
				b.Fatalf("rows=%d err=%v", len(rows), err)
			}
		}
	})
	for _, count := range []int{8, 2000} {
		ids := make([]any, count)
		for i := range ids {
			ids[i] = fmt.Sprintf("id-%d", i)
		}
		opts := &ragPreFilterOptions{IDColumn: "id", AllowedRowIDs: ids, Equals: map[string]any{"tenant": "tenant-0"}}
		key, _ := ragRowFilterCacheKeyFor(table, opts)
		b.Run(fmt.Sprintf("combined_uncached/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				ragRowFilterCacheMu.Lock()
				delete(ragRowFilterCache, key)
				ragRowFilterCacheMu.Unlock()
				filter, err := ragBuildRowFilter(table, opts)
				if err != nil || len(filter.rows) != count/4 {
					b.Fatalf("filter=%v err=%v", filter, err)
				}
			}
		})
	}
}

func BenchmarkRetrievalDatelineSQL(b *testing.B) {
	db := storage.NewDB()
	table := storage.NewTable("dateline_followup", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "geom", Type: storage.GeometryType}}, false)
	for i := 0; i < 20000; i++ {
		lon := 170 + float64(i%200)/10
		if lon > 180 {
			lon -= 360
		}
		lat := float64(i/200) / 10
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf(`{"type":"Point","coordinates":[%v,%v]}`, lon, lat)})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`SELECT id FROM GEO_SEARCH('dateline_followup','geom','bbox_intersects',175,2,-175,8)`)
	warm, err := Execute(context.Background(), db, "default", stmt)
	if err != nil || len(warm.Rows) != 6161 {
		b.Fatalf("warm result=%v err=%v", warm, err)
	}
	b.ReportAllocs()
	for b.Loop() {
		result, err := Execute(context.Background(), db, "default", stmt)
		if err != nil || len(result.Rows) != 6161 {
			b.Fatalf("result=%v err=%v", result, err)
		}
	}
}
