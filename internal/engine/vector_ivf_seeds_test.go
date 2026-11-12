package engine

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestIVFSeedsStratificationAndOwnership(t *testing.T) {
	for _, count := range []int{1, 2, 7, 127, 4096} {
		table := storage.NewTable("seed_rows", []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
		for i := range count {
			table.Rows = append(table.Rows, []any{[]float64{float64(i), 1}})
		}
		cache := buildVecColumnCache(table, 0, true)
		rows := validVectorRows(cache, 2)
		original := slices.Clone(rows)
		lists := chooseIVFListCount(count)
		centroids := seedIVFCentroids(rows, lists, cache)
		if !reflect.DeepEqual(centroids, seedIVFCentroids(rows, lists, cache)) || !slices.Equal(rows, original) {
			t.Fatal("seeding is not repeatable or changed row order")
		}
		start := 0
		for c, centroid := range centroids {
			size := count / lists
			if c < count%lists {
				size++
			}
			row := int(centroid[0])
			if row < start || row >= start+size {
				t.Fatalf("centroid %d: row %d outside [%d,%d)", c, row, start, start+size)
			}
			centroid[1] = -1
			if cache.vector(row)[1] != 1 {
				t.Fatal("centroid aliases the input vector")
			}
			start += size
		}
	}
}

// Build the same corpus in three ingestion orders. Periodic ingestion is a
// fixed-stride trap: 4096 rows / 32 lists = 128, divisible by all eight groups.
func ivfSeedQualityFixture(order string, rows, dims int) (*storage.Table, vecSearchColumnCacheEntry) {
	table := storage.NewTable("seed_quality", []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
	rng := rand.New(rand.NewSource(53))
	for row := range rows {
		v := make([]float64, dims)
		for d := range v {
			v[d] = rng.NormFloat64() * 0.15
			if d%8 == row%8 {
				v[d] += 10
			}
		}
		table.Rows = append(table.Rows, []any{v})
	}
	switch order {
	case "sorted":
		sort.SliceStable(table.Rows, func(i, j int) bool {
			a, b := table.Rows[i][0].([]float64), table.Rows[j][0].([]float64)
			group := func(v []float64) int {
				best := 0
				for d := 1; d < 8; d++ {
					if v[d] > v[best] {
						best = d
					}
				}
				return best
			}
			return group(a) < group(b)
		})
	case "shuffled":
		rng.Shuffle(len(table.Rows), func(i, j int) { table.Rows[i], table.Rows[j] = table.Rows[j], table.Rows[i] })
	}
	return table, buildVecColumnCache(table, 0, true)
}

func ivfIndependentDistance(metric string, a, b []float64) float64 {
	var squared, dot, normA, normB float64
	for d := range a {
		delta := a[d] - b[d]
		squared += delta * delta
		dot += a[d] * b[d]
		normA += a[d] * a[d]
		normB += b[d] * b[d]
	}
	if metric == "cosine" {
		return 1 - dot/math.Sqrt(normA*normB)
	}
	return squared
}

func ivfIndependentTopK(cache vecSearchColumnCacheEntry, query []float64, metric string, k int) []int {
	rows := make([]int, cache.rowCount())
	distances := make([]float64, len(rows))
	for row := range rows {
		rows[row] = row
		distances[row] = ivfIndependentDistance(metric, cache.vector(row), query)
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		return distances[a] < distances[b] || distances[a] == distances[b] && a < b
	})
	return rows[:k]
}

func TestIVFSeedsAvoidPeriodicIngestionAlias(t *testing.T) {
	for _, order := range []string{"periodic", "sorted", "shuffled"} {
		for _, metric := range []string{"l2", "cosine"} {
			t.Run(fmt.Sprintf("%s/%s", order, metric), func(t *testing.T) {
				table, cache := ivfSeedQualityFixture(order, 4096, 16)
				idx, err := buildVecIVFIndex(t.Context(), table, metric, 16, cache)
				if err != nil {
					t.Fatal(err)
				}
				var loss float64
				used := 0
				seen := make([]bool, cache.rowCount())
				for c, list := range idx.lists {
					if len(list) > 0 {
						used++
					}
					for _, row := range list {
						if seen[row] {
							t.Fatal("duplicate inverted-list row")
						}
						seen[row] = true
						loss += ivfIndependentDistance(metric, cache.vector(row), idx.centroids[c])
					}
				}
				for _, found := range seen {
					if !found {
						t.Fatal("missing inverted-list row")
					}
				}
				loss /= float64(cache.rowCount())
				maxLoss := 0.5
				if metric == "cosine" {
					maxLoss = 0.002
				}
				if used < len(idx.lists)/2 || loss > maxLoss {
					t.Fatalf("poor partition: occupied lists %d/%d, mean loss %g", used, len(idx.lists), loss)
				}
				hits := 0
				for group := 0; group < 8; group++ {
					// Held-out query: not one of the training vectors.
					query := make([]float64, 16)
					for d := range query {
						query[d] = math.Sin(float64(d+group)) * 0.03
						if d%8 == group {
							query[d] += 10
						}
					}
					want := ivfIndependentTopK(cache, query, metric, 10)
					got, err := idx.search(t.Context(), query, vectorL2Norm(query), 10, cache)
					if err != nil || len(got) != 10 {
						t.Fatalf("search: %d rows, %v", len(got), err)
					}
					for _, row := range got {
						if slices.Contains(want, row.rowIdx) {
							hits++
						}
					}
				}
				if hits < 72 {
					t.Fatalf("held-out recall@10 = %d/80, want >= 72", hits)
				}
				t.Logf("occupied=%d/%d mean-loss=%g recall@10=%d/80", used, len(idx.lists), loss, hits)
			})
		}
	}
}
