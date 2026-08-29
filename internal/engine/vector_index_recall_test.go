package engine

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ivfRecallDB builds a corpus with enough rows and clustering for the IVF
// index to produce more than a couple of lists.
func ivfRecallDB(t *testing.T, rows, dims int) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE docs (id INT, emb TEXT)`)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < rows; i++ {
		// Cluster around a handful of centres so k-means has real structure.
		centre := float64(i % 8)
		vec := make([]byte, 0, dims*8)
		vec = append(vec, '[')
		for d := 0; d < dims; d++ {
			if d > 0 {
				vec = append(vec, ',')
			}
			vec = fmt.Appendf(vec, "%.4f", centre+rng.NormFloat64()*0.35)
		}
		vec = append(vec, ']')
		execSQL(t, db, fmt.Sprintf(`INSERT INTO docs VALUES (%d, '%s')`, i, vec))
	}
	return db
}

func vecSearchIDs(t *testing.T, db *storage.DB, k int, metric, index string) []any {
	t.Helper()
	rs := execSQL(t, db, fmt.Sprintf(
		`SELECT id FROM VEC_SEARCH('docs', 'emb', '[3.0, 3.0, 3.0, 3.0]', %d, '%s', '%s')`,
		k, metric, index))
	ids := make([]any, 0, len(rs.Rows))
	for _, r := range rs.Rows {
		v, _ := ragValue(r, "id")
		ids = append(ids, v)
	}
	return ids
}

// An approximate index may rank differently from an exact scan, but it must
// still return the number of rows asked for when the table holds that many.
// The probe budget is a recall/latency trade and once capped the result count
// too: ivf returned 7 rows for k=10 on a 10-row table.
func TestIVFReturnsRequestedRowCount(t *testing.T) {
	for _, tc := range []struct{ rows, k int }{
		{10, 10},
		{10, 5},
		{200, 50},
		{200, 200},
	} {
		db := ivfRecallDB(t, tc.rows, 4)
		for _, metric := range []string{"cosine", "l2"} {
			got := len(vecSearchIDs(t, db, tc.k, metric, "ivf"))
			want := tc.k
			if tc.rows < want {
				want = tc.rows
			}
			if got != want {
				t.Errorf("rows=%d k=%d metric=%s: ivf returned %d rows, want %d",
					tc.rows, tc.k, metric, got, want)
			}
		}
	}
}

// The IVF inverted lists were built from the assignments made before the final
// k-means centroid update, so a row could sit in a list whose centroid was no
// longer its nearest and be missed even when that centroid was probed. With
// the lists consistent with the centroids, IVF should agree closely with the
// exact scan on a well-clustered corpus.
func TestIVFAgreesWithFlatScan(t *testing.T) {
	db := ivfRecallDB(t, 400, 4)
	const k = 20
	for _, metric := range []string{"cosine", "l2"} {
		exact := vecSearchIDs(t, db, k, metric, "flat")
		approx := vecSearchIDs(t, db, k, metric, "ivf")
		if len(approx) != k {
			t.Fatalf("metric=%s: ivf returned %d rows, want %d", metric, len(approx), k)
		}
		exactSet := make(map[any]struct{}, len(exact))
		for _, id := range exact {
			exactSet[id] = struct{}{}
		}
		overlap := 0
		for _, id := range approx {
			if _, ok := exactSet[id]; ok {
				overlap++
			}
		}
		// Deliberately not demanding an exact match: IVF is approximate by
		// design. This is a floor that the stale-assignment bug broke.
		if min := k * 8 / 10; overlap < min {
			t.Errorf("metric=%s: ivf overlapped the exact top-%d on only %d ids, want at least %d",
				metric, k, overlap, min)
		}
	}
}
