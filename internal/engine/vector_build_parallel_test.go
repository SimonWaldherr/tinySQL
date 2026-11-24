package engine

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"unsafe"
)

// vecBuildTestRows makes rows with every shape of cell a vector column can hold
// in practice: well-formed vectors of a few dimensions (a column in the middle of
// an embedding migration is ragged), NULLs, missing cells, empty and zero vectors,
// vectors stored as text, and values that are not vectors at all.
func vecBuildTestRows(rng *rand.Rand, n int) [][]any {
	dims := []int{3, 3, 3, 4, 8, 3, 3, 16}
	rows := make([][]any, n)
	for i := range rows {
		switch k := rng.Intn(40); {
		case k == 0:
			rows[i] = []any{i, nil}
		case k == 1:
			rows[i] = []any{i} // the cell is missing
		case k == 2:
			rows[i] = []any{i, []float64{}}
		case k == 3:
			rows[i] = []any{i, []float64{0, 0, 0}}
		case k == 4:
			rows[i] = []any{i, "[0.5, 1.5, -2.5]"}
		case k == 5:
			rows[i] = []any{i, "not a vector"}
		case k == 6:
			rows[i] = []any{i, 42}
		case k == 7:
			rows[i] = []any{i, []float32{1, 2, 3}}
		default:
			v := make([]float64, dims[rng.Intn(len(dims))])
			for j := range v {
				v[j] = rng.NormFloat64()
			}
			rows[i] = []any{i, v}
		}
	}
	return rows
}

// The parallel build is the serial build, byte for byte: same packed array, same
// per-row views (clipped to their own length and aliasing the array at the right
// offsets), same norms and validity.
func TestBuildVecColumnSegmentParallelMatchesSerial(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for _, n := range []int{0, 1, 2, vecBuildChunkRows - 1, vecBuildChunkRows, vecBuildChunkRows + 1, vecBuildParallelMinRows, 4097, 20_000} {
		rows := vecBuildTestRows(rng, n)
		for _, norms := range []bool{false, true} {
			want := buildVecColumnSegmentSerial(rows, 1, norms, 7)
			for _, workers := range []int{2, 3, 5, 16} {
				t.Run(fmt.Sprintf("rows=%d/norms=%t/workers=%d", n, norms, workers), func(t *testing.T) {
					got := buildVecColumnSegmentParallel(rows, 1, norms, 7, workers)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("parallel build differs from the serial build (data %d/%d floats, %d/%d norms)",
							len(got.data), len(want.data), len(got.norms), len(want.norms))
					}
					offset := 0
					for i, vec := range got.vectors {
						if !got.valid[i] {
							if vec != nil {
								t.Fatalf("row %d is invalid but has a vector", i)
							}
							continue
						}
						if cap(vec) != len(vec) {
							t.Fatalf("row %d: capacity %d, length %d (an append could overwrite the next row)", i, cap(vec), len(vec))
						}
						if len(vec) > 0 && unsafe.Pointer(&vec[0]) != unsafe.Pointer(&got.data[offset]) {
							t.Fatalf("row %d does not sit at offset %d of the packed array", i, offset)
						}
						offset += len(vec)
					}
					if offset != len(got.data) {
						t.Fatalf("rows cover %d floats, the array holds %d", offset, len(got.data))
					}
				})
			}
		}
	}
}

// The public entry point picks the parallel build only for large inputs and
// agrees with the reference either way.
func TestBuildVecColumnSegmentDispatch(t *testing.T) {
	if vecBuildWorkerCount(vecBuildParallelMinRows-1) != 1 {
		t.Fatal("a small segment must be built serially")
	}
	rng := rand.New(rand.NewSource(12))
	for _, n := range []int{10, vecBuildParallelMinRows - 1, 50_000} {
		rows := vecBuildTestRows(rng, n)
		if got, want := buildVecColumnSegment(rows, 1, true, 0), buildVecColumnSegmentSerial(rows, 1, true, 0); !reflect.DeepEqual(got, want) {
			t.Fatalf("rows=%d: buildVecColumnSegment differs from the serial build", n)
		}
	}
}
