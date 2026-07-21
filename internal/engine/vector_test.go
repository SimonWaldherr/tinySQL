// Vector functions and RAG vector search tests for tinySQL.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func execSQL(t *testing.T, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(context.Background(), db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("SQL failed: %s\n  error: %v", sql, err)
	}
	return rs
}

func expectFloat(t *testing.T, got any, want float64, tol float64, label string) {
	t.Helper()
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("%s: expected float64, got %T (%v)", label, got, got)
	}
	if math.Abs(f-want) > tol {
		t.Errorf("%s: got %v, want %v (±%v)", label, f, want, tol)
	}
}

func expectInt(t *testing.T, got any, want int, label string) {
	t.Helper()
	switch v := got.(type) {
	case int:
		if v != want {
			t.Errorf("%s: got %d, want %d", label, v, want)
		}
	case float64:
		if int(v) != want {
			t.Errorf("%s: got %v, want %d", label, v, want)
		}
	default:
		t.Fatalf("%s: expected int, got %T (%v)", label, got, got)
	}
}

// ---------------------------------------------------------------------------
// VEC_FROM_JSON / VEC_TO_JSON
// ---------------------------------------------------------------------------

func TestVecFromJSON(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_FROM_JSON('[1.0, 2.0, 3.0]') as v`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	vec, ok := rs.Rows[0]["v"].([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", rs.Rows[0]["v"])
	}
	if len(vec) != 3 || vec[0] != 1.0 || vec[1] != 2.0 || vec[2] != 3.0 {
		t.Errorf("unexpected vector: %v", vec)
	}
}

func TestVecToJSON(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_TO_JSON(VEC_FROM_JSON('[1.5, 2.5, 3.5]')) as j`)
	s, ok := rs.Rows[0]["j"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rs.Rows[0]["j"])
	}
	var arr []float64
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(arr) != 3 || arr[0] != 1.5 {
		t.Errorf("unexpected JSON array: %v", arr)
	}
}

// ---------------------------------------------------------------------------
// VEC_DIM
// ---------------------------------------------------------------------------

func TestVecDim(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_DIM(VEC_FROM_JSON('[1,2,3,4,5]')) as d`)
	expectInt(t, rs.Rows[0]["d"], 5, "VEC_DIM")
}

// ---------------------------------------------------------------------------
// VEC_NORM
// ---------------------------------------------------------------------------

func TestVecNorm(t *testing.T) {
	db := storage.NewDB()
	// Norm of [3, 4] should be 5
	rs := execSQL(t, db, `SELECT VEC_NORM(VEC_FROM_JSON('[3.0, 4.0]')) as n`)
	expectFloat(t, rs.Rows[0]["n"], 5.0, 1e-9, "VEC_NORM")
}

// ---------------------------------------------------------------------------
// VEC_NORMALIZE
// ---------------------------------------------------------------------------

func TestVecNormalize(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_NORMALIZE(VEC_FROM_JSON('[3.0, 4.0]')) as v`)
	vec, ok := rs.Rows[0]["v"].([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", rs.Rows[0]["v"])
	}
	// [3/5, 4/5] = [0.6, 0.8]
	expectFloat(t, vec[0], 0.6, 1e-9, "VEC_NORMALIZE[0]")
	expectFloat(t, vec[1], 0.8, 1e-9, "VEC_NORMALIZE[1]")
}

// ---------------------------------------------------------------------------
// VEC_ADD / VEC_SUB
// ---------------------------------------------------------------------------

func TestVecAdd(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_ADD(VEC_FROM_JSON('[1,2,3]'), VEC_FROM_JSON('[4,5,6]')) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	if vec[0] != 5 || vec[1] != 7 || vec[2] != 9 {
		t.Errorf("VEC_ADD: expected [5,7,9], got %v", vec)
	}
}

func TestVecSub(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_SUB(VEC_FROM_JSON('[5,7,9]'), VEC_FROM_JSON('[1,2,3]')) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	if vec[0] != 4 || vec[1] != 5 || vec[2] != 6 {
		t.Errorf("VEC_SUB: expected [4,5,6], got %v", vec)
	}
}

// ---------------------------------------------------------------------------
// VEC_MUL
// ---------------------------------------------------------------------------

func TestVecMul(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_MUL(VEC_FROM_JSON('[2,3,4]'), VEC_FROM_JSON('[5,6,7]')) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	if vec[0] != 10 || vec[1] != 18 || vec[2] != 28 {
		t.Errorf("VEC_MUL: expected [10,18,28], got %v", vec)
	}
}

// ---------------------------------------------------------------------------
// VEC_SCALE
// ---------------------------------------------------------------------------

func TestVecScale(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_SCALE(VEC_FROM_JSON('[1,2,3]'), 2.5) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	if vec[0] != 2.5 || vec[1] != 5.0 || vec[2] != 7.5 {
		t.Errorf("VEC_SCALE: expected [2.5,5,7.5], got %v", vec)
	}
}

// ---------------------------------------------------------------------------
// VEC_DOT
// ---------------------------------------------------------------------------

func TestVecDot(t *testing.T) {
	db := storage.NewDB()
	// [1,2,3] · [4,5,6] = 4 + 10 + 18 = 32
	rs := execSQL(t, db, `SELECT VEC_DOT(VEC_FROM_JSON('[1,2,3]'), VEC_FROM_JSON('[4,5,6]')) as d`)
	expectFloat(t, rs.Rows[0]["d"], 32.0, 1e-9, "VEC_DOT")
}

func TestVecDotLargeVectorKernel(t *testing.T) {
	a := make([]float64, 257)
	b := make([]float64, 257)
	var want float64
	for i := range a {
		a[i] = math.Sin(float64(i) * 0.11)
		b[i] = math.Cos(float64(i) * 0.07)
		want += a[i] * b[i]
	}
	expectFloat(t, vectorDot(a, b), want, 1e-9, "large vector dot kernel")
	if vectorMathBackend == "" {
		t.Fatal("vector math backend must be named")
	}
}

func TestVecL2SquaredLargeVectorKernel(t *testing.T) {
	a := make([]float64, 257)
	b := make([]float64, 257)
	var want float64
	for i := range a {
		a[i] = math.Sin(float64(i)*0.13) * 0.75
		b[i] = math.Cos(float64(i)*0.09) * 0.5
		d := a[i] - b[i]
		want += d * d
	}
	expectFloat(t, vectorL2Squared(a, b), want, 1e-9, "large vector l2 kernel")
}

// TestVecL1LargeVectorKernel exercises vectorL1Distance above the SIMD
// threshold (128 elements) so it actually dispatches to vectorL1Kernel
// (SSE2 on amd64) rather than the portable unrolled fallback, checked
// against a plain, unoptimized reference sum computed independently here.
func TestVecL1LargeVectorKernel(t *testing.T) {
	a := make([]float64, 257)
	b := make([]float64, 257)
	var want float64
	for i := range a {
		a[i] = math.Sin(float64(i)*0.17) * 0.75
		b[i] = math.Cos(float64(i)*0.05) * 0.5
		want += math.Abs(a[i] - b[i])
	}
	expectFloat(t, vectorL1Distance(a, b), want, 1e-9, "large vector l1 kernel")
}

// TestVecL1KernelMatchesUnrolledAcrossSizes checks the SIMD kernel and the
// portable unrolled fallback agree across sizes that straddle the SIMD
// threshold (128) and the assembly's own inner-loop width (8) and tail
// handling, including sizes with no full 8-wide iteration at all.
func TestVecL1KernelMatchesUnrolledAcrossSizes(t *testing.T) {
	for _, n := range []int{0, 1, 3, 7, 8, 9, 15, 16, 17, 127, 128, 129, 255, 256, 300} {
		a := make([]float64, n)
		b := make([]float64, n)
		for i := range a {
			a[i] = math.Sin(float64(i)*0.31) * 2.0
			b[i] = math.Cos(float64(i)*0.19) * 1.5
		}
		got := vectorL1Distance(a, b)
		want := vectorL1Unrolled(a, b)
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("n=%d: kernel=%v unrolled=%v (diff %v)", n, got, want, got-want)
		}
	}
}

// TestVecDotKernelMatchesUnrolledAcrossSizes checks kernel dispatch (which on
// amd64 may route to SSE2 or AVX2+FMA depending on the CPU and length
// threshold) against the portable unrolled loop, across sizes straddling the
// AVX2 dispatch threshold (16), the main-loop widths (16 for dot/l2), and
// odd tails.
func TestVecDotKernelMatchesUnrolledAcrossSizes(t *testing.T) {
	for _, n := range []int{0, 1, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 127, 128, 129, 300, 768} {
		a := make([]float64, n)
		b := make([]float64, n)
		for i := range a {
			a[i] = math.Sin(float64(i)*0.23) * 1.5
			b[i] = math.Cos(float64(i)*0.29) * 0.8
		}
		got := vectorDot(a, b)
		want := vectorDotUnrolled(a, b)
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("dot n=%d: kernel=%v unrolled=%v (diff %v)", n, got, want, got-want)
		}
		got = vectorL2Squared(a, b)
		want = vectorL2SquaredUnrolled(a, b)
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("l2 n=%d: kernel=%v unrolled=%v (diff %v)", n, got, want, got-want)
		}
	}
}

// TestVecCosineKernelMatchesUnrolledAcrossSizes checks the fused
// dot/normA²/normB² kernel against the portable fused loop across sizes
// straddling the AVX2 dispatch threshold (8) and tail handling.
func TestVecCosineKernelMatchesUnrolledAcrossSizes(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 63, 64, 127, 128, 129, 300, 768} {
		a := make([]float64, n)
		b := make([]float64, n)
		for i := range a {
			a[i] = math.Sin(float64(i)*0.37) * 1.2
			b[i] = math.Cos(float64(i)*0.41) * 0.9
		}
		gd, gna, gnb := vectorCosineParts(a, b)
		wd, wna, wnb := vectorCosineUnrolled(a, b)
		if math.Abs(gd-wd) > 1e-9 || math.Abs(gna-wna) > 1e-9 || math.Abs(gnb-wnb) > 1e-9 {
			t.Errorf("cosine n=%d: kernel=(%v,%v,%v) unrolled=(%v,%v,%v)", n, gd, gna, gnb, wd, wna, wnb)
		}
	}
}

// TestDropTablePurgesVectorCaches guards against the vector caches pinning a
// dropped table's row data for the life of the process: the column cache and
// the IVF/HNSW index caches each hold a *storage.Table, and their keys
// (tenant, table, column) are never written again after a DROP, so without
// an eager purge the entries — and through them every row of the dropped
// table — stayed reachable forever.
func TestDropTablePurgesVectorCaches(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE purge_me (id INT, embedding VECTOR)`)
	for i := 0; i < 8; i++ {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO purge_me VALUES (%d, VEC_FROM_JSON('[%d.0, 1.0, 0.5]'))`, i, i%3))
	}
	// Warm all three cache kinds.
	execSQL(t, db, `SELECT * FROM VEC_WARM('purge_me', 'embedding', 'cosine', 'ivf')`)
	execSQL(t, db, `SELECT * FROM VEC_WARM('purge_me', 'embedding', 'cosine', 'hnsw')`)

	countEntries := func() (n int) {
		vecSearchColumnCacheMu.RLock()
		for k := range vecSearchColumnCache {
			if k.table == "purge_me" {
				n++
			}
		}
		vecSearchColumnCacheMu.RUnlock()
		vecIVFCacheMu.RLock()
		for k := range vecIVFCache {
			if k.table == "purge_me" {
				n++
			}
		}
		vecIVFCacheMu.RUnlock()
		vecHNSWCacheMu.RLock()
		for k := range vecHNSWCache {
			if k.table == "purge_me" {
				n++
			}
		}
		vecHNSWCacheMu.RUnlock()
		return n
	}

	if n := countEntries(); n == 0 {
		t.Fatal("expected warm caches to hold entries before DROP")
	}
	execSQL(t, db, `DROP TABLE purge_me`)
	if n := countEntries(); n != 0 {
		t.Fatalf("expected all vector cache entries purged after DROP, %d remain", n)
	}
}

// ---------------------------------------------------------------------------
// Parse-time constant folding of pure vector functions
// ---------------------------------------------------------------------------

// TestVecFromJSONConstantFolding verifies that VEC_FROM_JSON over a string
// literal is folded to a vector literal at parse time — the optimization that
// keeps RAG queries from re-parsing the query-vector JSON once per row.
func TestVecFromJSONConstantFolding(t *testing.T) {
	folded := foldConstFuncCall(&FuncCall{
		Name: "VEC_FROM_JSON",
		Args: []Expr{&Literal{Val: "[1.5, 2.5]"}},
	})
	lit, ok := folded.(*Literal)
	if !ok {
		t.Fatalf("expected fold to *Literal, got %T", folded)
	}
	vec, ok := lit.Val.([]float64)
	if !ok || len(vec) != 2 || vec[0] != 1.5 || vec[1] != 2.5 {
		t.Fatalf("expected [1.5 2.5], got %v", lit.Val)
	}

	// Non-literal args must not fold.
	unfolded := foldConstFuncCall(&FuncCall{
		Name: "VEC_FROM_JSON",
		Args: []Expr{&VarRef{Name: "col"}},
	})
	if _, ok := unfolded.(*FuncCall); !ok {
		t.Fatalf("expected non-literal args to stay a *FuncCall, got %T", unfolded)
	}

	// Invalid input must stay unfolded so the error surfaces at execution.
	bad := foldConstFuncCall(&FuncCall{
		Name: "VEC_FROM_JSON",
		Args: []Expr{&Literal{Val: "not json"}},
	})
	if _, ok := bad.(*FuncCall); !ok {
		t.Fatalf("expected invalid JSON to stay a *FuncCall, got %T", bad)
	}
}

// TestVecFromJSONInvalidStillErrors guards the error semantics after
// constant folding: a query evaluating VEC_FROM_JSON on malformed JSON must
// still fail at execution time.
func TestVecFromJSONInvalidStillErrors(t *testing.T) {
	db := storage.NewDB()
	_, err := Execute(context.Background(), db, "default", mustParse(`SELECT VEC_FROM_JSON('nope') as v`))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

// ---------------------------------------------------------------------------
// VEC_COSINE_SIMILARITY / VEC_COSINE_DISTANCE
// ---------------------------------------------------------------------------

func TestVecCosineSimilarity(t *testing.T) {
	db := storage.NewDB()
	// Same vector → similarity = 1.0
	rs := execSQL(t, db, `SELECT VEC_COSINE_SIMILARITY(VEC_FROM_JSON('[1,0]'), VEC_FROM_JSON('[1,0]')) as s`)
	expectFloat(t, rs.Rows[0]["s"], 1.0, 1e-9, "VEC_COSINE_SIMILARITY same")

	// Orthogonal → similarity = 0.0
	rs = execSQL(t, db, `SELECT VEC_COSINE_SIMILARITY(VEC_FROM_JSON('[1,0]'), VEC_FROM_JSON('[0,1]')) as s`)
	expectFloat(t, rs.Rows[0]["s"], 0.0, 1e-9, "VEC_COSINE_SIMILARITY orthogonal")

	// Opposite → similarity = -1.0
	rs = execSQL(t, db, `SELECT VEC_COSINE_SIMILARITY(VEC_FROM_JSON('[1,0]'), VEC_FROM_JSON('[-1,0]')) as s`)
	expectFloat(t, rs.Rows[0]["s"], -1.0, 1e-9, "VEC_COSINE_SIMILARITY opposite")
}

func TestVecCosineDistance(t *testing.T) {
	db := storage.NewDB()
	// Same vector → distance = 0.0
	rs := execSQL(t, db, `SELECT VEC_COSINE_DISTANCE(VEC_FROM_JSON('[1,0]'), VEC_FROM_JSON('[1,0]')) as d`)
	expectFloat(t, rs.Rows[0]["d"], 0.0, 1e-9, "VEC_COSINE_DISTANCE same")

	// Opposite → distance = 2.0
	rs = execSQL(t, db, `SELECT VEC_COSINE_DISTANCE(VEC_FROM_JSON('[1,0]'), VEC_FROM_JSON('[-1,0]')) as d`)
	expectFloat(t, rs.Rows[0]["d"], 2.0, 1e-9, "VEC_COSINE_DISTANCE opposite")
}

// ---------------------------------------------------------------------------
// VEC_L2_DISTANCE
// ---------------------------------------------------------------------------

func TestVecL2Distance(t *testing.T) {
	db := storage.NewDB()
	// Distance between [0,0] and [3,4] = 5
	rs := execSQL(t, db, `SELECT VEC_L2_DISTANCE(VEC_FROM_JSON('[0,0]'), VEC_FROM_JSON('[3,4]')) as d`)
	expectFloat(t, rs.Rows[0]["d"], 5.0, 1e-9, "VEC_L2_DISTANCE")
}

// ---------------------------------------------------------------------------
// VEC_MANHATTAN_DISTANCE
// ---------------------------------------------------------------------------

func TestVecManhattanDistance(t *testing.T) {
	db := storage.NewDB()
	// Manhattan [1,2,3] to [4,6,8] = 3+4+5 = 12
	rs := execSQL(t, db, `SELECT VEC_MANHATTAN_DISTANCE(VEC_FROM_JSON('[1,2,3]'), VEC_FROM_JSON('[4,6,8]')) as d`)
	expectFloat(t, rs.Rows[0]["d"], 12.0, 1e-9, "VEC_MANHATTAN_DISTANCE")
}

// ---------------------------------------------------------------------------
// VEC_DISTANCE with metric parameter
// ---------------------------------------------------------------------------

func TestVecDistance(t *testing.T) {
	db := storage.NewDB()

	// Cosine (default)
	rs := execSQL(t, db, `SELECT VEC_DISTANCE(VEC_FROM_JSON('[1,0]'), VEC_FROM_JSON('[1,0]')) as d`)
	expectFloat(t, rs.Rows[0]["d"], 0.0, 1e-9, "VEC_DISTANCE cosine default")

	// L2
	rs = execSQL(t, db, `SELECT VEC_DISTANCE(VEC_FROM_JSON('[0,0]'), VEC_FROM_JSON('[3,4]'), 'l2') as d`)
	expectFloat(t, rs.Rows[0]["d"], 5.0, 1e-9, "VEC_DISTANCE l2")

	// Manhattan
	rs = execSQL(t, db, `SELECT VEC_DISTANCE(VEC_FROM_JSON('[0,0]'), VEC_FROM_JSON('[3,4]'), 'manhattan') as d`)
	expectFloat(t, rs.Rows[0]["d"], 7.0, 1e-9, "VEC_DISTANCE manhattan")
}

// ---------------------------------------------------------------------------
// VEC_SLICE
// ---------------------------------------------------------------------------

func TestVecSlice(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_SLICE(VEC_FROM_JSON('[10,20,30,40,50]'), 1, 3) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	if len(vec) != 3 || vec[0] != 20 || vec[1] != 30 || vec[2] != 40 {
		t.Errorf("VEC_SLICE: expected [20,30,40], got %v", vec)
	}
}

// ---------------------------------------------------------------------------
// VEC_CONCAT
// ---------------------------------------------------------------------------

func TestVecConcat(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_CONCAT(VEC_FROM_JSON('[1,2]'), VEC_FROM_JSON('[3,4,5]')) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	if len(vec) != 5 || vec[0] != 1 || vec[4] != 5 {
		t.Errorf("VEC_CONCAT: expected [1,2,3,4,5], got %v", vec)
	}
}

// ---------------------------------------------------------------------------
// VEC_QUANTIZE
// ---------------------------------------------------------------------------

func TestVecQuantize(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_QUANTIZE(VEC_FROM_JSON('[0.0, 0.5, 1.0]'), 8) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	// After 8-bit quantization, values should be close to original
	if math.Abs(vec[0]-0.0) > 0.01 || math.Abs(vec[2]-1.0) > 0.01 {
		t.Errorf("VEC_QUANTIZE: unexpected result %v", vec)
	}
}

// ---------------------------------------------------------------------------
// VEC_RANDOM
// ---------------------------------------------------------------------------

func TestVecRandom(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_RANDOM(128, 42) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	if len(vec) != 128 {
		t.Fatalf("VEC_RANDOM: expected 128 dims, got %d", len(vec))
	}
	// Should be unit vector
	var norm float64
	for _, v := range vec {
		norm += v * v
	}
	norm = math.Sqrt(norm)
	if math.Abs(norm-1.0) > 1e-9 {
		t.Errorf("VEC_RANDOM: expected unit vector (norm=1), got norm=%v", norm)
	}

	// Same seed should give same vector (deterministic)
	rs2 := execSQL(t, db, `SELECT VEC_RANDOM(128, 42) as v`)
	vec2 := rs2.Rows[0]["v"].([]float64)
	for i := range vec {
		if vec[i] != vec2[i] {
			t.Errorf("VEC_RANDOM: determinism failed at index %d", i)
			break
		}
	}
}

// ---------------------------------------------------------------------------
// VEC_AVG
// ---------------------------------------------------------------------------

func TestVecAvg(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_AVG(VEC_FROM_JSON('[2,4,6]'), VEC_FROM_JSON('[4,6,8]')) as v`)
	vec := rs.Rows[0]["v"].([]float64)
	if vec[0] != 3 || vec[1] != 5 || vec[2] != 7 {
		t.Errorf("VEC_AVG: expected [3,5,7], got %v", vec)
	}
}

// ---------------------------------------------------------------------------
// VECTOR column type in CREATE TABLE + INSERT + SELECT
// ---------------------------------------------------------------------------

func TestVectorColumnType(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create table with VECTOR column
	Execute(ctx, db, "default", mustParse("CREATE TABLE embeddings (id INT, label TEXT, vec VECTOR)"))

	// Insert with JSON string → auto-coerced to []float64
	Execute(ctx, db, "default", mustParse("INSERT INTO embeddings VALUES (1, 'hello', '[1.0, 0.0, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO embeddings VALUES (2, 'world', '[0.0, 1.0, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO embeddings VALUES (3, 'foo', '[0.0, 0.0, 1.0]')"))

	// Select and verify vectors are stored correctly
	rs := execSQL(t, db, "SELECT id, vec FROM embeddings ORDER BY id")
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rs.Rows))
	}

	vec1, ok := rs.Rows[0]["vec"].([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T (%v)", rs.Rows[0]["vec"], rs.Rows[0]["vec"])
	}
	if vec1[0] != 1.0 || vec1[1] != 0.0 || vec1[2] != 0.0 {
		t.Errorf("row 1 vec: expected [1,0,0], got %v", vec1)
	}
}

// ---------------------------------------------------------------------------
// Vector similarity in WHERE clause
// ---------------------------------------------------------------------------

func TestVectorInWhereClause(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse("CREATE TABLE docs (id INT, embedding VECTOR)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO docs VALUES (1, '[1.0, 0.0, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO docs VALUES (2, '[0.9, 0.1, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO docs VALUES (3, '[0.0, 1.0, 0.0]')"))

	// Find vectors with cosine similarity > 0.9 to [1,0,0]
	rs := execSQL(t, db, `
		SELECT id, VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0, 0.0]')) as sim
		FROM docs
		WHERE VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0, 0.0]')) > 0.9
		ORDER BY sim DESC
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 similar rows, got %d", len(rs.Rows))
	}
}

func TestVectorWhereAndSimpleCondition(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse("CREATE TABLE docs (id INT, embedding VECTOR)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO docs VALUES (1, '[1.0, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO docs VALUES (2, '[0.0, 1.0]')"))

	rs := execSQL(t, db, `
		SELECT id
		FROM docs
		WHERE id = 1
			AND VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0]')) > 0.5
	`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 matching row, got %d", len(rs.Rows))
	}

	rs = execSQL(t, db, `
		SELECT id
		FROM docs
		WHERE VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0]')) > 0.5
			AND id = 1
	`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 matching row for reversed order, got %d", len(rs.Rows))
	}
}

// ---------------------------------------------------------------------------
// VEC_SEARCH table-valued function (k-NN)
// ---------------------------------------------------------------------------

func TestVecSearchTVF(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Create a document store with embeddings
	Execute(ctx, db, "default", mustParse("CREATE TABLE documents (id INT, title TEXT, embedding VECTOR)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO documents VALUES (1, 'apple', '[1.0, 0.0, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO documents VALUES (2, 'banana', '[0.9, 0.1, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO documents VALUES (3, 'cherry', '[0.0, 1.0, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO documents VALUES (4, 'date', '[0.0, 0.0, 1.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO documents VALUES (5, 'elderberry', '[0.8, 0.2, 0.0]')"))

	// Search for vectors closest to [1,0,0] – top 3
	rs := execSQL(t, db, `
		SELECT * FROM VEC_SEARCH('documents', 'embedding', VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 3)
	`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 results, got %d", len(rs.Rows))
	}

	// First result should be "apple" (exact match, distance=0)
	title0, _ := rs.Rows[0]["title"].(string)
	if title0 != "apple" {
		t.Errorf("expected closest match 'apple', got %q", title0)
	}

	// Check that _vec_distance is present and rank is 1
	dist0, _ := rs.Rows[0]["_vec_distance"].(float64)
	if dist0 > 1e-9 {
		t.Errorf("expected distance ~0 for exact match, got %v", dist0)
	}
	expectInt(t, rs.Rows[0]["_vec_rank"], 1, "rank of closest")
}

// TestVecSearchEmitsSimilarityColumn guards against feeding _vec_distance
// directly into RAG_HYBRID_SCORE/RAG_RANK_SCORE, which expect a similarity
// (higher = closer) and would silently invert ranking on a distance input.
func TestVecSearchEmitsSimilarityColumn(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse("CREATE TABLE documents (id INT, embedding VECTOR)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO documents VALUES (1, '[1.0, 0.0, 0.0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO documents VALUES (2, '[0.0, 1.0, 0.0]')"))

	// cosine: exact match (distance 0) must report similarity 1.0, matching
	// VEC_COSINE_SIMILARITY's [-1, 1] range, not _vec_distance's [0, 2].
	rs := execSQL(t, db, `SELECT * FROM VEC_SEARCH('documents', 'embedding', VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 2, 'cosine')`)
	expectFloat(t, rs.Rows[0]["_vec_similarity"], 1.0, 1e-9, "cosine similarity for exact match")
	dist, _ := rs.Rows[0]["_vec_distance"].(float64)
	sim, _ := rs.Rows[0]["_vec_similarity"].(float64)
	expectFloat(t, sim, 1.0-dist, 1e-9, "cosine similarity = 1 - distance")

	// l2: similarity is the negated distance (higher = closer), so ordering by
	// similarity DESC must match ordering by distance ASC.
	rs = execSQL(t, db, `SELECT * FROM VEC_SEARCH('documents', 'embedding', VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 2, 'l2')`)
	dist0, _ := rs.Rows[0]["_vec_distance"].(float64)
	sim0, _ := rs.Rows[0]["_vec_similarity"].(float64)
	dist1, _ := rs.Rows[1]["_vec_distance"].(float64)
	sim1, _ := rs.Rows[1]["_vec_similarity"].(float64)
	expectFloat(t, sim0, -dist0, 1e-9, "l2 similarity = -distance (row 0)")
	expectFloat(t, sim1, -dist1, 1e-9, "l2 similarity = -distance (row 1)")
	if sim0 <= sim1 {
		t.Errorf("expected closer row (smaller distance) to have larger similarity: sim0=%v sim1=%v", sim0, sim1)
	}
}

func TestVecSearchWithL2Metric(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse("CREATE TABLE pts (id INT, pos VECTOR)"))
	Execute(ctx, db, "default", mustParse("INSERT INTO pts VALUES (1, '[0, 0]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO pts VALUES (2, '[3, 4]')"))
	Execute(ctx, db, "default", mustParse("INSERT INTO pts VALUES (3, '[1, 1]')"))

	// L2 distance from origin [0,0], top 2
	rs := execSQL(t, db, `SELECT * FROM VEC_SEARCH('pts', 'pos', VEC_FROM_JSON('[0,0]'), 2, 'l2')`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 results, got %d", len(rs.Rows))
	}
	// Closest should be [0,0] itself (distance 0)
	expectFloat(t, rs.Rows[0]["_vec_distance"], 0.0, 1e-9, "L2 closest")
	// Second should be [1,1] (distance sqrt(2) ≈ 1.414)
	expectFloat(t, rs.Rows[1]["_vec_distance"], math.Sqrt(2), 1e-9, "L2 second")
}

// TestVecSearchNaNRowExcluded is a regression test: a row whose vector
// contains a NaN component (e.g. inserted via a non-SQL path, bypassing
// VEC_FROM_JSON validation) used to compute a NaN distance that, once it
// landed at the top-k heap root, made every later "is this closer?"
// comparison against it false (NaN comparisons are always false per IEEE
// 754) — silently rejecting genuinely closer real rows and returning the
// poisoned NaN row in the results. The NaN row is inserted first and a
// mid-distance row second so both fill the k=2 heap before the true exact
// match is scanned, reproducing the original failure mode if the NaN guard
// regresses.
func TestVecSearchNaNRowExcluded(t *testing.T) {
	db := storage.NewDB()
	table := storage.NewTable("nan_docs", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	table.Rows = append(table.Rows,
		[]any{1, []float64{math.NaN(), 0, 0}}, // poisoned row: must never appear in results
		[]any{2, []float64{0, 1, 0}},          // orthogonal: cosine distance 1
		[]any{3, []float64{1, 0, 0}},          // exact match: cosine distance 0
	)
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	rs := execSQL(t, db, `SELECT id, _vec_distance FROM VEC_SEARCH('nan_docs', 'embedding', VEC_FROM_JSON('[1,0,0]'), 2)`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 results (NaN row excluded), got %d: %#v", len(rs.Rows), rs.Rows)
	}
	for _, r := range rs.Rows {
		if r["id"] == 1 {
			t.Fatalf("NaN-poisoned row leaked into results: %#v", rs.Rows)
		}
		if d, ok := r["_vec_distance"].(float64); ok && math.IsNaN(d) {
			t.Fatalf("result row has NaN distance: %#v", r)
		}
	}
	expectInt(t, rs.Rows[0]["id"], 3, "closest match should be the exact-match row")
	expectFloat(t, rs.Rows[0]["_vec_distance"], 0.0, 1e-9, "exact match distance")
	expectInt(t, rs.Rows[1]["id"], 2, "second closest should be the orthogonal row")
	expectFloat(t, rs.Rows[1]["_vec_distance"], 1.0, 1e-9, "orthogonal row distance")
}

// TestVecSearchTopKWorkerPanicRecovered is a regression test for crash
// safety: vecSearchTopK fans out to parallel workers once a table has
// vecSearchParallelMinRows+ rows, and none of those goroutines had a
// recover(), so a panic in a distance function (e.g. a future edge case)
// crashed the whole process even though the caller's own HTTP/gRPC handler
// recover() only protects its own goroutine, not ones it spawns. This
// exercises the parallel path directly with a distFn that panics on one row
// and asserts the panic surfaces as an ordinary error instead of escaping.
func TestVecSearchTopKWorkerPanicRecovered(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("requires GOMAXPROCS >= 2 to exercise the parallel worker path")
	}
	const numRows = vecSearchParallelMinRows
	const dims = 4

	cache := vecSearchColumnCacheEntry{
		vectors: make([][]float64, numRows),
		valid:   make([]bool, numRows),
	}
	for i := range cache.vectors {
		cache.vectors[i] = []float64{1, 0, 0, 0}
		cache.valid[i] = true
	}
	panicRow := numRows - 1 // falls in the last worker's chunk
	distFn := func(_ []float64, rowIdx int) (float64, bool) {
		if rowIdx == panicRow {
			panic("synthetic distance panic for test")
		}
		return float64(rowIdx), true
	}

	fakeRows := make([][]any, numRows)
	_, err := vecSearchTopK(context.Background(), fakeRows, dims, 5, cache, distFn)
	if err == nil {
		t.Fatal("expected the worker panic to surface as an error; a nil error here means it either crashed the process or was silently swallowed")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("expected error to reference the panic, got: %v", err)
	}
}

func TestVecSearchWithANNIndexModes(t *testing.T) {
	db := storage.NewDB()
	table := storage.NewTable("ann_docs", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "label", Type: storage.TextType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)

	for i := 0; i < 512; i++ {
		vec := []float64{
			math.Sin(float64(i) * 0.17),
			math.Cos(float64(i) * 0.11),
			math.Sin(float64(i)*0.07 + 1.0),
			math.Cos(float64(i)*0.13 + 2.0),
			float64(i%17) / 17.0,
		}
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("doc-%03d", i), vec})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	targetID := 137
	query := table.Rows[targetID][2].([]float64)
	for _, mode := range []string{"ivf", "hnsw"} {
		t.Run(mode, func(t *testing.T) {
			rs := execSQL(t, db, fmt.Sprintf(`
				SELECT id, _vec_distance, _vec_rank
				FROM VEC_SEARCH('ann_docs', 'embedding', VEC_FROM_JSON('%s'), 10, 'cosine', '%s')
			`, mustVecJSON(t, query), mode))
			if len(rs.Rows) == 0 {
				t.Fatalf("%s returned no rows", mode)
			}
			found := false
			for _, row := range rs.Rows {
				if row["id"] == targetID {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("%s did not return exact query row %d in top-10: %#v", mode, targetID, rs.Rows)
			}
		})
	}
}

// TestVecSearchHNSWNeighborPruningStable pins the exact ordered top-10 row
// IDs an HNSW search returns for a fixed dataset/query. pruneHNSWNeighbors
// was rewritten to precompute each neighbor's distance once into a
// vecScoredRow slice and insertion-sort it, instead of sort.Slice with a
// comparator that recomputed rowDistance on every comparison — a
// performance-only change. The dataset here (2000 rows x 16 dims) is large
// enough, relative to vecHNSWM, that pruneHNSWNeighbors runs on essentially
// every insertion, so an unchanged result set here confirms the rewritten
// sort preserves the exact nearest-M-by-distance, tie-broken-by-rowIdx
// ordering the original comparator produced.
func TestVecSearchHNSWNeighborPruningStable(t *testing.T) {
	db := storage.NewDB()
	table := storage.NewTable("hnsw_prune_docs", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)

	const numRows, dims = 2000, 16
	vecAt := func(i int) []float64 {
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			vec[d] = math.Sin(float64(i)*0.013+float64(d)*0.7) + math.Cos(float64(i)*0.021*float64(d+1))
		}
		return vec
	}
	for i := 0; i < numRows; i++ {
		table.Rows = append(table.Rows, []any{i, vecAt(i)})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	query := vecAt(777)
	rs := execSQL(t, db, fmt.Sprintf(`
		SELECT id
		FROM VEC_SEARCH('hnsw_prune_docs', 'embedding', VEC_FROM_JSON('%s'), 10, 'cosine', 'hnsw')
	`, mustVecJSON(t, query)))

	if len(rs.Rows) != 10 {
		t.Fatalf("expected 10 rows, got %d: %#v", len(rs.Rows), rs.Rows)
	}
	got := make([]int, len(rs.Rows))
	for i, row := range rs.Rows {
		id, ok := row["id"].(int)
		if !ok {
			t.Fatalf("row %d id has unexpected type %T", i, row["id"])
		}
		got[i] = id
	}

	// Captured from the pre-optimization pruneHNSWNeighbors implementation
	// (sort.Slice with a recomputing comparator) and re-verified unchanged
	// after the insertion-sort rewrite.
	want := []int{777, 778, 776, 779, 775, 780, 774, 1317, 1316, 1318}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("HNSW top-10 changed at position %d: got %v, want %v", i, got, want)
		}
	}
}

func TestVecSearchANNIndexInvalidatesOnTableVersion(t *testing.T) {
	db := storage.NewDB()
	table := storage.NewTable("ann_mutable", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	for i := 0; i < 384; i++ {
		table.Rows = append(table.Rows, []any{i, []float64{
			math.Sin(float64(i) * 0.09),
			math.Cos(float64(i) * 0.05),
			math.Sin(float64(i) * 0.03),
			math.Cos(float64(i) * 0.02),
		}})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}

	base := table.Rows[151][1].([]float64)
	query := []float64{base[0] + 0.001, base[1] - 0.001, base[2] + 0.001, base[3] - 0.001}
	for _, mode := range []string{"ivf", "hnsw"} {
		_ = execSQL(t, db, fmt.Sprintf(`
			SELECT id
			FROM VEC_SEARCH('ann_mutable', 'embedding', VEC_FROM_JSON('%s'), 5, 'l2', '%s')
		`, mustVecJSON(t, query), mode))
	}

	table.Rows = append(table.Rows, []any{999, query})
	table.Version++

	for _, mode := range []string{"ivf", "hnsw"} {
		t.Run(mode, func(t *testing.T) {
			rs := execSQL(t, db, fmt.Sprintf(`
				SELECT id
				FROM VEC_SEARCH('ann_mutable', 'embedding', VEC_FROM_JSON('%s'), 5, 'l2', '%s')
			`, mustVecJSON(t, query), mode))
			if len(rs.Rows) == 0 || rs.Rows[0]["id"] != 999 {
				t.Fatalf("%s did not rebuild after table version change: %#v", mode, rs.Rows)
			}
		})
	}
}

func TestVecSearchConcurrentColdIndexBuild(t *testing.T) {
	db := setupTestDB()
	execSQL(t, db, `CREATE TABLE ann_docs (id INT, embedding VECTOR)`)
	for i := 0; i < 128; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO ann_docs VALUES (%d, '[1.0, 0.0, 0.0]')`, i))
	}
	// Remove any state created by other tests for this table name before a
	// concurrent first HNSW request. The test exercises the cache singleflight
	// path and, under -race, guards its synchronization contract.
	purgeVectorCachesFor("default", "ann_docs")
	const readers = 16
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stmt, err := NewParser(`SELECT id FROM VEC_SEARCH('ann_docs', 'embedding', '[1.0, 0.0, 0.0]', 5, 'cosine', 'hnsw')`).ParseStatement()
			if err != nil {
				errs <- err
				return
			}
			rs, err := Execute(context.Background(), db, "default", stmt)
			if err != nil {
				errs <- err
				return
			}
			if len(rs.Rows) != 5 {
				errs <- fmt.Errorf("rows = %d, want 5", len(rs.Rows))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestVecSearchResultCacheAndAnalytics(t *testing.T) {
	ConfigureVectorCache(VectorCacheConfig{ResultCacheEntries: 8, ResultCacheTTL: time.Minute, Analytics: true, AnalyticsWindow: time.Minute, AnalyticsMaxEvents: 8})
	t.Cleanup(func() { ConfigureVectorCache(VectorCacheConfig{}) })
	db := setupTestDB()
	execSQL(t, db, `CREATE TABLE cached_vectors (id INT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO cached_vectors VALUES (1, '[1.0, 0.0]'), (2, '[0.0, 1.0]')`)
	q := `SELECT id FROM VEC_SEARCH('cached_vectors', 'embedding', '[1.0, 0.0]', 1, 'cosine', 'flat')`
	execSQL(t, db, q)
	execSQL(t, db, q)
	stats := VectorCacheAnalytics()
	if stats.Hits != 1 || stats.Misses != 1 || stats.Entries != 1 || len(stats.RecentQueries) != 2 || !stats.RecentQueries[1].CacheHit {
		t.Fatalf("cache analytics = %#v", stats)
	}
	execSQL(t, db, `INSERT INTO cached_vectors VALUES (3, '[1.0, 0.0]')`)
	execSQL(t, db, q)
	stats = VectorCacheAnalytics()
	if stats.Misses != 2 {
		t.Fatalf("table-version mutation must invalidate result key: %#v", stats)
	}
}

// TestVecSearchConcurrentWithCacheReconfiguration races many concurrent
// VEC_SEARCH callers against concurrent ConfigureVectorCache calls that
// flip the result cache and analytics on and off. It exists to guard the
// lock-free atomic.Bool fast path in vecQueryCacheEnabled/recordVecQuery
// (vector_query_cache.go): those functions read cacheEnabled/analyticsEnabled
// without the mutex, so a wrong implementation could plausibly corrupt
// vecQueryCacheState.entries/events under exactly this interleaving even
// though neither TestVecSearchResultCacheAndAnalytics above (sequential) nor
// TestVecQueryCacheExpiryWithSynctest (no concurrent Configure calls)
// exercises that interleaving. Without -race available in every environment
// this repo is built in, this test still catches the outward symptoms a
// broken fast path would produce: panics, wrong VEC_SEARCH results, or an
// impossible (negative, or over the configured max) analytics snapshot.
func TestVecSearchConcurrentWithCacheReconfiguration(t *testing.T) {
	ConfigureVectorCache(VectorCacheConfig{})
	t.Cleanup(func() { ConfigureVectorCache(VectorCacheConfig{}) })

	db := setupTestDB()
	execSQL(t, db, `CREATE TABLE concurrent_vectors (id INT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO concurrent_vectors VALUES (1, '[1.0, 0.0]'), (2, '[0.0, 1.0]')`)
	q := `SELECT id FROM VEC_SEARCH('concurrent_vectors', 'embedding', '[1.0, 0.0]', 1, 'cosine', 'flat')`

	const (
		searchers  = 16
		configurers = 4
		duration   = 200 * time.Millisecond
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(searchers)
	for i := 0; i < searchers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rs := execSQL(t, db, q)
				if len(rs.Rows) != 1 {
					t.Errorf("expected 1 row, got %d", len(rs.Rows))
					return
				}
			}
		}()
	}

	wg.Add(configurers)
	for i := 0; i < configurers; i++ {
		i := i
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if i%2 == 0 {
					ConfigureVectorCache(VectorCacheConfig{ResultCacheEntries: 8, ResultCacheTTL: time.Minute, Analytics: true, AnalyticsWindow: time.Minute, AnalyticsMaxEvents: 8})
				} else {
					ConfigureVectorCache(VectorCacheConfig{})
				}
				_ = VectorCacheAnalytics()
			}
		}()
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	stats := VectorCacheAnalytics()
	if stats.Hits > uint64(searchers)*1_000_000 || stats.Misses > uint64(searchers)*1_000_000 {
		t.Fatalf("implausible analytics snapshot after concurrent run: %#v", stats)
	}
	if len(stats.RecentQueries) > 8 {
		t.Fatalf("analytics events exceeded configured max: got %d, want <= 8", len(stats.RecentQueries))
	}
}

func mustVecJSON(t *testing.T, vec []float64) string {
	t.Helper()
	b, err := json.Marshal(vec)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestVecDimensionMismatch(t *testing.T) {
	db := storage.NewDB()
	_, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT VEC_DOT(VEC_FROM_JSON('[1,2]'), VEC_FROM_JSON('[1,2,3]')) as d`,
	))
	if err == nil {
		t.Fatal("expected dimension mismatch error")
	}
}

func TestVecFromJSONInvalidInput(t *testing.T) {
	db := storage.NewDB()
	_, err := Execute(context.Background(), db, "default", mustParse(
		`SELECT VEC_FROM_JSON('not a json array') as v`,
	))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// ---------------------------------------------------------------------------
// RAG-style end-to-end scenario
// ---------------------------------------------------------------------------

func TestRAGWorkflow(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// 1. Create a knowledge base table
	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE knowledge_base (
			id INT,
			content TEXT,
			embedding VECTOR
		)
	`))

	// 2. Insert documents with embeddings (simulated)
	docs := []struct {
		id        int
		content   string
		embedding string
	}{
		{1, "Go is a statically typed language", "[0.9, 0.1, 0.0, 0.0, 0.0]"},
		{2, "Python is dynamically typed", "[0.8, 0.2, 0.1, 0.0, 0.0]"},
		{3, "Databases store structured data", "[0.0, 0.0, 0.9, 0.1, 0.0]"},
		{4, "Vector search enables AI applications", "[0.1, 0.0, 0.1, 0.9, 0.1]"},
		{5, "Machine learning uses neural networks", "[0.1, 0.0, 0.0, 0.8, 0.2]"},
		{6, "SQL is a query language for databases", "[0.1, 0.0, 0.8, 0.2, 0.0]"},
	}
	for _, d := range docs {
		sql := fmt.Sprintf("INSERT INTO knowledge_base VALUES (%d, '%s', '%s')",
			d.id, d.content, d.embedding)
		Execute(ctx, db, "default", mustParse(sql))
	}

	// 3. Query: "Find documents about programming languages"
	//    Simulated query embedding close to Go/Python docs
	queryVec := "[0.85, 0.15, 0.05, 0.0, 0.0]"
	rs := execSQL(t, db, fmt.Sprintf(`
		SELECT * FROM VEC_SEARCH('knowledge_base', 'embedding', VEC_FROM_JSON('%s'), 3, 'cosine')
	`, queryVec))

	if len(rs.Rows) != 3 {
		t.Fatalf("RAG: expected 3 results, got %d", len(rs.Rows))
	}

	// Top results should be about programming languages (ids 1, 2)
	topContent, _ := rs.Rows[0]["content"].(string)
	if topContent != "Go is a statically typed language" && topContent != "Python is dynamically typed" {
		t.Errorf("RAG: expected programming language doc as top result, got %q", topContent)
	}

	// 4. Compute similarity scores inline
	rs2 := execSQL(t, db, fmt.Sprintf(`
		SELECT content,
		       VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('%s')) as similarity
		FROM knowledge_base
		ORDER BY similarity DESC
	`, queryVec))

	if len(rs2.Rows) != 6 {
		t.Fatalf("RAG similarity: expected 6 rows, got %d", len(rs2.Rows))
	}

	// Top result should have high similarity
	topSim, ok := rs2.Rows[0]["similarity"].(float64)
	if !ok || topSim < 0.9 {
		t.Errorf("RAG: expected top similarity > 0.9, got %v", topSim)
	}

	// 5. Use VEC_DISTANCE for ORDER BY with threshold
	rs3 := execSQL(t, db, fmt.Sprintf(`
		SELECT content,
		       VEC_DISTANCE(embedding, VEC_FROM_JSON('%s'), 'cosine') as dist
		FROM knowledge_base
		WHERE VEC_DISTANCE(embedding, VEC_FROM_JSON('%s'), 'cosine') < 0.5
		ORDER BY dist
	`, queryVec, queryVec))

	if len(rs3.Rows) < 1 {
		t.Fatal("RAG threshold: expected at least 1 result")
	}

	// Distances should be < 0.5
	for _, row := range rs3.Rows {
		d, _ := row["dist"].(float64)
		if d >= 0.5 {
			t.Errorf("RAG threshold: got distance %v >= 0.5", d)
		}
	}
}

// ---------------------------------------------------------------------------
// Bulk coverage: call all vector functions to ensure they don't panic
// ---------------------------------------------------------------------------

func TestCallAllVectorFunctions(t *testing.T) {
	env := ExecEnv{}
	row := Row{}

	vecFuncs := getVectorFunctions()
	for name := range vecFuncs {
		ex := &FuncCall{
			Name: name,
			Args: []Expr{&Literal{Val: "[1.0, 2.0, 3.0]"}},
		}
		// We expect some errors (wrong arg count, etc.) but no panics
		_, _ = evalFuncCall(env, ex, row)
	}
}
