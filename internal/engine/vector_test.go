// Vector functions and RAG vector search tests for tinySQL.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"

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
