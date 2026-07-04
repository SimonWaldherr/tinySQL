package engine

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var vectorMathBenchmarkSink float64

func makeVectorMathBenchmarkInputs(dims int) ([]float64, []float64) {
	a := make([]float64, dims)
	b := make([]float64, dims)
	for i := range a {
		a[i] = math.Sin(float64(i)*0.11) * 0.75
		b[i] = math.Cos(float64(i)*0.07) * 0.5
	}
	return a, b
}

func BenchmarkVectorDot768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = vectorDot(a, vecB)
	}
}

func BenchmarkVectorDotUnrolled768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = vectorDotUnrolled(a, vecB)
	}
}

func BenchmarkVectorL2Squared768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = vectorL2Squared(a, vecB)
	}
}

func BenchmarkVectorL2SquaredUnrolled768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = vectorL2SquaredUnrolled(a, vecB)
	}
}

func BenchmarkVectorL1Distance768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = vectorL1Distance(a, vecB)
	}
}

func BenchmarkVectorL1DistanceUnrolled768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = vectorL1Unrolled(a, vecB)
	}
}

func makeVecSearchBenchmarkTable(rows, dims int) *storage.DB {
	db := storage.NewDB()
	table := storage.NewTable("vec_docs", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)

	for i := 0; i < rows; i++ {
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			angle := float64(i*d) / float64(rows*dims+1)
			vec[d] = math.Sin(angle) * 0.5
		}
		table.Rows = append(table.Rows, []any{i, vec})
	}

	if err := db.Put("default", table); err != nil {
		panic(err)
	}

	return db
}

func benchmarkVecSearch(b *testing.B, metric string) {
	db := makeVecSearchBenchmarkTable(4096, 64)
	fn := &VecSearchTableFunc{}
	env := ExecEnv{ctx: context.Background(), tenant: "default", db: db}
	query := make([]float64, 64)
	for i := range query {
		query[i] = float64(i) / 64.0
	}

	args := []Expr{
		&Literal{Val: "vec_docs"},
		&Literal{Val: "embedding"},
		&Literal{Val: query},
		&Literal{Val: 5},
		&Literal{Val: metric},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := fn.Execute(context.Background(), args, env, Row{})
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 5 {
			b.Fatalf("expected top-5 results, got %d", len(rs.Rows))
		}
	}
}

func benchmarkVecSearchIndexed(b *testing.B, indexMode string) {
	db := makeRAGHybridBenchmarkTable(12000, 64)
	fn := &VecSearchTableFunc{}
	env := ExecEnv{ctx: context.Background(), tenant: "default", db: db}
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	args := []Expr{
		&Literal{Val: "rag_hybrid"},
		&Literal{Val: "embedding"},
		&Literal{Val: query},
		&Literal{Val: 20},
		&Literal{Val: "cosine"},
		&Literal{Val: indexMode},
	}

	if _, err := fn.Execute(context.Background(), args, env, Row{}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := fn.Execute(context.Background(), args, env, Row{})
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) == 0 || len(rs.Rows) > 20 {
			b.Fatalf("expected up to 20 results, got %d", len(rs.Rows))
		}
	}
}

func makeWhereAndBenchmarkTable(rows, dims int) *storage.DB {
	db := storage.NewDB()
	table := storage.NewTable("rag_docs", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "score", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)

	for i := 0; i < rows; i++ {
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			angle := 0.08*float64(i) + 0.17*float64(d)
			vec[d] = math.Sin(angle)
		}
		table.Rows = append(table.Rows, []any{i, i, vec})
	}

	if err := db.Put("default", table); err != nil {
		panic(err)
	}

	return db
}

func makeRAGHybridBenchmarkTable(rows, dims int) *storage.DB {
	db := storage.NewDB()
	table := storage.NewTable("rag_hybrid", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "score", Type: storage.IntType},
		{Name: "quality", Type: storage.FloatType},
		{Name: "created_at", Type: storage.TextType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)

	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < rows; i++ {
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			angle := 0.08*float64(i) + 0.17*float64(d)
			vec[d] = math.Sin(angle)
		}
		ts := base.Add(-time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05")
		quality := 0.5 + 0.5*math.Sin(float64(i)*0.03)
		table.Rows = append(table.Rows, []any{i, i, quality, ts, vec})
	}

	if err := db.Put("default", table); err != nil {
		panic(err)
	}
	return db
}

func makeRAGChunkBenchmarkTable(rows, dims int) *storage.DB {
	db := storage.NewDB()
	table := storage.NewTable("rag_chunks", []storage.Column{
		{Name: "doc_id", Type: storage.TextType},
		{Name: "chunk_index", Type: storage.IntType},
		{Name: "chunk_text", Type: storage.TextType},
		{Name: "quality", Type: storage.FloatType},
		{Name: "created_at", Type: storage.TextType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)

	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < rows; i++ {
		vec := make([]float64, dims)
		for d := 0; d < dims; d++ {
			angle := 0.08*float64(i) + 0.17*float64(d)
			vec[d] = math.Sin(angle)
		}
		docID := "doc-" + strconv.Itoa(i/12)
		chunkIndex := i % 12
		chunkText := "chunk " + strconv.Itoa(i)
		quality := 0.5 + 0.5*math.Sin(float64(i)*0.03)
		ts := base.Add(-time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05")
		table.Rows = append(table.Rows, []any{docID, chunkIndex, chunkText, quality, ts, vec})
	}

	if err := db.Put("default", table); err != nil {
		panic(err)
	}
	return db
}

func benchmarkWhereAndVector(b *testing.B, vectorFirst bool) {
	db := makeWhereAndBenchmarkTable(12000, 32)
	query := make([]float64, 32)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		b.Fatal(err)
	}

	var stmt Statement
	if vectorFirst {
		stmt = mustParse(`
			SELECT id, score
			FROM rag_docs
			WHERE VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('` + string(queryJSON) + `')) > -1.0
				AND score > 9000
		`)
	} else {
		stmt = mustParse(`
			SELECT id, score
			FROM rag_docs
			WHERE score > 9000
				AND VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('` + string(queryJSON) + `')) > -1.0
		`)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 2999 {
			b.Fatalf("expected 2999 rows, got %d", len(rs.Rows))
		}
	}
}

func BenchmarkVecSearchCosineTopK(b *testing.B) {
	benchmarkVecSearch(b, "cosine")
}

func BenchmarkVecSearchL2TopK(b *testing.B) {
	benchmarkVecSearch(b, "l2")
}

func BenchmarkVecSearchCosineTopK_IVFCached(b *testing.B) {
	benchmarkVecSearchIndexed(b, "ivf")
}

func BenchmarkVecSearchCosineTopK_HNSWCached(b *testing.B) {
	benchmarkVecSearchIndexed(b, "hnsw")
}

func BenchmarkVecSearchIndexModesSameTable(b *testing.B) {
	db := makeRAGHybridBenchmarkTable(12000, 64)
	fn := &VecSearchTableFunc{}
	env := ExecEnv{ctx: context.Background(), tenant: "default", db: db}
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}

	for _, indexMode := range []string{"flat", "ivf", "hnsw"} {
		indexMode := indexMode
		b.Run(indexMode, func(b *testing.B) {
			args := []Expr{
				&Literal{Val: "rag_hybrid"},
				&Literal{Val: "embedding"},
				&Literal{Val: query},
				&Literal{Val: 20},
				&Literal{Val: "cosine"},
				&Literal{Val: indexMode},
			}
			if _, err := fn.Execute(context.Background(), args, env, Row{}); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rs, err := fn.Execute(context.Background(), args, env, Row{})
				if err != nil {
					b.Fatal(err)
				}
				if len(rs.Rows) == 0 || len(rs.Rows) > 20 {
					b.Fatalf("expected up to 20 results, got %d", len(rs.Rows))
				}
			}
		})
	}
}

func BenchmarkWhereVectorAndSimpleCondition_VectorThenScalar(b *testing.B) {
	benchmarkWhereAndVector(b, true)
}

func BenchmarkWhereVectorAndSimpleCondition_SimpleThenVector(b *testing.B) {
	benchmarkWhereAndVector(b, false)
}

func BenchmarkHybridOrderByVectorAndRecency(b *testing.B) {
	db := makeRAGHybridBenchmarkTable(12000, 64)
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`
		SELECT id,
		       RAG_HYBRID_SCORE(
		           VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('` + string(queryJSON) + `')),
		           created_at,
		           30,
		           0.7,
		           '2026-01-01 01:00:00'
		       ) AS rag_score
		FROM rag_hybrid
		WHERE VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('` + string(queryJSON) + `')) > -1.0
		  AND RECENCY_SCORE(created_at, 30, '2026-01-01 01:00:00') > 0.0
		ORDER BY rag_score DESC
		LIMIT 20
	`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 20 {
			b.Fatalf("expected 20 results, got %d", len(rs.Rows))
		}
	}
}

func BenchmarkRAGRankScoreOrderByLimit(b *testing.B) {
	db := makeRAGHybridBenchmarkTable(12000, 64)
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`
		SELECT id,
		       RAG_RANK_SCORE(
		           VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('` + string(queryJSON) + `')),
		           created_at,
		           30,
		           quality,
		           0.65,
		           0.25,
		           0.10,
		           '2026-01-01 01:00:00'
		       ) AS rag_score
		FROM rag_hybrid
		ORDER BY rag_score DESC
		LIMIT 20
	`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 20 {
			b.Fatalf("expected 20 results, got %d", len(rs.Rows))
		}
	}
}

func BenchmarkRAGContextFromTopK(b *testing.B) {
	db := makeRAGChunkBenchmarkTable(12000, 64)
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`
		WITH topk AS (
			SELECT doc_id, chunk_index, _vec_rank
			FROM VEC_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('` + string(queryJSON) + `'), 20, 'cosine')
		)
		SELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset
		FROM RAG_CONTEXT_FROM('rag_chunks', 'doc_id', 'chunk_index', 'topk', 'doc_id', 'chunk_index', 1, 1)
		ORDER BY _context_rank
	`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) == 0 {
			b.Fatal("expected context rows")
		}
	}
}

func BenchmarkOrderByVectorLimit_NoWhere(b *testing.B) {
	db := makeRAGHybridBenchmarkTable(12000, 64)
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`
		SELECT VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('` + string(queryJSON) + `')) AS sim
		FROM rag_hybrid
		ORDER BY sim DESC
		LIMIT 20
	`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 20 {
			b.Fatalf("expected 20 results, got %d", len(rs.Rows))
		}
	}
}

func BenchmarkOrderByVectorLimit_WithWhereAndScalar(b *testing.B) {
	db := makeRAGHybridBenchmarkTable(12000, 64)
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`
		SELECT VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('` + string(queryJSON) + `')) AS sim
		FROM rag_hybrid
		WHERE score > 9000
		ORDER BY sim DESC
		LIMIT 20
	`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs.Rows) != 20 {
			b.Fatalf("expected 20 results, got %d", len(rs.Rows))
		}
	}
}

func BenchmarkCompareTopK_VecSearchVsOrderBy(b *testing.B) {
	db := makeRAGHybridBenchmarkTable(12000, 64)
	query := make([]float64, 64)
	for i := range query {
		query[i] = math.Cos(0.08*float64(i) + 0.5)
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		b.Fatal(err)
	}
	queryJSONStr := string(queryJSON)

	stmtOrder := mustParse(`
		SELECT VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('` + queryJSONStr + `')) AS sim
		FROM rag_hybrid
		ORDER BY sim DESC
		LIMIT 20
	`)
	stmtVec := mustParse(`
		SELECT * FROM VEC_SEARCH('rag_hybrid', 'embedding', VEC_FROM_JSON('` + queryJSONStr + `'), 20, 'cosine')
	`)

	b.Run("order_by_vector", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rs, err := Execute(context.Background(), db, "default", stmtOrder)
			if err != nil {
				b.Fatal(err)
			}
			if len(rs.Rows) != 20 {
				b.Fatalf("expected 20 results, got %d", len(rs.Rows))
			}
		}
	})

	b.Run("vec_search", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rs, err := Execute(context.Background(), db, "default", stmtVec)
			if err != nil {
				b.Fatal(err)
			}
			if len(rs.Rows) != 20 {
				b.Fatalf("expected 20 results, got %d", len(rs.Rows))
			}
		}
	})
}
