package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkSelectConversions(b *testing.B) {
	db := storage.NewDB()
	defer db.Close()
	table := storage.NewTable("conversion_rows", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "input", Type: storage.TextType}, {Name: "dirty", Type: storage.TextType}}, false)
	for i := range 20000 {
		var value any = strconv.Itoa(i)
		if i%17 == 0 {
			value = nil
		}
		dirty := value
		if i%13 == 0 {
			dirty = "invalid"
		}
		table.Rows = append(table.Rows, []any{i, value, dirty})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, query := range []struct{ name, sql string }{
		{"cast-projection", "SELECT CAST(input AS INT) AS value FROM conversion_rows"},
		{"convert-projection", "SELECT CONVERT(VARCHAR(4),input) AS value FROM conversion_rows"},
		{"cast-between", "SELECT COUNT(*) FROM conversion_rows WHERE CAST(input AS INT) BETWEEN 9500 AND 10500"},
		{"try-convert-between", "SELECT COUNT(*) FROM conversion_rows WHERE TRY_CONVERT(INT,dirty) BETWEEN 9500 AND 10500"},
		{"convert-compare", "SELECT COUNT(*) FROM conversion_rows WHERE CONVERT(INT,input)>=19500"},
		{"column-between-control", "SELECT COUNT(*) FROM conversion_rows WHERE id BETWEEN 9500 AND 10500"},
	} {
		b.Run(query.name, func(b *testing.B) { runBench(b, db, query.sql) })
	}
}

func BenchmarkSelectConstantVector(b *testing.B) {
	db := makeWhereAndBenchmarkTable(5000, 32)
	defer db.Close()
	query := make([]float64, 32)
	for i := range query {
		query[i] = float64(i+1) / 32
	}
	encoded, err := json.Marshal(query)
	if err != nil {
		b.Fatal(err)
	}
	for _, call := range []struct{ name, sql string }{
		{"cosine-json-literal", "VEC_COSINE_DISTANCE(embedding,'%s')"},
		{"l2-json-constructor", "VEC_L2_DISTANCE(embedding,VEC_FROM_JSON('%s'))"},
		{"l2-alias", "VEC_EUCLIDEAN_DISTANCE(embedding,'%s')"},
	} {
		b.Run(call.name, func(b *testing.B) {
			sql := "SELECT id," + fmt.Sprintf(call.sql, encoded) + " AS distance FROM rag_docs WHERE score BETWEEN 1500 AND 2500 ORDER BY distance LIMIT 20"
			runBench(b, db, sql)
		})
	}
}
