package benchmarks

// ═══════════════════════════════════════════════════════════════════════════
// Per-function speed: every scalar/aggregate/window function tinySQL supports
// that has (or has a well-known equivalent idiom in) modernc.org/sqlite,
// benchmarked the same way — SELECT <expr> FROM bench, scanned through
// database/sql on both sides.
//
// This is deliberately a different axis from sqlite_parity_benchmark_test.go,
// which measures whole workloads (insert/update/scan/join). Here the goal is
// isolating one function's per-row evaluation cost against the same data, so
// a regression or a win in one function doesn't hide inside a workload
// average.
//
// Scope: this file covers the "portable" SQL surface — string, numeric,
// date/time, JSON, type/conditional, encoding, window, and aggregate
// functions — because that's the surface modernc.org/sqlite can also
// execute, which is what a comparison requires. tinySQL's GEO_*, VEC_*,
// FTS_* boolean query engine, RAG_*, routing, tile, and CRS functions have no
// SQLite equivalent (SQLite has no native GIS/vector/tile support), so they
// are out of scope here the same way BenchmarkParityVectorTopK (in
// sqlite_parity_benchmark_test.go) treats vector search: any comparison would
// be measuring "an application doing the work in Go" rather than the
// function itself.
//
// Every case's tinySQL expression is always benchmarked. Its SQLite
// expression is benchmarked too whenever one exists — some entries
// deliberately have no SQLite equivalent (sqliteExpr == "") and are recorded
// as tinySQL-only, matching the request to measure every function even where
// no comparable SQLite function exists.
//
// A case whose trial query errors (e.g. a function this build of
// modernc.org/sqlite doesn't register, such as REGEXP) is skipped rather than
// failing the whole run, so one missing function doesn't hide every other
// result.
//
// Run with:
//
//	go test ./benchmarks/ -run='^$' -bench='FuncSpeed' -benchtime=200ms -timeout=30m
//
// To compare against SQLite specifically:
//
//	go test ./benchmarks/ -run='^$' -bench='FuncSpeed' -benchtime=200ms -timeout=30m | grep -E 'FuncSpeed|PASS|ok'
// ═══════════════════════════════════════════════════════════════════════════

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

type fnCase struct {
	category string
	name     string // display name, unique within category
	tinyExpr string // expression using tinySQL syntax/semantics
	// sqliteExpr is the semantically-equivalent modernc.org/sqlite expression,
	// or "" when SQLite has no comparable function/idiom.
	sqliteExpr string
}

// funcSpeedRows is the row count of the shared "bench" table both engines
// scan for every case. Large enough that per-row function cost dominates
// fixed per-query overhead, small enough that the full suite (~100 cases ×
// up to 2 engines) finishes in a couple of minutes at a modest -benchtime.
const funcSpeedRows = 2000

// funcCases is intentionally a flat data table rather than ~200 hand-written
// benchmark functions: every entry gets identical treatment (trial run, skip
// on error, timed scan), so adding a function is a one-line addition instead
// of a new function to keep in sync with the runner.
func funcCases() []fnCase {
	return []fnCase{
		// baseline has no function call at all — SELECT id FROM bench. It
		// measures fixed per-query/per-row overhead (parsing, planning,
		// driver round-trip, row scanning) common to every other case, so a
		// case's *own* cost is closer to (its ns/op − baseline's ns/op) than
		// to its raw ns/op, and its ratio to SQLite should be read relative
		// to the baseline ratio, not to 1.0.
		{"_baseline", "NOOP", "id", "id"},

		// ── Strings ──────────────────────────────────────────────────────
		{"string", "UPPER", "UPPER(name)", "upper(name)"},
		{"string", "LOWER", "LOWER(name)", "lower(name)"},
		{"string", "INITCAP", "INITCAP(sentence)", ""},
		{"string", "CONCAT", "CONCAT(name, ' ', sentence)", "name || ' ' || sentence"},
		{"string", "CONCAT_WS", "CONCAT_WS(', ', name, sentence)", ""},
		{"string", "LENGTH", "LENGTH(sentence)", "length(sentence)"},
		{"string", "CHAR_LENGTH", "CHAR_LENGTH(sentence)", "length(sentence)"},
		{"string", "SUBSTRING", "SUBSTRING(sentence, 5, 10)", "substr(sentence, 5, 10)"},
		{"string", "SUBSTR", "SUBSTR(sentence, 5, 10)", "substr(sentence, 5, 10)"},
		{"string", "LEFT", "LEFT(sentence, 10)", "substr(sentence, 1, 10)"},
		{"string", "RIGHT", "RIGHT(sentence, 10)", "substr(sentence, -10)"},
		{"string", "REVERSE", "REVERSE(name)", ""},
		{"string", "REPEAT", "REPEAT(name, 3)", ""},
		{"string", "TRIM", "TRIM(padded)", "trim(padded)"},
		{"string", "LTRIM", "LTRIM(padded)", "ltrim(padded)"},
		{"string", "RTRIM", "RTRIM(padded)", "rtrim(padded)"},
		{"string", "LPAD", "LPAD(padsrc, 10, '0')", ""},
		{"string", "RPAD", "RPAD(padsrc, 10, '0')", ""},
		{"string", "REPLACE", "REPLACE(sentence, 'the', 'THE')", "replace(sentence, 'the', 'THE')"},
		{"string", "INSTR", "INSTR(sentence, 'fox')", "instr(sentence, 'fox')"},
		{"string", "LOCATE", "LOCATE('fox', sentence)", "instr(sentence, 'fox')"},
		{"string", "POSITION", "POSITION('fox' IN sentence)", "instr(sentence, 'fox')"},
		{"string", "ASCII", "ASCII(name)", "unicode(name)"},
		{"string", "CHAR", "CHAR(65)", "char(65)"},
		{"string", "SPACE", "SPACE(5)", ""},
		{"string", "SOUNDEX", "SOUNDEX(name)", "soundex(name)"},
		{"string", "QUOTE", "QUOTE(name)", "quote(name)"},
		{"string", "SPLIT_PART", "SPLIT_PART(csvline, ',', 3)", ""},
		{"string", "STARTS_WITH", "STARTS_WITH(name, 'User')", "name LIKE 'User%'"},
		{"string", "ENDS_WITH", "ENDS_WITH(name, '9')", "name LIKE '%9'"},
		{"string", "CONTAINS", "CONTAINS(sentence, 'fox')", "sentence LIKE '%fox%'"},
		{"string", "LEVENSHTEIN", "LEVENSHTEIN(name, 'User_42')", ""},
		{"string", "EDIT_DISTANCE", "EDIT_DISTANCE(name, 'User_42')", ""},
		{"string", "REGEXP_MATCH", `REGEXP_MATCH(sentence, '\d+')`, ""},
		{"string", "REGEXP_REPLACE", `REGEXP_REPLACE(sentence, '\d+', 'N')`, ""},
		{"string", "REGEXP_EXTRACT", `REGEXP_EXTRACT(sentence, '\d+')`, ""},
		{"string", "PRINTF", "PRINTF('%d-%s', n, name)", "printf('%d-%s', n, name)"},
		{"string", "FORMAT", "FORMAT('%.2f', num)", "format('%.2f', num)"},
		{"string", "HTML_ESCAPE", "HTML_ESCAPE(sentence)", ""},
		{"string", "URL_ENCODE", "URL_ENCODE(sentence)", ""},
		{"string", "TEXT_WORD_COUNT", "TEXT_WORD_COUNT(sentence)", ""},

		// ── Numeric / math ───────────────────────────────────────────────
		{"numeric", "ABS", "ABS(num)", "abs(num)"},
		{"numeric", "ROUND", "ROUND(num, 2)", "round(num, 2)"},
		{"numeric", "CEIL", "CEIL(num)", "ceil(num)"},
		{"numeric", "CEILING", "CEILING(num)", "ceiling(num)"},
		{"numeric", "FLOOR", "FLOOR(num)", "floor(num)"},
		{"numeric", "SIGN", "SIGN(num)", "sign(num)"},
		{"numeric", "TRUNC", "TRUNC(num)", "trunc(num)"},
		{"numeric", "TRUNCATE", "TRUNCATE(num, 1)", ""},
		{"numeric", "MOD", "MOD(n, 7)", "mod(n, 7)"},
		{"numeric", "POWER", "POWER(n, 2)", "power(n, 2)"},
		{"numeric", "POW", "POW(n, 2)", "pow(n, 2)"},
		{"numeric", "SQRT", "SQRT(ABS(num))", "sqrt(abs(num))"},
		{"numeric", "EXP", "EXP(n / 50.0)", "exp(n / 50.0)"},
		{"numeric", "LN", "LN(ABS(num) + 1)", "ln(abs(num) + 1)"},
		{"numeric", "LOG", "LOG(ABS(num) + 1)", "ln(abs(num) + 1)"},
		{"numeric", "LOG10", "LOG10(ABS(num) + 1)", "log10(abs(num) + 1)"},
		{"numeric", "LOG2", "LOG2(ABS(num) + 1)", "log2(abs(num) + 1)"},
		{"numeric", "SIN", "SIN(num)", "sin(num)"},
		{"numeric", "COS", "COS(num)", "cos(num)"},
		{"numeric", "TAN", "TAN(n / 10.0)", "tan(n / 10.0)"},
		{"numeric", "ATAN", "ATAN(num)", "atan(num)"},
		{"numeric", "ATAN2", "ATAN2(num, n + 1)", "atan2(num, n + 1)"},
		{"numeric", "ASIN", "ASIN((n - 50) / 50.0)", "asin((n - 50) / 50.0)"},
		{"numeric", "ACOS", "ACOS((n - 50) / 50.0)", "acos((n - 50) / 50.0)"},
		{"numeric", "DEGREES", "DEGREES(num)", "degrees(num)"},
		{"numeric", "RADIANS", "RADIANS(num)", "radians(num)"},
		{"numeric", "PI", "PI()", "pi()"},
		{"numeric", "GREATEST", "GREATEST(n, id % 50, 10)", "max(n, id % 50, 10)"},
		{"numeric", "LEAST", "LEAST(n, id % 50, 10)", "min(n, id % 50, 10)"},
		{"numeric", "RANDOM", "RANDOM()", "random()"},

		// ── Date / time ──────────────────────────────────────────────────
		{"datetime", "YEAR", "YEAR(dt)", "strftime('%Y', dt)"},
		{"datetime", "MONTH", "MONTH(dt)", "strftime('%m', dt)"},
		{"datetime", "DAY", "DAY(dt)", "strftime('%d', dt)"},
		{"datetime", "HOUR", "HOUR(dt)", "strftime('%H', dt)"},
		{"datetime", "MINUTE", "MINUTE(dt)", "strftime('%M', dt)"},
		{"datetime", "SECOND", "SECOND(dt)", "strftime('%S', dt)"},
		{"datetime", "DAYOFWEEK", "DAYOFWEEK(dt)", "strftime('%w', dt)"},
		{"datetime", "DAYOFYEAR", "DAYOFYEAR(dt)", "strftime('%j', dt)"},
		{"datetime", "STRFTIME", "STRFTIME('%Y-%m-%d', dt)", "strftime('%Y-%m-%d', dt)"},
		{"datetime", "DATE", "DATE(dt)", "date(dt)"},
		{"datetime", "TIME", "TIME(dt)", "time(dt)"},
		{"datetime", "DATETIME", "DATETIME(dt)", "datetime(dt)"},
		{"datetime", "JULIANDAY", "JULIANDAY(dt)", "julianday(dt)"},
		{"datetime", "UNIXEPOCH", "UNIXEPOCH(dt)", "unixepoch(dt)"},
		{"datetime", "DATE_ADD", "DATE_ADD(dt, 7, 'DAY')", "datetime(dt, '+7 days')"},
		{"datetime", "DATE_SUB", "DATE_SUB(dt, 7, 'DAY')", "datetime(dt, '-7 days')"},
		{"datetime", "DATEDIFF", "DATEDIFF('DAYS', dt, '2030-01-01')", "julianday('2030-01-01') - julianday(dt)"},
		{"datetime", "EXTRACT", "EXTRACT('YEAR', dt)", "strftime('%Y', dt)"},
		{"datetime", "DATE_TRUNC", "DATE_TRUNC('MONTH', dt)", "date(dt, 'start of month')"},
		{"datetime", "ADD_MONTHS", "ADD_MONTHS(dt, 3)", "datetime(dt, '+3 months')"},

		// ── JSON ─────────────────────────────────────────────────────────
		{"json", "JSON_EXTRACT", "JSON_EXTRACT(doc, 'a')", "json_extract(docjson, '$.a')"},
		{"json", "JSON_EXTRACT_NESTED", "JSON_EXTRACT(doc, 'b.c')", "json_extract(docjson, '$.b.c')"},
		{"json", "JSON_GET", "JSON_GET(doc, 'a')", "json_extract(docjson, '$.a')"},
		{"json", "JSON_SET", "JSON_SET(doc, 'a', 99)", "json_set(docjson, '$.a', 99)"},

		// ── Type conversion / conditional ────────────────────────────────
		{"type", "CAST_INT", "CAST(num AS INT)", "CAST(num AS INTEGER)"},
		{"type", "CAST_TEXT", "CAST(id AS TEXT)", "CAST(id AS TEXT)"},
		{"type", "TYPEOF", "TYPEOF(num)", "typeof(num)"},
		{"type", "COALESCE", "COALESCE(NULL, name)", "coalesce(NULL, name)"},
		{"type", "IFNULL", "IFNULL(NULL, name)", "ifnull(NULL, name)"},
		{"type", "NVL", "NVL(NULL, name)", "ifnull(NULL, name)"},
		{"type", "NULLIF", "NULLIF(n, 5)", "nullif(n, 5)"},
		{"type", "IIF", "IIF(n > 50, 'big', 'small')", "iif(n > 50, 'big', 'small')"},
		{"type", "IF", "IF(n > 50, 'big', 'small')", "iif(n > 50, 'big', 'small')"},

		// ── Encoding / hashing ───────────────────────────────────────────
		{"encoding", "HEX", "HEX(name)", "hex(name)"},
		{"encoding", "UNHEX", "UNHEX(HEX(name))", "unhex(hex(name))"},
		{"encoding", "BASE64", "BASE64(name)", ""},
		{"encoding", "BASE64_DECODE", "BASE64_DECODE(BASE64(name))", ""},
		{"encoding", "MD5", "MD5(name)", ""},
		{"encoding", "SHA1", "SHA1(name)", ""},
		{"encoding", "SHA256", "SHA256(name)", ""},
		{"encoding", "SHA512", "SHA512(name)", ""},

		// ── Array (no SQLite array type; tinySQL-only) ──────────────────
		{"array", "SPLIT", "ARRAY_LENGTH(SPLIT(csvline, ','))", ""},
		{"array", "ARRAY_CONTAINS", "ARRAY_CONTAINS(SPLIT(csvline, ','), 'field2')", ""},
		{"array", "ARRAY_JOIN", "ARRAY_JOIN(SPLIT(csvline, ','), '-')", ""},
		{"array", "ARRAY_DISTINCT", "ARRAY_LENGTH(ARRAY_DISTINCT(SPLIT(csvline, ',')))", ""},
		{"array", "ARRAY_SORT", "ARRAY_JOIN(ARRAY_SORT(SPLIT(csvline, ',')), ',')", ""},

		// ── Window functions ─────────────────────────────────────────────
		{"window", "ROW_NUMBER", "ROW_NUMBER() OVER (ORDER BY id)", "row_number() OVER (ORDER BY id)"},
		{"window", "RANK", "RANK() OVER (ORDER BY n)", "rank() OVER (ORDER BY n)"},
		{"window", "LAG", "LAG(num) OVER (ORDER BY id)", "lag(num) OVER (ORDER BY id)"},
		{"window", "LEAD", "LEAD(num) OVER (ORDER BY id)", "lead(num) OVER (ORDER BY id)"},
		{"window", "FIRST_VALUE", "FIRST_VALUE(num) OVER (ORDER BY id)", "first_value(num) OVER (ORDER BY id)"},
		{"window", "LAST_VALUE", "LAST_VALUE(num) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)", "last_value(num) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"},
		{"window", "SUM_OVER", "SUM(num) OVER (ORDER BY id)", "sum(num) OVER (ORDER BY id)"},
		{"window", "MOVING_AVG", "MOVING_AVG(5, num) OVER (ORDER BY id)", ""},

		// ── Aggregates (single-row whole-table results) ─────────────────
		{"aggregate", "COUNT", "COUNT(*)", "count(*)"},
		{"aggregate", "SUM", "SUM(num)", "sum(num)"},
		{"aggregate", "AVG", "AVG(num)", "avg(num)"},
		{"aggregate", "MIN", "MIN(num)", "min(num)"},
		{"aggregate", "MAX", "MAX(num)", "max(num)"},
	}
}

// funcBenchSeed writes funcSpeedRows deterministic rows shared by both
// engines' "bench" table. dialect selects the CAST/JSON-column spelling that
// differs between them; the data itself is identical.
func funcBenchSeed(b *testing.B, db *sql.DB, dialect string) {
	b.Helper()
	var createSQL string
	switch dialect {
	case "tinysql":
		createSQL = `CREATE TABLE bench (
			id INT, name TEXT, sentence TEXT, num FLOAT, n INT,
			doc JSON, dt TEXT, padded TEXT, padsrc TEXT, csvline TEXT
		)`
	case "sqlite":
		createSQL = `CREATE TABLE bench (
			id INTEGER, name TEXT, sentence TEXT, num REAL, n INTEGER,
			doc TEXT, docjson TEXT, dt TEXT, padded TEXT, padsrc TEXT, csvline TEXT
		)`
	default:
		b.Fatalf("unknown dialect %q", dialect)
	}
	mustExec(b, db, createSQL)

	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("begin seed: %v", err)
	}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < funcSpeedRows; i++ {
		name := fmt.Sprintf("User_%d", i)
		sentence := fmt.Sprintf("The quick brown fox jumps over the lazy dog %d times", i)
		num := float64(i)*3.14159 - float64(funcSpeedRows)/2
		n := i % 100
		doc := fmt.Sprintf(`{"a":%d,"b":{"c":%d},"tags":["x","y","z"]}`, i, i*2)
		dt := base.Add(time.Duration(i) * time.Hour).Format("2006-01-02 15:04:05")
		padded := "  " + name + "  "
		padsrc := fmt.Sprintf("%d", i)
		csvline := fmt.Sprintf("field0,field1,field2,field3_%d,field4", i)

		var q string
		var args []any
		switch dialect {
		case "tinysql":
			q = `INSERT INTO bench (id, name, sentence, num, n, doc, dt, padded, padsrc, csvline)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
			args = []any{i, name, sentence, num, n, doc, dt, padded, padsrc, csvline}
		case "sqlite":
			q = `INSERT INTO bench (id, name, sentence, num, n, doc, docjson, dt, padded, padsrc, csvline)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
			args = []any{i, name, sentence, num, n, doc, doc, dt, padded, padsrc, csvline}
		}
		if _, err := tx.Exec(q, args...); err != nil {
			_ = tx.Rollback()
			b.Fatalf("seed insert %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("commit seed: %v", err)
	}
}

// funcScanOnce runs query and drains every row, scanning the single result
// column into a throwaway interface{} — this only needs to force full
// evaluation, not interpret the value, so it works unchanged across every
// function's return type (int, float, string, blob, nil).
func funcScanOnce(db *sql.DB, query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	var v any
	for rows.Next() {
		if err := rows.Scan(&v); err != nil {
			return err
		}
	}
	return rows.Err()
}

func runFuncBenchmark(b *testing.B, db *sql.DB, expr string) {
	b.Helper()
	query := "SELECT " + expr + " FROM bench"
	if err := funcScanOnce(db, query); err != nil {
		b.Skipf("query not supported: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := funcScanOnce(db, query); err != nil {
			b.Fatalf("query: %v", err)
		}
	}
}

// BenchmarkFuncSpeed is the per-function comparison described in the file
// header. Sub-benchmark names are Benchmark FuncSpeed/<category>/<name>/
// {tinySQL,SQLite}, so `-bench='FuncSpeed/numeric'` (etc.) narrows to one
// category and `go test -bench=FuncSpeed -benchmem | benchstat` (grouping by
// the shared <category>/<name> prefix) compares engines directly.
func BenchmarkFuncSpeed(b *testing.B) {
	tiny := openParity(b, "tinysql", "mem://?tenant=default")
	funcBenchSeed(b, tiny, "tinysql")

	lite := openParity(b, "sqlite", ":memory:")
	funcBenchSeed(b, lite, "sqlite")

	for _, fc := range funcCases() {
		fc := fc
		b.Run(fc.category+"/"+fc.name, func(b *testing.B) {
			b.Run("tinySQL", func(b *testing.B) {
				runFuncBenchmark(b, tiny, fc.tinyExpr)
			})
			if fc.sqliteExpr != "" {
				b.Run("SQLite", func(b *testing.B) {
					runFuncBenchmark(b, lite, fc.sqliteExpr)
				})
			}
		})
	}
}
