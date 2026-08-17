// Benchmarks for per-row scalar function evaluation — the dispatch overhead
// (registry lookup, argument marshalling) plus the string-function
// implementations themselves (REPLACE, TRIM, LEFT, SUBSTR, UPPER, CONCAT,
// INSTR). LENGTH approximates pure call overhead: its body is one len().
// All queries run over the shared 20,000-row perf table; projection and WHERE
// variants exercise the raw fast path's evalRawFuncCall on both sides.
package engine

import "testing"

func BenchmarkScalarLengthProjection(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT LENGTH(note) FROM t`)
}

func BenchmarkScalarUpperProjection(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT UPPER(note) FROM t`)
}

func BenchmarkScalarReplaceProjection(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT REPLACE(note, 'number', 'nr') FROM t`)
}

func BenchmarkScalarTrimProjection(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT TRIM(note) FROM t`)
}

func BenchmarkScalarLeftProjection(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT LEFT(note, 8) FROM t`)
}

func BenchmarkScalarSubstrProjection(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT SUBSTR(note, 6, 12) FROM t`)
}

func BenchmarkScalarConcatProjection(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT CONCAT(grp, '-', sub) FROM t`)
}

func BenchmarkScalarInstrWhere(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE INSTR(note, 'number 123') > 0`)
}

func BenchmarkScalarLengthWhere(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT id FROM t WHERE LENGTH(note) > 28`)
}

func BenchmarkScalarNestedUpperLower(b *testing.B) {
	db := setupPerfTable(b, 20000)
	runBench(b, db, `SELECT LOWER(UPPER(note)) FROM t`)
}
