// Benchmarks for the lexer and parser themselves -- distinct from every
// other benchmark in this package, which parses a query once with mustParse
// outside the timed loop and then repeatedly executes the already-built AST.
// Nothing in the repository measured parse/lex cost in isolation before
// this file. It matters wherever a query's SQL text is not reused across
// executions (ad-hoc queries, one INSERT statement built per row by a bulk
// loader without parameter binding, etc.) -- exactly the case every other
// benchmark's mustParse-outside-the-loop pattern deliberately excludes.
package engine

import (
	"strconv"
	"strings"
	"testing"
)

// BenchmarkParseSimpleSelect measures the common case: a short, single-table
// query with a handful of keywords and identifiers.
func BenchmarkParseSimpleSelect(b *testing.B) {
	sql := `SELECT id, name, email FROM users WHERE id = 42 AND active = true`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		if _, err := p.ParseStatement(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseComplexSelect measures a query that exercises far more of
// the lexer's keyword surface per statement: multiple JOINs, GROUP BY,
// HAVING, ORDER BY, a window function, and several scalar function calls.
func BenchmarkParseComplexSelect(b *testing.B) {
	sql := `
		SELECT o.id, c.name, SUM(o.amount) AS total,
		       AVG(o.amount) AS avg_amount,
		       ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY o.created_at DESC) AS rn,
		       UPPER(c.name) AS name_upper,
		       DATE_ADD(o.created_at, 7, 'DAY') AS due_date
		FROM orders o
		JOIN customers c ON o.customer_id = c.id
		LEFT JOIN shipments s ON s.order_id = o.id
		WHERE o.status = 'open' AND o.amount > 100 AND c.region IN ('EU', 'US', 'APAC')
		GROUP BY o.id, c.name, c.id, o.created_at
		HAVING SUM(o.amount) > 500
		ORDER BY total DESC
		LIMIT 50`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		if _, err := p.ParseStatement(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseBulkInsert measures a many-row INSERT, the shape a bulk
// loader without parameter binding builds once per batch: heavy on
// tokenizeNumber/tokenizeString and light on keywords relative to its total
// token count.
func BenchmarkParseBulkInsert(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO events (id, user_id, kind, payload, score) VALUES ")
	for i := 0; i < 200; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(", ")
		sb.WriteString(strconv.Itoa(i % 50))
		sb.WriteString(", 'click', 'payload text here', ")
		sb.WriteString(strconv.FormatFloat(float64(i)*1.5, 'f', 2, 64))
		sb.WriteByte(')')
	}
	sql := sb.String()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := NewParser(sql)
		if _, err := p.ParseStatement(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLexerNextTokenOnly isolates the lexer from the parser entirely:
// it drains a token stream without building any AST, attributing cost
// purely to nextToken/isKeyword/upper rather than to parser recursion.
func BenchmarkLexerNextTokenOnly(b *testing.B) {
	sql := `
		SELECT o.id, c.name, SUM(o.amount) AS total,
		       AVG(o.amount) AS avg_amount,
		       ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY o.created_at DESC) AS rn,
		       UPPER(c.name) AS name_upper,
		       DATE_ADD(o.created_at, 7, 'DAY') AS due_date
		FROM orders o
		JOIN customers c ON o.customer_id = c.id
		LEFT JOIN shipments s ON s.order_id = o.id
		WHERE o.status = 'open' AND o.amount > 100 AND c.region IN ('EU', 'US', 'APAC')
		GROUP BY o.id, c.name, c.id, o.created_at
		HAVING SUM(o.amount) > 500
		ORDER BY total DESC
		LIMIT 50`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lx := newLexer(sql)
		for {
			tok := lx.nextToken()
			if tok.Typ == tEOF {
				break
			}
		}
	}
}
