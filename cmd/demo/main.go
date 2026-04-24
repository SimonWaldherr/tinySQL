// demo – tinySQL feature showcase and benchmark tool.
//
// In showcase mode (default) the tool walks through common SQL patterns
// supported by tinySQL and prints the results, optionally with timing
// information.  In benchmark mode (-bench) every statement is executed
// -count times and aggregate timing is reported.
//
// Usage:
//
//	demo [-dsn <dsn>] [-bench] [-count N] [-timing]
//
// Flags:
//
//	-dsn     Storage DSN (default: in-memory)
//	-bench   Run benchmark mode (repeat each statement -count times)
//	-count   Number of iterations for benchmark mode (default: 100)
//	-timing  Print elapsed time for every statement in showcase mode
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"strings"
	"time"

	_ "github.com/SimonWaldherr/tinySQL/internal/driver"
)

var (
	flagDSN    = flag.String("dsn", "mem://?tenant=default", "DSN (mem:// or file:/path.db?tenant=...&autosave=1)")
	flagBench  = flag.Bool("bench", false, "Run benchmark mode instead of showcase")
	flagCount  = flag.Int("count", 100, "Iterations per statement in benchmark mode")
	flagTiming = flag.Bool("timing", false, "Print elapsed time for each statement in showcase mode")
)

func main() {
	flag.Parse()

	db, err := sql.Open("tinysql", *flagDSN)
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	if *flagBench {
		runBenchmark(db, *flagCount)
	} else {
		runShowcase(db, *flagTiming)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Showcase mode
// ─────────────────────────────────────────────────────────────────────────────

func runShowcase(db *sql.DB, timing bool) {
	fmt.Println("=== tinySQL Feature Showcase ===")
	fmt.Println()

	totalStart := time.Now()
	var ops int

	exec := func(label, q string, args ...any) {
		if label != "" {
			fmt.Printf("── %s\n", label)
		}
		fmt.Println("SQL>", strings.TrimSpace(q))
		start := time.Now()
		up := strings.ToUpper(strings.TrimSpace(q))
		if strings.HasPrefix(up, "SELECT") {
			rows, err := db.Query(q, args...)
			if err != nil {
				fmt.Println("ERR:", err)
				fmt.Println()
				return
			}
			defer rows.Close()
			cols, _ := rows.Columns()
			printRows(rows, cols)
		} else {
			if _, err := db.Exec(q, args...); err != nil {
				fmt.Println("ERR:", err)
			} else {
				fmt.Println("(ok)")
			}
		}
		if timing {
			fmt.Printf("   [%v]\n", time.Since(start).Round(time.Microsecond))
		}
		fmt.Println()
		ops++
	}

	// ── Schema ─────────────────────────────────────────────────────────────
	exec("Schema setup",
		`CREATE TABLE users (id INT, name TEXT, email TEXT, active BOOL)`)
	exec("",
		`CREATE TABLE orders (id INT, user_id INT, amount FLOAT, status TEXT, meta JSON)`)

	// ── Inserts ────────────────────────────────────────────────────────────
	exec("INSERT rows",
		`INSERT INTO users (id, name, email, active) VALUES (1, 'Alice', 'alice@example.com', TRUE)`)
	exec("",
		`INSERT INTO users (id, name, email, active) VALUES (2, 'Bob', NULL, TRUE)`)
	exec("",
		`INSERT INTO users (id, name, email, active) VALUES (3, 'Carol', 'carol@example.com', NULL)`)
	exec("",
		`INSERT INTO orders VALUES (101, 1, 100.5, 'PAID', '{"device":"web","items":[{"sku":"A","qty":1}]}')`)
	exec("",
		`INSERT INTO orders VALUES (102, 1,  75.0, 'PAID', '{"device":"app","items":[{"sku":"B","qty":2}]}')`)
	exec("",
		`INSERT INTO orders VALUES (103, 2, 200.0, 'PAID', '{"device":"web"}')`)
	exec("",
		`INSERT INTO orders VALUES (104, 2,  20.0, 'CANCELED', NULL)`)

	// ── Queries ────────────────────────────────────────────────────────────
	exec("SELECT DISTINCT", `SELECT DISTINCT active FROM users ORDER BY active ASC`)

	exec("JSON_GET", `SELECT id, JSON_GET(meta, 'device') AS device FROM orders ORDER BY id`)

	exec("LEFT JOIN + GROUP BY + ORDER BY", `
		SELECT u.name AS user, SUM(o.amount) AS total, COUNT(*) AS cnt
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'PAID'
		GROUP BY u.name
		ORDER BY total DESC`)

	exec("RIGHT OUTER JOIN", `
		SELECT o.id AS order_id, u.name AS user
		FROM users u
		RIGHT OUTER JOIN orders o ON u.id = o.user_id
		ORDER BY order_id`)

	exec("CTAS (CREATE TEMP TABLE AS SELECT) + HAVING", `
		CREATE TEMP TABLE big_spenders AS
		SELECT u.id AS user_id, u.name, SUM(o.amount) AS total
		FROM users u
		JOIN orders o ON u.id = o.user_id
		WHERE o.status = 'PAID'
		GROUP BY u.id, u.name
		HAVING SUM(o.amount) >= 150`)
	exec("", `SELECT * FROM big_spenders ORDER BY total DESC`)

	exec("UPDATE + DELETE", `UPDATE users SET email = 'alice@new.example', active = TRUE WHERE name = 'Alice'`)
	exec("", `DELETE FROM users WHERE active = FALSE`)
	exec("", `SELECT * FROM users ORDER BY id`)

	// ── Window / advanced ─────────────────────────────────────────────────
	exec("CASE expression", `
		SELECT name,
		       CASE WHEN active = TRUE THEN 'active' ELSE 'inactive' END AS status
		FROM users
		ORDER BY name`)

	fmt.Printf("=== Done: %d statements in %v ===\n", ops, time.Since(totalStart).Round(time.Millisecond))
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark mode
// ─────────────────────────────────────────────────────────────────────────────

type benchCase struct {
	label string
	setup []string // run once before the timed loop
	query string   // executed -count times
	isSelect bool
}

func runBenchmark(db *sql.DB, count int) {
	fmt.Printf("=== tinySQL Benchmark (n=%d) ===\n\n", count)

	// One-time schema + seed
	seed := []string{
		`CREATE TABLE bench_users (id INT, name TEXT, score FLOAT)`,
	}
	for _, q := range seed {
		if _, err := db.Exec(q); err != nil {
			fmt.Println("seed ERR:", err)
			return
		}
	}
	for i := 1; i <= 1000; i++ {
		q := fmt.Sprintf(`INSERT INTO bench_users VALUES (%d, 'user%d', %f)`, i, i, float64(i)*1.5)
		if _, err := db.Exec(q); err != nil {
			fmt.Println("seed insert ERR:", err)
			return
		}
	}

	cases := []benchCase{
		{
			label:    "SELECT full scan (1000 rows)",
			query:    `SELECT * FROM bench_users`,
			isSelect: true,
		},
		{
			label:    "SELECT with WHERE filter",
			query:    `SELECT * FROM bench_users WHERE score > 500`,
			isSelect: true,
		},
		{
			label:    "SELECT aggregate COUNT + SUM",
			query:    `SELECT COUNT(*) AS cnt, SUM(score) AS total FROM bench_users`,
			isSelect: true,
		},
		{
			label:    "SELECT ORDER BY + LIMIT 10",
			query:    `SELECT * FROM bench_users ORDER BY score DESC LIMIT 10`,
			isSelect: true,
		},
		{
			label:    "INSERT single row",
			query:    `INSERT INTO bench_users VALUES (9999, 'bench', 0.0)`,
			isSelect: false,
			setup:    []string{`DELETE FROM bench_users WHERE id = 9999`},
		},
	}

	type result struct {
		label string
		min, max, avg time.Duration
		ops int
	}
	results := make([]result, 0, len(cases))

	for _, bc := range cases {
		for _, s := range bc.setup {
			if _, err := db.Exec(s); err != nil {
				fmt.Printf("setup ERR (%s): %v\n", bc.label, err)
			}
		}

		var minD, maxD, totalD time.Duration
		for i := 0; i < count; i++ {
			// Reset insert target so each iteration can re-insert
			if !bc.isSelect && i > 0 {
				for _, s := range bc.setup {
					_, _ = db.Exec(s)
				}
			}
			start := time.Now()
			if bc.isSelect {
				rows, err := db.Query(bc.query)
				if err != nil {
					fmt.Printf("ERR (%s): %v\n", bc.label, err)
					break
				}
				// Drain results
				for rows.Next() {
				}
				rows.Close()
			} else {
				if _, err := db.Exec(bc.query); err != nil {
					fmt.Printf("ERR (%s): %v\n", bc.label, err)
					break
				}
			}
			d := time.Since(start)
			totalD += d
			if i == 0 || d < minD {
				minD = d
			}
			if d > maxD {
				maxD = d
			}
		}
		avg := totalD / time.Duration(count)
		results = append(results, result{
			label: bc.label,
			min:   minD, max: maxD, avg: avg,
			ops: int(time.Second / avg),
		})
	}

	// Print results table
	fmt.Printf("%-45s  %10s  %10s  %10s  %10s\n", "Statement", "min", "avg", "max", "ops/s")
	fmt.Println(strings.Repeat("-", 92))
	for _, r := range results {
		fmt.Printf("%-45s  %10v  %10v  %10v  %10d\n",
			r.label,
			r.min.Round(time.Microsecond),
			r.avg.Round(time.Microsecond),
			r.max.Round(time.Microsecond),
			r.ops,
		)
	}
	fmt.Println()
}

//nolint:gocyclo // Demo printer handles formatting, scanning, and alignment logic together.
func printRows(rows *sql.Rows, cols []string) {
	type rowMap = map[string]any
	var out []rowMap
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Println("ERR:", err)
			return
		}
		m := rowMap{}
		for i, c := range cols {
			m[c] = dePtr(ptrs[i])
		}
		out = append(out, m)
	}
	width := make([]int, len(cols))
	for i, c := range cols {
		width[i] = len(c)
	}
	cell := func(v any) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("%v", v)
	}
	for _, r := range out {
		for i, c := range cols {
			if w := len(cell(r[c])); w > width[i] {
				width[i] = w
			}
		}
	}
	for i, c := range cols {
		fmt.Print(padRight(c, width[i]))
		if i < len(cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	for i := range cols {
		fmt.Print(strings.Repeat("-", width[i]))
		if i < len(cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	for _, r := range out {
		for i, c := range cols {
			fmt.Print(padRight(cell(r[c]), width[i]))
			if i < len(cols)-1 {
				fmt.Print("  ")
			}
		}
		fmt.Println()
	}
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}

func dePtr(p any) any {
	switch v := p.(type) {
	case *any:
		return *v
	default:
		return v
	}
}
