// Package main provides a practical SQL playground for tinySQL.
//
// It seeds sample tables (users, orders) and then lets you:
//   - run any SQL script file via -script
//   - drop into an interactive REPL via -interactive
//   - time every statement via -timer
//
// Usage:
//
//	demo [OPTIONS]
//	  -dsn string        Storage DSN (default: in-memory)
//	  -seed              Populate sample tables (default: true)
//	  -script FILE       Execute SQL statements from FILE
//	  -interactive       Start interactive SQL shell after setup
//	  -timer             Print execution time for every statement
//	  -quiet             Suppress DDL/DML output; show only query results
package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	_ "github.com/SimonWaldherr/tinySQL/internal/driver"
)

var (
	flagDSN         = flag.String("dsn", "mem://?tenant=default", "DSN (mem:// or file:/path.db?tenant=...&autosave=1)")
	flagSeed        = flag.Bool("seed", true, "Populate sample tables (users, orders) before running")
	flagScript      = flag.String("script", "", "Path to a SQL script file to execute")
	flagInteractive = flag.Bool("interactive", false, "Start an interactive SQL shell after setup")
	flagTimer       = flag.Bool("timer", false, "Print execution time for every statement")
	flagQuiet       = flag.Bool("quiet", false, "Suppress DDL/DML confirmation output; show only SELECT results")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `tinySQL SQL Playground

Seeds sample data and lets you query it interactively or via a script.

Usage:
  demo [OPTIONS]

Options:
`)
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  # Show feature tour with sample data
  demo

  # Execute a SQL script against sample data
  demo -script queries.sql

  # Load sample data and start interactive shell
  demo -interactive

  # Use a persistent file database
  demo -dsn "file:/tmp/mydb.db?tenant=main" -interactive
`)
	}
	flag.Parse()

	db, err := sql.Open("tinysql", *flagDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open error:", err)
		os.Exit(1)
	}
	defer db.Close()

	exec := newExecutor(db, *flagTimer, *flagQuiet)

	if *flagSeed {
		seedSampleData(exec)
	}

	if *flagScript != "" {
		if err := runScriptFile(exec, *flagScript); err != nil {
			fmt.Fprintln(os.Stderr, "script error:", err)
			os.Exit(1)
		}
	}

	if *flagInteractive {
		runInteractive(db, *flagTimer)
		return
	}

	// Default: run the built-in feature tour when no script/interactive mode.
	if *flagScript == "" && !*flagInteractive {
		if !*flagSeed {
			fmt.Fprintln(os.Stderr, "Nothing to do: use -seed (default), -script, or -interactive.")
			flag.Usage()
			os.Exit(1)
		}
		runFeatureTour(exec)
	}
}

// ---- executor ---------------------------------------------------------------

type executor struct {
	db    *sql.DB
	timer bool
	quiet bool
}

func newExecutor(db *sql.DB, timer, quiet bool) *executor {
	return &executor{db: db, timer: timer, quiet: quiet}
}

// run executes a single SQL statement and prints results.
func (e *executor) run(q string) error {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil
	}

	start := time.Now()
	up := strings.ToUpper(q)
	isQuery := strings.HasPrefix(up, "SELECT") || strings.HasPrefix(up, "WITH")

	if isQuery {
		rows, err := e.db.Query(q)
		if err != nil {
			return err
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		printRows(os.Stdout, rows, cols)
	} else {
		res, err := e.db.Exec(q)
		if err != nil {
			return err
		}
		if !e.quiet {
			n, _ := res.RowsAffected()
			if n > 0 {
				fmt.Printf("(ok, %d row(s) affected)\n", n)
			} else {
				fmt.Println("(ok)")
			}
		}
	}

	if e.timer {
		fmt.Printf("-- %.3fms\n", float64(time.Since(start).Microseconds())/1000.0)
	}
	return nil
}

// ---- seed data --------------------------------------------------------------

func seedSampleData(exec *executor) {
	stmts := []string{
		`CREATE TABLE users (id INT, name TEXT, email TEXT, active BOOL)`,
		`CREATE TABLE orders (id INT, user_id INT, amount FLOAT, status TEXT, meta JSON)`,
		`INSERT INTO users (id, name, email, active) VALUES (1, 'Alice', 'alice@example.com', TRUE)`,
		`INSERT INTO users (id, name, email, active) VALUES (2, 'Bob', NULL, TRUE)`,
		`INSERT INTO users (id, name, email, active) VALUES (3, 'Carol', 'carol@example.com', NULL)`,
		`INSERT INTO orders VALUES (101, 1, 100.5, 'PAID', '{"device":"web","items":[{"sku":"A","qty":1}]}')`,
		`INSERT INTO orders VALUES (102, 1, 75.0,  'PAID', '{"device":"app","items":[{"sku":"B","qty":2}]}')`,
		`INSERT INTO orders VALUES (103, 2, 200.0, 'PAID', '{"device":"web"}')`,
		`INSERT INTO orders VALUES (104, 2, 20.0,  'CANCELED', NULL)`,
	}
	for _, s := range stmts {
		if err := exec.run(s); err != nil {
			fmt.Fprintln(os.Stderr, "seed error:", err)
		}
	}
}

// ---- feature tour -----------------------------------------------------------

func runFeatureTour(exec *executor) {
	type step struct {
		label string
		sql   string
	}
	steps := []step{
		{"DISTINCT values", `SELECT DISTINCT active FROM users ORDER BY active ASC`},
		{"JSON extraction", `SELECT id, JSON_GET(meta, 'device') AS device FROM orders ORDER BY id`},
		{"LEFT JOIN + GROUP BY + aggregates", `
SELECT u.name AS user, SUM(o.amount) AS total, COUNT(*) AS cnt
FROM users u
LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'PAID'
GROUP BY u.name
ORDER BY total DESC`},
		{"RIGHT OUTER JOIN", `
SELECT o.id AS order_id, u.name AS user
FROM users u
RIGHT OUTER JOIN orders o ON u.id = o.user_id
ORDER BY order_id`},
		{"CREATE TEMP TABLE AS SELECT + HAVING", `
CREATE TEMP TABLE big_spenders AS
SELECT u.id AS user_id, u.name, SUM(o.amount) AS total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.status = 'PAID'
GROUP BY u.id, u.name
HAVING SUM(o.amount) >= 150`},
		{"Query temp table", `SELECT * FROM big_spenders ORDER BY total DESC`},
		{"UPDATE", `UPDATE users SET email = 'alice@new.example', active = TRUE WHERE name = 'Alice'`},
		{"DELETE + SELECT", `DELETE FROM users WHERE active IS NULL`},
		{"Final state", `SELECT * FROM users ORDER BY id`},
	}

	fmt.Println("=== tinySQL Feature Tour ===")
	fmt.Println("(Sample tables: users, orders)")
	fmt.Println()

	for _, s := range steps {
		fmt.Printf("-- %s\n", s.label)
		q := strings.TrimSpace(s.sql)
		fmt.Println(q)
		if err := exec.run(q); err != nil {
			fmt.Fprintln(os.Stderr, "ERR:", err)
		}
		fmt.Println()
	}
}

// ---- script execution -------------------------------------------------------

// runScriptFile reads SQL statements from path and executes them.
func runScriptFile(exec *executor, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return runScript(exec, f, path)
}

// runScript executes SQL statements from reader, splitting on semicolons.
func runScript(exec *executor, r io.Reader, name string) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024), 4*1024*1024)

	var buf strings.Builder
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Skip blank lines and SQL comments outside a statement buffer.
		if buf.Len() == 0 && (trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "/*")) {
			continue
		}

		buf.WriteString(line)
		buf.WriteByte('\n')

		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(buf.String())
			stmt = strings.TrimSuffix(stmt, ";")
			buf.Reset()
			if stmt == "" {
				continue
			}
			fmt.Println(stmt + ";")
			if err := exec.run(stmt); err != nil {
				return fmt.Errorf("%s line ~%d: %w", name, lineNo, err)
			}
			fmt.Println()
		}
	}

	// Execute any trailing statement that lacks a final semicolon.
	if s := strings.TrimSpace(buf.String()); s != "" {
		fmt.Println(s)
		if err := exec.run(s); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		fmt.Println()
	}

	return scanner.Err()
}

// ---- interactive REPL -------------------------------------------------------

func runInteractive(db *sql.DB, timer bool) {
	exec := newExecutor(db, timer, false)

	fmt.Println("tinySQL playground — type SQL ending with ';' to execute, '.quit' to exit.")
	fmt.Println("  .tables   list tables      .help   show this message")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024), 4*1024*1024)

	var buf strings.Builder
	prompt := func() {
		if buf.Len() == 0 {
			fmt.Print("sql> ")
		} else {
			fmt.Print("  -> ")
		}
	}

	prompt()
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if buf.Len() == 0 {
			switch trimmed {
			case ".quit", ".exit":
				fmt.Println("Bye.")
				return
			case ".help":
				fmt.Println("  .tables     list tables")
				fmt.Println("  .quit       exit")
				prompt()
				continue
			case ".tables":
				rows, err := db.Query(`SELECT name FROM information_schema.tables WHERE table_schema = 'public'`)
				if err != nil {
					// Fall back: query tinySQL internal tables list.
					fmt.Println("(cannot list tables:", err, ")")
				} else {
					for rows.Next() {
						var name string
						_ = rows.Scan(&name)
						fmt.Println(" ", name)
					}
					rows.Close()
				}
				prompt()
				continue
			}
		}

		buf.WriteString(line)
		buf.WriteByte('\n')

		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSuffix(strings.TrimSpace(buf.String()), ";")
			buf.Reset()
			if stmt != "" {
				if err := exec.run(stmt); err != nil {
					fmt.Fprintln(os.Stderr, "ERR:", err)
				}
			}
			fmt.Println()
		}

		prompt()
	}
}

// ---- output helpers ---------------------------------------------------------

//nolint:gocyclo // Printer handles formatting, scanning, and alignment in one pass.
func printRows(out io.Writer, rows *sql.Rows, cols []string) {
	type rowMap = map[string]any
	var data []rowMap

	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintln(out, "ERR:", err)
			return
		}
		m := rowMap{}
		for i, c := range cols {
			m[c] = dePtr(ptrs[i])
		}
		data = append(data, m)
	}

	cell := func(v any) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("%v", v)
	}

	width := make([]int, len(cols))
	for i, c := range cols {
		width[i] = len(c)
	}
	for _, r := range data {
		for i, c := range cols {
			if w := len(cell(r[c])); w > width[i] {
				width[i] = w
			}
		}
	}

	// Header
	for i, c := range cols {
		fmt.Fprint(out, padRight(c, width[i]))
		if i < len(cols)-1 {
			fmt.Fprint(out, "  ")
		}
	}
	fmt.Fprintln(out)
	for i := range cols {
		fmt.Fprint(out, strings.Repeat("-", width[i]))
		if i < len(cols)-1 {
			fmt.Fprint(out, "  ")
		}
	}
	fmt.Fprintln(out)

	// Rows
	for _, r := range data {
		for i, c := range cols {
			fmt.Fprint(out, padRight(cell(r[c]), width[i]))
			if i < len(cols)-1 {
				fmt.Fprint(out, "  ")
			}
		}
		fmt.Fprintln(out)
	}
	fmt.Fprintf(out, "(%d row(s))\n", len(data))
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}

func dePtr(p any) any {
	if v, ok := p.(*any); ok {
		return *v
	}
	return p
}
