// Package main provides a practical SQL playground for tinySQL.
//
// It seeds sample tables (users, orders, sales, articles, docs) and then
// lets you:
//   - run any SQL script file via -script
//   - drop into an interactive REPL via -interactive
//   - time every statement via -timer
//   - export query results in multiple formats via -output
//   - import CSV/JSON files via .import command in REPL
//
// The built-in feature tour (run with no -script/-interactive flag) covers
// PIVOT, window functions (RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST/NTILE),
// FULL OUTER / CROSS JOIN, whole-row full-text search (FTS_SEARCH and
// ROW_TO_TEXT), vector search with VEC_WARM index warm-up, and the
// SQL:2008 OFFSET ... FETCH syntax, alongside traditional JOIN/GROUP
// BY/aggregate queries. It also shows a row trigger that writes an audit log
// whenever a new order is inserted.
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
//	  -output FORMAT     Output format: table, csv, json (default: table)
package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/SimonWaldherr/tinySQL/driver"
	"github.com/SimonWaldherr/tinySQL/sqlutil"
)

var (
	flagDSN         = flag.String("dsn", "mem://?tenant=default", "DSN (mem:// or file:/path.db?tenant=...&autosave=1)")
	flagSeed        = flag.Bool("seed", true, "Populate sample tables (users, orders) before running")
	flagScript      = flag.String("script", "", "Path to a SQL script file to execute")
	flagInteractive = flag.Bool("interactive", false, "Start an interactive SQL shell after setup")
	flagTimer       = flag.Bool("timer", false, "Print execution time for every statement")
	flagQuiet       = flag.Bool("quiet", false, "Suppress DDL/DML confirmation output; show only SELECT results")
	flagOutput      = flag.String("output", "table", "Output format: table, csv, json")
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

  # Use human-readable JSON-per-table storage instead of the GOB default
  demo -dsn "file:/tmp/mydb?tenant=main&mode=json" -interactive
`)
	}
	flag.Parse()

	db, err := sql.Open("tinysql", *flagDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open error:", err)
		os.Exit(1)
	}
	defer db.Close()

	exec := newExecutor(db, *flagTimer, *flagQuiet, *flagOutput)

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
		runInteractive(db, *flagTimer, *flagOutput)
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
	db     *sql.DB
	timer  bool
	quiet  bool
	output string // "table", "csv", "json"
}

func newExecutor(db *sql.DB, timer, quiet bool, output string) *executor {
	return &executor{db: db, timer: timer, quiet: quiet, output: output}
}

// run executes a single SQL statement and prints results.
func (e *executor) run(q string) error {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil
	}

	start := time.Now()

	if sqlutil.IsResultProducing(q) {
		rows, err := e.db.Query(q)
		if err != nil {
			return err
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		switch e.output {
		case "csv":
			printRowsCSV(os.Stdout, rows, cols)
		case "json":
			printRowsJSON(os.Stdout, rows, cols)
		default:
			printRows(os.Stdout, rows, cols)
		}
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
		`CREATE TABLE order_audit (order_id INT, event TEXT)`,
		`CREATE TRIGGER orders_after_insert AFTER INSERT ON orders FOR EACH ROW BEGIN INSERT INTO order_audit VALUES (new.id, 'created'); END`,
		`INSERT INTO users (id, name, email, active) VALUES (1, 'Alice', 'alice@example.com', TRUE)`,
		`INSERT INTO users (id, name, email, active) VALUES (2, 'Bob', NULL, TRUE)`,
		`INSERT INTO users (id, name, email, active) VALUES (3, 'Carol', 'carol@example.com', NULL)`,
		`INSERT INTO orders VALUES (101, 1, 100.5, 'PAID', '{"device":"web","items":[{"sku":"A","qty":1}]}')`,
		`INSERT INTO orders VALUES (102, 1, 75.0,  'PAID', '{"device":"app","items":[{"sku":"B","qty":2}]}')`,
		`INSERT INTO orders VALUES (103, 2, 200.0, 'PAID', '{"device":"web"}')`,
		`INSERT INTO orders VALUES (104, 2, 20.0,  'CANCELED', NULL)`,

		// sales: region/category/amount for PIVOT and window-function examples.
		`CREATE TABLE sales (id INT, region TEXT, category TEXT, amount FLOAT)`,
		`INSERT INTO sales VALUES (1, 'East', 'Electronics', 1200)`,
		`INSERT INTO sales VALUES (2, 'East', 'Furniture', 350)`,
		`INSERT INTO sales VALUES (3, 'East', 'Electronics', 800)`,
		`INSERT INTO sales VALUES (4, 'West', 'Electronics', 950)`,
		`INSERT INTO sales VALUES (5, 'West', 'Furniture', 500)`,
		`INSERT INTO sales VALUES (6, 'West', 'Furniture', 275)`,

		// articles: for FTS_SEARCH (whole-row search) and ROW_TO_TEXT examples.
		`CREATE TABLE articles (id INT, title TEXT, body TEXT)`,
		`INSERT INTO articles VALUES (1, 'Go Programming', 'Go is a fast compiled language for systems programming')`,
		`INSERT INTO articles VALUES (2, 'Python Tutorial', 'Python is a dynamic scripting language popular for data science')`,
		`INSERT INTO articles VALUES (3, 'Database Design', 'Relational databases store data in tables with relationships')`,

		// docs: for vector search (VEC_SEARCH / VEC_WARM). Tiny 3-dim toy
		// embeddings so the example runs with no external embedding model.
		`CREATE TABLE docs (id INT, title TEXT, embedding VECTOR)`,
		`INSERT INTO docs VALUES (1, 'Cat facts', '[0.9, 0.1, 0.0]')`,
		`INSERT INTO docs VALUES (2, 'Dog facts', '[0.8, 0.2, 0.0]')`,
		`INSERT INTO docs VALUES (3, 'Rocket science', '[0.0, 0.1, 0.9]')`,
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
		{"Trigger: insert an order and inspect its audit row", `INSERT INTO orders VALUES (105, 3, 49.0, 'PAID', '{"device":"web"}')`},
		{"  (the AFTER INSERT trigger wrote this audit row)", `SELECT order_id, event FROM order_audit ORDER BY order_id`},
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
		{"FULL OUTER JOIN (every user, every order, matched or not)", `
SELECT u.name AS user, o.id AS order_id
FROM users u
FULL OUTER JOIN orders o ON u.id = o.user_id
ORDER BY user, order_id`},
		{"CROSS JOIN (Cartesian product: every user paired with every order)", `
SELECT u.name AS user, o.id AS order_id
FROM users u
CROSS JOIN orders o
ORDER BY user, order_id`},
		{"PIVOT: category totals as columns, one row per region", `
SELECT *
FROM (SELECT region, category, amount FROM sales) AS s
PIVOT (SUM(amount) FOR category IN ('Electronics' AS electronics, 'Furniture' AS furniture))
ORDER BY region`},
		{"Window functions: RANK/DENSE_RANK/PERCENT_RANK/NTILE", `
SELECT region, category, amount,
       RANK() OVER (ORDER BY amount DESC) AS rank,
       DENSE_RANK() OVER (ORDER BY amount DESC) AS dense_rank,
       PERCENT_RANK() OVER (ORDER BY amount) AS percent_rank,
       NTILE(2) OVER (ORDER BY amount) AS half
FROM sales
ORDER BY amount DESC`},
		{"FTS_SEARCH: whole-row ranked search (no column list needed)", `
SELECT id, title, _fts_score, _fts_rank
FROM FTS_SEARCH('articles', 'programming language', 2)
ORDER BY _fts_rank`},
		{"ROW_TO_TEXT(): ad-hoc whole-row substring search in WHERE", `
SELECT id, title FROM articles WHERE ROW_TO_TEXT() LIKE '%Python%'`},
		// This table has 3 toy rows, so warm-up is instant here — the pattern
		// matters on real RAG-sized tables (thousands+ rows), where building
		// the HNSW graph is real work best done once via VEC_WARM (e.g. right
		// after a bulk load) rather than on whichever query runs first. See
		// BENCHMARKS.md for index build/query numbers at that scale.
		{"VEC_WARM + VEC_SEARCH: prebuild the index, then query it", `
SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`},
		{"  (now served from the warmed index)", `
SELECT id, title, _vec_distance
FROM VEC_SEARCH('docs', 'embedding', '[0.85, 0.15, 0.0]', 2, 'cosine', 'hnsw')
ORDER BY _vec_rank`},
		{"LIMIT ALL (explicit 'no limit')", `SELECT id FROM articles ORDER BY id LIMIT ALL`},
		{"SQL:2008 OFFSET ... FETCH syntax", `
SELECT id, title FROM articles ORDER BY id OFFSET 1 ROWS FETCH FIRST 1 ROW ONLY`},
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

func runInteractive(db *sql.DB, timer bool, output string) {
	exec := newExecutor(db, timer, false, output)

	fmt.Println("tinySQL playground — type SQL ending with ';' to execute, '.quit' to exit.")
	fmt.Println("  .help for commands")
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

		if buf.Len() == 0 && strings.HasPrefix(trimmed, ".") {
			handleDotCommand(db, exec, trimmed)
			prompt()
			continue
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

// handleDotCommand processes REPL dot-commands.
// demoListAllTableNames returns all table names from the database.
func demoListAllTableNames(db *sql.DB) []string {
	rows, err := db.Query(`SELECT name FROM sys.tables ORDER BY name`)
	if err != nil {
		fmt.Println("Error:", err)
		return nil
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		tables = append(tables, n)
	}
	return tables
}

// handleDotSchema handles the .schema dot command.
func handleDotSchema(db *sql.DB, parts []string) {
	if len(parts) > 1 {
		printTableSchema(db, parts[1])
		return
	}
	for _, t := range demoListAllTableNames(db) {
		printTableSchema(db, t)
		fmt.Println()
	}
}

// handleDotCount handles the .count dot command.
func handleDotCount(db *sql.DB, parts []string) {
	tables := demoListAllTableNames(db)
	if len(parts) > 1 {
		tables = parts[1:]
	}
	for _, t := range tables {
		printTableCount(db, t)
	}
}

// handleDotImport handles the .import dot command.
func handleDotImport(db *sql.DB, parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: .import FILE [TABLE]")
		return
	}
	filePath := parts[1]
	tableName := ""
	if len(parts) > 2 {
		tableName = parts[2]
	} else {
		base := filepath.Base(filePath)
		tableName = strings.TrimSuffix(base, filepath.Ext(base))
	}
	if err := importFile(db, filePath, tableName); err != nil {
		fmt.Fprintln(os.Stderr, "Import error:", err)
	} else {
		fmt.Printf("Imported into table '%s'\n", tableName)
	}
}

// handleDotDump handles the .dump dot command.
func handleDotDump(db *sql.DB, parts []string) {
	if len(parts) > 1 {
		dumpTable(db, parts[1])
		return
	}
	for _, t := range demoListAllTableNames(db) {
		dumpTable(db, t)
	}
}

// handleDotOutput handles the .output dot command.
func handleDotOutput(exec *executor, parts []string) {
	if len(parts) < 2 {
		fmt.Printf("Current output format: %s\n", exec.output)
		return
	}
	switch parts[1] {
	case "table", "csv", "json":
		exec.output = parts[1]
		fmt.Printf("Output format set to: %s\n", exec.output)
	default:
		fmt.Println("Unknown format. Use: table, csv, json")
	}
}

// handleDotTimer handles the .timer dot command.
func handleDotTimer(exec *executor, parts []string) {
	if len(parts) < 2 {
		if exec.timer {
			fmt.Println("Timer: on")
		} else {
			fmt.Println("Timer: off")
		}
		return
	}
	exec.timer = (parts[1] == "on")
	fmt.Printf("Timer: %s\n", parts[1])
}

func handleDotCommand(db *sql.DB, exec *executor, cmd string) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return
	}
	switch parts[0] {
	case ".quit", ".exit":
		fmt.Println("Bye.")
		os.Exit(0)

	case ".help":
		fmt.Println(`Commands:
  .tables               List all tables
  .schema [TABLE]       Show column info (all or specific table)
  .count [TABLE]        Show row counts
  .import FILE [TABLE]  Import CSV/JSON file into TABLE
  .dump [TABLE]         Dump table(s) as INSERT statements
  .output FORMAT        Set output format (table, csv, json)
  .timer on|off         Toggle query timing
  .quit                 Exit`)

	case ".tables":
		rows, err := db.Query(`SELECT name FROM sys.tables ORDER BY name`)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		for rows.Next() {
			var name string
			_ = rows.Scan(&name)
			fmt.Println(" ", name)
		}
		rows.Close()

	case ".schema":
		handleDotSchema(db, parts)

	case ".count":
		handleDotCount(db, parts)

	case ".import":
		handleDotImport(db, parts)

	case ".dump":
		handleDotDump(db, parts)

	case ".output":
		handleDotOutput(exec, parts)

	case ".timer":
		handleDotTimer(exec, parts)

	default:
		fmt.Printf("Unknown command: %s (type .help)\n", parts[0])
	}
}

// printTableSchema shows columns for a table.
func printTableSchema(db *sql.DB, table string) {
	rows, err := db.Query(fmt.Sprintf(
		`SELECT name, type, nullable, pk FROM sys.columns WHERE table_name = '%s' ORDER BY ordinal`,
		table))
	if err != nil {
		// Fall back to a simple SELECT to infer columns.
		r, err2 := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", table))
		if err2 != nil {
			fmt.Printf("Cannot describe %s: %v\n", table, err)
			return
		}
		cols, _ := r.Columns()
		r.Close()
		fmt.Printf("Table: %s\n", table)
		for _, c := range cols {
			fmt.Printf("  %s\n", c)
		}
		return
	}
	defer rows.Close()
	fmt.Printf("Table: %s\n", table)
	for rows.Next() {
		var name, typ string
		var nullable, pk bool
		if err := rows.Scan(&name, &typ, &nullable, &pk); err != nil {
			// Simplified fallback: just print the name
			fmt.Printf("  (scan error: %v)\n", err)
			continue
		}
		flags := ""
		if pk {
			flags += " PK"
		}
		if !nullable {
			flags += " NOT NULL"
		}
		fmt.Printf("  %-20s %-10s%s\n", name, typ, flags)
	}
}

// printTableCount shows row count for a table.
func printTableCount(db *sql.DB, table string) {
	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		fmt.Printf("  %-20s (error: %v)\n", table, err)
		return
	}
	fmt.Printf("  %-20s %d rows\n", table, cnt)
}

// importFile imports a CSV or JSON file into the given table.
func importFile(db *sql.DB, path, table string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv", ".tsv":
		return importCSV(db, f, table, ext == ".tsv")
	case ".json":
		return importJSON(db, f, table)
	default:
		return fmt.Errorf("unsupported format: %s (use .csv, .tsv, .json)", ext)
	}
}

// importCSV imports CSV data by creating a table from the header and inserting rows.
func importCSV(db *sql.DB, r io.Reader, table string, tab bool) error {
	reader := csv.NewReader(r)
	if tab {
		reader.Comma = '\t'
	}
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading CSV header: %w", err)
	}
	// Sanitize column names
	for i, h := range header {
		header[i] = sanitizeIdent(h)
	}

	// Create table with TEXT columns
	var colDefs []string
	for _, h := range header {
		colDefs = append(colDefs, h+" TEXT")
	}
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, strings.Join(colDefs, ", "))
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	// Insert rows
	count := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed rows
		}
		var vals []string
		for _, v := range record {
			vals = append(vals, "'"+strings.ReplaceAll(v, "'", "''")+"'")
		}
		// Pad with NULLs if needed
		for len(vals) < len(header) {
			vals = append(vals, "NULL")
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table, strings.Join(header, ", "), strings.Join(vals[:len(header)], ", "))
		if _, err := db.Exec(insertSQL); err != nil {
			fmt.Fprintf(os.Stderr, "  skip row %d: %v\n", count+1, err)
			continue
		}
		count++
	}
	fmt.Printf("  %d rows imported\n", count)
	return nil
}

// importJSON imports an array-of-objects JSON file.
func importJSON(db *sql.DB, r io.Reader, table string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var records []map[string]any
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("parsing JSON: %w", err)
	}
	if len(records) == 0 {
		return fmt.Errorf("empty JSON array")
	}

	// Collect all keys
	keySet := make(map[string]bool)
	for _, rec := range records {
		for k := range rec {
			keySet[k] = true
		}
	}
	var cols []string
	for k := range keySet {
		cols = append(cols, sanitizeIdent(k))
	}

	// Create table
	var colDefs []string
	for _, c := range cols {
		colDefs = append(colDefs, c+" TEXT")
	}
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, strings.Join(colDefs, ", "))
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	// Insert rows
	count := 0
	for _, rec := range records {
		var vals []string
		for _, c := range cols {
			v, ok := rec[c]
			if !ok || v == nil {
				vals = append(vals, "NULL")
			} else {
				s := fmt.Sprintf("%v", v)
				vals = append(vals, "'"+strings.ReplaceAll(s, "'", "''")+"'")
			}
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table, strings.Join(cols, ", "), strings.Join(vals, ", "))
		if _, err := db.Exec(insertSQL); err != nil {
			fmt.Fprintf(os.Stderr, "  skip row %d: %v\n", count+1, err)
			continue
		}
		count++
	}
	fmt.Printf("  %d rows imported\n", count)
	return nil
}

// dumpTable outputs INSERT statements for all rows in a table.
func dumpTable(db *sql.DB, table string) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dump %s: %v\n", table, err)
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	fmt.Printf("-- Table: %s\n", table)
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		var vals []string
		for _, p := range ptrs {
			v := dePtr(p)
			if v == nil {
				vals = append(vals, "NULL")
			} else if s, ok := v.(string); ok {
				vals = append(vals, "'"+strings.ReplaceAll(s, "'", "''")+"'")
			} else {
				vals = append(vals, fmt.Sprintf("%v", v))
			}
		}
		fmt.Printf("INSERT INTO %s (%s) VALUES (%s);\n",
			table, strings.Join(cols, ", "), strings.Join(vals, ", "))
	}
}

// sanitizeIdent makes a string safe for use as a SQL identifier.
func sanitizeIdent(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	for i, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			b.WriteRune(c)
		} else if c >= '0' && c <= '9' {
			if i == 0 {
				b.WriteByte('_')
			}
			b.WriteRune(c)
		} else {
			b.WriteByte('_')
		}
	}
	result := strings.ToLower(b.String())
	if result == "" {
		return "col"
	}
	return result
}

// ---- output helpers ---------------------------------------------------------

//nolint:gocyclo // Printer handles formatting, scanning, and alignment in one pass.
func printRows(out io.Writer, rows *sql.Rows, cols []string) {
	data := scanRows(rows, cols)
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

// printRowsCSV writes rows as CSV.
func printRowsCSV(out io.Writer, rows *sql.Rows, cols []string) {
	data := scanRows(rows, cols)
	w := csv.NewWriter(out)
	w.Write(cols)
	for _, r := range data {
		var record []string
		for _, c := range cols {
			v := r[c]
			if v == nil {
				record = append(record, "")
			} else {
				record = append(record, fmt.Sprintf("%v", v))
			}
		}
		w.Write(record)
	}
	w.Flush()
}

// printRowsJSON writes rows as a JSON array.
func printRowsJSON(out io.Writer, rows *sql.Rows, cols []string) {
	data := scanRows(rows, cols)
	var jsonData []map[string]any
	for _, r := range data {
		m := make(map[string]any)
		for _, c := range cols {
			m[c] = r[c]
		}
		jsonData = append(jsonData, m)
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	enc.Encode(jsonData)
}

// scanRows reads all rows into a slice of maps.
func scanRows(rows *sql.Rows, cols []string) []map[string]any {
	type rowMap = map[string]any
	var data []rowMap
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		m := rowMap{}
		for i, c := range cols {
			m[c] = dePtr(ptrs[i])
		}
		data = append(data, m)
	}
	return data
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
