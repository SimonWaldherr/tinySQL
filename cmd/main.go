package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "tinysql/internal/driver"
)

var flagDemo = flag.Bool("demo", false, "run built-in demo instead of REPL")
var flagDSN = flag.String("dsn", "mem://?tenant=default", "DSN (mem:// or file:/path.db?tenant=...&autosave=1)")

func main() {
	flag.Parse()

	db, err := sql.Open("tinysql", *flagDSN)
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	if *flagDemo {
		runDemo(db)
		return
	}
	runREPL(db)
}

func runDemo(db *sql.DB) {
	exec := func(q string, args ...any) {
		fmt.Println("SQL>", q)
		up := strings.ToUpper(strings.TrimSpace(q))
		if strings.HasPrefix(up, "SELECT") {
			rows, err := db.Query(q, args...)
			if err != nil {
				fmt.Println("ERR:", err)
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
		fmt.Println()
	}

	// Schema
	exec(`CREATE TABLE users (id INT, name TEXT, email TEXT, active BOOL)`)
	exec(`CREATE TABLE orders (id INT, user_id INT, amount FLOAT, status TEXT, meta JSON)`)

	// Inserts
	exec(`INSERT INTO users (id, name, email, active) VALUES (1, 'Alice', 'alice@example.com', TRUE)`)
	exec(`INSERT INTO users (id, name, email, active) VALUES (2, 'Bob', NULL, TRUE)`)
	exec(`INSERT INTO users (id, name, email, active) VALUES (3, 'Carol', 'carol@example.com', NULL)`)

	exec(`INSERT INTO orders VALUES (101, 1, 100.5, 'PAID', '{"device":"web","items":[{"sku":"A","qty":1}]}' )`)
	exec(`INSERT INTO orders VALUES (102, 1,  75.0, 'PAID', '{"device":"app","items":[{"sku":"B","qty":2}]}' )`)
	exec(`INSERT INTO orders VALUES (103, 2, 200.0, 'PAID', '{"device":"web"}' )`)
	exec(`INSERT INTO orders VALUES (104, 2,  20.0, 'CANCELED', NULL )`)

	// DISTINCT
	exec(`SELECT DISTINCT active FROM users ORDER BY active ASC`)

	// JSON
	exec(`SELECT id, JSON_GET(meta, 'device') AS device FROM orders ORDER BY id`)

	// JOIN + GROUP BY
	exec(`
		SELECT u.name AS user, SUM(o.amount) AS total, COUNT(*) AS cnt
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'PAID'
		GROUP BY u.name
		ORDER BY total DESC
	`)

	// RIGHT JOIN
	exec(`
		SELECT o.id AS order_id, u.name AS user
		FROM users u
		RIGHT OUTER JOIN orders o ON u.id = o.user_id
		ORDER BY order_id
	`)

	// Temp table
	exec(`
		CREATE TEMP TABLE big_spenders AS
		SELECT u.id AS user_id, u.name, SUM(o.amount) AS total
		FROM users u
		JOIN orders o ON u.id = o.user_id
		WHERE o.status = 'PAID'
		GROUP BY u.id, u.name
		HAVING SUM(o.amount) >= 150
	`)
	exec(`SELECT * FROM big_spenders ORDER BY total DESC`)

	// UPDATE/DELETE
	exec(`UPDATE users SET email = 'alice@new.example', active = TRUE WHERE name = 'Alice'`)
	exec(`DELETE FROM users WHERE active = FALSE`)
	exec(`SELECT * FROM users ORDER BY id`)
}

func runREPL(db *sql.DB) {
	fmt.Println("tinysql REPL (database/sql). Statement mit ';' beenden. '.help' für Hilfe.")
	sc := bufio.NewScanner(os.Stdin)
	var buf strings.Builder
	for {
		if buf.Len() == 0 {
			fmt.Print("sql> ")
		} else {
			fmt.Print(" ... ")
		}
		if !sc.Scan() {
			fmt.Println()
			return
		}
		line := strings.TrimSpace(sc.Text())
		if buf.Len() == 0 && strings.HasPrefix(line, ".") {
			if handleMeta(db, line) {
				continue
			}
		}
		buf.WriteString(line)
		if strings.HasSuffix(line, ";") {
			q := strings.TrimSpace(strings.TrimSuffix(buf.String(), ";"))
			buf.Reset()
			if q == "" {
				continue
			}
			up := strings.ToUpper(q)
			if strings.HasPrefix(up, "SELECT") {
				rows, err := db.Query(q)
				if err != nil {
					fmt.Println("ERR:", err)
					continue
				}
				defer rows.Close()
				cols, _ := rows.Columns()
				printRows(rows, cols)
			} else {
				if _, err := db.Exec(q); err != nil {
					fmt.Println("ERR:", err)
				} else {
					fmt.Println("(ok)")
				}
			}
		} else {
			buf.WriteString(" ")
		}
	}
}

func handleMeta(db *sql.DB, line string) bool {
	switch {
	case line == ".help":
		fmt.Println(`
.meta:
  .help                 Hilfe
  .quit                 Beenden`)
		return true
	case line == ".quit":
		os.Exit(0)
	}
	return false
}

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
