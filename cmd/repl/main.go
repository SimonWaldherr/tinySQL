package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/SimonWaldherr/tinySQL/internal/driver"
)

var flagDSN = flag.String("dsn", "mem://?tenant=default", "DSN (mem:// or file:/path.db?tenant=...&autosave=1)")

func main() {
	flag.Parse()

	db, err := sql.Open("tinysql", *flagDSN)
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	runREPL(db)
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
