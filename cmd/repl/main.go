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
var flagEcho = flag.Bool("echo", false, "Echo SQL statements before execution")
var flagFormat = flag.String("format", "table", "Output format: table, csv, tsv, json, yaml, markdown")

func main() {
	flag.Parse()

	db, err := sql.Open("tinysql", *flagDSN)
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	runREPL(db, *flagEcho, *flagFormat)
}

func runREPL(db *sql.DB, echo bool, format string) {
	fmt.Println("tinysql REPL (database/sql). Statement mit ';' beenden. '.help' für Hilfe.")
	sc := bufio.NewScanner(os.Stdin)
	var buf strings.Builder
	firstPrompt := true
	for {
		if buf.Len() == 0 {
			if !firstPrompt {
				fmt.Println()
			}
			firstPrompt = false
			fmt.Print("sql> ")
		} else {
			fmt.Print(" ... ")
		}
		if !sc.Scan() {
			fmt.Println()
			return
		}
		line := strings.TrimSpace(sc.Text())

		// Skip pure comment lines
		if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "/*") {
			continue
		}

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
			if echo {
				fmt.Println("--", q)
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
				printRows(rows, cols, format)
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

//nolint:gocyclo // REPL printer performs scanning, formatting, and alignment for display.
func printRows(rows *sql.Rows, cols []string, format string) {
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

	switch strings.ToLower(format) {
	case "json":
		printJSON(out)
	case "yaml":
		printYAML(out, cols)
	case "csv":
		printCSV(out, cols)
	case "tsv":
		printTSV(out, cols)
	case "markdown", "md":
		printMarkdown(out, cols)
	default:
		printTable(out, cols)
	}
}

func printTable(out []map[string]any, cols []string) {
	width := make([]int, len(cols))
	for i, c := range cols {
		width[i] = len(c)
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

func cell(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

func printJSON(out []map[string]any) {
	fmt.Println("[")
	for i, r := range out {
		fmt.Print("  {")
		j := 0
		for k, v := range r {
			if j > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("\"%s\": ", k)
			if v == nil {
				fmt.Print("null")
			} else if s, ok := v.(string); ok {
				fmt.Printf("\"%s\"", strings.ReplaceAll(s, "\"", "\\\""))
			} else {
				fmt.Printf("%v", v)
			}
			j++
		}
		fmt.Print("}")
		if i < len(out)-1 {
			fmt.Println(",")
		} else {
			fmt.Println()
		}
	}
	fmt.Println("]")
}

func printYAML(out []map[string]any, cols []string) {
	for i, r := range out {
		fmt.Printf("- ")
		for j, c := range cols {
			if j > 0 {
				fmt.Print("  ")
			}
			v := r[c]
			if v == nil {
				fmt.Printf("%s: null", c)
			} else if s, ok := v.(string); ok {
				fmt.Printf("%s: \"%s\"", c, s)
			} else {
				fmt.Printf("%s: %v", c, v)
			}
			if j < len(cols)-1 {
				fmt.Println()
			}
		}
		if i < len(out)-1 {
			fmt.Println()
		}
		fmt.Println()
	}
}

func printCSV(out []map[string]any, cols []string) {
	// Header
	for i, c := range cols {
		if i > 0 {
			fmt.Print(",")
		}
		if strings.ContainsAny(c, ",\"\n") {
			fmt.Printf("\"%s\"", strings.ReplaceAll(c, "\"", "\"\""))
		} else {
			fmt.Print(c)
		}
	}
	fmt.Println()

	// Rows
	for _, r := range out {
		for i, c := range cols {
			if i > 0 {
				fmt.Print(",")
			}
			v := r[c]
			s := cell(v)
			if strings.ContainsAny(s, ",\"\n") {
				fmt.Printf("\"%s\"", strings.ReplaceAll(s, "\"", "\"\""))
			} else {
				fmt.Print(s)
			}
		}
		fmt.Println()
	}
}

func printTSV(out []map[string]any, cols []string) {
	// Header
	for i, c := range cols {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(c)
	}
	fmt.Println()

	// Rows
	for _, r := range out {
		for i, c := range cols {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Print(cell(r[c]))
		}
		fmt.Println()
	}
}

func printMarkdown(out []map[string]any, cols []string) {
	// Calculate widths
	width := make([]int, len(cols))
	for i, c := range cols {
		width[i] = len(c)
	}
	for _, r := range out {
		for i, c := range cols {
			if w := len(cell(r[c])); w > width[i] {
				width[i] = w
			}
		}
	}

	// Header
	fmt.Print("|")
	for i, c := range cols {
		fmt.Print(" ")
		fmt.Print(padRight(c, width[i]))
		fmt.Print(" |")
	}
	fmt.Println()

	// Separator
	fmt.Print("|")
	for i := range cols {
		fmt.Print(strings.Repeat("-", width[i]+2))
		fmt.Print("|")
	}
	fmt.Println()

	// Rows
	for _, r := range out {
		fmt.Print("|")
		for i, c := range cols {
			fmt.Print(" ")
			fmt.Print(padRight(cell(r[c]), width[i]))
			fmt.Print(" |")
		}
		fmt.Println()
	}
}
