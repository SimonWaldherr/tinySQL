// debug – tinySQL SQL diagnostic tool.
//
// Parses and executes SQL statements against an in-memory tinySQL database,
// reporting the statement type, execution time, and results. Useful for
// quickly testing SQL compatibility and debugging query behaviour.
//
// SQL can be provided via the -sql flag, as positional arguments, or read
// from stdin when no flags are given.
//
// Usage:
//
//	debug -sql "SELECT 1 + 1 AS result"
//	debug "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t"
//	echo "SELECT 42 AS answer" | debug
//
// Flags:
//
//	-sql      SQL statement(s) to execute (semicolon-separated)
//	-dsn      Storage DSN (default: in-memory)
//	-timing   Print execution time for each statement
//	-verbose  Print statement type and extra diagnostics
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

var (
	flagSQL     = flag.String("sql", "", "SQL statement(s) to execute (semicolon-separated)")
	flagTiming  = flag.Bool("timing", true, "Print execution time for each statement")
	flagVerbose = flag.Bool("verbose", false, "Print statement type and extra diagnostics")
)

func main() {
	flag.Parse()

	input := *flagSQL

	// Accept positional arguments as SQL.
	if input == "" && flag.NArg() > 0 {
		input = strings.Join(flag.Args(), " ")
	}

	// Fall back to stdin when nothing else is provided.
	if input == "" {
		fi, _ := os.Stdin.Stat()
		if (fi.Mode() & os.ModeCharDevice) == 0 {
			b, err := io.ReadAll(bufio.NewReader(os.Stdin))
			if err != nil {
				fmt.Fprintln(os.Stderr, "read stdin:", err)
				os.Exit(1)
			}
			input = strings.TrimSpace(string(b))
		}
	}

	if input == "" {
		flag.Usage()
		os.Exit(0)
	}

	db := tinysql.NewDB()
	ctx := context.Background()

	stmts := splitStatements(input)
	if len(stmts) == 0 {
		fmt.Println("(no statements)")
		return
	}

	for i, raw := range stmts {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		fmt.Printf("[%d] SQL> %s\n", i+1, raw)

		start := time.Now()
		stmt, err := tinysql.ParseSQL(raw)
		if err != nil {
			fmt.Printf("    PARSE ERROR: %v\n\n", err)
			continue
		}

		if *flagVerbose {
			fmt.Printf("    type: %s\n", stmtType(stmt))
		}

		rs, err := tinysql.Execute(ctx, db, "default", stmt)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("    EXEC ERROR: %v\n", err)
		} else if rs != nil && len(rs.Rows) > 0 {
			printResultSet(rs)
		} else if rs != nil {
			fmt.Println("    (0 rows)")
		} else {
			fmt.Println("    (ok)")
		}

		if *flagTiming {
			fmt.Printf("    elapsed: %v\n", elapsed.Round(time.Microsecond))
		}
		fmt.Println()
	}
}

// stmtType returns a human-readable label for a parsed statement.
func stmtType(stmt tinysql.Statement) string {
	switch stmt.(type) {
	case *engine.Select:
		return "SELECT"
	case *engine.Insert:
		return "INSERT"
	case *engine.Update:
		return "UPDATE"
	case *engine.Delete:
		return "DELETE"
	case *engine.CreateTable:
		return "CREATE TABLE"
	case *engine.DropTable:
		return "DROP TABLE"
	case *engine.CreateIndex:
		return "CREATE INDEX"
	case *engine.DropIndex:
		return "DROP INDEX"
	case *engine.CreateView:
		return "CREATE VIEW"
	case *engine.DropView:
		return "DROP VIEW"
	case *engine.CreateJob:
		return "CREATE JOB"
	default:
		return fmt.Sprintf("%T", stmt)
	}
}

// splitStatements splits a SQL string on semicolons, respecting string literals.
// It handles both standard SQL doubled-quote escapes ('it''s') and
// backslash-escaped quotes ('it\'s').
func splitStatements(sql string) []string {
	var stmts []string
	var cur strings.Builder
	inStr := false
	var strChar rune

	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		switch {
		case inStr:
			cur.WriteRune(ch)
			if ch == '\\' && i+1 < len(runes) {
				// Backslash escape: consume the next character as-is.
				i++
				cur.WriteRune(runes[i])
			} else if ch == strChar {
				// Check for doubled-quote escape (e.g. '' inside '...').
				if i+1 < len(runes) && runes[i+1] == strChar {
					i++
					cur.WriteRune(runes[i])
				} else {
					inStr = false
				}
			}
		case ch == '\'' || ch == '"':
			inStr = true
			strChar = ch
			cur.WriteRune(ch)
		case ch == ';':
			if s := strings.TrimSpace(cur.String()); s != "" {
				stmts = append(stmts, s)
			}
			cur.Reset()
		default:
			cur.WriteRune(ch)
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// printResultSet prints a result set as an aligned table.
func printResultSet(rs *tinysql.ResultSet) {
	cols := rs.Cols
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	cell := func(v any) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("%v", v)
	}
	for _, row := range rs.Rows {
		for i, c := range cols {
			if w := len(cell(row[c])); w > widths[i] {
				widths[i] = w
			}
		}
	}

	pad := func(s string, w int) string {
		if len(s) >= w {
			return s
		}
		return s + strings.Repeat(" ", w-len(s))
	}

	var sb strings.Builder
	for i, c := range cols {
		sb.WriteString("    ")
		sb.WriteString(pad(c, widths[i]))
		if i < len(cols)-1 {
			sb.WriteString("  ")
		}
	}
	fmt.Println(sb.String())

	sb.Reset()
	for i, w := range widths {
		sb.WriteString("    ")
		sb.WriteString(strings.Repeat("-", w))
		if i < len(widths)-1 {
			sb.WriteString("  ")
		}
	}
	fmt.Println(sb.String())

	for _, row := range rs.Rows {
		sb.Reset()
		for i, c := range cols {
			sb.WriteString("    ")
			sb.WriteString(pad(cell(row[c]), widths[i]))
			if i < len(cols)-1 {
				sb.WriteString("  ")
			}
		}
		fmt.Println(sb.String())
	}
	fmt.Printf("    (%d row(s))\n", len(rs.Rows))
}
