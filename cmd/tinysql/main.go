package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type outputMode string

const (
	modeColumn outputMode = "column"
	modeList   outputMode = "list"
	modeCSV    outputMode = "csv"
	modeJSON   outputMode = "json"
	modeTable  outputMode = "table"
)

func main() {
	if len(os.Args) > 1 {
		if handled, err := tryUtilityCommand(os.Args[1], os.Args[2:]); handled {
			exitIfErr(err)
			return
		}
	}
	exitIfErr(runSQLiteCompat(os.Args[1:]))
}

func exitIfErr(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, "tinysql:", err)
	os.Exit(1)
}

// tryUtilityCommand executes sqlite-utils style subcommands when detected.
func tryUtilityCommand(name string, args []string) (bool, error) {
	switch name {
	case "tables":
		return true, runTablesCommand(args)
	case "schema":
		return true, runSchemaCommand(args)
	case "query":
		return true, runQueryCommand(args)
	case "rows":
		return true, runRowsCommand(args)
	case "insert":
		return true, runInsertCommand(args)
	default:
		return false, nil
	}
}

func runSQLiteCompat(args []string) error {
	fs := flag.NewFlagSet("tinysql", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	tenant := fs.String("tenant", "default", "Tenant/schema name")
	mode := fs.String("mode", string(modeColumn), "Output mode: column|list|csv|json|table")
	headers := fs.Bool("header", true, "Include column headers in result output")
	echo := fs.Bool("echo", false, "Echo SQL statements before execution")
	cmd := fs.String("cmd", "", "Execute the provided SQL then exit")
	batch := fs.Bool("batch", false, "Batch mode (non-interactive; fail when no SQL provided)")
	outFile := fs.String("output", "", "Write query output to file instead of stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	remaining := fs.Args()
	dbPath := ":memory:"
	var inlineSQL string
	switch len(remaining) {
	case 0:
		// in-memory DB
	case 1:
		dbPath = remaining[0]
	default:
		dbPath = remaining[0]
		inlineSQL = strings.Join(remaining[1:], " ")
	}

	db, pathForSave, err := openDatabase(dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if pathForSave != "" {
			_ = tsql.SaveToFile(db, pathForSave)
		}
	}()

	writer := io.Writer(os.Stdout)
	if *outFile != "" {
		f, err := os.Create(*outFile)
		if err != nil {
			return err
		}
		defer f.Close()
		writer = f
	}

	run := func(sqlText string) error {
		if strings.TrimSpace(sqlText) == "" {
			return nil
		}
		dirty, err := executeSQLStatements(context.Background(), db, *tenant, sqlText, outputMode(*mode), *headers, *echo, writer)
		if err != nil {
			return err
		}
		if dirty && pathForSave != "" {
			return tsql.SaveToFile(db, pathForSave)
		}
		return nil
	}

	if *cmd != "" {
		return run(*cmd)
	}
	if inlineSQL != "" {
		return run(inlineSQL)
	}

	if info, err := os.Stdin.Stat(); err == nil && (info.Mode()&os.ModeCharDevice) == 0 {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		return run(string(data))
	}

	if *batch {
		return errors.New("no SQL supplied in batch mode")
	}

	return startShell(db, pathForSave, *tenant, outputMode(*mode), *headers, writer)
}

func openDatabase(path string) (*tsql.DB, string, error) {
	if path == "" || path == ":memory:" {
		return tsql.NewDB(), "", nil
	}
	if _, err := os.Stat(path); err == nil {
		db, err := tsql.LoadFromFile(path)
		return db, path, err
	} else if errors.Is(err, os.ErrNotExist) {
		dir := filepath.Dir(path)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, "", err
			}
		}
		return tsql.NewDB(), path, nil
	} else {
		return nil, "", err
	}
}

func executeSQLStatements(ctx context.Context, db *tsql.DB, tenant string, sqlText string, mode outputMode, headers bool, echo bool, out io.Writer) (bool, error) {
	stmts := splitStatements(sqlText)
	if len(stmts) == 0 {
		return false, nil
	}
	dirty := false
	for _, raw := range stmts {
		if echo {
			fmt.Fprintln(out, raw)
		}
		stmt, err := tsql.ParseSQL(raw)
		if err != nil {
			return dirty, err
		}
		rs, err := tsql.Execute(ctx, db, tenant, stmt)
		if err != nil {
			return dirty, err
		}
		if rs == nil {
			dirty = true
			fmt.Fprintln(out, "OK")
			continue
		}
		if err := renderResultSet(out, rs, mode, headers); err != nil {
			return dirty, err
		}
	}
	return dirty, nil
}

func splitStatements(sql string) []string {
	var stmts []string
	var buf strings.Builder
	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		var next byte
		if i+1 < len(sql) {
			next = sql[i+1]
		}
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			buf.WriteByte(ch)
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				inBlockComment = false
				buf.WriteByte(ch)
				i++
				buf.WriteByte('/')
				continue
			}
			buf.WriteByte(ch)
			continue
		}
		if !inSingle && !inDouble && !inBacktick {
			if ch == '-' && next == '-' {
				inLineComment = true
				buf.WriteByte(ch)
				i++
				buf.WriteByte('-')
				continue
			}
			if ch == '/' && next == '*' {
				inBlockComment = true
				buf.WriteByte(ch)
				i++
				buf.WriteByte('*')
				continue
			}
		}
		switch ch {
		case '\'':
			if !inDouble && !inBacktick {
				if inSingle && next == '\'' {
					buf.WriteByte(ch)
					i++
					buf.WriteByte('\'')
					continue
				}
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case ';':
			if !inSingle && !inDouble && !inBacktick {
				stmt := strings.TrimSpace(buf.String())
				if stmt != "" {
					stmts = append(stmts, stmt)
				}
				buf.Reset()
				continue
			}
		}
		buf.WriteByte(ch)
	}
	if s := strings.TrimSpace(buf.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

func renderResultSet(out io.Writer, rs *tsql.ResultSet, mode outputMode, headers bool) error {
	switch mode {
	case modeCSV:
		return renderCSV(out, rs, headers)
	case modeJSON:
		return renderJSON(out, rs)
	case modeList:
		return renderList(out, rs, headers)
	case modeTable:
		return renderTable(out, rs, true)
	default:
		return renderColumn(out, rs, headers)
	}
}

func renderColumn(out io.Writer, rs *tsql.ResultSet, headers bool) error {
	widths := make([]int, len(rs.Cols))
	formatter := func(v any) string {
		if v == nil {
			return "NULL"
		}
		switch val := v.(type) {
		case time.Time:
			return val.Format(time.RFC3339)
		case []byte:
			return string(val)
		default:
			return fmt.Sprintf("%v", val)
		}
	}
	if headers {
		for i, c := range rs.Cols {
			widths[i] = len(c)
		}
	}
	for _, row := range rs.Rows {
		for i, c := range rs.Cols {
			val := formatter(row[strings.ToLower(c)])
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}
	if headers {
		for i, c := range rs.Cols {
			fmt.Fprintf(out, "%s  ", padRight(c, widths[i]))
		}
		fmt.Fprintln(out)
		for i := range rs.Cols {
			fmt.Fprintf(out, "%s  ", strings.Repeat("-", widths[i]))
		}
		fmt.Fprintln(out)
	}
	for _, row := range rs.Rows {
		for i, c := range rs.Cols {
			fmt.Fprintf(out, "%s  ", padRight(formatter(row[strings.ToLower(c)]), widths[i]))
		}
		fmt.Fprintln(out)
	}
	return nil
}

func renderTable(out io.Writer, rs *tsql.ResultSet, headers bool) error {
	return renderColumn(out, rs, headers)
}

func renderList(out io.Writer, rs *tsql.ResultSet, headers bool) error {
	for _, row := range rs.Rows {
		for i, c := range rs.Cols {
			if i > 0 {
				fmt.Fprint(out, "|")
			}
			if headers {
				fmt.Fprintf(out, "%s=%v", c, row[strings.ToLower(c)])
			} else {
				fmt.Fprintf(out, "%v", row[strings.ToLower(c)])
			}
		}
		fmt.Fprintln(out)
	}
	return nil
}

func renderCSV(out io.Writer, rs *tsql.ResultSet, headers bool) error {
	w := csv.NewWriter(out)
	if headers {
		if err := w.Write(rs.Cols); err != nil {
			return err
		}
	}
	for _, row := range rs.Rows {
		record := make([]string, len(rs.Cols))
		for i, c := range rs.Cols {
			record[i] = formatScalar(row[strings.ToLower(c)])
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func renderJSON(out io.Writer, rs *tsql.ResultSet) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	rows := make([]map[string]any, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		obj := make(map[string]any)
		for _, c := range rs.Cols {
			obj[c] = row[strings.ToLower(c)]
		}
		rows = append(rows, obj)
	}
	return enc.Encode(rows)
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func formatScalar(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339)
	case fmt.Stringer:
		return val.String()
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

type shellState struct {
	db       *tsql.DB
	tenant   string
	mode     outputMode
	headers  bool
	savePath string
	writer   io.Writer
}

func startShell(db *tsql.DB, savePath, tenant string, mode outputMode, headers bool, writer io.Writer) error {
	state := &shellState{db: db, tenant: tenant, mode: mode, headers: headers, savePath: savePath, writer: writer}
	fmt.Fprintf(os.Stdout, "tinysql shell (tenant=%s). Statements end with ';'. Type .help for help.\n", tenant)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var buf strings.Builder
	for {
		if buf.Len() == 0 {
			fmt.Fprint(os.Stdout, "tinysql> ")
		} else {
			fmt.Fprint(os.Stdout, "   ...> ")
		}
		if !scanner.Scan() {
			fmt.Fprintln(os.Stdout)
			return nil
		}
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if buf.Len() == 0 && strings.HasPrefix(trimmed, ".") {
			if err := state.handleMeta(trimmed); err != nil {
				fmt.Fprintln(os.Stdout, "error:", err)
			}
			continue
		}
		buf.WriteString(line)
		buf.WriteByte('\n')
		if strings.HasSuffix(trimmed, ";") {
			sqlText := buf.String()
			buf.Reset()
			dirty, err := executeSQLStatements(context.Background(), state.db, state.tenant, sqlText, state.mode, state.headers, false, state.writer)
			if err != nil {
				fmt.Fprintln(os.Stdout, "error:", err)
				continue
			}
			if dirty && state.savePath != "" {
				if err := tsql.SaveToFile(state.db, state.savePath); err != nil {
					fmt.Fprintln(os.Stdout, "save error:", err)
				}
			}
		}
	}
}

func (s *shellState) handleMeta(line string) error {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}
	switch fields[0] {
	case ".help":
		fmt.Fprintln(s.writer, `.help                  Show this help`)
		fmt.Fprintln(s.writer, `.tables                List tables in the current tenant`)
		fmt.Fprintln(s.writer, `.schema [table]        Display CREATE TABLE statement(s)`)
		fmt.Fprintln(s.writer, `.mode MODE             Set output mode (column,list,csv,json,table)`)
		fmt.Fprintln(s.writer, `.headers on|off        Toggle column headers`)
		fmt.Fprintln(s.writer, `.read FILE             Execute SQL from file`)
		fmt.Fprintln(s.writer, `.save FILE             Save database to file`)
		fmt.Fprintln(s.writer, `.import FILE TABLE     Import data via auto-detected format`)
		fmt.Fprintln(s.writer, `.quit/.exit            Exit the shell`)
	case ".quit", ".exit":
		os.Exit(0)
	case ".tables":
		tables := s.db.ListTables(s.tenant)
		if len(tables) == 0 {
			fmt.Fprintln(s.writer, "(no tables)")
			return nil
		}
		names := make([]string, len(tables))
		for i, t := range tables {
			names[i] = t.Name
		}
		sort.Strings(names)
		fmt.Fprintln(s.writer, strings.Join(names, " "))
	case ".schema":
		var target string
		if len(fields) > 1 {
			target = fields[1]
		}
		return printSchema(s.writer, s.db, s.tenant, target)
	case ".mode":
		if len(fields) != 2 {
			return errors.New("usage: .mode MODE")
		}
		s.mode = outputMode(strings.ToLower(fields[1]))
		fmt.Fprintln(s.writer, "mode set to", s.mode)
	case ".headers":
		if len(fields) != 2 {
			return errors.New("usage: .headers on|off")
		}
		switch strings.ToLower(fields[1]) {
		case "on":
			s.headers = true
		case "off":
			s.headers = false
		default:
			return errors.New("expected on or off")
		}
		fmt.Fprintf(s.writer, "headers %s\n", map[bool]string{true: "on", false: "off"}[s.headers])
	case ".read":
		if len(fields) != 2 {
			return errors.New("usage: .read FILE")
		}
		data, err := os.ReadFile(fields[1])
		if err != nil {
			return err
		}
		dirty, err := executeSQLStatements(context.Background(), s.db, s.tenant, string(data), s.mode, s.headers, false, s.writer)
		if err != nil {
			return err
		}
		if dirty && s.savePath != "" {
			return tsql.SaveToFile(s.db, s.savePath)
		}
	case ".save":
		if len(fields) != 2 {
			return errors.New("usage: .save FILE")
		}
		if err := tsql.SaveToFile(s.db, fields[1]); err != nil {
			return err
		}
		s.savePath = fields[1]
	case ".import":
		if len(fields) != 3 {
			return errors.New("usage: .import FILE TABLE")
		}
		if _, err := tsql.ImportFile(context.Background(), s.db, s.tenant, fields[2], fields[1], &tsql.ImportOptions{CreateTable: true, TypeInference: true}); err != nil {
			return err
		}
		if s.savePath != "" {
			return tsql.SaveToFile(s.db, s.savePath)
		}
	default:
		return fmt.Errorf("unknown meta command %s", fields[0])
	}
	return nil
}

func printSchema(out io.Writer, db *tsql.DB, tenant, table string) error {
	tables := db.ListTables(tenant)
	if len(tables) == 0 {
		fmt.Fprintln(out, "(no tables)")
		return nil
	}
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		if table == "" || strings.EqualFold(table, t.Name) {
			names = append(names, t.Name)
		}
	}
	if len(names) == 0 {
		return fmt.Errorf("table %s not found", table)
	}
	sort.Strings(names)
	for _, name := range names {
		t, err := db.Get(tenant, name)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "CREATE TABLE %s (\n", name)
		for i, col := range t.Cols {
			parts := []string{fmt.Sprintf("  %s %s", col.Name, col.Type.String())}
			switch col.Constraint {
			case storage.PrimaryKey:
				parts = append(parts, "PRIMARY KEY")
			case storage.Unique:
				parts = append(parts, "UNIQUE")
			case storage.ForeignKey:
				if col.ForeignKey != nil {
					parts = append(parts, fmt.Sprintf("REFERENCES %s(%s)", col.ForeignKey.Table, col.ForeignKey.Column))
				}
			}
			line := strings.Join(parts, " ")
			if i < len(t.Cols)-1 {
				line += ","
			}
			fmt.Fprintln(out, line)
		}
		fmt.Fprintln(out, ");")
		fmt.Fprintln(out)
	}
	return nil
}

// ---- sqlite-utils style commands -------------------------------------------------

func runTablesCommand(args []string) error {
	fs := flag.NewFlagSet("tables", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant/schema name")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of plain text")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("usage: tinysql tables <database>")
	}
	db, _, err := openDatabase(fs.Arg(0))
	if err != nil {
		return err
	}
	tables := db.ListTables(*tenant)
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Name
	}
	sort.Strings(names)
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(names)
	}
	for _, name := range names {
		fmt.Println(name)
	}
	return nil
}

func runSchemaCommand(args []string) error {
	fs := flag.NewFlagSet("schema", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant/schema name")
	table := fs.String("table", "", "Specific table name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("usage: tinysql schema <database> [--table name]")
	}
	db, _, err := openDatabase(fs.Arg(0))
	if err != nil {
		return err
	}
	return printSchema(os.Stdout, db, *tenant, *table)
}

func runQueryCommand(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant/schema name")
	mode := fs.String("mode", string(modeTable), "Output mode")
	headers := fs.Bool("header", true, "Include column headers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return errors.New("usage: tinysql query <database> <sql>")
	}
	db, path, err := openDatabase(fs.Arg(0))
	if err != nil {
		return err
	}
	dirty, err := executeSQLStatements(context.Background(), db, *tenant, strings.Join(fs.Args()[1:], " "), outputMode(*mode), *headers, false, os.Stdout)
	if err != nil {
		return err
	}
	if dirty && path != "" {
		return tsql.SaveToFile(db, path)
	}
	return nil
}

func runRowsCommand(args []string) error {
	fs := flag.NewFlagSet("rows", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant/schema name")
	limit := fs.Int("limit", 50, "Row limit (0 = all)")
	jsonOut := fs.Bool("json", false, "Emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return errors.New("usage: tinysql rows <database> <table>")
	}
	db, _, err := openDatabase(fs.Arg(0))
	if err != nil {
		return err
	}
	sqlText := fmt.Sprintf("SELECT * FROM %s", fs.Arg(1))
	if *limit > 0 {
		sqlText += fmt.Sprintf(" LIMIT %d", *limit)
	}
	mode := modeTable
	if *jsonOut {
		mode = modeJSON
	}
	_, err = executeSQLStatements(context.Background(), db, *tenant, sqlText, mode, true, false, os.Stdout)
	return err
}

func runInsertCommand(args []string) error {
	fs := flag.NewFlagSet("insert", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant/schema name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 3 {
		return errors.New("usage: tinysql insert <database> <table> <json-row> [<json-row>...]")
	}
	db, path, err := openDatabase(fs.Arg(0))
	if err != nil {
		return err
	}
	table := fs.Arg(1)
	rows := fs.Args()[2:]
	for _, raw := range rows {
		obj, err := decodeJSONRow(raw)
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		vals := make([]string, len(keys))
		for i, k := range keys {
			vals[i] = encodeLiteral(obj[k])
		}
		sqlText := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", table, strings.Join(keys, ", "), strings.Join(vals, ", "))
		if _, err := executeSQLStatements(context.Background(), db, *tenant, sqlText, modeTable, true, false, io.Discard); err != nil {
			return err
		}
	}
	if path != "" {
		if err := tsql.SaveToFile(db, path); err != nil {
			return err
		}
	}
	fmt.Printf("Inserted %d row(s) into %s\n", len(rows), table)
	return nil
}

func decodeJSONRow(raw string) (map[string]any, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, fmt.Errorf("invalid JSON row: %w", err)
	}
	for k, v := range obj {
		switch num := v.(type) {
		case json.Number:
			if strings.Contains(num.String(), ".") {
				f, err := num.Float64()
				if err == nil {
					obj[k] = f
					continue
				}
			}
			if i, err := num.Int64(); err == nil {
				obj[k] = i
			} else {
				obj[k] = num.String()
			}
		}
	}
	return obj, nil
}

func encodeLiteral(v any) string {
	switch val := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case time.Time:
		return "'" + val.Format(time.RFC3339) + "'"
	default:
		b, _ := json.Marshal(val)
		return "'" + strings.ReplaceAll(string(b), "'", "''") + "'"
	}
}
