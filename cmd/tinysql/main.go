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
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Config holds the runtime configuration.
type Config struct {
	Tenant    string
	Output    string
	Header    bool
	Echo      bool
	Batch     bool
	Timer     bool
	NullValue string
	Mode      OutputMode
}

type OutputMode string

const (
	ModeColumn OutputMode = "column"
	ModeList   OutputMode = "list"
	ModeCSV    OutputMode = "csv"
	ModeJSON   OutputMode = "json"
	ModeTable  OutputMode = "table"
)

func main() {
	// 1. Check for "sqlite-utils" style subcommands first
	if len(os.Args) > 1 {
		if handled, err := tryUtilityCommand(os.Args[1], os.Args[2:]); handled {
			exitIfErr(err)
			return
		}
	}

	// 2. Run Main SQLite-compatible CLI
	if err := runCLI(os.Args[1:]); err != nil {
		exitIfErr(err)
	}
}

func exitIfErr(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}

// ---- Core CLI Logic ---------------------------------------------------------

func runCLI(args []string) error {
	fs := flag.NewFlagSet("tinysql", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: tinysql [OPTIONS] FILENAME [SQL]\n")
		fs.PrintDefaults()
	}

	var (
		tenant  = fs.String("tenant", "default", "Tenant/schema name")
		mode    = fs.String("mode", "column", "Output mode: column|list|csv|json|table")
		headers = fs.Bool("header", true, "Include column headers")
		echo    = fs.Bool("echo", false, "Echo SQL before execution")
		cmd     = fs.String("cmd", "", "Run specific SQL and exit")
		batch   = fs.Bool("batch", false, "Force batch mode")
		outFile = fs.String("output", "", "Write output to file")
	)

	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := &Config{
		Tenant:    *tenant,
		Output:    *outFile,
		Header:    *headers,
		Echo:      *echo,
		Batch:     *batch,
		Mode:      OutputMode(*mode),
		NullValue: "", // default empty for column mode, usually
	}

	// Determine Database Path
	remaining := fs.Args()
	dbPath := ":memory:"
	inlineSQL := ""

	if len(remaining) >= 1 {
		dbPath = remaining[0]
	}
	if len(remaining) > 1 {
		inlineSQL = strings.Join(remaining[1:], " ")
	}

	// Initialize Database
	db, savePath, err := openDatabase(dbPath)
	if err != nil {
		return err
	}
	defer func() {
		if savePath != "" {
			_ = tsql.SaveToFile(db, savePath)
		}
	}()

	// Setup Output Writer
	var out io.Writer = os.Stdout
	if cfg.Output != "" {
		f, err := os.Create(cfg.Output)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}

	// Execution Helper
	exec := func(sql string) error {
		if strings.TrimSpace(sql) == "" {
			return nil
		}
		dirty, err := execute(context.Background(), db, cfg, sql, out)
		if dirty && savePath != "" && err == nil {
			return tsql.SaveToFile(db, savePath)
		}
		return err
	}

	// Scenario A: -cmd flag
	if *cmd != "" {
		return exec(*cmd)
	}

	// Scenario B: Inline SQL argument
	if inlineSQL != "" {
		return exec(inlineSQL)
	}

	// Scenario C: Piped Input (Stdin)
	if isInputPiped() {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		return exec(string(data))
	}

	// Scenario D: Batch mode requested but no input
	if cfg.Batch {
		return errors.New("batch mode requested but no SQL provided")
	}

	// Scenario E: Interactive Shell (REPL)
	repl := NewRepl(db, cfg, savePath, out)
	return repl.Run()
}

// ---- REPL (Interactive Shell) -----------------------------------------------

type Repl struct {
	db       *tsql.DB
	cfg      *Config
	savePath string
	out      io.Writer
	buf      strings.Builder
}

func NewRepl(db *tsql.DB, cfg *Config, savePath string, out io.Writer) *Repl {
	return &Repl{
		db:       db,
		cfg:      cfg,
		savePath: savePath,
		out:      out,
	}
}

func (r *Repl) Run() error {
	fmt.Fprintf(r.out, "TinySQL version 0.2 (mimicking sqlite3)\n")
	fmt.Fprintf(r.out, "Enter \".help\" for usage hints.\n")
	fmt.Fprintf(r.out, "Connected to: %s\n", r.savePath)

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB buffer

	// Handle Ctrl+C gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		for range sigChan {
			// If user hits Ctrl+C, reset buffer or exit if empty
			if r.buf.Len() > 0 {
				fmt.Fprintln(r.out, "^C")
				r.buf.Reset()
				fmt.Fprint(r.out, "tinysql> ")
			} else {
				os.Exit(0)
			}
		}
	}()

	r.printPrompt()

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Meta commands (only processed if buffer is empty)
		if r.buf.Len() == 0 && strings.HasPrefix(trimmed, ".") {
			if err := r.handleMeta(trimmed); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			}
			r.printPrompt()
			continue
		}

		r.buf.WriteString(line)
		r.buf.WriteByte('\n')

		if strings.HasSuffix(trimmed, ";") {
			sqlText := r.buf.String()
			r.buf.Reset()

			dirty, err := execute(context.Background(), r.db, r.cfg, sqlText, r.out)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			}
			if dirty && r.savePath != "" {
				if err := tsql.SaveToFile(r.db, r.savePath); err != nil {
					fmt.Fprintf(os.Stderr, "Auto-save failed: %v\n", err)
				}
			}
		}
		r.printPrompt()
	}
	return scanner.Err()
}

func (r *Repl) printPrompt() {
	if r.buf.Len() == 0 {
		fmt.Fprint(r.out, "tinysql> ")
	} else {
		fmt.Fprint(r.out, "   ...> ")
	}
}

func (r *Repl) handleMeta(line string) error {
	parts := strings.Fields(line)
	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case ".help":
		printHelp(r.out)
	case ".quit", ".exit":
		os.Exit(0)
	case ".tables":
		printTables(r.out, r.db, r.cfg.Tenant)
	case ".schema":
		target := ""
		if len(args) > 0 {
			target = args[0]
		}
		return printSchema(r.out, r.db, r.cfg.Tenant, target)
	case ".mode":
		if len(args) < 1 {
			return errors.New("usage: .mode MODE")
		}
		r.cfg.Mode = OutputMode(args[0])
	case ".headers":
		if len(args) < 1 {
			return errors.New("usage: .headers on|off")
		}
		r.cfg.Header = (args[0] == "on")
	case ".timer":
		if len(args) < 1 {
			return errors.New("usage: .timer on|off")
		}
		r.cfg.Timer = (args[0] == "on")
	case ".nullvalue":
		if len(args) < 1 {
			return errors.New("usage: .nullvalue STRING")
		}
		r.cfg.NullValue = args[0]
	case ".read":
		if len(args) < 1 {
			return errors.New("usage: .read FILE")
		}
		data, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		_, err = execute(context.Background(), r.db, r.cfg, string(data), r.out)
		return err
	case ".save":
		if len(args) < 1 {
			return errors.New("usage: .save FILE")
		}
		return tsql.SaveToFile(r.db, args[0])
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
	return nil
}

func printHelp(out io.Writer) {
	fmt.Fprintln(out, `
.exit                  Exit this program
.headers on|off        Turn display of headers on or off
.help                  Show this message
.mode MODE             Set output mode (column, list, csv, json, table)
.nullvalue STRING      Use STRING in place of NULL values
.read FILENAME         Execute SQL in FILENAME
.save FILENAME         Write in-memory database into FILENAME
.schema ?TABLE?        Show the CREATE statements
.tables                List names of tables
.timer on|off          Turn SQL timer on or off`)
}

// ---- Execution Engine -------------------------------------------------------

func execute(ctx context.Context, db *tsql.DB, cfg *Config, sqlText string, out io.Writer) (bool, error) {
	stmts := splitStatements(sqlText)
	dirty := false

	for _, stmtSQL := range stmts {
		if cfg.Echo {
			fmt.Fprintln(out, stmtSQL)
		}

		start := time.Now()
		parsed, err := tsql.ParseSQL(stmtSQL)
		if err != nil {
			return dirty, err
		}

		res, err := tsql.Execute(ctx, db, cfg.Tenant, parsed)
		duration := time.Since(start)

		if err != nil {
			return dirty, err
		}

		if res == nil {
			// DDL or INSERT/UPDATE usually returns nil ResultSet in tinySQL
			dirty = true
		} else {
			// It's a query
			printer := getPrinter(cfg.Mode)
			if err := printer.Print(out, res, cfg); err != nil {
				return dirty, err
			}
		}

		if cfg.Timer {
			fmt.Fprintf(out, "Run Time: real %.3fs\n", duration.Seconds())
		}
	}
	return dirty, nil
}

// ---- Output Formatters ------------------------------------------------------

type Printer interface {
	Print(w io.Writer, rs *tsql.ResultSet, cfg *Config) error
}

func getPrinter(mode OutputMode) Printer {
	switch mode {
	case ModeCSV:
		return &CSVPrinter{}
	case ModeJSON:
		return &JSONPrinter{}
	case ModeList:
		return &ListPrinter{}
	case ModeColumn, ModeTable:
		return &ColumnPrinter{}
	default:
		return &ListPrinter{}
	}
}

// ColumnPrinter uses tabwriter for aligned output
type ColumnPrinter struct{}

func (cp *ColumnPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)

	// Headers
	if cfg.Header {
		for i, col := range rs.Cols {
			fmt.Fprint(w, col)
			if i < len(rs.Cols)-1 {
				fmt.Fprint(w, "\t")
			}
		}
		fmt.Fprintln(w)
		// Separator line
		for i, col := range rs.Cols {
			fmt.Fprint(w, strings.Repeat("-", len(col)))
			if i < len(rs.Cols)-1 {
				fmt.Fprint(w, "\t")
			}
		}
		fmt.Fprintln(w)
	}

	// Data
	for _, row := range rs.Rows {
		for i, col := range rs.Cols {
			val := row[strings.ToLower(col)]
			fmt.Fprint(w, fmtScalar(val, cfg.NullValue))
			if i < len(rs.Cols)-1 {
				fmt.Fprint(w, "\t")
			}
		}
		fmt.Fprintln(w)
	}
	return w.Flush()
}

type ListPrinter struct{}

func (lp *ListPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	for _, row := range rs.Rows {
		for i, col := range rs.Cols {
			if i > 0 {
				fmt.Fprint(out, "|")
			}
			val := row[strings.ToLower(col)]
			fmt.Fprint(out, fmtScalar(val, cfg.NullValue))
		}
		fmt.Fprintln(out)
	}
	return nil
}

type CSVPrinter struct{}

func (cp *CSVPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	w := csv.NewWriter(out)
	if cfg.Header {
		if err := w.Write(rs.Cols); err != nil {
			return err
		}
	}
	for _, row := range rs.Rows {
		record := make([]string, len(rs.Cols))
		for i, col := range rs.Cols {
			val := row[strings.ToLower(col)]
			record[i] = fmtScalar(val, "")
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

type JSONPrinter struct{}

func (jp *JSONPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	output := make([]map[string]any, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		item := make(map[string]any)
		for _, col := range rs.Cols {
			item[col] = row[strings.ToLower(col)]
		}
		output = append(output, item)
	}
	return enc.Encode(output)
}

// ---- Helpers ----------------------------------------------------------------

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
			_ = os.MkdirAll(dir, 0o755)
		}
		return tsql.NewDB(), path, nil
	} else {
		return nil, "", err
	}
}

func fmtScalar(v any, nullVal string) string {
	if v == nil {
		return nullVal
	}
	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339)
	case []byte:
		return string(val)
	case float64:
		// Attempt to format integers cleanly if they have no decimal part
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10)
		}
		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func isInputPiped() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) == 0
}

func printTables(out io.Writer, db *tsql.DB, tenant string) {
	tables := db.ListTables(tenant)
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Name
	}
	sort.Strings(names)

	// sqlite3 prints tables in columns, but simple list is okay for now
	// To strictly mimic sqlite3 column-wrapping for .tables is complex
	// without terminal width detection, so we print space-separated.
	for _, name := range names {
		fmt.Fprintf(out, "%s  ", name)
	}
	fmt.Fprintln(out)
}

func printSchema(out io.Writer, db *tsql.DB, tenant, tableFilter string) error {
	tables := db.ListTables(tenant)
	for _, t := range tables {
		if tableFilter != "" && !strings.EqualFold(t.Name, tableFilter) {
			continue
		}
		fullTable, err := db.Get(tenant, t.Name)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "CREATE TABLE %s (\n", fullTable.Name)
		for i, col := range fullTable.Cols {
			def := fmt.Sprintf("  %s %s", col.Name, col.Type)
			if col.Constraint == storage.PrimaryKey {
				def += " PRIMARY KEY"
			}
			if i < len(fullTable.Cols)-1 {
				def += ","
			}
			fmt.Fprintln(out, def)
		}
		fmt.Fprintln(out, ");")
	}
	return nil
}

// splitStatements is a simple state-machine splitter.
// Ideally, use a proper lexer, but this suffices for a CLI wrapper.
func splitStatements(sql string) []string {
	var stmts []string
	var buf strings.Builder
	inSingle := false
	inDouble := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case ';':
			if !inSingle && !inDouble {
				s := strings.TrimSpace(buf.String())
				if s != "" {
					stmts = append(stmts, s)
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

// ---- Legacy Utility Commands (sqlite-utils style) ---------------------------

func tryUtilityCommand(name string, args []string) (bool, error) {
	switch name {
	case "tables":
		return true, runTablesUtil(args)
	case "schema":
		return true, runSchemaUtil(args)
	case "query":
		return true, runQueryUtil(args)
	case "insert":
		return true, runInsertUtil(args)
	default:
		return false, nil
	}
}

func runTablesUtil(args []string) error {
	fs := flag.NewFlagSet("tables", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant")
	jsonOut := fs.Bool("json", false, "Emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
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
	for _, n := range names {
		fmt.Println(n)
	}
	return nil
}

func runSchemaUtil(args []string) error {
	fs := flag.NewFlagSet("schema", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant")
	if err := fs.Parse(args); err != nil {
		return err
	}
	db, _, err := openDatabase(fs.Arg(0))
	if err != nil {
		return err
	}
	return printSchema(os.Stdout, db, *tenant, "")
}

func runQueryUtil(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant")
	mode := fs.String("mode", "table", "Mode")
	if err := fs.Parse(args); err != nil {
		return err
	}

	db, _, err := openDatabase(fs.Arg(0))
	if err != nil {
		return err
	}

	sql := strings.Join(fs.Args()[1:], " ")
	cfg := &Config{Tenant: *tenant, Mode: OutputMode(*mode), Header: true}
	_, err = execute(context.Background(), db, cfg, sql, os.Stdout)
	return err
}

func runInsertUtil(args []string) error {
	fs := flag.NewFlagSet("insert", flag.ExitOnError)
	tenant := fs.String("tenant", "default", "Tenant")
	if err := fs.Parse(args); err != nil || fs.NArg() < 3 {
		return errors.New("usage: insert <db> <table> <json...>")
	}

	db, path, err := openDatabase(fs.Arg(0))
	if err != nil {
		return err
	}

	table := fs.Arg(1)
	count := 0
	for _, rowJSON := range fs.Args()[2:] {
		// Import assumes simple JSON object. Using tsql.ParseSQL for INSERTs is safer if we construct them manually
		// For brevity, we reconstruct the INSERT statement
		var data map[string]any
		if err := json.Unmarshal([]byte(rowJSON), &data); err != nil {
			return err
		}

		cols := make([]string, 0, len(data))
		vals := make([]string, 0, len(data))
		for k, v := range data {
			cols = append(cols, k)
			// Simple escaping
			valStr := fmt.Sprintf("'%v'", v)
			if str, ok := v.(string); ok {
				valStr = "'" + strings.ReplaceAll(str, "'", "''") + "'"
			} else if v == nil {
				valStr = "NULL"
			}
			vals = append(vals, valStr)
		}

		sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), strings.Join(vals, ","))
		if _, err := execute(context.Background(), db, &Config{Tenant: *tenant}, sql, io.Discard); err != nil {
			return err
		}
		count++
	}
	if path != "" {
		tsql.SaveToFile(db, path)
	}
	fmt.Printf("Inserted %d rows.\n", count)
	return nil
}
