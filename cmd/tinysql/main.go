package main

import (
	"bufio"
	"context"
	"encoding/base64"
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
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/exporter"
)

// Version is replaced at build time, for example with:
//
//	go build -ldflags "-X main.Version=v0.43.0" ./cmd/tinysql
//
// Keep a useful value for local `go run` and test builds where no linker flag
// is supplied.
var Version = "dev"

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
	// GeomCol names the geometry column for ModeGeoJSON/ModeTopoJSON.
	// Empty auto-detects it (see exporter.ExportGeoJSON/ExportTopoJSON).
	GeomCol string
}

type OutputMode string

const (
	ModeColumn   OutputMode = "column"
	ModeList     OutputMode = "list"
	ModeCSV      OutputMode = "csv"
	ModeJSON     OutputMode = "json"
	ModeJSONL    OutputMode = "jsonl"
	ModeNDJSON   OutputMode = "ndjson"
	ModeTable    OutputMode = "table"
	ModeGeoJSON  OutputMode = "geojson"
	ModeTopoJSON OutputMode = "topojson"
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

// parseCLIFlags parses args and reports whether the caller should return.
//
// -h and --help are requests, not failures: the flag package has already
// written the usage text by the time it returns ErrHelp, so propagating that
// error would append "Error: flag: help requested" and exit 1.
func parseCLIFlags(fs *flag.FlagSet, args []string) (done bool, err error) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return true, nil
		}
		return true, err
	}
	return false, nil
}

func exitIfErr(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "Interrupted.")
		os.Exit(130)
	}
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}

// ---- Core CLI Logic ---------------------------------------------------------

func runCLI(args []string) (runErr error) {
	if len(args) == 1 && args[0] == "version" {
		return printVersion(os.Stdout)
	}
	fs := flag.NewFlagSet("tinysql", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: tinysql [OPTIONS] [DATABASE] [SQL]\n\n")
		fmt.Fprintf(fs.Output(), "Subcommands:\n"+
			"  shell, repl <database>        Interactive shell (same flags as above)\n"+
			"  tables [flags] <database>     List tables\n"+
			"  schema [flags] <database>     Print the schema\n"+
			"  query [flags] <db> <sql...>   Run one query\n"+
			"  insert <db> <table> <json...> Insert JSON rows\n"+
			"  version                       Print version and exit\n"+
			"  help                          Show this help\n\n")
		fmt.Fprintf(fs.Output(), "Options:\n")
		fs.PrintDefaults()
	}

	var (
		tenant  = fs.String("tenant", "default", "Tenant/schema name")
		mode    = fs.String("mode", "column", "Output mode: column|list|csv|json|jsonl|ndjson|table|geojson|topojson")
		headers = fs.Bool("header", true, "Include column headers")
		echo    = fs.Bool("echo", false, "Echo SQL before execution")
		cmd     = fs.String("cmd", "", "Run specific SQL and exit")
		batch   = fs.Bool("batch", false, "Force batch mode")
		outFile = fs.String("output", "", "Write output to file")
		geomCol = fs.String("geom-col", "", "Geometry column for -mode geojson|topojson (auto-detected if omitted)")
		version = fs.Bool("version", false, "Print version and exit")
	)
	storageFlags := addStorageFlags(fs)

	if done, err := parseCLIFlags(fs, args); done {
		return err
	}
	if *version {
		return printVersion(os.Stdout)
	}
	selectedMode, err := parseOutputMode(*mode)
	if err != nil {
		return err
	}
	storageOpts, err := storageFlags.options()
	if err != nil {
		return err
	}

	cfg := &Config{
		Tenant:    *tenant,
		Output:    *outFile,
		Header:    *headers,
		Echo:      *echo,
		Batch:     *batch,
		Mode:      selectedMode,
		NullValue: "", // default empty for column mode, usually
		GeomCol:   *geomCol,
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
	db, savePath, err := openDatabaseWithOptions(dbPath, storageOpts)
	if err != nil {
		return err
	}
	defer func() {
		// The compatibility "auto" backend retains the historical single-file
		// snapshot behaviour. Explicit storage backends flush in DB.Close.
		if savePath != "" && !storageOpts.ReadOnly {
			if err := tsql.SaveToFile(db, savePath); err != nil && runErr == nil {
				runErr = err
			}
		}
		if err := db.Close(); err != nil && runErr == nil {
			runErr = err
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
	execSQL := func(ctx context.Context, sql string) error {
		if strings.TrimSpace(sql) == "" {
			return nil
		}
		dirty, err := execute(ctx, db, cfg, sql, out)
		if dirty && savePath != "" && err == nil {
			return tsql.SaveToFile(db, savePath)
		}
		return err
	}
	runBatch := func(sql string) error {
		return runWithSignalContext(func(ctx context.Context) error {
			return execSQL(ctx, sql)
		})
	}

	// Scenario A: -cmd flag
	if *cmd != "" {
		return runBatch(*cmd)
	}

	// Scenario B: Inline SQL argument
	if inlineSQL != "" {
		return runBatch(inlineSQL)
	}

	// Scenario C: Piped Input (Stdin)
	if isInputPiped() {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		return runBatch(string(data))
	}

	// Scenario D: Batch mode requested but no input
	if cfg.Batch {
		return errors.New("batch mode requested but no SQL provided")
	}

	// Scenario E: Interactive Shell (REPL)
	repl := NewRepl(db, cfg, savePath, out)
	return repl.Run()
}

// runWithSignalContext turns Ctrl+C and SIGTERM into context cancellation for
// batch queries. The query engine observes the context while scanning, and a
// stream is closed by the caller when writing to stdout fails (for example
// because the downstream end of a pipe was closed).
func runWithSignalContext(run func(context.Context) error) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx)
}

func versionString() string {
	if v := strings.TrimSpace(Version); v != "" {
		return v
	}
	return "dev"
}

func printVersion(out io.Writer) error {
	_, err := fmt.Fprintf(out, "tinySQL %s\n", versionString())
	return err
}

func parseOutputMode(raw string) (OutputMode, error) {
	mode := OutputMode(strings.ToLower(strings.TrimSpace(raw)))
	switch mode {
	case ModeColumn, ModeList, ModeCSV, ModeJSON, ModeJSONL, ModeNDJSON, ModeTable, ModeGeoJSON, ModeTopoJSON:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown output mode %q (valid: column, list, csv, json, jsonl, ndjson, table, geojson, topojson)", raw)
	}
}

// cliStorageOptions keeps CLI-specific parsing separate from the public
// StorageConfig. "auto" is deliberately the default so existing positional
// database files continue to use tinySQL's historical single-snapshot format.
// Selecting a concrete backend opts into OpenDB and its lifecycle semantics.
type cliStorageOptions struct {
	Mode          string
	ReadOnly      bool
	MemoryLimit   int64
	WALSync       tsql.WALSyncMode
	SyncOnMutate  bool
	CompressFiles bool
}

type storageFlagValues struct {
	mode          *string
	readOnly      *bool
	memoryLimit   *string
	walSync       *string
	syncOnMutate  *bool
	compressFiles *bool
}

func addStorageFlags(fs *flag.FlagSet) storageFlagValues {
	return storageFlagValues{
		mode:          fs.String("storage", "auto", "Storage backend: auto|memory|wal|disk|json|index|hybrid|advanced_wal|paged_index|sqlite"),
		readOnly:      fs.Bool("read-only", false, "Reject mutating SQL statements"),
		memoryLimit:   fs.String("memory-limit", "", "Memory limit for hybrid/index storage (for example 256MiB)"),
		walSync:       fs.String("wal-sync", "full", "WAL durability: full|normal"),
		syncOnMutate:  fs.Bool("sync-on-mutate", false, "Flush durable storage after each mutation"),
		compressFiles: fs.Bool("compress", false, "Compress disk-backed table/checkpoint files"),
	}
}

func (f storageFlagValues) options() (cliStorageOptions, error) {
	mode := strings.ToLower(strings.TrimSpace(*f.mode))
	if mode == "" {
		mode = "auto"
	}
	if mode != "auto" {
		if _, err := tsql.ParseStorageMode(mode); err != nil {
			return cliStorageOptions{}, err
		}
	}
	walSync, err := tsql.ParseWALSyncMode(*f.walSync)
	if err != nil {
		return cliStorageOptions{}, err
	}
	memoryLimit, err := parseByteSize(*f.memoryLimit)
	if err != nil {
		return cliStorageOptions{}, fmt.Errorf("invalid -memory-limit: %w", err)
	}
	return cliStorageOptions{
		Mode:          mode,
		ReadOnly:      *f.readOnly,
		MemoryLimit:   memoryLimit,
		WALSync:       walSync,
		SyncOnMutate:  *f.syncOnMutate,
		CompressFiles: *f.compressFiles,
	}, nil
}

// parseByteSize accepts raw bytes and the common binary/decimal suffixes used
// in CLI configuration (64MiB, 256MB, 1GiB). Empty means "use backend
// default", and is represented as zero.
func parseByteSize(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, nil
	}
	lower := strings.ToLower(s)
	multiplier := int64(1)
	for _, unit := range []struct {
		suffix string
		value  int64
	}{
		{"gib", 1 << 30}, {"gb", 1_000_000_000},
		{"mib", 1 << 20}, {"mb", 1_000_000},
		{"kib", 1 << 10}, {"kb", 1_000},
		{"b", 1},
	} {
		if strings.HasSuffix(lower, unit.suffix) {
			multiplier = unit.value
			s = strings.TrimSpace(s[:len(s)-len(unit.suffix)])
			break
		}
	}
	if s == "" {
		return 0, errors.New("missing number")
	}
	value, err := strconv.ParseInt(s, 10, 64)
	if err != nil || value < 0 {
		if err == nil {
			err = errors.New("must not be negative")
		}
		return 0, err
	}
	if value > 0 && value > (int64(^uint64(0)>>1)/multiplier) {
		return 0, errors.New("value overflows int64")
	}
	return value * multiplier, nil
}

// ---- REPL (Interactive Shell) -----------------------------------------------

type Repl struct {
	db       *tsql.DB
	cfg      *Config
	savePath string
	out      io.Writer
	buf      strings.Builder

	mu           sync.Mutex
	activeCancel context.CancelFunc
}

func NewRepl(db *tsql.DB, cfg *Config, savePath string, out io.Writer) *Repl {
	return &Repl{
		db:       db,
		cfg:      cfg,
		savePath: savePath,
		out:      out,
	}
}

var errREPLExit = errors.New("leave repl")

func (r *Repl) Run() error {
	fmt.Fprintf(r.out, "TinySQL version %s (mimicking sqlite3)\n", versionString())
	fmt.Fprintf(r.out, "Enter \".help\" for usage hints.\n")
	if r.savePath == "" {
		fmt.Fprintln(r.out, "Connected to: :memory:")
	} else {
		fmt.Fprintf(r.out, "Connected to: %s\n", r.savePath)
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB buffer

	// A signal listener may cancel an active query, but it must never call
	// os.Exit from its goroutine. A second channel lets the main REPL loop
	// handle Ctrl+C while it is waiting for input or collecting a statement.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	interrupts := make(chan struct{}, 1)
	listenerDone := make(chan struct{})
	defer close(listenerDone)
	go func() {
		for {
			select {
			case <-listenerDone:
				return
			case <-sigChan:
				if r.cancelActiveQuery() {
					continue
				}
				select {
				case interrupts <- struct{}{}:
				default:
				}
			}
		}
	}()

	lines := make(chan string)
	scanErr := make(chan error, 1)
	readerDone := make(chan struct{})
	defer close(readerDone)
	go func() {
		defer close(lines)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-readerDone:
				return
			}
		}
		scanErr <- scanner.Err()
	}()

	r.printPrompt()
	for {
		select {
		case <-interrupts:
			if r.buf.Len() == 0 {
				return nil
			}
			fmt.Fprintln(r.out, "^C")
			r.buf.Reset()
			r.printPrompt()

		case line, ok := <-lines:
			if !ok {
				return <-scanErr
			}
			trimmed := strings.TrimSpace(line)

			// Meta commands (only processed if buffer is empty)
			if r.buf.Len() == 0 && strings.HasPrefix(trimmed, ".") {
				if err := r.handleMeta(trimmed); err != nil {
					if errors.Is(err, errREPLExit) {
						return nil
					}
					if errors.Is(err, context.Canceled) {
						fmt.Fprintln(r.out, "^C")
					} else {
						fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					}
				}
				r.printPrompt()
				continue
			}

			r.buf.WriteString(line)
			r.buf.WriteByte('\n')

			if strings.HasSuffix(trimmed, ";") {
				sqlText := r.buf.String()
				r.buf.Reset()

				dirty, err := r.execute(sqlText)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						fmt.Fprintln(r.out, "^C")
					} else {
						fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					}
				}
				if dirty && r.savePath != "" {
					if err := tsql.SaveToFile(r.db, r.savePath); err != nil {
						fmt.Fprintf(os.Stderr, "Auto-save failed: %v\n", err)
					}
				}
			}
			r.printPrompt()
		}
	}
}

// execute gives every interactive statement an independently cancellable
// context. The signal listener only invokes this cancellation function, so
// Ctrl+C aborts the current scan without terminating the process or the REPL.
func (r *Repl) execute(sql string) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r.mu.Lock()
	r.activeCancel = cancel
	r.mu.Unlock()
	defer func() {
		cancel()
		r.mu.Lock()
		r.activeCancel = nil
		r.mu.Unlock()
	}()
	return execute(ctx, r.db, r.cfg, sql, r.out)
}

func (r *Repl) cancelActiveQuery() bool {
	r.mu.Lock()
	cancel := r.activeCancel
	r.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
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
	if len(parts) == 0 {
		return nil
	}
	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case ".help":
		printHelp(r.out)
	case ".quit", ".exit":
		return errREPLExit
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
			fmt.Fprintf(r.out, "current mode: %s\n", r.cfg.Mode)
			return nil
		}
		mode, err := parseOutputMode(args[0])
		if err != nil {
			return err
		}
		r.cfg.Mode = mode
	case ".headers":
		if len(args) < 1 {
			return errors.New("usage: .headers on|off")
		}
		switch strings.ToLower(args[0]) {
		case "on":
			r.cfg.Header = true
		case "off":
			r.cfg.Header = false
		default:
			return errors.New("usage: .headers on|off")
		}
	case ".timer":
		if len(args) < 1 {
			if r.cfg.Timer {
				fmt.Fprintln(r.out, "timer: on")
			} else {
				fmt.Fprintln(r.out, "timer: off")
			}
			return nil
		}
		switch strings.ToLower(args[0]) {
		case "on":
			r.cfg.Timer = true
		case "off":
			r.cfg.Timer = false
		default:
			return errors.New("usage: .timer on|off")
		}
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
		_, err = r.execute(string(data))
		return err
	case ".save":
		if len(args) < 1 {
			return errors.New("usage: .save FILE")
		}
		return tsql.SaveToFile(r.db, args[0])
	case ".dump":
		return dumpTables(r.out, r.db, r.cfg.Tenant, args)
	case ".import":
		if len(args) < 1 {
			return errors.New("usage: .import FILE [TABLE]")
		}
		return importFileCmd(r.db, r.cfg.Tenant, args, r.out)
	case ".count":
		return countTables(r.out, r.db, r.cfg.Tenant, args)
	case ".stats":
		return showStats(r.out, r.db, r.cfg.Tenant)
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
	return nil
}

func printHelp(out io.Writer) {
	fmt.Fprintln(out, `
.count [TABLE...]      Show row counts for tables
.dump [TABLE...]       Dump tables as INSERT statements
.exit                  Exit this program
.headers on|off        Turn display of headers on or off
.help                  Show this message
.import FILE [TABLE]   Import a file into a table (CSV, TSV, JSON, GeoJSON, TopoJSON, KML, OSM XML, ...)
.mode MODE             Set output mode (column, list, csv, json, jsonl/ndjson, table, geojson, topojson)
.nullvalue STRING      Use STRING in place of NULL values
.read FILENAME         Execute SQL in FILENAME
.save FILENAME         Write in-memory database into FILENAME
.schema ?TABLE?        Show the CREATE statements
.stats                 Show database statistics
.tables                List names of tables
.timer on|off          Turn SQL timer on or off`)
}

// dumpTables outputs INSERT statements for specified tables (or all).
func dumpTables(out io.Writer, db *tsql.DB, tenant string, args []string) error {
	tables := db.ListTables(tenant)
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Name
	}
	sort.Strings(names)

	// Filter if args given
	if len(args) > 0 {
		filter := make(map[string]bool)
		for _, a := range args {
			filter[strings.ToLower(a)] = true
		}
		var filtered []string
		for _, n := range names {
			if filter[strings.ToLower(n)] {
				filtered = append(filtered, n)
			}
		}
		names = filtered
	}

	ctx := context.Background()
	for _, name := range names {
		tbl, err := db.Get(tenant, name)
		if err != nil {
			continue
		}
		// Print CREATE TABLE
		fmt.Fprintf(out, "CREATE TABLE %s (\n", tbl.Name)
		for i, col := range tbl.Cols {
			def := fmt.Sprintf("  %s %s", col.Name, col.Type)
			if col.Constraint == tsql.PrimaryKey {
				def += " PRIMARY KEY"
			}
			if i < len(tbl.Cols)-1 {
				def += ","
			}
			fmt.Fprintln(out, def)
		}
		fmt.Fprintln(out, ");")

		// Print INSERT statements
		stmt, err := tsql.ParseSQL(fmt.Sprintf("SELECT * FROM %s", name))
		if err != nil {
			continue
		}
		rs, err := tsql.Execute(ctx, db, tenant, stmt)
		if err != nil || rs == nil {
			continue
		}
		if err := exporter.ExportSQL(out, rs, name); err != nil {
			return err
		}
		fmt.Fprintln(out)
	}
	return nil
}

// importFileCmd imports a file into a table. It delegates to the shared
// tsql.ImportFile extension dispatcher -- the same one importer.OpenFile and
// every other public entry point uses -- instead of hand-duplicating a
// narrow subset of its format list here. Before this, ".import" only
// understood .csv/.tsv/.json even though ImportFile has long supported
// .geojson/.topojson/.kml/.osm/.mbtiles/.shp/... too; delegating fixes both
// that pre-existing gap and adds .topojson in one change.
func importFileCmd(db *tsql.DB, tenant string, args []string, out io.Writer) error {
	filePath := args[0]
	tableName := ""
	if len(args) > 1 {
		tableName = args[1]
	}

	ctx := context.Background()
	result, err := tsql.ImportFile(ctx, db, tenant, tableName, filePath, nil)
	if err != nil {
		return err
	}

	if tableName == "" {
		base := filepath.Base(filePath)
		tableName = strings.TrimSuffix(base, filepath.Ext(base))
	}
	fmt.Fprintf(out, "Imported %d rows into %s\n", result.RowsInserted, tableName)
	return nil
}

// countTables shows row counts for tables.
func countTables(out io.Writer, db *tsql.DB, tenant string, args []string) error {
	tables := db.ListTables(tenant)
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	sort.Strings(names)

	// Filter if args given
	if len(args) > 0 {
		filter := make(map[string]bool)
		for _, a := range args {
			filter[strings.ToLower(a)] = true
		}
		var filtered []string
		for _, n := range names {
			if filter[strings.ToLower(n)] {
				filtered = append(filtered, n)
			}
		}
		names = filtered
	}

	ctx := context.Background()
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "TABLE\tROWS\n")
	fmt.Fprintf(w, "-----\t----\n")
	total := 0
	for _, name := range names {
		stmt, err := tsql.ParseSQL(fmt.Sprintf("SELECT COUNT(*) AS cnt FROM %s", name))
		if err != nil {
			continue
		}
		rs, err := tsql.Execute(ctx, db, tenant, stmt)
		if err != nil || rs == nil || len(rs.Rows) == 0 {
			fmt.Fprintf(w, "%s\t?\n", name)
			continue
		}
		cnt := 0
		if v, ok := rs.Rows[0]["cnt"]; ok {
			switch n := v.(type) {
			case int:
				cnt = n
			case int64:
				cnt = int(n)
			case float64:
				cnt = int(n)
			}
		}
		total += cnt
		fmt.Fprintf(w, "%s\t%d\n", name, cnt)
	}
	fmt.Fprintf(w, "-----\t----\n")
	fmt.Fprintf(w, "TOTAL\t%d\n", total)
	return w.Flush()
}

// showStats displays database-level statistics.
func showStats(out io.Writer, db *tsql.DB, tenant string) error {
	tables := db.ListTables(tenant)
	totalTables := len(tables)
	totalRows := 0
	totalCols := 0
	ctx := context.Background()

	for _, t := range tables {
		tbl, err := db.Get(tenant, t.Name)
		if err != nil {
			continue
		}
		totalCols += len(tbl.Cols)

		stmt, _ := tsql.ParseSQL(fmt.Sprintf("SELECT COUNT(*) AS cnt FROM %s", t.Name))
		if stmt != nil {
			rs, err := tsql.Execute(ctx, db, tenant, stmt)
			if err == nil && rs != nil && len(rs.Rows) > 0 {
				if v, ok := rs.Rows[0]["cnt"]; ok {
					switch n := v.(type) {
					case int:
						totalRows += n
					case int64:
						totalRows += int(n)
					case float64:
						totalRows += int(n)
					}
				}
			}
		}
	}

	fmt.Fprintf(out, "Database Statistics:\n")
	fmt.Fprintf(out, "  Tenant:       %s\n", tenant)
	fmt.Fprintf(out, "  Tables:       %d\n", totalTables)
	fmt.Fprintf(out, "  Columns:      %d\n", totalCols)
	fmt.Fprintf(out, "  Total rows:   %d\n", totalRows)
	return nil
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

		// List, CSV and JSON-oriented output can be emitted one row at a time.
		// Keep column/table and geo formats on the materialized path: they need
		// global alignment or topology construction before their first byte is
		// meaningful. The engine transparently materializes blocking SELECT
		// shapes (ORDER BY, joins, aggregates, ...) behind ExecuteStream, so this
		// remains semantically identical for those queries too.
		if isStreamingOutputMode(cfg.Mode) && startsWithSelect(stmtSQL) {
			err = executeAndPrintStream(ctx, db, cfg, parsed, out)
			duration := time.Since(start)
			if err != nil {
				return dirty, err
			}
			if cfg.Timer {
				fmt.Fprintf(out, "Run Time: real %.3fs\n", duration.Seconds())
			}
			continue
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

func isStreamingOutputMode(mode OutputMode) bool {
	switch mode {
	case ModeList, ModeCSV, ModeJSON, ModeJSONL, ModeNDJSON:
		return true
	default:
		return false
	}
}

// startsWithSelect recognizes the intentionally conservative subset that is
// definitely a SELECT statement. WITH can contain mutating DML in SQL, so it
// stays on the ordinary execution path rather than risking an incorrect dirty
// flag. Leading line and block comments are accepted because they are common
// in batch scripts.
func startsWithSelect(sql string) bool {
	s := strings.TrimSpace(sql)
	for {
		switch {
		case strings.HasPrefix(s, "--"):
			if newline := strings.IndexByte(s, '\n'); newline >= 0 {
				s = strings.TrimSpace(s[newline+1:])
				continue
			}
			return false
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s[2:], "*/")
			if end < 0 {
				return false
			}
			s = strings.TrimSpace(s[end+4:])
			continue
		}
		break
	}
	if len(s) < len("select") || !strings.EqualFold(s[:len("select")], "select") {
		return false
	}
	return len(s) == len("select") || !isSQLIdentifierByte(s[len("select")])
}

func isSQLIdentifierByte(b byte) bool {
	return b == '_' || b == '$' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

func executeAndPrintStream(ctx context.Context, db *tsql.DB, cfg *Config, stmt tsql.Statement, out io.Writer) error {
	stream, err := tsql.ExecuteStream(ctx, db, cfg.Tenant, stmt)
	if err != nil {
		return err
	}
	printErr := printResultStream(out, stream, cfg)
	// Close is what releases a held engine read lock when the writer returns an
	// error early (notably a broken pipe). It is also safe and cheap at EOF.
	closeErr := stream.Close()
	if printErr != nil {
		return printErr
	}
	return closeErr
}

func printResultStream(out io.Writer, stream *tsql.ResultStream, cfg *Config) error {
	if stream == nil {
		return errors.New("nil result stream")
	}
	cols := stream.Columns()
	switch cfg.Mode {
	case ModeList:
		return printListStream(out, stream, cols, cfg)
	case ModeCSV:
		return printCSVStream(out, stream, cols, cfg)
	case ModeJSON:
		return printJSONStream(out, stream, cols, false)
	case ModeJSONL, ModeNDJSON:
		return printJSONStream(out, stream, cols, true)
	default:
		return fmt.Errorf("output mode %q cannot stream", cfg.Mode)
	}
}

func printListStream(out io.Writer, stream *tsql.ResultStream, cols []string, cfg *Config) error {
	for stream.Next() {
		row := stream.Row()
		for i, col := range cols {
			if i > 0 {
				if _, err := io.WriteString(out, "|"); err != nil {
					return err
				}
			}
			value, _ := tsql.GetVal(row, col)
			if _, err := io.WriteString(out, fmtScalar(value, cfg.NullValue)); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(out, "\n"); err != nil {
			return err
		}
	}
	return stream.Err()
}

func printCSVStream(out io.Writer, stream *tsql.ResultStream, cols []string, cfg *Config) error {
	csvWriter := csv.NewWriter(out)
	if cfg.Header {
		if err := csvWriter.Write(cols); err != nil {
			return err
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return err
		}
	}
	for stream.Next() {
		row := stream.Row()
		values := make([]string, len(cols))
		for i, col := range cols {
			value, _ := tsql.GetVal(row, col)
			values[i] = csvScalar(value)
		}
		if err := csvWriter.Write(values); err != nil {
			return err
		}
		// Flush after each record so a pipeline sees its first result without
		// waiting for encoding/csv's internal buffer to fill.
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return err
		}
	}
	return stream.Err()
}

func printJSONStream(out io.Writer, stream *tsql.ResultStream, cols []string, lines bool) error {
	if lines {
		encoder := json.NewEncoder(out)
		for stream.Next() {
			if err := encoder.Encode(streamJSONRow(stream.Row(), cols)); err != nil {
				return err
			}
		}
		return stream.Err()
	}

	if _, err := io.WriteString(out, "["); err != nil {
		return err
	}
	written := 0
	for stream.Next() {
		encoded, err := json.MarshalIndent(streamJSONRow(stream.Row(), cols), "  ", "  ")
		if err != nil {
			return err
		}
		separator := "\n  "
		if written > 0 {
			separator = "," + separator
		}
		if _, err := io.WriteString(out, separator); err != nil {
			return err
		}
		if _, err := out.Write(encoded); err != nil {
			return err
		}
		written++
	}
	if err := stream.Err(); err != nil {
		return err
	}
	if written > 0 {
		if _, err := io.WriteString(out, "\n"); err != nil {
			return err
		}
	}
	_, err := io.WriteString(out, "]\n")
	return err
}

// streamJSONRow mirrors exporter.ExportJSON/ExportNDJSON: only display
// columns are included and BLOBs use tinySQL's self-describing envelope.
func streamJSONRow(row tsql.Row, cols []string) map[string]any {
	result := make(map[string]any, len(cols))
	for _, col := range cols {
		value, _ := tsql.GetVal(row, col)
		if blob, ok := value.([]byte); ok {
			result[col] = map[string]string{
				"$tinysql": "blob",
				"base64":   base64.StdEncoding.EncodeToString(blob),
			}
			continue
		}
		result[col] = value
	}
	return result
}

func csvScalar(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case time.Time:
		return v.Format(time.RFC3339)
	case []byte:
		return "base64:" + base64.StdEncoding.EncodeToString(v)
	default:
		return fmt.Sprint(v)
	}
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
	case ModeJSONL, ModeNDJSON:
		return &JSONLPrinter{}
	case ModeGeoJSON:
		return &GeoJSONPrinter{}
	case ModeTopoJSON:
		return &TopoJSONPrinter{}
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
			val, _ := tsql.GetVal(row, col)
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
			val, _ := tsql.GetVal(row, col)
			fmt.Fprint(out, fmtScalar(val, cfg.NullValue))
		}
		fmt.Fprintln(out)
	}
	return nil
}

type CSVPrinter struct{}

func (cp *CSVPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	return exporter.ExportCSV(out, rs, exporter.Options{CSVNoHeader: !cfg.Header})
}

type JSONPrinter struct{}

func (jp *JSONPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	return exporter.ExportJSON(out, rs, exporter.Options{PrettyJSON: true})
}

// JSONLPrinter writes one independently valid JSON object per row. ndjson is
// accepted as an alias because both names are common in shell pipelines.
type JSONLPrinter struct{}

func (jp *JSONLPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	return exporter.ExportNDJSON(out, rs, exporter.Options{})
}

// GeoJSONPrinter writes a query result as an RFC 7946 FeatureCollection.
// cfg.GeomCol selects the geometry column ("" auto-detects it).
type GeoJSONPrinter struct{}

func (gp *GeoJSONPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	return exporter.ExportGeoJSON(out, rs, cfg.GeomCol, exporter.Options{PrettyJSON: true})
}

// TopoJSONPrinter writes a query result as a TopoJSON v3 Topology, the
// format Power BI Shape Maps and most BI/mapping tools prefer over
// GeoJSON. cfg.GeomCol selects the geometry column ("" auto-detects it).
type TopoJSONPrinter struct{}

func (tp *TopoJSONPrinter) Print(out io.Writer, rs *tsql.ResultSet, cfg *Config) error {
	return exporter.ExportTopoJSON(out, rs, cfg.GeomCol, "", exporter.Options{PrettyJSON: true})
}

// ---- Helpers ----------------------------------------------------------------

func openDatabase(path string) (*tsql.DB, string, error) {
	return openDatabaseWithOptions(path, cliStorageOptions{Mode: "auto", WALSync: tsql.WALSyncFull})
}

// openDatabaseWithOptions is the CLI's bridge between its historical
// positional snapshot database and the package's StorageConfig/OpenDB API.
// "auto" deliberately preserves the old snapshot workflow; every explicit
// backend is opened through OpenDB and persists via DB.Close/Sync.
func openDatabaseWithOptions(path string, opts cliStorageOptions) (*tsql.DB, string, error) {
	if path == "" {
		path = ":memory:"
	}
	modeName := strings.ToLower(strings.TrimSpace(opts.Mode))
	if modeName == "" {
		modeName = "auto"
	}
	if modeName == "auto" {
		if path == ":memory:" {
			db := tsql.NewDB()
			db.SetReadOnly(opts.ReadOnly)
			return db, "", nil
		}
		if _, err := os.Stat(path); err == nil {
			db, loadErr := tsql.LoadFromFile(path)
			if loadErr != nil {
				return nil, "", loadErr
			}
			db.SetReadOnly(opts.ReadOnly)
			return db, legacySavePath(path, opts.ReadOnly), nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, "", err
		}
		if opts.ReadOnly {
			return nil, "", fmt.Errorf("read-only open requires an existing database file %q", path)
		}
		dir := filepath.Dir(path)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, "", err
			}
		}
		return tsql.NewDB(), legacySavePath(path, false), nil
	}

	mode, err := tsql.ParseStorageMode(modeName)
	if err != nil {
		return nil, "", err
	}
	if path == ":memory:" && mode != tsql.ModeMemory {
		return nil, "", fmt.Errorf("-storage %s requires a filesystem path, not :memory:", modeName)
	}
	// MemoryBackend uses its configured Path as a save-on-Close target. For a
	// read-only snapshot that would be a surprising write, so load it through
	// the existing snapshot API instead.
	if mode == tsql.ModeMemory && opts.ReadOnly && path != ":memory:" {
		info, statErr := os.Stat(path)
		if statErr != nil {
			return nil, "", fmt.Errorf("read-only open requires an existing database file %q: %w", path, statErr)
		}
		if info.IsDir() {
			return nil, "", fmt.Errorf("read-only memory storage requires a snapshot file, got directory %q", path)
		}
		db, loadErr := tsql.LoadFromFile(path)
		if loadErr != nil {
			return nil, "", loadErr
		}
		db.SetReadOnly(true)
		return db, "", nil
	}

	config := tsql.DefaultStorageConfig(mode)
	if path != ":memory:" {
		config.Path = path
	}
	config.ReadOnly = opts.ReadOnly
	config.MaxMemoryBytes = opts.MemoryLimit
	config.WALSync = opts.WALSync
	config.SyncOnMutate = opts.SyncOnMutate
	config.CompressFiles = opts.CompressFiles
	db, err := tsql.OpenDB(config)
	if err != nil {
		return nil, "", err
	}
	return db, "", nil
}

func legacySavePath(path string, readOnly bool) string {
	if readOnly || path == "" || path == ":memory:" {
		return ""
	}
	return path
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
			if col.Constraint == tsql.PrimaryKey {
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
	case "version":
		return true, printVersion(os.Stdout)
	case "help":
		// Without this, "help" falls through to runCLI and is taken as the
		// database path, which creates a file named "help" in the working
		// directory and opens an interactive shell on it.
		return true, runCLI([]string{"-h"})
	case "shell", "repl":
		// Keep cmd/repl as a compatible standalone binary, but make the main
		// tool the natural entry point for interactive use too. Passing the
		// remaining arguments through preserves the ordinary database, storage,
		// tenant, and output flags.
		return true, runCLI(args)
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
	fs := flag.NewFlagSet("tables", flag.ContinueOnError)
	tenant := fs.String("tenant", "default", "Tenant")
	jsonOut := fs.Bool("json", false, "Emit JSON")
	storageFlags := addStorageFlags(fs)
	if done, err := parseCLIFlags(fs, args); done {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: tables [flags] <database>")
	}
	storageOpts, err := storageFlags.options()
	if err != nil {
		return err
	}

	db, _, err := openDatabaseWithOptions(fs.Arg(0), storageOpts)
	if err != nil {
		return err
	}
	defer db.Close()

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
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	tenant := fs.String("tenant", "default", "Tenant")
	storageFlags := addStorageFlags(fs)
	if done, err := parseCLIFlags(fs, args); done {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: schema [flags] <database>")
	}
	storageOpts, err := storageFlags.options()
	if err != nil {
		return err
	}
	db, _, err := openDatabaseWithOptions(fs.Arg(0), storageOpts)
	if err != nil {
		return err
	}
	defer db.Close()
	return printSchema(os.Stdout, db, *tenant, "")
}

func runQueryUtil(args []string) (runErr error) {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	tenant := fs.String("tenant", "default", "Tenant")
	mode := fs.String("mode", "table", "Output mode")
	headers := fs.Bool("header", true, "Include column headers")
	storageFlags := addStorageFlags(fs)
	if done, err := parseCLIFlags(fs, args); done {
		return err
	}
	if fs.NArg() < 2 {
		return errors.New("usage: query [flags] <database> <sql...>")
	}
	selectedMode, err := parseOutputMode(*mode)
	if err != nil {
		return err
	}
	storageOpts, err := storageFlags.options()
	if err != nil {
		return err
	}

	db, savePath, err := openDatabaseWithOptions(fs.Arg(0), storageOpts)
	if err != nil {
		return err
	}
	defer func() {
		if savePath != "" && !storageOpts.ReadOnly && runErr == nil {
			if err := tsql.SaveToFile(db, savePath); err != nil {
				runErr = err
			}
		}
		if err := db.Close(); err != nil && runErr == nil {
			runErr = err
		}
	}()

	sql := strings.Join(fs.Args()[1:], " ")
	cfg := &Config{Tenant: *tenant, Mode: selectedMode, Header: *headers}
	return runWithSignalContext(func(ctx context.Context) error {
		dirty, err := execute(ctx, db, cfg, sql, os.Stdout)
		if err != nil {
			return err
		}
		if dirty && savePath != "" {
			return tsql.SaveToFile(db, savePath)
		}
		return nil
	})
}

func runInsertUtil(args []string) (runErr error) {
	fs := flag.NewFlagSet("insert", flag.ContinueOnError)
	tenant := fs.String("tenant", "default", "Tenant")
	storageFlags := addStorageFlags(fs)
	if done, err := parseCLIFlags(fs, args); done {
		return err
	}
	if fs.NArg() < 3 {
		return errors.New("usage: insert <db> <table> <json...>")
	}
	storageOpts, err := storageFlags.options()
	if err != nil {
		return err
	}

	db, path, err := openDatabaseWithOptions(fs.Arg(0), storageOpts)
	if err != nil {
		return err
	}
	defer func() {
		if path != "" && !storageOpts.ReadOnly && runErr == nil {
			if err := tsql.SaveToFile(db, path); err != nil {
				runErr = err
			}
		}
		if err := db.Close(); err != nil && runErr == nil {
			runErr = err
		}
	}()

	table := fs.Arg(1)
	count := 0
	return runWithSignalContext(func(ctx context.Context) error {
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
				_ = v
			}
			sort.Strings(cols)
			for _, k := range cols {
				v := data[k]
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
			if _, err := execute(ctx, db, &Config{Tenant: *tenant}, sql, io.Discard); err != nil {
				return err
			}
			count++
		}
		fmt.Printf("Inserted %d rows.\n", count)
		return nil
	})
}
