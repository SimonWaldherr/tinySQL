package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

const (
	defaultTenant       = "default"
	defaultWorkers      = 4
	defaultCacheSize    = 256
	defaultQueryTimeout = 30 * time.Second
)

type Config struct {
	Files               []string
	Query               string
	TableName           string
	Delimiter           string
	DelimiterCandidates []rune
	Interactive         bool
	Verbose             bool
	Output              string
	FuzzyImport         bool
	CacheEnabled        bool
	CacheSize           int
	ParallelLoad        bool
	MaxWorkers          int
	QueryTimeout        time.Duration
}

type loadJob struct {
	file      string
	tableName string
}

type Runner struct {
	db         *tinysql.DB
	tenant     string
	config     Config
	queryCache *tinysql.QueryCache
}

// Track loaded table names for interactive status.
var (
	tableNames    = make(map[string]string)
	tableNamesMux sync.RWMutex
)

func main() {
	config, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(2)
	}

	if config.Interactive {
		runInteractiveMode(config)
		return
	}

	if len(config.Files) == 0 || strings.TrimSpace(config.Query) == "" {
		fmt.Fprintf(os.Stderr, "Error: both file and query are required in non-interactive mode\n")
		flag.Usage()
		os.Exit(2)
	}

	if err := executeQuery(config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() (Config, error) {
	config := Config{}

	flag.StringVar(&config.Query, "query", "", "SQL query to execute")
	flag.StringVar(&config.TableName, "table", "", "Table name (default: filename without extension)")
	flag.StringVar(&config.Delimiter, "delimiter", "auto", "CSV delimiter: auto, comma, semicolon, tab, pipe, or single-char")
	flag.BoolVar(&config.Interactive, "interactive", false, "Run in interactive mode")
	flag.BoolVar(&config.Verbose, "verbose", false, "Verbose output with timing and statistics")
	flag.StringVar(&config.Output, "output", "table", "Output format: table, json, csv")
	flag.BoolVar(&config.FuzzyImport, "fuzzy", true, "Enable fuzzy import for malformed files")
	flag.BoolVar(&config.CacheEnabled, "cache", true, "Enable query caching for better performance")
	flag.IntVar(&config.CacheSize, "cache-size", defaultCacheSize, "Query cache size (ignored when -cache=false)")
	flag.BoolVar(&config.ParallelLoad, "parallel", false, "Load files in parallel")
	flag.IntVar(&config.MaxWorkers, "workers", defaultWorkers, "Number of parallel workers (with -parallel)")
	flag.DurationVar(&config.QueryTimeout, "query-timeout", defaultQueryTimeout, "Per-query timeout (0 disables timeout)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "tinySQL query_files\n\n")
		fmt.Fprintf(os.Stderr, "Query CSV/JSON files with SQL from CLI or interactive mode.\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s [options] <file-or-dir> [more files/dirs]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  %s -query \"SELECT * FROM users LIMIT 10\" users.csv\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -parallel -workers 8 -query \"SELECT * FROM users u JOIN orders o ON u.id=o.user_id\" users.csv orders.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -interactive ./data\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	config.Files = flag.Args()

	if err := normalizeConfig(&config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func normalizeConfig(config *Config) error {
	config.Query = strings.TrimSpace(config.Query)
	config.TableName = sanitizeTableName(strings.TrimSpace(config.TableName))
	config.Output = strings.ToLower(strings.TrimSpace(config.Output))
	config.Delimiter = strings.TrimSpace(config.Delimiter)

	switch config.Output {
	case "table", "json", "csv":
	default:
		return fmt.Errorf("invalid output format %q (valid: table, json, csv)", config.Output)
	}

	if config.MaxWorkers <= 0 {
		return fmt.Errorf("workers must be > 0")
	}
	if config.QueryTimeout < 0 {
		return fmt.Errorf("query-timeout must be >= 0")
	}

	delims, err := parseDelimiterSpec(config.Delimiter)
	if err != nil {
		return err
	}
	config.DelimiterCandidates = delims

	if config.CacheEnabled {
		if config.CacheSize < 0 {
			return fmt.Errorf("cache-size must be >= 0")
		}
		if config.CacheSize == 0 {
			config.CacheSize = defaultCacheSize
		}
	} else {
		config.CacheSize = 0
	}

	return nil
}

func parseDelimiterSpec(raw string) ([]rune, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch raw {
	case "", "auto":
		return nil, nil
	case `\t`, `\\t`, "tab":
		return []rune{'\t'}, nil
	case "comma":
		return []rune{','}, nil
	case "semicolon", "semi":
		return []rune{';'}, nil
	case "pipe":
		return []rune{'|'}, nil
	}

	runes := []rune(raw)
	if len(runes) != 1 {
		return nil, fmt.Errorf("invalid delimiter %q (use auto, comma, semicolon, tab, pipe, or a single character)", raw)
	}
	return runes, nil
}

func newRunner(config Config) *Runner {
	r := &Runner{
		db:     tinysql.NewDB(),
		tenant: defaultTenant,
		config: config,
	}
	if config.CacheEnabled {
		r.queryCache = tinysql.NewQueryCache(config.CacheSize)
	}
	return r
}

func executeQuery(config Config) error {
	runner := newRunner(config)
	if err := runner.loadInputs(config.Files, config.TableName); err != nil {
		return err
	}

	if config.Verbose {
		fmt.Fprintf(os.Stderr, "Executing query: %s\n", config.Query)
	}

	result, duration, err := runner.executeSQL(config.Query)
	if err != nil {
		return err
	}

	if result != nil {
		if err := outputResults(result, config.Output); err != nil {
			return err
		}
		if config.Verbose {
			fmt.Fprintf(os.Stderr, "\n(%d rows in %v)\n", len(result.Rows), duration)
		}
	}
	return nil
}

func (r *Runner) executeSQL(sqlText string) (*tinysql.ResultSet, time.Duration, error) {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return nil, 0, fmt.Errorf("sql must not be empty")
	}

	ctx := context.Background()
	cancel := func() {}
	if r.config.QueryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, r.config.QueryTimeout)
	}
	defer cancel()

	start := time.Now()

	if r.queryCache != nil {
		compiled, err := r.queryCache.Compile(sqlText)
		if err != nil {
			return nil, time.Since(start), fmt.Errorf("parse error: %w", err)
		}
		result, err := tinysql.ExecuteCompiled(ctx, r.db, r.tenant, compiled)
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, time.Since(start), fmt.Errorf("query timeout after %s", r.config.QueryTimeout)
			}
			return nil, time.Since(start), fmt.Errorf("execute error: %w", err)
		}
		return result, time.Since(start), nil
	}

	stmt, err := tinysql.ParseSQL(sqlText)
	if err != nil {
		return nil, time.Since(start), fmt.Errorf("parse error: %w", err)
	}
	result, err := tinysql.Execute(ctx, r.db, r.tenant, stmt)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, time.Since(start), fmt.Errorf("query timeout after %s", r.config.QueryTimeout)
		}
		return nil, time.Since(start), fmt.Errorf("execute error: %w", err)
	}
	return result, time.Since(start), nil
}

func (r *Runner) loadInputs(inputs []string, explicitTable string) error {
	jobs, err := collectLoadJobs(inputs, explicitTable)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return fmt.Errorf("no supported files found to load")
	}

	if r.config.ParallelLoad && len(jobs) > 1 {
		return r.loadJobsParallel(jobs)
	}

	for _, job := range jobs {
		if r.config.Verbose {
			fmt.Fprintf(os.Stderr, "Loading %s into table '%s'...\n", job.file, job.tableName)
		}
		start := time.Now()
		if err := r.loadFile(job.file, job.tableName); err != nil {
			return fmt.Errorf("failed to load %s: %w", job.file, err)
		}
		if r.config.Verbose {
			fmt.Fprintf(os.Stderr, "✓ Loaded %s in %v\n", job.file, time.Since(start))
		}
	}
	return nil
}

func (r *Runner) loadJobsParallel(jobs []loadJob) error {
	workers := r.config.MaxWorkers
	if workers > len(jobs) {
		workers = len(jobs)
	}
	if workers <= 0 {
		workers = 1
	}

	jobCh := make(chan loadJob, len(jobs))
	errCh := make(chan error, len(jobs))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				start := time.Now()
				err := r.loadFile(job.file, job.tableName)
				if err != nil {
					errCh <- fmt.Errorf("failed to load %s: %w", job.file, err)
					continue
				}
				if r.config.Verbose {
					fmt.Fprintf(os.Stderr, "✓ Loaded %s in %v\n", job.file, time.Since(start))
				}
			}
		}()
	}

	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func collectLoadJobs(inputs []string, explicitTable string) ([]loadJob, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	files := make([]string, 0, len(inputs))
	for _, input := range inputs {
		expanded, err := expandInputPath(input)
		if err != nil {
			return nil, err
		}
		files = append(files, expanded...)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no supported files found")
	}

	if explicitTable != "" && len(files) > 1 {
		return nil, fmt.Errorf("-table can only be used with a single input file")
	}

	sort.Strings(files)
	jobs := make([]loadJob, 0, len(files))
	for _, file := range files {
		tableName := explicitTable
		if tableName == "" {
			tableName = getTableNameFromFile(file)
		}
		jobs = append(jobs, loadJob{file: file, tableName: tableName})
	}
	return jobs, nil
}

func expandInputPath(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}

	if !info.IsDir() {
		if !isSupportedFile(path) {
			return nil, fmt.Errorf("unsupported file format: %s", path)
		}
		return []string{path}, nil
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", path, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		candidate := filepath.Join(path, entry.Name())
		if isSupportedFile(candidate) {
			files = append(files, candidate)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no supported files found in directory %s", path)
	}
	return files, nil
}

func isSupportedFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv", ".tsv", ".txt", ".json", ".jsonl", ".ndjson":
		return true
	default:
		return false
	}
}

func (r *Runner) loadFile(filename, tableName string) error {
	tableNamesMux.Lock()
	tableNames[filename] = tableName
	tableNamesMux.Unlock()

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	ctx := context.Background()
	ext := strings.ToLower(filepath.Ext(filename))

	if r.config.FuzzyImport {
		opts := &tinysql.FuzzyImportOptions{
			ImportOptions: &tinysql.ImportOptions{
				CreateTable:   true,
				Truncate:      false,
				HeaderMode:    "auto",
				TypeInference: true,
				TableName:     tableName,
			},
			SkipInvalidRows:    true,
			TrimWhitespace:     true,
			FixQuotes:          true,
			CoerceTypes:        true,
			AllowMixedTypes:    true,
			MaxSkippedRows:     100,
			FuzzyJSON:          true,
			RemoveInvalidChars: true,
			AutoFixDelimiters:  true,
		}
		if len(r.config.DelimiterCandidates) > 0 {
			opts.DelimiterCandidates = append([]rune(nil), r.config.DelimiterCandidates...)
		}

		var result *tinysql.ImportResult
		switch ext {
		case ".csv", ".tsv", ".txt":
			result, err = tinysql.FuzzyImportCSV(ctx, r.db, r.tenant, tableName, file, opts)
		case ".json", ".jsonl", ".ndjson":
			result, err = tinysql.FuzzyImportJSON(ctx, r.db, r.tenant, tableName, file, opts)
		default:
			return fmt.Errorf("unsupported file format %s (supported: .csv, .tsv, .txt, .json, .jsonl, .ndjson)", ext)
		}
		if err != nil {
			return err
		}

		if r.config.Verbose {
			if len(result.Errors) > 0 {
				fmt.Fprintf(os.Stderr, "  Warnings for %s:\n", filename)
				for i, msg := range result.Errors {
					if i >= 5 {
						fmt.Fprintf(os.Stderr, "  ... and %d more warnings\n", len(result.Errors)-5)
						break
					}
					fmt.Fprintf(os.Stderr, "  - %s\n", msg)
				}
			}
			fmt.Fprintf(os.Stderr, "  Imported %d rows, skipped %d rows\n", result.RowsInserted, result.RowsSkipped)
		}
		return nil
	}

	opts := &tinysql.ImportOptions{
		CreateTable:   true,
		Truncate:      false,
		HeaderMode:    "auto",
		TypeInference: true,
		TableName:     tableName,
	}
	if len(r.config.DelimiterCandidates) > 0 {
		opts.DelimiterCandidates = append([]rune(nil), r.config.DelimiterCandidates...)
	}

	var result *tinysql.ImportResult
	switch ext {
	case ".csv", ".tsv", ".txt":
		result, err = tinysql.ImportCSV(ctx, r.db, r.tenant, tableName, file, opts)
	case ".json", ".jsonl", ".ndjson":
		result, err = tinysql.ImportJSON(ctx, r.db, r.tenant, tableName, file, opts)
	default:
		return fmt.Errorf("unsupported file format %s (supported: .csv, .tsv, .txt, .json, .jsonl, .ndjson)", ext)
	}
	if err != nil {
		return err
	}
	if r.config.Verbose {
		fmt.Fprintf(os.Stderr, "  Imported %d rows\n", result.RowsInserted)
	}
	return nil
}

func runInteractiveMode(config Config) {
	runner := newRunner(config)

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  tinySQL query_files - Interactive Mode                       ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Type 'help' for commands, 'exit' to quit")
	fmt.Println()

	if len(config.Files) > 0 {
		if err := runner.loadInputs(config.Files, config.TableName); err != nil {
			fmt.Printf("⚠ Warning: %v\n", err)
		}
		fmt.Println()
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	for {
		fmt.Print("sql> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		switch strings.ToLower(input) {
		case "exit", "quit", "q":
			fmt.Println("Goodbye!")
			return
		case "help", "h":
			printHelp()
			continue
		case "tables", "show tables", ".tables":
			showTables()
			continue
		case "stats", ".stats":
			showStats(runner)
			continue
		case "clear cache", ".clear":
			runner.clearCache()
			fmt.Println("✓ Cache cleared")
			continue
		}

		if strings.HasPrefix(strings.ToLower(input), "load ") {
			path := strings.TrimSpace(strings.TrimPrefix(input, "load "))
			if path == "" {
				fmt.Println("✗ Usage: load <file-or-directory>")
				continue
			}
			if err := runner.loadInputs([]string{path}, ""); err != nil {
				fmt.Printf("✗ Error: %v\n", err)
			} else {
				fmt.Println("✓ Load complete")
			}
			continue
		}

		result, duration, err := runner.executeSQL(input)
		if err != nil {
			fmt.Printf("✗ %v\n", err)
			continue
		}

		if result != nil {
			if err := outputResults(result, config.Output); err != nil {
				fmt.Printf("✗ Output error: %v\n", err)
				continue
			}
			fmt.Printf("\n(%d rows in %v)\n\n", len(result.Rows), duration)
		} else {
			fmt.Printf("✓ OK (%v)\n\n", duration)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("✗ Input error: %v\n", err)
	}
}

func (r *Runner) clearCache() {
	if r.queryCache != nil {
		r.queryCache.Clear()
	}
}

func showStats(r *Runner) {
	tableNamesMux.RLock()
	tCount := len(tableNames)
	tableNamesMux.RUnlock()

	cacheSize := 0
	cacheMax := 0
	if r.queryCache != nil {
		cacheSize = r.queryCache.Size()
		stats := r.queryCache.Stats()
		if v, ok := stats["maxSize"].(int); ok {
			cacheMax = v
		}
	}

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Runtime Statistics                                            ║")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Loaded tables:  %-45d ║\n", tCount)
	fmt.Printf("║  Query cache size: %-42d ║\n", cacheSize)
	fmt.Printf("║  Query cache max:  %-42d ║\n", cacheMax)
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}

func showTables() {
	tableNamesMux.RLock()
	defer tableNamesMux.RUnlock()

	if len(tableNames) == 0 {
		fmt.Println("No tables loaded")
		return
	}

	keys := make([]string, 0, len(tableNames))
	for filename := range tableNames {
		keys = append(keys, filename)
	}
	sort.Strings(keys)

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Loaded Tables                                                 ║")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	for _, filename := range keys {
		tableName := tableNames[filename]
		fmt.Printf("║  %-30s → %-30s ║\n", filepath.Base(filename), tableName)
	}
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}

func outputResults(result *tinysql.ResultSet, format string) error {
	switch format {
	case "json":
		return outputJSON(result)
	case "csv":
		return outputCSV(result)
	default:
		outputTable(result)
		return nil
	}
}

func outputTable(result *tinysql.ResultSet) {
	if len(result.Rows) == 0 {
		fmt.Println("No results")
		return
	}

	lowerCols := make([]string, len(result.Cols))
	widths := make([]int, len(result.Cols))
	for i, col := range result.Cols {
		lowerCols[i] = strings.ToLower(col)
		widths[i] = len(col)
	}

	for _, row := range result.Rows {
		for i, lc := range lowerCols {
			if value, ok := row[lc]; ok {
				str := fmt.Sprintf("%v", value)
				if len(str) > widths[i] {
					widths[i] = len(str)
				}
			}
		}
	}

	for i := range widths {
		if widths[i] > 50 {
			widths[i] = 50
		}
	}

	for i, col := range result.Cols {
		if len(col) > widths[i] && widths[i] > 3 {
			col = col[:widths[i]-3] + "..."
		}
		fmt.Printf("%-*s  ", widths[i], col)
	}
	fmt.Println()

	for i := range result.Cols {
		fmt.Print(strings.Repeat("─", widths[i]) + "  ")
	}
	fmt.Println()

	for _, row := range result.Rows {
		for i, lc := range lowerCols {
			value := ""
			if v, ok := row[lc]; ok && v != nil {
				value = fmt.Sprintf("%v", v)
				if len(value) > widths[i] && widths[i] > 3 {
					value = value[:widths[i]-3] + "..."
				}
			}
			fmt.Printf("%-*s  ", widths[i], value)
		}
		fmt.Println()
	}
}

func outputJSON(result *tinysql.ResultSet) error {
	records := make([]map[string]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		record := make(map[string]any, len(result.Cols))
		for _, col := range result.Cols {
			record[col] = row[strings.ToLower(col)]
		}
		records = append(records, record)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(records)
}

func outputCSV(result *tinysql.ResultSet) error {
	writer := csv.NewWriter(os.Stdout)
	if err := writer.Write(result.Cols); err != nil {
		return err
	}

	for _, row := range result.Rows {
		record := make([]string, len(result.Cols))
		for i, col := range result.Cols {
			if value, ok := row[strings.ToLower(col)]; ok && value != nil {
				record[i] = fmt.Sprintf("%v", value)
			}
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func printHelp() {
	fmt.Print(`
╔════════════════════════════════════════════════════════════════╗
║  Available Commands                                            ║
╠════════════════════════════════════════════════════════════════╣
║  help, h                   - Show this help                    ║
║  exit, quit, q             - Exit the program                  ║
║  tables, show tables       - Show loaded tables                ║
║  stats                     - Show cache/runtime statistics     ║
║  clear cache               - Clear query cache                 ║
║  load <file-or-dir>        - Load file(s) into tables          ║
╚════════════════════════════════════════════════════════════════╝
`)
}

func getTableNameFromFile(filename string) string {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := sanitizeTableName(strings.TrimSuffix(base, ext))
	if name == "" {
		return "table"
	}
	return name
}

func sanitizeTableName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, name)
	name = strings.Trim(name, "_")
	if name == "" {
		return ""
	}
	if name[0] >= '0' && name[0] <= '9' {
		name = "t_" + name
	}
	return strings.ToLower(name)
}
