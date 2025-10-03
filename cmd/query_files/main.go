package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

type Config struct {
	Files        []string
	Query        string
	TableName    string
	Delimiter    string
	Interactive  bool
	Verbose      bool
	Output       string
	FuzzyImport  bool
	CacheEnabled bool
	ParallelLoad bool
	MaxWorkers   int
}

// Cache for parsed queries
var (
	queryCache    = make(map[string]tinysql.Statement)
	queryCacheMux sync.RWMutex
	tableNames    = make(map[string]string) // file -> tableName mapping
	tableNamesMux sync.RWMutex
)

func main() {
	config := parseFlags()

	if config.Interactive {
		runInteractiveMode(config)
	} else {
		if len(config.Files) == 0 || config.Query == "" {
			fmt.Fprintf(os.Stderr, "Error: Both file and query are required in non-interactive mode\n")
			flag.Usage()
			os.Exit(1)
		}
		err := executeQuery(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}
}

func parseFlags() Config {
	var config Config

	flag.StringVar(&config.Query, "query", "", "SQL query to execute")
	flag.StringVar(&config.TableName, "table", "", "Table name (default: filename without extension)")
	flag.StringVar(&config.Delimiter, "delimiter", ",", "CSV delimiter (or auto-detect)")
	flag.BoolVar(&config.Interactive, "interactive", false, "Run in interactive mode")
	flag.BoolVar(&config.Verbose, "verbose", false, "Verbose output with timing and statistics")
	flag.StringVar(&config.Output, "output", "table", "Output format: table, json, csv")
	flag.BoolVar(&config.FuzzyImport, "fuzzy", true, "Enable fuzzy import for malformed files")
	flag.BoolVar(&config.CacheEnabled, "cache", true, "Enable query caching for better performance")
	flag.BoolVar(&config.ParallelLoad, "parallel", false, "Load files in parallel")
	flag.IntVar(&config.MaxWorkers, "workers", 4, "Number of parallel workers (with -parallel)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "tinySQL File Query Tool (Optimized with Fuzzy Import)\n\n")
		fmt.Fprintf(os.Stderr, "Query CSV, JSON, and other data files using SQL with intelligent fuzzy parsing.\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s [options] file1 [file2 ...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Simple query with automatic fuzzy parsing\n")
		fmt.Fprintf(os.Stderr, "  %s -query \"SELECT * FROM users WHERE age > 25\" users.csv\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Join multiple files with parallel loading\n")
		fmt.Fprintf(os.Stderr, "  %s -parallel -query \"SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id\" users.csv orders.json\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Interactive mode for data exploration\n")
		fmt.Fprintf(os.Stderr, "  %s -interactive data/\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Strict mode (disable fuzzy parsing)\n")
		fmt.Fprintf(os.Stderr, "  %s -fuzzy=false -query \"SELECT * FROM data\" data.csv\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # JSON output for piping\n")
		fmt.Fprintf(os.Stderr, "  %s -output json -query \"SELECT name, email FROM users\" users.csv | jq '.'\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Fuzzy Import Features (enabled by default):\n")
		fmt.Fprintf(os.Stderr, "  • Auto-detects delimiters (comma, semicolon, tab, pipe)\n")
		fmt.Fprintf(os.Stderr, "  • Handles inconsistent column counts (pads/truncates)\n")
		fmt.Fprintf(os.Stderr, "  • Fixes unmatched quotes automatically\n")
		fmt.Fprintf(os.Stderr, "  • Parses numbers with thousand separators\n")
		fmt.Fprintf(os.Stderr, "  • Supports line-delimited JSON (NDJSON)\n")
		fmt.Fprintf(os.Stderr, "  • Removes invalid UTF-8 characters\n")
		fmt.Fprintf(os.Stderr, "  • Type coercion for mixed-type columns\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	config.Files = flag.Args()
	return config
}

func executeQuery(config Config) error {
	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	// Load files into database
	if config.ParallelLoad && len(config.Files) > 1 {
		err := loadFilesParallel(db, ctx, tenant, config)
		if err != nil {
			return err
		}
	} else {
		for _, file := range config.Files {
			tableName := config.TableName
			if tableName == "" {
				tableName = getTableNameFromFile(file)
			}

			if config.Verbose {
				fmt.Fprintf(os.Stderr, "Loading %s into table '%s'...\n", file, tableName)
			}

			start := time.Now()
			err := loadFile(db, ctx, tenant, file, tableName, config)
			if err != nil {
				return fmt.Errorf("failed to load %s: %v", file, err)
			}

			if config.Verbose {
				fmt.Fprintf(os.Stderr, "✓ Loaded %s in %v\n", file, time.Since(start))
			}
		}
	}

	// Execute query
	if config.Verbose {
		fmt.Fprintf(os.Stderr, "Executing query: %s\n", config.Query)
	}

	var stmt tinysql.Statement
	var err error

	if config.CacheEnabled {
		stmt, err = getCachedQuery(config.Query)
	} else {
		stmt, err = tinysql.ParseSQL(config.Query)
	}

	if err != nil {
		return fmt.Errorf("parse error: %v", err)
	}

	start := time.Now()
	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	duration := time.Since(start)

	if err != nil {
		return fmt.Errorf("execute error: %v", err)
	}

	// Output results
	if result != nil {
		outputResults(result, config.Output)
		if config.Verbose {
			fmt.Fprintf(os.Stderr, "\n(%d rows in %v)\n", len(result.Rows), duration)
		}
	}

	return nil
}

func loadFilesParallel(db *tinysql.DB, ctx context.Context, tenant string, config Config) error {
	type loadJob struct {
		file      string
		tableName string
	}

	jobs := make(chan loadJob, len(config.Files))
	errors := make(chan error, len(config.Files))
	var wg sync.WaitGroup

	// Start workers
	for w := 0; w < config.MaxWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				err := loadFile(db, ctx, tenant, job.file, job.tableName, config)
				if err != nil {
					errors <- fmt.Errorf("failed to load %s: %v", job.file, err)
				} else if config.Verbose {
					fmt.Fprintf(os.Stderr, "✓ Loaded %s\n", job.file)
				}
			}
		}()
	}

	// Queue jobs
	for _, file := range config.Files {
		tableName := config.TableName
		if tableName == "" {
			tableName = getTableNameFromFile(file)
		}
		jobs <- loadJob{file: file, tableName: tableName}
	}
	close(jobs)

	// Wait for completion
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func loadFile(db *tinysql.DB, ctx context.Context, tenant, filename, tableName string, config Config) error {
	// Track table name for later reference
	tableNamesMux.Lock()
	tableNames[filename] = tableName
	tableNamesMux.Unlock()

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(filename))

	if config.FuzzyImport {
		// Use fuzzy importer
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

		if config.Delimiter != "" && config.Delimiter != "," {
			opts.DelimiterCandidates = []rune{rune(config.Delimiter[0])}
		}

		var result *tinysql.ImportResult

		switch ext {
		case ".csv", ".tsv", ".txt":
			result, err = tinysql.FuzzyImportCSV(ctx, db, tenant, tableName, file, opts)
		case ".json", ".jsonl", ".ndjson":
			result, err = tinysql.FuzzyImportJSON(ctx, db, tenant, tableName, file, opts)
		default:
			return fmt.Errorf("unsupported file format: %s (supported: .csv, .tsv, .txt, .json, .jsonl)", ext)
		}

		if err != nil {
			return err
		}

		if config.Verbose && len(result.Errors) > 0 {
			fmt.Fprintf(os.Stderr, "  Warnings for %s:\n", filename)
			for i, errMsg := range result.Errors {
				if i >= 5 {
					fmt.Fprintf(os.Stderr, "  ... and %d more warnings\n", len(result.Errors)-5)
					break
				}
				fmt.Fprintf(os.Stderr, "  - %s\n", errMsg)
			}
		}

		if config.Verbose {
			fmt.Fprintf(os.Stderr, "  Imported %d rows, skipped %d rows\n", result.RowsInserted, result.RowsSkipped)
		}
	} else {
		// Use standard importer
		opts := &tinysql.ImportOptions{
			CreateTable:   true,
			Truncate:      false,
			HeaderMode:    "auto",
			TypeInference: true,
			TableName:     tableName,
		}

		if config.Delimiter != "" {
			opts.DelimiterCandidates = []rune{rune(config.Delimiter[0])}
		}

		var result *tinysql.ImportResult

		switch ext {
		case ".csv", ".tsv", ".txt":
			result, err = tinysql.ImportCSV(ctx, db, tenant, tableName, file, opts)
		case ".json", ".jsonl", ".ndjson":
			result, err = tinysql.ImportJSON(ctx, db, tenant, tableName, file, opts)
		default:
			return fmt.Errorf("unsupported file format: %s", ext)
		}

		if err != nil {
			return err
		}

		if config.Verbose {
			fmt.Fprintf(os.Stderr, "  Imported %d rows\n", result.RowsInserted)
		}
	}

	return nil
}

func runInteractiveMode(config Config) {
	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  tinySQL File Query Tool - Interactive Mode                   ║")
	fmt.Println("║  Features: Fuzzy parsing, query caching, parallel loading     ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Type 'help' for commands, 'exit' to quit")
	fmt.Println()

	// Load files from directory if provided
	if len(config.Files) > 0 {
		for _, path := range config.Files {
			if isDirectory(path) {
				loadDirectory(db, ctx, tenant, path, config)
			} else {
				tableName := getTableNameFromFile(path)
				err := loadFile(db, ctx, tenant, path, tableName, config)
				if err != nil {
					fmt.Printf("⚠ Warning: Failed to load %s: %v\n", path, err)
				} else {
					fmt.Printf("✓ Loaded %s as table '%s'\n", path, tableName)
				}
			}
		}
		fmt.Println()
	}

	// Interactive loop
	scanner := bufio.NewScanner(os.Stdin)
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
			showStats()
			continue
		case "clear cache", ".clear":
			clearCache()
			fmt.Println("✓ Cache cleared")
			continue
		}

		// Handle load command
		if strings.HasPrefix(strings.ToLower(input), "load ") {
			parts := strings.SplitN(input, " ", 2)
			if len(parts) == 2 {
				file := strings.TrimSpace(parts[1])
				tableName := getTableNameFromFile(file)
				err := loadFile(db, ctx, tenant, file, tableName, config)
				if err != nil {
					fmt.Printf("✗ Error: %v\n", err)
				} else {
					fmt.Printf("✓ Loaded %s as table '%s'\n", file, tableName)
				}
			}
			continue
		}

		// Execute SQL query
		var stmt tinysql.Statement
		var err error

		if config.CacheEnabled {
			stmt, err = getCachedQuery(input)
		} else {
			stmt, err = tinysql.ParseSQL(input)
		}

		if err != nil {
			fmt.Printf("✗ Parse error: %v\n", err)
			continue
		}

		start := time.Now()
		result, err := tinysql.Execute(ctx, db, tenant, stmt)
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("✗ Execute error: %v\n", err)
			continue
		}

		if result != nil {
			outputResults(result, "table")
			fmt.Printf("\n(%d rows in %v)\n\n", len(result.Rows), duration)
		} else {
			fmt.Printf("✓ OK (%v)\n\n", duration)
		}
	}
}

func getCachedQuery(queryStr string) (tinysql.Statement, error) {
	queryCacheMux.RLock()
	if stmt, exists := queryCache[queryStr]; exists {
		queryCacheMux.RUnlock()
		return stmt, nil
	}
	queryCacheMux.RUnlock()

	// Parse query
	stmt, err := tinysql.ParseSQL(queryStr)
	if err != nil {
		return nil, err
	}

	// Cache it
	queryCacheMux.Lock()
	queryCache[queryStr] = stmt
	queryCacheMux.Unlock()

	return stmt, nil
}

func clearCache() {
	queryCacheMux.Lock()
	queryCache = make(map[string]tinysql.Statement)
	queryCacheMux.Unlock()
}

func showStats() {
	queryCacheMux.RLock()
	qCount := len(queryCache)
	queryCacheMux.RUnlock()

	tableNamesMux.RLock()
	tCount := len(tableNames)
	tableNamesMux.RUnlock()

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Cache Statistics                                              ║")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Cached queries: %-45d ║\n", qCount)
	fmt.Printf("║  Loaded tables:  %-45d ║\n", tCount)
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}

func showTables() {
	tableNamesMux.RLock()
	defer tableNamesMux.RUnlock()

	if len(tableNames) == 0 {
		fmt.Println("No tables loaded")
		return
	}

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Loaded Tables                                                 ║")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	for filename, tableName := range tableNames {
		fmt.Printf("║  %-30s → %-30s ║\n", filepath.Base(filename), tableName)
	}
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}

func loadDirectory(db *tinysql.DB, ctx context.Context, tenant, dir string, config Config) {
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("⚠ Warning: Failed to read directory %s: %v\n", dir, err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := filepath.Join(dir, file.Name())
		ext := strings.ToLower(filepath.Ext(file.Name()))

		if ext == ".csv" || ext == ".json" || ext == ".tsv" || ext == ".txt" || ext == ".jsonl" || ext == ".ndjson" {
			tableName := getTableNameFromFile(file.Name())
			err := loadFile(db, ctx, tenant, filename, tableName, config)
			if err != nil {
				fmt.Printf("⚠ Warning: Failed to load %s: %v\n", filename, err)
			} else {
				fmt.Printf("✓ Loaded %s as table '%s'\n", filename, tableName)
			}
		}
	}
}

func outputResults(result *tinysql.ResultSet, format string) {
	switch format {
	case "json":
		outputJSON(result)
	case "csv":
		outputCSV(result)
	default:
		outputTable(result)
	}
}

func outputTable(result *tinysql.ResultSet) {
	if len(result.Rows) == 0 {
		fmt.Println("No results")
		return
	}

	// Calculate column widths
	widths := make([]int, len(result.Cols))
	for i, col := range result.Cols {
		widths[i] = len(col)
	}

	for _, row := range result.Rows {
		for i, col := range result.Cols {
			if value, ok := row[strings.ToLower(col)]; ok {
				str := fmt.Sprintf("%v", value)
				if len(str) > widths[i] {
					widths[i] = len(str)
				}
			}
		}
	}

	// Limit column width to 50 characters for readability
	for i := range widths {
		if widths[i] > 50 {
			widths[i] = 50
		}
	}

	// Print header
	for i, col := range result.Cols {
		if len(col) > widths[i] {
			col = col[:widths[i]-3] + "..."
		}
		fmt.Printf("%-*s  ", widths[i], col)
	}
	fmt.Println()

	// Print separator
	for i := range result.Cols {
		fmt.Print(strings.Repeat("─", widths[i]) + "  ")
	}
	fmt.Println()

	// Print rows
	for _, row := range result.Rows {
		for i, col := range result.Cols {
			value := ""
			if v, ok := row[strings.ToLower(col)]; ok && v != nil {
				value = fmt.Sprintf("%v", v)
				if len(value) > widths[i] {
					value = value[:widths[i]-3] + "..."
				}
			}
			fmt.Printf("%-*s  ", widths[i], value)
		}
		fmt.Println()
	}
}

func outputJSON(result *tinysql.ResultSet) {
	var records []map[string]interface{}
	for _, row := range result.Rows {
		record := make(map[string]interface{})
		for _, col := range result.Cols {
			if value, ok := row[strings.ToLower(col)]; ok {
				record[col] = value
			}
		}
		records = append(records, record)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.Encode(records)
}

func outputCSV(result *tinysql.ResultSet) {
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// Write header
	writer.Write(result.Cols)

	// Write rows
	for _, row := range result.Rows {
		record := make([]string, len(result.Cols))
		for i, col := range result.Cols {
			if value, ok := row[strings.ToLower(col)]; ok && value != nil {
				record[i] = fmt.Sprintf("%v", value)
			}
		}
		writer.Write(record)
	}
}

func printHelp() {
	fmt.Println(`
╔════════════════════════════════════════════════════════════════╗
║  Available Commands                                            ║
╠════════════════════════════════════════════════════════════════╣
║  help, h                   - Show this help                    ║
║  exit, quit, q             - Exit the program                  ║
║  tables, show tables       - Show loaded tables                ║
║  stats                     - Show cache statistics             ║
║  clear cache               - Clear query cache                 ║
║  load <file>               - Load a file into a table          ║
╠════════════════════════════════════════════════════════════════╣
║  SQL Examples                                                  ║
╠════════════════════════════════════════════════════════════════╣
║  SELECT * FROM users WHERE age > 25                            ║
║  SELECT name, email FROM users ORDER BY name                   ║
║  SELECT u.name, COUNT(o.id) AS orders                          ║
║    FROM users u LEFT JOIN orders o ON u.id = o.user_id         ║
║    GROUP BY u.name                                             ║
║  SELECT * FROM products                                        ║
║    WHERE JSON_GET(meta, 'category') = 'electronics'            ║
╠════════════════════════════════════════════════════════════════╣
║  Fuzzy Import Features (Auto-enabled)                          ║
╠════════════════════════════════════════════════════════════════╣
║  ✓ Auto-detects delimiters (comma, semicolon, tab, pipe)      ║
║  ✓ Handles malformed CSV with inconsistent columns            ║
║  ✓ Fixes unmatched quotes automatically                        ║
║  ✓ Parses numbers with thousand separators (1,234.56)         ║
║  ✓ Supports line-delimited JSON (NDJSON)                       ║
║  ✓ Removes invalid UTF-8 characters                            ║
║  ✓ Type coercion for mixed-type columns                        ║
╚════════════════════════════════════════════════════════════════╝
`)
}

func getTableNameFromFile(filename string) string {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	// Sanitize table name
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, name)

	return name
}

func isDirectory(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
