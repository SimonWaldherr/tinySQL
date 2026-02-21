package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"

	// External database drivers
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
	_ "modernc.org/sqlite"
)

// connRegistry stores named external database connections.
var (
	connRegistry    = make(map[string]*sql.DB)
	connDSN         = make(map[string]string)
	connDrivers     = make(map[string]string)
	connRegistryMux sync.RWMutex
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "interactive", "repl":
		runInteractive()
	case "import-file":
		if err := runImportFile(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "import-db":
		if err := runImportDB(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "export-file":
		if err := runExportFile(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "export-db":
		if err := runExportDB(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "pipeline":
		if err := runPipeline(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `tinySQL Data Migration Tool

A smart tool for data pipelines and processing. Import and export data
between CSV/JSON files, tinySQL, and external databases (MySQL/MariaDB,
PostgreSQL, SQLite, MS SQL Server).

Usage:
  migrate <command> [options]

Commands:
  interactive          Start interactive REPL for data migration
  import-file          Import a CSV/JSON file into tinySQL
  import-db            Import data from an external database into tinySQL
  export-file          Export a tinySQL table to CSV/JSON file
  export-db            Export a tinySQL table to an external database
  pipeline             Run a multi-step migration pipeline from a script
  help                 Show this help message

Examples:
  # Import a CSV file into tinySQL, query it, export results
  migrate import-file -file users.csv -table users -query "SELECT * FROM users WHERE age > 25" -output results.json

  # Import from PostgreSQL into tinySQL
  migrate import-db -dsn "postgres://user:pass@localhost/mydb?sslmode=disable" -query "SELECT * FROM users" -table users

  # Export tinySQL table to MySQL
  migrate export-db -dsn "mysql://user:pass@tcp(localhost:3306)/mydb" -table users -target users_copy

  # Interactive mode for building data pipelines
  migrate interactive

  # Run a pipeline script
  migrate pipeline -script migration.sql

Inter-database Queries (interactive mode):
  connect pg postgres://user:pass@localhost/mydb?sslmode=disable
  connect mysql mysql://user:pass@tcp(localhost:3306)/mydb
  import pg "SELECT * FROM users" AS users
  import pg "SELECT * FROM orders" AS orders
  SELECT u.name, COUNT(o.id) AS order_count FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name
  export mysql users_summary
  COPY SELECT * FROM users WHERE active=1 INTO mysql.users_active
`)
}

// ============================================================================
// import-file: Import CSV/JSON files into tinySQL
// ============================================================================

func runImportFile(args []string) error {
	fs := flag.NewFlagSet("import-file", flag.ExitOnError)
	file := fs.String("file", "", "Path to CSV/JSON file to import")
	table := fs.String("table", "", "Target table name (default: filename without extension)")
	query := fs.String("query", "", "SQL query to execute after import")
	output := fs.String("output", "", "Output file for query results (default: stdout)")
	format := fs.String("format", "table", "Output format: table, json, csv")
	fuzzy := fs.Bool("fuzzy", true, "Enable fuzzy import for malformed files")
	verbose := fs.Bool("verbose", false, "Verbose output")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *file == "" {
		// Check for positional argument
		if fs.NArg() > 0 {
			*file = fs.Arg(0)
		} else {
			return fmt.Errorf("file path is required (-file or positional argument)")
		}
	}

	tableName := *table
	if tableName == "" {
		tableName = tableNameFromFile(*file)
	}

	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	start := time.Now()
	if err := importFileToTinySQL(db, ctx, tenant, *file, tableName, *fuzzy, *verbose); err != nil {
		return err
	}
	if *verbose {
		fmt.Fprintf(os.Stderr, "✓ Imported %s as table '%s' in %v\n", *file, tableName, time.Since(start))
	}

	if *query != "" {
		return executeAndOutput(db, ctx, tenant, *query, *output, *format, *verbose)
	}

	return nil
}

// ============================================================================
// import-db: Import from external database into tinySQL
// ============================================================================

func runImportDB(args []string) error {
	fs := flag.NewFlagSet("import-db", flag.ExitOnError)
	dsn := fs.String("dsn", "", "Database connection string (e.g., postgres://user:pass@host/db)")
	sourceQuery := fs.String("query", "", "SQL query to execute on source database")
	sourceTable := fs.String("source-table", "", "Source table name (alternative to -query)")
	table := fs.String("table", "", "Target table name in tinySQL")
	tinyQuery := fs.String("tinyquery", "", "SQL query to execute on tinySQL after import")
	output := fs.String("output", "", "Output file for query results")
	format := fs.String("format", "table", "Output format: table, json, csv")
	verbose := fs.Bool("verbose", false, "Verbose output")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *dsn == "" {
		return fmt.Errorf("database DSN is required (-dsn)")
	}

	if *sourceQuery == "" && *sourceTable == "" {
		return fmt.Errorf("either -query or -source-table is required")
	}

	if *sourceQuery == "" {
		*sourceQuery = fmt.Sprintf("SELECT * FROM %s", *sourceTable)
	}

	if *table == "" {
		if *sourceTable != "" {
			*table = sanitizeTableName(*sourceTable)
		} else {
			*table = "imported"
		}
	}

	driver, connStr := parseDSN(*dsn)
	extDB, err := sql.Open(driver, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", driver, err)
	}
	defer extDB.Close()

	if err := extDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping %s: %v", driver, err)
	}

	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	start := time.Now()
	count, err := importFromExternal(db, ctx, tenant, extDB, *sourceQuery, *table, *verbose)
	if err != nil {
		return err
	}
	if *verbose {
		fmt.Fprintf(os.Stderr, "✓ Imported %d rows into table '%s' in %v\n", count, *table, time.Since(start))
	}

	if *tinyQuery != "" {
		return executeAndOutput(db, ctx, tenant, *tinyQuery, *output, *format, *verbose)
	}

	return nil
}

// ============================================================================
// export-file: Export tinySQL table to CSV/JSON file
// ============================================================================

func runExportFile(args []string) error {
	fs := flag.NewFlagSet("export-file", flag.ExitOnError)
	files := fs.String("files", "", "Comma-separated input files to load first")
	query := fs.String("query", "", "SQL query to select data for export")
	table := fs.String("table", "", "Table name to export (exports all rows)")
	output := fs.String("output", "", "Output file path (required)")
	format := fs.String("format", "", "Output format: csv, json (auto-detected from extension)")
	verbose := fs.Bool("verbose", false, "Verbose output")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *output == "" {
		return fmt.Errorf("output file is required (-output)")
	}

	if *query == "" && *table == "" {
		return fmt.Errorf("either -query or -table is required")
	}

	if *query == "" {
		*query = fmt.Sprintf("SELECT * FROM %s", *table)
	}

	if *format == "" {
		ext := strings.ToLower(filepath.Ext(*output))
		switch ext {
		case ".json":
			*format = "json"
		default:
			*format = "csv"
		}
	}

	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	// Load input files
	if *files != "" {
		for _, f := range strings.Split(*files, ",") {
			f = strings.TrimSpace(f)
			tableName := tableNameFromFile(f)
			if err := importFileToTinySQL(db, ctx, tenant, f, tableName, true, *verbose); err != nil {
				return fmt.Errorf("failed to load %s: %v", f, err)
			}
		}
	}

	return executeAndOutput(db, ctx, tenant, *query, *output, *format, *verbose)
}

// ============================================================================
// export-db: Export tinySQL table to external database
// ============================================================================

func runExportDB(args []string) error {
	fs := flag.NewFlagSet("export-db", flag.ExitOnError)
	dsn := fs.String("dsn", "", "Target database connection string")
	files := fs.String("files", "", "Comma-separated input files to load first")
	query := fs.String("query", "", "SQL query to select data for export")
	table := fs.String("table", "", "Source table name in tinySQL")
	target := fs.String("target", "", "Target table name in external database")
	createTable := fs.Bool("create", true, "Create target table if it doesn't exist")
	verbose := fs.Bool("verbose", false, "Verbose output")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *dsn == "" {
		return fmt.Errorf("target DSN is required (-dsn)")
	}

	if *query == "" && *table == "" {
		return fmt.Errorf("either -query or -table is required")
	}

	if *target == "" {
		if *table != "" {
			*target = *table
		} else {
			*target = "exported"
		}
	}

	if *query == "" {
		*query = fmt.Sprintf("SELECT * FROM %s", *table)
	}

	driver, connStr := parseDSN(*dsn)
	extDB, err := sql.Open(driver, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", driver, err)
	}
	defer extDB.Close()

	if err := extDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping %s: %v", driver, err)
	}

	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	// Load input files
	if *files != "" {
		for _, f := range strings.Split(*files, ",") {
			f = strings.TrimSpace(f)
			tableName := tableNameFromFile(f)
			if err := importFileToTinySQL(db, ctx, tenant, f, tableName, true, *verbose); err != nil {
				return fmt.Errorf("failed to load %s: %v", f, err)
			}
		}
	}

	// Execute query
	stmt, err := tinysql.ParseSQL(*query)
	if err != nil {
		return fmt.Errorf("parse error: %v", err)
	}

	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		return fmt.Errorf("execute error: %v", err)
	}

	if result == nil || len(result.Rows) == 0 {
		if *verbose {
			fmt.Fprintf(os.Stderr, "No data to export\n")
		}
		return nil
	}

	start := time.Now()
	count, err := exportToExternal(extDB, driver, result, *target, *createTable)
	if err != nil {
		return err
	}

	if *verbose {
		fmt.Fprintf(os.Stderr, "✓ Exported %d rows to %s.%s in %v\n", count, driver, *target, time.Since(start))
	}

	return nil
}

// ============================================================================
// pipeline: Run multi-step migration from a script
// ============================================================================

func runPipeline(args []string) error {
	fs := flag.NewFlagSet("pipeline", flag.ExitOnError)
	script := fs.String("script", "", "Path to pipeline script file")
	verbose := fs.Bool("verbose", false, "Verbose output")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *script == "" {
		if fs.NArg() > 0 {
			*script = fs.Arg(0)
		} else {
			return fmt.Errorf("script file is required (-script or positional argument)")
		}
	}

	data, err := os.ReadFile(*script)
	if err != nil {
		return fmt.Errorf("failed to read script: %v", err)
	}

	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "#") {
			continue
		}
		if *verbose {
			fmt.Fprintf(os.Stderr, "[%d] %s\n", i+1, line)
		}
		if err := executePipelineCommand(db, ctx, tenant, line, *verbose); err != nil {
			return fmt.Errorf("line %d: %v", i+1, err)
		}
	}

	return nil
}

// ============================================================================
// Interactive REPL
// ============================================================================

func runInteractive() {
	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  tinySQL Data Migration Tool - Interactive Mode               ║")
	fmt.Println("║  Transfer data between files, tinySQL, and external databases ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Type 'help' for commands, 'exit' to quit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	for {
		fmt.Print("migrate> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		lower := strings.ToLower(input)

		switch {
		case lower == "exit" || lower == "quit" || lower == "q":
			closeAllConnections()
			fmt.Println("Goodbye!")
			return
		case lower == "help" || lower == "h":
			printInteractiveHelp()
		case lower == "tables" || lower == "show tables" || lower == ".tables":
			showTinyTables(db, tenant)
		case lower == "connections" || lower == "show connections":
			showConnections()
		case strings.HasPrefix(lower, "connect "):
			handleConnect(input)
		case strings.HasPrefix(lower, "disconnect "):
			handleDisconnect(input)
		case strings.HasPrefix(lower, "load ") || strings.HasPrefix(lower, "import file "):
			handleLoadFile(db, ctx, tenant, input)
		case strings.HasPrefix(lower, "import "):
			handleImportDB(db, ctx, tenant, input)
		case strings.HasPrefix(lower, "export "):
			handleExport(db, ctx, tenant, input)
		case strings.HasPrefix(lower, "copy "):
			handleCopy(db, ctx, tenant, input)
		default:
			// Execute as SQL query in tinySQL
			executeInteractiveSQL(db, ctx, tenant, input)
		}
	}
}

func printInteractiveHelp() {
	fmt.Print(`
╔════════════════════════════════════════════════════════════════╗
║  Commands                                                      ║
╠════════════════════════════════════════════════════════════════╣
║  Connection Management                                         ║
║  ─────────────────────                                         ║
║  connect <name> <dsn>           Register external database     ║
║  disconnect <name>              Close a connection              ║
║  connections                    List active connections         ║
║                                                                ║
║  Data Import                                                   ║
║  ───────────                                                   ║
║  load <file>                    Load CSV/JSON into tinySQL     ║
║  load <file> AS <table>         Load with explicit table name  ║
║  import <conn> <table>          Import table from ext DB       ║
║  import <conn> "<query>" AS <t> Import query result as table   ║
║                                                                ║
║  Data Export                                                   ║
║  ───────────                                                   ║
║  export file <query> TO <file>  Export query results to file   ║
║  export <conn> <table>          Export tinySQL table to ext DB ║
║                                                                ║
║  Cross-Database Operations                                     ║
║  ────────────────────────                                      ║
║  COPY SELECT ... INTO <conn>.<table>                           ║
║    Execute query in tinySQL, write results to ext DB           ║
║                                                                ║
║  SQL Queries                                                   ║
║  ───────────                                                   ║
║  Any SQL statement is executed against the tinySQL engine      ║
║                                                                ║
║  General                                                       ║
║  ───────                                                       ║
║  tables                         Show loaded tinySQL tables     ║
║  help                           Show this help                 ║
║  exit                           Exit the program               ║
╚════════════════════════════════════════════════════════════════╝

DSN Formats:
  PostgreSQL:  postgres://user:pass@host:5432/dbname?sslmode=disable
  MySQL:       mysql://user:pass@tcp(host:3306)/dbname
  SQLite:      sqlite://path/to/database.db
  MS SQL:      mssql://user:pass@host:1433?database=dbname
`)
}

// ============================================================================
// Connection Management
// ============================================================================

func handleConnect(input string) {
	// connect <name> <dsn>
	parts := strings.SplitN(input, " ", 3)
	if len(parts) < 3 {
		fmt.Println("✗ Usage: connect <name> <dsn>")
		return
	}

	name := parts[1]
	dsn := parts[2]

	driver, connStr := parseDSN(dsn)

	extDB, err := sql.Open(driver, connStr)
	if err != nil {
		fmt.Printf("✗ Failed to open %s connection: %v\n", driver, err)
		return
	}

	if err := extDB.Ping(); err != nil {
		extDB.Close()
		fmt.Printf("✗ Failed to connect to %s: %v\n", driver, err)
		return
	}

	connRegistryMux.Lock()
	if old, exists := connRegistry[name]; exists {
		old.Close()
	}
	connRegistry[name] = extDB
	connDSN[name] = dsn
	connDrivers[name] = driver
	connRegistryMux.Unlock()

	fmt.Printf("✓ Connected '%s' (%s)\n", name, driver)
}

func handleDisconnect(input string) {
	parts := strings.Fields(input)
	if len(parts) < 2 {
		fmt.Println("✗ Usage: disconnect <name>")
		return
	}
	name := parts[1]

	connRegistryMux.Lock()
	defer connRegistryMux.Unlock()

	if db, exists := connRegistry[name]; exists {
		db.Close()
		delete(connRegistry, name)
		delete(connDSN, name)
		delete(connDrivers, name)
		fmt.Printf("✓ Disconnected '%s'\n", name)
	} else {
		fmt.Printf("✗ Connection '%s' not found\n", name)
	}
}

func showConnections() {
	connRegistryMux.RLock()
	defer connRegistryMux.RUnlock()

	if len(connRegistry) == 0 {
		fmt.Println("No active connections")
		return
	}

	fmt.Println("Active Connections:")
	for name, dsn := range connDSN {
		driver := connDrivers[name]
		// Mask password in display
		fmt.Printf("  %-15s  %-10s  %s\n", name, driver, maskDSN(dsn))
	}
}

func getConnection(name string) (*sql.DB, string, error) {
	connRegistryMux.RLock()
	defer connRegistryMux.RUnlock()

	db, exists := connRegistry[name]
	if !exists {
		return nil, "", fmt.Errorf("connection '%s' not found (use 'connect' first)", name)
	}
	return db, connDrivers[name], nil
}

func closeAllConnections() {
	connRegistryMux.Lock()
	defer connRegistryMux.Unlock()

	for name, db := range connRegistry {
		db.Close()
		delete(connRegistry, name)
	}
}

// ============================================================================
// Interactive Command Handlers
// ============================================================================

func handleLoadFile(db *tinysql.DB, ctx context.Context, tenant, input string) {
	// load <file> [AS <table>]
	raw := input
	if strings.HasPrefix(strings.ToLower(raw), "import file ") {
		raw = "load " + raw[len("import file "):]
	}

	parts := strings.Fields(raw)
	if len(parts) < 2 {
		fmt.Println("✗ Usage: load <file> [AS <table>]")
		return
	}

	file := parts[1]
	tableName := tableNameFromFile(file)

	// Check for AS clause
	for i, p := range parts {
		if strings.EqualFold(p, "AS") && i+1 < len(parts) {
			tableName = parts[i+1]
			break
		}
	}

	start := time.Now()
	if err := importFileToTinySQL(db, ctx, tenant, file, tableName, true, false); err != nil {
		fmt.Printf("✗ Error: %v\n", err)
		return
	}
	fmt.Printf("✓ Loaded %s as table '%s' (%v)\n", file, tableName, time.Since(start))
}

func handleImportDB(db *tinysql.DB, ctx context.Context, tenant, input string) {
	// import <conn> <table>
	// import <conn> "<query>" AS <table>
	parts := splitQuotedFields(input)
	if len(parts) < 3 {
		fmt.Println("✗ Usage: import <conn> <table-or-query> [AS <table>]")
		return
	}

	connName := parts[1]
	sourceExpr := parts[2]

	extDB, _, err := getConnection(connName)
	if err != nil {
		fmt.Printf("✗ %v\n", err)
		return
	}

	// Determine query and target table
	var query, tableName string
	if strings.Contains(strings.ToUpper(sourceExpr), "SELECT") {
		query = sourceExpr
		tableName = "imported"
	} else {
		query = fmt.Sprintf("SELECT * FROM %s", sourceExpr)
		tableName = sanitizeTableName(sourceExpr)
	}

	// Check for AS clause
	for i, p := range parts {
		if strings.EqualFold(p, "AS") && i+1 < len(parts) {
			tableName = parts[i+1]
			break
		}
	}

	start := time.Now()
	count, err := importFromExternal(db, ctx, tenant, extDB, query, tableName, false)
	if err != nil {
		fmt.Printf("✗ Error: %v\n", err)
		return
	}
	fmt.Printf("✓ Imported %d rows from %s into table '%s' (%v)\n", count, connName, tableName, time.Since(start))
}

func handleExport(db *tinysql.DB, ctx context.Context, tenant, input string) {
	lower := strings.ToLower(input)

	// export file <query> TO <file>
	if strings.HasPrefix(lower, "export file ") {
		handleExportFile(db, ctx, tenant, input)
		return
	}

	// export <conn> <table> [AS <target>]
	parts := strings.Fields(input)
	if len(parts) < 3 {
		fmt.Println("✗ Usage: export <conn> <table> [AS <target>]")
		return
	}

	connName := parts[1]
	tableName := parts[2]

	extDB, driver, err := getConnection(connName)
	if err != nil {
		fmt.Printf("✗ %v\n", err)
		return
	}

	targetTable := tableName
	for i, p := range parts {
		if strings.EqualFold(p, "AS") && i+1 < len(parts) {
			targetTable = parts[i+1]
			break
		}
	}

	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	stmt, err := tinysql.ParseSQL(query)
	if err != nil {
		fmt.Printf("✗ Parse error: %v\n", err)
		return
	}

	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		fmt.Printf("✗ Execute error: %v\n", err)
		return
	}

	if result == nil || len(result.Rows) == 0 {
		fmt.Println("No data to export")
		return
	}

	start := time.Now()
	count, err := exportToExternal(extDB, driver, result, targetTable, true)
	if err != nil {
		fmt.Printf("✗ Export error: %v\n", err)
		return
	}
	fmt.Printf("✓ Exported %d rows to %s.%s (%v)\n", count, connName, targetTable, time.Since(start))
}

func handleExportFile(db *tinysql.DB, ctx context.Context, tenant, input string) {
	// export file <query> TO <file>
	raw := input[len("export file "):]

	// Find TO keyword
	toIdx := -1
	upper := strings.ToUpper(raw)
	toIdx = strings.LastIndex(upper, " TO ")
	if toIdx == -1 {
		fmt.Println("✗ Usage: export file <query> TO <file>")
		return
	}

	query := strings.TrimSpace(raw[:toIdx])
	file := strings.TrimSpace(raw[toIdx+4:])

	// Determine format from extension
	format := "csv"
	ext := strings.ToLower(filepath.Ext(file))
	if ext == ".json" {
		format = "json"
	}

	if err := executeAndOutput(db, ctx, tenant, query, file, format, false); err != nil {
		fmt.Printf("✗ Error: %v\n", err)
		return
	}
	fmt.Printf("✓ Exported results to %s\n", file)
}

func handleCopy(db *tinysql.DB, ctx context.Context, tenant, input string) {
	// COPY SELECT ... INTO <conn>.<table>
	upper := strings.ToUpper(input)
	intoIdx := strings.LastIndex(upper, " INTO ")
	if intoIdx == -1 {
		fmt.Println("✗ Usage: COPY SELECT ... INTO <conn>.<table>")
		return
	}

	query := strings.TrimSpace(input[5:intoIdx]) // skip "COPY "
	target := strings.TrimSpace(input[intoIdx+6:])

	// Parse <conn>.<table>
	dotIdx := strings.Index(target, ".")
	if dotIdx == -1 {
		fmt.Println("✗ Target must be <connection>.<table>")
		return
	}

	connName := target[:dotIdx]
	targetTable := target[dotIdx+1:]

	extDB, driver, err := getConnection(connName)
	if err != nil {
		fmt.Printf("✗ %v\n", err)
		return
	}

	// Execute query in tinySQL
	stmt, err := tinysql.ParseSQL(query)
	if err != nil {
		fmt.Printf("✗ Parse error: %v\n", err)
		return
	}

	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		fmt.Printf("✗ Execute error: %v\n", err)
		return
	}

	if result == nil || len(result.Rows) == 0 {
		fmt.Println("No data to copy")
		return
	}

	start := time.Now()
	count, err := exportToExternal(extDB, driver, result, targetTable, true)
	if err != nil {
		fmt.Printf("✗ Export error: %v\n", err)
		return
	}
	fmt.Printf("✓ Copied %d rows to %s.%s (%v)\n", count, connName, targetTable, time.Since(start))
}

func executeInteractiveSQL(db *tinysql.DB, ctx context.Context, tenant, input string) {
	// Check for COPY ... INTO syntax
	upper := strings.ToUpper(strings.TrimSpace(input))
	if strings.HasPrefix(upper, "COPY ") {
		handleCopy(db, ctx, tenant, input)
		return
	}

	stmt, err := tinysql.ParseSQL(input)
	if err != nil {
		fmt.Printf("✗ Parse error: %v\n", err)
		return
	}

	start := time.Now()
	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("✗ Execute error: %v\n", err)
		return
	}

	if result != nil {
		outputTable(result)
		fmt.Printf("\n(%d rows in %v)\n\n", len(result.Rows), duration)
	} else {
		fmt.Printf("✓ OK (%v)\n\n", duration)
	}
}

func showTinyTables(db *tinysql.DB, tenant string) {
	tables := db.ListTables(tenant)
	if len(tables) == 0 {
		fmt.Println("No tables loaded")
		return
	}

	fmt.Println("Tables in tinySQL:")
	for _, t := range tables {
		fmt.Printf("  %-30s  %d rows, %d columns\n", t.Name, len(t.Rows), len(t.Cols))
	}
}

// ============================================================================
// Data Transfer Core
// ============================================================================

func importFileToTinySQL(db *tinysql.DB, ctx context.Context, tenant, filename, tableName string, fuzzy, verbose bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(filename))

	if fuzzy {
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

		switch ext {
		case ".csv", ".tsv", ".txt":
			result, err := tinysql.FuzzyImportCSV(ctx, db, tenant, tableName, file, opts)
			if err != nil {
				return err
			}
			if verbose && len(result.Errors) > 0 {
				for i, e := range result.Errors {
					if i >= 5 {
						fmt.Fprintf(os.Stderr, "  ... and %d more warnings\n", len(result.Errors)-5)
						break
					}
					fmt.Fprintf(os.Stderr, "  ⚠ %s\n", e)
				}
			}
		case ".json", ".jsonl", ".ndjson":
			_, err = tinysql.FuzzyImportJSON(ctx, db, tenant, tableName, file, opts)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported file format: %s (supported: .csv, .tsv, .txt, .json, .jsonl)", ext)
		}
	} else {
		opts := &tinysql.ImportOptions{
			CreateTable:   true,
			Truncate:      false,
			HeaderMode:    "auto",
			TypeInference: true,
			TableName:     tableName,
		}

		switch ext {
		case ".csv", ".tsv", ".txt":
			_, err = tinysql.ImportCSV(ctx, db, tenant, tableName, file, opts)
		case ".json", ".jsonl", ".ndjson":
			_, err = tinysql.ImportJSON(ctx, db, tenant, tableName, file, opts)
		default:
			return fmt.Errorf("unsupported file format: %s", ext)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func importFromExternal(db *tinysql.DB, ctx context.Context, tenant string, extDB *sql.DB, query, tableName string, verbose bool) (int, error) {
	rows, err := extDB.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("external query failed: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to get columns: %v", err)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("failed to get column types: %v", err)
	}

	// Build CREATE TABLE statement
	createSQL := buildCreateTable(tableName, cols, colTypes)
	createStmt, err := tinysql.ParseSQL(createSQL)
	if err != nil {
		return 0, fmt.Errorf("failed to parse CREATE TABLE: %v", err)
	}

	if _, err := tinysql.Execute(ctx, db, tenant, createStmt); err != nil {
		return 0, fmt.Errorf("failed to create table: %v", err)
	}

	// Insert rows
	count := 0
	for rows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "  ⚠ Skipping row: %v\n", err)
			}
			continue
		}

		insertSQL := buildInsert(tableName, cols, values)
		insertStmt, err := tinysql.ParseSQL(insertSQL)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "  ⚠ Skipping row (parse): %v\n", err)
			}
			continue
		}

		if _, err := tinysql.Execute(ctx, db, tenant, insertStmt); err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "  ⚠ Skipping row (insert): %v\n", err)
			}
			continue
		}
		count++
	}

	return count, rows.Err()
}

func exportToExternal(extDB *sql.DB, driver string, result *tinysql.ResultSet, targetTable string, createTable bool) (int, error) {
	if createTable {
		createSQL := buildExternalCreateTable(driver, targetTable, result.Cols)
		if _, err := extDB.Exec(createSQL); err != nil {
			// Table might already exist; log but continue with insert
			fmt.Fprintf(os.Stderr, "Note: CREATE TABLE skipped (%v)\n", err)
		}
	}

	// Build insert statement with placeholders
	placeholders := make([]string, len(result.Cols))
	for i := range placeholders {
		switch driver {
		case "postgres":
			placeholders[i] = "$" + strconv.Itoa(i+1)
		default:
			placeholders[i] = "?"
		}
	}

	quotedCols := make([]string, len(result.Cols))
	for i, c := range result.Cols {
		quotedCols[i] = quoteIdentifier(driver, c)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(driver, targetTable),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	tx, err := extDB.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to prepare insert: %v", err)
	}
	defer stmt.Close()

	count := 0
	for _, row := range result.Rows {
		values := make([]any, len(result.Cols))
		for i, col := range result.Cols {
			values[i] = row[strings.ToLower(col)]
		}

		if _, err := stmt.Exec(values...); err != nil {
			tx.Rollback()
			return count, fmt.Errorf("insert failed at row %d: %v", count+1, err)
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return count, fmt.Errorf("commit failed: %v", err)
	}

	return count, nil
}

// ============================================================================
// Pipeline Command Execution
// ============================================================================

func executePipelineCommand(db *tinysql.DB, ctx context.Context, tenant, line string, verbose bool) error {
	lower := strings.ToLower(strings.TrimSpace(line))

	switch {
	case strings.HasPrefix(lower, "connect "):
		handleConnect(line)
		return nil
	case strings.HasPrefix(lower, "load ") || strings.HasPrefix(lower, "import file "):
		handleLoadFile(db, ctx, tenant, line)
		return nil
	case strings.HasPrefix(lower, "import "):
		handleImportDB(db, ctx, tenant, line)
		return nil
	case strings.HasPrefix(lower, "export "):
		handleExport(db, ctx, tenant, line)
		return nil
	case strings.HasPrefix(lower, "copy "):
		handleCopy(db, ctx, tenant, line)
		return nil
	default:
		// Execute as SQL on tinySQL
		stmt, err := tinysql.ParseSQL(line)
		if err != nil {
			return fmt.Errorf("parse error: %v", err)
		}
		result, err := tinysql.Execute(ctx, db, tenant, stmt)
		if err != nil {
			return fmt.Errorf("execute error: %v", err)
		}
		if result != nil && verbose {
			outputTable(result)
			fmt.Printf("(%d rows)\n", len(result.Rows))
		}
		return nil
	}
}

// ============================================================================
// SQL Builders
// ============================================================================

func buildCreateTable(tableName string, cols []string, colTypes []*sql.ColumnType) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(tableName)
	sb.WriteString(" (")

	for i, col := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sanitizeColumnName(col))
		sb.WriteString(" ")
		sb.WriteString(mapExternalType(colTypes[i]))
	}

	sb.WriteString(")")
	return sb.String()
}

func buildInsert(tableName string, cols []string, values []any) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")

	for i, col := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sanitizeColumnName(col))
	}

	sb.WriteString(") VALUES (")

	for i, val := range values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(formatValue(val))
	}

	sb.WriteString(")")
	return sb.String()
}

func buildExternalCreateTable(driver, tableName string, cols []string) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
	sb.WriteString(quoteIdentifier(driver, tableName))
	sb.WriteString(" (")

	for i, col := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdentifier(driver, col))
		sb.WriteString(" TEXT") // Use TEXT as safe default for all columns
	}

	sb.WriteString(")")
	return sb.String()
}

// ============================================================================
// Output Formatting
// ============================================================================

func executeAndOutput(db *tinysql.DB, ctx context.Context, tenant, query, outputFile, format string, verbose bool) error {
	stmt, err := tinysql.ParseSQL(query)
	if err != nil {
		return fmt.Errorf("parse error: %v", err)
	}

	start := time.Now()
	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	duration := time.Since(start)

	if err != nil {
		return fmt.Errorf("execute error: %v", err)
	}

	if result == nil {
		if verbose {
			fmt.Fprintf(os.Stderr, "✓ OK (%v)\n", duration)
		}
		return nil
	}

	var out io.Writer = os.Stdout
	if outputFile != "" {
		f, err := os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("failed to create output file: %v", err)
		}
		defer f.Close()
		out = f
	}

	switch format {
	case "json":
		outputJSON(out, result)
	case "csv":
		outputCSV(out, result)
	default:
		outputTableWriter(out, result)
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "(%d rows in %v)\n", len(result.Rows), duration)
	}

	return nil
}

func outputTable(result *tinysql.ResultSet) {
	outputTableWriter(os.Stdout, result)
}

func outputTableWriter(out io.Writer, result *tinysql.ResultSet) {
	if len(result.Rows) == 0 {
		fmt.Fprintln(out, "No results")
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
		fmt.Fprintf(out, "%-*s  ", widths[i], col)
	}
	fmt.Fprintln(out)

	for i := range result.Cols {
		fmt.Fprint(out, strings.Repeat("─", widths[i])+"  ")
	}
	fmt.Fprintln(out)

	for _, row := range result.Rows {
		for i, col := range result.Cols {
			value := ""
			if v, ok := row[strings.ToLower(col)]; ok && v != nil {
				value = fmt.Sprintf("%v", v)
				if len(value) > widths[i] {
					value = value[:widths[i]-3] + "..."
				}
			}
			fmt.Fprintf(out, "%-*s  ", widths[i], value)
		}
		fmt.Fprintln(out)
	}
}

func outputJSON(out io.Writer, result *tinysql.ResultSet) {
	var records []map[string]any
	for _, row := range result.Rows {
		record := make(map[string]any)
		for _, col := range result.Cols {
			if value, ok := row[strings.ToLower(col)]; ok {
				record[col] = value
			}
		}
		records = append(records, record)
	}

	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	encoder.Encode(records)
}

func outputCSV(out io.Writer, result *tinysql.ResultSet) {
	writer := csv.NewWriter(out)
	defer writer.Flush()

	writer.Write(result.Cols)

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

// ============================================================================
// Helpers
// ============================================================================

func parseDSN(dsn string) (driver, connStr string) {
	// Support prefixed DSN formats
	lower := strings.ToLower(dsn)

	switch {
	case strings.HasPrefix(lower, "postgres://") || strings.HasPrefix(lower, "postgresql://"):
		return "postgres", dsn
	case strings.HasPrefix(lower, "mysql://"):
		// Strip mysql:// prefix for go-sql-driver/mysql
		return "mysql", dsn[len("mysql://"):]
	case strings.HasPrefix(lower, "sqlite://"):
		return "sqlite", dsn[len("sqlite://"):]
	case strings.HasPrefix(lower, "mssql://") || strings.HasPrefix(lower, "sqlserver://"):
		// Convert mssql:// to sqlserver:// for the driver
		if strings.HasPrefix(lower, "mssql://") {
			return "sqlserver", "sqlserver://" + dsn[len("mssql://"):]
		}
		return "sqlserver", dsn
	default:
		// Try to auto-detect from content
		if strings.Contains(dsn, "tcp(") || strings.Contains(dsn, "@/") {
			return "mysql", dsn
		}
		if strings.Contains(dsn, "sslmode=") || strings.Contains(dsn, "host=") {
			return "postgres", dsn
		}
		if strings.HasSuffix(dsn, ".db") || strings.HasSuffix(dsn, ".sqlite") || strings.HasSuffix(dsn, ".sqlite3") {
			return "sqlite", dsn
		}
		// Default to SQLite for file paths
		return "sqlite", dsn
	}
}

func mapExternalType(ct *sql.ColumnType) string {
	typeName := strings.ToUpper(ct.DatabaseTypeName())

	switch {
	case strings.Contains(typeName, "INT"):
		return "INT"
	case strings.Contains(typeName, "FLOAT") || strings.Contains(typeName, "DOUBLE") ||
		strings.Contains(typeName, "REAL") || strings.Contains(typeName, "NUMERIC") ||
		strings.Contains(typeName, "DECIMAL"):
		return "FLOAT"
	case strings.Contains(typeName, "BOOL"):
		return "BOOL"
	case strings.Contains(typeName, "TIME") || strings.Contains(typeName, "DATE"):
		return "TEXT"
	case strings.Contains(typeName, "JSON"):
		return "JSON"
	default:
		return "TEXT"
	}
}

func formatValue(val any) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case time.Time:
		return "'" + v.Format("2006-01-02 15:04:05") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	default:
		s := fmt.Sprintf("%v", v)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
}

func quoteIdentifier(driver, name string) string {
	switch driver {
	case "mysql":
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case "sqlserver":
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	default:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

func tableNameFromFile(filename string) string {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	return sanitizeTableName(name)
}

func sanitizeTableName(name string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, name)
}

func sanitizeColumnName(name string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, name)
}

func maskDSN(dsn string) string {
	// Simple password masking - replace anything between : and @ with ***
	atIdx := strings.Index(dsn, "@")
	if atIdx == -1 {
		return dsn
	}
	colonIdx := strings.Index(dsn[:atIdx], ":")
	if colonIdx == -1 {
		return dsn
	}
	// Find second colon (password separator) after the scheme
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd != -1 {
		rest := dsn[schemeEnd+3:]
		colonInRest := strings.Index(rest, ":")
		if colonInRest != -1 && schemeEnd+3+colonInRest < atIdx {
			masked := dsn[:schemeEnd+3+colonInRest+1] + "***" + dsn[atIdx:]
			return masked
		}
	}
	return dsn
}

func splitQuotedFields(input string) []string {
	var fields []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(input); i++ {
		ch := input[i]

		if !inQuote && (ch == '"' || ch == '\'') {
			inQuote = true
			quoteChar = ch
			continue
		}

		if inQuote && ch == quoteChar {
			inQuote = false
			fields = append(fields, current.String())
			current.Reset()
			continue
		}

		if !inQuote && ch == ' ' {
			if current.Len() > 0 {
				fields = append(fields, current.String())
				current.Reset()
			}
			continue
		}

		current.WriteByte(ch)
	}

	if current.Len() > 0 {
		fields = append(fields, current.String())
	}

	return fields
}
