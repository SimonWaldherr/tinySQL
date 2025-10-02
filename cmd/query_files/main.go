package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type Config struct {
	Files       []string
	Query       string
	TableName   string
	Delimiter   string
	Interactive bool
	Verbose     bool
	Output      string
}

// JSONRecord represents a generic JSON object
type JSONRecord map[string]interface{}

// XMLNode represents a generic XML element
type XMLNode struct {
	XMLName xml.Name
	Attrs   []xml.Attr `xml:",any,attr"`
	Content string     `xml:",chardata"`
	Nodes   []XMLNode  `xml:",any"`
}

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
	flag.StringVar(&config.Delimiter, "delimiter", ",", "CSV delimiter")
	flag.BoolVar(&config.Interactive, "interactive", false, "Run in interactive mode")
	flag.BoolVar(&config.Verbose, "verbose", false, "Verbose output")
	flag.StringVar(&config.Output, "output", "table", "Output format: table, json, csv")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "tinySQL File Query Tool\n\n")
		fmt.Fprintf(os.Stderr, "Query CSV, JSON, and XML files using SQL syntax.\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s [options] file1 [file2 ...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Query a CSV file\n")
		fmt.Fprintf(os.Stderr, "  %s -query \"SELECT * FROM users WHERE age > 25\" users.csv\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Join multiple files\n")
		fmt.Fprintf(os.Stderr, "  %s -query \"SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id\" users.csv orders.json\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Interactive mode\n")
		fmt.Fprintf(os.Stderr, "  %s -interactive data/\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	config.Files = flag.Args()
	return config
}

func executeQuery(config Config) error {
	db := storage.NewDB()
	ctx := context.Background()
	tenant := "default"

	// Load files into database
	for _, file := range config.Files {
		tableName := config.TableName
		if tableName == "" {
			tableName = getTableNameFromFile(file)
		}

		if config.Verbose {
			fmt.Printf("Loading %s into table '%s'...\n", file, tableName)
		}

		err := loadFile(db, ctx, tenant, file, tableName, config)
		if err != nil {
			return fmt.Errorf("failed to load %s: %v", file, err)
		}

		if config.Verbose {
			fmt.Printf("✓ Loaded %s\n", file)
		}
	}

	// Execute query
	if config.Verbose {
		fmt.Printf("Executing query: %s\n", config.Query)
	}

	p := engine.NewParser(config.Query)
	stmt, err := p.ParseStatement()
	if err != nil {
		return fmt.Errorf("parse error: %v", err)
	}

	result, err := engine.Execute(ctx, db, tenant, stmt)
	if err != nil {
		return fmt.Errorf("execute error: %v", err)
	}

	// Output results
	if result != nil {
		outputResults(result, config.Output)
	}

	return nil
}

func runInteractiveMode(config Config) {
	db := storage.NewDB()
	ctx := context.Background()
	tenant := "default"

	fmt.Println("tinySQL File Query Tool - Interactive Mode")
	fmt.Println("Type 'help' for commands, 'exit' to quit")

	// Load files from directory if provided
	if len(config.Files) > 0 {
		for _, path := range config.Files {
			if isDirectory(path) {
				loadDirectory(db, ctx, tenant, path, config)
			} else {
				tableName := getTableNameFromFile(path)
				err := loadFile(db, ctx, tenant, path, tableName, config)
				if err != nil {
					fmt.Printf("Warning: Failed to load %s: %v\n", path, err)
				} else {
					fmt.Printf("✓ Loaded %s as table '%s'\n", path, tableName)
				}
			}
		}
	}

	// Interactive loop
	for {
		fmt.Print("sql> ")
		var input string
		fmt.Scanln(&input)

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
		case "tables", "show tables":
			showTables(db, ctx, tenant)
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
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Printf("✓ Loaded %s as table '%s'\n", file, tableName)
				}
			}
			continue
		}

		// Execute SQL query
		p := engine.NewParser(input)
		stmt, err := p.ParseStatement()
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		start := time.Now()
		result, err := engine.Execute(ctx, db, tenant, stmt)
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("Execute error: %v\n", err)
			continue
		}

		if result != nil {
			outputResults(result, "table")
			fmt.Printf("\n(%d rows in %v)\n", len(result.Rows), duration)
		} else {
			fmt.Printf("OK (%v)\n", duration)
		}
	}
}

func loadFile(db *storage.DB, ctx context.Context, tenant, filename, tableName string, config Config) error {
	ext := strings.ToLower(filepath.Ext(filename))

	switch ext {
	case ".csv":
		return loadCSV(db, ctx, tenant, filename, tableName, config.Delimiter)
	case ".json":
		return loadJSON(db, ctx, tenant, filename, tableName)
	case ".xml":
		return loadXML(db, ctx, tenant, filename, tableName)
	default:
		return fmt.Errorf("unsupported file format: %s", ext)
	}
}

func loadCSV(db *storage.DB, ctx context.Context, tenant, filename, tableName string, delimiter string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = rune(delimiter[0])
	reader.TrimLeadingSpace = true

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %v", err)
	}

	// Create table
	columns := make([]storage.Column, len(headers))
	for i, header := range headers {
		columns[i] = storage.Column{
			Name: strings.TrimSpace(header),
			Type: storage.TextType, // Start with TEXT, we'll infer types
		}
	}

	// Read all records to infer types
	var records [][]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record: %v", err)
		}
		records = append(records, record)
	}

	// Infer column types
	inferColumnTypes(columns, records)

	// Create table
	table := storage.NewTable(tableName, columns, false)
	err = db.Put(tenant, table)
	if err != nil {
		return err
	}

	// Insert records
	for _, record := range records {
		row := make([]any, len(columns))
		for i, value := range record {
			if i < len(columns) {
				row[i] = convertValue(value, columns[i].Type)
			}
		}
		table.Rows = append(table.Rows, row)
	}

	return nil
}

func loadJSON(db *storage.DB, ctx context.Context, tenant, filename, tableName string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	// Try to read as array first
	var records []JSONRecord
	if err := decoder.Decode(&records); err != nil {
		// Reset and try as single object
		file.Seek(0, 0)
		decoder = json.NewDecoder(file)
		var record JSONRecord
		if err := decoder.Decode(&record); err != nil {
			return fmt.Errorf("failed to parse JSON: %v", err)
		}
		records = []JSONRecord{record}
	}

	if len(records) == 0 {
		return fmt.Errorf("no records found in JSON file")
	}

	// Determine columns from all records
	columnSet := make(map[string]bool)
	for _, record := range records {
		for key := range record {
			columnSet[key] = true
		}
	}

	var columnNames []string
	for name := range columnSet {
		columnNames = append(columnNames, name)
	}

	// Create columns
	columns := make([]storage.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = storage.Column{
			Name: name,
			Type: storage.JsonType, // Use JSON type for flexibility
		}
	}

	// Create table
	table := storage.NewTable(tableName, columns, false)
	err = db.Put(tenant, table)
	if err != nil {
		return err
	}

	// Insert records
	for _, record := range records {
		row := make([]any, len(columns))
		for i, colName := range columnNames {
			if value, exists := record[colName]; exists {
				row[i] = value
			}
		}
		table.Rows = append(table.Rows, row)
	}

	return nil
}

func loadXML(db *storage.DB, ctx context.Context, tenant, filename, tableName string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	var root XMLNode
	err = xml.Unmarshal(content, &root)
	if err != nil {
		return fmt.Errorf("failed to parse XML: %v", err)
	}

	// Convert XML to flat records
	records := flattenXML(root)
	if len(records) == 0 {
		return fmt.Errorf("no records found in XML file")
	}

	// Determine columns
	columnSet := make(map[string]bool)
	for _, record := range records {
		for key := range record {
			columnSet[key] = true
		}
	}

	var columnNames []string
	for name := range columnSet {
		columnNames = append(columnNames, name)
	}

	// Create columns
	columns := make([]storage.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = storage.Column{
			Name: name,
			Type: storage.TextType,
		}
	}

	// Create table
	table := storage.NewTable(tableName, columns, false)
	err = db.Put(tenant, table)
	if err != nil {
		return err
	}

	// Insert records
	for _, record := range records {
		row := make([]any, len(columns))
		for i, colName := range columnNames {
			if value, exists := record[colName]; exists {
				row[i] = value
			}
		}
		table.Rows = append(table.Rows, row)
	}

	return nil
}

func flattenXML(node XMLNode) []map[string]interface{} {
	var records []map[string]interface{}

	// If this node has child nodes of the same type, treat each as a record
	childGroups := make(map[string][]XMLNode)
	for _, child := range node.Nodes {
		childGroups[child.XMLName.Local] = append(childGroups[child.XMLName.Local], child)
	}

	// Find the most common child type (likely our records)
	var maxCount int
	var recordType string
	for tagName, children := range childGroups {
		if len(children) > maxCount {
			maxCount = len(children)
			recordType = tagName
		}
	}

	if maxCount > 1 {
		// Convert each child of the record type to a record
		for _, child := range childGroups[recordType] {
			record := make(map[string]interface{})

			// Add attributes
			for _, attr := range child.Attrs {
				record[attr.Name.Local] = attr.Value
			}

			// Add child elements
			for _, grandchild := range child.Nodes {
				if grandchild.Content != "" {
					record[grandchild.XMLName.Local] = strings.TrimSpace(grandchild.Content)
				}
			}

			// Add content if no children
			if len(child.Nodes) == 0 && child.Content != "" {
				record["content"] = strings.TrimSpace(child.Content)
			}

			records = append(records, record)
		}
	} else {
		// Single record from the root
		record := make(map[string]interface{})

		// Add attributes
		for _, attr := range node.Attrs {
			record[attr.Name.Local] = attr.Value
		}

		// Add child elements
		for _, child := range node.Nodes {
			if child.Content != "" {
				record[child.XMLName.Local] = strings.TrimSpace(child.Content)
			}
		}

		records = append(records, record)
	}

	return records
}

func inferColumnTypes(columns []storage.Column, records [][]string) {
	for i := range columns {
		isInt := true
		isFloat := true
		isBool := true

		for _, record := range records {
			if i >= len(record) {
				continue
			}
			value := strings.TrimSpace(record[i])
			if value == "" {
				continue
			}

			// Check if it's an integer
			if _, err := strconv.Atoi(value); err != nil {
				isInt = false
			}

			// Check if it's a float
			if _, err := strconv.ParseFloat(value, 64); err != nil {
				isFloat = false
			}

			// Check if it's a boolean
			if strings.ToLower(value) != "true" && strings.ToLower(value) != "false" &&
				value != "1" && value != "0" {
				isBool = false
			}
		}

		if isInt {
			columns[i].Type = storage.IntType
		} else if isFloat {
			columns[i].Type = storage.FloatType
		} else if isBool {
			columns[i].Type = storage.BoolType
		} else {
			columns[i].Type = storage.TextType
		}
	}
}

func convertValue(value string, dataType storage.ColType) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	switch dataType {
	case storage.IntType:
		if v, err := strconv.Atoi(value); err == nil {
			return v
		}
	case storage.FloatType:
		if v, err := strconv.ParseFloat(value, 64); err == nil {
			return v
		}
	case storage.BoolType:
		switch strings.ToLower(value) {
		case "true", "1":
			return true
		case "false", "0":
			return false
		}
	}
	return value
}

func getTableNameFromFile(filename string) string {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

func isDirectory(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func loadDirectory(db *storage.DB, ctx context.Context, tenant, dir string, config Config) {
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("Warning: Failed to read directory %s: %v\n", dir, err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := filepath.Join(dir, file.Name())
		ext := strings.ToLower(filepath.Ext(file.Name()))

		if ext == ".csv" || ext == ".json" || ext == ".xml" {
			tableName := getTableNameFromFile(file.Name())
			err := loadFile(db, ctx, tenant, filename, tableName, config)
			if err != nil {
				fmt.Printf("Warning: Failed to load %s: %v\n", filename, err)
			} else {
				fmt.Printf("✓ Loaded %s as table '%s'\n", filename, tableName)
			}
		}
	}
}

func outputResults(result *engine.ResultSet, format string) {
	switch format {
	case "json":
		outputJSON(result)
	case "csv":
		outputCSV(result)
	default:
		outputTable(result)
	}
}

func outputTable(result *engine.ResultSet) {
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

	// Print header
	for i, col := range result.Cols {
		fmt.Printf("%-*s", widths[i]+2, col)
	}
	fmt.Println()

	// Print separator
	for i := range result.Cols {
		fmt.Print(strings.Repeat("-", widths[i]+2))
	}
	fmt.Println()

	// Print rows
	for _, row := range result.Rows {
		for i, col := range result.Cols {
			value := ""
			if v, ok := row[strings.ToLower(col)]; ok && v != nil {
				value = fmt.Sprintf("%v", v)
			}
			fmt.Printf("%-*s", widths[i]+2, value)
		}
		fmt.Println()
	}
}

func outputJSON(result *engine.ResultSet) {
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

func outputCSV(result *engine.ResultSet) {
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

func showTables(db *storage.DB, ctx context.Context, tenant string) {
	fmt.Println("Available tables:")
	// This would require extending the storage interface to list tables
	// For now, we'll show a placeholder
	fmt.Println("(Table listing not yet implemented)")
}

func printHelp() {
	fmt.Println(`Available commands:
  help                    - Show this help
  exit, quit, q          - Exit the program
  tables, show tables    - Show available tables
  load <file>            - Load a file into a table
  
SQL Examples:
  SELECT * FROM users WHERE age > 25
  SELECT name, email FROM users ORDER BY name
  SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name
  SELECT * FROM products WHERE JSON_GET(meta, 'category') = 'electronics'`)
}
